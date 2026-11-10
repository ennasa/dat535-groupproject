"""Derived Silver fields and safe optional reference-data joins."""

from __future__ import annotations

import re
from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.model import KEY_FIELDS, REFERENCE_JOINS, ReferenceJoin

DERIVED_COLUMNS: tuple[str, ...] = (
    "record_id",
    "landing_year",
    "landing_month",
    "landing_year_month",
    "is_delivered_following_year",
    "days_catch_to_landing",
    "is_domestic_landing",
    "has_fao_mapping",
)


def add_derived_fields(df: DataFrame) -> DataFrame:
    """Add deterministic fields that do not require external reference data."""
    missing = sorted(
        {
            *KEY_FIELDS,
            "landing_date",
            "landing_month_code",
            "last_catch_date",
            "landing_country_code",
            "species_fao_code",
        }
        - set(df.columns)
    )
    if missing:
        raise ValueError(f"Cannot enrich without Silver fields: {', '.join(missing)}")

    key_json = F.to_json(F.struct(*[F.col(name) for name in KEY_FIELDS]))
    return (
        df.withColumn("record_id", F.sha2(key_json, 256))
        .withColumn("landing_year", F.year("landing_date"))
        .withColumn("landing_month", F.month("landing_date"))
        .withColumn("landing_year_month", F.date_format("landing_date", "yyyy-MM"))
        .withColumn(
            "is_delivered_following_year",
            F.col("landing_month_code") == F.lit("13"),
        )
        .withColumn(
            "days_catch_to_landing",
            F.datediff(F.col("landing_date"), F.col("last_catch_date")),
        )
        .withColumn(
            "is_domestic_landing",
            F.col("landing_country_code") == F.lit("NOR"),
        )
        .withColumn("has_fao_mapping", F.col("species_fao_code").isNotNull())
    )


def _validate_reference(reference: DataFrame, spec: ReferenceJoin) -> list[str]:
    key = spec.silver_key
    if key not in reference.columns:
        raise ValueError(f"Reference {spec.name!r} is missing join key {key!r}")

    attributes = [column for column in reference.columns if column != key]
    invalid = [column for column in attributes if not re.fullmatch(r"[a-z][a-z0-9_]*", column)]
    if invalid:
        raise ValueError(
            f"Reference {spec.name!r} has non-snake-case attributes: {', '.join(invalid)}"
        )

    bad_key = (
        reference.groupBy(key)
        .count()
        .filter(F.col(key).isNull() | (F.col("count") > 1))
        .limit(1)
        .count()
    )
    if bad_key:
        raise ValueError(f"Reference {spec.name!r} must have one non-null row per {key}")
    return attributes


def join_reference(df: DataFrame, reference: DataFrame, spec: ReferenceJoin) -> DataFrame:
    """Apply one validated many-to-one left join and add a match flag."""
    if spec.silver_key not in df.columns:
        raise ValueError(f"Silver input is missing reference key {spec.silver_key!r}")

    attributes = _validate_reference(reference, spec)
    prefix = f"ref_{spec.name}_"
    collisions = sorted(
        {f"{prefix}{column}" for column in attributes} & set(df.columns)
        | {f"{prefix}matched"} & set(df.columns)
    )
    if collisions:
        raise ValueError(f"Reference output columns already exist: {', '.join(collisions)}")

    right_key = f"__ref_{spec.name}_key"
    prepared = reference.select(
        F.col(spec.silver_key).alias(right_key),
        *[F.col(column).alias(f"{prefix}{column}") for column in attributes],
    )
    joined = df.join(prepared, F.col(spec.silver_key) == F.col(right_key), "left")
    return joined.withColumn(f"{prefix}matched", F.col(right_key).isNotNull()).drop(right_key)


def enrich(df: DataFrame, references: Mapping[str, DataFrame] | None = None) -> DataFrame:
    """Add derived fields and every configured reference join.

    A null match flag means the reference table was not configured. False means it was
    configured but the row's code did not match.
    """
    references = references or {}
    unknown = sorted(set(references) - {spec.name for spec in REFERENCE_JOINS})
    if unknown:
        raise ValueError(f"Unknown reference tables: {', '.join(unknown)}")

    result = add_derived_fields(df)
    for spec in REFERENCE_JOINS:
        if spec.name in references:
            result = join_reference(result, references[spec.name], spec)
        else:
            result = result.withColumn(
                f"ref_{spec.name}_matched", F.lit(None).cast("boolean")
            )
    return result
