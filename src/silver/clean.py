"""Bronze-to-Silver cleaning expressions for the proposed A5 model."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from silver.model import KEY_FIELDS, SILVER_FIELDS, SilverField

SOURCE_DATE_FORMAT = "dd.MM.yyyy"
SOURCE_TIMESTAMP_FORMAT = "dd.MM.yyyy HH.mm.ss,SSSSSSSSS"
SOURCE_TIMEZONE = "Europe/Oslo"


def _quoted(name: str) -> Column:
    """Reference a Bronze field literally, including names containing dots."""
    return F.col(f"`{name.replace('`', '``')}`")


def _text_value(column: Column) -> Column:
    """Trim source text and represent empty or whitespace-only values as null."""
    trimmed = F.trim(column.cast("string"))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _date_value(column: Column) -> Column:
    parsed = F.try_to_timestamp(_text_value(column), F.lit(SOURCE_DATE_FORMAT))
    return parsed.cast("date")


def _document_timestamp_utc(column: Column) -> Column:
    local_timestamp = F.try_to_timestamp(
        _text_value(column), F.lit(SOURCE_TIMESTAMP_FORMAT)
    )
    return F.to_utc_timestamp(local_timestamp, SOURCE_TIMEZONE)


def _decimal_value(column: Column) -> Column:
    normalized = F.regexp_replace(_text_value(column), ",", ".")
    return normalized.cast("decimal(24,6)")


def _boolean_value(column: Column) -> Column:
    value = _text_value(column)
    return F.when(value == "1", F.lit(True)).when(value == "0", F.lit(False))


def _clean_expression(field: SilverField) -> Column:
    source = _quoted(field.source)

    if field.name == "document_version_at_utc":
        return _document_timestamp_utc(source).alias(field.name)
    if field.name == "ingested_at":
        # Bronze created this value as a real UTC timestamp, not source text.
        return source.cast("timestamp").alias(field.name)
    if isinstance(field.data_type, StringType):
        return _text_value(source).alias(field.name)
    if isinstance(field.data_type, DateType):
        return _date_value(source).alias(field.name)
    if isinstance(field.data_type, BooleanType):
        return _boolean_value(source).cast("boolean").alias(field.name)
    if isinstance(field.data_type, (IntegerType, LongType)):
        return _text_value(source).cast(field.data_type).alias(field.name)
    if isinstance(field.data_type, DecimalType):
        return _decimal_value(source).cast(field.data_type).alias(field.name)
    if isinstance(field.data_type, TimestampType):
        raise ValueError(f"Timestamp field {field.name!r} needs an explicit time-zone rule")
    raise TypeError(f"Unsupported Silver type for {field.name!r}: {field.data_type}")


def clean(df: DataFrame, *, dedup_strategy: str = "optimized") -> DataFrame:
    """Return typed, named and exact-deduplicated Silver candidate records.

    Invalid casts become null and remain available for the quality rules. This function
    does not apply reject/correct/flag policy or resolve conflicting business keys.
    """
    required = {field.source for field in SILVER_FIELDS}
    missing = sorted(required - set(df.columns))
    if missing:
        preview = ", ".join(repr(name) for name in missing[:5])
        suffix = " ..." if len(missing) > 5 else ""
        raise ValueError(f"Bronze input is missing {len(missing)} model fields: {preview}{suffix}")

    if dedup_strategy not in {"baseline", "optimized"}:
        raise ValueError("dedup_strategy must be 'baseline' or 'optimized'")

    result = df.select(*[_clean_expression(field) for field in SILVER_FIELDS])
    content_fields = [field.name for field in SILVER_FIELDS if field.role != "provenance"]
    if dedup_strategy == "baseline":
        return result.dropDuplicates(content_fields)

    # An exact duplicate must also repeat the business key. A4 found the key unique,
    # and the A8 baseline showed that always shuffling every wide content field caused
    # nearly all spill and skew. Check the five narrow key fields first; retain the
    # full exact-deduplication fallback for future snapshots with candidate duplicates.
    has_duplicate_key = (
        result.groupBy(*KEY_FIELDS)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    )
    if has_duplicate_key:
        return result.dropDuplicates(content_fields)
    # The source Parquet has one much larger input split. Repartition once by the
    # well-distributed unique key so count, enrichment and write do not each wait for
    # the same straggler. The configured Spark shuffle partition count is used.
    return result.repartition(*[F.col(name) for name in KEY_FIELDS])
