"""Build the fisheries seasonality Gold candidate tables from trusted Silver."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from bronze.manifest import write_json
from common.config import layer_path
from common.contracts import LayerResult, RunContext

log = logging.getLogger(__name__)

GOLD_MART = "fisheries_seasonality"
GOLD_MODEL_VERSION = "a9-v1"
TABLE_NAMES: tuple[str, ...] = (
    "dim_month",
    "dim_landing_region",
    "dim_species",
    "fact_monthly_species_region",
)


@dataclass(frozen=True)
class GoldTables:
    dim_month: DataFrame
    dim_landing_region: DataFrame
    dim_species: DataFrame
    fact_monthly_species_region: DataFrame

    def as_dict(self) -> dict[str, DataFrame]:
        return {name: getattr(self, name) for name in TABLE_NAMES}


REQUIRED_SILVER_COLUMNS: frozenset[str] = frozenset(
    {
        "record_id",
        "catch_year",
        "sales_org_code",
        "document_type_code",
        "document_number",
        "landing_date",
        "landing_year",
        "landing_month",
        "landing_year_month",
        "landing_country_code",
        "landing_country_name",
        "landing_county_code",
        "landing_county_name",
        "species_code",
        "species_name",
        "species_group_code",
        "species_group_name",
        "species_main_group_code",
        "species_main_group_name",
        "species_fao_code",
        "species_fao_name",
        "round_weight_kg",
        "product_weight_kg",
        "catch_value_nok",
    }
)


def _with_dimension_keys(silver: DataFrame) -> DataFrame:
    unknown_code = F.lit("UNKNOWN")
    unknown_name = F.lit("Unknown / not supplied")
    normalized = (
        silver.withColumn(
            "gold_landing_country_code",
            F.coalesce("landing_country_code", unknown_code),
        )
        .withColumn(
            "gold_landing_country_name",
            F.coalesce("landing_country_name", unknown_name),
        )
        .withColumn(
            "gold_landing_county_code",
            F.coalesce("landing_county_code", unknown_code),
        )
        .withColumn(
            "gold_landing_county_name",
            F.coalesce("landing_county_name", unknown_name),
        )
    )
    return (
        normalized.withColumn(
            "region_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        "gold_landing_country_code",
                        "gold_landing_country_name",
                        "gold_landing_county_code",
                        "gold_landing_county_name",
                    )
                ),
                256,
            ),
        )
        .withColumn(
            "species_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        "species_code",
                        "species_name",
                        "species_group_code",
                        "species_group_name",
                        "species_main_group_code",
                        "species_main_group_name",
                        "species_fao_code",
                        "species_fao_name",
                    )
                ),
                256,
            ),
        )
        .withColumn("month_id", F.col("landing_year_month"))
    )


def build_tables(silver: DataFrame) -> GoldTables:
    """Create dimensions and the monthly region/species aggregate."""
    missing = sorted(REQUIRED_SILVER_COLUMNS - set(silver.columns))
    if missing:
        raise ValueError(f"Silver input is missing Gold fields: {', '.join(missing)}")

    keyed = _with_dimension_keys(silver)
    dim_month = (
        keyed.select(
            "month_id",
            F.col("landing_year").alias("year"),
            F.col("landing_month").alias("month"),
            F.trunc("landing_date", "month").alias("month_start"),
        )
        .dropDuplicates(["month_id"])
        .orderBy("month_id")
    )
    dim_region = keyed.select(
        "region_id",
        F.col("gold_landing_country_code").alias("landing_country_code"),
        F.col("gold_landing_country_name").alias("landing_country_name"),
        F.col("gold_landing_county_code").alias("landing_county_code"),
        F.col("gold_landing_county_name").alias("landing_county_name"),
    ).dropDuplicates(["region_id"])
    dim_species = keyed.select(
        "species_id",
        "species_code",
        "species_name",
        "species_group_code",
        "species_group_name",
        "species_main_group_code",
        "species_main_group_name",
        "species_fao_code",
        "species_fao_name",
    ).dropDuplicates(["species_id"])

    document_identity = F.struct(
        "catch_year", "sales_org_code", "document_type_code", "document_number"
    )
    fact = keyed.groupBy("month_id", "region_id", "species_id").agg(
        F.count("*").alias("line_count"),
        F.countDistinct(document_identity).alias("document_count"),
        F.sum("round_weight_kg").alias("total_round_weight_kg"),
        F.sum("product_weight_kg").alias("total_product_weight_kg"),
        F.sum("catch_value_nok").alias("total_catch_value_nok"),
        F.sum(F.when(F.col("round_weight_kg").isNull(), 1).otherwise(0)).alias(
            "missing_round_weight_rows"
        ),
        F.sum(F.when(F.col("round_weight_kg") == 0, 1).otherwise(0)).alias(
            "zero_round_weight_rows"
        ),
    )
    region_month = Window.partitionBy("month_id", "region_id")
    fact = fact.withColumn(
        "region_month_round_weight_kg",
        F.sum("total_round_weight_kg").over(region_month),
    ).withColumn(
        "species_share_of_region_month",
        F.when(
            F.col("region_month_round_weight_kg") > 0,
            F.col("total_round_weight_kg") / F.col("region_month_round_weight_kg"),
        ),
    )
    return GoldTables(dim_month, dim_region, dim_species, fact)


def _silver_inputs(ctx: RunContext) -> list[tuple[Path, dict]]:
    root = Path(layer_path(ctx.config, "silver")).resolve() / "catch_document_line"
    allow_unvalidated = ctx.config.get("gold", {}).get("allow_unvalidated_silver", False)
    inputs: list[tuple[Path, dict]] = []
    for period in dict.fromkeys(str(value) for value in ctx.config["source"]["periods"]):
        candidates = []
        for marker in root.glob(
            f"period={period}/source_version=*/model_version=*/_snapshot.json"
        ):
            if (marker.parent / "records" / "_SUCCESS").is_file():
                candidates.append(marker)
        if len(candidates) != 1:
            raise ValueError(
                f"Expected one completed Silver candidate for {period}, found {len(candidates)}"
            )
        metadata = json.loads(candidates[0].read_text(encoding="utf-8"))
        trusted = (
            metadata.get("quality_mode") == "rules-engine"
            and metadata.get("model_approval") == "approved"
        )
        if not trusted and not allow_unvalidated:
            raise ValueError(f"Silver candidate for {period} is not approved and validated")
        inputs.append((candidates[0].parent / "records", metadata))
    return inputs


def _candidate_path(inputs: list[tuple[Path, dict]], ctx: RunContext) -> Path:
    identity = [
        {
            "period": metadata["period"],
            "source_sha256": metadata["source_sha256"],
            "model_version": metadata["model_version"],
        }
        for _, metadata in inputs
    ]
    fingerprint = hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return (
        Path(layer_path(ctx.config, "gold")).resolve()
        / GOLD_MART
        / f"model_version={GOLD_MODEL_VERSION}"
        / f"input_version={fingerprint}"
    )


def run(spark: SparkSession, ctx: RunContext) -> LayerResult:
    """Build an immutable Gold candidate; Person B's publish gate promotes it later."""
    result = LayerResult("gold", "skipped", message="No source periods configured")
    if not ctx.config["source"]["periods"]:
        return result
    try:
        inputs = _silver_inputs(ctx)
        destination = _candidate_path(inputs, ctx)
        if (destination / "_candidate.json").is_file():
            result.outputs.append(str(destination))
            result.message = "Gold candidate already built"
            return result
        if destination.exists():
            raise RuntimeError(f"Incomplete Gold candidate already exists: {destination}")

        silver = spark.read.parquet(*[path.resolve().as_posix() for path, _ in inputs]).persist()
        rows_in = silver.count()
        tables = build_tables(silver)
        table_frames = {name: frame.persist() for name, frame in tables.as_dict().items()}
        counts = {name: frame.count() for name, frame in table_frames.items()}
        fact = table_frames["fact_monthly_species_region"]
        fact_lines = fact.agg(F.sum("line_count").alias("value")).first()["value"] or 0
        if int(fact_lines) != rows_in:
            raise ValueError(f"Gold line reconciliation failed: {rows_in} -> {fact_lines}")

        root = Path(layer_path(ctx.config, "gold")).resolve() / GOLD_MART
        root.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=f".staging-{ctx.run_id}-", dir=root) as temporary:
            staging = Path(temporary) / "candidate"
            staging.mkdir()
            for name, frame in table_frames.items():
                frame.write.mode("errorifexists").parquet((staging / name).resolve().as_posix())
                stored = spark.read.parquet((staging / name).resolve().as_posix()).count()
                if stored != counts[name]:
                    raise ValueError(
                        f"Gold {name} reconciliation failed: {counts[name]} -> {stored}"
                    )
            write_json(
                staging / "_candidate.json",
                {
                    "mart": GOLD_MART,
                    "gold_model_version": GOLD_MODEL_VERSION,
                    "consumer_question": (
                        "How do landed fish quantity and species composition vary by "
                        "Norwegian region and month?"
                    ),
                    "primary_measure": "total_round_weight_kg",
                    "input_snapshots": [metadata for _, metadata in inputs],
                    "unvalidated_preview": any(
                        metadata.get("quality_mode") != "rules-engine"
                        or metadata.get("model_approval") != "approved"
                        for _, metadata in inputs
                    ),
                    "rows_in": rows_in,
                    "table_rows": counts,
                    "fact_line_count": int(fact_lines),
                    "run_id": ctx.run_id,
                    "output": str(destination),
                },
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            staging.rename(destination)

        result.status = "success"
        result.rows_in = rows_in
        result.rows_out = counts["fact_monthly_species_region"]
        result.outputs.extend(str(destination / name) for name in TABLE_NAMES)
        result.message = (
            f"Gold candidate built: {rows_in} Silver lines -> "
            f"{result.rows_out} monthly region/species facts"
        )
        for frame in table_frames.values():
            frame.unpersist()
        silver.unpersist()
    except Exception as exc:
        log.exception("run_id=%s layer=gold status=failed", ctx.run_id)
        result.status = "failed"
        result.message = str(exc)
    return result
