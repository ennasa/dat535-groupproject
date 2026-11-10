"""Produce compact A9/A10 evidence from the isolated Gold preview."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from common.config import load_config
from common.spark import get_spark


def records(rows, dimensions: list[str]) -> list[dict]:
    return [
        {
            **{name: row[name] for name in dimensions},
            "round_weight_tonnes": round(float(row["round_weight_tonnes"] or 0), 3),
            "line_count": int(row["line_count"] or 0),
        }
        for row in rows
    ]


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    mart = root / "data/preview/gold/fisheries_seasonality"
    candidates = list(mart.glob("model_version=*/input_version=*/_candidate.json"))
    if len(candidates) != 1:
        raise SystemExit(f"Expected one Gold preview candidate, found {len(candidates)}")
    candidate = candidates[0].parent
    marker = json.loads(candidates[0].read_text(encoding="utf-8"))

    config = load_config("dev")
    spark = get_spark(config)
    try:
        fact = spark.read.parquet(str(candidate / "fact_monthly_species_region"))
        month = spark.read.parquet(str(candidate / "dim_month"))
        region = spark.read.parquet(str(candidate / "dim_landing_region"))
        species = spark.read.parquet(str(candidate / "dim_species"))
        detail = fact.join(month, "month_id").join(region, "region_id").join(
            species, "species_id"
        )
        main_scope = detail.filter(
            (F.col("landing_country_code") == "NOR") & (F.col("year") == 2023)
        )

        monthly = (
            main_scope.groupBy("month_id")
            .agg(
                (F.sum("total_round_weight_kg") / 1000).alias("round_weight_tonnes"),
                F.sum("line_count").alias("line_count"),
            )
            .orderBy("month_id")
            .collect()
        )
        top_species = (
            main_scope.groupBy("species_code", "species_name")
            .agg(
                (F.sum("total_round_weight_kg") / 1000).alias("round_weight_tonnes"),
                F.sum("line_count").alias("line_count"),
            )
            .orderBy(F.desc("round_weight_tonnes"))
            .limit(10)
            .collect()
        )
        top_counties = (
            main_scope.groupBy("landing_county_code", "landing_county_name")
            .agg(
                (F.sum("total_round_weight_kg") / 1000).alias("round_weight_tonnes"),
                F.sum("line_count").alias("line_count"),
            )
            .orderBy(F.desc("round_weight_tonnes"))
            .limit(10)
            .collect()
        )
        scope_totals = main_scope.agg(
            (F.sum("total_round_weight_kg") / 1000).alias("round_weight_tonnes"),
            F.sum("line_count").alias("line_count"),
            F.sum("missing_round_weight_rows").alias("missing_round_weight_rows"),
            F.sum("zero_round_weight_rows").alias("zero_round_weight_rows"),
        ).first()
        landing_year_coverage = [
            {
                "year": int(row["year"]),
                "line_count": int(row["line_count"]),
            }
            for row in detail.groupBy("year")
            .agg(F.sum("line_count").alias("line_count"))
            .orderBy("year")
            .collect()
        ]
    finally:
        spark.stop()

    evidence = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "unvalidated local preview; not published Gold",
        "consumer_question": marker["consumer_question"],
        "main_scope": "landing_country_code=NOR and landing year=2023",
        "primary_measure": "round weight in tonnes (source kilograms / 1000)",
        "gold_candidate": {
            "silver_rows_in": marker["rows_in"],
            "fact_line_count": marker["fact_line_count"],
            "table_rows": marker["table_rows"],
            "unvalidated_preview": marker["unvalidated_preview"],
        },
        "scope_totals": {
            "round_weight_tonnes": round(float(scope_totals["round_weight_tonnes"]), 3),
            "line_count": int(scope_totals["line_count"]),
            "missing_round_weight_rows": int(scope_totals["missing_round_weight_rows"]),
            "zero_round_weight_rows": int(scope_totals["zero_round_weight_rows"]),
        },
        "landing_year_coverage": landing_year_coverage,
        "monthly": records(monthly, ["month_id"]),
        "top_species": records(top_species, ["species_code", "species_name"]),
        "top_counties": records(
            top_counties, ["landing_county_code", "landing_county_name"]
        ),
    }
    path = root / "docs/evidence/a9-a10-preview.json"
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
