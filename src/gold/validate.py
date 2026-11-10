"""Check Gold against Silver before it is published (#29).

`reconcile()` returns a list of problems. An empty list means Gold is correct.
It re-reads the stored Gold tables, so it checks what consumers will actually read.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FACT = "fact_monthly_species_region"
GRAIN = ["month_id", "region_id", "species_id"]
DIMENSIONS = {
    "dim_month": "month_id",
    "dim_landing_region": "region_id",
    "dim_species": "species_id",
}

# Silver column -> Gold fact column; the totals must be the same
TOTALS = {
    "round_weight_kg": "total_round_weight_kg",
    "product_weight_kg": "total_product_weight_kg",
    "catch_value_nok": "total_catch_value_nok",
}
SHARE_TOLERANCE = 1e-6


def _totals_match(silver: DataFrame, fact: DataFrame) -> list[str]:
    silver_sums = silver.agg(
        F.count(F.lit(1)).alias("lines"),
        F.sum(F.col("round_weight_kg").isNull().cast("long")).alias("missing_weight"),
        *[F.sum(column).alias(column) for column in TOTALS],
    ).first()
    gold_sums = fact.agg(
        F.sum("line_count").alias("lines"),
        F.sum("missing_round_weight_rows").alias("missing_weight"),
        *[F.sum(gold).alias(silver_col) for silver_col, gold in TOTALS.items()],
    ).first()

    problems = []
    for name in ["lines", "missing_weight", *TOTALS]:
        if (silver_sums[name] or 0) != (gold_sums[name] or 0):
            problems.append(f"{name}: Silver {silver_sums[name]} != Gold {gold_sums[name]}")
    return problems


def _grain_is_unique(fact: DataFrame) -> list[str]:
    duplicates = fact.groupBy(*GRAIN).count().filter("count > 1").count()
    return [f"{duplicates} duplicate month/region/species rows in {FACT}"] if duplicates else []


def _dimensions_are_valid(tables: dict[str, DataFrame]) -> list[str]:
    problems = []
    for dim, key in DIMENSIONS.items():
        frame = tables[dim]
        duplicates = frame.count() - frame.select(key).distinct().count()
        if duplicates:
            problems.append(f"{dim}: {duplicates} duplicate {key}")
        # every fact row must point to an existing dimension row
        orphans = tables[FACT].join(frame, on=key, how="left_anti").count()
        if orphans:
            problems.append(f"{FACT}: {orphans} rows with unknown {key}")
    return problems


def _shares_add_up(fact: DataFrame) -> list[str]:
    problems = []
    # 1. the species totals in a region and month must add up exactly to the region total
    wrong_total = (
        fact.groupBy("month_id", "region_id")
        .agg(
            F.sum("total_round_weight_kg").alias("species_sum"),
            F.max("region_month_round_weight_kg").alias("region_total"),
        )
        .filter(~F.col("species_sum").eqNullSafe(F.col("region_total")))
        .count()
    )
    if wrong_total:
        problems.append(f"{wrong_total} region/months where species totals don't add up")

    # 2. each share must be its species total divided by the region total.
    # Shares are stored with 6 decimals, so allow a rounding difference of 0.000001
    # (adding up all shares can be off by more when there are many species).
    shares = fact.filter(F.col("region_month_round_weight_kg") > 0)
    expected = F.col("total_round_weight_kg").cast("double") / F.col(
        "region_month_round_weight_kg"
    ).cast("double")
    wrong_share = shares.filter(
        F.abs(F.col("species_share_of_region_month").cast("double") - expected)
        > SHARE_TOLERANCE
    ).count()
    out_of_range = shares.filter(
        (F.col("species_share_of_region_month") < 0) | (F.col("species_share_of_region_month") > 1)
    ).count()
    if wrong_share:
        problems.append(f"{wrong_share} species shares that don't match their totals")
    if out_of_range:
        problems.append(f"{out_of_range} species shares outside 0..1")
    return problems


def reconcile(silver: DataFrame, tables: dict[str, DataFrame]) -> list[str]:
    """Return all problems found. Empty list = Gold is valid."""
    missing = [name for name in [FACT, *DIMENSIONS] if name not in tables]
    if missing:
        return [f"missing Gold tables: {missing}"]
    fact = tables[FACT]
    return (
        _totals_match(silver, fact)
        + _grain_is_unique(fact)
        + _dimensions_are_valid(tables)
        + _shares_add_up(fact)
    )