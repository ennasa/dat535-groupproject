"""Write rejected Silver rows to quarantine (#20).

Rejected rows are not deleted. They are saved per run, so we can see what was removed
and why (the `_failed_rules` column from rules.py).

Layout: quarantine/<table>/period=<year>/run_id=<run id>/
    records/         the rejected rows (Parquet)
    _summary.json    how many rows, and how many per rule
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bronze.manifest import write_json
from common.config import layer_path
from common.contracts import RunContext
from silver.rules import FAILED_RULES


def quarantine_path(table: str, period: str, ctx: RunContext) -> Path:
    root = Path(layer_path(ctx.config, "quarantine")).resolve()
    return root / table / f"period={period}" / f"run_id={ctx.run_id}"


def write_rejected(rejected: DataFrame, period: str, ctx: RunContext, *, table: str) -> list[str]:
    """Save rejected rows and a summary. Returns the folder path."""
    path = quarantine_path(table, period, ctx)
    records = path / "records"

    # errorifexists: a run never overwrites quarantine from another run
    rejected.withColumn("_quarantine_run_id", F.lit(ctx.run_id)).write.mode(
        "errorifexists"
    ).parquet(records.as_posix())

    # check that everything was written
    expected = rejected.count()
    stored = rejected.sparkSession.read.parquet(records.as_posix()).count()
    if stored != expected:
        raise ValueError(f"Quarantine row check failed: {expected} -> {stored}")

    # one row can break several rules, so the per-rule counts can add up to more than `rows`
    per_rule = (
        rejected.select(F.explode(FAILED_RULES).alias("rule")).groupBy("rule").count().collect()
    )
    write_json(
        path / "_summary.json",
        {
            "run_id": ctx.run_id,
            "table": table,
            "period": period,
            "rows": stored,
            "rules": {row["rule"]: row["count"] for row in sorted(per_rule)},
        },
    )
    return [str(path)]