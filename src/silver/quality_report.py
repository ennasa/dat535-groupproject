"""Before/after quality report for Silver (#22).

One JSON file per run: reports/quality/<run_id>.json

For each catch year it has:
- row counts: Bronze -> after cleaning -> rejected / accepted (and how many are flagged)
- the result of every quality rule
- for important columns: missing values before (Bronze), after cleaning, and in Silver.
  "invalid" = had a value in Bronze, but it could not be read (e.g. a bad date), so it became null.
- a check that the quarantine has the same number of rows as were rejected
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bronze.manifest import write_json
from common.config import layer_path
from common.contracts import RuleResult, RunContext
from silver.model import source_to_target
from silver.quarantine import quarantine_path
from silver.rules import QUALITY_FLAGS


def report_path(ctx: RunContext) -> Path:
    return Path(layer_path(ctx.config, "reports")).resolve() / "quality" / f"{ctx.run_id}.json"


def _count_missing(df: DataFrame, columns: list[str], blank_is_missing: bool) -> dict[str, int]:
    """Count missing values for many columns in one pass."""
    if not columns:
        return {}
    exprs = []
    for column in columns:
        missing = F.col(column).isNull()
        if blank_is_missing:  # Bronze stores everything as text, so "" also means missing
            missing = missing | (F.trim(F.col(column).cast("string")) == "")
        exprs.append(F.sum(missing.cast("long")).alias(column))
    row = df.agg(*exprs).first()
    return {column: int(row[column] or 0) for column in columns}


def column_profile(
    bronze: DataFrame, cleaned: DataFrame, accepted: DataFrame, columns: list[str]
) -> list[dict[str, Any]]:
    """Missing values per column in Bronze, after cleaning and in Silver."""
    to_source = {target: source for source, target in source_to_target().items()}
    columns = [c for c in columns if c in cleaned.columns]  # skip columns that don't exist
    sources = {c: to_source.get(c, c) for c in columns}
    in_bronze = [c for c in columns if sources[c] in bronze.columns]

    before = _count_missing(bronze, [sources[c] for c in in_bronze], blank_is_missing=True)
    after_clean = _count_missing(cleaned, columns, blank_is_missing=False)
    in_silver = _count_missing(accepted, columns, blank_is_missing=False)

    profile = []
    for column in columns:
        missing_before = before.get(sources[column]) if column in in_bronze else None
        invalid = None
        if missing_before is not None:
            invalid = max(after_clean[column] - missing_before, 0)
        profile.append(
            {
                "column": column,
                "source_column": sources[column] if column in in_bronze else None,
                "missing_in_bronze": missing_before,
                "invalid_values": invalid,
                "missing_after_clean": after_clean[column],
                "missing_in_silver": in_silver[column],
            }
        )
    return profile


def build_period_report(
    period: str,
    bronze: DataFrame,
    cleaned: DataFrame,
    accepted: DataFrame,
    *,
    rows_in: int,
    rows_cleaned: int,
    rows_rejected: int,
    results: list[RuleResult],
    ctx: RunContext,
    table: str,
) -> dict[str, Any]:
    columns = list(ctx.config["quality"].get("report_columns", []))

    flagged = 0
    if QUALITY_FLAGS in accepted.columns:
        flagged = accepted.filter(F.size(QUALITY_FLAGS) > 0).count()

    quarantine = {"rows": 0, "matches_rejected": rows_rejected == 0}
    summary = quarantine_path(table, period, ctx) / "_summary.json"
    if summary.is_file():
        rows = json.loads(summary.read_text(encoding="utf-8"))["rows"]
        quarantine = {"rows": rows, "matches_rejected": rows == rows_rejected}

    return {
        "rows": {
            "bronze": rows_in,
            "after_clean": rows_cleaned,
            "duplicates_removed": rows_in - rows_cleaned,
            "rejected": rows_rejected,
            "accepted": rows_cleaned - rows_rejected,
            "accepted_with_flags": flagged,
        },
        "rules": [
            {"rule": r.rule, "action": r.action, "failed": r.failed} for r in results
        ],
        "columns": column_profile(bronze, cleaned, accepted, columns),
        "quarantine": quarantine,
    }


def write_report(ctx: RunContext, periods: dict[str, dict[str, Any]]) -> Path:
    path = report_path(ctx)
    write_json(
        path,
        {
            "run_id": ctx.run_id,
            "env": ctx.env,
            "started_at": ctx.started_at.isoformat(),
            "periods": periods,
        },
    )
    return path