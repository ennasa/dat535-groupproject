"""Orchestrate clean -> quality rules -> enrich -> versioned Silver write."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import DataFrame, SparkSession

from bronze import manifest as bronze_manifest
from common.config import layer_path
from common.contracts import LayerResult, RuleResult, RunContext
from silver.clean import clean
from silver.enrich import DERIVED_COLUMNS, enrich
from silver.model import APPROVAL_STATUS, MODEL_VERSION
from silver.quality_report import build_period_report, report_path, write_report
from silver.quarantine import write_rejected

log = logging.getLogger(__name__)

TABLE_NAME = "catch_document_line"
RulesEngine = Callable[
    [DataFrame, dict], tuple[DataFrame, DataFrame, list[RuleResult]]
]
RejectedWriter = Callable[[DataFrame, str, RunContext], list[str] | None]


def _completed_bronze_snapshot(period: str, ctx: RunContext) -> tuple[Path, str]:
    if not re.fullmatch(r"20\d{2}", period):
        raise ValueError(f"Invalid annual period {period!r}")

    period_root = bronze_manifest.source_path(ctx) / f"period={period}"
    candidates: list[tuple[Path, str, str]] = []
    for marker in sorted(period_root.glob("source_version=*/_snapshot.json")):
        records = marker.parent / "records"
        if not (records / "_SUCCESS").is_file():
            continue
        snapshot = json.loads(marker.read_text(encoding="utf-8"))
        if snapshot.get("schema", {}).get("changed"):
            continue
        sha256 = marker.parent.name.removeprefix("source_version=")
        if re.fullmatch(r"[0-9a-f]{64}", sha256):
            candidates.append((records, sha256, snapshot.get("fetched_at", "")))

    approved = ctx.config.get("silver", {}).get("approved_source_versions", {}).get(period)
    if approved:
        matches = [candidate for candidate in candidates if candidate[1] == approved]
        if len(matches) != 1:
            raise ValueError(f"Approved Bronze version for {period} is not completed: {approved}")
        return matches[0][:2]
    if not candidates:
        raise ValueError(f"No completed unchanged Bronze snapshot for {period}")
    # Several versions means the source file was corrected. Use the newest download.
    records, sha256, _ = max(candidates, key=lambda candidate: candidate[2])
    return records, sha256


def _silver_snapshot_path(period: str, source_sha256: str, ctx: RunContext) -> Path:
    if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
        raise ValueError("Invalid source SHA-256")
    return (
        Path(layer_path(ctx.config, "silver")).resolve()
        / TABLE_NAME
        / f"period={period}"
        / f"source_version={source_sha256}"
        / f"model_version={MODEL_VERSION}"
    )


def _resolve_rules_engine(rules: dict, supplied: RulesEngine | None) -> RulesEngine | None:
    if supplied is not None:
        return supplied
    if not rules:
        return None
    try:
        from silver.rules import apply_rules
    except ImportError as exc:
        raise RuntimeError("Quality rules are configured but silver.rules is unavailable") from exc
    return apply_rules


def _apply_quality(
    df: DataFrame,
    ctx: RunContext,
    rules_engine: RulesEngine | None,
) -> tuple[DataFrame, DataFrame, list[RuleResult], str]:
    quality = ctx.config.get("quality", {})
    rules = quality.get("rules", {}) or {}
    engine = _resolve_rules_engine(rules, rules_engine)
    if engine is None:
        if not quality.get("allow_unvalidated_silver", False):
            raise RuntimeError(
                "No Silver quality rules are active; set quality rules or explicitly allow "
                "an unvalidated development run"
            )
        return df, df.limit(0), [], "unvalidated-development"
    accepted, rejected, results = engine(df, rules)
    return accepted, rejected, results, "rules-engine"


def run(
    spark: SparkSession,
    ctx: RunContext,
    *,
    references: Mapping[str, DataFrame] | None = None,
    rules_engine: RulesEngine | None = None,
    rejected_writer: RejectedWriter | None = None,
) -> LayerResult:
    """Build immutable Silver snapshots for configured source periods."""
    periods = list(dict.fromkeys(str(value) for value in ctx.config["source"]["periods"]))
    result = LayerResult("silver", "skipped", message="No source periods configured")
    if not periods:
        return result

    allow_provisional = ctx.config.get("silver", {}).get("allow_provisional_model", False)
    if APPROVAL_STATUS != "approved" and not allow_provisional:
        return LayerResult(
            "silver",
            "failed",
            message=(
                f"Silver model {MODEL_VERSION} is {APPROVAL_STATUS}; joint A5 approval is required"
            ),
        )
    
    messages: list[str] = []
    report: dict[str, dict] = {}
    try:
        for period in periods:
            bronze_records, source_sha256 = _completed_bronze_snapshot(period, ctx)
            destination = _silver_snapshot_path(period, source_sha256, ctx)
            if (destination / "records" / "_SUCCESS").is_file() and (
                destination / "_snapshot.json"
            ).is_file():
                result.outputs.append(str(destination))
                messages.append(f"{period}: already built")
                continue
            if destination.exists():
                raise RuntimeError(f"Incomplete Silver destination already exists: {destination}")

            bronze_df = spark.read.parquet(bronze_records.resolve().as_posix())
            rows_in = bronze_df.count()
            dedup_strategy = ctx.config.get("silver", {}).get(
                "dedup_strategy", "optimized"
            )
            cleaned = clean(bronze_df, dedup_strategy=dedup_strategy).persist()
            rows_cleaned = cleaned.count()
            accepted, rejected, rule_results, quality_mode = _apply_quality(
                cleaned, ctx, rules_engine
            )
            rejected = rejected.persist()
            rows_rejected = rejected.count()
            result.rules.extend(rule_results)

            if rows_rejected:
                writer = rejected_writer or partial(write_rejected, table=TABLE_NAME)
                result.outputs.extend(writer(rejected, period, ctx) or [])

            # written before the reject-ratio check, so a failed run still has a report
            report[period] = build_period_report(
                period,
                bronze_df,
                cleaned,
                accepted,
                rows_in=rows_in,
                rows_cleaned=rows_cleaned,
                rows_rejected=rows_rejected,
                results=rule_results,
                ctx=ctx,
                table=TABLE_NAME,
            )
            write_report(ctx, report)

            reject_ratio = rows_rejected / rows_cleaned if rows_cleaned else 0.0
            if reject_ratio > ctx.config["quality"]["max_reject_ratio"]:
                raise RuntimeError(
                    f"Rejected ratio {reject_ratio:.6f} exceeds "
                    f"{ctx.config['quality']['max_reject_ratio']:.6f}"
                )

            enriched = enrich(accepted, references).persist()
            rows_out = enriched.count()
            table_root = Path(layer_path(ctx.config, "silver")).resolve() / TABLE_NAME
            table_root.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(prefix=f".staging-{ctx.run_id}-", dir=table_root) as temporary:
                staging = Path(temporary) / "snapshot"
                staging.mkdir()
                records = staging / "records"
                enriched.write.mode("errorifexists").parquet(records.resolve().as_posix())
                stored_rows = spark.read.parquet(records.resolve().as_posix()).count()
                if stored_rows != rows_out:
                    raise ValueError(
                        f"Silver row reconciliation failed: {rows_out} -> {stored_rows}"
                    )
                bronze_manifest.write_json(
                    staging / "_snapshot.json",
                    {
                        "period": period,
                        "table": TABLE_NAME,
                        "source_sha256": source_sha256,
                        "model_version": MODEL_VERSION,
                        "model_approval": APPROVAL_STATUS,
                        "dedup_strategy": dedup_strategy,
                        "quality_mode": quality_mode,
                        "reference_tables": sorted((references or {}).keys()),
                        "derived_columns": list(DERIVED_COLUMNS),
                        "rows_in": rows_in,
                        "rows_after_clean": rows_cleaned,
                        "rows_rejected": rows_rejected,
                        "rows_out": rows_out,
                        "run_id": ctx.run_id,
                        "output": str(destination),
                    },
                )
                destination.parent.mkdir(parents=True, exist_ok=True)
                staging.rename(destination)

            result.rows_in += rows_in
            result.rows_rejected += rows_rejected
            result.rows_out += rows_out
            result.outputs.append(str(destination))
            result.status = "success"
            messages.append(
                f"{period}: {rows_in} Bronze -> {rows_cleaned} clean -> "
                f"{rows_out} Silver ({quality_mode})"
            )
            enriched.unpersist()
            rejected.unpersist()
            cleaned.unpersist()
        result.message = " | ".join(messages)
    except Exception as exc:
        log.exception("run_id=%s layer=silver status=failed", ctx.run_id)
        result.status = "failed"
        result.message = str(exc)
    if report:
        result.outputs.append(str(report_path(ctx)))
    return result
