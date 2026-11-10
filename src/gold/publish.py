"""Build, validate and publish Gold (#30).

gold/build.py makes a "candidate". This module checks it with validate.py and only then
publishes it by updating gold/<mart>/_published.json, which says which candidate is the
official one. If a check fails, the pointer is not changed, so consumers keep the
previous good version.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from bronze.manifest import write_json
from common.config import layer_path
from common.contracts import LayerResult, RunContext
from gold import build
from gold.validate import reconcile


def pointer_path(ctx: RunContext) -> Path:
    return Path(layer_path(ctx.config, "gold")).resolve() / build.GOLD_MART / "_published.json"


def published(ctx: RunContext) -> dict | None:
    """The currently published Gold version, or None if nothing is published yet."""
    path = pointer_path(ctx)
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def published_tables(ctx: RunContext) -> dict[str, str]:
    """Paths of the published Gold tables, for notebooks and dashboards."""
    current = published(ctx)
    if current is None:
        raise FileNotFoundError("No published Gold yet")
    return {name: str(Path(current["candidate"]) / name) for name in build.TABLE_NAMES}


def publish_candidate(candidate: Path, problems: list[str], ctx: RunContext) -> bool:
    """Record the validation result; move the pointer only if there were no problems."""
    passed = not problems
    write_json(
        candidate / f"_validation-{ctx.run_id}.json",
        {
            "run_id": ctx.run_id,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "passed": passed,
            "problems": problems,
        },
    )
    if not passed:
        return False

    previous = published(ctx)
    write_json(
        pointer_path(ctx),
        {
            "candidate": str(candidate),
            "run_id": ctx.run_id,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "previous": previous["candidate"] if previous else None,
        },
    )
    return True


def run(spark: SparkSession, ctx: RunContext) -> LayerResult:
    result = build.run(spark, ctx)
    if result.status == "failed" or not ctx.config["source"]["periods"]:
        return result

    try:
        inputs = build._silver_inputs(ctx)
        candidate = build._candidate_path(inputs, ctx)
        current = published(ctx)
        if current and current["candidate"] == str(candidate):
            result.message += "; already published"
            return result

        # check the stored files, not the DataFrames in memory
        silver = spark.read.parquet(*[path.resolve().as_posix() for path, _ in inputs])
        tables = {
            name: spark.read.parquet((candidate / name).as_posix()) for name in build.TABLE_NAMES
        }
        problems = reconcile(silver, tables)

        if publish_candidate(candidate, problems, ctx):
            result.status = "success"
            result.message += "; validated and published"
            result.outputs.append(str(pointer_path(ctx)))
        else:
            result.status = "failed"
            result.message += "; not published: " + "; ".join(problems)
    except Exception as exc:
        result.status = "failed"
        result.message = f"Gold publish failed: {exc}"
    return result