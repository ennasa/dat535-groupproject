"""Ingest annual archives into immutable, versioned Bronze snapshots."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from bronze import manifest, metadata, source
from bronze import schema as source_schema
from common.contracts import LayerResult, RunContext

log = logging.getLogger(__name__)


def run(spark: SparkSession, ctx: RunContext) -> LayerResult:
    periods = ctx.config["source"]["periods"]
    result = LayerResult("bronze", "skipped", message="No source periods configured")
    files: list[dict] = []
    schema_messages: list[str] = []
    schema_changed = False
    try:
        root = manifest.source_path(ctx)
        root.mkdir(parents=True, exist_ok=True)
        for period in dict.fromkeys(str(value) for value in periods):
            record = {"period": period, "status": "failed"}
            files.append(record)
            # Staging is on the same filesystem as the final directory for atomic rename.
            with TemporaryDirectory(prefix=".staging-", dir=root) as temporary:
                staging = Path(temporary) / "snapshot"
                staging.mkdir()
                fetched = source.download(period, staging, ctx)
                record.update(
                    {
                        "name": fetched.archive.name,
                        "source_name": ctx.config["source"]["name"],
                        "url": fetched.url,
                        "fetched_at": fetched.fetched_at,
                        "bytes": fetched.archive.stat().st_size,
                        "sha256": fetched.sha256,
                    }
                )
                destination = manifest.snapshot_path(fetched.sha256, period, ctx)
                if manifest.is_ingested(fetched.sha256, ctx) and destination.is_dir():
                    snapshot = json.loads((destination / "_snapshot.json").read_text("utf-8"))
                    schema_report = source_schema.compare_schema(snapshot["columns"], ctx.config)
                    schema_changed = schema_changed or schema_report.changed
                    schema_messages.append(f"{period}: {schema_report.summary()}")
                    if schema_report.changed:
                        log.warning(
                            "run_id=%s period=%s %s",
                            ctx.run_id,
                            period,
                            schema_report.summary(),
                        )
                    record.update(snapshot, status="skipped")
                    record["schema"] = schema_report.as_dict()
                    # Record this fetch separately from the snapshot's original creation.
                    record["fetched_at"] = fetched.fetched_at
                    record["url"] = fetched.url
                    result.outputs.append(str(destination))
                    log.info(
                        "run_id=%s period=%s status=skipped sha256=%s",
                        ctx.run_id,
                        period,
                        fetched.sha256,
                    )
                    continue

                inspected = source.extract_and_inspect(fetched, staging)
                schema_report = source_schema.compare_schema(inspected.columns, ctx.config)
                schema_changed = schema_changed or schema_report.changed
                schema_messages.append(f"{period}: {schema_report.summary()}")
                if schema_report.changed:
                    log.warning(
                        "run_id=%s period=%s %s",
                        ctx.run_id,
                        period,
                        schema_report.summary(),
                    )
                schema = StructType(
                    [StructField(name, StringType(), True) for name in inspected.columns]
                )
                df = (
                    spark.read.schema(schema)
                    .options(
                        header=True,
                        sep=";",
                        encoding="UTF-8",
                        quote='"',
                        escape='"',
                        multiLine=True,
                        mode="FAILFAST",
                        nullValue="\x00",
                        emptyValue="",
                    )
                    .csv(inspected.path.resolve().as_posix())
                    .na.fill("")
                )
                enriched = metadata.add_metadata(
                    df, ctx.config["source"]["name"], inspected.path.name, period, ctx
                ).withColumn("_source_sha256", F.lit(fetched.sha256))
                records = staging / "records"
                enriched.write.mode("errorifexists").parquet(records.resolve().as_posix())
                rows_out = spark.read.parquet(records.resolve().as_posix()).count()
                if rows_out != inspected.rows:
                    raise ValueError(f"Row reconciliation failed: {inspected.rows} -> {rows_out}")
                snapshot = {
                    **record,
                    "status": "ingested",
                    "csv_sha256": inspected.sha256,
                    "csv_bytes": inspected.path.stat().st_size,
                    "rows": rows_out,
                    "columns": inspected.columns,
                    "schema": schema_report.as_dict(),
                    "output": str(destination),
                    "ingestion_run_id": ctx.run_id,
                }
                manifest.write_json(staging / "_snapshot.json", snapshot)
                destination.parent.mkdir(parents=True, exist_ok=True)
                staging.rename(destination)
                record.update(snapshot)
                result.rows_in += inspected.rows
                result.rows_out += rows_out
                result.outputs.append(str(destination))
                result.status = "success"
                log.info(
                    "run_id=%s period=%s status=ingested rows=%s sha256=%s",
                    ctx.run_id,
                    period,
                    rows_out,
                    fetched.sha256,
                )
        if schema_changed:
            result.status = "failed"
        if periods:
            action = "ingested" if result.rows_out else "already ingested"
            result.message = f"Configured snapshots {action}; " + " | ".join(schema_messages)
    except Exception as exc:
        log.exception("run_id=%s layer=bronze status=failed", ctx.run_id)
        result.status = "failed"
        result.message = str(exc)
    manifest.write_manifest(ctx, files, result)
    return result
