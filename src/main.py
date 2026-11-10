"""Pipeline entry point: python src/main.py --layers bronze."""

from __future__ import annotations

import argparse
import logging
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from bronze.manifest import record_run_layers
from common.config import load_config
from common.contracts import LayerResult, RunContext

log = logging.getLogger("pipeline")

def _runner(layer: str):
    """Import a layer's run() only when it is needed."""
    if layer == "bronze":
        from bronze.ingest import run
    elif layer == "silver":
        from silver.transform import run
    else:
        from gold.publish import run
    return run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the medallion pipeline")
    parser.add_argument("--layers", help="Comma-separated layers, default from config")
    parser.add_argument("--run-id", default=None, help="Run identifier (default: random UUID)")
    parser.add_argument(
        "--force", action="store_true", help="Re-ingest source files even if already in Bronze"
    )
    args = parser.parse_args()

    config = load_config()
    logging.basicConfig(
        level=config["logging"]["level"], format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    run_id = args.run_id or str(uuid.uuid4())
    layers = args.layers.split(",") if args.layers else config["run"]["layers"]
    if not layers or any(layer not in {"bronze", "silver", "gold"} for layer in layers):
        parser.error("--layers must be a comma-separated subset of bronze,silver,gold")
    ctx = RunContext(run_id, config["env"], datetime.now(timezone.utc), config, force=args.force)

    log.info("Starting run %s in env=%s, layers=%s", run_id, config["env"], layers)
    spark = None
    results: list[LayerResult] = []
    retention: dict | None = None
    try:
        for layer in layers:
            if any(r.status == "failed" for r in results):
                # An earlier layer failed: don't run this one, but record that it was skipped.
                result = LayerResult(layer, "skipped", message="upstream layer failed")
            else:
                from common.spark import get_spark

                spark = spark or get_spark(config)
                result = _runner(layer)(spark, ctx)
            log.info("run_id=%s result=%s", run_id, asdict(result))
            results.append(result)

        # Clean up old versions only after a full, successful run, so nothing is deleted
        # that might be needed to recover from a failure.
        full_run = set(layers) == {"bronze", "silver", "gold"}
        if full_run and not any(r.status == "failed" for r in results):
            from common.retention import apply_retention

            retention = apply_retention(ctx)
            log.info(
                "run_id=%s retention removed %s folders, freed %.1f MB",
                run_id,
                len(retention["removed"]),
                retention["freed_bytes"] / 1e6,
            )
    finally:
        if spark is not None:
            spark.stop()
        if results:
            extra = {"retention": retention} if retention is not None else None
            log.info("run_id=%s manifest=%s", run_id, record_run_layers(ctx, results, extra))

    failed = [r.layer for r in results if r.status == "failed"]
    if failed:
        log.error("Run %s failed in %s", run_id, failed)
        raise SystemExit(1)
    log.info("Run %s finished", run_id)


if __name__ == "__main__":
    main()
