"""Pipeline entry point: python src/main.py --layers bronze."""

from __future__ import annotations

import argparse
import logging
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from common.config import load_config
from common.contracts import LayerResult, RunContext

log = logging.getLogger("pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the medallion pipeline")
    parser.add_argument("--layers", help="Comma-separated layers, default from config")
    parser.add_argument("--run-id", default=None, help="Run identifier (default: random UUID)")
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
    ctx = RunContext(run_id, config["env"], datetime.now(timezone.utc), config)

    log.info("Starting run %s in env=%s, layers=%s", run_id, config["env"], layers)
    spark = None
    try:
        for layer in layers:
            if layer == "bronze":
                from bronze.ingest import run
                from common.spark import get_spark

                spark = spark or get_spark(config)
                result = run(spark, ctx)
            elif layer == "silver":
                from common.spark import get_spark
                from silver.transform import run

                spark = spark or get_spark(config)
                result = run(spark, ctx)
            elif layer == "gold":
                from common.spark import get_spark
                from gold.build import run

                spark = spark or get_spark(config)
                result = run(spark, ctx)
            else:
                result = LayerResult(layer, "skipped", message="Unknown layer")
            log.info("run_id=%s result=%s", run_id, asdict(result))
            if result.status == "failed":
                raise SystemExit(1)
    finally:
        if spark is not None:
            spark.stop()
    log.info("Run %s finished", run_id)


if __name__ == "__main__":
    main()
