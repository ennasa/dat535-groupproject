"""Run one isolated full-data Silver performance measurement."""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from common.config import load_config
from common.contracts import RunContext
from common.spark import get_spark
from silver.transform import run


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True, help="Unique label, e.g. baseline-local")
    parser.add_argument(
        "--dedup-strategy", choices=("baseline", "optimized"), default="optimized"
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", args.label):
        parser.error("--label must contain lowercase letters, digits and hyphens")

    root = Path(__file__).resolve().parents[1]
    performance_root = root / "data" / "performance" / args.label
    if performance_root.exists():
        raise SystemExit(f"Refusing to overwrite existing measurement: {performance_root}")
    event_root = performance_root / "spark-events"
    event_root.mkdir(parents=True)

    config = load_config("dev")
    # Read the normal dev Bronze snapshot, but isolate all benchmark Silver output.
    config["storage"]["silver"] = f"../performance/{args.label}/silver"
    config["silver"]["allow_provisional_model"] = True
    config["silver"]["dedup_strategy"] = args.dedup_strategy
    config["quality"]["allow_unvalidated_silver"] = True
    config["spark"]["conf"].update(
        {
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": event_root.resolve().as_uri(),
            "spark.eventLog.compress": "false",
            "spark.ui.retainedStages": "500",
            "spark.ui.retainedJobs": "500",
        }
    )
    run_id = f"perf-{args.label}"
    ctx = RunContext(run_id, "dev", datetime.now(timezone.utc), config)

    started = datetime.now(timezone.utc)
    before = time.perf_counter()
    spark = get_spark(config)
    try:
        result = run(spark, ctx)
    finally:
        spark.stop()
    duration_seconds = time.perf_counter() - before
    finished = datetime.now(timezone.utc)

    record = {
        "label": args.label,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": finished.isoformat(),
        "wall_duration_seconds": round(duration_seconds, 3),
        "spark": {
            "master": config["spark"]["master"],
            "shuffle_partitions": config["spark"]["conf"][
                "spark.sql.shuffle.partitions"
            ],
            "event_log": str(event_root.relative_to(root)).replace("\\", "/"),
        },
        "dedup_strategy": args.dedup_strategy,
        "result": asdict(result),
    }
    (performance_root / "run-result.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(record, ensure_ascii=False, indent=2))
    if result.status != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
