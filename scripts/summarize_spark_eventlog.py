"""Summarise Spark task metrics from an uncompressed event log."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def nested(value: dict, *keys: str) -> int:
    current = value
    for key in keys:
        current = current.get(key, {})
    return int(current or 0) if not isinstance(current, dict) else 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("event_log", type=Path)
    parser.add_argument("--run-result", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    logs = [path for path in args.event_log.iterdir() if path.is_file()]
    if len(logs) != 1:
        raise SystemExit(f"Expected one event-log file in {args.event_log}, found {len(logs)}")

    stage_names: dict[int, str] = {}
    tasks: dict[int, list[dict]] = defaultdict(list)
    jobs: list[dict] = []
    app_start = None
    app_end = None
    with logs[0].open(encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            event_type = event.get("Event")
            if event_type == "SparkListenerApplicationStart":
                app_start = event.get("Timestamp")
            elif event_type == "SparkListenerApplicationEnd":
                app_end = event.get("Timestamp")
            elif event_type == "SparkListenerStageCompleted":
                info = event["Stage Info"]
                stage_names[int(info["Stage ID"])] = info["Stage Name"]
            elif event_type == "SparkListenerJobEnd":
                jobs.append(
                    {
                        "job_id": event["Job ID"],
                        "completion_time_ms": event["Completion Time"],
                        "result": event["Job Result"].get("Result"),
                    }
                )
            elif event_type == "SparkListenerTaskEnd":
                metrics = event.get("Task Metrics", {})
                info = event.get("Task Info", {})
                tasks[int(event["Stage ID"])].append(
                    {
                        "duration_ms": int(info.get("Finish Time", 0))
                        - int(info.get("Launch Time", 0)),
                        "executor_run_ms": nested(metrics, "Executor Run Time"),
                        "executor_cpu_ns": nested(metrics, "Executor CPU Time"),
                        "gc_ms": nested(metrics, "JVM GC Time"),
                        "input_bytes": nested(metrics, "Input Metrics", "Bytes Read"),
                        "output_bytes": nested(metrics, "Output Metrics", "Bytes Written"),
                        "shuffle_read_bytes": nested(
                            metrics, "Shuffle Read Metrics", "Remote Bytes Read"
                        )
                        + nested(metrics, "Shuffle Read Metrics", "Local Bytes Read"),
                        "shuffle_write_bytes": nested(
                            metrics, "Shuffle Write Metrics", "Shuffle Bytes Written"
                        ),
                        "memory_spill_bytes": nested(metrics, "Memory Bytes Spilled"),
                        "disk_spill_bytes": nested(metrics, "Disk Bytes Spilled"),
                    }
                )

    stages = []
    for stage_id, stage_tasks in sorted(tasks.items()):
        durations = [task["duration_ms"] for task in stage_tasks]
        median = statistics.median(durations) if durations else 0
        maximum = max(durations, default=0)
        stages.append(
            {
                "stage_id": stage_id,
                "name": stage_names.get(stage_id, "unknown"),
                "tasks": len(stage_tasks),
                "task_duration_ms": {
                    "median": round(median, 3),
                    "maximum": maximum,
                    "max_to_median_ratio": round(maximum / median, 3) if median else None,
                },
                **{
                    metric: sum(task[metric] for task in stage_tasks)
                    for metric in (
                        "executor_run_ms",
                        "executor_cpu_ns",
                        "gc_ms",
                        "input_bytes",
                        "output_bytes",
                        "shuffle_read_bytes",
                        "shuffle_write_bytes",
                        "memory_spill_bytes",
                        "disk_spill_bytes",
                    )
                },
            }
        )

    totals = {
        metric: sum(stage[metric] for stage in stages)
        for metric in (
            "executor_run_ms",
            "executor_cpu_ns",
            "gc_ms",
            "input_bytes",
            "output_bytes",
            "shuffle_read_bytes",
            "shuffle_write_bytes",
            "memory_spill_bytes",
            "disk_spill_bytes",
        )
    }
    summary = {
        "application_duration_seconds": (
            round((app_end - app_start) / 1000, 3)
            if app_start is not None and app_end is not None
            else None
        ),
        "jobs": len(jobs),
        "stages": len(stages),
        "tasks": sum(stage["tasks"] for stage in stages),
        "totals": totals,
        "highest_shuffle_write_stages": sorted(
            stages, key=lambda stage: stage["shuffle_write_bytes"], reverse=True
        )[:5],
        "highest_skew_ratio_stages": sorted(
            (stage for stage in stages if stage["task_duration_ms"]["max_to_median_ratio"]),
            key=lambda stage: stage["task_duration_ms"]["max_to_median_ratio"],
            reverse=True,
        )[:5],
    }
    if args.run_result:
        summary["run"] = json.loads(args.run_result.read_text(encoding="utf-8"))

    rendered = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
