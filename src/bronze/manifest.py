"""Atomic per-run manifests and checksum-based snapshot discovery."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict
from pathlib import Path

from common.config import layer_path
from common.contracts import LayerResult, RunContext


def source_path(ctx: RunContext) -> Path:
    name = ctx.config["source"]["name"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", name):
        raise ValueError("source.name must be a simple directory name")
    return Path(layer_path(ctx.config, "bronze")).resolve() / name


def snapshot_path(sha256: str, period: str, ctx: RunContext) -> Path:
    if not re.fullmatch(r"[0-9a-f]{64}", sha256) or not re.fullmatch(r"20\d{2}", period):
        raise ValueError("Invalid snapshot checksum or annual period")
    return source_path(ctx) / f"period={period}" / f"source_version={sha256}"


def is_ingested(sha256: str, ctx: RunContext) -> bool:
    """A completed snapshot is authoritative even if its run manifest write failed."""
    if not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise ValueError("Invalid SHA-256")
    for marker in source_path(ctx).glob(f"period=*/source_version={sha256}/_snapshot.json"):
        record = json.loads(marker.read_text(encoding="utf-8"))
        if record["sha256"] == sha256 and (marker.parent / "records" / "_SUCCESS").is_file():
            return True
    return False


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_manifest(ctx: RunContext, files: list[dict], result: LayerResult) -> Path:
    path = Path(layer_path(ctx.config, "manifests")) / f"{ctx.run_id}.json"
    write_json(
        path,
        {
            "run_id": ctx.run_id,
            "env": ctx.env,
            "started_at": ctx.started_at.isoformat(),
            "files": files,
            "layers": [asdict(result)],
        },
    )
    return path


def record_run_layers(ctx: RunContext, results: list[LayerResult]) -> Path:
    """Store every layer's result in this run's manifest.

    Bronze writes the manifest first (with the files it saw). This adds Silver and Gold,
    keeping Bronze's file list, so one file describes the whole run.
    """
    path = Path(layer_path(ctx.config, "manifests")) / f"{ctx.run_id}.json"
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {
        "run_id": ctx.run_id,
        "env": ctx.env,
        "started_at": ctx.started_at.isoformat(),
        "files": [],
    }
    data["force"] = ctx.force
    data["layers"] = [asdict(result) for result in results]
    data["status"] = "failed" if any(r.status == "failed" for r in results) else "success"
    write_json(path, data)
    return path