"""Delete old data versions so the VM disk does not fill up.

The last three catch years can change every night, and every new version of a year file
is a new Bronze snapshot of about 1.4 GB. Without clean-up, the nightly run would fill
the disk in a couple of weeks.

Rules (per catch year):
- Bronze: keep the newest N usable versions (retention.bronze_versions_per_period).
  Versions with a changed schema are never deleted; they wait for someone to review them.
- Silver: keep only the Silver built from the newest Bronze version.
- Gold: keep the published version and the one before it.

Only finished folders are deleted, never staging folders from a running job.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from bronze.manifest import source_path
from common.config import layer_path
from common.contracts import RunContext


def _size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _remove(path: Path, removed: list[dict]) -> None:
    removed.append({"path": str(path), "bytes": _size(path)})
    shutil.rmtree(path)


def _bronze_versions(period_root: Path) -> list[tuple[str, Path, bool]]:
    """(fetched_at, folder, schema_changed) for finished versions, newest first."""
    versions = []
    for marker in period_root.glob("source_version=*/_snapshot.json"):
        if not (marker.parent / "records" / "_SUCCESS").is_file():
            continue
        snapshot = json.loads(marker.read_text(encoding="utf-8"))
        changed = bool(snapshot.get("schema", {}).get("changed"))
        versions.append((snapshot.get("fetched_at", ""), marker.parent, changed))
    return sorted(versions, key=lambda v: v[0], reverse=True)


def _clean_period(period: str, ctx: RunContext, keep: int, removed: list[dict]) -> None:
    usable = [v for v in _bronze_versions(source_path(ctx) / f"period={period}") if not v[2]]
    if not usable:
        return
    for _, folder, _ in usable[keep:]:
        _remove(folder, removed)

    newest = usable[0][1].name  # e.g. "source_version=<sha256>"
    silver_period = Path(layer_path(ctx.config, "silver")).resolve() / "catch_document_line"
    for folder in (silver_period / f"period={period}").glob("source_version=*"):
        if folder.name != newest and folder.is_dir():
            _remove(folder, removed)


def _clean_gold(ctx: RunContext, removed: list[dict]) -> None:
    from gold.build import GOLD_MART
    from gold.publish import published

    current = published(ctx)
    if current is None:  # nothing published yet: keep everything
        return
    keep = {current["candidate"], current.get("previous")}
    mart = Path(layer_path(ctx.config, "gold")).resolve() / GOLD_MART
    for marker in mart.glob("model_version=*/input_version=*/_candidate.json"):
        if str(marker.parent) not in keep:
            _remove(marker.parent, removed)


def apply_retention(ctx: RunContext) -> dict:
    """Delete old versions. Returns what was removed and how many bytes were freed."""
    settings = ctx.config.get("retention", {})
    if not settings.get("enabled", False):
        return {"enabled": False, "removed": [], "freed_bytes": 0}

    keep = int(settings.get("bronze_versions_per_period", 2))
    if keep < 1:
        raise ValueError("retention.bronze_versions_per_period must be at least 1")

    removed: list[dict] = []
    for period in dict.fromkeys(str(p) for p in ctx.config["source"]["periods"]):
        _clean_period(period, ctx, keep, removed)
    _clean_gold(ctx, removed)
    return {
        "enabled": True,
        "removed": removed,
        "freed_bytes": sum(r["bytes"] for r in removed),
    }
