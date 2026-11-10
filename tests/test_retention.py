"""Newest-version selection and retention (Phase 4)."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from common.config import load_config
from common.contracts import RunContext
from common.retention import apply_retention
from gold import publish
from gold.build import _silver_inputs
from silver.transform import _completed_bronze_snapshot

OLD, NEW, NEWER = "a" * 64, "b" * 64, "c" * 64


def make_ctx(tmp_path, keep=1, enabled=True):
    config = load_config("test")
    config["storage"]["root"] = str(tmp_path)
    config["source"]["periods"] = ["2023"]
    config["retention"] = {"enabled": enabled, "bronze_versions_per_period": keep}
    return RunContext("ret-test", "test", datetime.now(timezone.utc), config)


def bronze_version(tmp_path, sha, fetched_at, schema_changed=False):
    folder = tmp_path / "bronze/fiskeridir_catch/period=2023" / f"source_version={sha}"
    (folder / "records").mkdir(parents=True)
    (folder / "records/_SUCCESS").write_text("")
    (folder / "records/part-0.parquet").write_bytes(b"x" * 1000)
    (folder / "_snapshot.json").write_text(
        json.dumps({"fetched_at": fetched_at, "schema": {"changed": schema_changed}})
    )
    return folder


def silver_version(tmp_path, sha):
    folder = (
        tmp_path / "silver/catch_document_line/period=2023"
        / f"source_version={sha}" / "model_version=a5-proposal-1"
    )
    (folder / "records").mkdir(parents=True)
    (folder / "records/_SUCCESS").write_text("")
    (folder / "_snapshot.json").write_text(
        json.dumps({
            "period": "2023", "source_sha256": sha, "model_version": "a5-proposal-1",
            "quality_mode": "rules-engine", "model_approval": "approved",
        })
    )
    return folder.parent  # the source_version=... folder


def gold_candidate(tmp_path, name):
    folder = tmp_path / "gold/fisheries_seasonality/model_version=a9-v1" / f"input_version={name}"
    folder.mkdir(parents=True)
    (folder / "_candidate.json").write_text("{}")
    return folder.resolve()


# --- Silver and Gold use the newest Bronze version ---


def test_silver_uses_newest_bronze_version(tmp_path):
    ctx = make_ctx(tmp_path)
    bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00")
    bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00")

    _, sha = _completed_bronze_snapshot("2023", ctx)

    assert sha == NEW


def test_approved_version_still_wins(tmp_path):
    ctx = make_ctx(tmp_path)
    ctx.config["silver"]["approved_source_versions"] = {"2023": OLD}
    bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00")
    bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00")

    _, sha = _completed_bronze_snapshot("2023", ctx)

    assert sha == OLD


def test_changed_schema_version_is_not_used(tmp_path):
    ctx = make_ctx(tmp_path)
    bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00")
    bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00", schema_changed=True)

    _, sha = _completed_bronze_snapshot("2023", ctx)

    assert sha == OLD


def test_no_bronze_version_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="No completed unchanged Bronze snapshot"):
        _completed_bronze_snapshot("2023", make_ctx(tmp_path))


def test_gold_picks_silver_from_newest_bronze(tmp_path):
    ctx = make_ctx(tmp_path)
    bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00")
    bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00")
    silver_version(tmp_path, OLD)
    silver_version(tmp_path, NEW)

    [(_, metadata)] = _silver_inputs(ctx)

    assert metadata["source_sha256"] == NEW


# --- retention ---


def test_bronze_keeps_newest_versions(tmp_path):
    old = bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00")
    new = bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00")
    newer = bronze_version(tmp_path, NEWER, "2026-11-03T02:00:00+00:00")

    result = apply_retention(make_ctx(tmp_path, keep=2))

    assert not old.exists()
    assert new.exists() and newer.exists()
    assert [r["path"] for r in result["removed"]] == [str(old)]
    assert result["freed_bytes"] > 0


def test_changed_schema_versions_are_never_deleted(tmp_path):
    changed = bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00", schema_changed=True)
    bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00")

    apply_retention(make_ctx(tmp_path, keep=1))

    assert changed.exists()  # waits for someone to review it


def test_silver_keeps_only_newest_version(tmp_path):
    bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00")
    bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00")
    old_silver = silver_version(tmp_path, OLD)
    new_silver = silver_version(tmp_path, NEW)

    apply_retention(make_ctx(tmp_path, keep=2))

    assert not old_silver.exists()
    assert new_silver.exists()


def test_gold_keeps_published_and_previous(tmp_path):
    ctx = make_ctx(tmp_path)
    oldest, previous, current = (gold_candidate(tmp_path, n) for n in ["1", "2", "3"])
    publish.publish_candidate(previous, [], ctx)
    publish.publish_candidate(current, [], ctx)

    apply_retention(ctx)

    assert not oldest.exists()
    assert previous.exists() and current.exists()


def test_gold_is_untouched_when_nothing_is_published(tmp_path):
    candidate = gold_candidate(tmp_path, "1")
    apply_retention(make_ctx(tmp_path))
    assert candidate.exists()


def test_disabled_retention_deletes_nothing(tmp_path):
    old = bronze_version(tmp_path, OLD, "2026-11-01T02:00:00+00:00")
    bronze_version(tmp_path, NEW, "2026-11-02T02:00:00+00:00")

    result = apply_retention(make_ctx(tmp_path, keep=1, enabled=False))

    assert old.exists()
    assert result == {"enabled": False, "removed": [], "freed_bytes": 0}


def test_keep_must_be_at_least_one(tmp_path):
    with pytest.raises(ValueError, match="at least 1"):
        apply_retention(make_ctx(tmp_path, keep=0))


def test_empty_storage_is_fine(tmp_path):
    assert apply_retention(make_ctx(tmp_path))["removed"] == []


def test_prod_config_has_periods_and_retention():
    prod = load_config("prod")
    assert prod["source"]["periods"], "prod must process at least one catch year"
    assert prod["retention"]["enabled"] is True
    assert Path(prod["storage"]["root"]).is_absolute()