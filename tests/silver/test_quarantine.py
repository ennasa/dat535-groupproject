"""Quarantine for rejected Silver rows (#20)."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from test_transform import context, create_snapshot  # helpers from the Silver tests

from common.config import load_config
from common.contracts import RunContext
from silver import transform
from silver.quarantine import quarantine_path, write_rejected
from silver.rules import FAILED_RULES


def make_ctx(tmp_path, run_id="q-test"):
    config = load_config("test")
    config["storage"]["root"] = str(tmp_path)
    return RunContext(run_id, "test", datetime.now(timezone.utc), config)


@pytest.fixture
def rejected(spark):
    return spark.createDataFrame(
        [
            ("doc-1", ["key_not_null"]),
            ("doc-2", ["key_unique", "round_weight_not_negative"]),  # broke two rules
            ("doc-3", ["key_unique"]),
        ],
        f"document_number string, {FAILED_RULES} array<string>",
    )


def test_rows_and_summary_are_written(spark, tmp_path, rejected):
    ctx = make_ctx(tmp_path)
    outputs = write_rejected(rejected, "2023", ctx, table="catch_document_line")

    path = quarantine_path("catch_document_line", "2023", ctx)
    assert outputs == [str(path)]
    assert path.parts[-3:] == ("catch_document_line", "period=2023", "run_id=q-test")

    stored = spark.read.parquet((path / "records").as_posix())
    assert stored.count() == 3
    assert {r["document_number"]: r[FAILED_RULES] for r in stored.collect()}["doc-2"] == [
        "key_unique",
        "round_weight_not_negative",
    ]
    assert {r["_quarantine_run_id"] for r in stored.collect()} == {"q-test"}

    summary = json.loads((path / "_summary.json").read_text())
    assert summary["rows"] == 3
    assert summary["rules"] == {
        "key_not_null": 1,
        "key_unique": 2,
        "round_weight_not_negative": 1,
    }


def test_same_run_cannot_overwrite_quarantine(tmp_path, rejected):
    ctx = make_ctx(tmp_path)
    write_rejected(rejected, "2023", ctx, table="t")
    with pytest.raises(Exception, match="already exists"):
        write_rejected(rejected, "2023", ctx, table="t")


def test_new_run_gets_its_own_folder(tmp_path, rejected):
    first = write_rejected(rejected, "2023", make_ctx(tmp_path, "run-1"), table="t")
    second = write_rejected(rejected, "2023", make_ctx(tmp_path, "run-2"), table="t")
    assert first != second


# --- Silver end to end: rejected rows go to quarantine without passing a writer ---

REJECT_ALL = {
    "document_must_be_ok": {
        "check": "regex",
        "column": "document_number",
        "pattern": "^ok$",
        "action": "reject",
    }
}


def _silver_ctx(spark, tmp_path, monkeypatch, max_reject_ratio):
    ctx = context(tmp_path)
    ctx.config["quality"]["rules"] = REJECT_ALL
    ctx.config["quality"]["max_reject_ratio"] = max_reject_ratio
    create_snapshot(spark, ctx)
    monkeypatch.setattr(transform, "clean", lambda df, **kwargs: df)
    return ctx


def test_silver_writes_rejected_rows_to_quarantine(spark, tmp_path, monkeypatch):
    ctx = _silver_ctx(spark, tmp_path, monkeypatch, max_reject_ratio=1.0)

    result = transform.run(spark, ctx)

    assert result.status == "success", result.message
    assert result.rows_rejected == 1
    path = quarantine_path(transform.TABLE_NAME, "2023", ctx)
    assert str(path) in result.outputs
    stored = spark.read.parquet((path / "records").as_posix()).collect()
    assert [r["document_number"] for r in stored] == ["doc-1"]
    assert stored[0][FAILED_RULES] == ["document_must_be_ok"]


def test_quarantine_is_kept_when_too_many_rows_are_rejected(spark, tmp_path, monkeypatch):
    ctx = _silver_ctx(spark, tmp_path, monkeypatch, max_reject_ratio=0.05)

    result = transform.run(spark, ctx)

    assert result.status == "failed"
    assert "exceeds" in result.message
    # Silver is not written, but the rejected rows are still saved for inspection
    assert Path(quarantine_path(transform.TABLE_NAME, "2023", ctx), "records").is_dir()
    assert not list(Path(tmp_path, "silver").rglob("_SUCCESS"))