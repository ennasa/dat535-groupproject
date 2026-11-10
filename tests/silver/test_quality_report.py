"""Before/after quality report (#22)."""

import json
from datetime import date, datetime, timezone

import pytest
from test_quarantine import _silver_ctx

from common.config import load_config
from common.contracts import RuleResult, RunContext
from silver import transform
from silver.model import silver_schema, source_to_target
from silver.quality_report import build_period_report, column_profile, report_path
from silver.quarantine import write_rejected
from silver.rules import FAILED_RULES, QUALITY_FLAGS

TO_SOURCE = {target: source for source, target in source_to_target().items()}
DOC, DATE = TO_SOURCE["document_number"], TO_SOURCE["landing_date"]


@pytest.fixture
def frames(spark):
    # Bronze: everything is text, "" means missing
    bronze = spark.createDataFrame(
        [("d1", "04.03.2023"), ("", "not a date"), ("d3", "")], [DOC, DATE]
    )
    # after cleaning: blanks and unreadable values are null
    cleaned = spark.createDataFrame(
        [("d1", date(2023, 3, 4)), (None, None), ("d3", None)],
        "document_number string, landing_date date",
    )
    # Silver: the row without a document number was rejected; d3 is flagged
    accepted = spark.createDataFrame(
        [("d1", date(2023, 3, 4), []), ("d3", None, ["landing_date_missing"])],
        f"document_number string, landing_date date, {QUALITY_FLAGS} array<string>",
    )
    return bronze, cleaned, accepted


def ctx_for(tmp_path, run_id="r-test"):
    config = load_config("test")
    config["storage"]["root"] = str(tmp_path)
    config["quality"]["report_columns"] = ["document_number", "landing_date", "not_a_column"]
    return RunContext(run_id, "test", datetime.now(timezone.utc), config)


def test_column_profile_before_and_after(frames):
    bronze, cleaned, accepted = frames
    profile = {
        p["column"]: p
        for p in column_profile(bronze, cleaned, accepted, ["document_number", "landing_date"])
    }

    assert profile["document_number"] == {
        "column": "document_number",
        "source_column": DOC,
        "missing_in_bronze": 1,
        "invalid_values": 0,
        "missing_after_clean": 1,
        "missing_in_silver": 0,  # that row was rejected
    }
    # one blank date, one date that could not be read
    assert profile["landing_date"]["missing_in_bronze"] == 1
    assert profile["landing_date"]["invalid_values"] == 1
    assert profile["landing_date"]["missing_after_clean"] == 2
    assert profile["landing_date"]["missing_in_silver"] == 1


def test_unknown_report_columns_are_skipped(frames):
    bronze, cleaned, accepted = frames
    assert column_profile(bronze, cleaned, accepted, ["not_a_column"]) == []


def test_period_report_counts_and_quarantine_check(spark, tmp_path, frames):
    bronze, cleaned, accepted = frames
    ctx = ctx_for(tmp_path)
    rejected = spark.createDataFrame(
        [(None, ["key_not_null"])], f"document_number string, {FAILED_RULES} array<string>"
    )
    write_rejected(rejected, "2023", ctx, table="t")

    report = build_period_report(
        "2023",
        bronze,
        cleaned,
        accepted,
        rows_in=3,
        rows_cleaned=3,
        rows_rejected=1,
        results=[RuleResult("key_not_null", "reject", 1)],
        ctx=ctx,
        table="t",
    )

    assert report["rows"] == {
        "bronze": 3,
        "after_clean": 3,
        "duplicates_removed": 0,
        "rejected": 1,
        "accepted": 2,
        "accepted_with_flags": 1,
    }
    assert report["rules"] == [{"rule": "key_not_null", "action": "reject", "failed": 1}]
    assert [c["column"] for c in report["columns"]] == ["document_number", "landing_date"]
    assert report["quarantine"] == {"rows": 1, "matches_rejected": True}


def test_quarantine_mismatch_is_reported(tmp_path, frames):
    bronze, cleaned, accepted = frames
    report = build_period_report(
        "2023", bronze, cleaned, accepted, rows_in=3, rows_cleaned=3, rows_rejected=1,
        results=[], ctx=ctx_for(tmp_path), table="t",
    )
    # 1 row rejected, but nothing in quarantine
    assert report["quarantine"] == {"rows": 0, "matches_rejected": False}


def test_report_columns_in_config_exist_in_silver():
    columns = load_config("dev")["quality"]["report_columns"]
    assert set(columns) <= set(silver_schema().fieldNames())


# --- Silver end to end ---


def _report(ctx):
    return json.loads(report_path(ctx).read_text(encoding="utf-8"))


def test_silver_run_writes_report(spark, tmp_path, monkeypatch):
    ctx = _silver_ctx(spark, tmp_path, monkeypatch, max_reject_ratio=1.0)

    result = transform.run(spark, ctx)

    assert result.status == "success", result.message
    assert str(report_path(ctx)) in result.outputs
    period = _report(ctx)["periods"]["2023"]
    assert period["rows"]["rejected"] == 1
    assert period["rules"][0]["failed"] == 1
    assert period["quarantine"] == {"rows": 1, "matches_rejected": True}


def test_report_is_written_even_when_silver_fails(spark, tmp_path, monkeypatch):
    ctx = _silver_ctx(spark, tmp_path, monkeypatch, max_reject_ratio=0.05)

    result = transform.run(spark, ctx)

    assert result.status == "failed"
    assert _report(ctx)["periods"]["2023"]["rows"]["rejected"] == 1