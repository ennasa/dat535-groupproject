import json
from datetime import date, datetime, timezone

from common.config import load_config
from common.contracts import RunContext
from silver import transform

SOURCE_SHA256 = "a" * 64


def context(tmp_path, *, provisional=True, unvalidated=True):
    config = load_config("test")
    config["storage"]["root"] = str(tmp_path)
    config["source"]["periods"] = ["2023"]
    config["silver"]["allow_provisional_model"] = provisional
    config["quality"]["allow_unvalidated_silver"] = unvalidated
    return RunContext("a7-test", "test", datetime.now(timezone.utc), config)


def create_snapshot(spark, ctx):
    records = (
        tmp_path_from(ctx)
        / "bronze/fiskeridir_catch/period=2023"
        / f"source_version={SOURCE_SHA256}"
        / "records"
    )
    records.parent.mkdir(parents=True)
    spark.createDataFrame(
        [
            (
                2023,
                "08",
                "0",
                "doc-1",
                1,
                date(2023, 3, 4),
                "03",
                date(2023, 3, 1),
                "NOR",
                "COD",
                "061101",
            )
        ],
        [
            "catch_year",
            "sales_org_code",
            "document_type_code",
            "document_number",
            "line_number",
            "landing_date",
            "landing_month_code",
            "last_catch_date",
            "landing_country_code",
            "species_fao_code",
            "species_fdir_code",
        ],
    ).write.parquet(str(records))
    (records.parent / "_snapshot.json").write_text(
        json.dumps({"schema": {"changed": False}}), encoding="utf-8"
    )


def tmp_path_from(ctx):
    from pathlib import Path

    return Path(ctx.config["storage"]["root"])


def test_transform_writes_reconciled_versioned_snapshot(spark, tmp_path, monkeypatch):
    ctx = context(tmp_path)
    create_snapshot(spark, ctx)
    monkeypatch.setattr(transform, "clean", lambda df, **kwargs: df)

    first = transform.run(spark, ctx)
    second = transform.run(spark, ctx)

    assert first.status == "success"
    assert first.rows_in == first.rows_out == 1
    assert first.rows_rejected == 0
    assert "unvalidated-development" in first.message
    output = tmp_path_from(ctx) / first.outputs[0]
    assert (output / "records/_SUCCESS").is_file()
    snapshot = json.loads((output / "_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot["source_sha256"] == SOURCE_SHA256
    assert snapshot["rows_in"] == snapshot["rows_out"] == 1
    stored = spark.read.parquet(str(output / "records"))
    assert "record_id" in stored.columns
    assert second.status == "skipped"
    assert second.rows_out == 0


def test_transform_blocks_unapproved_model(spark, tmp_path):
    result = transform.run(spark, context(tmp_path, provisional=False))

    assert result.status == "failed"
    assert "joint A5 approval" in result.message


def test_transform_blocks_publication_without_quality_rules(spark, tmp_path, monkeypatch):
    ctx = context(tmp_path, unvalidated=False)
    create_snapshot(spark, ctx)
    monkeypatch.setattr(transform, "clean", lambda df, **kwargs: df)

    result = transform.run(spark, ctx)

    assert result.status == "failed"
    assert "No Silver quality rules are active" in result.message
    assert not (tmp_path_from(ctx) / "silver").exists()
