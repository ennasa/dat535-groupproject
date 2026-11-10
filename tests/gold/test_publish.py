"""Gold validation (#29) and publishing (#30)."""

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from test_build import create_silver_candidate, gold_context, silver_sample

from gold import publish
from gold.build import build_tables
from gold.validate import FACT, reconcile


@pytest.fixture
def silver(spark):
    return silver_sample(spark)


@pytest.fixture
def tables(silver):
    return build_tables(silver).as_dict()


def test_correct_gold_has_no_problems(silver, tables):
    assert reconcile(silver, tables) == []


def test_wrong_total_is_found(silver, tables):
    tables[FACT] = tables[FACT].withColumn(
        "total_round_weight_kg", F.col("total_round_weight_kg") + 1
    )
    problems = reconcile(silver, tables)
    assert any(p.startswith("round_weight_kg:") for p in problems)


def test_duplicate_fact_row_is_found(silver, tables):
    tables[FACT] = tables[FACT].unionByName(tables[FACT].limit(1))
    problems = reconcile(silver, tables)
    assert any("duplicate month/region/species" in p for p in problems)
    assert any(p.startswith("lines:") for p in problems)  # counted twice -> totals differ too


def test_fact_pointing_to_missing_dimension_is_found(silver, tables):
    tables["dim_species"] = tables["dim_species"].limit(0)
    problems = reconcile(silver, tables)
    assert any("unknown species_id" in p for p in problems)


def test_wrong_share_is_found(silver, tables):
    tables[FACT] = tables[FACT].withColumn(
        "species_share_of_region_month", F.col("species_share_of_region_month") / 2
    )
    problems = reconcile(silver, tables)
    assert any("don't match their totals" in p for p in problems)


def test_wrong_region_total_is_found(silver, tables):
    tables[FACT] = tables[FACT].withColumn(
        "region_month_round_weight_kg", F.col("region_month_round_weight_kg") + 1
    )
    problems = reconcile(silver, tables)
    assert any("species totals don't add up" in p for p in problems)


def test_rounded_shares_with_many_species_are_ok(spark):
    # Real Silver uses decimal(24,6), so each share is rounded to 6 decimals.
    # Six species of 1 kg: each share is 0.166667, and together they add up to 1.000002.
    # This failed the first version of the check on the real 2023 data.
    template = silver_sample(spark).limit(1)
    six_species = None
    for i in range(6):
        row = template.withColumn("record_id", F.lit(f"s{i}")).withColumn(
            "species_code", F.lit(f"S{i}")
        )
        six_species = row if six_species is None else six_species.unionByName(row)
    silver = six_species.withColumn("round_weight_kg", F.lit(1).cast("decimal(24,6)"))
    for column in ["product_weight_kg", "catch_value_nok"]:
        silver = silver.withColumn(column, F.col(column).cast("decimal(24,6)"))

    assert reconcile(silver, build_tables(silver).as_dict()) == []


def test_missing_table_is_found(silver, tables):
    del tables["dim_month"]
    assert reconcile(silver, tables) == ["missing Gold tables: ['dim_month']"]


# --- publishing ---


def test_run_validates_and_publishes(spark, tmp_path):
    ctx = gold_context(tmp_path)
    create_silver_candidate(spark, ctx)

    result = publish.run(spark, ctx)

    assert result.status == "success", result.message
    assert "validated and published" in result.message
    pointer = publish.published(ctx)
    candidate = Path(pointer["candidate"])
    assert (candidate / "_candidate.json").is_file()
    assert pointer["previous"] is None
    validation = json.loads((candidate / "_validation-gold-test.json").read_text())
    assert validation == {**validation, "passed": True, "problems": []}

    tables = publish.published_tables(ctx)
    assert spark.read.parquet(tables[FACT]).count() == 3


def test_rerun_does_not_publish_twice(spark, tmp_path):
    ctx = gold_context(tmp_path)
    create_silver_candidate(spark, ctx)
    publish.run(spark, ctx)

    again = publish.run(spark, ctx)

    assert "already published" in again.message


def test_failed_validation_is_not_published(spark, tmp_path, monkeypatch):
    ctx = gold_context(tmp_path)
    create_silver_candidate(spark, ctx)
    monkeypatch.setattr(publish, "reconcile", lambda silver, tables: ["totals differ"])

    result = publish.run(spark, ctx)

    assert result.status == "failed"
    assert "not published: totals differ" in result.message
    assert publish.published(ctx) is None


def test_failed_validation_keeps_previous_version(tmp_path):
    ctx = gold_context(tmp_path)
    good, bad = tmp_path / "candidate-good", tmp_path / "candidate-bad"
    good.mkdir()
    bad.mkdir()

    assert publish.publish_candidate(good, [], ctx) is True
    assert publish.publish_candidate(bad, ["totals differ"], ctx) is False

    assert publish.published(ctx)["candidate"] == str(good)  # pointer did not move
    record = json.loads((bad / "_validation-gold-test.json").read_text())
    assert record["passed"] is False
    assert record["problems"] == ["totals differ"]


def test_published_tables_needs_a_published_version(tmp_path):
    with pytest.raises(FileNotFoundError):
        publish.published_tables(gold_context(tmp_path))