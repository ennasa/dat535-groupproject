import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.config import load_config
from common.contracts import RunContext
from gold.build import build_tables, run


def silver_sample(spark):
    common = {
        "catch_year": 2023,
        "sales_org_code": "08",
        "document_type_code": "0",
        "landing_year": 2023,
        "species_group_code": "0101",
        "species_group_name": "Groundfish",
        "species_main_group_code": "01",
        "species_main_group_name": "Fish",
        "species_fao_code": "COD",
        "species_fao_name": "Atlantic cod",
    }
    rows = [
        {
            **common,
            "record_id": "r1",
            "document_number": "d1",
            "landing_date": date(2023, 1, 10),
            "landing_month": 1,
            "landing_year_month": "2023-01",
            "landing_country_code": "NOR",
            "landing_country_name": "Norway",
            "landing_county_code": "03",
            "landing_county_name": "County A",
            "species_code": "COD",
            "species_name": "Cod",
            "round_weight_kg": Decimal("100.000000"),
            "product_weight_kg": Decimal("80.000000"),
            "catch_value_nok": Decimal("1000.000000"),
        },
        {
            **common,
            "record_id": "r2",
            "document_number": "d2",
            "landing_date": date(2023, 1, 11),
            "landing_month": 1,
            "landing_year_month": "2023-01",
            "landing_country_code": "NOR",
            "landing_country_name": "Norway",
            "landing_county_code": "03",
            "landing_county_name": "County A",
            "species_code": "COD",
            "species_name": "Cod",
            "round_weight_kg": Decimal("50.000000"),
            "product_weight_kg": Decimal("40.000000"),
            "catch_value_nok": Decimal("500.000000"),
        },
        {
            **common,
            "record_id": "r3",
            "document_number": "d3",
            "landing_date": date(2023, 1, 12),
            "landing_month": 1,
            "landing_year_month": "2023-01",
            "landing_country_code": "NOR",
            "landing_country_name": "Norway",
            "landing_county_code": "03",
            "landing_county_name": "County A",
            "species_code": "HAD",
            "species_name": "Haddock",
            "round_weight_kg": Decimal("50.000000"),
            "product_weight_kg": Decimal("40.000000"),
            "catch_value_nok": Decimal("400.000000"),
            "species_fao_code": "HAD",
            "species_fao_name": "Haddock",
        },
        {
            **common,
            "record_id": "r4",
            "document_number": "d4",
            "landing_date": date(2023, 2, 1),
            "landing_month": 2,
            "landing_year_month": "2023-02",
            "landing_country_code": "DNK",
            "landing_country_name": "Denmark",
            "landing_county_code": None,
            "landing_county_name": None,
            "species_code": "COD",
            "species_name": "Cod",
            "round_weight_kg": None,
            "product_weight_kg": Decimal("10.000000"),
            "catch_value_nok": None,
        },
    ]
    return spark.createDataFrame(rows)


def test_build_tables_has_declared_grain_measures_and_shares(spark):
    tables = build_tables(silver_sample(spark))
    fact = tables.fact_monthly_species_region

    assert tables.dim_month.count() == 2
    assert tables.dim_landing_region.count() == 2
    assert tables.dim_species.count() == 2
    assert fact.count() == 3
    assert fact.agg({"line_count": "sum"}).first()[0] == 4

    result = (
        fact.join(tables.dim_month, "month_id")
        .join(tables.dim_landing_region, "region_id")
        .join(tables.dim_species, "species_id")
    )
    cod = result.filter(
        (result.month_id == "2023-01") & (result.species_code == "COD")
    ).first()
    haddock = result.filter(
        (result.month_id == "2023-01") & (result.species_code == "HAD")
    ).first()
    foreign = result.filter(result.month_id == "2023-02").first()

    assert cod.line_count == cod.document_count == 2
    assert cod.total_round_weight_kg == Decimal("150.000000")
    assert float(cod.species_share_of_region_month) == pytest.approx(0.75)
    assert float(haddock.species_share_of_region_month) == pytest.approx(0.25)
    assert foreign.landing_county_code == "UNKNOWN"
    assert foreign.missing_round_weight_rows == 1
    assert foreign.species_share_of_region_month is None


def test_gold_round_weight_reconciles_with_silver(spark):
    silver = silver_sample(spark)
    fact = build_tables(silver).fact_monthly_species_region

    silver_total = silver.agg({"round_weight_kg": "sum"}).first()[0]
    gold_total = fact.agg({"total_round_weight_kg": "sum"}).first()[0]

    assert gold_total == silver_total == Decimal("200.000000")


def test_build_rejects_incomplete_silver_input(spark):
    with pytest.raises(ValueError, match="Silver input is missing Gold fields"):
        build_tables(spark.createDataFrame([(1,)], ["record_id"]))


def create_silver_candidate(spark, ctx, *, trusted=True):
    root = Path(ctx.config["storage"]["root"])
    snapshot = (
        root
        / "silver/catch_document_line/period=2023"
        / f"source_version={'a' * 64}"
        / "model_version=a5-proposal-1"
    )
    snapshot.mkdir(parents=True)
    silver_sample(spark).write.parquet(str(snapshot / "records"))
    metadata = {
        "period": "2023",
        "source_sha256": "a" * 64,
        "model_version": "a5-proposal-1",
        "quality_mode": "rules-engine" if trusted else "unvalidated-development",
        "model_approval": "approved" if trusted else "awaiting-person-b-review",
    }
    (snapshot / "_snapshot.json").write_text(json.dumps(metadata), encoding="utf-8")


def gold_context(tmp_path):
    config = load_config("test")
    config["storage"]["root"] = str(tmp_path)
    config["source"]["periods"] = ["2023"]
    return RunContext("gold-test", "test", datetime.now(timezone.utc), config)


def test_gold_run_writes_reconciled_candidate_and_skips_rerun(spark, tmp_path):
    ctx = gold_context(tmp_path)
    create_silver_candidate(spark, ctx)

    first = run(spark, ctx)
    second = run(spark, ctx)

    assert first.status == "success"
    assert first.rows_in == 4
    assert first.rows_out == 3
    candidate = Path(first.outputs[0]).parent
    marker = json.loads((candidate / "_candidate.json").read_text(encoding="utf-8"))
    assert marker["fact_line_count"] == 4
    assert marker["unvalidated_preview"] is False
    assert second.status == "skipped"


def test_gold_run_blocks_unvalidated_silver(spark, tmp_path):
    ctx = gold_context(tmp_path)
    create_silver_candidate(spark, ctx, trusted=False)

    result = run(spark, ctx)

    assert result.status == "failed"
    assert "not approved and validated" in result.message
