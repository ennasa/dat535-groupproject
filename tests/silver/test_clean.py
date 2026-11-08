import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from silver.clean import clean
from silver.model import SILVER_FIELDS, silver_schema


def bronze_row(**overrides):
    """Build a complete synthetic Bronze row from the model contract."""
    row = {}
    for field in SILVER_FIELDS:
        if field.source == "_ingested_at":
            value = datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc)
        elif field.name == "document_version_at_utc":
            value = "02.01.2023 10.15.30,123456789"
        elif isinstance(field.data_type, DateType):
            value = "02.01.2023"
        elif isinstance(field.data_type, BooleanType):
            value = "0"
        elif isinstance(field.data_type, (IntegerType, LongType)):
            value = "1"
        elif isinstance(field.data_type, DecimalType):
            value = "1,5"
        elif isinstance(field.data_type, StringType):
            value = "  value  "
        elif isinstance(field.data_type, TimestampType):
            raise AssertionError(f"No synthetic rule for {field.name}")
        else:
            raise AssertionError(f"No synthetic rule for {field.data_type}")
        row[field.source] = value

    row.update(
        {
            "Fangstår": "2023",
            "Salgslag (kode)": "08",
            "Dokumenttype (kode)": "0",
            "Dokumentnummer": "0001591148",
            "Linjenummer": "1",
            "Landingsmåned (kode)": "01",
            "Art": "  Torsk  ",
        }
    )
    row.update(overrides)
    return row


def test_clean_casts_names_units_and_norwegian_formats(spark):
    cleaned = clean(spark.createDataFrame([bronze_row()]))
    row = cleaned.first()
    expected_schema = silver_schema()

    assert cleaned.columns == expected_schema.fieldNames()
    assert [field.dataType for field in cleaned.schema] == [
        field.dataType for field in expected_schema
    ]
    assert row.catch_year == 2023
    assert row.sales_org_code == "08"
    assert row.document_number == "0001591148"
    assert row.species_name == "Torsk"
    assert row.landing_month_code == "01"
    assert row.document_sale_date.isoformat() == "2023-01-02"
    # Check inside Spark: Python collection renders timestamps in the WSL host zone.
    utc_text = cleaned.select(
        F.date_format("document_version_at_utc", "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("value")
    ).first()["value"]
    assert utc_text == "2023-01-02 09:15:30.123456"
    assert row.is_partial_landing is False
    assert row.vessel_length_m == Decimal("1.500000")
    assert row.product_weight_kg == Decimal("1.500000")


def test_clean_turns_blank_and_invalid_values_into_null(spark):
    source = bronze_row(
        **{
            "Mottaksstasjon": "   ",
            "Dokument salgsdato": "31.02.2023",
            "Dokument versjonstidspunkt": "not-a-timestamp",
            "Dellanding (signal)": "x",
            "Antall stykk": "1,5",
            "Rundvekt": "not-a-number",
        }
    )

    row = clean(spark.createDataFrame([source])).first()

    assert row.landing_station is None
    assert row.document_sale_date is None
    assert row.document_version_at_utc is None
    assert row.is_partial_landing is None
    assert row.piece_count is None
    assert row.round_weight_kg is None


def test_clean_removes_exact_duplicates_after_normalisation(spark):
    first = bronze_row(**{"Art": "Torsk", "_run_id": "run-1"})
    duplicate = bronze_row(**{"Art": "  Torsk  ", "_run_id": "run-2"})
    different_line = bronze_row(**{"Art": "Torsk", "Linjenummer": "2", "_run_id": "run-1"})

    cleaned = clean(spark.createDataFrame([first, duplicate, different_line]))

    assert cleaned.count() == 2
    assert {row.line_number for row in cleaned.select("line_number").collect()} == {1, 2}


def test_clean_rejects_incomplete_bronze_contract(spark):
    incomplete = spark.createDataFrame([("2023",)], ["Fangstår"])

    with pytest.raises(ValueError, match="Bronze input is missing"):
        clean(incomplete)


def test_clean_rejects_unknown_dedup_strategy(spark):
    source = spark.createDataFrame([bronze_row()])

    with pytest.raises(ValueError, match="dedup_strategy"):
        clean(source, dedup_strategy="invented")


def test_clean_reads_the_actual_source_fixture(spark):
    root = Path(__file__).parents[2]
    baseline = json.loads(
        (root / "src/bronze/expected_schema.json").read_text(encoding="utf-8")
    )
    source_schema = StructType(
        [StructField(field["name"], StringType(), True) for field in baseline["fields"]]
    )
    bronze = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("quote", '"')
        .schema(source_schema)
        .csv(str(root / "tests/data/fangstdata_2023_sample.csv"))
        .withColumn("_source_name", F.lit("fiskeridir_catch"))
        .withColumn("_source_file", F.lit("fangstdata_2023.csv"))
        .withColumn("_source_period", F.lit("2023"))
        .withColumn("_source_sha256", F.lit("a" * 64))
        .withColumn("_ingested_at", F.to_timestamp(F.lit("2023-01-01 00:00:00")))
        .withColumn("_run_id", F.lit("test-run"))
    )

    cleaned = clean(bronze)
    rows = cleaned.orderBy("document_number").collect()

    assert len(rows) == 2
    assert rows[0].catch_year == 2023
    assert rows[0].species_name == "Norsk vårgytende sild"
    assert rows[0].product_weight_kg == Decimal("32420.000000")
    assert rows[0].document_version_at_utc is not None
