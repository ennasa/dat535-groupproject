import json
from pathlib import Path

from pyspark.sql.types import DecimalType, StringType

from silver.model import (
    APPROVAL_STATUS,
    DUPLICATE_ORDER,
    KEY_FIELDS,
    REFERENCE_JOINS,
    SILVER_FIELDS,
    silver_schema,
    source_to_target,
)


def test_model_has_unique_mapping_and_expected_key():
    schema = silver_schema()
    mapping = source_to_target()

    assert len(schema) == len(SILVER_FIELDS) == len(mapping)
    assert len(schema.fieldNames()) == len(set(schema.fieldNames()))
    assert KEY_FIELDS == (
        "catch_year",
        "sales_org_code",
        "document_type_code",
        "document_number",
        "line_number",
    )
    assert all(not schema[name].nullable for name in KEY_FIELDS)


def test_model_sources_exist_in_bronze_contract():
    baseline_path = Path(__file__).parents[2] / "src" / "bronze" / "expected_schema.json"
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    bronze_fields = {field["name"] for field in baseline["fields"]}
    bronze_fields.update(
        {
            "_source_name",
            "_source_file",
            "_source_period",
            "_source_sha256",
            "_ingested_at",
            "_run_id",
        }
    )

    assert set(source_to_target()).issubset(bronze_fields)


def test_model_preserves_codes_and_uses_typed_measures():
    schema = silver_schema()

    assert isinstance(schema["document_number"].dataType, StringType)
    assert isinstance(schema["landing_month_code"].dataType, StringType)
    assert isinstance(schema["round_weight_kg"].dataType, DecimalType)
    assert schema["round_weight_kg"].nullable
    assert schema["species_fao_code"].nullable


def test_review_sensitive_decisions_are_explicit():
    schema_names = set(silver_schema().fieldNames())

    assert APPROVAL_STATUS == "awaiting-person-b-review"
    assert set(DUPLICATE_ORDER).issubset(schema_names)
    assert all(join.join_type == "left" for join in REFERENCE_JOINS)
    assert all(join.unmatched_action == "retain-and-flag" for join in REFERENCE_JOINS)
