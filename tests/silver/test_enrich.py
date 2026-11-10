from datetime import date

import pytest

from silver.enrich import add_derived_fields, enrich, join_reference
from silver.model import REFERENCE_JOINS


def silver_rows(spark):
    return spark.createDataFrame(
        [
            (
                2023, "08", "0", "doc-1", 1, date(2023, 3, 4),
                "03", date(2023, 3, 1), "NOR", "COD", "061101",
            ),
            (
                2023, "08", "0", "doc-2", 1, date(2024, 1, 2),
                "13", date(2023, 12, 31), "DNK", None, "999999",
            ),
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
    )


def test_add_derived_fields_preserves_grain(spark):
    rows = {
        row.document_number: row
        for row in add_derived_fields(silver_rows(spark)).collect()
    }

    assert len(rows) == 2
    assert len(rows["doc-1"].record_id) == 64
    assert rows["doc-1"].landing_year == 2023
    assert rows["doc-1"].landing_month == 3
    assert rows["doc-1"].landing_year_month == "2023-03"
    assert rows["doc-1"].days_catch_to_landing == 3
    assert rows["doc-1"].is_domestic_landing is True
    assert rows["doc-1"].has_fao_mapping is True
    assert rows["doc-2"].is_delivered_following_year is True
    assert rows["doc-2"].has_fao_mapping is False


def test_enrich_left_joins_reference_and_flags_unmatched_codes(spark):
    reference = spark.createDataFrame(
        [("061101", "Clupea harengus")],
        ["species_fdir_code", "scientific_name"],
    )
    result = enrich(silver_rows(spark), {"species_fdir": reference})
    rows = {row.document_number: row for row in result.collect()}

    assert len(rows) == 2
    assert rows["doc-1"].ref_species_fdir_scientific_name == "Clupea harengus"
    assert rows["doc-1"].ref_species_fdir_matched is True
    assert rows["doc-2"].ref_species_fdir_scientific_name is None
    assert rows["doc-2"].ref_species_fdir_matched is False
    assert rows["doc-1"].ref_gear_matched is None


def test_reference_join_rejects_non_unique_or_null_keys(spark):
    spec = next(spec for spec in REFERENCE_JOINS if spec.name == "species_fdir")
    duplicate = spark.createDataFrame(
        [("061101", "a"), ("061101", "b")],
        ["species_fdir_code", "label"],
    )
    null_key = spark.createDataFrame(
        [(None, "a")],
        "species_fdir_code string, label string",
    )

    with pytest.raises(ValueError, match="one non-null row"):
        join_reference(silver_rows(spark), duplicate, spec)
    with pytest.raises(ValueError, match="one non-null row"):
        join_reference(silver_rows(spark), null_key, spec)


def test_enrich_rejects_unknown_reference_name(spark):
    reference = spark.createDataFrame([("x",)], ["code"])

    with pytest.raises(ValueError, match="Unknown reference tables"):
        enrich(silver_rows(spark), {"invented": reference})
