"""Checks that the quality rules in config/base.yaml fit the Silver model (#18)."""

from common.config import load_config
from silver.model import KEY_FIELDS, silver_schema
from silver.rules import validate_rules


def rules():
    return load_config("dev")["quality"]["rules"]


def test_rules_use_real_silver_columns():
    # fails if a rule has a typo or a column was renamed in model.py
    validate_rules(rules(), silver_schema().fieldNames())


def test_key_rules_match_the_model_key():
    assert tuple(rules()["key_not_null"]["columns"]) == KEY_FIELDS
    assert tuple(rules()["key_unique"]["columns"]) == KEY_FIELDS
    assert rules()["key_not_null"]["action"] == "reject"
    assert rules()["key_unique"]["action"] == "reject"


def test_known_data_issues_are_flagged_not_rejected():
    # these are real values in 2023 (A4 profiling), so we keep the rows
    for name in [
        "round_weight_missing",
        "round_weight_zero",
        "landing_county_missing",
        "species_fao_missing",
        "catch_value_missing",
    ]:
        assert rules()[name]["action"] == "flag", name