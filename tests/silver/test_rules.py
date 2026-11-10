"""Quality rules engine (#17)."""

from decimal import Decimal

import pytest

from silver.rules import FAILED_RULES, QUALITY_FLAGS, apply_rules

COLUMNS = ["document_number", "line_number", "round_weight_kg", "landing_county_code"]


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [
            ("doc-1", 1, Decimal("120.5"), "15"),  # clean row
            ("doc-2", 1, Decimal("0"), None),  # zero weight, missing county
            ("doc-3", 1, Decimal("-4"), "18"),  # negative weight
            (None, 1, Decimal("10"), "18"),  # missing key part
            ("doc-5", 1, None, "18"),  # missing weight
            ("doc-6", 1, Decimal("7"), "18"),  # duplicate key with the next row
            ("doc-6", 1, Decimal("8"), "18"),
        ],
        "document_number string, line_number int, round_weight_kg decimal(24,6), "
        "landing_county_code string",
    )


RULES = {
    "key_not_null": {
        "check": "not_null",
        "columns": ["document_number", "line_number"],
        "action": "reject",
    },
    "key_unique": {
        "check": "unique",
        "columns": ["document_number", "line_number"],
        "action": "reject",
    },
    "round_weight_not_negative": {
        "check": "range",
        "column": "round_weight_kg",
        "min": 0,
        "action": "reject",
    },
    "round_weight_missing": {"check": "not_null", "column": "round_weight_kg", "action": "flag"},
    "round_weight_zero": {
        "check": "range",
        "column": "round_weight_kg",
        "min": 0,
        "exclusive_min": True,
        "action": "flag",
    },
    "landing_county_missing": {
        "check": "not_null",
        "column": "landing_county_code",
        "action": "flag",
    },
}


def _by_doc(frame, column):
    return {row["document_number"]: sorted(row[column]) for row in frame.collect()}


def test_rows_are_split_into_accepted_and_rejected(df):
    accepted, rejected, _ = apply_rules(df, RULES)

    assert accepted.count() + rejected.count() == df.count()  # nothing lost or duplicated
    assert _by_doc(rejected, FAILED_RULES) == {
        "doc-3": ["round_weight_not_negative"],
        None: ["key_not_null"],
        "doc-6": ["key_unique"],  # both copies of the duplicate key
    }
    assert rejected.count() == 4


def test_flags_mark_accepted_rows_without_removing_them(df):
    accepted, _, _ = apply_rules(df, RULES)

    assert _by_doc(accepted, QUALITY_FLAGS) == {
        "doc-1": [],
        "doc-2": ["landing_county_missing", "round_weight_zero"],
        "doc-5": ["round_weight_missing"],  # null weight is not also counted as "zero"
    }


def test_counts_per_rule(df):
    _, _, results = apply_rules(df, RULES)

    assert {r.rule: (r.action, r.failed) for r in results} == {
        "key_not_null": ("reject", 1),
        "key_unique": ("reject", 2),
        "round_weight_not_negative": ("reject", 1),
        "round_weight_missing": ("flag", 1),
        "round_weight_zero": ("flag", 2),  # doc-2 (0) and doc-3 (-4)
        "landing_county_missing": ("flag", 1),
    }


def test_output_columns(df):
    accepted, rejected, _ = apply_rules(df, RULES)

    assert accepted.columns == COLUMNS + [QUALITY_FLAGS]
    assert rejected.columns == COLUMNS + [FAILED_RULES]


def test_correct_replaces_value_with_null_by_default(df):
    rules = {
        "round_weight_not_negative": {
            "check": "range",
            "column": "round_weight_kg",
            "min": 0,
            "action": "correct",
        }
    }
    accepted, rejected, results = apply_rules(df, rules)

    weights = {r["document_number"]: r["round_weight_kg"] for r in accepted.collect()}
    assert weights["doc-3"] is None  # corrected to null, never silently to zero
    assert weights["doc-1"] == Decimal("120.5")
    assert rejected.count() == 0
    assert results[0].failed == 1


@pytest.mark.parametrize(
    ("rule", "expected"),
    [
        ({"check": "in_set", "column": "landing_county_code", "values": ["15", "18"]}, 0),
        ({"check": "in_set", "column": "landing_county_code", "values": ["15"]}, 5),
        ({"check": "regex", "column": "document_number", "pattern": r"^doc-\d+$"}, 0),
        ({"check": "regex", "column": "document_number", "pattern": r"^doc-[1-3]$"}, 3),
        ({"check": "range", "column": "round_weight_kg", "max": 100}, 1),
    ],
)
def test_other_checks(df, rule, expected):
    _, _, results = apply_rules(df, {"r": {**rule, "action": "flag"}})
    assert results[0].failed == expected


def test_no_rules_accepts_everything(df):
    accepted, rejected, results = apply_rules(df, {})
    assert accepted.count() == df.count()
    assert rejected.count() == 0
    assert results == []


@pytest.mark.parametrize(
    ("rule", "message"),
    [
        ({"check": "not_null", "column": "nope", "action": "reject"}, "unknown column"),
        ({"check": "maybe", "column": "line_number", "action": "reject"}, "'check'"),
        ({"check": "not_null", "column": "line_number", "action": "drop"}, "'action'"),
        ({"check": "unique", "column": "line_number", "action": "correct"}, "cannot be corrected"),
        ({"check": "range", "column": "line_number", "action": "flag"}, "min"),
    ],
)
def test_misconfigured_rules_fail_clearly(df, rule, message):
    with pytest.raises(ValueError, match=message):
        apply_rules(df, {"bad": rule})