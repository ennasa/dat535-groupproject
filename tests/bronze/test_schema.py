"""Expected Bronze schema and change reporting."""

import json

from bronze.schema import SCHEMA_PATH, compare_schema, load_expected_schema


def test_checked_in_schema_matches_a1_baseline():
    version, fields = load_expected_schema()
    assert version == "2023-a1"
    assert len(fields) == 133
    assert set(fields.values()) == {"string"}
    assert list(fields)[:3] == ["Dokumentnummer", "Dokumenttype (kode)", "Dokumenttype"]
    assert list(fields)[-1] == "Oppdateringstidspunkt"
    assert json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))["version"] == version


def test_added_missing_retyped_and_reordered_columns_are_reported():
    config = {"source": {"expected_columns": ["a", "b", "c"]}}
    report = compare_schema(["b", "a", "d"], config, {"b": "double"})
    assert report.changed
    assert report.added == ["d"]
    assert report.missing == ["c"]
    assert report.reordered
    assert [(item.column, item.expected, item.actual) for item in report.retyped] == [
        ("b", "string", "double")
    ]
    assert "added=['d']" in report.summary()
    assert "missing=['c']" in report.summary()
    assert "retyped=['b:string->double']" in report.summary()


def test_unchanged_schema_has_short_summary():
    config = {"source": {"expected_columns": ["a", "b"]}}
    report = compare_schema(["a", "b"], config)
    assert not report.changed
    assert report.as_dict()["status"] == "unchanged"
    assert report.summary() == "schema unchanged (2 columns)"
