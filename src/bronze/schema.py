"""Expected raw schema and schema-change reporting for Bronze."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

RAW_TYPE = "string"
SCHEMA_PATH = Path(__file__).with_name("expected_schema.json")


@dataclass(frozen=True)
class TypeChange:
    column: str
    expected: str
    actual: str


@dataclass(frozen=True)
class SchemaReport:
    expected_version: str
    expected_columns: int
    actual_columns: int
    added: list[str]
    missing: list[str]
    retyped: list[TypeChange]
    reordered: bool

    @property
    def changed(self) -> bool:
        return bool(self.added or self.missing or self.retyped or self.reordered)

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["status"] = "changed" if self.changed else "unchanged"
        return value

    def summary(self) -> str:
        if not self.changed:
            return f"schema unchanged ({self.actual_columns} columns)"
        changes: list[str] = []
        if self.added:
            changes.append(f"added={self.added}")
        if self.missing:
            changes.append(f"missing={self.missing}")
        if self.retyped:
            values = [f"{item.column}:{item.expected}->{item.actual}" for item in self.retyped]
            changes.append(f"retyped={values}")
        if self.reordered:
            changes.append("reordered=true")
        return "schema changed: " + "; ".join(changes)


def load_expected_schema(config: dict[str, Any] | None = None) -> tuple[str, dict[str, str]]:
    """Return the versioned raw schema, with a small test override when configured."""
    configured = (config or {}).get("source", {}).get("expected_columns")
    if configured is not None:
        if not isinstance(configured, list) or not all(
            isinstance(name, str) for name in configured
        ):
            raise ValueError("source.expected_columns must be a list of column names")
        return "configured", dict.fromkeys(configured, RAW_TYPE)

    value = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    fields = value.get("fields")
    if not isinstance(fields, list) or not fields:
        raise ValueError("Expected schema must contain a nonempty fields list")
    expected: dict[str, str] = {}
    for field in fields:
        if not isinstance(field, dict) or set(field) != {"name", "type"}:
            raise ValueError("Each expected field needs exactly name and type")
        name, raw_type = field["name"], field["type"]
        if not isinstance(name, str) or not isinstance(raw_type, str) or name in expected:
            raise ValueError("Expected schema contains an invalid or duplicate field")
        expected[name] = raw_type
    return str(value["version"]), expected


def compare_schema(
    actual_columns: list[str],
    config: dict[str, Any] | None = None,
    actual_types: dict[str, str] | None = None,
) -> SchemaReport:
    """Compare names, order and raw types without inferring values from the CSV."""
    version, expected = load_expected_schema(config)
    types = dict.fromkeys(actual_columns, RAW_TYPE)
    types.update(actual_types or {})
    expected_names = list(expected)
    expected_set = set(expected_names)
    actual_set = set(actual_columns)
    shared_expected = [name for name in expected_names if name in actual_set]
    shared_actual = [name for name in actual_columns if name in expected_set]
    retyped = [
        TypeChange(name, expected[name], types[name])
        for name in shared_expected
        if types.get(name) != expected[name]
    ]
    return SchemaReport(
        expected_version=version,
        expected_columns=len(expected_names),
        actual_columns=len(actual_columns),
        added=[name for name in actual_columns if name not in expected_set],
        missing=[name for name in expected_names if name not in actual_set],
        retyped=retyped,
        reordered=shared_actual != shared_expected,
    )
