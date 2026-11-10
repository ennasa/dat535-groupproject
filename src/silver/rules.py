"""Quality rules for Silver (#17).

Rules are read from `quality.rules` in the config. Each rule has a check and an action:
reject (row goes to quarantine), flag (row is kept and marked) or correct (value set to null).
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from common.contracts import RuleResult

FAILED_RULES = "_failed_rules"
QUALITY_FLAGS = "_quality_flags"
CHECKS = {"not_null", "range", "in_set", "regex", "unique"}
ACTIONS = {"reject", "flag", "correct"}


def _columns(name: str, rule: dict[str, Any]) -> list[str]:
    columns = rule.get("columns") or ([rule["column"]] if "column" in rule else [])
    if not columns:
        raise ValueError(f"Rule {name!r} needs 'column' or 'columns'")
    return list(columns)


def _breaks(name: str, rule: dict[str, Any]) -> Column:
    """True for rows that break the rule."""
    check = rule["check"]
    columns = _columns(name, rule)
    col = F.col(columns[0])

    if check == "not_null":
        broken = F.lit(False)
        for column in columns:
            broken = broken | F.col(column).isNull()
        return broken
    if check == "range":
        if "min" not in rule and "max" not in rule:
            raise ValueError(f"Rule {name!r}: range needs 'min' and/or 'max'")
        broken = F.lit(False)
        if "min" in rule:
            too_low = col <= rule["min"] if rule.get("exclusive_min") else col < rule["min"]
            broken = broken | too_low
        if "max" in rule:
            broken = broken | (col > rule["max"])
        # null is handled by not_null rules, so it doesn't count here
        return F.coalesce(broken, F.lit(False))
    if check == "in_set":
        return F.coalesce(~col.isin(list(rule["values"])), F.lit(False))
    if check == "regex":
        return F.coalesce(~col.cast("string").rlike(rule["pattern"]), F.lit(False))
    if check == "unique":
        group = Window.partitionBy(*columns)
        return F.count(F.lit(1)).over(group) > 1
    raise ValueError(f"Rule {name!r}: unknown check {check!r}, expected one of {sorted(CHECKS)}")


def validate_rules(rules: dict[str, dict[str, Any]], columns: list[str]) -> None:
    for name, rule in rules.items():
        if rule.get("check") not in CHECKS:
            raise ValueError(f"Rule {name!r}: 'check' must be one of {sorted(CHECKS)}")
        if rule.get("action") not in ACTIONS:
            raise ValueError(f"Rule {name!r}: 'action' must be one of {sorted(ACTIONS)}")
        if rule["action"] == "correct" and rule["check"] in {"not_null", "unique"}:
            raise ValueError(f"Rule {name!r}: '{rule['check']}' cannot be corrected")
        missing = [c for c in _columns(name, rule) if c not in columns]
        if missing:
            raise ValueError(f"Rule {name!r} refers to unknown column(s) {missing}")


def apply_rules(
    df: DataFrame, rules: dict[str, dict[str, Any]]
) -> tuple[DataFrame, DataFrame, list[RuleResult]]:
    """Return (accepted, rejected, results)."""
    validate_rules(rules, df.columns)
    names = list(rules)
    if not names:
        no_flags = F.array().cast("array<string>")
        return df.withColumn(QUALITY_FLAGS, no_flags), df.limit(0), []

    # one True/False column per rule
    marked = df
    for i, name in enumerate(names):
        marked = marked.withColumn(f"_rule_{i}", _breaks(name, rules[name]))

    # count all rules in one pass
    counts = marked.agg(
        *[F.sum(F.col(f"_rule_{i}").cast("long")).alias(f"_rule_{i}") for i in range(len(names))]
    ).first()
    results = [
        RuleResult(name, rules[name]["action"], int(counts[f"_rule_{i}"] or 0))
        for i, name in enumerate(names)
    ]

    for i, name in enumerate(names):
        rule = rules[name]
        if rule["action"] == "correct":
            column = _columns(name, rule)[0]
            fixed = F.lit(rule.get("value")).cast(df.schema[column].dataType)
            marked = marked.withColumn(
                column, F.when(F.col(f"_rule_{i}"), fixed).otherwise(F.col(column))
            )

    def broken_names(action: str) -> Column:
        parts = [
            F.when(F.col(f"_rule_{i}"), F.lit(name))
            for i, name in enumerate(names)
            if rules[name]["action"] == action
        ]
        if not parts:
            return F.array().cast("array<string>")
        return F.filter(F.array(*parts), lambda x: x.isNotNull())

    temporary = [f"_rule_{i}" for i in range(len(names))]
    marked = marked.withColumn(FAILED_RULES, broken_names("reject")).withColumn(
        QUALITY_FLAGS, broken_names("flag")
    )

    is_rejected = F.size(FAILED_RULES) > 0
    rejected = marked.filter(is_rejected).drop(*temporary, QUALITY_FLAGS)
    accepted = marked.filter(~is_rejected).drop(*temporary, FAILED_RULES)
    return accepted, rejected, results