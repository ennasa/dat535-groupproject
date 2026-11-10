"""Shared pipeline interfaces defined in docs/design.md."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True)
class RunContext:
    run_id: str
    env: Literal["dev", "prod", "test"]
    started_at: datetime
    config: dict[str, Any]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", self.run_id):
            raise ValueError(
                "run_id must contain only letters, digits, dots, underscores or hyphens"
            )
        if self.started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")


@dataclass
class RuleResult:
    rule: str
    action: Literal["reject", "correct", "flag"]
    failed: int


@dataclass
class LayerResult:
    layer: Literal["bronze", "silver", "gold"]
    status: Literal["success", "failed", "skipped"]
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    outputs: list[str] = field(default_factory=list)
    rules: list[RuleResult] = field(default_factory=list)
    message: str = ""
