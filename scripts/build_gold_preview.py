"""Build the isolated A9/A10 Gold preview from the measured local Silver output."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from common.config import load_config
from common.contracts import RunContext
from common.spark import get_spark
from gold.build import run


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config("dev")
    config["storage"]["silver"] = "../performance/optimized-balanced-local/silver"
    config["storage"]["gold"] = "../preview/gold"
    config["gold"]["allow_unvalidated_silver"] = True
    ctx = RunContext("gold-preview-2023", "dev", datetime.now(timezone.utc), config)

    spark = get_spark(config)
    try:
        result = run(spark, ctx)
    finally:
        spark.stop()
    evidence = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "local unvalidated preview; not a published Gold release",
        "result": asdict(result),
    }
    path = root / "data/preview/gold-preview-result.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2))
    if result.status not in {"success", "skipped"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
