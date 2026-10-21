"""Pipeline entry point: python src/main.py [--layers bronze,silver,gold]

The layer steps are placeholders until Phase 1-3 implement them.
"""

from __future__ import annotations

import argparse
import logging
import uuid

from common.config import load_config

log = logging.getLogger("pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the medallion pipeline")
    parser.add_argument("--layers", help="Comma-separated layers, default from config")
    parser.add_argument("--run-id", default=None, help="Run identifier (default: random UUID)")
    args = parser.parse_args()

    config = load_config()
    logging.basicConfig(
        level=config["logging"]["level"], format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    run_id = args.run_id or str(uuid.uuid4())
    layers = args.layers.split(",") if args.layers else config["run"]["layers"]

    log.info("Starting run %s in env=%s, layers=%s", run_id, config["env"], layers)
    for layer in layers:
        log.info("[%s] not implemented yet", layer)
    log.info("Run %s finished", run_id)


if __name__ == "__main__":
    main()
