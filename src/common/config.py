"""Load pipeline configuration: config/base.yaml merged with config/<env>.yaml.

The environment is chosen by the PIPELINE_ENV variable (dev, prod or test).
"""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml

VALID_ENVS = ("dev", "prod", "test")
DEFAULT_ENV = "dev"
CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Return a new dict where nested keys in `override` replace those in `base`."""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a YAML mapping at the top level")
    return data


def load_config(env: str | None = None, config_dir: Path = CONFIG_DIR) -> dict[str, Any]:
    """Load the merged config for `env` (defaults to $PIPELINE_ENV, then 'dev')."""
    env = env or os.environ.get("PIPELINE_ENV", DEFAULT_ENV)
    if env not in VALID_ENVS:
        raise ValueError(f"Unknown PIPELINE_ENV '{env}', expected one of {VALID_ENVS}")

    base = _read_yaml(config_dir / "base.yaml")
    config = deep_merge(base, _read_yaml(config_dir / f"{env}.yaml"))
    if root := os.environ.get("PIPELINE_STORAGE_ROOT"):
        config["storage"]["root"] = root
    config["env"] = env
    return config


def layer_path(config: dict[str, Any], layer: str) -> str:
    """Full storage path for a layer, e.g. layer_path(cfg, 'bronze')."""
    storage = config["storage"]
    return str(Path(storage["root"]) / storage[layer])
