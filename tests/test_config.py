import pytest

from common.config import VALID_ENVS, deep_merge, layer_path, load_config


def test_deep_merge_overrides_nested_keys_only():
    base = {"a": 1, "spark": {"master": "local[*]", "conf": {"x": "1", "y": "2"}}}
    override = {"spark": {"conf": {"y": "9"}}}
    merged = deep_merge(base, override)
    assert merged == {"a": 1, "spark": {"master": "local[*]", "conf": {"x": "1", "y": "9"}}}
    assert base["spark"]["conf"]["y"] == "2"  # base not mutated


@pytest.mark.parametrize("env", VALID_ENVS)
def test_every_env_loads_with_required_sections(env):
    cfg = load_config(env)
    assert cfg["env"] == env
    for section in ("storage", "source", "spark", "quality", "run", "logging"):
        assert section in cfg


def test_env_var_selects_config(monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "prod")
    assert load_config()["env"] == "prod"


def test_unknown_env_is_rejected():
    with pytest.raises(ValueError):
        load_config("staging")


def test_environments_use_separate_storage():
    roots = {env: load_config(env)["storage"]["root"] for env in VALID_ENVS}
    assert len(set(roots.values())) == len(VALID_ENVS)


def test_layer_path(config):
    assert layer_path(config, "bronze").endswith("bronze")


def test_no_secrets_in_config():
    """Config keys must not look like credentials; those belong in env vars / GitHub secrets."""
    import pathlib
    import re

    secret_key = re.compile(r"^\s*[\w.-]*(password|secret|token|api_key)[\w.-]*\s*:", re.I)
    for path in pathlib.Path("config").glob("*.yaml"):
        for line in path.read_text().splitlines():
            assert not secret_key.match(line), f"{path}: '{line.strip()}' looks like a secret"
