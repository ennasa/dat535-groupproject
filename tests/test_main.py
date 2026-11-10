"""Pipeline entry point executes Bronze and stops on failure."""

import json
import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

import main
from common.config import load_config
from common.contracts import LayerResult


@pytest.fixture
def tmp_config(tmp_path):
    cfg = load_config("test")
    cfg["storage"]["root"] = str(tmp_path / "storage")
    return cfg


def _manifest(cfg, run_id="cli"):
    return json.loads((Path(cfg["storage"]["root"]) / "manifests" / f"{run_id}.json").read_text())


@pytest.mark.parametrize("status", ["success", "failed"])
def test_bronze_execution_and_failure_exit(monkeypatch, caplog, status, tmp_config):
    spark = Mock()
    runner = Mock(return_value=LayerResult("bronze", status))
    silver_runner = Mock(return_value=LayerResult("silver", "success"))
    monkeypatch.setattr("common.spark.get_spark", lambda config: spark)
    monkeypatch.setattr("bronze.ingest.run", runner)
    monkeypatch.setattr("silver.transform.run", silver_runner)
    monkeypatch.setattr(main, "load_config", lambda: tmp_config)
    monkeypatch.setattr("sys.argv", ["main.py", "--layers", "bronze,silver", "--run-id", "cli"])
    with caplog.at_level(logging.INFO):
        if status == "failed":
            with pytest.raises(SystemExit) as error:
                main.main()
            assert error.value.code == 1
            silver_runner.assert_not_called()
            layers = _manifest(tmp_config)["layers"]
            assert [(r["layer"], r["status"]) for r in layers] == [
                ("bronze", "failed"),
                ("silver", "skipped"),
            ]
            assert layers[1]["message"] == "upstream layer failed"
            assert _manifest(tmp_config)["status"] == "failed"
            assert "retention" not in _manifest(tmp_config)  # never clean up after a failure
        else:
            main.main()
            assert "'layer': 'silver'" in caplog.text
            silver_runner.assert_called_once()
    runner.assert_called_once()
    assert runner.call_args.args[1].run_id == "cli"
    spark.stop.assert_called_once()


def test_gold_layer_executes(monkeypatch, caplog, tmp_config):
    spark = Mock()
    runner = Mock(return_value=LayerResult("gold", "success"))
    monkeypatch.setattr("common.spark.get_spark", lambda config: spark)
    monkeypatch.setattr("gold.build.run", runner)
    monkeypatch.setattr(main, "load_config", lambda: tmp_config)
    monkeypatch.setattr("sys.argv", ["main.py", "--layers", "gold", "--run-id", "cli"])

    with caplog.at_level(logging.INFO):
        main.main()

    runner.assert_called_once()
    assert "'layer': 'gold'" in caplog.text
    spark.stop.assert_called_once()


def test_force_flag_reaches_layers(monkeypatch, tmp_config):
    runner = Mock(return_value=LayerResult("bronze", "success"))
    monkeypatch.setattr("common.spark.get_spark", lambda config: Mock())
    monkeypatch.setattr("bronze.ingest.run", runner)
    monkeypatch.setattr(main, "load_config", lambda: tmp_config)
    monkeypatch.setattr("sys.argv", ["main.py", "--layers", "bronze", "--run-id", "cli", "--force"])
    main.main()
    assert runner.call_args.args[1].force is True
    assert _manifest(tmp_config)["force"] is True
    assert "retention" not in _manifest(tmp_config)  # bronze only -> no clean-up


def test_run_manifest_lists_every_layer(monkeypatch, tmp_config):
    monkeypatch.setattr("common.spark.get_spark", lambda config: Mock())
    modules = {"bronze": "bronze.ingest", "silver": "silver.transform", "gold": "gold.build"}
    for layer, module in modules.items():
        monkeypatch.setattr(f"{module}.run", Mock(return_value=LayerResult(layer, "success")))
    monkeypatch.setattr(main, "load_config", lambda: tmp_config)
    monkeypatch.setattr("sys.argv", ["main.py", "--run-id", "cli"])
    main.main()
    data = _manifest(tmp_config)
    assert [r["layer"] for r in data["layers"]] == ["bronze", "silver", "gold"]
    assert data["status"] == "success"
    assert data["retention"]["enabled"] is True  # full successful run -> clean-up ran