"""Pipeline entry point executes Bronze and stops on failure."""

import logging
from unittest.mock import Mock

import pytest

import main
from common.config import load_config
from common.contracts import LayerResult


@pytest.mark.parametrize("status", ["success", "failed"])
def test_bronze_execution_and_failure_exit(monkeypatch, caplog, status):
    spark = Mock()
    runner = Mock(return_value=LayerResult("bronze", status))
    silver_runner = Mock(return_value=LayerResult("silver", "success"))
    monkeypatch.setattr("common.spark.get_spark", lambda config: spark)
    monkeypatch.setattr("bronze.ingest.run", runner)
    monkeypatch.setattr("silver.transform.run", silver_runner)
    monkeypatch.setattr(main, "load_config", lambda: load_config("test"))
    monkeypatch.setattr("sys.argv", ["main.py", "--layers", "bronze,silver", "--run-id", "cli"])
    with caplog.at_level(logging.INFO):
        if status == "failed":
            with pytest.raises(SystemExit) as error:
                main.main()
            assert error.value.code == 1
            assert "'layer': 'silver'" not in caplog.text
        else:
            main.main()
            assert "'layer': 'silver'" in caplog.text
            silver_runner.assert_called_once()
    runner.assert_called_once()
    assert runner.call_args.args[1].run_id == "cli"
    spark.stop.assert_called_once()


def test_gold_layer_executes(monkeypatch, caplog):
    spark = Mock()
    runner = Mock(return_value=LayerResult("gold", "success"))
    monkeypatch.setattr("common.spark.get_spark", lambda config: spark)
    monkeypatch.setattr("gold.build.run", runner)
    monkeypatch.setattr(main, "load_config", lambda: load_config("test"))
    monkeypatch.setattr("sys.argv", ["main.py", "--layers", "gold", "--run-id", "cli"])

    with caplog.at_level(logging.INFO):
        main.main()

    runner.assert_called_once()
    assert "'layer': 'gold'" in caplog.text
    spark.stop.assert_called_once()
