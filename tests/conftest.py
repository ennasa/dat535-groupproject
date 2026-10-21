import os
import sys

import pytest

os.environ.setdefault("PIPELINE_ENV", "test")
# Spark workers must use the same Python as the driver (the test interpreter).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def config():
    from common.config import load_config

    return load_config("test")


@pytest.fixture(scope="session")
def spark(config):
    from common.spark import get_spark

    session = get_spark(config)
    yield session
    session.stop()
