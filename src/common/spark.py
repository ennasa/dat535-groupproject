"""Create a SparkSession from the pipeline config."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession


def get_spark(config: dict[str, Any]) -> SparkSession:
    spark_cfg = config["spark"]
    builder = SparkSession.builder.appName(f"{spark_cfg['app_name']}-{config['env']}").master(
        spark_cfg["master"]
    )
    for key, value in spark_cfg.get("conf", {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
