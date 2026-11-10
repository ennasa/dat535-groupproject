"""Add ingestion provenance without changing source columns."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.contracts import RunContext


def add_metadata(
    df: DataFrame, source_name: str, source_file: str, period: str, ctx: RunContext
) -> DataFrame:
    values = {
        "_source_name": source_name,
        "_source_file": source_file,
        "_source_period": period,
        "_run_id": ctx.run_id,
    }
    if (set(values) | {"_ingested_at"}) & set(df.columns):
        raise ValueError("Source columns collide with ingestion metadata")
    result = df
    for name, value in values.items():
        result = result.withColumn(name, F.lit(value))
    return result.withColumn("_ingested_at", F.lit(ctx.started_at).cast("timestamp"))
