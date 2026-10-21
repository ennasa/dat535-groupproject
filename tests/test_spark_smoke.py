def test_spark_session_starts_and_counts(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
    assert df.filter(df.id > 1).count() == 2


def test_spark_uses_test_config(spark):
    assert spark.conf.get("spark.sql.shuffle.partitions") == "1"
