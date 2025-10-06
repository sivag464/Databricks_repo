from __future__ import annotations

import os

from pyspark.sql import SparkSession

from pyspark_app.transforms import add_timestamp_to_datetime


def _make_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", os.getenv("SPARK_SQL_TZ", "UTC"))
        .getOrCreate()
    )


def test_add_timestamp_to_datetime():
    spark = _make_spark()
    try:
        df = spark.createDataFrame([("1590183026",), ("2000000000",)], ["ts_col"])
        actual = add_timestamp_to_datetime(df, "ts_col", "date_col").collect()
        assert len(actual) == 2
        assert "date_col" in add_timestamp_to_datetime(df, "ts_col", "date_col").columns
    finally:
        spark.stop()
