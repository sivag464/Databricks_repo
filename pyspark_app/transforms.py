from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime


def add_timestamp_to_datetime(df: DataFrame, timestamp_column: str, output_column: str = "datetime") -> DataFrame:
    return df.withColumn(output_column, from_unixtime(col(timestamp_column)))
