from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def read_csv(
    spark: SparkSession,
    path: str,
    header: bool = True,
    infer_schema: bool = True,
    sep: str = ",",
    options: Optional[dict[str, str]] = None,
) -> DataFrame:
    reader = spark.read.options(header=str(header).lower(), inferSchema=str(infer_schema).lower(), sep=sep)
    if options:
        reader = reader.options(**options)
    return reader.csv(path)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite", partition_by: Optional[list[str]] = None) -> None:
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
