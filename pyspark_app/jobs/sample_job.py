from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import SparkSession

from pyspark_app.io import read_csv, write_parquet
from pyspark_app.transforms import add_timestamp_to_datetime
from pyspark_app.spark import create_spark


logger = logging.getLogger(__name__)


def run(
    input_path: str,
    output_path: str,
    input_sep: str = ",",
    input_header: bool = True,
    infer_schema: bool = True,
    spark: Optional[SparkSession] = None,
) -> None:
    spark_session = spark or create_spark(app_name="sample-job")
    logger.info("starting sample_job")

    df = read_csv(spark_session, input_path, header=input_header, infer_schema=infer_schema, sep=input_sep)
    if "ts_col" in df.columns:
        df = add_timestamp_to_datetime(df, "ts_col", "date_col")

    write_parquet(df, output_path)
    logger.info("finished sample_job")
