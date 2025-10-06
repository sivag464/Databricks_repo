from __future__ import annotations

import json
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession


def _json_formatter(record: logging.LogRecord) -> str:
    payload = {
        "level": record.levelname,
        "message": record.getMessage(),
        "name": record.name,
        "time": getattr(record, "asctime", None),
    }
    return json.dumps(payload, ensure_ascii=False)


class _JsonLogHandler(logging.StreamHandler):
    def __init__(self) -> None:
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        return _json_formatter(record)


def setup_logging(level: str | int = "INFO") -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(level if isinstance(level, int) else getattr(logging, str(level).upper(), logging.INFO))
    handler = _JsonLogHandler()
    root.addHandler(handler)


def create_spark(app_name: str = "pyspark-app", extra_configs: Optional[dict[str, str]] = None) -> SparkSession:
    setup_logging()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", os.getenv("SPARK_SQL_TZ", "UTC"))
    )

    if os.getenv("SPARK_LOCAL", "0") == "1":
        builder = builder.master("local[*]")

    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
