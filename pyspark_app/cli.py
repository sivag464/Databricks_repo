from __future__ import annotations

import argparse
import logging
from typing import Sequence

from pyspark_app.jobs.sample_job import run as run_sample


logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pyspark-app", description="PySpark production template CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sample = sub.add_parser("sample", help="Run sample ETL job")
    sample.add_argument("--input", required=True, help="Input CSV path")
    sample.add_argument("--output", required=True, help="Output Parquet path")
    sample.add_argument("--sep", default=",", help="CSV separator")
    sample.add_argument("--no-header", action="store_true", help="CSV without header row")
    sample.add_argument("--no-infer-schema", action="store_true", help="Do not infer schema")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "sample":
        run_sample(
            input_path=args.input,
            output_path=args.output,
            input_sep=args.sep,
            input_header=not args.no_header,
            infer_schema=not args.no_infer_schema,
        )
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
