"""Verify full-column reads of recovered Parquet tables."""

import json
import os
from pathlib import Path

from pyspark.sql import functions as F

from src.schemas import TABLE_COLUMNS
from src.spark_session import get_spark


def main():
    spark = get_spark("verify-local-parquet")
    spark.sparkContext.setLogLevel("ERROR")
    results = []
    root = os.environ.get("VERIFY_PARQUET_DIR", os.environ.get("DATA_DIR", "/home/jovyan/data") + "/parquet/raw")

    try:
        for table in TABLE_COLUMNS:
            print(f"CHECKING: {table}", flush=True)

            try:
                df = spark.read.parquet(
                    f"{root}/{table}"
                )

                expressions = [
                    F.count("*").alias("total_rows")
                ]

                for column, _ in TABLE_COLUMNS[table]:
                    expressions.append(
                        F.sum(
                            F.length(
                                F.coalesce(
                                    F.col(column).cast("string"),
                                    F.lit(""),
                                )
                            )
                        ).alias(f"{column}_characters")
                    )

                metrics = df.agg(*expressions).first().asDict()

                if metrics["total_rows"] == 0:
                    raise RuntimeError("Empty table")

                results.append({
                    "table": table,
                    "status": "PASS",
                    "metrics": metrics,
                })

                print(
                    f"PASS: {table} — "
                    f"{metrics['total_rows']} rows",
                    flush=True,
                )

            except Exception as exc:
                results.append({
                    "table": table,
                    "status": "FAIL",
                    "error": str(exc),
                })
                print(f"FAIL: {table}", flush=True)

        Path(root).joinpath("verification.json").write_text(
            json.dumps(results, indent=2),
            encoding="utf-8",
        )

        print("\nFINAL SUMMARY", flush=True)
        for result in results:
            print(
                f"{result['status']}: {result['table']}",
                flush=True,
            )

        if any(r["status"] == "FAIL" for r in results):
            raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()