"""Rebuild title.episode in the configured data directory and verify the resulting Parquet."""

import os
from pyspark.sql import functions as F

from src.loaders import read_tsv
from src.spark_session import get_spark


def table_signature(df):
    """Read every column and calculate verification metrics."""
    checks = [F.count("*").alias("total_rows")]

    for column in df.columns:
        checks.append(
            F.sum(
                F.length(
                    F.coalesce(F.col(column), F.lit(""))
                )
            ).alias(f"{column}_characters")
        )

        checks.append(
            F.count(
                F.when(
                    F.col(column).isNull(),
                    F.lit(1),
                )
            ).alias(f"{column}_nulls")
        )

    return df.agg(*checks).first().asDict()


def main():
    spark = get_spark("rebuild-episode")
    spark.sparkContext.setLogLevel("WARN")

    output = os.environ.get("REBUILD_OUTPUT", os.environ.get("DATA_DIR", "/home/jovyan/data") + "/parquet/recovered/title.episode")

    try:
        print(
            "Reading the recovered TSV from the project...",
            flush=True,
        )
        source = read_tsv(spark, "title.episode")

        source_signature = table_signature(source)

        print(
            f"Source metrics: {source_signature}",
            flush=True,
        )

        if source_signature["total_rows"] <= 0:
            raise RuntimeError("The source table is empty.")

        print(
            f"Writing recovered Parquet to {output}",
            flush=True,
        )
        source.write.mode("overwrite").parquet(output)

        print(
            "Reading back every Parquet column...",
            flush=True,
        )
        rebuilt = spark.read.parquet(output)
        rebuilt_signature = table_signature(rebuilt)

        print(
            f"Rebuilt metrics: {rebuilt_signature}",
            flush=True,
        )

        if source_signature != rebuilt_signature:
            raise RuntimeError(
                "Verification failed: source and rebuilt "
                "metrics differ. Do not replace the original table."
            )

        print(
            "REBUILD VERIFIED: "
            f"{rebuilt_signature['total_rows']} rows; "
            "all verification metrics match.",
            flush=True,
        )

        print(
            f"Container output: {output}",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()