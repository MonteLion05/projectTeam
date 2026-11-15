"""Test TSV-to-Parquet conversion entirely on USB."""

import os
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.schemas import TABLE_COLUMNS
from src.spark_session import get_spark


def signature(df):
    checks = [F.count("*").alias("rows")]

    for column in df.columns:
        checks.append(
            F.sum(
                F.length(
                    F.coalesce(F.col(column), F.lit(""))
                )
            ).alias(f"{column}_length")
        )
        checks.append(
            F.count(
                F.when(F.col(column).isNull(), F.lit(1))
            ).alias(f"{column}_nulls")
        )

    return df.agg(*checks).first().asDict()


data_dir = os.environ.get("DATA_DIR", "/home/jovyan/data")
spark = get_spark("test-usb")
spark.sparkContext.setLogLevel("WARN")

try:
    schema = StructType([
        StructField(name, StringType(), True)
        for name, _ in TABLE_COLUMNS["title.basics"]
    ])

    source = (
        spark.read
        .option("sep", "\t")
        .option("header", True)
        .option("quote", "")
        .schema(schema)
        .csv(f"{data_dir}/raw/title.basics.tsv.gz")
    )

    print("Checking source...", flush=True)
    before = signature(source)
    print(before, flush=True)

    output = f"{data_dir}/parquet/usb_test/title.basics"

    print("Writing Parquet on USB...", flush=True)
    source.write.mode("overwrite").parquet(output)

    print("Reading Parquet back...", flush=True)
    after = signature(spark.read.parquet(output))
    print(after, flush=True)

    if before["rows"] <= 0 or before != after:
        raise RuntimeError("Verification metrics do not match.")

    print("USB PARQUET CHECK PASSED", flush=True)

finally:
    spark.stop()