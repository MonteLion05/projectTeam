"""Read IMDb tables: raw (all strings, '\\N' kept) or typed ('\\N' -> null)."""
import os

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.schemas import TABLE_COLUMNS

DATA_DIR = os.environ.get("DATA_DIR", "/home/jovyan/data")
NULL_TOKEN = "\\N"


def parquet_path(table):
    return f"{DATA_DIR}/parquet/raw/{table}"


def read_tsv(spark, table):
    """Read the original .tsv.gz as strings, without any cleaning."""
    schema = StructType(
        [StructField(c, StringType(), True) for c, _ in TABLE_COLUMNS[table]]
    )
    return (
        spark.read.option("sep", "\t")
        .option("header", True)
        .option("quote", "")
        .schema(schema)
        .csv(f"{DATA_DIR}/raw/{table}.tsv.gz")
    )


def read_raw(spark, table):
    """Raw layer (Parquet): use it for the data quality analysis."""
    return spark.read.parquet(parquet_path(table))


def read_typed(spark, table):
    """Typed layer: '\\N' -> null, then cast. Malformed values become null."""
    df = read_raw(spark, table)
    for col, dtype in TABLE_COLUMNS[table]:
        df = df.withColumn(
            col,
            F.when(F.col(col) == NULL_TOKEN, F.lit(None))
            .otherwise(F.col(col))
            .cast(dtype),
        )
    return df

def read_clean(spark, table):
    """Clean table (after quality.clean_all): typed, null for missing, dq_flags."""
    return spark.read.parquet(f"{DATA_DIR}/parquet/clean/{table}")