import os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from src.schemas import TABLE_COLUMNS
DATA_DIR = os.environ.get("DATA_DIR", "/home/jovyan/data")
NULL_TOKEN = "\\N"
def parquet_path(table):
    """Return the raw Parquet directory for a table."""
    return f"{DATA_DIR}/parquet/raw/{table}"
def read_tsv(spark, table):
    """Read original TSV files with all columns kept as strings."""
    schema = StructType([
        StructField(column_name, StringType(), True)
        for column_name, _ in TABLE_COLUMNS[table]
    ])
    return (
        spark.read
        .option("sep", "\t")
        .option("header", True)
        .option("quote", "")
        .schema(schema)
        .csv(f"{DATA_DIR}/raw/{table}.tsv.gz")
    )
def read_raw(spark, table):
    """Read the raw Parquet table without cleaning."""
    source = os.environ.get("INPUT_FORMAT", "tsv").lower()
    if source == "tsv":
        return read_tsv(spark, table)
    if source != "parquet":
        raise ValueError("INPUT_FORMAT must be tsv or parquet")
    path = parquet_path(table)
    if not os.path.isfile(f"{path}/_SUCCESS"):
        raise RuntimeError(f"Incomplete Parquet table: {path}")
    return spark.read.parquet(path)
def read_typed(spark, table):
    """Normalize missing values and safely cast column types."""
    df = read_raw(spark, table)
    for column_name, dtype in TABLE_COLUMNS[table]:
        value = F.col(column_name)
        trimmed = F.trim(value)
        is_missing = (
            value.isNull()
            | (trimmed == NULL_TOKEN)
            | (trimmed == "")
        )
        normalized = F.when(
            is_missing,
            F.lit(None),
        ).otherwise(value)
        df = df.withColumn(
            column_name,
            normalized.try_cast(dtype),
        )
    return df