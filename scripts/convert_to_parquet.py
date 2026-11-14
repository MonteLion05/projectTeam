"""Convert the raw TSV files to Parquet (strings) for fast reads."""
from src.loaders import parquet_path, read_tsv
from src.schemas import TABLE_COLUMNS
from src.spark_session import get_spark

if __name__ == "__main__":
    spark = get_spark("convert-to-parquet")
    for table in TABLE_COLUMNS:
        print(f"Converting {table}...")
        read_tsv(spark, table).write.mode("overwrite").parquet(parquet_path(table))