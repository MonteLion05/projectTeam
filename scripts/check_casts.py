import os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.loaders import read_raw
from src.schemas import TABLE_COLUMNS
from src.spark_session import get_spark
def main():
    spark = get_spark("imdb-cast-checks")
    spark.sparkContext.setLogLevel("WARN")
    results = []
    try:
        for table, columns in TABLE_COLUMNS.items():
            typed_columns = [
                (name, dtype)
                for name, dtype in columns
                if not isinstance(dtype, StringType)
            ]
            if not typed_columns:
                continue
            print(f"Checking conversions: {table}", flush=True)
            raw = read_raw(spark, table)
            expressions = [
                F.count(F.lit(1)).alias("total_rows")
            ]
            for name, dtype in typed_columns:
                value = F.col(name)
                text = F.trim(value)
                present = (
                    value.isNotNull()
                    & (text != "\\N")
                    & (text != "")
                )
                invalid = present & value.try_cast(dtype).isNull()
                expressions.append(
                    F.count(
                        F.when(invalid, F.lit(1))
                    ).alias(name)
                )
            summary = raw.agg(*expressions).first()
            total = summary["total_rows"]
            for name, dtype in typed_columns:
                invalid_count = summary[name]
                results.append((
                    table,
                    name,
                    dtype.simpleString(),
                    total,
                    invalid_count,
                    (
                        100.0 * invalid_count / total
                        if total else None
                    ),
                ))
        report = spark.createDataFrame(
            results,
            schema=(
                "table string, column string, target_type string, "
                "total_rows long, invalid_count long, "
                "invalid_pct double"
            ),
        ).orderBy("table", "column")
        data_dir = os.environ.get(
            "DATA_DIR", "/home/jovyan/data"
        )
        output = f"{data_dir}/quality/cast_checks"
        report.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(output)
        report.show(100, truncate=False)
        print(f"Results saved to: {output}", flush=True)
    finally:
        spark.stop()
if __name__ == "__main__":
    main()