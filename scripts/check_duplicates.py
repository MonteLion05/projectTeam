import os
from functools import reduce
from pyspark.sql import functions as F
from src.loaders import read_raw
from src.spark_session import get_spark
KEYS = {
    "name.basics": ["nconst"],
    "title.basics": ["tconst"],
    "title.crew": ["tconst"],
    "title.episode": ["tconst"],
    "title.ratings": ["tconst"],
    "title.akas": ["titleId", "ordering"],
    "title.principals": ["tconst", "ordering"],
}
def main():
    spark = get_spark("imdb-key-checks")
    spark.sparkContext.setLogLevel("WARN")
    results = []
    try:
        for table, keys in KEYS.items():
            print(f"Checking keys: {table}", flush=True)
            df = read_raw(spark, table).select(*keys)
            missing_conditions = [
                F.col(key).isNull()
                | (F.trim(F.col(key)) == "")
                | (F.trim(F.col(key)) == "\\N")
                for key in keys
            ]
            missing_key = reduce(
                lambda left, right: left | right,
                missing_conditions,
            )
            grouped = df.groupBy(*keys).count()
            valid_key = ~missing_key
            duplicate = valid_key & (F.col("count") > 1)
            summary = grouped.agg(
                F.coalesce(
                    F.sum("count"), F.lit(0)
                ).alias("total_rows"),
                F.coalesce(
                    F.sum(
                        F.when(missing_key, F.col("count"))
                        .otherwise(0)
                    ),
                    F.lit(0),
                ).alias("missing_key_rows"),
                F.count(
                    F.when(duplicate, F.lit(1))
                ).alias("duplicate_key_groups"),
                F.coalesce(
                    F.sum(
                        F.when(duplicate, F.col("count") - 1)
                        .otherwise(0)
                    ),
                    F.lit(0),
                ).alias("extra_duplicate_rows"),
            ).first()
            results.append((
                table,
                ", ".join(keys),
                summary["total_rows"],
                summary["missing_key_rows"],
                summary["duplicate_key_groups"],
                summary["extra_duplicate_rows"],
            ))
        report = spark.createDataFrame(
            results,
            schema=(
                "table string, key_columns string, "
                "total_rows long, missing_key_rows long, "
                "duplicate_key_groups long, "
                "extra_duplicate_rows long"
            ),
        ).orderBy("table")
        data_dir = os.environ.get(
            "DATA_DIR", "/home/jovyan/data"
        )
        output = f"{data_dir}/quality/key_checks"
        report.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(output)
        report.show(100, truncate=False)
        print(f"Results saved to: {output}", flush=True)
    finally:
        spark.stop()
if __name__ == "__main__":
    main()