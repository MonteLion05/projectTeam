import os
from functools import reduce
from pyspark.sql import functions as F
from src.loaders import read_raw
from src.quality import missing_profile
from src.schemas import TABLE_COLUMNS
from src.spark_session import get_spark
def main():
    spark = get_spark("imdb-missing-profile")
    spark.sparkContext.setLogLevel("WARN")
    data_dir = os.environ.get("DATA_DIR", "/home/jovyan/data")
    output_dir = f"{data_dir}/quality"
    try:
        profiles = []
        for table in TABLE_COLUMNS:
            print(f"Profiling: {table}", flush=True)
            raw = read_raw(spark, table)
            profiles.append(missing_profile(raw, table))
        profile = reduce(
            lambda left, right: left.unionByName(right),
            profiles,
        )
        ranked = profile.orderBy(
            F.col("missing_pct").desc_nulls_last(),
            F.col("table").asc(),
            F.col("column").asc(),
        )
        top_five = ranked.limit(5)
        ranked.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(f"{output_dir}/missing_profile")
        top_five.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(f"{output_dir}/top_five_missing")
        print("\nMissing-value profile:", flush=True)
        ranked.show(100, truncate=False)
        print("\nTop five columns:", flush=True)
        top_five.show(truncate=False)
        print(f"\nResults saved to: {output_dir}", flush=True)
    finally:
        spark.stop()
if __name__ == "__main__":
    main()