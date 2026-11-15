import os
from pyspark.sql import functions as F
from src.loaders import read_typed
from src.spark_session import get_spark
def main():
    spark = get_spark("imdb-value-checks")
    spark.sparkContext.setLogLevel("WARN")
    checks = {
        "name.basics": {
            "death_before_birth": (
                F.col("deathYear") < F.col("birthYear")
            ),
            "birth_year_nonpositive": F.col("birthYear") <= 0,
            "death_year_nonpositive": F.col("deathYear") <= 0,
        },
        "title.basics": {
            "runtime_nonpositive": F.col("runtimeMinutes") <= 0,
            "end_before_start": (
                F.col("endYear") < F.col("startYear")
            ),
            "start_year_nonpositive": F.col("startYear") <= 0,
            "end_year_nonpositive": F.col("endYear") <= 0,
        },
        "title.ratings": {
            "rating_out_of_range": (
                (F.col("averageRating") < 1)
                | (F.col("averageRating") > 10)
            ),
            "votes_negative": F.col("numVotes") < 0,
        },
        "title.episode": {
            "season_negative": F.col("seasonNumber") < 0,
            "episode_nonpositive": F.col("episodeNumber") <= 0,
        },
    }
    results = []
    try:
        for table, rules in checks.items():
            print(f"Checking: {table}", flush=True)
            df = read_typed(spark, table)
            expressions = [
                F.count(F.lit(1)).alias("total_rows")
            ]
            for name, condition in rules.items():
                expressions.append(
                    F.count(
                        F.when(condition, F.lit(1))
                    ).alias(name)
                )
            summary = df.agg(*expressions).first()
            total = summary["total_rows"]

            for name in rules:
                count = summary[name]
                percentage = (
                    100.0 * count / total if total else None
                )
                results.append((
                    table, name, total, count, percentage
                ))
        report = spark.createDataFrame(
            results,
            schema=(
                "table string, check_name string, "
                "total_rows long, flagged_rows long, "
                "flagged_pct double"
            ),
        ).orderBy("table", "check_name")

        data_dir = os.environ.get(
            "DATA_DIR", "/home/jovyan/data"
        )
        output = f"{data_dir}/quality/value_checks"
        report.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(output)
        report.show(100, truncate=False)
        print(f"Results saved to: {output}", flush=True)
    finally:
        spark.stop()
if __name__ == "__main__":
    main()