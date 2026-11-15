import os
from pyspark.sql import functions as F
from src.loaders import read_raw
from src.spark_session import get_spark
RELATIONS = [
    ("title.akas", "titleId", "title.basics", "tconst", False),
    ("title.crew", "tconst", "title.basics", "tconst", False),
    ("title.episode", "tconst", "title.basics", "tconst", False),
    ("title.episode", "parentTconst", "title.basics", "tconst", False),
    ("title.ratings", "tconst", "title.basics", "tconst", False),
    ("title.principals", "tconst", "title.basics", "tconst", False),
    ("title.principals", "nconst", "name.basics", "nconst", False),
    ("title.crew", "directors", "name.basics", "nconst", True),
    ("title.crew", "writers", "name.basics", "nconst", True),
    ("name.basics", "knownForTitles", "title.basics", "tconst", True),
]
def present(value):
    """Identify references that are neither missing nor blank."""
    return (
        value.isNotNull()
        & (F.trim(value) != "")
        & (F.trim(value) != "\\N")
    )
def main():
    spark = get_spark("imdb-relation-checks")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set(
        "spark.sql.adaptive.autoBroadcastJoinThreshold", "-1"
    )

    data_dir = os.environ.get("DATA_DIR", "/home/jovyan/data")
    output = f"{data_dir}/quality/relation_checks"
    results = []

    try:
        for table, column, parent_table, parent_key, is_list in RELATIONS:
            label = f"{table}.{column} -> {parent_table}.{parent_key}"
            print(f"Checking relation: {label}", flush=True)
            child = read_raw(spark, table).select(
                F.col(column).alias("source")
            ).filter(present(F.col("source")))

            if is_list:
                references = child.select(
                    F.explode(
                        F.split(F.col("source"), ",")
                    ).alias("reference")
                )
            else:
                references = child.select(
                    F.col("source").alias("reference")
                )

            references = references.select(
                F.trim(F.col("reference")).alias("reference")
            ).filter(present(F.col("reference")))
            reference_counts = references.groupBy(
                "reference"
            ).agg(F.count("*").alias("occurrences"))

            parents = (
                read_raw(spark, parent_table)
                .select(
                    F.trim(F.col(parent_key)).alias("reference")
                )
                .filter(present(F.col("reference")))
                .distinct()
                .withColumn("parent_exists", F.lit(True))
            )

            joined = reference_counts.join(
                parents, on="reference", how="left"
            )
            orphan = F.col("parent_exists").isNull()

            summary = joined.agg(
                F.coalesce(
                    F.sum("occurrences"), F.lit(0)
                ).alias("checked_references"),
                F.coalesce(
                    F.sum(
                        F.when(orphan, F.col("occurrences"))
                        .otherwise(0)
                    ),
                    F.lit(0),
                ).alias("orphan_references"),
                F.count(
                    F.when(orphan, F.lit(1))
                ).alias("distinct_orphan_ids"),
            ).first()

            checked = summary["checked_references"]
            missing = summary["orphan_references"]

            results.append((
                table,
                column,
                parent_table,
                parent_key,
                checked,
                missing,
                summary["distinct_orphan_ids"],
                100.0 * missing / checked if checked else None,
            ))

            print(
                f"  Missing references: {missing} / {checked}",
                flush=True,
            )

            report = spark.createDataFrame(
                results,
                schema=(
                    "child_table string, reference_column string, "
                    "parent_table string, parent_key string, "
                    "checked_references long, orphan_references long, "
                    "distinct_orphan_ids long, orphan_pct double"
                ),
            )

            report.coalesce(1).write.mode("overwrite").option(
                "header", True
            ).csv(output)

        report.orderBy(
            "child_table", "reference_column"
        ).show(100, truncate=False)

        print(f"Results saved to: {output}", flush=True)

    finally:
        spark.stop()
if __name__ == "__main__":
    main()