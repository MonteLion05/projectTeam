"""Clean IMDb values and resume previously completed tables."""

import os
import argparse
import json
from pathlib import Path
from src.integrity import fingerprint

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.loaders import read_typed
from src.schemas import TABLE_COLUMNS
from src.spark_session import get_spark


SUMMARY_SCHEMA = (
    "table string, total_rows long, flagged_rows long"
)

ARRAY_COLUMNS = {
    "name.basics": ["primaryProfession", "knownForTitles"],
    "title.basics": ["genres"],
    "title.crew": ["directors", "writers"],
}


def value_rules(table):
    """Return column, condition, flag, and repair decision."""
    if table == "name.basics":
        return [
            (
                "deathYear",
                F.col("deathYear") < F.col("birthYear"),
                "death_before_birth",
                True,
            ),
            (
                "birthYear",
                F.col("birthYear") <= 0,
                "birth_year_nonpositive",
                True,
            ),
            (
                "deathYear",
                F.col("deathYear") <= 0,
                "death_year_nonpositive",
                True,
            ),
        ]

    if table == "title.basics":
        return [
            (
                "runtimeMinutes",
                F.col("runtimeMinutes") <= 0,
                "runtime_nonpositive",
                True,
            ),
            (
                "endYear",
                F.col("endYear") < F.col("startYear"),
                "end_before_start",
                True,
            ),
            (
                "startYear",
                F.col("startYear") <= 0,
                "start_year_nonpositive",
                True,
            ),
            (
                "endYear",
                F.col("endYear") <= 0,
                "end_year_nonpositive",
                True,
            ),
        ]

    if table == "title.ratings":
        return [
            (
                "averageRating",
                (F.col("averageRating") < 1)
                | (F.col("averageRating") > 10),
                "rating_out_of_range",
                True,
            ),
            (
                "numVotes",
                F.col("numVotes") < 0,
                "votes_negative",
                True,
            ),
        ]

    if table == "title.episode":
        return [
            (
                "seasonNumber",
                F.col("seasonNumber") < 0,
                "season_negative",
                True,
            ),
            (
                "episodeNumber",
                F.col("episodeNumber") < 0,
                "episode_negative",
                True,
            ),
            (
                "episodeNumber",
                F.col("episodeNumber") == 0,
                "episode_zero_review",
                False,
            ),
        ]

    return []


def save_summary(spark, summaries, output):
    """Save the small progress summary."""
    report = spark.createDataFrame(
        summaries,
        schema=SUMMARY_SCHEMA,
    ).orderBy("table")

    (
        report.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(output)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tables', nargs='+', choices=list(TABLE_COLUMNS), default=list(TABLE_COLUMNS))
    parser.add_argument('--force', action='store_true', help='Recompute selected tables')
    args = parser.parse_args()
    spark = get_spark("imdb-clean-values")
    spark.sparkContext.setLogLevel("WARN")

    data_dir = os.environ.get(
        "DATA_DIR", "/home/jovyan/data"
    )
    summary_path = (
        f"{data_dir}/quality/value_cleaning_summary"
    )
    summaries = []

    try:
        # Read only the small summary into Python.
        if os.path.isfile(f"{summary_path}/_SUCCESS"):
            previous = (
                spark.read
                .option("header", True)
                .schema(SUMMARY_SCHEMA)
                .csv(summary_path)
            )

            summaries = [
                (
                    row["table"],
                    row["total_rows"],
                    row["flagged_rows"],
                )
                for row in previous.collect()
            ]

        completed_tables = {
            row[0] for row in summaries
        }

        for table in args.tables:
            source_path = (f"{data_dir}/parquet/raw/{table}" if os.environ.get('INPUT_FORMAT', 'tsv') == 'parquet'
                           else f"{data_dir}/raw/{table}.tsv.gz")
            source_hash = fingerprint(source_path)
            rules_hash = fingerprint(Path(__file__))
            marker_path = Path(data_dir)/'quality'/'clean_checkpoints'/f'{table}.json'
            marker = json.loads(marker_path.read_text()) if marker_path.exists() else {}
            staged_path = (
                f"{data_dir}/parquet/staged/{table}"
            )
            audit_path = (
                f"{data_dir}/quality/value_audit/{table}"
            )

            completed = (
                not args.force
                and marker.get('source_hash') == source_hash
                and marker.get('rules_hash') == rules_hash
                and table in completed_tables
                and os.path.isfile(
                    f"{staged_path}/_SUCCESS"
                )
                and os.path.isfile(
                    f"{audit_path}/_SUCCESS"
                )
            )

            if completed:
                completed = (marker.get('staged_hash') == fingerprint(staged_path)
                             and marker.get('audit_hash') == fingerprint(audit_path))
            if completed:
                print(
                    f"Already completed: {table}",
                    flush=True,
                )
                continue

            # Remove any stale summary for this table.
            summaries = [
                row for row in summaries
                if row[0] != table
            ]

            print(
                f"Cleaning values: {table}",
                flush=True,
            )

            original = read_typed(spark, table)
            rules = value_rules(table)

            if rules:
                flags = F.array(*[
                    F.when(condition, F.lit(label))
                    for _, condition, label, _ in rules
                ]).cast("array<string>")
            else:
                flags = F.array().cast("array<string>")

            flagged = original.withColumn(
                "dq_flags",
                F.filter(
                    flags,
                    lambda item: item.isNotNull(),
                ),
            ).persist(StorageLevel.DISK_ONLY)

            try:
                counts = flagged.agg(
                    F.count("*").alias("total_rows"),
                    F.count(
                        F.when(
                            F.size("dq_flags") > 0,
                            F.lit(1),
                        )
                    ).alias("flagged_rows"),
                ).first()

                # Preserve typed values before repairs.
                (
                    flagged.filter(
                        F.size("dq_flags") > 0
                    )
                    .write.mode("overwrite")
                    .parquet(audit_path)
                )

                cleaned = flagged

                # Use flags calculated before modifications.
                for column, _, label, replace in rules:
                    if replace:
                        cleaned = cleaned.withColumn(
                            column,
                            F.when(
                                F.array_contains(
                                    "dq_flags", label
                                ),
                                F.lit(None),
                            ).otherwise(F.col(column)),
                        )

                for column in ARRAY_COLUMNS.get(table, []):
                    cleaned = cleaned.withColumn(
                        column,
                        F.transform(
                            F.split(F.col(column), ","),
                            lambda item: F.trim(item),
                        ),
                    )

                (
                    cleaned.write
                    .mode("overwrite")
                    .parquet(staged_path)
                )

                # Full-column readback detects corrupt Parquet pages before marking completion.
                restored = spark.read.parquet(staged_path)
                checks = [F.count('*').alias('rows')] + [
                    F.sum(F.length(F.coalesce(F.col(c).cast('string'), F.lit('')))).alias(f'c{i}')
                    for i,c in enumerate(cleaned.columns)]
                before = cleaned.agg(*checks).first().asDict()
                after = restored.agg(*checks).first().asDict()
                if before != after:
                    raise RuntimeError(f'Readback metrics differ for {table}')
                marker_path.parent.mkdir(parents=True, exist_ok=True)
                marker_path.write_text(json.dumps({
                    'source_hash': source_hash, 'rules_hash': rules_hash,
                    'staged_hash': fingerprint(staged_path), 'audit_hash': fingerprint(audit_path),
                    'readback_metrics': after}, indent=2))
                summaries.append((
                    table,
                    counts["total_rows"],
                    counts["flagged_rows"],
                ))

                save_summary(
                    spark, summaries, summary_path
                )

                print(
                    f"Saved {table}: "
                    f"{counts['total_rows']} rows, "
                    f"{counts['flagged_rows']} flagged",
                    flush=True,
                )

            finally:
                flagged.unpersist(blocking=True)

        # Also works when every table was already completed.
        spark.createDataFrame(
            summaries,
            schema=SUMMARY_SCHEMA,
        ).orderBy("table").show(truncate=False)

        print(
            "Value cleaning completed.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()