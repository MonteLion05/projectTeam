from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
PROFILE_SCHEMA = StructType([
    StructField("table", StringType(), False),
    StructField("column", StringType(), False),
    StructField("total_rows", LongType(), False),
    StructField("n_null", LongType(), False),
    StructField("n_backslash_n", LongType(), False),
    StructField("n_empty", LongType(), False),
    StructField("n_missing", LongType(), False),
    StructField("missing_pct", DoubleType(), True),
])
def missing_profile(df, table_name):
    """Count nulls, IMDb missing tokens, and blank strings per column."""
    expressions = [F.count(F.lit(1)).alias("total_rows")]
    for index, column_name in enumerate(df.columns):
        value = F.col(column_name)
        text = F.trim(value.cast("string"))
        conditions = {
            "null": value.isNull(),
            "token": text == "\\N",
            "empty": text == "",
        }
        for label, condition in conditions.items():
            expressions.append(
                F.count(
                    F.when(condition, F.lit(1))
                ).alias(f"c{index}_{label}")
            )

    summary = df.agg(*expressions).first()
    total = summary["total_rows"]
    rows = []
    for index, column_name in enumerate(df.columns):
        n_null = summary[f"c{index}_null"]
        n_token = summary[f"c{index}_token"]
        n_empty = summary[f"c{index}_empty"]
        n_missing = n_null + n_token + n_empty
        percentage = (
            100.0 * n_missing / total if total else None
        )
        rows.append((
            table_name,
            column_name,
            total,
            n_null,
            n_token,
            n_empty,
            n_missing,
            percentage,
        ))
    return df.sparkSession.createDataFrame(rows, PROFILE_SCHEMA)