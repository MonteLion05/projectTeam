"""Data quality: profiling, checks and cleaning of the IMDb tables.

Raw tables (all columns are strings, "\\N" = missing) are read with
`loaders.read_raw`. Casts use try_cast because Spark 4 runs in ANSI mode by
default: a plain cast of an invalid string would raise an error.
"""
import os
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from src.definitions import (ARRAY_REFERENCES, CASTS, FOREIGN_KEYS, KEYS,
                             LIST_COLUMNS, MAX_YEAR, MIN_YEAR, NULLING,
                             SERIES_TYPES)

DATA_DIR = os.environ.get("DATA_DIR", "/home/jovyan/data")
PARQUET_DIR = f"{DATA_DIR}/parquet"

CLEAN_ORDER = ["title.basics", "name.basics", "title.ratings", "title.crew",
               "title.episode", "title.akas", "title.principals"]


# ---------------------------------------------------------------- normalise
def _normalised(col_name):
    """Trimmed string column, with "\\N" and empty strings turned into null."""
    c = F.trim(F.col(col_name))
    return F.when(c.isin("\\N", ""), F.lit(None)).otherwise(c)


def normalize(df: DataFrame, table: str) -> DataFrame:
    """Raw strings -> typed columns, missing markers -> null, lists -> arrays."""
    df = df.select([_normalised(c).alias(c) for c in df.columns])
    for col, typ in CASTS.get(table, {}).items():
        df = df.withColumn(col, F.col(col).try_cast(typ))
    for col in LIST_COLUMNS.get(table, []):
        df = df.withColumn(col, F.split(F.col(col), ","))
    return df


# ---------------------------------------------------------------- profiling
def missing_profile(df: DataFrame, table_name: str) -> DataFrame:
    """Missing values per column, computed in a single pass over `df` (raw).

    Columns: table, column, total_rows, n_null (real null), n_backslash_n
    ("\\N"), n_empty (empty or spaces), n_missing (sum of the three) and
    missing_pct = n_missing / total_rows * 100.
    """
    aggs = [F.count(F.lit(1)).alias("__total")]
    for c in df.columns:
        col = F.col(c)
        aggs += [
            F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(f"{c}__null"),
            F.sum(F.when(col == "\\N", 1).otherwise(0)).alias(f"{c}__bs"),
            F.sum(F.when(col.isNotNull() & (F.trim(col) == ""), 1)
                  .otherwise(0)).alias(f"{c}__empty"),
        ]
    r = df.agg(*aggs).first()
    total = int(r["__total"])
    rows = []
    for c in df.columns:
        n_null, n_bs, n_empty = (int(r[f"{c}__null"] or 0),
                                 int(r[f"{c}__bs"] or 0),
                                 int(r[f"{c}__empty"] or 0))
        n_missing = n_null + n_bs + n_empty
        pct = round(100.0 * n_missing / total, 4) if total else 0.0
        rows.append((table_name, c, total, n_null, n_bs, n_empty, n_missing, pct))
    schema = ("table string, column string, total_rows long, n_null long, "
              "n_backslash_n long, n_empty long, n_missing long, missing_pct double")
    return df.sparkSession.createDataFrame(rows, schema)


def missing_report(spark, read_raw, tables=CLEAN_ORDER) -> DataFrame:
    """Profile of all tables, united and sorted by missing_pct (desc)."""
    parts = [missing_profile(read_raw(spark, t), t) for t in tables]
    out = reduce(lambda a, b: a.unionByName(b), parts)
    return out.orderBy(F.desc("missing_pct"), "table", "column")


def top_missing(profile: DataFrame, n: int = 5) -> DataFrame:
    """The n columns with the highest missing_pct (ties: table, column order)."""
    return (profile.select("table", "column", "missing_pct")
            .orderBy(F.desc("missing_pct"), "table", "column").limit(n))


# ------------------------------------------------------------------- rules
def rules(table: str) -> dict:
    """flag name -> boolean Column, on a normalised (typed) table."""
    c = F.col
    return {
        "name.basics": {
            "death_before_birth": c("deathYear") < c("birthYear"),
            "birth_year_out_of_range": ~c("birthYear").between(MIN_YEAR, MAX_YEAR),
            "death_year_out_of_range": ~c("deathYear").between(MIN_YEAR, MAX_YEAR),
        },
        "title.basics": {
            "runtime_not_positive": c("runtimeMinutes") <= 0,
            "start_after_end": c("startYear") > c("endYear"),
            "start_year_in_future": c("startYear") > MAX_YEAR,
            "start_year_too_old": c("startYear") < MIN_YEAR,
            "is_adult_invalid": ~c("isAdult").isin(0, 1),
            "end_year_on_non_series": c("endYear").isNotNull()
            & ~c("titleType").isin(*SERIES_TYPES),
        },
        "title.ratings": {
            "rating_out_of_range": ~c("averageRating").between(1, 10),
            "negative_votes": c("numVotes") < 0,
        },
        "title.episode": {
            "season_without_episode": c("seasonNumber").isNotNull()
            & c("episodeNumber").isNull(),
        },
    }.get(table, {})


# ------------------------------------------------------------------ checks
def _agg_checks(df, table, conds, category):
    """One pass: count rows matching each condition."""
    aggs = [F.count(F.lit(1)).alias("__total")]
    aggs += [F.sum(F.when(cond, 1).otherwise(0)).alias(name)
             for name, cond in conds.items()]
    r = df.agg(*aggs).first()
    total = int(r["__total"])
    return [(category, name, table, int(r[name] or 0),
             round(100.0 * (r[name] or 0) / total, 4) if total else 0.0)
            for name in conds]


def run_checks(spark, read_raw, tables=CLEAN_ORDER) -> DataFrame:
    """All quality checks of step 3. Columns: category, check, table, n_rows, pct.

    pct is relative to the rows of the table (for cross-table checks, to the
    rows that could be compared, see the check name).
    """
    raw = {t: read_raw(spark, t) for t in tables}
    typed = {t: normalize(raw[t], t) for t in tables}
    rows = []

    # impossible values / life dates / type coherence (single table rules)
    for t in tables:
        r = rules(t)
        if r:
            cat = {"name.basics": "dates", "title.basics": "impossible values",
                   "title.ratings": "impossible values",
                   "title.episode": "type coherence"}[t]
            rows += _agg_checks(typed[t], t, r, cat)

    # malformed rows: non-empty value that becomes null at cast time
    for t in tables:
        casts = CASTS.get(t, {})
        if casts:
            conds = {f"cast_failure_{c}": _normalised(c).isNotNull()
                     & _normalised(c).try_cast(ty).isNull()
                     for c, ty in casts.items()}
            rows += _agg_checks(raw[t], t, conds, "malformed rows")

    # duplicates on the keys
    for t in tables:
        keys = KEYS[t]
        total = typed[t].count()
        distinct = typed[t].select(*keys).distinct().count()
        extra = total - distinct
        rows.append(("duplicates", f"duplicated_key_{'+'.join(keys)}", t, extra,
                     round(100.0 * extra / total, 4) if total else 0.0))

    # orphan foreign keys (rows whose reference has no parent)
    for t, fks in FOREIGN_KEYS.items():
        if t not in typed:
            continue
        total = typed[t].count()
        for col, parent, pcol in fks:
            parent_keys = typed[parent].select(F.col(pcol).alias("__pk")).distinct()
            n = (typed[t].filter(F.col(col).isNotNull())
                 .join(parent_keys, F.col(col) == F.col("__pk"), "left_anti")
                 .count())
            rows.append(("orphan foreign keys", f"{col}_not_in_{parent}.{pcol}", t,
                         n, round(100.0 * n / total, 4) if total else 0.0))

    # orphan references stored in lists (rows kept, only measured)
    for t, col, parent, pcol in ARRAY_REFERENCES:
        parent_keys = typed[parent].select(F.col(pcol).alias("__pk")).distinct()
        exploded = (typed[t].select(F.explode(col).alias("__ref"))
                    .filter(F.col("__ref").isNotNull()))
        total = exploded.count()
        n = exploded.join(parent_keys, F.col("__ref") == F.col("__pk"),
                          "left_anti").count()
        rows.append(("orphan foreign keys", f"{col}_values_not_in_{parent}.{pcol}",
                     t, n, round(100.0 * n / total, 4) if total else 0.0))

    # person / film coherence (principals + basics + name.basics)
    p = typed["title.principals"].select("tconst", "nconst")
    b = typed["title.basics"].select("tconst", "startYear").filter(
        F.col("startYear").isNotNull())
    n_ = typed["name.basics"].select("nconst", "birthYear", "deathYear").filter(
        F.col("birthYear").isNotNull())
    j = p.join(b, "tconst").join(n_, "nconst")
    rows += _agg_checks(
        j, "principals+basics+name.basics",
        {"release_before_birth (compared rows: both years known)":
         F.col("startYear") < F.col("birthYear"),
         "release_after_death (posthumous, legitimate, info only)":
         F.col("startYear") > F.col("deathYear")},
        "person/film coherence")

    schema = "category string, check string, table string, n_rows long, pct double"
    return spark.createDataFrame(rows, schema).orderBy("category", "table", "check")


# ---------------------------------------------------------------- cleaning
def apply_rules(df: DataFrame, table: str) -> DataFrame:
    """Add dq_flags (array<string>) and null the impossible values."""
    r = rules(table)
    nulling = NULLING.get(table, {})
    if r:
        flags = F.array_compact(F.array(*[F.when(cond, F.lit(name))
                                          for name, cond in r.items()]))
    else:
        flags = F.array().cast("array<string>")
    cols = []
    for col in df.columns:
        conds = [r[flag] for flag, target in nulling.items() if target == col]
        if conds:
            cols.append(F.when(reduce(or_, conds), F.lit(None))
                        .otherwise(F.col(col)).alias(col))
        else:
            cols.append(F.col(col))
    return df.select(*cols, flags.alias("dq_flags"))


def _split_orphans(spark, df, table):
    """Return (kept, quarantine) for the foreign keys of `table`."""
    fks = FOREIGN_KEYS.get(table, [])
    if not fks:
        return df, None, None
    joined, reasons = df, []
    for i, (col, parent, pcol) in enumerate(fks):
        pk = spark.read.parquet(f"{PARQUET_DIR}/clean/{parent}") \
            .select(F.col(pcol).alias(f"__pk{i}")).distinct() \
            .withColumn(f"__hit{i}", F.lit(True))
        joined = joined.join(pk, F.col(col) == F.col(f"__pk{i}"), "left")
        reasons.append(F.when(F.col(col).isNotNull() & F.col(f"__hit{i}").isNull(),
                              F.lit(f"orphan_{col}")))
    helpers = [f"__pk{i}" for i in range(len(fks))] + \
              [f"__hit{i}" for i in range(len(fks))]
    joined = joined.withColumn("__reasons", F.array_compact(F.array(*reasons)))
    joined = joined.persist(StorageLevel.DISK_ONLY)
    kept = joined.filter(F.size("__reasons") == 0).drop("__reasons", *helpers)
    quarantine = (joined.filter(F.size("__reasons") > 0)
                  .withColumn("quarantine_reason", F.concat_ws(",", "__reasons"))
                  .drop("__reasons", *helpers))
    return kept, quarantine, joined


def clean_table(spark, read_raw, table: str) -> dict:
    """Clean one table and write clean/ and quarantine/ Parquet. Returns stats.

    Order: normalise -> flags + null impossible values -> drop rows with null
    key and duplicated keys -> quarantine orphan foreign keys.
    Parents (title.basics, name.basics) must be cleaned first.
    """
    raw = read_raw(spark, table)
    rows_before = raw.count()
    df = apply_rules(normalize(raw, table), table)
    df = df.dropna(subset=KEYS[table]).dropDuplicates(KEYS[table])
    kept, quarantine, cached = _split_orphans(spark, df, table)
    kept.write.mode("overwrite").parquet(f"{PARQUET_DIR}/clean/{table}")
    n_quarantine = 0
    if quarantine is not None:
        quarantine.write.mode("overwrite").parquet(f"{PARQUET_DIR}/quarantine/{table}")
        n_quarantine = spark.read.parquet(f"{PARQUET_DIR}/quarantine/{table}").count()
    if cached is not None:
        cached.unpersist()
    clean = spark.read.parquet(f"{PARQUET_DIR}/clean/{table}")
    rows_clean = clean.count()
    flagged = clean.filter(F.size("dq_flags") > 0).count()
    return {"table": table, "rows_before": rows_before,
            "dropped_null_or_duplicate_key": rows_before - rows_clean - n_quarantine,
            "quarantined_orphans": n_quarantine, "rows_clean": rows_clean,
            "rows_with_dq_flags": flagged}


def clean_all(spark, read_raw, tables=CLEAN_ORDER) -> DataFrame:
    """Clean all tables (parents first) and return the before/after counts."""
    stats = []
    for t in tables:
        print(f"Cleaning {t}...", flush=True)
        stats.append(clean_table(spark, read_raw, t))
    schema = ("table string, rows_before long, dropped_null_or_duplicate_key long, "
              "quarantined_orphans long, rows_clean long, rows_with_dq_flags long")
    return spark.createDataFrame(
        [tuple(s[k] for k in ["table", "rows_before", "dropped_null_or_duplicate_key",
                              "quarantined_orphans", "rows_clean",
                              "rows_with_dq_flags"]) for s in stats], schema)
