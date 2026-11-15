"""Exploration questions Q1-Q4, on the clean tables (see quality.clean_all)."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.definitions import (FEATURE_TYPE, HIGH_RATING, LOW_RATING, MIN_VOTES,
                             YEAR_RANGE, Z_THRESHOLD)


def feature_films(basics):
    """Feature films: titleType == "movie" with a known runtime."""
    return basics.filter((F.col("titleType") == FEATURE_TYPE)
                         & F.col("runtimeMinutes").isNotNull())


def q1_runtime_outliers(basics, z=Z_THRESHOLD):
    """Q1. Films whose runtime differs notably from the average of their genre.

    A film with several genres is evaluated once per genre.
    Returns (outliers, genre_stats); outliers = genre, film, runtime, genre
    average, z-score, sorted by |z|.
    """
    g = feature_films(basics).select(
        "tconst", "primaryTitle", "startYear", "runtimeMinutes",
        F.explode("genres").alias("genre"))
    stats = g.groupBy("genre").agg(
        F.count("*").alias("n_films"),
        F.round(F.avg("runtimeMinutes"), 2).alias("genre_avg"),
        F.round(F.stddev("runtimeMinutes"), 2).alias("genre_std"))
    out = (g.join(stats, "genre")
           .withColumn("z_score", F.round((F.col("runtimeMinutes") - F.col("genre_avg"))
                                          / F.col("genre_std"), 2))
           .filter(F.abs("z_score") > z)
           .select("genre", "primaryTitle", "startYear", "runtimeMinutes",
                   "genre_avg", "z_score", "n_films")
           .orderBy(F.desc(F.abs("z_score"))))
    return out, stats.orderBy(F.desc("n_films"))


def genre_year_stats(basics, ratings, years=YEAR_RANGE):
    """Per (year, genre): popularity (sum of numVotes), n_films, avg rating/runtime.

    Only feature films with a rating are used (inner join with ratings).
    """
    films = (feature_films(basics)
             .filter(F.col("startYear").between(*years))
             .join(ratings.select("tconst", "averageRating", "numVotes"), "tconst"))
    g = films.select("startYear", "tconst", "averageRating", "numVotes",
                     "runtimeMinutes", F.explode("genres").alias("genre"))
    return g.groupBy("startYear", "genre").agg(
        F.sum("numVotes").alias("popularity"),
        F.count("*").alias("n_films"),
        F.avg("averageRating").alias("avg_rating"),
        F.avg("runtimeMinutes").alias("avg_runtime"))


def _rank(pop):
    w = Window.partitionBy("startYear").orderBy(F.desc("popularity"), "genre")
    return pop.withColumn("rank", F.row_number().over(w))


def q2_top_genres(pop, top=3):
    """Q2. Top genres per year by popularity (ties broken by genre name)."""
    return (_rank(pop).filter(F.col("rank") <= top)
            .select("startYear", "rank", "genre", "popularity", "n_films")
            .orderBy("startYear", "rank"))


def q3_top_vs_others(pop, top=3):
    """Q3. Per year: top genres vs the other genres (votes share, films, rating,
    runtime). Averages are weighted by the number of films of each genre."""
    ranked = _rank(pop).withColumn(
        "group", F.when(F.col("rank") <= top, "top3").otherwise("others"))
    agg = ranked.groupBy("startYear", "group").agg(
        F.sum("popularity").alias("votes"),
        F.sum("n_films").alias("n_films"),
        (F.sum(F.col("avg_rating") * F.col("n_films")) / F.sum("n_films")).alias("avg_rating"),
        (F.sum(F.col("avg_runtime") * F.col("n_films")) / F.sum("n_films")).alias("avg_runtime"))
    w = Window.partitionBy("startYear")
    return (agg.withColumn("votes_share", F.round(F.col("votes") / F.sum("votes").over(w), 4))
            .withColumn("avg_rating", F.round("avg_rating", 2))
            .withColumn("avg_runtime", F.round("avg_runtime", 1))
            .orderBy("startYear", "group"))


def q3_genre_frequency(top3):
    """How many years each genre appears in the top 3 (output of q2)."""
    return top3.groupBy("genre").agg(F.count("*").alias("years_in_top3")) \
        .orderBy(F.desc("years_in_top3"))


def q4_directors(basics, ratings, crew, names, min_votes=MIN_VOTES,
                 high=HIGH_RATING, low=LOW_RATING):
    """Q4. Directors with exactly one film rated >= high and at least two other
    films whose average rating is <= low.

    Only feature films with at least `min_votes` votes are counted (unreliable
    ratings are ignored). "Other films" = all the other counted films.
    """
    films = basics.filter(F.col("titleType") == FEATURE_TYPE) \
        .select("tconst", "primaryTitle")
    rated = ratings.filter(F.col("numVotes") >= min_votes) \
        .select("tconst", "averageRating")
    d = crew.select("tconst", F.explode("directors").alias("nconst"))
    j = d.join(films, "tconst").join(rated, "tconst")
    r = F.col("averageRating")
    agg = j.groupBy("nconst").agg(
        F.count("*").alias("n_films"),
        F.sum(F.when(r >= high, 1).otherwise(0)).alias("n_high"),
        F.sum(F.when(r >= high, r).otherwise(0.0)).alias("high_sum"),
        F.sum(r).alias("rating_sum"),
        F.max_by("primaryTitle", F.when(r >= high, r)).alias("high_film"))
    out = (agg.filter(F.col("n_high") == 1)
           .withColumn("n_other_films", F.col("n_films") - 1)
           .filter(F.col("n_other_films") >= 2)
           .withColumn("others_avg",
                       F.round((F.col("rating_sum") - F.col("high_sum"))
                               / F.col("n_other_films"), 2))
           .filter(F.col("others_avg") <= low)
           .join(names.select("nconst", "primaryName"), "nconst", "left")
           .select("primaryName", "high_film",
                   F.round(F.col("high_sum"), 1).alias("high_rating"),
                   "n_other_films", "others_avg")
           .orderBy("others_avg", "primaryName"))
    return out
