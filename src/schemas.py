"""Column definitions of the 7 IMDb tables (https://datasets.imdbws.com/)."""
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType,
)

S, I, D, B = StringType(), IntegerType(), DoubleType(), BooleanType()

# Array-like columns (genres, directors, ...) stay comma-separated strings;
# splitting them is part of the cleaning step.
TABLE_COLUMNS = {
    "name.basics": [
        ("nconst", S), ("primaryName", S), ("birthYear", I), ("deathYear", I),
        ("primaryProfession", S), ("knownForTitles", S),
    ],
    "title.akas": [
        ("titleId", S), ("ordering", I), ("title", S), ("region", S),
        ("language", S), ("types", S), ("attributes", S), ("isOriginalTitle", B),
    ],
    "title.basics": [
        ("tconst", S), ("titleType", S), ("primaryTitle", S),
        ("originalTitle", S), ("isAdult", B), ("startYear", I),
        ("endYear", I), ("runtimeMinutes", I), ("genres", S),
    ],
    "title.crew": [("tconst", S), ("directors", S), ("writers", S)],
    "title.episode": [
        ("tconst", S), ("parentTconst", S), ("seasonNumber", I),
        ("episodeNumber", I),
    ],
    "title.principals": [
        ("tconst", S), ("ordering", I), ("nconst", S), ("category", S),
        ("job", S), ("characters", S),
    ],
    "title.ratings": [("tconst", S), ("averageRating", D), ("numVotes", I)],
}