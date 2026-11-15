"""Shared definitions and thresholds (one place to change a choice)."""

# --- Data preparation -------------------------------------------------------
MIN_YEAR, MAX_YEAR = 1800, 2026          # plausible range for years
SERIES_TYPES = ["tvSeries", "tvMiniSeries"]

KEYS = {
    "name.basics": ["nconst"],
    "title.akas": ["titleId", "ordering"],
    "title.basics": ["tconst"],
    "title.crew": ["tconst"],
    "title.episode": ["tconst"],
    "title.principals": ["tconst", "ordering"],
    "title.ratings": ["tconst"],
}

# column -> target type (raw columns are strings, "\N" means missing)
CASTS = {
    "name.basics": {"birthYear": "int", "deathYear": "int"},
    "title.akas": {"ordering": "int", "isOriginalTitle": "int"},
    "title.basics": {"isAdult": "int", "startYear": "int", "endYear": "int",
                     "runtimeMinutes": "int"},
    "title.episode": {"seasonNumber": "int", "episodeNumber": "int"},
    "title.principals": {"ordering": "int"},
    "title.ratings": {"averageRating": "double", "numVotes": "long"},
}

# comma separated lists split into array<string>
LIST_COLUMNS = {
    "name.basics": ["primaryProfession", "knownForTitles"],
    "title.basics": ["genres"],
    "title.crew": ["directors", "writers"],
}

# (column in table, parent table, parent column)
FOREIGN_KEYS = {
    "title.principals": [("tconst", "title.basics", "tconst"),
                         ("nconst", "name.basics", "nconst")],
    "title.ratings": [("tconst", "title.basics", "tconst")],
    "title.crew": [("tconst", "title.basics", "tconst")],
    "title.akas": [("titleId", "title.basics", "tconst")],
    "title.episode": [("tconst", "title.basics", "tconst"),
                      ("parentTconst", "title.basics", "tconst")],
}

# array columns that reference another table (measured only, rows are kept)
ARRAY_REFERENCES = [
    ("title.crew", "directors", "name.basics", "nconst"),
    ("title.crew", "writers", "name.basics", "nconst"),
    ("name.basics", "knownForTitles", "title.basics", "tconst"),
]

# flag -> column set to null when the flag is raised
NULLING = {
    "name.basics": {"death_before_birth": "deathYear",
                    "birth_year_out_of_range": "birthYear",
                    "death_year_out_of_range": "deathYear"},
    "title.basics": {"runtime_not_positive": "runtimeMinutes",
                     "start_after_end": "endYear",
                     "start_year_in_future": "startYear",
                     "start_year_too_old": "startYear",
                     "is_adult_invalid": "isAdult",
                     "end_year_on_non_series": "endYear"},
    "title.ratings": {"rating_out_of_range": "averageRating",
                      "negative_votes": "numVotes"},
}

# --- Exploration ------------------------------------------------------------
FEATURE_TYPE = "movie"       # feature_films = titleType == "movie"
Z_THRESHOLD = 2.0            # Q1: |z-score| above this = notably different
MIN_VOTES = 1000             # Q4: minimum votes for a rating to be reliable
YEAR_RANGE = (1980, 2025)    # Q2/Q3
HIGH_RATING = 8.0            # Q4
LOW_RATING = 6.0             # Q4
