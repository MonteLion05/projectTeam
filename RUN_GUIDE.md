# Execution guide for the continued project

This extends the supplied projectTeam-code.zip. Existing quality scripts, schema, group README and REPORT template are retained. Read VALIDATION.md before claiming results.

## Runtime and data

The original `pyspark-notebook` and `kafka` services remain. The existing Jupyter image is not replaced.
Paths in `.env` select the host data, results and temporary directories. Container paths remain `/home/jovyan/data`, `/home/jovyan/results`, `/spark-tmp`.
`INPUT_FORMAT=tsv` is now the safe default because the previous Parquet copies were suspect. `INPUT_FORMAT=parquet` explicitly enables raw Parquet after verification.
The Kafka connector is derived from the installed PySpark version (Scala 2.13); `KAFKA_PACKAGE` can override it if required by the image. A matching published connector must exist. Local tests used Spark 4.0.1, not the user's 4.2.0 runtime.

## Existing preparation commands

Use the existing `scripts/profile_data.py`, `check_casts.py`, `check_duplicates.py`, `check_values.py`, `check_relations.py`, and `clean_values.py`.
The PowerShell runner executes them with the already working `docker compose exec` approach:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step Quality
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step Clean
```

Clean outputs remain `data/parquet/staged/<table>` and `data/quality/value_audit/<table>`.
New checkpoints include source/rules/output fingerprints and full-column readback metrics. These detect changed files and decoding errors, not every possible semantic discrepancy. Incomplete checkpoints are recomputed. `--tables title.episode` limits work; `--force` rebuilds selected tables.
No files are deleted by unpacking this archive. Download repair replaces a corrupt input only after the replacement passes full gzip validation.
`rebuild_episode.py` now writes under `DATA_DIR/parquet/recovered/title.episode` (overridable by REBUILD_OUTPUT), not a hard-coded C drive. `test_usb.py` uses DATA_DIR, and `verify_local.py` uses VERIFY_PARQUET_DIR (default DATA_DIR/parquet/raw).

## Exploration

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step Analyze -Question q1
# q1 ... q8, open, all
```

The added `src/core.py` reuses existing loaders, Spark session and `clean_values.value_rules`.
Analysis views apply those repairs, reject all ambiguous duplicate keys, exclude missing keys and orphan scalar references, and remove invalid array-reference members. Original raw files stay unchanged.
These are lazy views rather than another full clean dataset. Large repeated joins may be slow and need substantial temporary space.
Results: `results/exploration/<question>/{parquet,json}`. Outputs appear only after successful execution.

Implementation parameters to review before writing your own report:
- Films: `titleType=movie`, runtime >=60 minutes.
- Q1: genre duration beyond two population standard deviations.
- Q2: sum of votes by genre/year, 1980–2025, deterministic top-three ranking.
- Q3: descriptive top-three vs remaining genre memberships. Multi-genre films overlap.
- Q4: >=1000 votes by default (`MIN_VOTES`), exactly one rating >=8; at least two remaining eligible films average <=6.
- Q5: >=3 observed positive-numbered seasons, each >=10 episodes; tied best/worst episodes qualify.
- Q6: success >=7 and >=10000 votes; original-title language is a **proxy**, not verified spoken language. Coverage is output separately. External language validation may be needed for the intended question.
- Q7: >=20-year observed career, >=15 films; early third has higher mean and maximum eligible film rating. This rating-based definition is explicit rather than an established measure of personal fame.
- Q8: writer/director/actor roles; single vs multiple roles on films. Associations are descriptive, not causal.
- Open question: first vs subsequent actor/director collaborations and film rating.

## Enrichment

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step EnrichmentSelect
py -3.13 scripts/collect_wikipedia.py --contact YOUR_EMAIL --results D:/projectTeam/results
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step EnrichmentFinish
```

The host Python needs `requests` from requirements.txt. Pass your configured results path to the collector.
Selection uses top ten films by votes per genre/year, 1980–2025, with up to three available principals. Principal shortages are measured.
Wikidata P345 resolves exact IMDb IDs to human-authored English Wikipedia pages. The collector uses caching, rate limits, retries, URLs and timestamps. Film Plot/Synopsis sections are preferred; introduction fallback is labelled. Missing/ambiguous/error statuses are explicit. `--limit 10` is a smoke run only; the complete collection can take hours.
Spark performs schema enforcement, cleaning, joins and coverage measurement. No missing text is generated.

## Streaming

The existing `wikistream_producer.py` is corrected to use live recent changes rather than a fixed February replay date; it polls delivery callbacks and flushes at exit rather than calling unsupported `Producer.close()`.

```powershell
docker compose up -d kafka
# Terminal 1, consumer first:
docker compose exec -w /home/jovyan -e PYTHONPATH=/home/jovyan pyspark-notebook spark-submit --driver-memory 4g streaming/wikistream_consumer.py --seconds 1800
# Terminal 2, host Python with requests and confluent-kafka installed:
py -3.13 streaming/wikistream_producer.py --contact YOUR_EMAIL --seconds 1800
```

Kafka defaults remain topic `wikistreams`, host localhost:9092, internal kafka:19092.
The consumer matches normalized titles/names against IMDb. These are candidate relevance matches, not verified entity identities; ambiguous names may match multiple IMDb records.
Event-time deduplication uses a 10-minute watermark per event/IMDb ID. Window metrics count candidate joined rows; they may include duplicate source events. Late data beyond a watermark can be dropped. Checkpoints retain state.
Results: matches Parquet, per-batch window snapshots, progress JSON and producer delivery metrics. Do not sum successive window snapshots as disjoint counts. No matches or throughput are claimed without a real run.
The producer writes metrics to `results/streaming` relative to the current host directory; copy these to the configured results directory if different.

## Tests

Install PySpark 4.x in a separate local test environment with Java, or run the tests in the Spark container:

```powershell
docker compose exec -w /home/jovyan -e PYTHONPATH=/home/jovyan pyspark-notebook spark-submit --master local[1] tests/test_pipeline.py
```

The test mount is included in compose.yaml. Tests create synthetic temporary data only. `tests/check_resume.py` additionally checks cleaning, readback and source-change invalidation in separate Python processes; it requires host PySpark installation.

## Submission

Keep the original REPORT.md for team-written reasoning. Do not submit synthetic test output as IMDb analysis results.
Review the code and actual outputs, use a feature branch and peer-reviewed PR under the group's existing workflow. Do not upload data, virtual environments, `.env` or Spark temporary files.
