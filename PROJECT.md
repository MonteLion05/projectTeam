# IMDb Big Data Processing — Group gr-04

## 1. Project and contributors

This repository implements IMDb data preparation, batch exploration, enrichment and a Wikipedia/Kafka Structured Streaming pipeline. This file is the execution and operations guide. Results, methods and limitations are presented in [REPORT.md](REPORT.md).

| Contributor | GitHub | Email |
|---|---|---|
| Lionel MONTEIRO WORA | MonteLion05 | lionel.monteirowora@edu.ece.fr |
| Ahmed El Monaser | ahmedmonaser0401-cpu | ahmed.elmonaser@edu.ece.fr |

Repository: <https://github.com/MonteLion05/projectTeam>

## 2. Execution status and limitations

| Component | Recorded status |
|---|---|
| Input recovery | Seven fresh IMDb archives downloaded and fully decoded; SHA256 and retrieval times recorded |
| Preparation implementation | Value repairs, key filtering and reference handling executed for the recorded analytical snapshot |
| Detailed quality reports | Historical reports exist; regenerate the per-column and issue reports on the analytical snapshot before treating them as matching evidence |
| Exploration Q1–Q5, Q7–Q8 | Executed on the full downloaded snapshot; CSV outputs and SQL recorded |
| Exploration Q6 | Query executed, but inconclusive: the selected original-title-language proxy covered 0 of 431,386 eligible films |
| Open question | Executed: first versus subsequent actor/director collaborations and film ratings |
| Enrichment | Selection/join/coverage logic tested on synthetic data; full online text collection and coverage not demonstrated |
| Streaming | Producer/consumer implemented; no completed live 15–30 minute run with matching events and metrics demonstrated |
| Report | Results and methodological discussion documented; missing experimental evidence identified |

An empty Q6 output does **not** demonstrate that multilingual actors do not exist. The title-language proxy must be replaced or supplemented with verified film-language information to complete that question. Alternative-title translation languages must not be presented as proof of spoken/original film language.

This guide does not claim that all assignment deliverables are complete. In particular, online enrichment, live streaming evidence and Q6 remain outstanding.

## 3. Repository contents and evidence integration

| Path | Purpose |
|---|---|
| `PROJECT.md` | This guide and AI-use declaration |
| `REPORT.md` | Results, methodology, interpretation and trade-offs |
| `compose.yaml` | Existing Jupyter/Spark and Kafka services |
| `requirements.txt` | Host-side collection/producer dependencies |
| `.env.example` | Non-secret examples of host data/output paths |
| `src/` | Schemas, loaders, quality functions, cleaning and Spark configuration |
| `scripts/` | Download, integrity checking, preparation and full-snapshot execution |
| `jobs/analyze.py` | Q1–Q8 and the open-question SQL |
| `jobs/select_enrichment.py` | Top-film and principal selection |
| `jobs/finish_enrichment.py` | Spark text processing, joins and coverage |
| `streaming/` | Wikipedia producer and Spark consumer |
| `tests/` | Synthetic pipeline, resume and enrichment tests |
| `tables/` | Paginated Markdown result tables linked from REPORT.md |
| `evidence/verified_results/` | Committed small CSV outputs, manifests and execution evidence |
| `data/`, `results/`, `spark-tmp/` | Local generated data; not versioned |

### Integrate the executed-results handoff before merging this guide

The `IMDb_Executed_Questions_Results.zip` handoff contains newer analytical code as well as results. Download it from the project handoff, extract it outside the repository, and:

1. Merge the contents of `code/src/`, `code/jobs/` and `code/scripts/` into the matching repository directories. Preserve unrelated repository files. In particular, include `download_verified_snapshot.py`, `run_verified_questions.py` and the Q8 query with `career_role_names`.
2. Place the handoff's CSV files, `manifest.json`, `status.json`, `clean_row_counts.json`, `q6_language_coverage.json`, `code_sha256.json` and `execution/` under `evidence/verified_results/`.
3. Keep the evidence's `VALIDATION.md` with those files. Do not present the synthetic-test validation as full-data evidence.
4. Review and commit the code and evidence through the group's PR workflow. A clone must contain these files; this guide alone does not add them.
5. Check that the report matches the committed results and that the assistance declaration describes the work accurately.

`q4_with_titles.csv` supplements Q4 with film names. Q8 has 46,330 comparison rows representing 37,670 people; one person can have both single-role and multiple-role rows. The handoff contains the complete result tables, not just previews.

## 4. Prerequisites

The recorded batch run used **Linux, Python 3.12, Java 21.0.13 and PySpark 4.0.1**, with `local[2]` and a 3 GiB driver heap. The Linux route below matches that software family. Windows Docker operation is provided separately; the floating Jupyter image is not a pinned reproduction of the recorded runtime.

Required components:

- Git and internet access for dependencies and source data.
- For the recorded batch route: Python 3.12 and Java 21 installed and available on `PATH`.
- For Docker/streaming: Docker Desktop or Docker Engine with Compose; enough memory allocated for Spark and Kafka.
- Substantial free disk space for compressed inputs, several Parquet layers and shuffle files. The input archives alone are not a useful estimate of peak disk use. Monitor space throughout the run.

The previous USB copies experienced decoding failures. Do not reuse them solely because they exist or have `_SUCCESS` markers. The input and intermediate data are not included in Git.

## 5. Linux setup and full batch execution from scratch

Run these commands in Linux, including an appropriately configured Linux/WSL environment. They are **not PowerShell commands**.

### 5.1 Clone and install

```bash
git clone https://github.com/MonteLion05/projectTeam.git
cd projectTeam
# If the work is not merged yet, switch to the actual reviewed feature branch.
# Example: git switch ahmed/imdb-pipeline

test -f scripts/download_verified_snapshot.py
test -f scripts/run_verified_questions.py

python3.12 -m venv .venv
.venv/bin/python -m pip install "pyspark==4.0.1"
java -version
.venv/bin/spark-submit --version
```

If either `test -f` fails, stop and integrate the handoff files or select the branch that contains them. Do not proceed with an older runner.

### 5.2 Select local paths

```bash
export PYTHONPATH="$PWD"
export IMDB_RUN_DIR="$PWD/data"
export DATA_DIR="$IMDB_RUN_DIR"
export RESULTS_DIR="$IMDB_RUN_DIR/results"
export SPARK_LOCAL_DIRS="$IMDB_RUN_DIR/spark-tmp"
mkdir -p "$IMDB_RUN_DIR" "$RESULTS_DIR" "$SPARK_LOCAL_DIRS"
```

Use an empty directory for a new snapshot. The pipeline stores its processed input manifest and refuses to silently reuse intermediate data when that manifest changes.

### 5.3 Download and validate

```bash
.venv/bin/python scripts/download_verified_snapshot.py
```

This downloads the seven archives from `https://datasets.imdbws.com/`, fully decodes each gzip, counts source lines and records SHA256, size, source URL and UTC retrieval time in `data/manifest.json`. Download concurrency is three files. Nothing is assumed valid merely because a file exists.

To recheck existing inputs without downloading:

```bash
.venv/bin/python scripts/download_imdb.py --data "$IMDB_RUN_DIR" --verify-only
```

For an explicitly requested repair of a corrupt or missing archive, use `--repair` instead of `--verify-only`. A replacement is validated before it replaces the original. Do not repair inputs in a directory whose old Parquet will then be reused without rebuilding; use a new run directory for a changed snapshot.

### 5.4 Execute all analytical queries

```bash
.venv/bin/spark-submit --driver-memory 3g scripts/run_verified_questions.py
```

The runner performs these stages:

1. Read TSV as strings and materialize raw Parquet under `data/parquet/raw/`.
2. Normalize missing values, safely cast types, apply value rules and exclude missing or ambiguous duplicate keys; save `data/parquet/base/`.
3. Filter orphan scalar references and invalid array-reference members; save `data/parquet/clean/`.
4. Execute Q1–Q8 and the open query, saving Parquet, CSV and `data/results/status.json`.
5. Calculate the Q6 language-coverage evidence.

The Q8 role labels in the recorded handoff were computed in a supplementary pass over the same cleaned snapshot. Row counts and role-count consistency were checked. The packaged Q8 SQL now includes these labels directly. Logs distinguish the successful main run, a failed optional supplementary JVM attempt and the successful retry.

### 5.5 Resume a stopped analytical run

Re-run the same `spark-submit` command with the same paths and unchanged manifest. Completed Parquet stages are reused. Query outputs are overwritten when re-executed. Do not rerun the snapshot downloader into that same directory before resuming: its updated manifest will intentionally invalidate reuse.

If a completed Parquet stage is suspected of corruption, preserve it for diagnosis and rebuild in a fresh directory. `_SUCCESS` records completion, not a universal integrity guarantee.

## 6. Detailed preparation reports and value-cleaning checkpoints

After raw Parquet exists from section 5, keep the same Linux environment variables and run:

```bash
export INPUT_FORMAT=parquet
for job in profile_data check_casts check_duplicates check_values check_relations; do
    .venv/bin/spark-submit --master 'local[2]' --driver-memory 4g "scripts/$job.py" || break
done
```

Stop on any failure. Reports are written under `data/quality/`, including:

- `missing_profile/`: null, literal `\N`, empty and combined-missing percentages by column.
- `top_five_missing/`: top five columns by combined missing percentage. This is not a separately ranked null-only table; retain the individual counts when answering the assignment.
- `cast_checks/`, `key_checks/`, `value_checks/` and `relation_checks/`.

To use the existing resumable value-cleaning script and its audit outputs:

```bash
.venv/bin/spark-submit --master 'local[2]' --driver-memory 4g scripts/clean_values.py
# Optional single-table recovery:
# .venv/bin/spark-submit --master 'local[2]' --driver-memory 4g scripts/clean_values.py --tables title.episode
```

This separate script writes `data/parquet/staged/`, `data/quality/value_audit/`, `value_cleaning_summary/` and content-fingerprint checkpoints. It checks full-column readback metrics. It is not a prerequisite for the analytical runner, which performs its own cleaning using the shared rules. Do not confuse `staged/` value repairs with the analytical runner's `clean/` reference-filtered tables.

Do not combine historical quality counts with a new analytical snapshot without clearly identifying their different provenance. Regenerate the detailed reports before calling them current.

## 7. Query outputs and operational parameters

| Query | Output directory under `data/results/` | Definition implemented |
|---|---|---|
| Q1 | `q1/` | Movie, duration ≥60 minutes; duration more than two population standard deviations from its genre mean |
| Q2 | `q2/` | Top three genres by sum of votes per year, 1980–2025 |
| Q3 | `q3/` | Top-three vs other genre memberships: counts, votes, ratings and durations |
| Q4 | `q4/` | Exactly one eligible film rated ≥8; at least two other eligible films averaging ≤6; eligibility defaults to ≥1000 votes |
| Q5 | `q5/` | At least three observed positive-numbered seasons, each with ≥10 episodes; extreme episode ratings in the required seasons; ties accepted |
| Q6 | `q6/` | Success ≥7 and ≥10000 votes; original-title-language proxy; inconclusive for this snapshot |
| Q7 | `q7/` | Recorded career ≥20 years and ≥15 films; early third has higher mean and maximum eligible ratings; eligible ratings use ≥1000 votes |
| Q8 | `q8/` | Writer/director/actor career roles and single vs multiple roles per film; rating comparisons use ≥1000 votes |
| Open | `open/` | First versus later actor/director collaborations; descriptive film-rating differences |

The common film filter is `titleType=movie` and `runtimeMinutes>=60`. There is no global year cutoff for all questions. Q7 uses recorded `startYear` values, including announced titles if present, and is a rating-based operational definition rather than a direct measurement of personal fame. Multi-genre films can contribute to several genre groups. These definitions and limitations must be considered by the team when writing its own report.

For a single query without the materialized full-run workflow, set `DATA_DIR`, `RESULTS_DIR` and `INPUT_FORMAT` appropriately and execute:

```bash
.venv/bin/spark-submit --master 'local[2]' --driver-memory 4g jobs/analyze.py --question q1
```

The generic analyzer creates views for all seven tables, even for one question; it therefore requires all inputs. Its output location is `$RESULTS_DIR/exploration/q1/`, distinct from the full runner's `$IMDB_RUN_DIR/results/q1/`. A changed `MIN_VOTES` setting affects the generic analyzer; the full recorded-run runner fixes its SQL substitution at 1000.

## 8. Windows PowerShell and Docker route

Run these commands from the repository folder containing `compose.yaml`. Keep the same terminal open so the environment overrides remain active.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

if (-not (Test-Path .env)) { Copy-Item .env.example .env }

$env:IMDB_DATA_DIR = "$((Get-Location).Path.Replace('\','/'))/data"
$env:IMDB_RESULTS_DIR = "$env:IMDB_DATA_DIR/results"
$env:IMDB_TEMP_DIR = "$env:IMDB_DATA_DIR/spark-tmp"
$env:INPUT_FORMAT = 'tsv'
New-Item -ItemType Directory -Force .\data, .\data\results, .\data\spark-tmp | Out-Null

docker compose config --quiet
docker compose up -d --no-deps pyspark-notebook
docker compose exec pyspark-notebook spark-submit --version
docker compose exec pyspark-notebook ls -l /home/jovyan/scripts/run_verified_questions.py
```

The Compose image `quay.io/jupyter/pyspark-notebook` is currently unpinned. The recorded batch execution used Spark 4.0.1. A different container version may require compatibility checks. Record the actual image and Spark version when using Docker.

For a new snapshot in this Docker data directory:

```powershell
$env:IMDB_RUN_DIR = "$((Get-Location).Path)/data"
.\.venv\Scripts\python.exe scripts/download_verified_snapshot.py

docker compose exec -w /home/jovyan -e PYTHONPATH=/home/jovyan -e IMDB_RUN_DIR=/home/jovyan/data pyspark-notebook spark-submit --driver-memory 3g scripts/run_verified_questions.py
```

For the existing PowerShell preparation runner:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step Quality
```

Its `Verify` step uses `-DataPath` for a custom host data path; it does not automatically infer every `.env` override. The Docker commands use Compose mounts. If a mounted directory is empty inside the container, stop and correct the mount rather than analyzing missing or unrelated inputs.

## 9. Enrichment procedure — full collection still outstanding

Use the same raw-data snapshot. For the Linux route, keep `DATA_DIR` and `RESULTS_DIR` from section 5 and set `INPUT_FORMAT=parquet` after raw Parquet creation. Install the host collection dependencies:

```bash
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/spark-submit --master 'local[2]' --driver-memory 4g jobs/select_enrichment.py
.venv/bin/python scripts/collect_wikipedia.py --contact YOUR_EMAIL --results "$RESULTS_DIR"
.venv/bin/spark-submit --master 'local[2]' --driver-memory 4g jobs/finish_enrichment.py
```

Replace `YOUR_EMAIL` with a real project contact. The collector resolves IMDb IDs through Wikidata P345 to English Wikipedia pages. Text comes from human-authored Wikipedia; it is not generated. Film Plot/Synopsis sections are preferred; introduction fallback is explicitly labelled. The code includes a configurable pause, retry/backoff behavior and a descriptive User-Agent. Operators must comply with applicable API and content-license policies.

Selection is top ten films by votes per genre/year (1980–2025), with up to three available principal rows by ordering. Principal coverage is saved. This is not proof that every film has three distinct people or three retrieved biographies: inspect and remedy shortages before claiming the requirement is met.

Spark performs schema enforcement, text normalization, joining and coverage measurement. Outputs are under `$RESULTS_DIR/enrichment/`, including selections, targets, cache, enriched films/people, status counts and coverage. Every retrieved text has source URL and retrieval metadata; absent/ambiguous/error/empty and not-collected outcomes remain explicit.

`--limit 10` on the collector is only a smoke test, not a complete enrichment run. A full run can require many API requests. Majority coverage and complete three-principal enrichment have not been demonstrated by the current evidence.

## 10. Live streaming procedure — measured run still outstanding

The consumer currently uses the Docker-internal broker `kafka:19092`. Run it in the existing Compose network; use `localhost:9092` for the host producer.

```powershell
docker compose up -d kafka pyspark-notebook
```

Start the consumer in terminal 1, from the repository root:

```powershell
docker compose exec -w /home/jovyan -e PYTHONPATH=/home/jovyan pyspark-notebook spark-submit --driver-memory 4g streaming/wikistream_consumer.py --seconds 1800
```

Start the producer in terminal 2, also from the repository root:

```powershell
.\.venv\Scripts\python.exe streaming/wikistream_producer.py --contact YOUR_EMAIL --seconds 1800
```

The producer creates topic `wikistreams` if needed, captures live Wikipedia main-namespace edit/new events across languages, polls delivery callbacks and flushes before exit. The consumer matches normalized titles/person names against IMDb. Matches are **candidates**, not guaranteed entity identities; ambiguous names may yield several IMDb candidates.

The Spark Kafka connector is derived from the installed PySpark version and assumes Scala 2.13. A matching published connector must be available. `KAFKA_PACKAGE` can override the coordinate when justified by the runtime. Check the actual Spark version; do not substitute an incompatible connector to silence a dependency error.

The implementation uses a 10-minute event-time watermark and deduplication by event/IMDb ID for the match sink. Window counts are candidate joined rows and can include duplicate source events. Window updates are per-batch snapshots, not disjoint counts to sum. Persisted checkpoints retain offsets and state; do not reuse them after incompatible query changes.

Operational evidence to retain from a real 15–30 minute observation window:

- Captured, delivered and failed producer counts.
- Actual matching events and distinct matched event IDs.
- Consumer progress, watermark/state information and any dropped-late-row metrics actually reported.
- Start/end times and the selected filtering rules.

Consumer outputs are under the configured container `RESULTS_DIR/streaming/`; host producer metrics are written to `results/streaming/producer_metrics.json` relative to its current working directory. These locations differ when custom mounts are used; collect both into the run's evidence directory. Inspect actual nonzero matches before claiming success. Broad filtering alone is not proof of the assignment's multiple-match requirement. The report should relate late-data handling to the measured consumer progress.

## 11. Tests and recorded validation

Synthetic tests check functionality, not assignment results. From the Linux environment in section 5:

```bash
.venv/bin/python tests/test_pipeline.py
.venv/bin/python tests/check_resume.py
.venv/bin/python tests/check_enrichment.py
```

Recorded checks include synthetic SQL execution, corrupt-gzip rejection, value-cleaning readback, checkpoint/source-change invalidation and offline enrichment joins. Full analytical results were subsequently generated from the fresh snapshot. The committed full-run `manifest.json`, `status.json`, CSV files and logs are the evidence for that execution.

The CSV handoff was re-exported with standard doubled-quote escaping. Record counts and column completeness were checked, and Q8 role-list sizes matched the recorded role counts. A later snapshot can legitimately produce different outputs.

## 12. Operations, troubleshooting and shutdown

- Run commands from the folder containing `compose.yaml`; do not confuse the ZIP extraction folder with the Git checkout.
- Use `docker compose exec ... spark-submit` for the existing notebook container. The earlier `compose run` route encountered a startup `sudo: spark-submit: command not found` error.
- A gzip decoding or Parquet checksum failure is a failed input check. Do not disable integrity checks or ignore corrupt files to manufacture a complete answer.
- A filesystem `_SUCCESS` marker is not a replacement for input validation and full readback checks.
- Keep data, intermediate outputs, virtual environments, secrets and Spark temporary files out of Git. Commit only selected small evidence under `evidence/`.
- Do not delete raw sources, checkpoints or Docker volumes while a run is active. Preserve completed evidence before cleanup.
- To inspect container state: `docker compose ps`. To inspect notebook startup logs: `docker compose logs --tail 100 pyspark-notebook`.
- To stop services without deleting volumes: `docker compose stop`.

## 13. Git contribution and review workflow

Each task must originate on its own feature branch and be submitted through a PR. At least one other group member must review and approve before merge. Direct commits to `main` and web uploads are prohibited by the assignment. The Git history and actual PR discussions are the evidence of contributions; this document does not certify a review or equal participation that has not occurred.

For this documentation change, for example:

```bash
git switch -c docs/project-execution-guide
git add PROJECT.md
git commit -m "Document execution paths, evidence and remaining limitations"
git push -u origin docs/project-execution-guide
```

Open a PR, request peer review, address comments and merge only after approval. Add analytical code and evidence through their corresponding task PRs. Do not rewrite or fabricate past contribution/review history.

## 14. AI and LLM assistance

Claude (Anthropic) | Lionel MONTEIRO WORA | Declaration retained from the original project: project analysis/work split, Docker/Spark/Kafka debugging, initial download/conversion and shared-module code, and initial document structure 

 ChatGPT / OpenAI | Ahmed El Monaser | to clarify the project requirements, plan the implementation stages, understand and troubleshoot errors, and assist with data cleaning and code development.
