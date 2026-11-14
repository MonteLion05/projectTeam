# ECE Big Data Processing project - Group gr-04

Pipeline built on the IMDb non-commercial datasets with Apache Spark
(PySpark batch and Structured Streaming) and Apache Kafka.
Final results and methodology are in [REPORT.md](REPORT.md).

## Contributors

| Name | GitHub | Email |
|---|---|---|
| Lionel MONTEIRO WORA | [MonteLion05](https://github.com/MonteLion05) | lionel.monteirowora@edu.ece.fr |
| Ahmed El Monaser | [ahmedmonaser0401-cpu](https://github.com/ahmedmonaser0401-cpu) | ahmed.elmonaser@edu.ece.fr |
| Ahmed Amine ZAHRAOUI | [ahmed-zhr](https://github.com/ahmed-zhr) | ahmedamine.zahraoui@edu.ece.fr |

## Repository structure

```
projectTeam/
├── compose.yaml            # Docker services (Spark/Jupyter, Kafka)
├── requirements.txt        # Python packages for scripts run on the host
├── PROJECT.md              # this file
├── REPORT.md               # results and methodological explanations
├── src/                    # shared Spark modules (session, schemas, loaders)
├── scripts/                # download and conversion scripts
├── streaming/              # Kafka admin script and Wikipedia producer
├── notebooks/              # Jupyter notebooks
└── data/                   # IMDb data (not versioned)
```

## Prerequisites

| Tool | Version |
|---|---|
| Git (with Git Bash on Windows) | 2.51.0 |
| Docker Desktop (with Docker Compose) | Docker Engine 27.4.0 |
| Python | 3.13.5 |

Docker Desktop must be running, and it needs enough memory for Spark
(`title.principals` is the largest table). The conversion was tested with a
4 GB Spark driver; allow at least 6 GB to Docker (WSL2 on Windows).

### Versions used

| Component | Version |
|---|---|
| Spark | 4.2.0 (image `quay.io/jupyter/pyspark-notebook`) |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` |
| Python packages (host) | pinned in `requirements.txt` |

The Kafka connector version must match the Spark version of the image,
otherwise the streaming query fails with a `NoSuchMethodError`.
The connector is defined in one place: `src/spark_session.py`.

## Setup from scratch

Commands are given for Git Bash (Windows). Run them from the repository root.

1. Clone the repository and enter it:

```bash
git clone https://github.com/MonteLion05/projectTeam.git
cd projectTeam
```

2. Create and activate a virtual environment, then install the host
   dependencies:

```bash
python -m venv .venv
source .venv/Scripts/activate      # Linux/macOS: source .venv/bin/activate
python -m pip install -r requirements.txt
```

3. Download the 7 IMDb datasets into `data/raw/` (large download, the script
   skips files already present):

```bash
python scripts/download_imdb.py
```

4. Start the Docker environment:

```bash
docker compose up -d
```

5. Convert the raw TSV files to Parquet (run once, about 20 minutes):

```bash
MSYS_NO_PATHCONV=1 docker exec -w /home/jovyan -e PYTHONPATH=/home/jovyan pyspark_notebook spark-submit --driver-memory 4g scripts/convert_to_parquet.py
```

`MSYS_NO_PATHCONV=1` prevents Git Bash from rewriting `/home/jovyan` into a
Windows path. It is harmless on Linux and macOS. `spark-submit` is required:
PySpark is not installed as a Python package in the image, so a plain
`python script.py` fails with `ModuleNotFoundError: No module named 'pyspark'`.
The output is written to `data/parquet/raw/<table>`.

6. Open JupyterLab: get the URL with its token from the container logs, then
   open it in a browser (`http://localhost:8888/lab?token=...`):

```bash
docker logs pyspark_notebook
```

7. Check the setup in a notebook:

```python
import sys
sys.path.insert(0, "/home/jovyan")

from src.spark_session import get_spark
from src.loaders import read_typed

spark = get_spark()
basics = read_typed(spark, "title.basics")
basics.printSchema()
print(basics.count())
```

To stop everything: `docker compose down`.

## Running each part

Each section is completed by its owner in the Pull Request that adds the code.

### 1. Data preparation

`<to complete: notebook/script to run, expected outputs, location of the cleaned tables>`

### 2. Data exploration

`<to complete: one entry per question, with the notebook/script to run>`

### 3. Data enrichment

`<to complete: how to run the collection step, rate limit settings, output location>`

### 4. Streaming

Run from the repository root with the virtual environment active.

1. Create the Kafka topic: `python streaming/admin.py`
2. Start the producer (it runs for the duration set in the script):
   `python streaming/wikistream_producer.py`
3. Start the Spark Structured Streaming job: `<to complete: notebook/script>`
4. Read the console output of the streaming queries:
   `docker logs -f pyspark_notebook`

`<to complete: filters used, test window, where metrics are stored>`

### 5. Open question

`<to complete>`

## Git workflow

- One feature branch per task, merged through a Pull Request.
- Every Pull Request is reviewed and approved by another group member
  before merging.
- No direct commits to `main` and no web uploads (the only exception is
  `README.md`).
- Review rotation: Lionel is reviewed by Ahmed Amine, Ahmed Amine by
  Ahmed El Monaser, Ahmed El Monaser by Lionel.

## AI / LLM tools declaration

| Tool | Used by | Tasks |
|---|---|---|
| Claude (Anthropic) | Lionel MONTEIRO WORA | Project analysis and work split; Docker/Spark/Kafka environment debugging; code for `scripts/download_imdb.py`, `scripts/convert_to_parquet.py` and `src/` modules; structure of `PROJECT.md` and `REPORT.md` |
| `<tool>` | `<member>` | `<tasks>` |

Each member adds the tools they use and the tasks concerned.
AI tools were not used to write explanations or reasoning in `REPORT.md`.

## Troubleshooting

- **`no configuration file provided: not found`**: run `docker compose`
  from the folder containing `compose.yaml`.
- **`NoSuchMethodError` in the streaming query**: the Kafka connector
  version differs from the Spark version (see "Versions used").
- **`ModuleNotFoundError: No module named 'pyspark'`** with `docker exec ... python`:
  run Spark scripts with `spark-submit` (step 5), or from a JupyterLab notebook.
- **`/home/jovyan` turned into `C:/Program Files/Git/...`**: prefix the
  command with `MSYS_NO_PATHCONV=1`.
- **Streaming output not visible in the notebook**: the console sink writes
  to the container logs (`docker logs -f pyspark_notebook`).
