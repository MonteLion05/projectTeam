"""Download the 7 IMDb datasets into data/raw/."""
from pathlib import Path

import requests
from tqdm import tqdm

BASE_URL = "https://datasets.imdbws.com"
FILES = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def download(name):
    dest = RAW_DIR / name
    if dest.exists():
        print(f"{name}: already downloaded, skipping")
        return
    tmp = dest.with_name(dest.name + ".part")
    with requests.get(f"{BASE_URL}/{name}", stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(tmp, "wb") as out, tqdm(
            total=total, unit="B", unit_scale=True, desc=name
        ) as bar:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                out.write(chunk)
                bar.update(len(chunk))
    tmp.replace(dest)


if __name__ == "__main__":
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for file_name in FILES:
        download(file_name)