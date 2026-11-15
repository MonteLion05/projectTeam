"""Validate existing IMDb archives; explicitly repair corrupt ones on request."""
import argparse
import gzip
import hashlib
import json
import os
import shutil
import urllib.request
from pathlib import Path
from datetime import datetime, timezone

TABLES = ['name.basics','title.akas','title.basics','title.crew','title.episode','title.principals','title.ratings']

def validate(path):
    with gzip.open(path, 'rb') as source, open(os.devnull, 'wb') as sink:
        shutil.copyfileobj(source, sink, 1024 * 1024)
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default=str(Path(__file__).resolve().parents[1] / 'data'))
    parser.add_argument('--tables', nargs='+', choices=TABLES, default=TABLES)
    parser.add_argument('--repair', action='store_true', help='Replace corrupt files only after the new download validates')
    parser.add_argument('--verify-only', action='store_true', help='Never download or replace input files')
    args = parser.parse_args()
    root = Path(args.data); (root/'raw').mkdir(parents=True, exist_ok=True)
    manifest_path = root/'download_manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    failures = []
    for table in args.tables:
        target = root/'raw'/f'{table}.tsv.gz'
        url = f'https://datasets.imdbws.com/{table}.tsv.gz'
        try:
            digest = None
            if target.exists():
                try:
                    digest = validate(target)
                except Exception as error:
                    # All decompressor error types, including zlib.error, fail closed.
                    if not args.repair or args.verify_only:
                        raise RuntimeError(f'Invalid gzip {target}; rerun with --repair after checking storage') from error
            if digest is None:
                if args.verify_only:
                    raise FileNotFoundError(target)
                temporary = target.with_name(target.name + '.part')
                print(f'DOWNLOADING {url} -> {temporary}', flush=True)
                urllib.request.urlretrieve(url, temporary)
                digest = validate(temporary)
                # Never replace a previous file with an unverified transfer.
                temporary.replace(target)
            manifest[table] = {'sha256': digest, 'bytes': target.stat().st_size,
                'validated_at': datetime.now(timezone.utc).isoformat(), 'url': url}
            tmp_manifest = manifest_path.with_suffix('.json.tmp')
            tmp_manifest.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
            tmp_manifest.replace(manifest_path)
            print('VERIFIED', table, flush=True)
        except Exception as error:
            failures.append(table)
            print(f'FAIL {table}: {error}', flush=True)
    if failures:
        raise SystemExit('Failed archives: ' + ', '.join(failures))

if __name__ == '__main__':
    main()
