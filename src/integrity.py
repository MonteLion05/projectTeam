"""Content fingerprints for local filesystem inputs and checkpoint outputs."""
import hashlib
from pathlib import Path

def fingerprint(path):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    files = [path] if path.is_file() else sorted(p for p in path.rglob('*') if p.is_file() and not p.name.startswith('.'))
    if not files:
        raise ValueError(f'No files at {path}')
    digest = hashlib.sha256()
    for file in files:
        digest.update((file.name if path.is_file() else str(file.relative_to(path))).encode())
        with file.open('rb') as stream:
            for block in iter(lambda: stream.read(1024*1024), b''):
                digest.update(block)
    return digest.hexdigest()
