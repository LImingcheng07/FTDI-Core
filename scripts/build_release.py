"""Build a versioned release: per-file SHA-256 manifest + deterministic ZIP. No market data, no state.

    python scripts/build_release.py --version 0.3.0

* writes ``release/PACKAGE_MANIFEST.v<version>.json`` and ``dist/FTDI-Core-v<version>-windows.zip``;
* refuses to overwrite either, and refuses a dirty git working tree (release = committed content);
* never touches ``PACKAGE_MANIFEST.json`` (the shipped v0.2 baseline, kept as evidence);
* builds no installer, contacts no network.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / 'PACKAGE_MANIFEST.json'
ZIP_SHA256 = '78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9'
EXCLUDE_DIRS = {'.git', '__pycache__', '.pytest_cache', '.venv', 'runs', 'data', 'logs', 'build', 'dist', '.claude'}
EXCLUDE_SUFFIXES = {'.pyc', '.pyo', '.sqlite', '.sqlite3', '.db', '.tmp', '.key', '.pem', '.zip'}
VERSION_RE = re.compile(r'[0-9]+\.[0-9]+\.[0-9]+(?:[a-z0-9.+-]*)')


def release_files(exclude=()):
    files = []
    for path in sorted(ROOT.rglob('*')):
        rel = path.relative_to(ROOT)
        if not path.is_file() or any(part in EXCLUDE_DIRS for part in rel.parts) or path in exclude:
            continue
        if path.suffix in EXCLUDE_SUFFIXES or path.name.startswith('.env') or any(
                marker in path.name for marker in ('-wal', '-shm', '-journal')):
            continue
        files.append(path)
    return files


def git_dirty():
    try:
        done = subprocess.run(['git', 'status', '--porcelain'], cwd=ROOT, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout.strip() if done.returncode == 0 else None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--version', required=True)
    parser.add_argument('--allow-dirty', action='store_true', help='Build from an uncommitted tree (not a release)')
    args = parser.parse_args(argv)
    if not VERSION_RE.fullmatch(args.version):
        raise SystemExit('version must look like 0.3.0')
    sys.path.insert(0, str(ROOT))
    from ftdi_core import __version__
    if __version__ != args.version:
        raise SystemExit(f'ftdi_core.__version__ is {__version__}, not {args.version}')
    dirty = git_dirty()
    if dirty and not args.allow_dirty:
        raise SystemExit('git working tree is not clean; commit first (or --allow-dirty for a non-release build)')
    manifest_path = ROOT / 'release' / f'PACKAGE_MANIFEST.v{args.version}.json'
    zip_path = ROOT / 'dist' / f'FTDI-Core-v{args.version}-windows.zip'
    for path in (manifest_path, zip_path):
        if path.exists():
            raise SystemExit(f'refusing to overwrite {path}')
    files = release_files(exclude={manifest_path})
    manifest = {
        'schema': 'FTDI-Package-Manifest-v2',
        'release': f'FTDI-Core-v{args.version}',
        'version': args.version,
        'not_production_certified': True,
        'source_data_included': False,
        'built_from_clean_git_tree': dirty == '',
        'base_manifest': BASELINE.name,
        'base_manifest_note': 'shipped v0.2 baseline, kept byte-identical as evidence',
        'base_manifest_sha256': hashlib.sha256(BASELINE.read_bytes()).hexdigest(),
        'source_zip_sha256_expected_when_user_supplies_archival_fixture': ZIP_SHA256,
        'files': {p.relative_to(ROOT).as_posix(): {'bytes': p.stat().st_size,
                                                   'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
                  for p in files},
    }
    manifest_path.parent.mkdir(exist_ok=True)
    with open(manifest_path, 'x', encoding='utf-8', newline='\n') as handle:
        handle.write(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    zip_path.parent.mkdir(exist_ok=True)
    top = f'FTDI-Core-v{args.version}'
    with zipfile.ZipFile(zip_path, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in [*files, manifest_path]:
            info = zipfile.ZipInfo(f'{top}/{path.relative_to(ROOT).as_posix()}', date_time=(2026, 11, 7, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, path.read_bytes())
    with zipfile.ZipFile(zip_path) as archive:
        bad = archive.testzip()
        if bad:
            raise SystemExit(f'corrupt packaged member: {bad}')
    print(json.dumps({'zip': str(zip_path), 'zip_bytes': zip_path.stat().st_size,
                      'zip_sha256': hashlib.sha256(zip_path.read_bytes()).hexdigest(),
                      'manifest': str(manifest_path.relative_to(ROOT)), 'files': len(files) + 1}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
