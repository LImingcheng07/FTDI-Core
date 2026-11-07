"""Write a per-file SHA-256 manifest for an UNRELEASED development candidate.

Release-manifest process for work on top of the shipped baseline:
* never modifies or regenerates PACKAGE_MANIFEST.json / DEVKIT_MANIFEST.json (baseline evidence);
* writes a NEW file and refuses to overwrite an existing one;
* records the SHA-256 of the baseline manifest it descends from;
* builds no ZIP, contacts no network.

    python scripts/build_candidate_manifest.py --label task001-archive-e2e-audit
    python scripts/build_candidate_manifest.py --check release/CANDIDATE_MANIFEST.task001-archive-e2e-audit.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / 'PACKAGE_MANIFEST.json'
EXCLUDE_DIRS = {'.git', '__pycache__', '.pytest_cache', '.venv', 'runs', 'data', 'logs', 'build', 'dist',
                'release', '.claude'}
EXCLUDE_SUFFIXES = {'.pyc', '.pyo', '.sqlite', '.sqlite3', '.db', '.tmp', '.key', '.pem'}
LABEL_RE = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,80}')


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_files():
    files = {}
    for path in sorted(ROOT.rglob('*')):
        rel = path.relative_to(ROOT)
        if not path.is_file() or any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        if path.suffix in EXCLUDE_SUFFIXES or path.name.startswith('.env') or '-wal' in path.name \
                or '-shm' in path.name or '-journal' in path.name:
            continue
        files[rel.as_posix()] = {'bytes': path.stat().st_size, 'sha256': sha256(path)}
    return files


def build(label):
    if not LABEL_RE.fullmatch(label):
        raise SystemExit('label must match ' + LABEL_RE.pattern)
    out = ROOT / 'release' / f'CANDIDATE_MANIFEST.{label}.json'
    if out.exists():
        raise SystemExit(f'refusing to overwrite existing manifest: {out}')
    baseline = json.loads(BASELINE.read_text(encoding='utf-8'))
    manifest = {
        'schema': 'FTDI-Candidate-Manifest-v1',
        'label': label,
        'status': 'UNRELEASED_DEVELOPMENT_CANDIDATE',
        'not_production_certified': True,
        'base_release': baseline.get('release'),
        'base_manifest': BASELINE.name,
        'base_manifest_sha256': sha256(BASELINE),
        'source_data_included': False,
        'files': tree_files(),
    }
    out.parent.mkdir(exist_ok=True)
    with open(out, 'x', encoding='utf-8', newline='\n') as handle:
        handle.write(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps({'manifest': str(out.relative_to(ROOT)), 'files': len(manifest['files']),
                      'base_manifest_sha256': manifest['base_manifest_sha256']}, indent=2))


def check(path):
    manifest = json.loads(Path(path).read_text(encoding='utf-8'))
    expected, actual = manifest['files'], tree_files()
    issues = [f'MISSING: {rel}' for rel in expected if rel not in actual]
    issues += [f'MISMATCH: {rel}' for rel in expected if rel in actual and actual[rel] != expected[rel]]
    issues += [f'EXTRA: {rel}' for rel in actual if rel not in expected]
    if sha256(BASELINE) != manifest['base_manifest_sha256']:
        issues.append(f'BASELINE CHANGED: {BASELINE.name}')
    print(f"Verified {len(expected) - sum(not i.startswith('EXTRA') for i in issues)} / {len(expected)} files "
          f"against {Path(path).name}")
    for issue in issues:
        print(issue, file=sys.stderr)
    return 1 if issues else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--label')
    group.add_argument('--check', type=Path)
    args = parser.parse_args(argv)
    if args.label:
        build(args.label)
        return 0
    return check(args.check)


if __name__ == '__main__':
    raise SystemExit(main())
