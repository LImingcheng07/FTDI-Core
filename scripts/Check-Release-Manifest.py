"""Verify the source tree against a package manifest.

    python scripts/Check-Release-Manifest.py              # newest release/PACKAGE_MANIFEST.v*.json
    python scripts/Check-Release-Manifest.py PACKAGE_MANIFEST.json   # the shipped v0.2 baseline

Every listed file must exist with the recorded size and SHA-256. Files present in the tree but absent
from the manifest are reported as EXTRA. Exit 1 on any difference.
"""
from pathlib import Path
import hashlib
import json
import re
import sys

root = Path(__file__).resolve().parents[1]


def newest_release_manifest():
    found = []
    for path in (root / 'release').glob('PACKAGE_MANIFEST.v*.json'):
        match = re.fullmatch(r'PACKAGE_MANIFEST\.v(\d+)\.(\d+)\.(\d+)\.json', path.name)
        if match:
            found.append((tuple(int(x) for x in match.groups()), path))
    return max(found)[1] if found else root / 'PACKAGE_MANIFEST.json'


manifest_path = Path(sys.argv[1]) if len(sys.argv) > 1 else newest_release_manifest()
if not manifest_path.is_absolute():
    manifest_path = root / manifest_path
manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
issues = []
for rel, meta in manifest['files'].items():
    path = root / rel
    if not path.is_file():
        issues.append(f'MISSING: {rel}')
        continue
    data = path.read_bytes()
    if len(data) != meta['bytes'] or hashlib.sha256(data).hexdigest() != meta['sha256']:
        issues.append(f'MISMATCH: {rel}')
if manifest.get('schema') == 'FTDI-Package-Manifest-v2':
    sys.path.insert(0, str(root / 'scripts'))
    from build_release import release_files
    listed = set(manifest['files'])
    for path in release_files(exclude={manifest_path}):
        rel = path.relative_to(root).as_posix()
        if rel not in listed:
            issues.append(f'EXTRA: {rel}')
print(f"{manifest_path.name}: verified {len(manifest['files']) - sum(not i.startswith('EXTRA') for i in issues)}"
      f" / {len(manifest['files'])} files ({manifest.get('release')})")
for issue in issues:
    print(issue, file=sys.stderr)
sys.exit(1 if issues else 0)
