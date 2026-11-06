"""Check the shipped Core source package manifest. After developer edits, mismatch is expected."""
from pathlib import Path
import hashlib
import json
import sys

root = Path(__file__).resolve().parents[1]
manifest = json.loads((root/'PACKAGE_MANIFEST.json').read_text(encoding='utf-8'))
issues = []
for rel, meta in manifest['files'].items():
    path = root/rel
    if not path.is_file():
        issues.append(f'MISSING: {rel}')
        continue
    data = path.read_bytes()
    if len(data) != meta['bytes'] or hashlib.sha256(data).hexdigest() != meta['sha256']:
        issues.append(f'MISMATCH: {rel}')
print('Verified', len(manifest['files'])-len(issues), '/', len(manifest['files']), 'baseline files')
for issue in issues:
    print(issue, file=sys.stderr)
sys.exit(1 if issues else 0)
