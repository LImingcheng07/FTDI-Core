"""Database audit. This confirms local integrity, never venue completeness."""
from __future__ import annotations

from contextlib import closing
from pathlib import Path
import json
from .adapters.s070041.durable_stream import StreamDB


def audit(root: Path) -> dict:
    root = Path(root)
    registry = json.loads((root/'lane_registry.json').read_text(encoding='utf-8'))
    lanes = {}
    for alias,source in sorted(registry.items()):
        path = root/'lanes'/alias/'state.sqlite3'
        with StreamDB(path,source['source'],expected_scope=source['scope']) as db:
            r = db.report()
        checks = {'source_hash_stable':r['source_file_still_matches_initial_sha256'],
                  'journal_checksum':r['raw_journal_checksum_mismatches']==0,
                  'outbox_checksum':r['outbox_content_mismatches']==0,
                  'no_orphan_outbox':r['orphan_or_unprocessed_outputs']==0,
                  'delivery_integrity':r['delivery_inconsistencies']==0,
                  'file_exhausted':r['source_file_exhausted'],
                  'processed':r['raw_unprocessed']==0,
                  'no_pending':r['pending']==0,
                  'no_quarantine':r['quarantine_records']==0,
                  'no_conflict':all(v['integrity'] != 'CONFLICT' for v in r['lanes'])}
        lanes[alias] = {'passed':all(checks.values()),'checks':checks,
                        'source_records_acknowledged':r['source_records_acknowledged'],
                        'outbox_records':r['outbox_records'],
                        'outbox_unacked':r['outbox_unacked'],
                        'delivery_records':r['delivery_records'],
                        'lanes':r['lanes'], 'note':'First-observed prefix remains unverified by exchange'}
    out={'schema':'FTDI-Core-Audit-v1','scope':'local journal/outbox only',
         'passed':all(v['passed'] for v in lanes.values()),'lanes':lanes}
    (root/'AUDIT_REPORT.json').write_text(json.dumps(out,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    return out
