"""FTDI-Core v0.2: deterministic, read-only, two-database delivery reconciliation.

Producer outbox and consumer effects are distinct SQLite databases.  This audit
covers RESIDENT outbox rows only; historical prefixes pruned into archives need
a separate archive-aware audit.  Comparing counts alone is never sufficient.
"""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3

from .audit import audit as local_audit
from .adapters.s070041.external_delivery import envelope, validate


def _readonly(path: Path):
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')  # One consistent snapshot within this DB.
        return conn
    except BaseException:
        conn.close()
        raise


def _effects(db):
    for row in db.execute('SELECT * FROM effects ORDER BY lane,seq,epoch'):
        yield row


def _producer(root, registry, connections):
    for alias, info in sorted(registry.items(), key=lambda item: item[1]['scope']):
        db = connections[alias]
        for row in db.execute('SELECT * FROM outbox ORDER BY lane,seq'):
            yield alias, row


def _remote_valid(row):
    try:
        payload = json.loads(row['payload'])
        validate({'event_id': row['event_id'], 'lane': row['lane'],
                  'seq': row['seq'], 'epoch': row['epoch'],
                  'payload_sha256': row['payload_sha256'], 'payload': payload})
        return True
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False


def reconcile(root: Path, consumer_db: Path, *, report=True):
    """Reconcile all resident producer outbox events with actual consumer effects.

    COMPLETE requires exact event_id/payload match, every producer delivery ACK,
    and zero orphan effects.  Remote commit with lost producer ACK is explicitly
    RETRY_REQUIRED, not failed integrity and not complete.  This method makes no
    claim about other effects outside the consumer SQLite database.
    """
    root = Path(root)
    consumer_db = Path(consumer_db).resolve()
    reg = json.loads((root/'lane_registry.json').read_text(encoding='utf-8'))
    for alias in reg:
        if consumer_db == (root/'lanes'/alias/'state.sqlite3').resolve():
            raise ValueError('Consumer effects DB must be independent of producer lane DB')
    local = local_audit(root)
    metrics = dict(producer_outbox=0, producer_acked=0, consumer_effects=0,
                   matched_effects=0, remote_missing=0, remote_committed_unacked=0,
                   orphan_consumer_effects=0, content_conflicts=0, invalid_consumer_rows=0,
                   invalid_producer_rows=0, producer_receipt_inconsistencies=0,
                   archived_records_unsupported=0)
    issues = []
    def issue(code, key=None):
        if len(issues) < 20:
            issues.append({'code':code,'key':list(key) if key is not None else None})

    conns = {}
    try:
        for alias in reg:
            conns[alias] = _readonly(root/'lanes'/alias/'state.sqlite3')
            meta = conns[alias].execute("SELECT v FROM meta WHERE k='archived_records'").fetchone()
            archived = int(meta['v']) if meta else 0
            metrics['archived_records_unsupported'] += archived
            if archived:
                issue('ARCHIVED_PREFIX_NEEDS_ARCHIVE_AWARE_AUDIT',(alias,archived))
        with closing(_readonly(consumer_db)) as remote:
            producer = iter(_producer(root,reg,conns))
            consumers = iter(_effects(remote))
            p = next(producer,None)
            c = next(consumers,None)
            while p is not None or c is not None:
                pkey = (p[1]['lane'],p[1]['seq'],0) if p is not None else None
                ckey = (c['lane'],c['seq'],c['epoch']) if c is not None else None
                if p is not None and (c is None or pkey < ckey):
                    alias,row = p
                    metrics['producer_outbox'] += 1
                    if row['delivered']:
                        metrics['producer_acked'] += 1
                        metrics['remote_missing'] += 1
                        metrics['producer_receipt_inconsistencies'] += 1
                        issue('PRODUCER_ACK_WITHOUT_CONSUMER_EFFECT',pkey)
                    else:
                        metrics['remote_missing'] += 1
                    try:
                        envelope(row)
                    except (ValueError,KeyError,TypeError,json.JSONDecodeError):
                        metrics['invalid_producer_rows'] += 1
                        issue('PRODUCER_OUTBOX_TAMPERED',pkey)
                    p = next(producer,None)
                elif c is not None and (p is None or ckey < pkey):
                    metrics['consumer_effects'] += 1
                    metrics['orphan_consumer_effects'] += 1
                    if not _remote_valid(c):
                        metrics['invalid_consumer_rows'] += 1
                        issue('INVALID_ORPHAN_CONSUMER_EFFECT',ckey)
                    else:
                        issue('ORPHAN_CONSUMER_EFFECT',ckey)
                    c = next(consumers,None)
                else:
                    alias,row = p
                    metrics['producer_outbox'] += 1
                    metrics['consumer_effects'] += 1
                    if row['delivered']:
                        metrics['producer_acked'] += 1
                    else:
                        metrics['remote_committed_unacked'] += 1
                    try:
                        event = envelope(row)
                    except (ValueError,KeyError,TypeError,json.JSONDecodeError):
                        metrics['invalid_producer_rows'] += 1
                        issue('PRODUCER_OUTBOX_TAMPERED',pkey)
                        event = None
                    if not _remote_valid(c):
                        metrics['invalid_consumer_rows'] += 1
                        issue('CONSUMER_EFFECT_TAMPERED',ckey)
                    if event is not None and (event['event_id'] != c['event_id'] or
                            event['payload_sha256'] != c['payload_sha256'] or
                            json.dumps(event['payload'],sort_keys=True,ensure_ascii=False,separators=(',',':')) != c['payload']):
                        metrics['content_conflicts'] += 1
                        issue('CONSUMER_PRODUCER_CONTENT_CONFLICT',pkey)
                    elif event is not None:
                        metrics['matched_effects'] += 1
                    # Local producer receipt must exist if delivered and match its row.
                    receipt=conns[alias].execute('SELECT raw_id,raw_sha256 FROM delivery WHERE lane=? AND seq=?',
                                                 (row['lane'],row['seq'])).fetchone()
                    if (row['delivered'] and (receipt is None or
                        receipt['raw_id']!=row['raw_id'] or receipt['raw_sha256']!=row['raw_sha256'])) or \
                       (not row['delivered'] and receipt is not None):
                        metrics['producer_receipt_inconsistencies'] += 1
                        issue('PRODUCER_ACK_LEDGER_INCONSISTENT',pkey)
                    p = next(producer,None)
                    c = next(consumers,None)
    finally:
        for db in conns.values():
            db.close()

    bad = (not local['passed'] or any(metrics[k] for k in (
        'orphan_consumer_effects','content_conflicts','invalid_consumer_rows',
        'invalid_producer_rows','producer_receipt_inconsistencies','archived_records_unsupported')))
    # Remote missing is normal for undelivered events, but never if producer ACKed.
    if bad:
        status='INCONSISTENT'
    elif metrics['remote_committed_unacked']:
        status='RETRY_REQUIRED'
    elif metrics['remote_missing']:
        status='PENDING_DELIVERY'
    else:
        status='COMPLETE'
    result={'schema':'FTDI-Core-E2E-Audit-v1','status':status,'passed':status=='COMPLETE',
            'producer_local_audit_passed':local['passed'],'metrics':metrics,'issues':issues,
            'limits':['Resident outbox only; archived prefixes require separate verified archive audit',
                      'Consumer SQLite effect only; not exactly-once for arbitrary external side effects',
                      'Separate SQLite snapshots are not a global distributed transaction; run under maintenance window'],
            'consumer_db':str(consumer_db)}
    if report:
        (root/'END_TO_END_AUDIT.json').write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    return result
