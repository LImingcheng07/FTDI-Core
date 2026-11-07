"""Core front-end for the receipt-gated producer archive (``archive-producer``). Dry run unless ``apply``.

The destructive work is done by the maintained S07 adapter ``prune`` (verified gzip segment, SHA-256 checked,
rows deleted in the same transaction that registers the segment, exact delete counts). This module adds what the
Core CLI requires (ADR-003):

* a mandatory read-only ``audit-e2e-archive`` before planning or applying: any fatal finding anywhere in the run
  (status ``INCONSISTENT``) refuses, so archiving can never erase evidence of an inconsistency;
* a dry run on read-only snapshots that applies the adapter's own ``collect_archivable`` and receipt rules
  unchanged, predicts segment-file collisions, and refuses if a database changed while it ran;
* chunked application up to an explicit limit, the reason why archiving stopped, and on any error the progress
  actually committed (read back from the producer's cursor). Archive files are never deleted.
"""
from __future__ import annotations

from contextlib import closing
import gzip
import hashlib
import io
import json
from pathlib import Path
import sqlite3

from .adapters.s070041.durable_stream import canonical, sha256_file
from .adapters.s070041.retention import (collect_archivable, count_receipts, get_meta, prune,
                                         require_independent)
from .archive_e2e import NOTICE_CODES, _fingerprint, _Snapshot, _Unopenable, reconcile_archive_aware

CHUNK_MAX = 20000
AUDIT_ACCEPTED = ('COMPLETE', 'PENDING_DELIVERY', 'RETRY_REQUIRED')
MIRRORED = ('raw_journal', 'pending', 'disposition', 'outbox', 'delivery')


def _registered(root, lane):
    registry = json.loads((root / 'lane_registry.json').read_text(encoding='utf-8'))
    if lane not in registry:
        raise ValueError(f'unknown registered lane: {lane}')
    db_path = root / 'lanes' / lane / 'state.sqlite3'
    if not db_path.is_file():
        raise FileNotFoundError(f'producer state database missing: {db_path}')
    return registry[lane], db_path


class _ReadOnly:
    """Read-only snapshot (``immutable=1`` when quiescent, else ``mode=ro`` which only uses ``-shm``)."""

    def __init__(self, path, label):
        try:
            self.snap = _Snapshot(Path(path).resolve(), label)
        except _Unopenable as exc:
            raise ValueError(f'{label}: refusing read-only open ({exc})') from None
        self.label = label

    def __enter__(self):
        return self.snap.conn

    def __exit__(self, kind, value, traceback):
        self.snap.close()
        if kind is None and not self.snap.stable:
            raise ValueError(f'{self.label} changed during the dry run; stop writers and retry')
        return False


def _files(*db_paths):
    """Fingerprints of the evidence files (database and WAL; ``-shm`` is SQLite coordination state)."""
    return {f'{p}{suffix}': _fingerprint(Path(f'{p}{suffix}'), False) for p in db_paths for suffix in ('', '-wal')}


def _pre_audit(root, consumer_db):
    report = reconcile_archive_aware(root, consumer_db)
    fatal = sorted(code for code in report['issue_counts'] if code not in NOTICE_CODES)
    if report['status'] not in AUDIT_ACCEPTED:
        raise ValueError(f"audit-e2e-archive status is {report['status']} ({', '.join(fatal) or 'see its report'}); "
                         'archiving would erase evidence, refused')
    return {'status': report['status'], 'issue_counts': report['issue_counts']}


def _mirror(db, pos, want):
    """In-memory copy of one chunk's rows with raw_id indexes, so the adapter's own ``collect_archivable``
    rules run unchanged on a read-only snapshot. The producer has no raw_id index on outbox/delivery/pending
    (the adapter creates them only when it prunes); per-row lookups would be O(n) each on the real DB."""
    raw = db.execute('SELECT * FROM raw_journal WHERE file_index >= ? ORDER BY file_index LIMIT ?',
                     (pos, want)).fetchall()
    mem = sqlite3.connect(':memory:')
    try:
        mem.row_factory = sqlite3.Row
        for table in MIRRORED:
            ddl = db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
            if ddl is None:
                raise ValueError(f'producer table missing: {table}')
            mem.execute(ddl[0])
            if table != 'raw_journal':
                mem.execute(f'CREATE INDEX mirror_{table}_raw ON {table}(raw_id)')
        if raw:
            low, high = min(r['id'] for r in raw), max(r['id'] for r in raw)
            for table in MIRRORED:
                rows = raw if table == 'raw_journal' else db.execute(
                    f'SELECT * FROM {table} WHERE raw_id BETWEEN ? AND ?', (low, high)).fetchall()
                if rows:
                    marks = ','.join('?' * len(rows[0].keys()))
                    mem.executemany(f'INSERT INTO {table} VALUES ({marks})', [tuple(r) for r in rows])
        return mem
    except BaseException:
        mem.close()
        raise


def _segment_collision(root, lane, start, items):
    """Predict the adapter's ``export_gzip``: an existing segment file must hold exactly the same bytes."""
    path = root / 'archives' / lane / f'raw_{start:012d}_{start + len(items):012d}.jsonl.gz'
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or not path.is_file():
        raise ValueError(f'Archive segment path exists and is not a regular file: {path.name}')
    buffer = io.BytesIO()
    with gzip.GzipFile(filename='', mode='wb', fileobj=buffer, mtime=0, compresslevel=6) as stream:
        for item in items:
            stream.write((canonical(item) + '\n').encode('utf-8'))
    if hashlib.sha256(buffer.getvalue()).hexdigest() != sha256_file(path):
        raise ValueError(f'Archive segment file collision with different content: {path.name}')


def _blocker(db, pos):
    """Why the contiguous archivable prefix ends at file_index ``pos`` (same order of rules as the adapter)."""
    row = db.execute('SELECT * FROM raw_journal WHERE file_index >= ? ORDER BY file_index LIMIT 1', (pos,)).fetchone()
    if row is None:
        return 'END_OF_JOURNAL'
    if row['file_index'] != pos:
        return 'JOURNAL_GAP'
    if not row['processed']:
        return 'UNPROCESSED'
    if db.execute('SELECT 1 FROM pending WHERE raw_id = ?', (row['id'],)).fetchone():
        return 'PENDING_SEQUENCE_GAP'
    if db.execute('SELECT 1 FROM disposition WHERE raw_id = ?', (row['id'],)).fetchone() is None:
        return 'NO_DISPOSITION'
    outbox = db.execute('SELECT delivered FROM outbox WHERE raw_id = ?', (row['id'],)).fetchone()
    if outbox is not None and not outbox['delivered']:
        return 'NOT_ACKNOWLEDGED'
    return 'NOT_ARCHIVABLE'


def _cursor(db_path):
    """(archived_records, registered segments) as committed in the producer DB."""
    with _ReadOnly(db_path, 'producer') as db:
        total = int(get_meta(db, 'archived_records') or 0)
        table = db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'archive_segments'").fetchone()
        segments = db.execute('SELECT COUNT(*) FROM archive_segments').fetchone()[0] if table else 0
    return total, segments


def _stopped(db_path, pos):
    with _ReadOnly(db_path, 'producer') as db:
        return _blocker(db, pos)


def _dry_run(root, lane, entry, db_path, consumer_db, limit, chunk):
    before = _files(db_path, Path(consumer_db).resolve())
    pre_audit = _pre_audit(root, consumer_db)
    if not Path(entry['source']).is_file() or sha256_file(entry['source']) != entry['sha256']:
        raise ValueError('Source immutable fingerprint not satisfied; refuse archival')
    hot = digest = 0
    with _ReadOnly(db_path, 'producer') as db, _ReadOnly(consumer_db, 'consumer') as receipts:
        start = pos = int(get_meta(db, 'archived_records') or 0)
        while pos - start < limit:
            want = min(chunk, limit - (pos - start))
            with closing(_mirror(db, pos, want)) as chunk_db:
                items = collect_archivable(chunk_db, pos, want)
            if items:
                h, d = count_receipts(receipts, items)
                hot, digest = hot + h, digest + d
                _segment_collision(root, lane, pos, items)
            pos += len(items)
            if len(items) < want:
                break
        stopped = 'LIMIT_REACHED' if pos - start >= limit else _blocker(db, pos)
    if _files(db_path, Path(consumer_db).resolve()) != before:
        raise ValueError('a producer or consumer database changed during the dry run; stop writers and retry')
    return {'status': 'DRY_RUN', 'lane': lane, 'pre_audit': pre_audit, 'archived_total': start,
            'would_archive': pos - start, 'would_archive_total': pos,
            'receipts': {'consumer_hot_rows': hot, 'consumer_digests': digest}, 'stopped': stopped,
            'changed': False, 'apply_required': True}


def archive_producer(root, lane, *, consumer_db, limit=CHUNK_MAX, apply=False, chunk=CHUNK_MAX):
    """Archive at most ``limit`` events of the contiguous, acknowledged and independently receipted prefix."""
    root = Path(root)
    if type(limit) is not int or limit < 1:
        raise ValueError('limit must be a positive integer')
    if type(chunk) is not int or not 1 <= chunk <= CHUNK_MAX:
        raise ValueError(f'chunk must be 1..{CHUNK_MAX}')
    if consumer_db is None or not Path(consumer_db).is_file():
        raise ValueError('an existing independent --consumer-db is required (receipts)')
    entry, db_path = _registered(root, lane)
    require_independent(root, consumer_db)
    if not apply:
        return _dry_run(root, lane, entry, db_path, consumer_db, limit, chunk)
    pre_audit = _pre_audit(root, consumer_db)
    start_total, start_segments = _cursor(db_path)
    archived, segments = 0, []
    try:
        while archived < limit:
            result = prune(root, lane, min(chunk, limit - archived), False, consumer_db)
            if not result['archived_new']:
                break
            archived += result['archived_new']
            segments.append({'archive_file': Path(result['archive_file']).relative_to(root).as_posix(),
                             'records': result['archived_new'], 'sha256': result['archive_sha256'],
                             'bytes': result['archive_bytes']})
    except Exception as exc:
        try:
            total, count = _cursor(db_path)
            progress = (f'committed by this run: {total - start_total} events in {count - start_segments} '
                        f'segments; archived_total is now {total}')
        except Exception as again:  # report what is known, never a guess
            progress = f'committed progress could not be read back ({again})'
        raise ValueError(f'{exc} ({progress}; every committed segment is complete and auditable)') from exc
    total, _ = _cursor(db_path)
    if total - start_total != archived:
        raise ValueError(f'producer cursor moved by {total - start_total}, expected {archived}; audit the run')
    return {'status': 'ARCHIVED' if archived else 'NOTHING_ARCHIVABLE', 'lane': lane, 'pre_audit': pre_audit,
            'archived_new': archived, 'archived_total': total, 'segments': segments,
            'stopped': 'LIMIT_REACHED' if archived >= limit else _stopped(db_path, total),
            'changed': bool(archived), 'archive_files_kept': True,
            'note': 'database pages are reused; the file shrinks only after a VACUUM (not performed)'}
