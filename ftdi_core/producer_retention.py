"""Core front-end for the receipt-gated producer archive (``archive-producer``). Dry run unless ``apply``.

The destructive work is done by the maintained S07 adapter ``prune`` (verified gzip segment, SHA-256 checked,
rows deleted in the same transaction that registers the segment). This module adds what the Core CLI requires
(ADR-003): a read-only dry run that applies the same archivability and receipt rules, chunked application up to
an explicit limit, and an explicit reason why archiving stopped. Archive files are never deleted.
"""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3

from .adapters.s070041.durable_stream import sha256_file
from .adapters.s070041.retention import collect_archivable, count_receipts, get_meta, prune
from .archive_e2e import _Snapshot, _Unopenable

CHUNK_MAX = 20000


def _registered(root, lane):
    registry = json.loads((root / 'lane_registry.json').read_text(encoding='utf-8'))
    if lane not in registry:
        raise ValueError(f'unknown registered lane: {lane}')
    db_path = root / 'lanes' / lane / 'state.sqlite3'
    if not db_path.is_file():
        raise FileNotFoundError(f'producer state database missing: {db_path}')
    return registry[lane], db_path


class _ReadOnly:
    """Strict read-only snapshot (touches no file when quiescent); refuses an unstable read."""

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


MIRRORED = ('raw_journal', 'pending', 'disposition', 'outbox', 'delivery')


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


def _stopped(db_path, pos, archived_in_run, limit):
    if archived_in_run >= limit:
        return 'LIMIT_REACHED'
    with _ReadOnly(db_path, 'producer') as db:
        return _blocker(db, pos)


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
    if not apply:
        if not Path(entry['source']).is_file() or sha256_file(entry['source']) != entry['sha256']:
            raise ValueError('Source immutable fingerprint not satisfied; refuse archival')
        hot = digest = 0
        with _ReadOnly(db_path, 'producer') as db, _ReadOnly(consumer_db, 'consumer') as receipts:
            start = pos = int(get_meta(db, 'archived_records') or 0)
            while pos - start < limit:
                want = min(chunk, limit - (pos - start))
                with closing(_mirror(db, pos, want)) as chunk_db:
                    items = collect_archivable(chunk_db, pos, want)
                h, d = count_receipts(receipts, items)
                hot, digest = hot + h, digest + d
                pos += len(items)
                if len(items) < want:
                    break
            stopped = 'LIMIT_REACHED' if pos - start >= limit else _blocker(db, pos)
        return {'status': 'DRY_RUN', 'lane': lane, 'archived_total': start, 'would_archive': pos - start,
                'would_archive_total': pos, 'receipts': {'consumer_hot_rows': hot, 'consumer_digests': digest},
                'stopped': stopped, 'changed': False, 'apply_required': True}
    archived, segments, total = 0, [], None
    try:
        while archived < limit:
            result = prune(root, lane, min(chunk, limit - archived), False, consumer_db)
            total = result['archived_total']
            if not result['archived_new']:
                break
            archived += result['archived_new']
            segments.append({'archive_file': Path(result['archive_file']).relative_to(root).as_posix(),
                             'records': result['archived_new'], 'sha256': result['archive_sha256'],
                             'bytes': result['archive_bytes']})
    except Exception as exc:
        raise ValueError(f'{exc} (this run archived {archived} events in {len(segments)} committed segments '
                         f'before stopping)') from exc
    return {'status': 'ARCHIVED' if archived else 'NOTHING_ARCHIVABLE', 'lane': lane, 'archived_new': archived,
            'archived_total': total, 'segments': segments, 'stopped': _stopped(db_path, total, archived, limit),
            'changed': bool(archived), 'archive_files_kept': True,
            'note': 'database pages are reused; the file shrinks only after a VACUUM (not performed)'}
