"""Core front-end for the receipt-gated producer archive (``archive-producer``). Dry run unless ``apply``.

The destructive work is done by the maintained S07 adapter ``prune`` (verified gzip segment, SHA-256 checked,
rows deleted in the same transaction that registers the segment, exact delete and table counts, no triggers).
This module adds what the Core CLI requires (ADR-003):

* a mandatory read-only ``audit-e2e-archive`` before planning or applying: unless it is COMPLETE,
  PENDING_DELIVERY or RETRY_REQUIRED the archive is refused, so archiving never erases evidence of an
  inconsistency; on apply, the evidence files must be byte-identical before and after that audit;
* one chunk planner shared by the dry run and every applied chunk: it runs the adapter's own
  ``collect_archivable`` and receipt rules unchanged on a read-only snapshot, validates every record with the
  auditor's own archived-record parser (never write an archive the audit cannot read back), computes the exact
  segment bytes, and predicts collisions, overlapping unreferenced segment files and blocked temp paths;
* on apply, the committed segment must have exactly the planned SHA-256; on any error the progress actually
  committed is read back from the producer cursor. Archive files are never deleted.

Precondition (as for every retention command): an exclusive maintenance window, no other writer.
"""
from __future__ import annotations

from contextlib import closing
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3

from .adapters.s070041.durable_stream import canonical, sha256_file
from .adapters.s070041.retention import (collect_archivable, count_receipts, get_meta, prune,
                                         require_independent, require_no_triggers)
from .archive_e2e import (NOTICE_CODES, _binding_ok, _fingerprint, _parse_item, _published_event, _Reject,
                          _Snapshot, _Unopenable, reconcile_archive_aware)

CHUNK_MAX = 20000
AUDIT_ACCEPTED = ('COMPLETE', 'PENDING_DELIVERY', 'RETRY_REQUIRED')
MIRRORED = ('raw_journal', 'pending', 'disposition', 'outbox', 'delivery')
SEGMENT_NAME = re.compile(r'raw_(\d{12})_(\d{12})\.jsonl\.gz')


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
            raise ValueError(f'{self.label} changed while it was read; stop writers and retry')
        return False


def _files(*db_paths):
    """Content fingerprints of the evidence files (database and WAL; ``-shm`` is coordination state)."""
    return {f'{p}{suffix}': _fingerprint(Path(f'{p}{suffix}'), True) for p in db_paths for suffix in ('', '-wal')}


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


def _segment_bytes(items):
    """Exactly what the adapter's ``export_gzip`` writes; every record must be readable by the auditor."""
    buffer = io.BytesIO()
    with gzip.GzipFile(filename='', mode='wb', fileobj=buffer, mtime=0, compresslevel=6) as stream:
        for item in items:
            try:
                line = canonical(item)
                _parse_item(line.encode('utf-8'), item['file_index'])
                if 'outbox' in item:
                    o = item['outbox']
                    _published_event(o['lane'], o['seq'], o['raw_sha256'], o['payload'])
                    if not _binding_ok(item['payload'], o['payload']):
                        raise ValueError('published event is not the canonical form of its raw record')
            except _Reject as exc:
                raise ValueError(f"record file_index={item.get('file_index')} would not be readable by "
                                 f"audit-e2e-archive ({exc.code} {exc.detail}); refuse archival") from None
            except (TypeError, ValueError, KeyError, RecursionError) as exc:
                raise ValueError(f"record file_index={item.get('file_index')} cannot be archived "
                                 f"({type(exc).__name__}: {exc}); refuse archival") from None
            stream.write((line + '\n').encode('utf-8'))
    return buffer.getvalue()


def _check_destination(root, lane, start, end, data, registered):
    """Predict ``export_gzip`` and the audit after the commit (registered = paths of committed segments)."""
    directory = root / 'archives' / lane
    name = f'raw_{start:012d}_{end:012d}.jsonl.gz'
    path, temp = directory / name, directory / f'{name}.tmp'
    if temp.is_symlink() or (temp.exists() and not temp.is_file()):
        raise ValueError(f'Archive temp path is blocked by a non-file: {temp.name}')
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError(f'Archive segment path exists and is not a regular file: {name}')
    if path.is_file() and sha256_file(path) != hashlib.sha256(data).hexdigest():
        raise ValueError(f'Archive segment file collision with different content: {name}')
    if directory.is_dir():
        for entry in directory.iterdir():
            match = SEGMENT_NAME.fullmatch(entry.name)
            if (match and entry.name != name and entry.name not in registered
                    and int(match.group(1)) < end and start < int(match.group(2))):
                raise ValueError(f'Unreferenced archive file {entry.name} overlaps the range [{start}, {end}); '
                                 'an interrupted run with another --chunk may have left it: retry with that '
                                 'chunk size, or move the file out of archives/ after checking it')


def _plan_chunk(root, lane, db_path, consumer_db, pos, want):
    """Read-only plan of the next chunk: (items, receipts (hot, digest), segment bytes or None, stop reason)."""
    with _ReadOnly(db_path, 'producer') as db, _ReadOnly(consumer_db, 'consumer') as receipts:
        require_no_triggers(db)
        with closing(_mirror(db, pos, want)) as chunk_db:
            items = collect_archivable(chunk_db, pos, want)
        stop = None if len(items) == want else _blocker(db, pos + len(items))
        if not items:
            return items, (0, 0), None, stop
        hot_digest = count_receipts(receipts, items)
        table = db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'archive_segments'").fetchone()
        registered = {Path(r[0]).name for r in db.execute('SELECT archive_path FROM archive_segments')} if table else set()
    data = _segment_bytes(items)
    _check_destination(root, lane, pos, pos + len(items), data, registered)
    return items, hot_digest, data, stop


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


def _source_ok(entry):
    if not Path(entry['source']).is_file() or sha256_file(entry['source']) != entry['sha256']:
        raise ValueError('Source immutable fingerprint not satisfied; refuse archival')


def _dry_run(root, lane, entry, db_path, consumer_db, limit, chunk):
    before = _files(db_path, Path(consumer_db).resolve())
    pre_audit = _pre_audit(root, consumer_db)
    _source_ok(entry)
    start = pos = _cursor(db_path)[0]
    hot = digest = 0
    stopped = 'LIMIT_REACHED'
    while pos - start < limit:
        items, (h, d), _, stop = _plan_chunk(root, lane, db_path, consumer_db, pos, min(chunk, limit - (pos - start)))
        hot, digest, pos = hot + h, digest + d, pos + len(items)
        if stop is not None:
            stopped = stop
            break
    if _files(db_path, Path(consumer_db).resolve()) != before:
        raise ValueError('a producer or consumer database changed during the dry run; stop writers and retry')
    return {'status': 'DRY_RUN', 'lane': lane, 'pre_audit': pre_audit, 'archived_total': start,
            'would_archive': pos - start, 'would_archive_total': pos,
            'receipts': {'consumer_hot_rows': hot, 'consumer_digests': digest}, 'stopped': stopped,
            'changed': False, 'apply_required': True}


def _apply(root, lane, entry, db_path, consumer_db, limit, chunk):
    before = _files(db_path, Path(consumer_db).resolve())
    pre_audit = _pre_audit(root, consumer_db)
    if _files(db_path, Path(consumer_db).resolve()) != before:
        raise ValueError('a producer or consumer database changed during the pre-audit; stop writers and retry')
    _source_ok(entry)
    start_total, start_segments = _cursor(db_path)
    archived, segments, stopped = 0, [], 'LIMIT_REACHED'
    try:
        while archived < limit:
            pos = start_total + archived
            items, _, data, stop = _plan_chunk(root, lane, db_path, consumer_db, pos, min(chunk, limit - archived))
            if items:
                result = prune(root, lane, len(items), False, consumer_db)
                if result['archived_new'] != len(items) or result['archive_sha256'] != hashlib.sha256(data).hexdigest():
                    raise ValueError('the committed segment differs from the planned one (another writer?); '
                                     'audit the run')
                archived += len(items)
                segments.append({'archive_file': Path(result['archive_file']).relative_to(root).as_posix(),
                                 'records': len(items), 'sha256': result['archive_sha256'],
                                 'bytes': result['archive_bytes']})
            if stop is not None:
                stopped = stop
                break
        total, _ = _cursor(db_path)
        if total - start_total != archived:
            raise ValueError(f'producer cursor moved by {total - start_total}, expected {archived}; audit the run')
    except Exception as exc:
        try:
            now, count = _cursor(db_path)
            progress = (f'committed by this run: {now - start_total} events in {count - start_segments} '
                        f'segments; archived_total is now {now}')
        except Exception as again:  # report what is known, never a guess
            progress = f'committed progress could not be read back ({again})'
        raise ValueError(f'{exc} ({progress}; every committed segment is complete and auditable)') from exc
    return {'status': 'ARCHIVED' if archived else 'NOTHING_ARCHIVABLE', 'lane': lane, 'pre_audit': pre_audit,
            'archived_new': archived, 'archived_total': total, 'segments': segments, 'stopped': stopped,
            'changed': bool(archived), 'archive_files_kept': True,
            'note': 'database pages are reused; the file shrinks only after a VACUUM (not performed)'}


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
    run = _apply if apply else _dry_run
    return run(root, lane, entry, db_path, consumer_db, limit, chunk)
