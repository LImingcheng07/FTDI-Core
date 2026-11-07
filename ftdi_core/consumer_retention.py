"""FTDI-Core Task 002 phase 1: consumer retention v2 (contract: docs/CONSUMER_RETENTION_V2.md).

Retention moves evidence between tiers and never forgets an identity's digest (ADR-007):
hot ``effects`` rows (seq > C) -> compact ``effect_digests`` + verified, hash-chained gzip
segments (first_seq <= seq <= C).  Every operation is a dry run unless ``apply=True``.

Safety layers:
* ``TieredEffectStore.accept`` answers replays of compacted identities from the digest tier and
  refuses the batch (no ACK) when that tier cannot prove the replay;
* the ``effects_guard_compacted`` trigger makes ANY writer (also the S07 adapter or the frozen
  reference consumer) fail instead of inserting a second effect into a compacted range;
* compaction holds one ``BEGIN IMMEDIATE`` per segment: rows are read, archived, fsynced and
  re-verified before digests are written and the rows deleted in the same transaction (an
  orphan file is preferable to data loss); rehydration reverses it and never deletes archives.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3

from .adapters.s070041.durable_stream import canonical
from .adapters.s070041.external_delivery import Conflict, EffectStore, batch_digest, validate
from .archive_e2e import (EFFECT_COLUMNS, ArchiveLimits, _effect_valid, _gzip_lines, _load_registry,
                          _plain_table, _redirected, _Reject, _Snapshot, _Unopenable)

SCHEMA_V2 = 'FTDI-Consumer-v2'
CHAIN_TAG = 'FTDI-Consumer-Segment-v1'
GENESIS = '0' * 64
MAX_SEGMENT_RECORDS = 20000
TRIGGER_NAME = 'effects_guard_compacted'
SEGMENT_KEYS = frozenset({'event_id', 'lane', 'seq', 'epoch', 'payload_sha256', 'payload'})
V2_COLUMNS = {
    'consumer_meta': ('k', 'v'),
    'lane_ids': ('lane_id', 'lane'),
    'retention_state': ('lane', 'epoch', 'first_seq', 'content_watermark', 'digest_watermark', 'segments',
                        'chain_head'),
    'effect_digests': ('lane_id', 'epoch', 'seq', 'payload_sha256'),
    'effect_segments': ('lane', 'epoch', 'start_seq', 'end_seq', 'record_count', 'relative_path', 'sha256',
                        'prev_chain', 'chain'),
}
TRIGGER_SQL = (f"CREATE TRIGGER {TRIGGER_NAME} BEFORE INSERT ON effects "
               "WHEN EXISTS (SELECT 1 FROM retention_state r WHERE r.lane = NEW.lane AND r.epoch = NEW.epoch "
               "AND NEW.seq BETWEEN r.first_seq AND r.content_watermark) "
               "OR EXISTS (SELECT 1 FROM lane_ids l JOIN effect_digests d ON d.lane_id = l.lane_id "
               "WHERE l.lane = NEW.lane AND d.epoch = NEW.epoch AND d.seq = NEW.seq) "
               "BEGIN SELECT RAISE(ABORT, 'FTDI: insert into a compacted range'); END")
DDL = [
    'CREATE TABLE consumer_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL)',
    'CREATE TABLE lane_ids(lane_id INTEGER PRIMARY KEY, lane TEXT NOT NULL UNIQUE)',
    'CREATE TABLE retention_state(lane TEXT NOT NULL, epoch INTEGER NOT NULL, first_seq INTEGER NOT NULL, '
    'content_watermark INTEGER NOT NULL, digest_watermark INTEGER, segments INTEGER NOT NULL, '
    'chain_head TEXT NOT NULL, PRIMARY KEY(lane, epoch))',
    'CREATE TABLE effect_digests(lane_id INTEGER NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL, '
    'payload_sha256 BLOB NOT NULL CHECK(length(payload_sha256) = 32), PRIMARY KEY(lane_id, epoch, seq)) '
    'WITHOUT ROWID',
    'CREATE TABLE effect_segments(lane TEXT NOT NULL, epoch INTEGER NOT NULL, start_seq INTEGER NOT NULL, '
    'end_seq INTEGER NOT NULL, record_count INTEGER NOT NULL, relative_path TEXT NOT NULL, sha256 TEXT NOT NULL, '
    'prev_chain TEXT NOT NULL, chain TEXT NOT NULL, PRIMARY KEY(lane, epoch, start_seq))',
    TRIGGER_SQL,
]
CRASH_STAGES = ('after_segment_file', 'after_commit')
_HEX64 = re.compile(r'[0-9a-f]{64}')


class TierIntegrityError(Exception):
    """A compacted tier cannot prove a replay (e.g. digest row missing): refuse, never ACK."""


# -- shared helpers (also used by the auditor) --------------------------------------------
def archive_root(consumer_db):
    return Path(f'{Path(consumer_db).resolve()}.archive')


def segment_relative_path(lane_id, epoch, start_seq, end_seq):
    return f'lane-{lane_id}/effects_e{epoch}_{start_seq:012d}_{end_seq:012d}.jsonl.gz'


def segment_chain(prev_chain, lane, epoch, start_seq, end_seq, record_count, sha256):
    material = canonical([CHAIN_TAG, prev_chain, lane, epoch, start_seq, end_seq, record_count, sha256])
    return hashlib.sha256(material.encode('utf-8')).hexdigest()


def segment_bytes(rows):
    """Deterministic one-member gzip of canonical effect rows (dicts with SEGMENT_KEYS)."""
    buffer = io.BytesIO()
    with gzip.GzipFile(filename='', mode='wb', fileobj=buffer, mtime=0, compresslevel=6) as zipped:
        for row in rows:
            zipped.write((canonical(row) + '\n').encode('utf-8'))
    return buffer.getvalue()


def parse_segment(data, *, lane, epoch, start_seq, end_seq, limits=None):
    """Strictly decode a segment; returns rows or raises _Reject (CONSUMER_SEGMENT_* codes)."""
    limits = limits or ArchiveLimits()
    rows, expected = [], start_seq
    try:
        for line in _gzip_lines(data, limits):
            try:
                text = line.decode('utf-8')
                row = json.loads(text)
            except (UnicodeDecodeError, ValueError, RecursionError):
                raise _Reject('CONSUMER_SEGMENT_RECORD_INVALID', reason='not UTF-8 JSON', seq=expected) from None
            if not isinstance(row, dict) or set(row) != SEGMENT_KEYS or canonical(row) != text:
                raise _Reject('CONSUMER_SEGMENT_RECORD_INVALID', reason='unexpected or non-canonical record',
                              seq=expected)
            if row['lane'] != lane or row['epoch'] != epoch or row['seq'] != expected or expected > end_seq:
                raise _Reject('CONSUMER_SEGMENT_RECORD_INVALID', reason='record outside the segment identity',
                              seq=expected)
            if not _effect_valid(row):
                raise _Reject('CONSUMER_SEGMENT_RECORD_INVALID', reason='effect identity/payload invalid', seq=expected)
            rows.append(row)
            expected += 1
    except _Reject as rejection:
        if rejection.code.startswith('ARCHIVE_'):
            raise _Reject('CONSUMER_SEGMENT_UNREADABLE', reason=rejection.code, **rejection.detail) from None
        raise
    if expected != end_seq + 1:
        raise _Reject('CONSUMER_SEGMENT_RECORD_INVALID', reason='record count differs from the manifest range')
    return rows


def validate_segment_bytes(data, *, lane, epoch, start_seq, end_seq, limits=None):
    """Refuse a segment that the bounded audit/rehydrate reader could not read back."""
    limits = limits or ArchiveLimits()
    if len(data) > limits.max_compressed_bytes:
        raise ValueError(f'segment [{start_seq}, {end_seq}] would be {len(data)} compressed bytes, above the '
                         f'{limits.max_compressed_bytes}-byte audit limit; use a smaller segment_size')
    try:
        parse_segment(data, lane=lane, epoch=epoch, start_seq=start_seq, end_seq=end_seq, limits=limits)
    except _Reject as rejection:
        raise ValueError(f'segment [{start_seq}, {end_seq}] would not be readable under the audit limits: '
                         f'{rejection.code} {rejection.detail}') from None


def _confined(directory):
    """Refuse an archive directory reached through a symlink/junction (writes must stay in the archive root)."""
    if os.path.lexists(directory) and _redirected(directory):
        raise ValueError(f'archive directory is redirected (symlink/junction): {directory}')


def v1_problem(db):
    """None if ``effects`` is the recognized v1 table with both unique constraints, else a reason."""
    problem = _plain_table(db, 'effects', EFFECT_COLUMNS)
    if problem:
        return problem
    unique = set()
    for index in db.execute('SELECT name, "unique" AS u, partial FROM pragma_index_list(\'effects\')').fetchall():
        if index['u'] and not index['partial']:
            unique.add(tuple(r['name'] for r in db.execute(
                'SELECT name FROM pragma_index_info(?) ORDER BY seqno', (index['name'],))))
    if ('event_id',) not in unique or ('lane', 'seq', 'epoch') not in unique:
        return 'effects lacks UNIQUE(event_id) or UNIQUE(lane, seq, epoch)'
    return None


def has_v2_objects(db):
    return db.execute("SELECT 1 FROM sqlite_master WHERE lower(name) = 'consumer_meta'").fetchone() is not None


def v2_table_problem(db):
    """None if every v2 table and the schema marker are exactly as specified."""
    for table, columns in V2_COLUMNS.items():
        problem = _plain_table(db, table, columns)
        if problem:
            return f'{table}: {problem}'
    row = db.execute("SELECT v FROM consumer_meta WHERE k = 'schema'").fetchone()
    if row is None or row[0] != SCHEMA_V2:
        return 'consumer_meta schema marker missing or wrong'
    return None


def trigger_problem(db):
    trigger = db.execute("SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
                         (TRIGGER_NAME,)).fetchone()
    if trigger is None or ' '.join(str(trigger[0]).split()) != ' '.join(TRIGGER_SQL.split()):
        return f'trigger {TRIGGER_NAME} missing or altered'
    return None


def v2_problem(db):
    """None if every v2 object is exactly as specified (tables, trigger, schema marker)."""
    return v2_table_problem(db) or trigger_problem(db)


def _snapshot(path):
    try:
        return _Snapshot(Path(path).resolve(), 'consumer')
    except _Unopenable as exc:
        raise ValueError(f'consumer: refusing read-only open ({exc})') from None


def _writer(path):
    db = sqlite3.connect(str(Path(path).resolve()), timeout=30, isolation_level=None)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=30000')
        db.execute('PRAGMA synchronous=FULL')
        return db
    except BaseException:
        db.close()
        raise


def _existing(path):
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


# -- migration -----------------------------------------------------------------------------
def migrate_consumer_v2(consumer_db, *, apply=False):
    path = _existing(consumer_db)
    snap = _snapshot(path)
    try:
        problem = v1_problem(snap.conn)
        already = has_v2_objects(snap.conn)
        v2 = v2_problem(snap.conn) if already else None
    finally:
        snap.close()
    if problem:
        raise ValueError(f'unrecognized v1 consumer schema: {problem}')
    if already:
        if v2:
            raise ValueError(f'partial or altered v2 schema: {v2}')
        return {'status': 'ALREADY_V2', 'consumer_db': str(path), 'schema': SCHEMA_V2}
    if not apply:
        return {'status': 'DRY_RUN', 'consumer_db': str(path), 'would_execute': DDL + [
            f"INSERT INTO consumer_meta VALUES ('schema', '{SCHEMA_V2}')"]}
    db = _writer(path)
    try:
        db.execute('BEGIN IMMEDIATE')
        try:
            if v1_problem(db) or has_v2_objects(db):
                raise ValueError('consumer schema changed concurrently; migration aborted')
            for statement in DDL:
                db.execute(statement)
            db.execute('INSERT INTO consumer_meta(k, v) VALUES (?, ?)', ('schema', SCHEMA_V2))
            db.execute('COMMIT')
        except BaseException:
            if db.in_transaction:
                db.execute('ROLLBACK')
            raise
    finally:
        db.close()
    return {'status': 'MIGRATED', 'consumer_db': str(path), 'schema': SCHEMA_V2}


# -- compaction ----------------------------------------------------------------------------
def _producer_low_unacked(producer_root, lane):
    root = Path(producer_root).resolve()
    _, registry = _load_registry(root / 'lane_registry.json', ArchiveLimits())
    aliases = [alias for alias, record in registry.items() if record['scope'] == lane]
    if not aliases:
        raise ValueError('lane is not registered in the producer root')
    path = root / 'lanes' / aliases[0] / 'state.sqlite3'
    if not path.is_file():
        raise FileNotFoundError(path)
    try:
        snap = _Snapshot(path, 'producer')
    except _Unopenable as exc:
        raise ValueError(f'producer: refusing read-only open ({exc})') from None
    try:
        low = snap.conn.execute('SELECT MIN(seq) FROM outbox WHERE delivered = 0').fetchone()[0]
        if low is None:
            row = snap.conn.execute('SELECT next_seq FROM lanes WHERE lane = ?', (lane,)).fetchone()
            low = row[0] if row is not None else None
    finally:
        snap.close()
    if type(low) is not int:
        raise ValueError('producer low-unacked sequence is unknown')
    return aliases[0], low


def _row_dict(row):
    return {key: row[key] for key in EFFECT_COLUMNS}


def _check_range(db, lane, epoch, start, end):
    """Rows [start, end] as dicts, refusing holes, out-of-range and invalid rows."""
    rows = [_row_dict(r) for r in db.execute(
        'SELECT event_id, lane, seq, epoch, payload_sha256, payload FROM effects '
        'WHERE lane = ? AND epoch = ? AND seq BETWEEN ? AND ? ORDER BY seq', (lane, epoch, start, end))]
    if len(rows) != end - start + 1 or [r['seq'] for r in rows] != list(range(start, end + 1)):
        raise ValueError(f'range [{start}, {end}] has a hole or exceeds the applied effects')
    for row in rows:
        if not _effect_valid(row):
            raise ValueError(f'effect seq {row["seq"]} is invalid (identity/payload hash/canonical text)')
    return rows


def _plan(db, lane, epoch, watermark, segment_size):
    problem = v2_problem(db) if has_v2_objects(db) else 'consumer DB is not migrated to v2'
    if problem:
        raise ValueError(problem)
    state = db.execute('SELECT * FROM retention_state WHERE lane = ? AND epoch = ?', (lane, epoch)).fetchone()
    if state is not None:
        start = state['content_watermark'] + 1
    else:
        start = db.execute('SELECT MIN(seq) FROM effects WHERE lane = ? AND epoch = ?', (lane, epoch)).fetchone()[0]
        if start is None:
            raise ValueError('lane has no effects')
    if watermark < start:
        raise ValueError(f'watermark {watermark} is not above the current compacted range (next seq {start})')
    count = db.execute('SELECT COUNT(*) FROM effects WHERE lane = ? AND epoch = ? AND seq BETWEEN ? AND ?',
                       (lane, epoch, start, watermark)).fetchone()[0]
    if count != watermark - start + 1:
        raise ValueError(f'range [{start}, {watermark}] has a hole or exceeds the applied effects')
    chunks = [(s, min(s + segment_size - 1, watermark)) for s in range(start, watermark + 1, segment_size)]
    return state, chunks


def compact_consumer(consumer_db, *, lane, epoch=0, content_watermark, producer_root=None,
                     ignore_producer_window=False, apply=False, segment_size=MAX_SEGMENT_RECORDS, crash_stage=None):
    path = _existing(consumer_db)
    if not isinstance(lane, str) or not lane:
        raise ValueError('lane must be the exact lane scope string')
    if type(epoch) is not int or epoch != 0:
        raise ValueError('only epoch 0 is supported (no verified epoch re-anchoring)')
    if type(content_watermark) is not int:
        raise ValueError('content_watermark must be an integer')
    if type(segment_size) is not int or not 1 <= segment_size <= MAX_SEGMENT_RECORDS:
        raise ValueError(f'segment_size must be 1..{MAX_SEGMENT_RECORDS}')
    if crash_stage not in (None,) + CRASH_STAGES:
        raise ValueError(f'unknown crash_stage {crash_stage!r}')
    producer = None
    if producer_root is not None:
        alias, low = _producer_low_unacked(producer_root, lane)
        if content_watermark >= low:
            raise ValueError(f'watermark {content_watermark} is not below the producer low-unacked seq {low}')
        producer = {'alias': alias, 'low_unacked': low}
    elif not ignore_producer_window:
        raise ValueError('give producer_root, or ignore_producer_window=True explicitly')

    snap = _snapshot(path)
    try:
        state, chunks = _plan(snap.conn, lane, epoch, content_watermark, segment_size)
        lane_row = snap.conn.execute('SELECT lane_id FROM lane_ids WHERE lane = ?', (lane,)).fetchone()
        lane_id = lane_row['lane_id'] if lane_row else snap.conn.execute(
            'SELECT COALESCE(MAX(lane_id), 0) + 1 FROM lane_ids').fetchone()[0]
        rows = payload_bytes = 0
        for start, end in chunks:                 # validate everything first, one segment in memory at a time
            chunk = _check_range(snap.conn, lane, epoch, start, end)
            rows += len(chunk)
            payload_bytes += sum(len(r['payload']) for r in chunk)
            data = segment_bytes(chunk)
            validate_segment_bytes(data, lane=lane, epoch=epoch, start_seq=start, end_seq=end)
            target = archive_root(path) / segment_relative_path(lane_id, epoch, start, end)
            _confined(archive_root(path))
            _confined(target.parent)
            if os.path.lexists(target) and (not target.is_file() or target.read_bytes() != data):
                raise ValueError(f'existing archive file has different content: {target.name}')
    finally:
        snap.close()
    planned = [{'start_seq': s, 'end_seq': e, 'record_count': e - s + 1} for s, e in chunks]
    if not apply:
        return {'status': 'DRY_RUN', 'consumer_db': str(path), 'lane': lane, 'epoch': epoch,
                'current_content_watermark': state['content_watermark'] if state else None,
                'content_watermark': content_watermark, 'segments': planned, 'rows': rows,
                'payload_bytes': payload_bytes, 'producer_window': producer,
                'lane_id': lane_id}

    written = []
    db = _writer(path)
    try:
        lane_id = _allocate_lane_id(db, lane)
        for start, end in chunks:
            written.append(_compact_segment(db, path, lane, epoch, start, end, lane_id, crash_stage))
    finally:
        db.close()
    final = written[-1]
    return {'status': 'COMPACTED', 'consumer_db': str(path), 'lane': lane, 'epoch': epoch,
            'content_watermark': content_watermark, 'segments': written, 'rows': sum(w['record_count'] for w in written),
            'chain_head': final['chain'], 'producer_window': producer}


def _allocate_lane_id(db, lane):
    """Committed on its own: a crash after a segment file is written cannot hand this id to another lane."""
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT lane_id FROM lane_ids WHERE lane = ?', (lane,)).fetchone()
        lane_id = row['lane_id'] if row else db.execute('INSERT INTO lane_ids(lane) VALUES (?)', (lane,)).lastrowid
        db.execute('COMMIT')
    except BaseException:
        if db.in_transaction:
            db.execute('ROLLBACK')
        raise
    return lane_id


def _compact_segment(db, path, lane, epoch, start, end, lane_id, crash_stage):
    db.execute('BEGIN IMMEDIATE')
    try:
        state = db.execute('SELECT * FROM retention_state WHERE lane = ? AND epoch = ?', (lane, epoch)).fetchone()
        expected_start = state['content_watermark'] + 1 if state else start
        lane_row = db.execute('SELECT lane_id FROM lane_ids WHERE lane = ?', (lane,)).fetchone()
        if v2_problem(db) or expected_start != start or lane_row is None or lane_row['lane_id'] != lane_id:
            raise ValueError('consumer changed concurrently; compaction aborted')
        rows = _check_range(db, lane, epoch, start, end)
        data = segment_bytes(rows)
        validate_segment_bytes(data, lane=lane, epoch=epoch, start_seq=start, end_seq=end)
        sha = hashlib.sha256(data).hexdigest()
        relative = segment_relative_path(lane_id, epoch, start, end)
        final = archive_root(path) / relative
        _write_segment_file(final, data, lane, epoch, start, end)
        if crash_stage == 'after_segment_file':
            os._exit(86)
        prev = state['chain_head'] if state else GENESIS
        chain = segment_chain(prev, lane, epoch, start, end, len(rows), sha)
        db.executemany('INSERT INTO effect_digests(lane_id, epoch, seq, payload_sha256) VALUES (?, ?, ?, ?)',
                       [(lane_id, epoch, r['seq'], bytes.fromhex(r['payload_sha256'])) for r in rows])
        db.execute('INSERT INTO effect_segments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (lane, epoch, start, end, len(rows), relative, sha, prev, chain))
        if state is None:
            db.execute('INSERT INTO retention_state VALUES (?, ?, ?, ?, NULL, 1, ?)', (lane, epoch, start, end, chain))
        else:
            db.execute('UPDATE retention_state SET content_watermark = ?, segments = segments + 1, chain_head = ? '
                       'WHERE lane = ? AND epoch = ?', (end, chain, lane, epoch))
        deleted = db.execute('DELETE FROM effects WHERE lane = ? AND epoch = ? AND seq BETWEEN ? AND ?',
                             (lane, epoch, start, end)).rowcount
        if deleted != len(rows):
            raise ValueError('row count changed while compacting; aborted')
        db.execute('COMMIT')
    except BaseException:
        if db.in_transaction:
            db.execute('ROLLBACK')
        raise
    if crash_stage == 'after_commit':
        os._exit(86)
    return {'start_seq': start, 'end_seq': end, 'record_count': len(rows), 'relative_path': relative,
            'sha256': sha, 'prev_chain': prev, 'chain': chain}


def _write_segment_file(final, data, lane, epoch, start, end):
    """Write, fsync and re-verify; reuse an identical existing file (crash retry), refuse a different one."""
    root = final.parent.parent
    _confined(root)
    final.parent.mkdir(parents=True, exist_ok=True)
    _confined(final.parent)
    if os.path.lexists(final):
        if os.path.islink(final) or not final.is_file() or final.read_bytes() != data:
            raise ValueError(f'existing archive file has different content: {final.name}')
        return                                      # identical bytes, already validated by the caller
    temporary = final.with_name(final.name + '.tmp')
    with open(temporary, 'wb') as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        written = temporary.read_bytes()
        if written != data:
            raise ValueError('archive file did not read back identically')
        parse_segment(written, lane=lane, epoch=epoch, start_seq=start, end_seq=end)
        os.replace(temporary, final)
    finally:
        if temporary.exists():
            temporary.unlink()
    if final.read_bytes() != data:
        raise ValueError('archive file changed after write')


# -- rehydration ---------------------------------------------------------------------------
def _verified_segments(db, path, lane, epoch, state):
    """All segments of (lane, epoch), chain-verified from genesis, with their parsed rows lazily."""
    segments = [dict(r) for r in db.execute('SELECT * FROM effect_segments WHERE lane = ? AND epoch = ? '
                                            'ORDER BY start_seq', (lane, epoch))]
    expected, prev = state['first_seq'], GENESIS
    lane_row = db.execute('SELECT lane_id FROM lane_ids WHERE lane = ?', (lane,)).fetchone()
    if lane_row is None:
        raise ValueError('lane_ids entry missing for a compacted lane')
    for seg in segments:
        if (seg['start_seq'] != expected or seg['record_count'] != seg['end_seq'] - seg['start_seq'] + 1
                or seg['prev_chain'] != prev
                or seg['chain'] != segment_chain(prev, lane, epoch, seg['start_seq'], seg['end_seq'],
                                                 seg['record_count'], seg['sha256'])
                or seg['relative_path'] != segment_relative_path(lane_row['lane_id'], epoch, seg['start_seq'],
                                                                  seg['end_seq'])):
            raise ValueError(f'segment manifest/chain does not verify at seq {seg["start_seq"]}')
        expected, prev = seg['end_seq'] + 1, seg['chain']
    if expected - 1 != state['content_watermark'] or prev != state['chain_head'] or len(segments) != state['segments']:
        raise ValueError('segments do not tile [first_seq, C] or chain_head differs')
    return lane_row['lane_id'], segments


def _segment_rows(path, lane, epoch, seg, limits=None):
    limits = limits or ArchiveLimits()
    file = archive_root(path) / seg['relative_path']
    _confined(archive_root(path))
    _confined(file.parent)
    if os.path.islink(file) or not file.is_file():
        raise ValueError(f'archive segment missing or not a regular file: {seg["relative_path"]}')
    if file.stat().st_size > limits.max_compressed_bytes:
        raise ValueError(f'archive segment exceeds the {limits.max_compressed_bytes}-byte limit')
    with open(file, 'rb') as handle:
        data = handle.read(limits.max_compressed_bytes + 1)
    if hashlib.sha256(data).hexdigest() != seg['sha256']:
        raise ValueError(f'archive segment SHA-256 mismatch: {seg["relative_path"]}')
    try:
        return parse_segment(data, lane=lane, epoch=epoch, start_seq=seg['start_seq'], end_seq=seg['end_seq'],
                             limits=limits)
    except _Reject as rejection:
        raise ValueError(f'archive segment unreadable: {rejection.code} {rejection.detail}') from None


def _check_digests(db, lane_id, epoch, rows):
    for row in rows:
        found = db.execute('SELECT payload_sha256 FROM effect_digests WHERE lane_id = ? AND epoch = ? AND seq = ?',
                           (lane_id, epoch, row['seq'])).fetchone()
        if found is None or not isinstance(found[0], bytes) or found[0].hex() != row['payload_sha256']:
            raise ValueError(f'digest tier disagrees with the archive at seq {row["seq"]}')


def rehydrate_consumer(consumer_db, *, lane, epoch=0, to_watermark=None, apply=False):
    path = _existing(consumer_db)
    snap = _snapshot(path)
    try:
        if not has_v2_objects(snap.conn) or v2_problem(snap.conn):
            raise ValueError('consumer DB is not a recognized v2 database')
        state = snap.conn.execute('SELECT * FROM retention_state WHERE lane = ? AND epoch = ?',
                                  (lane, epoch)).fetchone()
        if state is None:
            return {'status': 'NOTHING_TO_DO', 'consumer_db': str(path), 'lane': lane, 'epoch': epoch}
        lane_id, segments = _verified_segments(snap.conn, path, lane, epoch, state)
        requested = state['first_seq'] - 1 if to_watermark is None else to_watermark
        if type(requested) is not int or not state['first_seq'] - 1 <= requested < state['content_watermark']:
            raise ValueError(f'to_watermark must be in [{state["first_seq"] - 1}, {state["content_watermark"] - 1}]')
        boundaries = {state['first_seq'] - 1} | {seg['end_seq'] for seg in segments}
        target = max(b for b in boundaries if b <= requested)   # round down: restore at least what was asked
        restore = [seg for seg in reversed(segments) if seg['start_seq'] > target]
        for seg in restore:                              # verify files AND digests before changing anything
            _check_digests(snap.conn, lane_id, epoch, _segment_rows(path, lane, epoch, seg))
    finally:
        snap.close()
    plan = [{'start_seq': s['start_seq'], 'end_seq': s['end_seq'], 'record_count': s['record_count']} for s in restore]
    if not apply:
        return {'status': 'DRY_RUN', 'consumer_db': str(path), 'lane': lane, 'epoch': epoch,
                'content_watermark': state['content_watermark'], 'requested_to_watermark': requested,
                'to_watermark': target, 'segments': plan}
    db = _writer(path)
    try:
        for seg in restore:
            db.execute('BEGIN IMMEDIATE')
            try:
                current = db.execute('SELECT * FROM retention_state WHERE lane = ? AND epoch = ?',
                                     (lane, epoch)).fetchone()
                if current is None or current['content_watermark'] != seg['end_seq'] or current['chain_head'] != seg['chain']:
                    raise ValueError('consumer changed concurrently; rehydration aborted')
                rows = _segment_rows(path, lane, epoch, seg)
                _check_digests(db, lane_id, epoch, rows)
                if seg['start_seq'] - 1 < current['first_seq']:
                    db.execute('DELETE FROM retention_state WHERE lane = ? AND epoch = ?', (lane, epoch))
                else:
                    db.execute('UPDATE retention_state SET content_watermark = ?, segments = segments - 1, '
                               'chain_head = ? WHERE lane = ? AND epoch = ?',
                               (seg['start_seq'] - 1, seg['prev_chain'], lane, epoch))
                db.execute('DELETE FROM effect_digests WHERE lane_id = ? AND epoch = ? AND seq BETWEEN ? AND ?',
                           (lane_id, epoch, seg['start_seq'], seg['end_seq']))
                db.executemany('INSERT INTO effects(event_id, lane, seq, epoch, payload_sha256, payload) '
                               'VALUES (?, ?, ?, ?, ?, ?)',
                               [tuple(row[k] for k in EFFECT_COLUMNS) for row in rows])
                db.execute('DELETE FROM effect_segments WHERE lane = ? AND epoch = ? AND start_seq = ?',
                           (lane, epoch, seg['start_seq']))
                db.execute('COMMIT')
            except BaseException:
                if db.in_transaction:
                    db.execute('ROLLBACK')
                raise
    finally:
        db.close()
    return {'status': 'REHYDRATED', 'consumer_db': str(path), 'lane': lane, 'epoch': epoch,
            'requested_to_watermark': requested, 'to_watermark': target, 'segments': plan, 'archive_files_kept': True}


# -- tier-aware consumer store -------------------------------------------------------------
class TieredEffectStore(EffectStore):
    """Drop-in for ``EffectStore``: identical on v1 DBs; on v2 answers compacted replays from digests."""

    def accept(self, events):
        if not isinstance(events, list) or not 1 <= len(events) <= 1024:
            raise ValueError('Expected nonempty batch of at most 1024')
        rows = [validate(e) for e in events]
        inserted = duplicated = 0
        self.db.execute('BEGIN IMMEDIATE')
        try:
            v2 = has_v2_objects(self.db)
            states = {}
            for event_id, lane, seq, epoch, digest, payload in rows:
                if v2:
                    if (lane, epoch) not in states:
                        state = self.db.execute('SELECT first_seq, content_watermark FROM retention_state '
                                                'WHERE lane = ? AND epoch = ?', (lane, epoch)).fetchone()
                        lane_row = self.db.execute('SELECT lane_id FROM lane_ids WHERE lane = ?', (lane,)).fetchone()
                        states[(lane, epoch)] = (state, lane_row['lane_id'] if lane_row else None)
                    state, lane_id = states[(lane, epoch)]
                    in_range = state is not None and state['first_seq'] <= seq <= state['content_watermark']
                    found = None if lane_id is None else self.db.execute(
                        'SELECT payload_sha256 FROM effect_digests WHERE lane_id = ? AND epoch = ? AND seq = ?',
                        (lane_id, epoch, seq)).fetchone()
                    # A digest row alone proves the identity was applied, even if retention_state is damaged.
                    if in_range or found is not None:
                        if not in_range:
                            raise TierIntegrityError(f'digest exists outside the compacted range for lane={lane}, '
                                                     f'seq={seq}: retention state inconsistent')
                        if found is None:
                            raise TierIntegrityError(f'compacted digest missing for lane={lane}, seq={seq}')
                        if not isinstance(found[0], bytes) or found[0].hex() != digest:
                            raise Conflict(f'Conflicting event for lane={lane}, seq={seq} (compacted tier)')
                        duplicated += 1
                        continue
                old = self.db.execute('SELECT event_id, payload_sha256 FROM effects WHERE lane = ? AND seq = ? '
                                      'AND epoch = ?', (lane, seq, epoch)).fetchone()
                if old is not None:
                    if old['event_id'] != event_id or old['payload_sha256'] != digest:
                        raise Conflict(f'Conflicting event for lane={lane}, seq={seq}')
                    duplicated += 1
                    continue
                self.db.execute('INSERT INTO effects VALUES (?, ?, ?, ?, ?, ?)',
                                (event_id, lane, seq, epoch, digest, payload))
                inserted += 1
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {'accepted': len(rows), 'inserted': inserted, 'duplicates': duplicated,
                'batch_digest': batch_digest(events)}
