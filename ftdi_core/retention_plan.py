"""FTDI-Core Task 002: read-only planner for consumer idempotency retention (dry run only).

Design: ``docs/TASK002_CONSUMER_RETENTION_DESIGN.md`` (ADR-007, PROPOSED).  This module
never deletes, compacts or VACUUMs anything; it reports what a future, separately approved
compactor could safely cover:

* per ``(lane, epoch)``: applied range, contiguity and the first hole, the contiguous high;
* optionally the producer's low-unacked sequence (normal-retry window) from a run root;
* the proposed content watermark ``C = min(contiguous_high, producer_low_unacked - 1) - margin``;
* rows and bytes at or below ``C`` (estimates; reclaiming file space would also need VACUUM).

Databases are opened with the same read-only snapshot rules as ``audit-e2e-archive``
(``immutable=1`` when quiescent; refusing opens that would create sidecars).  The only write
is the optional report.  The plan is advisory: a compactor must re-verify everything itself.
"""
from __future__ import annotations

import os
from pathlib import Path

from .archive_e2e import (EFFECT_COLUMNS, ArchiveLimits, _fingerprint, _load_registry, _norm,
                          _plain_table, _Snapshot, _Unopenable, _write_report)

PLAN_SCHEMA = 'FTDI-Core-Consumer-Retention-Plan-v1'
DIGEST_ROW_BYTES_ESTIMATE = 50   # (lane_id, epoch, seq, 32-byte sha) in a WITHOUT ROWID table
NOTES = [
    'Dry run: nothing was deleted, compacted or vacuumed.',
    'Byte figures are estimates from column lengths; the SQLite file only shrinks after an explicit VACUUM.',
    'Correctness of retention does not depend on the producer window; it keeps normal retries on the fast path.',
    'A future compactor must re-verify contiguity and evidence inside its own BEGIN IMMEDIATE transaction.',
]


def _open(path, label):
    try:
        return _Snapshot(path, label)
    except _Unopenable as exc:
        raise ValueError(f'{label}: refusing read-only open ({exc})') from None


def _first_hole(db, lane, epoch, first_seq):
    """Largest contiguous seq from first_seq and the first missing seq (streaming, O(1) memory)."""
    expected = first_seq
    for (seq,) in db.execute('SELECT seq FROM effects WHERE lane = ? AND epoch = ? ORDER BY seq', (lane, epoch)):
        if seq != expected:
            return expected - 1, expected
        expected += 1
    return expected - 1, None


def _producer_windows(producer_root, limits):
    """{scope: (alias, low_unacked or None, reason)} read from the producer, read-only."""
    root = Path(producer_root).resolve()
    _, registry = _load_registry(root / 'lane_registry.json', limits)
    windows, snapshots = {}, []
    try:
        for alias in sorted(registry):
            path = root / 'lanes' / alias / 'state.sqlite3'
            if not path.is_file():
                raise FileNotFoundError(path)
            snap = _open(path, f'lane:{alias}')
            snapshots.append(snap)
            db = snap.conn
            if _plain_table(db, 'outbox', ('lane', 'seq', 'raw_id', 'raw_sha256', 'payload', 'delivered')) or \
                    _plain_table(db, 'lanes', ('lane', 'next_seq', 'integrity')):
                windows[registry[alias]['scope']] = (alias, None, 'producer schema unrecognized')
                continue
            low = db.execute('SELECT MIN(seq) FROM outbox WHERE delivered = 0').fetchone()[0]
            if low is None:
                row = db.execute('SELECT next_seq FROM lanes WHERE lane = ?', (registry[alias]['scope'],)).fetchone()
                low = row[0] if row is not None else None
            reason = None if isinstance(low, int) and not isinstance(low, bool) else 'producer low-unacked unknown'
            windows[registry[alias]['scope']] = (alias, low if reason is None else None, reason)
    finally:
        for snap in snapshots:
            snap.close()
    return windows, [s for s in snapshots], root


def plan_consumer_retention(consumer_db, *, producer_root=None, margin=0, report_path=None, limits=None):
    """Return a dry-run retention plan; raises FileNotFoundError/ValueError on preconditions."""
    if type(margin) is not int or margin < 0:
        raise ValueError('margin must be a non-negative integer')
    limits = limits or ArchiveLimits()
    consumer_db = Path(consumer_db).resolve()
    if not consumer_db.is_file():
        raise FileNotFoundError(consumer_db)
    protected = [Path(f'{consumer_db}{s}') for s in ('', '-wal', '-shm', '-journal')]
    forbidden_dirs = []
    if producer_root is not None:
        root = Path(producer_root).resolve()
        forbidden_dirs = [root / 'lanes', root / 'archives']
        protected.append(root / 'lane_registry.json')
    target = None
    if report_path is not None:
        target = Path(report_path).resolve()
        if target.is_dir() or not target.parent.is_dir():
            raise ValueError(f'invalid report path: {target}')
        key = _norm(target)
        if key in {_norm(p) for p in protected} | {_norm(Path(p).resolve()) for p in protected} or any(
                key.startswith(_norm(d) + os.sep) or key.startswith(_norm(d.resolve()) + os.sep)
                for d in forbidden_dirs if os.path.lexists(d)):
            raise ValueError('report path would overwrite or sit inside evidence')

    # Fingerprint the database and its WAL/journal, never -shm: a read-only WAL reader legitimately
    # updates the shared-memory coordination file (the archive auditor uses the same rule).
    watched = [Path(f'{consumer_db}{s}') for s in ('', '-wal', '-journal')]
    before = [_fingerprint(p, False) for p in watched]
    windows, producer_snapshots, resolved_root = ({}, [], None)
    if producer_root is not None:
        windows, producer_snapshots, resolved_root = _producer_windows(producer_root, limits)
    consumer = _open(consumer_db, 'consumer')
    blockers, lanes = [], []
    try:
        db = consumer.conn
        problem = _plain_table(db, 'effects', EFFECT_COLUMNS)
        unique = set()
        if not problem:
            for index in db.execute('SELECT name, "unique" AS u, partial FROM pragma_index_list(\'effects\')').fetchall():
                if index['u'] and not index['partial']:
                    unique.add(tuple(r['name'] for r in db.execute(
                        'SELECT name FROM pragma_index_info(?) ORDER BY seqno', (index['name'],))))
            if ('lane', 'seq', 'epoch') not in unique:
                problem = 'effects lacks UNIQUE(lane, seq, epoch)'
        if problem:
            blockers.append({'code': 'CONSUMER_SCHEMA_UNRECOGNIZED', 'reason': problem})
        else:
            for group in db.execute('SELECT lane, epoch, MIN(seq) AS lo, MAX(seq) AS hi, COUNT(*) AS n '
                                    'FROM effects GROUP BY lane, epoch ORDER BY lane, epoch').fetchall():
                lanes.append(_plan_lane(db, group, windows, producer_root is not None, margin))
    finally:
        consumer.close()
    after = [_fingerprint(p, False) for p in watched]
    stable = before == after and consumer.stable and all(s.stable for s in producer_snapshots)
    if not stable:
        blockers.append({'code': 'CONCURRENT_MODIFICATION_DETECTED',
                         'reason': 'evidence changed while planning; re-run in a maintenance window'})
    lane_blocked = any(lane['blockers'] for lane in lanes)
    candidates = [lane for lane in lanes if lane['proposed_content_watermark'] is not None]
    status = 'BLOCKED' if blockers or lane_blocked else ('PLANNED' if candidates else 'NOTHING_TO_COMPACT')
    totals = {key: sum(lane[key] for lane in candidates) for key in (
        'rows_at_or_below_watermark', 'payload_bytes_at_or_below_watermark',
        'identity_bytes_at_or_below_watermark', 'estimated_digest_tier_bytes')}
    result = {
        'schema': PLAN_SCHEMA, 'mode': 'DRY_RUN_READ_ONLY', 'destructive_actions_performed': False,
        'status': status, 'consumer_db': str(consumer_db),
        'producer_root': str(resolved_root) if resolved_root else None, 'margin': margin,
        'lanes': lanes, 'totals': totals, 'blockers': blockers,
        'producer_lanes_without_consumer_effects': sorted(
            alias for scope, (alias, _, _) in windows.items() if scope not in {lane['lane'] for lane in lanes}),
        'snapshot_stable': stable, 'notes': NOTES,
    }
    if target is not None:
        _write_report(target, result)
    return result


def _plan_lane(db, group, windows, have_producer, margin):
    lane, epoch, lo, hi, count = group['lane'], group['epoch'], group['lo'], group['hi'], group['n']
    blockers = []
    entry = {'lane': lane, 'epoch': epoch, 'first_seq': lo, 'max_seq': hi, 'rows': count}
    if not (isinstance(lane, str) and all(type(v) is int for v in (epoch, lo, hi, count))):
        entry.update(blockers=['effects rows have unexpected types'], proposed_content_watermark=None)
        return _zero(entry)
    if epoch != 0:
        blockers.append('epoch != 0 is not supported (no verified epoch re-anchoring)')
    contiguous = count == hi - lo + 1
    contiguous_high, first_missing = (hi, None) if contiguous else _first_hole(db, lane, epoch, lo)
    entry.update(contiguous=contiguous, first_missing_seq=first_missing, contiguous_high=contiguous_high)
    low_unacked = None
    if have_producer:
        alias, low_unacked, reason = windows.get(lane, (None, None, 'lane not registered in the producer root'))
        entry['producer_alias'] = alias
        if reason:
            blockers.append(reason)
    entry['producer_low_unacked'] = low_unacked
    candidate = contiguous_high if low_unacked is None else min(contiguous_high, low_unacked - 1)
    candidate -= margin
    watermark = candidate if candidate >= lo and not blockers else None
    entry['proposed_content_watermark'] = watermark
    entry['blockers'] = blockers
    if watermark is None:
        return _zero(entry)
    row = db.execute('SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0), '
                     'COALESCE(SUM(LENGTH(event_id) + LENGTH(payload_sha256) + LENGTH(lane)), 0) '
                     'FROM effects WHERE lane = ? AND epoch = ? AND seq <= ?', (lane, epoch, watermark)).fetchone()
    entry.update(rows_at_or_below_watermark=row[0], payload_bytes_at_or_below_watermark=row[1],
                 identity_bytes_at_or_below_watermark=row[2],
                 estimated_digest_tier_bytes=row[0] * DIGEST_ROW_BYTES_ESTIMATE)
    return entry


def _zero(entry):
    entry.update(rows_at_or_below_watermark=0, payload_bytes_at_or_below_watermark=0,
                 identity_bytes_at_or_below_watermark=0, estimated_digest_tier_bytes=0)
    return entry
