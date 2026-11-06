"""FTDI-S07-004: bounded streaming file -> durable journal -> ordered outbox -> DB sink.

Pure stdlib reference laboratory. Exactly-once ONLY within one SQLite database.
This is NOT a trading connector, exchange recovery adapter, or external delivery service.
"""
from __future__ import annotations
import hashlib
import json
import os
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

SCHEMA_VERSION = 'FTDI-S07-004.1-v1'
STAGES = ('before_raw_commit','after_raw_commit','before_process_commit',
          'after_process_commit','before_delivery_commit','after_delivery_commit')


def sha256_file(path):
    d = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            d.update(chunk)
    return d.hexdigest()


def canonical(e):
    return json.dumps(e, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def scope_of(e):
    return canonical([str(e.get(k, '?')) for k in ('source','symbol','venue','channel')])


def valid(e):
    try:
        s, t = e['source_seq'], e['event_time_ms']
        p, q = Decimal(str(e['price'])), Decimal(str(e['qty']))
        return (type(s) is int and s >= 0 and type(t) is int and t >= 0 and
                p.is_finite() and q.is_finite() and p > 0 and q > 0 and
                all(isinstance(e.get(k), str) and e[k] for k in ('source','symbol','venue','channel')))
    except (KeyError, TypeError, InvalidOperation, ValueError):
        return False


class CrashPoint:
    def __init__(self, stage=None, after=1):
        if stage is not None and stage not in STAGES:
            raise ValueError(f'unknown crash stage: {stage}')
        if after < 1:
            raise ValueError('after must be >= 1')
        self.stage, self.after, self.hits = stage, after, 0

    def hit(self, stage):
        if stage == self.stage:
            self.hits += 1
            if self.hits == self.after:
                # No Python teardown: exercise SQLite atomicity on abrupt process exit.
                os._exit(86)


class StreamDB:
    def __init__(self, db_path, source_path, *, max_pending=8192, max_backlog=8192,
                 crash=None, expected_scope=None):
        if min(max_pending, max_backlog) < 1:
            raise ValueError('limits must be positive')
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.source = Path(source_path).resolve()
        self.crash = crash or CrashPoint()
        self.expected_scope = expected_scope
        self.max_pending, self.max_backlog = max_pending, max_backlog
        self.db = sqlite3.connect(str(self.path), timeout=30)
        # sqlite3.Connection as a context manager commits/rolls back transactions,
        # but it does NOT close the underlying OS file handle. On Windows, any
        # validation error during __init__ otherwise leaks a locked SQLite file.
        try:
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA busy_timeout=30000')
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.execute('PRAGMA foreign_keys=ON')
            self._create_tables()
            fingerprint = sha256_file(self.source)
            with self.db:
                old = self.get_meta('source_sha256')
                if old is not None and (old != fingerprint or self.get_meta('schema') != SCHEMA_VERSION or
                                        self.get_meta('expected_scope') != (self.expected_scope or '')):
                    raise ValueError('Source or schema mismatch: refusing to reuse journal')
                if old is None:
                    self.set_meta('schema', SCHEMA_VERSION)
                    self.set_meta('source_sha256', fingerprint)
                    self.set_meta('expected_scope', self.expected_scope or '')
                    self.set_meta('source_filename', str(self.source))
                    self.set_meta('next_file_index', '0')
                    self.set_meta('next_byte_offset', '0')
            self.fingerprint = fingerprint

        except BaseException:
            # Source fingerprint/schema mismatch and missing source are expected
            # rejection paths. Always release the database before re-raising.
            self.db.close()
            raise

    def _create_tables(self):
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS raw_journal (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          file_index INTEGER NOT NULL UNIQUE,
          raw_sha256 TEXT NOT NULL,
          payload TEXT NOT NULL,
          processed INTEGER NOT NULL DEFAULT 0,
          received_ns INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_unprocessed ON raw_journal(processed, id);
        CREATE TABLE IF NOT EXISTS lanes (
          lane TEXT PRIMARY KEY,
          next_seq INTEGER NOT NULL,
          integrity TEXT NOT NULL DEFAULT 'CONTIGUOUS'
        );
        CREATE TABLE IF NOT EXISTS pending (
          lane TEXT NOT NULL,
          seq INTEGER NOT NULL,
          raw_id INTEGER NOT NULL,
          raw_sha256 TEXT NOT NULL,
          payload TEXT NOT NULL,
          PRIMARY KEY (lane,seq),
          FOREIGN KEY(raw_id) REFERENCES raw_journal(id)
        );
        CREATE TABLE IF NOT EXISTS outbox (
          lane TEXT NOT NULL,
          seq INTEGER NOT NULL,
          raw_id INTEGER NOT NULL,
          raw_sha256 TEXT NOT NULL,
          payload TEXT NOT NULL,
          delivered INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(lane,seq),
          FOREIGN KEY(raw_id) REFERENCES raw_journal(id)
        );
        CREATE INDEX IF NOT EXISTS idx_outbox_delivery ON outbox(delivered,lane,seq);
        CREATE TABLE IF NOT EXISTS delivery (
          lane TEXT NOT NULL,
          seq INTEGER NOT NULL,
          raw_id INTEGER NOT NULL,
          raw_sha256 TEXT NOT NULL,
          PRIMARY KEY(lane,seq),
          FOREIGN KEY(raw_id) REFERENCES raw_journal(id)
        );
        CREATE TABLE IF NOT EXISTS disposition (
          raw_id INTEGER PRIMARY KEY,
          outcome TEXT NOT NULL,
          detail TEXT,
          FOREIGN KEY(raw_id) REFERENCES raw_journal(id)
        );
        ''')
        self.db.commit()

    def get_meta(self, k):
        r = self.db.execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
        return r['v'] if r else None

    def set_meta(self, k, v):
        self.db.execute('INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',
                        (k, str(v)))

    def count(self, table, where='1=1'):
        return self.db.execute(f'SELECT COUNT(*) FROM {table} WHERE {where}').fetchone()[0]

    def ingest_batch(self, max_items=128):
        """Advance source file byte offset ONLY with journal commit; bounded working set."""
        if max_items < 1:
            raise ValueError('batch must be positive')
        free = self.max_backlog - self.count('raw_journal', 'processed=0')
        if free <= 0:
            return 0
        limit = min(max_items, free)
        pos = int(self.get_meta('next_byte_offset'))
        idx = int(self.get_meta('next_file_index'))
        rows = []
        with self.source.open('rb') as f:
            f.seek(pos)
            while len(rows) < limit:
                line = f.readline()
                if not line:
                    break
                old_pos = pos
                pos = f.tell()
                if not line.strip():
                    raise ValueError(f'Blank source line at byte {old_pos}; refuse implicit skip')
                try:
                    s = line.decode('utf-8')
                    e = json.loads(s)  # Semantic validation remains in processor.
                    if self.expected_scope is not None and (not isinstance(e, dict) or scope_of(e) != self.expected_scope):
                        raise ValueError(f'Cross-lane input at index {idx}; source must be partitioned by scope')
                except (UnicodeError, json.JSONDecodeError) as e:
                    raise ValueError(f'Malformed source line at index {idx}') from e
                rows.append((idx, hashlib.sha256(line.rstrip(b'\r\n')).hexdigest(), s.rstrip('\r\n'), time.time_ns()))
                idx += 1
        if not rows:
            return 0
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.executemany('INSERT INTO raw_journal(file_index,raw_sha256,payload,received_ns) VALUES (?,?,?,?)', rows)
            self.set_meta('next_file_index', idx)
            self.set_meta('next_byte_offset', pos)
            self.crash.hit('before_raw_commit')
            self.db.commit()
            self.crash.hit('after_raw_commit')
            return len(rows)
        except BaseException:
            self.db.rollback()
            raise

    def _disposition(self, raw_id, outcome, detail=None):
        self.db.execute('INSERT INTO disposition(raw_id,outcome,detail) VALUES(?,?,?)',
                        (raw_id,outcome,detail))

    def _publish(self, lane, seq, row):
        self.db.execute('INSERT INTO outbox(lane,seq,raw_id,raw_sha256,payload) VALUES(?,?,?,?,?)',
                        (lane,seq,row['raw_id'],row['raw_sha256'],row['payload']))
        self._disposition(row['raw_id'], 'ORDERED_PUBLISHED')

    def _drain_lane(self, lane, expected, status):
        while status != 'CONFLICT':
            row = self.db.execute('SELECT * FROM pending WHERE lane=? AND seq=?', (lane,expected)).fetchone()
            if row is None:
                break
            self._publish(lane, expected, row)
            self.db.execute('DELETE FROM pending WHERE lane=? AND seq=?', (lane,expected))
            expected += 1
        pending_min = self.db.execute('SELECT MIN(seq) FROM pending WHERE lane=?', (lane,)).fetchone()[0]
        if status != 'CONFLICT':
            status = 'WAITING_FOR_MISSING' if pending_min is not None and pending_min > expected else 'CONTIGUOUS'
        self.db.execute('UPDATE lanes SET next_seq=?, integrity=? WHERE lane=?', (expected,status,lane))

    def process_batch(self, limit=128):
        """Atomically advance processing cursor, pending table and internal outbox."""
        self.db.execute('BEGIN IMMEDIATE')
        processed = 0
        try:
            rows = self.db.execute('SELECT * FROM raw_journal WHERE processed=0 ORDER BY id LIMIT ?', (limit,)).fetchall()
            for r in rows:
                e = json.loads(r['payload'])
                raw_id = int(r['id'])
                if not isinstance(e, dict) or not valid(e):
                    self._disposition(raw_id, 'QUARANTINED_INVALID')
                else:
                    lane, seq = scope_of(e), e['source_seq']
                    st = self.db.execute('SELECT * FROM lanes WHERE lane=?', (lane,)).fetchone()
                    if st is None:
                        self.db.execute('INSERT INTO lanes(lane,next_seq,integrity) VALUES(?,?,?)',
                                        (lane,seq,'CONTIGUOUS'))
                        expected, status = seq, 'CONTIGUOUS'
                    else:
                        expected, status = st['next_seq'], st['integrity']
                    if status == 'CONFLICT':
                        self._disposition(raw_id, 'QUARANTINED_LANE_CONFLICT')
                    else:
                        existing = None
                        if seq < expected:
                            existing = self.db.execute('SELECT raw_sha256 FROM outbox WHERE lane=? AND seq=?', (lane,seq)).fetchone()
                        else:
                            existing = self.db.execute('SELECT raw_sha256 FROM pending WHERE lane=? AND seq=?', (lane,seq)).fetchone()
                        digest = hashlib.sha256(canonical(e).encode('utf-8')).hexdigest()
                        # Compare semantic hash, not source-line whitespace.
                        if existing is not None:
                            old = existing['raw_sha256']
                            if old == digest:
                                self._disposition(raw_id, 'DUPLICATE_IDENTICAL')
                            else:
                                self._disposition(raw_id, 'QUARANTINED_CONFLICT')
                                self.db.execute("UPDATE lanes SET integrity='CONFLICT' WHERE lane=?", (lane,))
                        elif seq < expected:
                            self._disposition(raw_id, 'QUARANTINED_STALE')
                        else:
                            if self.count('pending') >= self.max_pending and seq != expected:
                                break  # untouched current raw row: no silent drop/ACK.
                            # The currently missing expected event can drain a full buffer.
                            self.db.execute('INSERT INTO pending(lane,seq,raw_id,raw_sha256,payload) VALUES(?,?,?,?,?)',
                                            (lane,seq,raw_id,digest,canonical(e)))
                            self._drain_lane(lane, expected, status)
                self.db.execute('UPDATE raw_journal SET processed=1 WHERE id=?', (raw_id,))
                processed += 1
            self.crash.hit('before_process_commit') if processed else None
            self.db.commit()
            self.crash.hit('after_process_commit') if processed else None
            return processed
        except BaseException:
            self.db.rollback()
            raise

    def deliver_batch(self, limit=128):
        """Exactly once INSIDE SQLite only: sink insert and outbox ack share a tx."""
        self.db.execute('BEGIN IMMEDIATE')
        try:
            rows = self.db.execute('SELECT * FROM outbox WHERE delivered=0 ORDER BY lane,seq LIMIT ?', (limit,)).fetchall()
            for r in rows:
                self.db.execute('INSERT OR IGNORE INTO delivery(lane,seq,raw_id,raw_sha256) VALUES(?,?,?,?)',
                                (r['lane'],r['seq'],r['raw_id'],r['raw_sha256']))
                self.db.execute('UPDATE outbox SET delivered=1 WHERE lane=? AND seq=?',
                                (r['lane'],r['seq']))
            self.crash.hit('before_delivery_commit') if rows else None
            self.db.commit()
            self.crash.hit('after_delivery_commit') if rows else None
            return len(rows)
        except BaseException:
            self.db.rollback()
            raise

    def pump(self, batch=128, max_cycles=None):
        """Bounded input: once pending/backlog is full, stop file ACK rather than drop."""
        cycles = 0
        while True:
            if max_cycles is not None and cycles >= max_cycles:
                break
            cycles += 1
            p = self.process_batch(batch)
            d = self.deliver_batch(batch)
            capacity = self.max_pending - self.count('pending')
            # Pending consumes durable capacity. Do not ingest beyond configured ceiling.
            a = self.ingest_batch(min(batch, max(0, capacity))) if capacity else 0
            if p == 0 and d == 0 and a == 0:
                break
        return self.report()

    def report(self):
        d = self.db
        counts = {r['outcome']: r['n'] for r in d.execute('SELECT outcome,COUNT(*) n FROM disposition GROUP BY outcome')}
        lanes = []
        for r in d.execute('SELECT * FROM lanes ORDER BY lane'):
            first_pending = d.execute('SELECT MIN(seq) FROM pending WHERE lane=?', (r['lane'],)).fetchone()[0]
            lanes.append({'scope':json.loads(r['lane']), 'next_expected_seq':r['next_seq'],
                          'integrity':r['integrity'], 'lowest_pending_seq':first_pending})
        mismatched = d.execute('''SELECT COUNT(*) FROM outbox o LEFT JOIN raw_journal r ON r.id=o.raw_id
                               WHERE r.id IS NULL OR r.processed<>1''').fetchone()[0]
        delivery_mismatched = d.execute('''SELECT COUNT(*) FROM delivery v LEFT JOIN outbox o
                               ON o.lane=v.lane AND o.seq=v.seq
                               WHERE o.lane IS NULL OR o.raw_sha256<>v.raw_sha256 OR o.raw_id<>v.raw_id''').fetchone()[0]
        # Offline audit: validate both persisted raw bytes and semantic outbox identities.
        journal_corrupt = 0
        for r in d.execute('SELECT payload,raw_sha256 FROM raw_journal'):
            if hashlib.sha256(r['payload'].encode('utf-8')).hexdigest() != r['raw_sha256']:
                journal_corrupt += 1
        outbox_corrupt = 0
        for r in d.execute('SELECT lane,seq,payload,raw_sha256 FROM outbox'):
            try:
                e = json.loads(r['payload'])
                correct = (scope_of(e) == r['lane'] and e['source_seq'] == r['seq'] and
                           hashlib.sha256(canonical(e).encode('utf-8')).hexdigest() == r['raw_sha256'])
            except (KeyError, ValueError, TypeError):
                correct = False
            if not correct:
                outbox_corrupt += 1
        digest = hashlib.sha256()
        for r in d.execute('SELECT lane,seq,raw_sha256 FROM delivery ORDER BY lane,seq'):
            digest.update(canonical([r['lane'],r['seq'],r['raw_sha256']]).encode('utf-8')+b'\n')
        return {'experiment_id':'FTDI-S07-004', 'source_sha256':self.fingerprint,
                'source_records_acknowledged':int(self.get_meta('next_file_index')),
                'source_byte_offset':int(self.get_meta('next_byte_offset')),
                'source_file_bytes':self.source.stat().st_size,
                'raw_journal_records':self.count('raw_journal'),
                'raw_unprocessed':self.count('raw_journal','processed=0'),
                'pending':self.count('pending'),
                'outbox_records':self.count('outbox'),
                'outbox_unacked':self.count('outbox','delivered=0'),
                'delivery_records':self.count('delivery'),
                'quarantine_records':sum(v for k,v in counts.items() if k.startswith('QUARANTINED')),
                'dispositions':counts, 'lanes':lanes,
                'orphan_or_unprocessed_outputs':mismatched,
                'delivery_inconsistencies':delivery_mismatched,
                'raw_journal_checksum_mismatches':journal_corrupt,
                'outbox_content_mismatches':outbox_corrupt,
                'source_file_still_matches_initial_sha256':sha256_file(self.source)==self.fingerprint,
                'delivery_logical_sha256':digest.hexdigest(),
                'source_file_exhausted':int(self.get_meta('next_byte_offset'))==self.source.stat().st_size}

    def close(self):
        self.db.close()

    def __enter__(self): return self
    def __exit__(self,*args): self.close()
