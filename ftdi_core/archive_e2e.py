"""FTDI-Core: read-only, archive-aware end-to-end reconciliation (Task 001).

Contract: ``docs/ARCHIVE_E2E_AUDIT.md``.  Trust decisions: ADR-006 in ``docs/ADRS.md``.

``audit-e2e`` (``ftdi_core.e2e.reconcile``) refuses every lane whose history was
pruned into receipt-gated archives; that behaviour is unchanged.  This module is
the explicit archive-aware mode.  For every registered lane it proves

    source JSONL line i  ==  archived record i          (i < archived_records)
                         ==  resident raw_journal row i (archived_records <= i < next_file_index)
    published events (archived outbox + resident outbox, keyed by market seq)
                         ==  independent consumer ``effects`` rows, field by field

and that no evidence file changed while it was being read.  Archive manifest
hashes, ``acknowledged: true`` flags, registry/meta hashes and source manifests
live in the producer's own trust domain and are treated as claims; the source
bytes and the consumer database are the evidence.

Three identity domains are kept apart on purpose:
``raw_journal.id`` (producer row id), ``file_index`` (source line position) and
the market ``seq``/``source_seq``.  Market sequences may interleave between the
archived prefix and the resident suffix; file positions may not.

Read-only boundary: SQLite databases are opened ``mode=ro`` (plus
``immutable=1`` when quiescent, so SQLite creates no -wal/-shm sidecars); every
other file is opened ``rb``.  Mutating legacy helpers (``retention.connect_db``,
``retention.audit``, ``StreamDB`` through ``ftdi_core.audit``) are never called.
The only write is the optional JSON report.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat as stat_mode
import tempfile
import zlib

from .adapters.s070041.durable_stream import SCHEMA_VERSION, canonical, scope_of, valid
from .adapters.s070041.external_delivery import envelope, event_identity, validate
from .adapters.s070041.multi_lane import ALIAS_RE

REPORT_SCHEMA = 'FTDI-Core-E2E-Archive-Audit-v1'
ARCHIVE_FORMAT = 'S07-004.1 retention.export_gzip, implicit v1: one gzip member of canonical JSON lines'
SEGMENT_NAME = 'raw_{:012d}_{:012d}.jsonl.gz'
SEGMENT_RE = re.compile(r'raw_([0-9]{12})_([0-9]{12})\.jsonl\.gz')
HEX64 = re.compile(r'[0-9a-f]{64}')
CANONICAL_INT = re.compile(r'0|[1-9][0-9]{0,17}')  # <= 18 digits: no int() digit-limit errors
SOURCE_MANIFEST_NAME = 'source_manifest.json'
SOURCE_MANIFEST_SCHEMA = 'FTDI-Core-SourceManifest-v1'

KNOWN_OUTCOMES = frozenset({'ORDERED_PUBLISHED', 'DUPLICATE_IDENTICAL', 'QUARANTINED_INVALID',
                            'QUARANTINED_CONFLICT', 'QUARANTINED_STALE', 'QUARANTINED_LANE_CONFLICT'})
ITEM_KEYS = frozenset({'id', 'file_index', 'raw_sha256', 'payload', 'received_ns', 'outcome', 'detail'})
ITEM_KEYS_WITH_OUTBOX = ITEM_KEYS | {'outbox'}
OUTBOX_KEYS = frozenset({'lane', 'seq', 'raw_sha256', 'payload', 'acknowledged'})
PRODUCER_COLUMNS = {
    'meta': ('k', 'v'),
    'raw_journal': ('id', 'file_index', 'raw_sha256', 'payload', 'processed', 'received_ns'),
    'lanes': ('lane', 'next_seq', 'integrity'),
    'pending': ('lane', 'seq', 'raw_id', 'raw_sha256', 'payload'),
    'outbox': ('lane', 'seq', 'raw_id', 'raw_sha256', 'payload', 'delivered'),
    'delivery': ('lane', 'seq', 'raw_id', 'raw_sha256'),
    'disposition': ('raw_id', 'outcome', 'detail'),
}
SEGMENT_COLUMNS = ('start_file_index', 'end_file_index', 'record_count', 'outbox_count',
                   'archive_path', 'archive_sha256')
EFFECT_COLUMNS = ('event_id', 'lane', 'seq', 'epoch', 'payload_sha256', 'payload')

NOTICE_CODES = frozenset({'UNREFERENCED_ARCHIVE_FILE', 'UNRECOGNIZED_ARCHIVE_DIR_ENTRY',
                          'ARCHIVE_PATH_RELOCATED_ALLOWED', 'SOURCE_MANIFEST_ABSENT',
                          'UNREFERENCED_CONSUMER_SEGMENT_FILE'})
METRIC_KEYS = ('producer_outbox', 'producer_acked', 'archived_records', 'archived_records_parsed',
               'archived_segments', 'archived_segments_parsed', 'archived_events',
               'archived_events_matched', 'archived_quarantined', 'expected_events_total',
               'consumer_effects', 'matched_effects', 'remote_missing', 'remote_committed_unacked',
               'orphan_consumer_effects', 'content_conflicts', 'invalid_consumer_rows',
               'invalid_producer_rows', 'producer_receipt_inconsistencies',
               'archived_missing_consumer_effects', 'duplicate_identities',
               'unverifiable_lane_effects', 'source_lines_anchored', 'consumer_compacted_effects',
               'consumer_segments', 'consumer_segments_parsed')
STRUCTURAL_INVARIANTS = ('snapshot_consistent', 'consumer_schema_recognized', 'all_lanes_verifiable',
                         'every_source_line_anchored', 'archived_records_fully_parsed',
                         'sequences_contiguous')
EQUALITY_INVARIANTS = ('expected_equals_matched', 'consumer_effects_equals_matched',
                       'identity_digests_equal')
TRUST_MODEL = {
    'primary_anchor': 'immutable source JSONL bytes: re-hashed by this audit and compared line by '
                      'line with every archived and resident producer record',
    'independent_evidence': 'consumer effects database (separate SQLite file written by a separate '
                            'process; same host and same administrative trust domain)',
    'producer_domain_claims': ['lane_registry.json sha256', 'producer meta source_sha256',
                               'archive_segments.archive_sha256 and record counts',
                               'archived outbox acknowledged=true',
                               'source_manifest.json, including the original ZIP sha256'],
    'rule': 'producer-domain claims are cross-checked against evidence but never accepted as proof '
            'on their own',
}
LIMITS_OF_CLAIM = [
    'COMPLETE covers only the supplied source interval; events before the first observed sequence '
    'are UNVERIFIED_PREFIX and are not venue-certified',
    'No protection against an actor able to rewrite source, registry, producer DB, archives and '
    'consumer DB together (no signatures, no WORM storage)',
    'Consumer SQLite effects only; not exactly-once for any external side effect or across hosts',
    'Separate SQLite snapshots are made consistent by pre/post fingerprints, not by a distributed '
    'transaction: run in a maintenance window',
    'Offline historical evidence; says nothing about live exchange availability, latency or P99',
]
_CHUNK = 256 * 1024
_DIGEST_MOD = 1 << 256
_PARSE_ERRORS = (ValueError, TypeError, KeyError, RecursionError)


@dataclass(frozen=True)
class ArchiveLimits:
    """Resource ceilings; every field must be a positive ``int`` (``bool`` rejected)."""
    max_compressed_bytes: int = 32 * 1024 * 1024
    max_decompressed_bytes: int = 128 * 1024 * 1024
    max_line_bytes: int = 1024 * 1024
    max_records_per_segment: int = 20000
    max_source_line_bytes: int = 1024 * 1024
    max_events_per_lane: int = 100_000_000
    max_metadata_bytes: int = 4 * 1024 * 1024
    max_unreferenced_overlap_files: int = 16
    max_archive_dir_entries: int = 100_000

    def __post_init__(self):
        for name, value in asdict(self).items():
            if type(value) is not int or value < 1:
                raise ValueError(f'ArchiveLimits.{name} must be a positive integer')


class _Reject(Exception):
    """A structural archive problem that stops parsing of the current file."""

    def __init__(self, code, **detail):
        super().__init__(code)
        self.code = code
        self.detail = detail


def _is_int(value):
    return type(value) is int


def _meta_int(value):
    return int(value) if isinstance(value, str) and CANONICAL_INT.fullmatch(value) else None


def _norm(path):
    """String-only path normalisation (never touches the file system)."""
    return os.path.normcase(os.path.normpath(str(path)))


def _jsonable(value):
    if value is None or isinstance(value, (bool, int)):
        return value
    text = value if isinstance(value, str) else repr(value)
    return text if len(text) <= 240 else text[:237] + '...'


def _text_sha(value):
    if not isinstance(value, str):
        return None
    try:
        return hashlib.sha256(value.encode('utf-8')).hexdigest()
    except UnicodeEncodeError:
        return None


def _ident(event_id, payload_sha256):
    data = f'{event_id!r}|{payload_sha256!r}'.encode('utf-8', 'surrogatepass')
    return int.from_bytes(hashlib.sha256(data).digest(), 'big')


def _sha256_path(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _read_bounded(path, limit):
    """Read a whole small file without ever allocating more than its size + 1.

    ``BufferedReader.read(n)`` pre-allocates ``n`` bytes, so ``read(limit + 1)``
    would allocate the limit for every call regardless of the file's size.
    """
    with open(path, 'rb') as handle:
        size = os.fstat(handle.fileno()).st_size
        if size > limit:
            raise ValueError(f'{Path(path).name} exceeds {limit} bytes')
        data = handle.read(size + 1)
    if len(data) > limit:
        raise ValueError(f'{Path(path).name} exceeds {limit} bytes')
    return data


def _fingerprint(path, content):
    """Return a comparable fingerprint, ``None`` if absent, or an UNREADABLE marker."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        return ['UNREADABLE', type(exc).__name__]
    result = [stat_mode.S_IFMT(info.st_mode), info.st_size, info.st_mtime_ns, info.st_ino]
    if content and stat_mode.S_ISREG(info.st_mode):
        try:
            result.append(_sha256_path(path))
        except OSError as exc:
            return ['UNREADABLE', type(exc).__name__]
    return result


def _redirected(directory):
    """True if ``directory`` (built from the resolved root) is reached through a link/junction."""
    return _norm(os.path.realpath(directory)) != _norm(directory)


def _listing(directory):
    """Digest of a directory's entries; never enumerates a redirected directory."""
    if not os.path.lexists(directory):
        return None
    if _redirected(directory):
        return ['REDIRECTED']
    try:
        with os.scandir(directory) as entries:
            names = sorted(entry.name for entry in entries)
    except OSError as exc:
        return ['UNREADABLE', type(exc).__name__]
    digest = hashlib.sha256()
    for name in names:
        fingerprint = _fingerprint(os.path.join(directory, name), False)
        if _has_unreadable(fingerprint):
            return ['UNREADABLE', f'entry {name}']
        digest.update(json.dumps([name, fingerprint]).encode('utf-8', 'surrogatepass') + b'\n')
    return ['LISTING', len(names), digest.hexdigest()]


def _has_unreadable(value):
    if isinstance(value, list):
        return (len(value) == 2 and value[0] == 'UNREADABLE') or any(_has_unreadable(v) for v in value)
    return False


def _gzip_lines(data, limits):
    """Yield the lines of ONE in-memory gzip member, with hard resource limits.

    Requires exactly one member (zlib verifies CRC-32 and ISIZE), no trailing
    bytes and a final newline.  Peak memory is bounded by ``max_line_bytes``
    plus one inflate chunk regardless of the compression ratio.
    """
    inflater = zlib.decompressobj(wbits=31)
    pending = bytearray()
    total = records = 0
    tail = data
    try:
        while True:
            out = inflater.decompress(tail, _CHUNK)
            tail = inflater.unconsumed_tail
            if out:
                total += len(out)
                if total > limits.max_decompressed_bytes:
                    raise _Reject('ARCHIVE_DECOMPRESSED_TOO_LARGE', limit=limits.max_decompressed_bytes)
                pending += out
                start = 0
                while True:
                    newline = pending.find(b'\n', start)
                    if newline < 0:
                        break
                    if newline - start > limits.max_line_bytes:
                        raise _Reject('ARCHIVE_LINE_TOO_LONG', limit=limits.max_line_bytes)
                    records += 1
                    if records > limits.max_records_per_segment:
                        raise _Reject('ARCHIVE_TOO_MANY_RECORDS', limit=limits.max_records_per_segment)
                    yield bytes(pending[start:newline])
                    start = newline + 1
                del pending[:start]
                if len(pending) > limits.max_line_bytes:
                    raise _Reject('ARCHIVE_LINE_TOO_LONG', limit=limits.max_line_bytes)
            if inflater.eof:
                break
            if not out and not tail:
                raise _Reject('ARCHIVE_SEGMENT_UNREADABLE', reason='truncated gzip stream')
    except zlib.error as exc:
        raise _Reject('ARCHIVE_SEGMENT_UNREADABLE', reason=f'zlib: {exc}') from None
    if inflater.unused_data:
        raise _Reject('ARCHIVE_SEGMENT_UNREADABLE', reason='bytes after the gzip member')
    if pending:
        raise _Reject('ARCHIVE_SEGMENT_UNREADABLE', reason='final record has no newline')


def _parse_item(line, index):
    """Strictly decode one archived record of the implicit v1 format."""
    try:
        text = line.decode('utf-8')
        item = json.loads(text)
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='record is not UTF-8 JSON') from None
    if not isinstance(item, dict):
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='record is not a JSON object')
    try:
        canon = canonical(item)
    except (ValueError, TypeError, RecursionError):
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='record cannot be canonicalised') from None
    if canon != text:
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='record line is not canonical JSON')
    keys = set(item)
    if keys != ITEM_KEYS and keys != ITEM_KEYS_WITH_OUTBOX:
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='unexpected record fields',
                      fields=','.join(sorted(keys ^ ITEM_KEYS_WITH_OUTBOX)))
    if not (_is_int(item['id']) and item['id'] >= 1 and _is_int(item['file_index'])
            and isinstance(item['raw_sha256'], str) and HEX64.fullmatch(item['raw_sha256'])
            and isinstance(item['payload'], str) and _is_int(item['received_ns'])
            and item['received_ns'] >= 0 and isinstance(item['outcome'], str)
            and (item['detail'] is None or isinstance(item['detail'], str))):
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='unexpected record field types')
    if item['file_index'] != index:
        raise _Reject('ARCHIVE_INDEX_GAP', expected_file_index=index, found_file_index=item['file_index'])
    if item['outcome'] not in KNOWN_OUTCOMES:
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='unknown disposition outcome',
                      outcome=item['outcome'])
    if ('outbox' in item) != (item['outcome'] == 'ORDERED_PUBLISHED'):
        raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='outbox presence does not match outcome')
    outbox = item.get('outbox')
    if outbox is not None:
        if not isinstance(outbox, dict) or set(outbox) != OUTBOX_KEYS:
            raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='unexpected outbox fields')
        if not (isinstance(outbox['lane'], str) and _is_int(outbox['seq']) and outbox['seq'] >= 0
                and isinstance(outbox['raw_sha256'], str) and HEX64.fullmatch(outbox['raw_sha256'])
                and isinstance(outbox['payload'], str) and isinstance(outbox['acknowledged'], bool)):
            raise _Reject('ARCHIVE_RECORD_UNRECOGNIZED', reason='unexpected outbox field types')
    return item


def _published_event(scope, seq, digest, payload_text):
    """Envelope of an outbox-shaped record; raises one of ``_PARSE_ERRORS`` if invalid."""
    if not isinstance(payload_text, str) or not isinstance(digest, str) or not _is_int(seq):
        raise ValueError('outbox fields have unexpected types')
    payload = json.loads(payload_text)
    if not isinstance(payload, dict):
        raise ValueError('outbox payload is not a JSON object')
    if canonical(payload) != payload_text:
        raise ValueError('outbox payload is not canonical JSON')
    if not valid(payload):
        raise ValueError('outbox payload fails the event schema')
    return envelope({'lane': scope, 'seq': seq, 'raw_sha256': digest, 'payload': payload_text})


def _binding_ok(raw_payload, outbox_payload):
    """The published event must be exactly canonical(json(raw source line))."""
    if not isinstance(raw_payload, str):
        return False
    try:
        raw = json.loads(raw_payload)
        return isinstance(raw, dict) and canonical(raw) == outbox_payload
    except _PARSE_ERRORS:
        return False


def _effect_valid(row):
    try:
        payload = json.loads(row['payload'])
        validate({'event_id': row['event_id'], 'lane': row['lane'], 'seq': row['seq'],
                  'epoch': row['epoch'], 'payload_sha256': row['payload_sha256'], 'payload': payload})
        return canonical(payload) == row['payload']
    except _PARSE_ERRORS:
        return False


def _plain_table(db, name, expected_columns):
    """Return None if ``name`` is a plain table with exactly ``expected_columns``, else a reason.

    Views (possibly recursive) and virtual tables report the same column names through
    ``pragma_table_info``; they must never be accepted as evidence tables.
    """
    objects = db.execute('SELECT type, sql FROM sqlite_master WHERE lower(name) = lower(?)', (name,)).fetchall()
    if len(objects) != 1 or objects[0]['type'] != 'table':
        return 'missing, or not a plain table (view/index/duplicate name)'
    sql = (objects[0]['sql'] or '').lstrip().upper()
    if not sql.startswith('CREATE TABLE'):
        return 'not a plain CREATE TABLE (virtual or unrecognized table)'
    columns = tuple(r['name'] for r in db.execute(
        'SELECT name FROM pragma_table_xinfo(?) ORDER BY cid', (name,)))
    if columns != tuple(expected_columns):
        return 'columns ' + ','.join(columns)
    return None


class _Findings:
    SAMPLE_LIMIT = 50

    def __init__(self):
        self.counts = {}
        self.lane_counts = {}
        self.samples = []

    def add(self, code, lane=None, **detail):
        self.counts[code] = self.counts.get(code, 0) + 1
        if lane is not None:
            per_lane = self.lane_counts.setdefault(lane, {})
            per_lane[code] = per_lane.get(code, 0) + 1
        if len(self.samples) < self.SAMPLE_LIMIT:
            entry = {'code': code, 'severity': 'NOTICE' if code in NOTICE_CODES else 'FATAL'}
            if lane is not None:
                entry['lane'] = lane
            for key, value in detail.items():
                entry[key] = _jsonable(value)
            self.samples.append(entry)

    def fatal(self, lane=None):
        counts = self.counts if lane is None else self.lane_counts.get(lane, {})
        return sum(n for code, n in counts.items() if code not in NOTICE_CODES)


class _Unopenable(Exception):
    """Opening this SQLite file read-only would itself modify evidence; refuse."""


class _Snapshot:
    """One read-only SQLite read transaction (``immutable=1`` when quiescent).

    * no WAL frames and no ``-journal``: ``immutable=1`` -> SQLite touches no file at all;
    * WAL frames and an existing ``-shm``: ``mode=ro`` -> SQLite only uses its shared-memory
      coordination file (not evidence); ``data_version`` detects commits by others;
    * WAL frames without ``-shm`` (reading would create it) or a ``-journal`` (reading would
      roll a hot journal back): refused with ``_Unopenable``.
    """

    def __init__(self, path, label):
        self.label = label
        self.closed = True
        self.dv_after = None
        wal = _fingerprint(Path(f'{path}-wal'), False)
        shm = _fingerprint(Path(f'{path}-shm'), False)
        journal = _fingerprint(Path(f'{path}-journal'), False)
        if any(_has_unreadable(fp) for fp in (wal, shm, journal)):
            raise _Unopenable('cannot inspect SQLite sidecar files')
        if journal is not None:
            raise _Unopenable('rollback journal present; opening would require a hot-journal rollback')
        self.immutable = wal is None or wal[1] == 0
        if not self.immutable and shm is None:
            raise _Unopenable('WAL frames without -shm; a read-only open would create -shm')
        query = 'mode=ro&immutable=1' if self.immutable else 'mode=ro'
        self.conn = sqlite3.connect(f'{Path(path).as_uri()}?{query}', uri=True, timeout=30)
        self.closed = False
        try:
            self.conn.row_factory = sqlite3.Row
            self.conn.execute('PRAGMA query_only=ON')
            self.dv_before = self.conn.execute('PRAGMA data_version').fetchone()[0]
            self.conn.execute('BEGIN')
            self.conn.execute('SELECT count(*) FROM sqlite_master').fetchone()
        except BaseException:
            self.conn.close()
            self.closed = True
            raise

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.conn.in_transaction:
                self.conn.execute('ROLLBACK')
            self.dv_after = self.conn.execute('PRAGMA data_version').fetchone()[0]
        except sqlite3.Error:
            self.dv_after = None
        finally:
            self.conn.close()

    @property
    def stable(self):
        return self.immutable or (self.dv_after is not None and self.dv_after == self.dv_before)


class _SourceCursor:
    """Sequential, bounded reader that hashes lines exactly as ``ingest_batch`` did."""

    def __init__(self, path, max_line):
        self.max_line = max_line
        self.index = 0
        self.offset = 0
        self.eof = False
        self.failed = None
        self.reported = False
        self._digest = hashlib.sha256()
        self._handle = open(path, 'rb')

    def next_hash(self):
        if self.failed or self.eof:
            return None
        line = self._handle.readline(self.max_line + 1)
        self._digest.update(line)
        self.offset += len(line)
        if not line:
            self.eof = True
            return None
        if len(line) > self.max_line:
            self.failed = 'SOURCE_LINE_TOO_LONG'
            return None
        self.index += 1
        return hashlib.sha256(line.rstrip(b'\r\n')).hexdigest()

    def skip_to(self, index):
        while self.index < index and not self.failed and not self.eof:
            self.next_hash()

    def finish(self):
        """Hash the remainder; return (whole-file sha256, total bytes)."""
        size = self.offset
        for chunk in iter(lambda: self._handle.read(1024 * 1024), b''):
            self._digest.update(chunk)
            size += len(chunk)
        return self._digest.hexdigest(), size

    def close(self):
        self._handle.close()


class _Lane:
    def __init__(self, alias, record, root):
        self.alias = alias
        self.scope = record['scope']
        self.registry_sha = record['sha256']
        self.source = Path(record['source'])
        self.db_path = root / 'lanes' / alias / 'state.sqlite3'
        self.archive_dir = root / 'archives' / alias
        self.snapshot = None
        self.verifiable = True
        self.next_seq = None
        self.n_max = 0
        self.bitmap = bytearray()
        self.published = 0
        self.expected = 0
        self.matched = 0
        self.pending_delivery = 0
        self.retry = 0
        self.contiguous = False
        self.first_seq = None
        self.source_lines_expected = None
        self.archived_records = 0
        self.archived_complete = False
        self.dispositions = {'archived': {}, 'resident': {}}
        self.provenance = None
        self.counts = dict.fromkeys(('source_lines_anchored', 'archived_records_parsed', 'archived_segments',
                                     'archived_segments_parsed', 'relocated_segments',
                                     'unreferenced_archive_files', 'resident_raw_records',
                                     'resident_outbox', 'resident_outbox_delivered'), 0)

    def mark(self, seq):
        """Record one published market seq; 'ok' | 'dup' | 'range'."""
        if self.next_seq is None or not _is_int(seq):
            return 'range'
        offset = self.next_seq - 1 - seq
        if offset < 0 or offset >= self.n_max:
            return 'range'
        index, bit = divmod(offset, 8)
        if self.bitmap[index] >> bit & 1:
            return 'dup'
        self.bitmap[index] |= 1 << bit
        self.published += 1
        return 'ok'

    def has(self, seq):
        if self.next_seq is None or not _is_int(seq):
            return False
        offset = self.next_seq - 1 - seq
        if offset < 0 or offset >= self.n_max:
            return False
        index, bit = divmod(offset, 8)
        return bool(self.bitmap[index] >> bit & 1)


class _Auditor:
    def __init__(self, root, consumer_db, lanes, limits, allow_relocated):
        self.root = root
        self.consumer_db = consumer_db
        self.lanes = lanes
        self.limits = limits
        self.allow_relocated = allow_relocated
        self.findings = _Findings()
        self.metrics = dict.fromkeys(METRIC_KEYS, 0)
        self.by_scope = {lane.scope: lane for lane in lanes}
        self.consumer = None
        self.consumer_ok = False
        self.snapshots = []
        self.producer_digest = 0
        self.consumer_digest = 0
        self.consumer_v2 = False
        self.tiers = {}               # (lane, epoch) -> {'first', 'c', 'lane_id', 'ok'} for compacted consumer ranges
        self.content_claims = []      # (label, path, sha256) of every non-SQLite file the audit relied on
        self.content_changes = []
        self.content_unreadable = []

    # -- lifecycle -------------------------------------------------------
    def open(self):
        try:
            self.consumer = _Snapshot(self.consumer_db, 'consumer')
            self.snapshots.append(self.consumer)
        except _Unopenable as exc:
            self.findings.add('SNAPSHOT_UNVERIFIABLE', evidence='consumer', reason=str(exc))
        for lane in self.lanes:
            try:
                lane.snapshot = _Snapshot(lane.db_path, f'lane:{lane.alias}')
                self.snapshots.append(lane.snapshot)
            except _Unopenable as exc:
                lane.verifiable = False
                self.findings.add('SNAPSHOT_UNVERIFIABLE', lane=lane.alias, evidence=f'lane:{lane.alias}',
                                  reason=str(exc))

    def close(self):
        for snapshot in self.snapshots:
            snapshot.close()

    def run(self):
        self.consumer_ok = self.consumer is not None and self._consumer_schema()
        if self.consumer_ok:
            self._consumer_tiers()
        for lane in self.lanes:
            if lane.snapshot is not None:
                self._audit_lane(lane)
        if self.consumer_ok:
            self._consumer_pass()

    # -- consumer --------------------------------------------------------
    def _consumer_schema(self):
        db = self.consumer.conn
        problem = _plain_table(db, 'effects', EFFECT_COLUMNS)
        if problem:
            self.findings.add('CONSUMER_SCHEMA_UNRECOGNIZED', table='effects', reason=problem)
            return False
        unique = set()
        for index in db.execute('SELECT name, "unique" AS is_unique, partial '
                                "FROM pragma_index_list('effects')").fetchall():
            if index['is_unique'] and not index['partial']:
                unique.add(tuple(r['name'] for r in db.execute(
                    'SELECT name FROM pragma_index_info(?) ORDER BY seqno', (index['name'],))))
        if ('event_id',) not in unique or ('lane', 'seq', 'epoch') not in unique:
            self.findings.add('CONSUMER_SCHEMA_UNRECOGNIZED',
                              reason='effects lacks UNIQUE(event_id) or UNIQUE(lane,seq,epoch)')
            return False
        return True

    def _consumer_pass(self):
        findings, metrics = self.findings, self.metrics
        previous = None
        for row in self.consumer.conn.execute('SELECT event_id, lane, seq, epoch, payload_sha256, payload '
                                              'FROM effects ORDER BY lane, seq, epoch'):
            metrics['consumer_effects'] += 1
            self.consumer_digest = (self.consumer_digest
                                    + _ident(row['event_id'], row['payload_sha256'])) % _DIGEST_MOD
            lane = self.by_scope.get(row['lane']) if isinstance(row['lane'], str) else None
            alias = lane.alias if lane is not None else None
            key = (row['lane'], row['seq'], row['epoch'])
            if key == previous:
                findings.add('CONSUMER_DUPLICATE_IDENTITY', lane=alias, seq=row['seq'], side='consumer')
            previous = key
            if not _effect_valid(row):
                metrics['invalid_consumer_rows'] += 1
                findings.add('CONSUMER_EFFECT_TAMPERED', lane=alias, seq=row['seq'])
            if lane is None:
                metrics['orphan_consumer_effects'] += 1
                findings.add('ORPHAN_CONSUMER_EFFECT', seq=row['seq'], reason='lane is not registered')
            elif not lane.verifiable:
                metrics['unverifiable_lane_effects'] += 1
            elif row['epoch'] != 0 or not lane.has(row['seq']):
                metrics['orphan_consumer_effects'] += 1
                findings.add('ORPHAN_CONSUMER_EFFECT', lane=alias, seq=row['seq'])
        if self.consumer_v2:
            self._digest_tier_pass()

    def _digest_tier_pass(self):
        """Compacted effects (digest rows) count as consumer effects for orphans, totals and identity digest."""
        findings, metrics = self.findings, self.metrics
        for row in self.consumer.conn.execute('SELECT l.lane AS lane, d.epoch, d.seq, d.payload_sha256 '
                                              'FROM effect_digests d LEFT JOIN lane_ids l ON l.lane_id = d.lane_id '
                                              'ORDER BY d.lane_id, d.epoch, d.seq'):
            metrics['consumer_effects'] += 1
            metrics['consumer_compacted_effects'] += 1
            digest = bytes(row['payload_sha256']).hex() if isinstance(row['payload_sha256'], bytes) else None
            scope = row['lane'] if isinstance(row['lane'], str) else None
            identity = (event_identity(scope, row['seq'], row['epoch'], digest)
                        if scope is not None and digest is not None else repr(tuple(row)))
            self.consumer_digest = (self.consumer_digest + _ident(identity, digest)) % _DIGEST_MOD
            lane = self.by_scope.get(scope) if scope is not None else None
            if lane is None:
                metrics['orphan_consumer_effects'] += 1
                findings.add('ORPHAN_CONSUMER_EFFECT', seq=row['seq'], reason='compacted effect of an unregistered lane')
            elif not lane.verifiable:
                metrics['unverifiable_lane_effects'] += 1
            elif row['epoch'] != 0 or not lane.has(row['seq']):
                metrics['orphan_consumer_effects'] += 1
                findings.add('ORPHAN_CONSUMER_EFFECT', lane=lane.alias, seq=row['seq'], tier='digest')

    def _consumer_tiers(self):
        """Verify a v2 consumer's compacted tiers (segments, chain, digests, overlap) before matching."""
        from . import consumer_retention as cr
        findings, metrics, db = self.findings, self.metrics, self.consumer.conn
        if not cr.has_v2_objects(db):
            return
        self.consumer_v2 = True
        problem = cr.v2_table_problem(db)
        if problem:
            findings.add('CONSUMER_SCHEMA_UNRECOGNIZED', reason=problem)
            self.consumer_ok = False
            return
        trigger = cr.trigger_problem(db)
        if trigger:   # fatal, but keep verifying so e.g. an overlap behind a dropped trigger is named
            findings.add('CONSUMER_SCHEMA_UNRECOGNIZED', reason=trigger)
        root = cr.archive_root(self.consumer_db)
        redirected = os.path.lexists(root) and _redirected(root)
        lane_ids = {r['lane']: r['lane_id'] for r in db.execute('SELECT lane_id, lane FROM lane_ids')}
        registered = set()
        for state in db.execute('SELECT * FROM retention_state ORDER BY lane, epoch').fetchall():
            lane, epoch, first, last = state['lane'], state['epoch'], state['first_seq'], state['content_watermark']
            owner = self.by_scope.get(lane)
            alias = owner.alias if owner is not None else None
            if not (isinstance(lane, str) and all(_is_int(v) for v in (epoch, first, last, state['segments']))
                    and first <= last and isinstance(state['chain_head'], str)):
                findings.add('CONSUMER_SCHEMA_UNRECOGNIZED', lane=alias, reason='retention_state row has bad values')
                self.consumer_ok = False
                continue
            tier = {'first': first, 'c': last, 'lane_id': lane_ids.get(lane), 'ok': True}
            self.tiers[(lane, epoch)] = tier

            def bad(code, **detail):
                tier['ok'] = False
                findings.add(code, lane=alias, **detail)
            if tier['lane_id'] is None:
                bad('CONSUMER_SEGMENT_CONTINUITY', reason='lane_ids entry missing')
            expected, prev, count = first, cr.GENESIS, 0
            for seg in db.execute('SELECT * FROM effect_segments WHERE lane = ? AND epoch = ? ORDER BY start_seq',
                                  (lane, epoch)).fetchall():
                count += 1
                metrics['consumer_segments'] += 1
                start, end, records = seg['start_seq'], seg['end_seq'], seg['record_count']
                if not all(_is_int(v) for v in (start, end, records)) or not isinstance(seg['sha256'], str):
                    bad('CONSUMER_SEGMENT_CONTINUITY', reason='segment row has bad values')
                    continue
                if start != expected or records != end - start + 1 or not 1 <= records <= self.limits.max_records_per_segment:
                    bad('CONSUMER_SEGMENT_CONTINUITY', start_seq=start, expected_start=expected)
                if seg['prev_chain'] != prev or seg['chain'] != cr.segment_chain(prev, lane, epoch, start, end,
                                                                                 records, seg['sha256']):
                    bad('CONSUMER_CHAIN_MISMATCH', start_seq=start)
                expected, prev = end + 1, seg['chain']
                canonical_path = (cr.segment_relative_path(tier['lane_id'], epoch, start, end)
                                  if tier['lane_id'] is not None else None)
                file = root / canonical_path if canonical_path is not None else None
                if (seg['relative_path'] != canonical_path or redirected
                        or (file is not None and os.path.lexists(file.parent) and _redirected(file.parent))):
                    bad('CONSUMER_SEGMENT_PATH_INVALID', start_seq=start, relative_path=seg['relative_path'])
                    continue
                registered.add(canonical_path)
                data, problem = self._read_archive_file(owner, file, 'consumer segment')
                if problem:
                    mapped = {'ARCHIVE_SEGMENT_MISSING': 'CONSUMER_SEGMENT_MISSING',
                              'ARCHIVE_PATH_CONFLICT': 'CONSUMER_SEGMENT_PATH_INVALID'}.get(problem[0],
                                                                                           'CONSUMER_SEGMENT_UNREADABLE')
                    bad(mapped, start_seq=start, **problem[1])
                    continue
                if hashlib.sha256(data).hexdigest() != seg['sha256']:
                    bad('CONSUMER_SEGMENT_SHA256_MISMATCH', start_seq=start)
                    continue
                self.content_claims.append((f'consumer_segment:{canonical_path}', file, seg['sha256']))
                try:
                    rows = cr.parse_segment(data, lane=lane, epoch=epoch, start_seq=start, end_seq=end, limits=self.limits)
                except _Reject as rejection:
                    bad(rejection.code, start_seq=start, **rejection.detail)
                    continue
                for row in rows:
                    found = db.execute('SELECT payload_sha256 FROM effect_digests WHERE lane_id = ? AND epoch = ? '
                                       'AND seq = ?', (tier['lane_id'], epoch, row['seq'])).fetchone()
                    if found is None:
                        bad('CONSUMER_DIGEST_MISSING', seq=row['seq'])
                    elif not isinstance(found[0], bytes) or found[0].hex() != row['payload_sha256']:
                        bad('CONSUMER_DIGEST_MISMATCH', seq=row['seq'])
                metrics['consumer_segments_parsed'] += 1
            if expected - 1 != last or count != state['segments']:
                bad('CONSUMER_SEGMENT_CONTINUITY', reason='segments do not tile [first_seq, C]', content_watermark=last)
            if prev != state['chain_head']:
                bad('CONSUMER_CHAIN_MISMATCH', reason='chain_head differs from the last segment chain')
            if tier['lane_id'] is not None:
                extra = db.execute('SELECT COUNT(*) FROM effect_digests WHERE lane_id = ? AND epoch = ? AND '
                                   'seq NOT BETWEEN ? AND ?', (tier['lane_id'], epoch, first, last)).fetchone()[0]
                if extra:
                    bad('CONSUMER_DIGEST_EXTRA', count=extra)
            overlap = db.execute('SELECT COUNT(*) FROM effects WHERE lane = ? AND epoch = ? AND seq BETWEEN ? AND ?',
                                 (lane, epoch, first, last)).fetchone()[0]
            if overlap:
                bad('CONSUMER_TIER_OVERLAP', count=overlap)
        stray_segments = db.execute('SELECT COUNT(*) FROM effect_segments s WHERE NOT EXISTS (SELECT 1 FROM '
                                    'retention_state r WHERE r.lane = s.lane AND r.epoch = s.epoch)').fetchone()[0]
        if stray_segments:
            findings.add('CONSUMER_SEGMENT_CONTINUITY', reason='segment rows without retention_state', count=stray_segments)
        stray_digests = db.execute('SELECT COUNT(*) FROM effect_digests d WHERE NOT EXISTS (SELECT 1 FROM '
                                   'retention_state r JOIN lane_ids l ON l.lane = r.lane WHERE l.lane_id = d.lane_id '
                                   'AND r.epoch = d.epoch)').fetchone()[0]
        if stray_digests:
            findings.add('CONSUMER_DIGEST_EXTRA', reason='digest rows without retention_state', count=stray_digests)
        if os.path.isdir(root) and not redirected:
            seen = 0
            for directory in sorted(root.iterdir()):     # never descend through a link/junction
                entries = [directory] if not directory.is_dir() or os.path.islink(directory) else []
                if directory.is_dir() and _redirected(directory):
                    findings.add('CONSUMER_SEGMENT_PATH_INVALID', reason='redirected archive directory',
                                 directory=directory.name)
                    continue
                if not entries:
                    entries = sorted(directory.iterdir())
                for file in entries:
                    seen += 1
                    if seen > self.limits.max_archive_dir_entries:
                        findings.add('CONSUMER_SEGMENT_PATH_INVALID', reason='consumer archive has too many entries')
                        break
                    relative = file.relative_to(root).as_posix()
                    if relative not in registered:
                        findings.add('UNREFERENCED_CONSUMER_SEGMENT_FILE', file=relative)

    def _expect(self, lane, event, origin, delivered):
        """One published event: dedup by market seq, then compare with the consumer."""
        findings, metrics = self.findings, self.metrics
        seq = event['seq']
        mark = lane.mark(seq)
        if mark == 'dup':
            metrics['duplicate_identities'] += 1
            findings.add('PRODUCER_DUPLICATE_IDENTITY', lane=lane.alias, seq=seq, origin=origin)
            return
        if mark == 'range':
            lane.verifiable = False
            findings.add('PRODUCER_SEQUENCE_OUT_OF_RANGE', lane=lane.alias, seq=seq,
                         next_seq=lane.next_seq, origin=origin)
            return
        lane.expected += 1
        metrics['expected_events_total'] += 1
        self.producer_digest = (self.producer_digest
                                + _ident(event['event_id'], event['payload_sha256'])) % _DIGEST_MOD
        if not self.consumer_ok:
            return
        tier = self.tiers.get((lane.scope, 0))
        if tier is not None and tier['first'] <= seq <= tier['c']:
            found = None if tier['lane_id'] is None else self.consumer.conn.execute(
                'SELECT payload_sha256 FROM effect_digests WHERE lane_id = ? AND epoch = 0 AND seq = ?',
                (tier['lane_id'], seq)).fetchone()
            if found is None:
                return  # tier verification already reported the missing digest
            if origin == 'RESIDENT' and not delivered:
                metrics['remote_committed_unacked'] += 1
                lane.retry += 1
            if isinstance(found[0], bytes) and found[0].hex() == event['payload_sha256']:
                metrics['matched_effects'] += 1
                lane.matched += 1
                if origin == 'ARCHIVE':
                    metrics['archived_events_matched'] += 1
            else:
                metrics['content_conflicts'] += 1
                findings.add('CONSUMER_TIER_CONTENT_CONFLICT', lane=lane.alias, seq=seq)
            return
        rows = self.consumer.conn.execute(
            'SELECT event_id, lane, seq, epoch, payload_sha256, payload FROM effects '
            'WHERE lane = ? AND seq = ? AND epoch = 0', (lane.scope, seq)).fetchall()
        if not rows:
            if origin == 'ARCHIVE':
                metrics['archived_missing_consumer_effects'] += 1
                findings.add('ARCHIVED_ACK_WITHOUT_CONSUMER_EFFECT', lane=lane.alias, seq=seq)
            elif delivered:
                metrics['remote_missing'] += 1
                metrics['producer_receipt_inconsistencies'] += 1
                findings.add('PRODUCER_ACK_WITHOUT_CONSUMER_EFFECT', lane=lane.alias, seq=seq)
            else:
                metrics['remote_missing'] += 1
                lane.pending_delivery += 1
            return
        if len(rows) > 1:
            findings.add('CONSUMER_DUPLICATE_IDENTITY', lane=lane.alias, seq=seq, side='lookup')
        row = rows[0]
        if origin == 'RESIDENT' and not delivered:
            metrics['remote_committed_unacked'] += 1
            lane.retry += 1
        if (row['event_id'] == event['event_id'] and row['payload_sha256'] == event['payload_sha256']
                and row['payload'] == canonical(event['payload'])):
            metrics['matched_effects'] += 1
            lane.matched += 1
            if origin == 'ARCHIVE':
                metrics['archived_events_matched'] += 1
        else:
            metrics['content_conflicts'] += 1
            findings.add('CONSUMER_ARCHIVE_CONTENT_CONFLICT' if origin == 'ARCHIVE'
                         else 'CONSUMER_PRODUCER_CONTENT_CONFLICT', lane=lane.alias, seq=seq)

    # -- lane ------------------------------------------------------------
    def _scalar(self, lane, sql):
        return lane.snapshot.conn.execute(sql).fetchone()[0]

    def _audit_lane(self, lane):
        findings, alias = self.findings, lane.alias
        db = lane.snapshot.conn
        for table, expected in PRODUCER_COLUMNS.items():
            problem = _plain_table(db, table, expected)
            if problem:
                findings.add('PRODUCER_SCHEMA_UNRECOGNIZED', lane=alias, table=table, reason=problem)
                lane.verifiable = False
        if not lane.verifiable:
            return
        meta = self._meta(lane)
        if meta is None:
            lane.verifiable = False
            return
        next_file_index, next_byte_offset, archived = meta['_nfi'], meta['_nbo'], meta['_archived']
        lane.source_lines_expected = next_file_index
        self._lane_state(lane, next_file_index)
        try:
            computed = _sha256_path(lane.source)
        except OSError as exc:
            computed = None
            findings.add('SOURCE_FINGERPRINT_MISMATCH', lane=alias, reason=f'unreadable: {type(exc).__name__}')
        anchor = computed == lane.registry_sha
        if computed is not None and not anchor:
            findings.add('SOURCE_FINGERPRINT_MISMATCH', lane=alias, claim='registry/source bytes')
        if not anchor:
            lane.verifiable = False
        else:
            self.content_claims.append((f'source:{alias}', lane.source, lane.registry_sha))

        structure_ok, segments = self._manifest(lane, archived, next_file_index)
        lane.archived_records = archived or 0
        lane.counts['archived_segments'] = len(segments)
        dir_ok, entries = self._archive_dir(lane)
        overlaps = self._paths(lane, segments, entries, archived or 0, structure_ok and dir_ok)
        need = [(lo, min(hi, archived)) for _, lo, hi in overlaps]
        overlap_hashes = {}

        cursor = _SourceCursor(lane.source, self.limits.max_source_line_bytes) if anchor else None
        try:
            previous_id = None
            if structure_ok and dir_ok and archived:
                for segment in segments:
                    if cursor is not None:
                        cursor.skip_to(segment['start_file_index'])
                    previous_id = self._segment(lane, segment, cursor, previous_id, need, overlap_hashes)
                    if cursor is not None:
                        cursor.skip_to(segment['end_file_index'])
            lane.archived_complete = (structure_ok and dir_ok
                                      and lane.counts['archived_records_parsed'] == lane.archived_records)
            if archived != 0 and not lane.archived_complete:
                # Archived events that could not be read are absent from the seq bitmap;
                # their consumer effects are unverifiable, not orphans (a fatal finding
                # explaining why already exists).
                lane.verifiable = False
            resident_start = archived if structure_ok else None
            self._resident(lane, cursor, resident_start, previous_id, next_file_index)
            self._source_tail(lane, cursor, next_file_index, next_byte_offset)
        finally:
            if cursor is not None:
                cursor.close()
        self._resident_outbox(lane)
        self._resident_dispositions(lane)
        for name, lo, hi in overlaps:
            reason = self._compare_unreferenced(lane, name, lo, hi, archived, overlap_hashes)
            if reason:
                findings.add('ARCHIVE_UNREFERENCED_CONTENT_CONFLICT', lane=alias, file=name, reason=reason)
            else:
                findings.add('UNREFERENCED_ARCHIVE_FILE', lane=alias, file=name,
                             overlaps_archived_range=True, identical=True)
        self._contiguity(lane)
        lane.provenance = self._provenance(lane, meta, computed, next_file_index, next_byte_offset)

    def _meta(self, lane):
        findings, alias = self.findings, lane.alias
        rows = lane.snapshot.conn.execute('SELECT k, v FROM meta LIMIT 100001').fetchall()
        if len(rows) > 100000:
            findings.add('PRODUCER_META_INVALID', lane=alias, reason='meta table too large')
            return None
        meta = {row['k']: row['v'] for row in rows}
        if meta.get('schema') != SCHEMA_VERSION:
            findings.add('PRODUCER_META_INVALID', lane=alias, field='schema')
        if meta.get('expected_scope') != lane.scope:
            findings.add('PRODUCER_META_INVALID', lane=alias, field='expected_scope')
        if meta.get('source_sha256') != lane.registry_sha:
            findings.add('SOURCE_FINGERPRINT_MISMATCH', lane=alias, claim='producer meta source_sha256')
        nfi, nbo = _meta_int(meta.get('next_file_index')), _meta_int(meta.get('next_byte_offset'))
        if nfi is None or nbo is None:
            findings.add('PRODUCER_META_INVALID', lane=alias, field='next_file_index/next_byte_offset')
            return None
        if nfi > self.limits.max_events_per_lane:
            findings.add('PRODUCER_META_INVALID', lane=alias, reason='next_file_index exceeds max_events_per_lane')
            return None
        raw_cursor = meta.get('archived_records')
        archived = 0 if raw_cursor is None else _meta_int(raw_cursor)
        if archived is None:
            findings.add('ARCHIVE_CURSOR_INVALID', lane=alias, value=raw_cursor)
        meta.update(_nfi=nfi, _nbo=nbo, _archived=archived)
        return meta

    def _lane_state(self, lane, next_file_index):
        findings, alias = self.findings, lane.alias
        rows = lane.snapshot.conn.execute('SELECT lane, next_seq, integrity FROM lanes LIMIT 3').fetchall()
        if len(rows) > 1 or (rows and rows[0]['lane'] != lane.scope):
            findings.add('PRODUCER_LANE_STATE_UNRECOGNIZED', lane=alias, rows=len(rows))
            lane.verifiable = False
        elif rows:
            row = rows[0]
            if row['integrity'] == 'CONFLICT':
                findings.add('LANE_CONFLICT', lane=alias)
            elif row['integrity'] == 'WAITING_FOR_MISSING':
                findings.add('LANE_WAITING_FOR_MISSING', lane=alias, next_seq=row['next_seq'])
            elif row['integrity'] != 'CONTIGUOUS':
                findings.add('PRODUCER_LANE_STATE_UNRECOGNIZED', lane=alias, integrity=row['integrity'])
            if _is_int(row['next_seq']):
                lane.next_seq = row['next_seq']
                lane.n_max = next_file_index
                lane.bitmap = bytearray((next_file_index + 7) // 8)
            else:
                findings.add('PRODUCER_LANE_STATE_UNRECOGNIZED', lane=alias, next_seq=row['next_seq'])
                lane.verifiable = False
        pending = self._scalar(lane, 'SELECT COUNT(*) FROM pending')
        if pending:
            findings.add('PENDING_GAP_UNRESOLVED', lane=alias, pending=pending)
        unprocessed = self._scalar(lane, 'SELECT COUNT(*) FROM raw_journal WHERE processed = 0')
        if unprocessed:
            findings.add('RAW_UNPROCESSED', lane=alias, unprocessed=unprocessed)

    def _manifest(self, lane, archived, next_file_index):
        findings, alias, limits = self.findings, lane.alias, self.limits
        db = lane.snapshot.conn
        present = db.execute("SELECT 1 FROM sqlite_master WHERE lower(name) = 'archive_segments'").fetchone()
        if present is None:
            if archived:
                findings.add('ARCHIVE_MANIFEST_MISSING', lane=alias, archived_records=archived)
                return False, []
            return archived is not None, []
        problem = _plain_table(db, 'archive_segments', SEGMENT_COLUMNS)
        if problem:
            findings.add('ARCHIVE_MANIFEST_UNRECOGNIZED', lane=alias, reason=problem)
            return False, []
        # Every segment holds >= 1 source line, so more rows than lines is impossible.
        if self._scalar(lane, 'SELECT COUNT(*) FROM archive_segments') > next_file_index:
            findings.add('ARCHIVE_MANIFEST_UNRECOGNIZED', lane=alias,
                         reason='more segment rows than source lines')
            return False, []
        ok = archived is not None
        segments, expected_start = [], 0
        for row in db.execute('SELECT * FROM archive_segments ORDER BY start_file_index'):
            segment = dict(row)
            start, end = segment['start_file_index'], segment['end_file_index']
            count, outs = segment['record_count'], segment['outbox_count']
            if not (all(_is_int(v) for v in (start, end, count, outs))
                    and isinstance(segment['archive_path'], str)
                    and isinstance(segment['archive_sha256'], str)
                    and HEX64.fullmatch(segment['archive_sha256'])):
                findings.add('ARCHIVE_MANIFEST_UNRECOGNIZED', lane=alias, start_file_index=start,
                             reason='unexpected manifest field types')
                ok = False
                continue
            if count > limits.max_records_per_segment:
                findings.add('ARCHIVE_TOO_MANY_RECORDS', lane=alias, start_file_index=start,
                             record_count=count, limit=limits.max_records_per_segment)
                ok = False
            elif count < 1 or not 0 <= outs <= count:
                findings.add('ARCHIVE_MANIFEST_UNRECOGNIZED', lane=alias, start_file_index=start,
                             reason='record_count/outbox_count out of range')
                ok = False
            if start != expected_start or end != start + count:
                findings.add('ARCHIVE_SEGMENT_CONTINUITY', lane=alias, start_file_index=start,
                             end_file_index=end, expected_start=expected_start)
                ok = False
            expected_start = end
            segments.append(segment)
        if archived is not None and expected_start != archived:
            findings.add('ARCHIVE_CURSOR_MISMATCH', lane=alias, manifest_end=expected_start,
                         archived_records=archived)
            ok = False
        return ok, segments

    def _archive_dir(self, lane):
        """List archives/<alias>/ after proving it does not escape the run root."""
        directory, parent = lane.archive_dir, self.root / 'archives'
        if not os.path.lexists(directory):
            return True, None
        try:
            escaped = (_norm(parent.resolve()) != _norm(parent) or _norm(directory.resolve()) != _norm(directory)
                       or not directory.is_dir())
            if escaped:
                self.findings.add('ARCHIVE_PATH_CONFLICT', lane=lane.alias,
                                  reason='archive directory is not a plain directory inside the run root')
                return False, None
            names = []
            with os.scandir(directory) as found:
                for entry in found:
                    if len(names) >= self.limits.max_archive_dir_entries:
                        self.findings.add('ARCHIVE_PATH_CONFLICT', lane=lane.alias,
                                          reason='archive directory has too many entries to audit',
                                          limit=self.limits.max_archive_dir_entries)
                        return False, None
                    names.append(entry.name)
            return True, sorted(names)
        except OSError as exc:
            self.findings.add('ARCHIVE_PATH_CONFLICT', lane=lane.alias,
                              reason=f'cannot list archive directory: {type(exc).__name__}')
            return False, None

    def _paths(self, lane, segments, entries, archived, comparable):
        """Check stored paths (string-only) and classify unregistered directory entries."""
        findings, alias = self.findings, lane.alias
        registered, stored_seen = set(), set()
        for segment in segments:
            start, end = segment['start_file_index'], segment['end_file_index']
            if not (_is_int(start) and _is_int(end) and isinstance(segment['archive_path'], str)):
                continue
            name = SEGMENT_NAME.format(start, end) if start >= 0 and end >= 0 else f'invalid_{start}_{end}'
            segment['_name'] = name
            registered.add(name)
            stored = segment['archive_path']
            stored_base = re.split(r'[\\/]', stored)[-1]
            if stored_base != name:
                findings.add('ARCHIVE_PATH_UNRECOGNIZED', lane=alias, segment=name, stored_basename=stored_base)
            key = _norm(stored)
            if key in stored_seen:
                findings.add('ARCHIVE_PATH_CONFLICT', lane=alias, segment=name,
                             reason='two manifest rows share one stored archive_path')
            stored_seen.add(key)
            if key != _norm(lane.archive_dir / name):
                lane.counts['relocated_segments'] += 1
                if self.allow_relocated:
                    findings.add('ARCHIVE_PATH_RELOCATED_ALLOWED', lane=alias, segment=name)
                else:
                    findings.add('ARCHIVE_PATH_RELOCATED', lane=alias, segment=name)
            if entries is not None and name + '.tmp' in entries:
                findings.add('ARCHIVE_PATH_CONFLICT', lane=alias, segment=name,
                             reason='temporary sibling of a registered segment exists')
        overlaps = []
        for name in entries or ():
            if name in registered or (name.endswith('.tmp') and name[:-4] in registered):
                continue
            match = SEGMENT_RE.fullmatch(name)
            if match and int(match[1]) < int(match[2]):
                lo, hi = int(match[1]), int(match[2])
                lane.counts['unreferenced_archive_files'] += 1
                if lo < archived and comparable:
                    overlaps.append((name, lo, hi))
                else:
                    findings.add('UNREFERENCED_ARCHIVE_FILE', lane=alias, file=name,
                                 overlaps_archived_range=lo < archived)
            elif name.endswith('.tmp') and SEGMENT_RE.fullmatch(name[:-4]):
                lane.counts['unreferenced_archive_files'] += 1
                findings.add('UNREFERENCED_ARCHIVE_FILE', lane=alias, file=name, temporary=True)
            else:
                findings.add('UNRECOGNIZED_ARCHIVE_DIR_ENTRY', lane=alias, entry=name)
        if len(overlaps) > self.limits.max_unreferenced_overlap_files:
            findings.add('ARCHIVE_UNREFERENCED_CONTENT_CONFLICT', lane=alias, files=len(overlaps),
                         reason='too many unregistered files overlap the archived range to verify')
            return []
        return overlaps

    def _read_archive_file(self, lane, path, label):
        """Return the bytes of a regular in-root archive file, or ``(code, detail)``."""
        limits = self.limits
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            return None, ('ARCHIVE_SEGMENT_MISSING', {})
        except OSError as exc:
            return None, ('ARCHIVE_SEGMENT_UNREADABLE', {'reason': type(exc).__name__})
        if stat_mode.S_ISLNK(info.st_mode) or not stat_mode.S_ISREG(info.st_mode):
            return None, ('ARCHIVE_PATH_CONFLICT', {'reason': f'{label} path is not a regular file'})
        if info.st_size > limits.max_compressed_bytes:
            return None, ('ARCHIVE_SEGMENT_TOO_LARGE', {'bytes': info.st_size, 'limit': limits.max_compressed_bytes})
        try:
            with open(path, 'rb') as handle:
                opened = os.fstat(handle.fileno())
                if (opened.st_ino, opened.st_dev, opened.st_size) != (info.st_ino, info.st_dev, info.st_size):
                    return None, ('ARCHIVE_PATH_CONFLICT', {'reason': f'{label} changed between lstat and open'})
                # Read size + 1, never the limit: read(n) pre-allocates n bytes.
                data = handle.read(opened.st_size + 1)
        except OSError as exc:
            return None, ('ARCHIVE_SEGMENT_UNREADABLE', {'reason': type(exc).__name__})
        if len(data) != opened.st_size:
            return None, ('ARCHIVE_SEGMENT_UNREADABLE', {'reason': 'file size changed while reading'})
        return data, None

    def _segment(self, lane, segment, cursor, previous_id, need, overlap_hashes):
        findings, alias = self.findings, lane.alias
        name = segment.get('_name', '?')
        start, end = segment['start_file_index'], segment['end_file_index']
        data, problem = self._read_archive_file(lane, lane.archive_dir / name, 'canonical segment')
        if problem:
            findings.add(problem[0], lane=alias, segment=name, **problem[1])
            return None
        if hashlib.sha256(data).hexdigest() != segment['archive_sha256']:
            findings.add('ARCHIVE_SHA256_MISMATCH', lane=alias, segment=name)
            return None
        self.content_claims.append((f'archive:{alias}/{name}', lane.archive_dir / name, segment['archive_sha256']))
        records = outs = 0
        index = start
        try:
            for line in _gzip_lines(data, self.limits):
                if index >= end:
                    raise _Reject('ARCHIVE_MANIFEST_COUNT_MISMATCH', reason='more records than record_count')
                item = _parse_item(line, index)
                previous_id = self._archived_record(lane, item, line, cursor, previous_id, need, overlap_hashes)
                records += 1
                index += 1
                outs += 'outbox' in item
        except _Reject as rejection:
            findings.add(rejection.code, lane=alias, segment=name, file_index=index, **rejection.detail)
            return None
        if records != segment['record_count'] or outs != segment['outbox_count']:
            findings.add('ARCHIVE_MANIFEST_COUNT_MISMATCH', lane=alias, segment=name, records=records,
                         record_count=segment['record_count'], outbox_records=outs,
                         outbox_count=segment['outbox_count'])
            return previous_id
        lane.counts['archived_segments_parsed'] += 1
        return previous_id

    def _anchor(self, lane, cursor, index, raw_sha, origin):
        if cursor is None:
            return
        if cursor.index < index:
            cursor.skip_to(index)
        got = cursor.next_hash() if cursor.index == index else None
        if got is None:
            if cursor.failed and not cursor.reported:
                cursor.reported = True
                self.findings.add(cursor.failed, lane=lane.alias, file_index=cursor.index,
                                  limit=self.limits.max_source_line_bytes)
            elif not cursor.failed:
                self.findings.add('SOURCE_ANCHOR_MISMATCH', lane=lane.alias, file_index=index, origin=origin,
                                  reason='no source line at this position')
            return
        if got != raw_sha:
            self.findings.add('SOURCE_ANCHOR_MISMATCH', lane=lane.alias, file_index=index, origin=origin)
            return
        lane.counts['source_lines_anchored'] += 1
        self.metrics['source_lines_anchored'] += 1

    def _archived_record(self, lane, item, line, cursor, previous_id, need, overlap_hashes):
        findings, alias = self.findings, lane.alias
        index = item['file_index']
        if any(lo <= index < hi for lo, hi in need):
            overlap_hashes[index] = hashlib.sha256(line).hexdigest()
        if previous_id is not None and item['id'] <= previous_id:
            findings.add('RAW_ID_ORDER_VIOLATION', lane=alias, file_index=index, raw_id=item['id'],
                         previous_raw_id=previous_id)
        lane.counts['archived_records_parsed'] += 1
        self.metrics['archived_records_parsed'] += 1
        if _text_sha(item['payload']) != item['raw_sha256']:
            findings.add('ARCHIVE_RAW_HASH_MISMATCH', lane=alias, file_index=index)
        self._anchor(lane, cursor, index, item['raw_sha256'], 'archive')
        outcome = item['outcome']
        lane.dispositions['archived'][outcome] = lane.dispositions['archived'].get(outcome, 0) + 1
        if outcome.startswith('QUARANTINED'):
            self.metrics['archived_quarantined'] += 1
            findings.add('ARCHIVED_QUARANTINE_PRESENT', lane=alias, file_index=index, outcome=outcome)
        elif outcome == 'DUPLICATE_IDENTICAL':
            self._verify_duplicate(lane, item['payload'], index, 'archive')
        outbox = item.get('outbox')
        if outbox is not None:
            if outbox['acknowledged'] is not True:
                findings.add('ARCHIVED_RECORD_NOT_ACKED', lane=alias, file_index=index, seq=outbox['seq'])
            event = None
            if outbox['lane'] != lane.scope:
                findings.add('ARCHIVE_OUTBOX_INVALID', lane=alias, file_index=index,
                             reason='outbox lane differs from the registered scope')
            else:
                try:
                    event = _published_event(lane.scope, outbox['seq'], outbox['raw_sha256'], outbox['payload'])
                except _PARSE_ERRORS as exc:
                    findings.add('ARCHIVE_OUTBOX_INVALID', lane=alias, file_index=index, seq=outbox['seq'],
                                 reason=str(exc))
            if not _binding_ok(item['payload'], outbox['payload']):
                findings.add('ARCHIVE_RAW_OUTBOX_BINDING_MISMATCH', lane=alias, file_index=index,
                             seq=outbox['seq'])
            if event is None:
                lane.verifiable = False
            else:
                self.metrics['archived_events'] += 1
                self._expect(lane, event, 'ARCHIVE', True)
        return item['id']

    def _verify_duplicate(self, lane, raw_payload, file_index, origin):
        """A DUPLICATE_IDENTICAL disposition is a producer claim; prove it from evidence.

        The raw source event must be valid, in this lane, and byte-identical (same canonical
        SHA-256) to a published event with the same market seq: either a resident outbox row
        or a consumer effect (the consumer pass separately proves every effect belongs to a
        published event).  Otherwise an invalid record or a never-delivered event could be
        hidden behind the label.
        """
        reason = None
        try:
            event = json.loads(raw_payload) if isinstance(raw_payload, str) else None
        except _PARSE_ERRORS:
            event = None
        if not isinstance(event, dict) or not valid(event):
            reason = 'raw record is not a valid event'
        elif scope_of(event) != lane.scope:
            reason = 'raw record belongs to another lane'
        else:
            seq, digest = event['source_seq'], hashlib.sha256(canonical(event).encode('utf-8')).hexdigest()
            row = lane.snapshot.conn.execute('SELECT raw_sha256 FROM outbox WHERE lane = ? AND seq = ?',
                                             (lane.scope, seq)).fetchone()
            if row is not None:
                identical = row['raw_sha256'] == digest
            elif self.consumer_ok:
                rows = self.consumer.conn.execute('SELECT payload_sha256 FROM effects WHERE lane = ? AND '
                                                  'seq = ? AND epoch = 0', (lane.scope, seq)).fetchall()
                identical = len(rows) == 1 and rows[0]['payload_sha256'] == digest
            else:
                identical = False
            if not identical:
                reason = 'no identical published event with this seq'
        if reason:
            self.findings.add('DISPOSITION_CLAIM_UNVERIFIED', lane=lane.alias, file_index=file_index,
                              outcome='DUPLICATE_IDENTICAL', origin=origin, reason=reason)

    def _resident(self, lane, cursor, start, previous_id, next_file_index):
        findings, alias = self.findings, lane.alias
        expected, count = start, 0
        for row in lane.snapshot.conn.execute('SELECT r.id, r.file_index, r.raw_sha256, r.payload, r.processed, '
                                              'd.outcome FROM raw_journal r LEFT JOIN disposition d '
                                              'ON d.raw_id = r.id ORDER BY r.file_index'):
            count += 1
            file_index, raw_id = row['file_index'], row['id']
            if not (_is_int(file_index) and _is_int(raw_id)):
                findings.add('RESIDENT_JOURNAL_TAMPERED', lane=alias, reason='non-integer id/file_index')
                lane.verifiable = False
                continue
            if expected is None:
                expected = file_index
            if file_index != expected:
                findings.add('RESIDENT_JOURNAL_RANGE_MISMATCH', lane=alias, expected_file_index=expected,
                             found_file_index=file_index)
                expected = file_index
            expected += 1
            if previous_id is not None and raw_id <= previous_id:
                findings.add('RAW_ID_ORDER_VIOLATION', lane=alias, file_index=file_index, raw_id=raw_id,
                             previous_raw_id=previous_id)
            previous_id = raw_id
            if row['processed'] not in (0, 1) or _text_sha(row['payload']) != row['raw_sha256']:
                findings.add('RESIDENT_JOURNAL_TAMPERED', lane=alias, file_index=file_index)
            self._anchor(lane, cursor, file_index, row['raw_sha256'], 'resident')
            if row['outcome'] == 'DUPLICATE_IDENTICAL':
                self._verify_duplicate(lane, row['payload'], file_index, 'resident')
        lane.counts['resident_raw_records'] = count
        if expected is not None and expected != next_file_index:
            findings.add('RESIDENT_JOURNAL_RANGE_MISMATCH', lane=alias, expected_end=next_file_index,
                         found_end=expected)

    def _source_tail(self, lane, cursor, next_file_index, next_byte_offset):
        findings, alias = self.findings, lane.alias
        if cursor is None:
            try:
                size = os.stat(lane.source).st_size
            except OSError:
                size = None
        else:
            if not cursor.failed and cursor.index == next_file_index and cursor.offset != next_byte_offset:
                findings.add('SOURCE_OFFSET_MISMATCH', lane=alias, next_byte_offset=next_byte_offset,
                             anchored_bytes=cursor.offset)
            digest, size = cursor.finish()
            if digest != lane.registry_sha:
                findings.add('CONCURRENT_MODIFICATION_DETECTED', lane=alias, evidence='source',
                             reason='source bytes changed while being anchored')
        if size != next_byte_offset:
            findings.add('SOURCE_NOT_EXHAUSTED', lane=alias, source_bytes=size, next_byte_offset=next_byte_offset)

    def _resident_outbox(self, lane):
        findings, metrics, alias = self.findings, self.metrics, lane.alias
        sql = ('SELECT o.lane, o.seq, o.raw_id, o.raw_sha256, o.payload, o.delivered, '
               'r.id AS r_id, r.processed AS r_processed, r.payload AS r_payload, '
               'd.outcome AS d_outcome, v.raw_id AS v_raw_id, v.raw_sha256 AS v_raw_sha256 '
               'FROM outbox o '
               'LEFT JOIN raw_journal r ON r.id = o.raw_id '
               'LEFT JOIN disposition d ON d.raw_id = o.raw_id '
               'LEFT JOIN delivery v ON v.lane = o.lane AND v.seq = o.seq '
               'ORDER BY o.lane, o.seq')
        for row in lane.snapshot.conn.execute(sql):
            lane.counts['resident_outbox'] += 1
            metrics['producer_outbox'] += 1
            delivered, seq = row['delivered'], row['seq']
            if delivered == 1:
                lane.counts['resident_outbox_delivered'] += 1
                metrics['producer_acked'] += 1
            event = None
            if row['lane'] == lane.scope and delivered in (0, 1):
                try:
                    event = _published_event(lane.scope, seq, row['raw_sha256'], row['payload'])
                except _PARSE_ERRORS:
                    event = None
            if event is None:
                metrics['invalid_producer_rows'] += 1
                findings.add('PRODUCER_OUTBOX_TAMPERED', lane=alias, seq=seq)
                lane.verifiable = False
            if row['r_id'] is None or row['r_processed'] != 1:
                findings.add('PRODUCER_OUTBOX_ORPHAN', lane=alias, seq=seq)
            elif not _binding_ok(row['r_payload'], row['payload']):
                findings.add('RESIDENT_RAW_OUTBOX_BINDING_MISMATCH', lane=alias, seq=seq)
            if row['d_outcome'] != 'ORDERED_PUBLISHED':
                findings.add('PRODUCER_DISPOSITION_MISMATCH', lane=alias, seq=seq, outcome=row['d_outcome'])
            if delivered == 1:
                receipt_ok = row['v_raw_id'] == row['raw_id'] and row['v_raw_sha256'] == row['raw_sha256']
            else:
                receipt_ok = row['v_raw_id'] is None
            if not receipt_ok:
                metrics['producer_receipt_inconsistencies'] += 1
                findings.add('PRODUCER_ACK_LEDGER_INCONSISTENT', lane=alias, seq=seq)
            if event is not None:
                self._expect(lane, event, 'RESIDENT', delivered == 1)

    def _resident_dispositions(self, lane):
        findings, alias = self.findings, lane.alias
        resident = lane.dispositions['resident']
        known = sorted(KNOWN_OUTCOMES)
        marks = ','.join('?' * len(known))
        # Only known outcomes are grouped individually: report size stays bounded however many
        # distinct unknown labels the evidence contains.
        for row in lane.snapshot.conn.execute(f'SELECT outcome, COUNT(*) AS n FROM disposition '
                                              f'WHERE outcome IN ({marks}) GROUP BY outcome', known):
            resident[row['outcome']] = row['n']
            if row['outcome'].startswith('QUARANTINED'):
                findings.add('RESIDENT_QUARANTINE_PRESENT', lane=alias, outcome=row['outcome'], count=row['n'])
        unknown = lane.snapshot.conn.execute(f'SELECT COUNT(*) AS n, COUNT(DISTINCT outcome) AS d FROM disposition '
                                             f'WHERE outcome IS NULL OR outcome NOT IN ({marks})', known).fetchone()
        if unknown['n']:
            resident['UNRECOGNIZED'] = unknown['n']
            findings.add('PRODUCER_DISPOSITION_UNRECOGNIZED', lane=alias, count=unknown['n'],
                         distinct_labels=unknown['d'])
        outbox_rows = lane.counts['resident_outbox']
        if resident.get('ORDERED_PUBLISHED', 0) != outbox_rows:
            findings.add('PRODUCER_DISPOSITION_MISMATCH', lane=alias, reason='ORDERED_PUBLISHED count differs',
                         published=resident.get('ORDERED_PUBLISHED', 0), outbox=outbox_rows)
        if self._scalar(lane, 'SELECT COUNT(DISTINCT raw_id) FROM outbox') != outbox_rows:
            findings.add('PRODUCER_DISPOSITION_MISMATCH', lane=alias, reason='outbox rows share a raw_id')
        orphan = self._scalar(lane, 'SELECT COUNT(*) FROM disposition d LEFT JOIN raw_journal r '
                                    'ON r.id = d.raw_id WHERE r.id IS NULL')
        if orphan:
            findings.add('PRODUCER_DISPOSITION_ORPHAN', lane=alias, count=orphan)
        missing = self._scalar(lane, 'SELECT COUNT(*) FROM raw_journal r LEFT JOIN disposition d '
                                     'ON d.raw_id = r.id WHERE r.processed = 1 AND d.raw_id IS NULL '
                                     'AND r.id NOT IN (SELECT raw_id FROM pending)')
        if missing:
            findings.add('RESIDENT_DISPOSITION_MISSING', lane=alias, count=missing)
        receipts = self._scalar(lane, 'SELECT COUNT(*) FROM delivery v LEFT JOIN outbox o '
                                      'ON o.lane = v.lane AND o.seq = v.seq WHERE o.lane IS NULL')
        if receipts:
            self.metrics['producer_receipt_inconsistencies'] += receipts
            findings.add('PRODUCER_RECEIPT_ORPHAN', lane=alias, count=receipts)

    def _compare_unreferenced(self, lane, name, lo, hi, archived, overlap_hashes):
        """None if the unregistered file repeats the registered records byte for byte."""
        data, problem = self._read_archive_file(lane, lane.archive_dir / name, 'unregistered segment')
        if problem:
            return f'{problem[0]}: {problem[1]}'
        self.content_claims.append((f'archive:{lane.alias}/{name}', lane.archive_dir / name,
                                    hashlib.sha256(data).hexdigest()))
        index = lo
        try:
            for line in _gzip_lines(data, self.limits):
                if index >= hi:
                    return 'more records than its file name declares'
                if index < archived:
                    known = overlap_hashes.get(index)
                    if known is None:
                        return f'registered record {index} could not be verified for comparison'
                    if known != hashlib.sha256(line).hexdigest():
                        return f'record {index} differs from the registered archive'
                index += 1
        except _Reject as rejection:
            return f'{rejection.code}: {rejection.detail}'
        if index != hi:
            return 'fewer records than its file name declares'
        return None

    def _contiguity(self, lane):
        count = lane.published
        if lane.next_seq is None:
            lane.contiguous = count == 0 and lane.counts['resident_outbox'] == 0
            return
        full, rest = divmod(count, 8)
        lane.contiguous = (lane.bitmap[:full] == b'\xff' * full
                           and (rest == 0 or lane.bitmap[full] & ((1 << rest) - 1) == (1 << rest) - 1))
        # Only a fully read archive makes the bitmap complete; holes left by unreadable
        # segments are already explained by their own fatal finding and are not a "gap".
        if not lane.contiguous and lane.archived_complete:
            self.findings.add('PRODUCER_SEQUENCE_GAP', lane=lane.alias, next_seq=lane.next_seq,
                              published_events=count)
        lane.first_seq = lane.next_seq - count if count and lane.contiguous else None

    def _provenance(self, lane, meta, computed, next_file_index, next_byte_offset):
        findings, alias = self.findings, lane.alias
        provenance = {'source_path': str(lane.source), 'source_sha256_computed': computed,
                      'registry_sha256': lane.registry_sha,
                      'producer_meta_source_sha256': _jsonable(meta.get('source_sha256')),
                      'producer_meta_source_filename': _jsonable(meta.get('source_filename')),
                      'source_manifest': None}
        path = lane.source.parent / SOURCE_MANIFEST_NAME
        if not path.is_file():
            findings.add('SOURCE_MANIFEST_ABSENT', lane=alias)
            return provenance
        try:
            raw = _read_bounded(path, self.limits.max_metadata_bytes)
            self.content_claims.append((f'source_manifest:{alias}', path, hashlib.sha256(raw).hexdigest()))
            document = json.loads(raw.decode('utf-8'))
        except (OSError, UnicodeDecodeError, ValueError, RecursionError) as exc:
            findings.add('SOURCE_MANIFEST_UNRECOGNIZED', lane=alias, reason=type(exc).__name__)
            return provenance
        if not isinstance(document, dict):
            findings.add('SOURCE_MANIFEST_UNRECOGNIZED', lane=alias, reason='not a JSON object')
            return provenance
        if document.get('jsonl_filename') != lane.source.name:
            findings.add('SOURCE_MANIFEST_ABSENT', lane=alias, reason='source_manifest.json describes another file')
            return provenance
        if document.get('schema') != SOURCE_MANIFEST_SCHEMA or not isinstance(document.get('jsonl_sha256'), str):
            findings.add('SOURCE_MANIFEST_UNRECOGNIZED', lane=alias, reason='unexpected schema')
            return provenance
        matches = computed is not None and document['jsonl_sha256'] == computed
        if not matches:
            findings.add('SOURCE_MANIFEST_MISMATCH', lane=alias, field='jsonl_sha256')
        if matches and document.get('records') != next_file_index:
            findings.add('SOURCE_MANIFEST_MISMATCH', lane=alias, field='records',
                         declared=document.get('records'), lines=next_file_index)
        if matches and lane.verifiable and lane.contiguous and lane.first_seq is not None and (
                document.get('source_seq_first') != lane.first_seq
                or document.get('source_seq_last') != lane.next_seq - 1):
            findings.add('SOURCE_MANIFEST_MISMATCH', lane=alias, field='source_seq_first/last')
        declared = {key: _jsonable(document.get(key)) for key in (
            'schema', 'normalization_version', 'source_kind', 'archive_name', 'archive_member',
            'independent_checksum_provided', 'eighth_csv_boolean_preserved', 'jsonl_filename',
            'canonical_events_sha256')}
        declared.update({
            'path': str(path),
            'archive_sha256_declared': _jsonable(document.get('archive_sha256')),
            'archive_sha256_verified_by_this_audit': False,
            'jsonl_sha256_declared': _jsonable(document['jsonl_sha256']),
            'jsonl_sha256_matches_computed': matches,
            'records_declared': _jsonable(document.get('records')),
            'source_seq_first_declared': _jsonable(document.get('source_seq_first')),
            'source_seq_last_declared': _jsonable(document.get('source_seq_last')),
        })
        provenance['source_manifest'] = declared
        return provenance

    # -- result ----------------------------------------------------------
    def verify_content(self):
        """After every handle is closed, re-hash each relied-upon file by CONTENT.

        Stat fingerprints alone miss a same-size rewrite whose mtime was restored; a content
        mismatch here means the evidence on disk is no longer what was verified.
        """
        for label, path, expected in self.content_claims:
            try:
                actual = _sha256_path(path)
            except OSError as exc:
                self.content_unreadable.append(label)
                self.findings.add('SNAPSHOT_UNVERIFIABLE', evidence=label, reason=type(exc).__name__)
                continue
            if actual != expected:
                self.content_changes.append(label)
                self.findings.add('CONCURRENT_MODIFICATION_DETECTED', evidence=label, method='content')

    def _stability(self, before, after, registry_before, registry_after):
        findings = self.findings
        keys = sorted(set(before) | set(after))
        changed = [k for k in keys if before.get(k) != after.get(k)]
        unreadable = [k for k in keys if _has_unreadable(before.get(k)) or _has_unreadable(after.get(k))]
        if registry_after is None:
            unreadable.append('registry:lane_registry.json')
        elif registry_after != registry_before:
            changed.append('registry:lane_registry.json bytes')
        moved = [s.label for s in self.snapshots if not s.stable]
        for key in changed:
            findings.add('CONCURRENT_MODIFICATION_DETECTED', evidence=key, method='fingerprint')
        for label in moved:
            findings.add('CONCURRENT_MODIFICATION_DETECTED', evidence=label, method='data_version')
        for key in unreadable:
            findings.add('SNAPSHOT_UNVERIFIABLE', evidence=key)
        verified = (not changed and not moved and not unreadable
                    and not findings.counts.get('CONCURRENT_MODIFICATION_DETECTED')
                    and not findings.counts.get('SNAPSHOT_UNVERIFIABLE')
                    and all(s.closed for s in self.snapshots))
        return {
            'verified': verified,
            'methods': ['one read transaction per SQLite database',
                        'pre/post fingerprints: SQLite main/-wal/-journal (type, size, mtime_ns, inode, sha256), '
                        'registry bytes, source and source-manifest stat, archives/ directory listings',
                        'PRAGMA data_version before/after for databases with WAL frames',
                        'source bytes re-hashed while anchored; archive bytes hashed from the buffer that is inflated',
                        'post-audit content re-hash of every source, source manifest and archive file relied upon'],
            'immutable_databases': [s.label for s in self.snapshots if s.immutable],
            'wal_databases': [s.label for s in self.snapshots if not s.immutable],
            'changed_evidence': changed + moved + self.content_changes,
            'unverifiable_evidence': unreadable + self.content_unreadable,
        }

    def _lane_report(self, lane, snapshot_ok):
        fatal = self.findings.fatal(lane.alias)
        anchored = (lane.source_lines_expected is not None
                    and lane.counts['source_lines_anchored'] == lane.source_lines_expected)
        if fatal or not lane.verifiable or not snapshot_ok or not self.consumer_ok:
            status = 'INCONSISTENT'
        elif lane.retry:
            status = 'RETRY_REQUIRED'
        elif lane.pending_delivery:
            status = 'PENDING_DELIVERY'
        elif anchored and lane.archived_complete and lane.contiguous and lane.matched == lane.expected:
            status = 'COMPLETE'
        else:
            status = 'INCONSISTENT'
        report = {'status': status, 'scope': json.loads(lane.scope), 'verifiable': lane.verifiable,
                  'issue_counts': dict(sorted(self.findings.lane_counts.get(lane.alias, {}).items())),
                  'source_lines_expected': lane.source_lines_expected,
                  'archived_records': lane.archived_records}
        report.update(lane.counts)
        report.update({'published_events': lane.published, 'first_seq': lane.first_seq,
                       'next_seq': lane.next_seq, 'sequence_contiguous': lane.contiguous,
                       'dispositions': {side: dict(sorted(v.items())) for side, v in lane.dispositions.items()},
                       'provenance': lane.provenance,
                       'prefix_note': 'Events before first_seq are UNVERIFIED_PREFIX: this audit proves '
                                      'local evidence consistency, not venue completeness'})
        return report

    def result(self, before, after, registry_before, registry_after, options):
        findings, metrics = self.findings, self.metrics
        snapshot = self._stability(before, after, registry_before, registry_after)
        metrics['archived_records'] = sum(lane.archived_records for lane in self.lanes)
        metrics['archived_segments'] = sum(lane.counts['archived_segments'] for lane in self.lanes)
        metrics['archived_segments_parsed'] = sum(lane.counts['archived_segments_parsed'] for lane in self.lanes)
        invariants = {
            'snapshot_consistent': snapshot['verified'],
            'consumer_schema_recognized': self.consumer_ok,
            'all_lanes_verifiable': all(lane.verifiable for lane in self.lanes),
            'every_source_line_anchored': all(
                lane.source_lines_expected is not None
                and lane.counts['source_lines_anchored'] == lane.source_lines_expected for lane in self.lanes),
            'archived_records_fully_parsed': all(lane.archived_complete for lane in self.lanes),
            'sequences_contiguous': all(lane.contiguous for lane in self.lanes),
            'expected_equals_matched': metrics['expected_events_total'] == metrics['matched_effects'],
            'consumer_effects_equals_matched': metrics['consumer_effects'] == metrics['matched_effects'],
            'identity_digests_equal': self.producer_digest == self.consumer_digest,
        }
        if findings.fatal():
            status = 'INCONSISTENT'
        elif not all(invariants[k] for k in STRUCTURAL_INVARIANTS):
            findings.add('INTERNAL_INVARIANT_VIOLATION', failed=','.join(
                k for k in STRUCTURAL_INVARIANTS if not invariants[k]))
            status = 'INCONSISTENT'
        elif metrics['remote_committed_unacked']:
            status = 'RETRY_REQUIRED'
        elif metrics['remote_missing']:
            status = 'PENDING_DELIVERY'
        elif all(invariants[k] for k in EQUALITY_INVARIANTS):
            status = 'COMPLETE'
        else:
            findings.add('INTERNAL_INVARIANT_VIOLATION', failed=','.join(
                k for k in EQUALITY_INVARIANTS if not invariants[k]))
            status = 'INCONSISTENT'
        invariants['no_fatal_issues'] = findings.fatal() == 0
        lanes = {lane.alias: self._lane_report(lane, snapshot['verified']) for lane in self.lanes}
        fatal = findings.fatal()
        return {
            'schema': REPORT_SCHEMA,
            'mode': 'ARCHIVE_AWARE_READ_ONLY',
            'status': status,
            'passed': status == 'COMPLETE',
            'metrics': dict(metrics),
            'lanes': lanes,
            'issue_counts': dict(sorted(findings.counts.items())),
            'fatal_issue_count': fatal,
            'notice_count': sum(findings.counts.values()) - fatal,
            'issues': findings.samples,
            'snapshot_consistency': snapshot,
            'invariants': invariants,
            'options': options,
            'archive_format': ARCHIVE_FORMAT,
            'trust_model': TRUST_MODEL,
            'limits_of_claim': LIMITS_OF_CLAIM,
            'producer_root': str(self.root),
            'consumer_db': str(self.consumer_db),
        }


def _load_registry(path, limits):
    raw = _read_bounded(path, limits.max_metadata_bytes)
    try:
        registry = json.loads(raw.decode('utf-8'))
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ValueError(f'lane_registry.json is not valid UTF-8 JSON: {exc}') from None
    if not isinstance(registry, dict) or not registry:
        raise ValueError('lane_registry.json must be a non-empty JSON object')
    scopes = set()
    for alias, record in registry.items():
        if not ALIAS_RE.fullmatch(alias):
            raise ValueError(f'Invalid lane alias in registry: {alias!r}')
        if not isinstance(record, dict) or not all(isinstance(record.get(k), str) for k in ('source', 'scope', 'sha256')):
            raise ValueError(f'Registry entry {alias!r} lacks source/scope/sha256 strings')
        if not HEX64.fullmatch(record['sha256']):
            raise ValueError(f'Registry entry {alias!r} has an invalid sha256')
        try:
            scope = json.loads(record['scope'])
        except (ValueError, RecursionError):
            raise ValueError(f'Registry entry {alias!r} has an invalid scope') from None
        if not (isinstance(scope, list) and len(scope) == 4 and all(isinstance(x, str) and x for x in scope)
                and canonical(scope) == record['scope']):
            raise ValueError(f'Registry entry {alias!r} has an invalid scope')
        if record['scope'] in scopes:
            raise ValueError('Two registry aliases share one scope')
        scopes.add(record['scope'])
    return raw, registry


def _report_target(report_path, root, protected, lanes, extra_dirs=()):
    """Resolve and vet the report path against evidence, links/junctions included."""
    target = Path(report_path).resolve()
    if target.is_dir():
        raise ValueError(f'report_path is a directory: {target}')
    if not target.parent.is_dir():
        raise FileNotFoundError(target.parent)
    key = _norm(target)
    # Compare with both the literal and the link-resolved evidence directories: a junction
    # at <root>/archives must not let the report land on a real archive file elsewhere.
    directories = [root / 'lanes', root / 'archives', *extra_dirs]
    for lane in lanes:
        directories.extend([lane.archive_dir, lane.db_path.parent])
    forbidden = set()
    for directory in directories:
        forbidden.add(_norm(directory))
        if os.path.lexists(directory):
            forbidden.add(_norm(directory.resolve()))
    for base in forbidden:
        if key == base or key.startswith(base.rstrip(os.sep) + os.sep):
            raise ValueError(f'report_path must not be inside evidence directory {base}')
    evidence = set()
    for path in protected:
        evidence.add(_norm(path))
        evidence.add(_norm(Path(path).resolve()))
    if key in evidence:
        raise ValueError('report_path would overwrite audit evidence')
    return target


def _write_report(target, result):
    data = (json.dumps(result, indent=2, ensure_ascii=False) + '\n').encode('utf-8')
    descriptor, temporary = tempfile.mkstemp(prefix=f'.{target.name}.', suffix='.tmp', dir=target.parent)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def reconcile_archive_aware(root, consumer_db, *, report_path=None, allow_relocated_archives=False,
                            limits=None):
    """Read-only archive-aware reconciliation; see ``docs/ARCHIVE_E2E_AUDIT.md``.

    Raises FileNotFoundError / ValueError / sqlite3.Error for precondition failures;
    every evidence problem is reported as a finding with a fail-closed status.
    """
    if limits is None:
        limits = ArchiveLimits()
    elif not isinstance(limits, ArchiveLimits):
        raise ValueError('limits must be an ArchiveLimits instance')
    root = Path(root).resolve()
    if not root.is_dir():
        raise FileNotFoundError(root)
    consumer_db = Path(consumer_db).resolve()
    registry_path = root / 'lane_registry.json'
    registry_raw, registry = _load_registry(registry_path, limits)
    lanes = [_Lane(alias, registry[alias], root) for alias in sorted(registry)]
    for lane in lanes:
        if not lane.db_path.is_file():
            raise FileNotFoundError(lane.db_path)
        if not lane.source.is_file():
            raise FileNotFoundError(lane.source)
    if not consumer_db.is_file():
        raise FileNotFoundError(consumer_db)
    for lane in lanes:
        if _norm(lane.db_path.resolve()) == _norm(consumer_db) or os.path.samefile(lane.db_path, consumer_db):
            raise ValueError('Consumer effects DB must be independent of every producer lane DB')

    content, plain, protected = [], [registry_path], [registry_path]
    for database in [consumer_db] + [lane.db_path for lane in lanes]:
        sidecars = [Path(f'{database}{suffix}') for suffix in ('', '-wal', '-journal')]
        content.extend(sidecars)
        protected.extend(sidecars + [Path(f'{database}-shm')])
    for lane in lanes:
        manifest = lane.source.parent / SOURCE_MANIFEST_NAME
        plain.extend([lane.source, manifest])
        protected.extend([lane.source, manifest])
    directories = [root / 'archives'] + [lane.archive_dir for lane in lanes]
    consumer_archive = Path(f'{consumer_db}.archive')
    directories.append(consumer_archive)
    if consumer_archive.is_dir() and not _redirected(consumer_archive):
        directories.extend(sorted(p for p in consumer_archive.iterdir() if p.is_dir()))
    target = (_report_target(report_path, root, protected, lanes, extra_dirs=(consumer_archive,))
              if report_path is not None else None)

    def capture():
        snap = {}
        for path in content:
            snap[f'db:{path}'] = _fingerprint(path, True)
        for path in plain:
            snap[f'file:{path}'] = _fingerprint(path, False)
        for path in directories:
            snap[f'dir:{path}'] = _listing(path)
        return snap

    before = capture()
    auditor = _Auditor(root, consumer_db, lanes, limits, allow_relocated_archives)
    try:
        auditor.open()
        auditor.run()
    finally:
        auditor.close()
    after = capture()
    auditor.verify_content()
    try:
        registry_after = _read_bounded(registry_path, limits.max_metadata_bytes)
    except (OSError, ValueError):
        registry_after = None
    options = {'allow_relocated_archives': bool(allow_relocated_archives), 'limits': asdict(limits)}
    result = auditor.result(before, after, registry_raw, registry_after, options)
    if target is not None:
        _write_report(target, result)
    return result
