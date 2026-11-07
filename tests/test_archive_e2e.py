"""Independent black-box tests of docs/ARCHIVE_E2E_AUDIT.md.

Fixtures use the shipped replay, consumer and retention APIs.  The auditor's
implementation is deliberately neither read nor patched by this suite.
"""
from __future__ import annotations

from contextlib import closing
from copy import deepcopy
from dataclasses import FrozenInstanceError, fields
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch
import zipfile

from ftdi_core import audit as local_audit
from ftdi_core import e2e
from ftdi_core.adapters.s070041 import multi_lane, outbox_publisher, retention
from ftdi_core.adapters.s070041.durable_stream import canonical
from ftdi_core.adapters.s070041.external_delivery import (
    EffectStore, envelope, event_identity,
)
from ftdi_core.delivery import local_lab
from ftdi_core.preparation import prepare_archive
from ftdi_core.service import replay


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTICE_CODES = {
    'UNREFERENCED_ARCHIVE_FILE', 'UNRECOGNIZED_ARCHIVE_DIR_ENTRY',
    'ARCHIVE_PATH_RELOCATED_ALLOWED', 'SOURCE_MANIFEST_ABSENT',
}
INVARIANT_KEYS = {
    'no_fatal_issues', 'snapshot_consistent', 'consumer_schema_recognized',
    'all_lanes_verifiable', 'every_source_line_anchored',
    'archived_records_fully_parsed', 'sequences_contiguous',
    'expected_equals_matched', 'consumer_effects_equals_matched',
    'identity_digests_equal',
}
REPORT_KEYS = {
    'schema', 'mode', 'status', 'passed', 'metrics', 'lanes', 'issue_counts',
    'fatal_issue_count', 'notice_count', 'issues', 'snapshot_consistency',
    'invariants', 'options', 'trust_model', 'limits_of_claim', 'producer_root',
    'consumer_db',
}
METRIC_KEYS = {
    'producer_outbox', 'producer_acked', 'archived_records',
    'archived_records_parsed', 'archived_segments', 'archived_segments_parsed',
    'archived_events', 'archived_events_matched', 'archived_quarantined',
    'expected_events_total', 'consumer_effects', 'matched_effects',
    'remote_missing', 'remote_committed_unacked', 'orphan_consumer_effects',
    'content_conflicts', 'invalid_consumer_rows', 'invalid_producer_rows',
    'producer_receipt_inconsistencies', 'archived_missing_consumer_effects',
    'duplicate_identities', 'unverifiable_lane_effects', 'source_lines_anchored',
}
LANE_KEYS = {
    'status', 'scope', 'verifiable', 'issue_counts', 'source_lines_expected',
    'source_lines_anchored', 'archived_records', 'archived_records_parsed',
    'archived_segments', 'archived_segments_parsed', 'relocated_segments',
    'unreferenced_archive_files', 'resident_raw_records', 'resident_outbox',
    'resident_outbox_delivered', 'published_events', 'first_seq', 'next_seq',
    'sequence_contiguous', 'dispositions', 'provenance', 'prefix_note',
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def sql(path, statement, parameters=()):
    """Materialize results and close the connection, including on SQL errors."""
    with closing(sqlite3.connect(path)) as db, db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(statement, parameters).fetchall()]


def evidence_snapshot(*directories):
    """Include every file, including SQLite sidecars and unexpected new files."""
    snapshot = {}
    for number, directory in enumerate(directories):
        for path in sorted(Path(directory).rglob('*')):
            if path.is_file():
                data = path.read_bytes()
                snapshot[f'{number}/{path.relative_to(directory).as_posix()}'] = (
                    len(data), sha256(data),
                )
    return snapshot


def read_segment(path):
    with gzip.open(path, 'rb') as stream:
        return [json.loads(line) for line in stream]


def write_segment(path, items, *, canonical_lines=True):
    """Reproducible gzip bytes; close both the gzip wrapper and its file."""
    with Path(path).open('wb') as raw:
        with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as zipped:
            for item in items:
                line = canonical(item) if canonical_lines else json.dumps(item)
                zipped.write((line + '\n').encode('utf-8'))


class Fixture:
    """All paths belong to the caller's TemporaryDirectory; no HTTP by default."""

    def __init__(self, base):
        self.base = Path(base)
        self.root = self.base / 'run'
        self.sources = self.base / 'sources'
        self.sources.mkdir()
        self.consumers = self.base / 'consumer'
        self.consumers.mkdir()
        self.consumer = self.consumers / 'effects.sqlite3'
        self.source = self.sources / 'btc.jsonl'
        with closing(EffectStore(self.consumer)):
            pass

    def db(self, alias='BTC'):
        return self.root / 'lanes' / alias / 'state.sqlite3'

    def ingest(self, n=12, *, order=None, invalid_last=False, blocked_eth=False):
        if not 1 <= n <= 60:
            raise ValueError('test fixtures must have 1..60 events')
        multi_lane.make_synthetic(self.source, n, None, 'BTCUSDT')
        if order is not None or invalid_last:
            events = [json.loads(line) for line in self.source.read_text().splitlines()]
            if order is not None:
                events = [events[seq - 1] for seq in order]
            if invalid_last:
                events[-1]['price'] = '0'
            self.source.write_bytes(''.join(canonical(e) + '\n' for e in events).encode())
        definitions = [f'BTC={self.source}']
        if blocked_eth:
            eth = self.sources / 'eth.jsonl'
            multi_lane.make_synthetic(eth, 12, gap_at=2, symbol='ETHUSDT')
            definitions.append(f'ETH={eth}')
        return self.replay(definitions, max_pending=5 if blocked_eth else 60)

    def replay(self, definitions=None, **kwargs):
        # Only fixture receive times are controlled; auditor internals are untouched.
        with patch('ftdi_core.adapters.s070041.durable_stream.time.time_ns',
                   return_value=1717200000000000000):
            return replay(self.root, definitions or [f'BTC={self.source}'],
                          batch=4, **kwargs)

    def rows(self, alias='BTC', limit=60):
        return sql(self.db(alias),
                   'SELECT * FROM outbox WHERE delivered=0 ORDER BY seq LIMIT ?',
                   (limit,))

    def ack(self, rows, alias='BTC'):
        with closing(sqlite3.connect(self.db(alias))) as db:
            db.row_factory = sqlite3.Row
            outbox_publisher.commit_ack(db, rows)

    def deliver(self, alias='BTC', *, limit=60, ack=True, store=None):
        rows = self.rows(alias, limit)
        events = [envelope(row) for row in rows]
        if events:
            if store is None:
                with closing(EffectStore(self.consumer)) as remote:
                    remote.accept(events)
            else:
                store.accept(events)
            if ack:
                self.ack(rows, alias)
        return rows

    def prune(self, limit=8, alias='BTC'):
        return retention.prune(self.root, alias, limit, consumer_db=self.consumer)

    def segments(self, alias='BTC'):
        return sql(self.db(alias),
                   'SELECT * FROM archive_segments ORDER BY start_file_index')

    def segment_path(self, number=0, alias='BTC'):
        row = self.segments(alias)[number]
        return self.root / 'archives' / alias / (
            f"raw_{row['start_file_index']:012d}_{row['end_file_index']:012d}.jsonl.gz")

    def update_segment_hash(self, number=0, alias='BTC'):
        row = self.segments(alias)[number]
        sql(self.db(alias),
            'UPDATE archive_segments SET archive_sha256=? WHERE start_file_index=?',
            (sha256(self.segment_path(number, alias).read_bytes()), row['start_file_index']))

    def rewrite_segment(self, mutate, number=0, *, canonical_lines=True):
        path = self.segment_path(number)
        items = read_segment(path)
        mutate(items)
        write_segment(path, items, canonical_lines=canonical_lines)
        # Counts deliberately remain unchanged unless a test explicitly changes them.
        self.update_segment_hash(number)
        return items

    def archived(self, n=12, limits=(4, 4)):
        self.ingest(n)
        self.deliver()
        for limit in limits:
            result = self.prune(limit)
            if result['archived_new'] != limit:
                raise AssertionError(result)
        return self

    def snapshot(self):
        return evidence_snapshot(self.root, self.consumers, self.sources)


def audit_fixture(fixture, **kwargs):
    # Lazy import permits helper validation before the implementation exists.
    from ftdi_core.archive_e2e import reconcile_archive_aware
    return reconcile_archive_aware(fixture.root, fixture.consumer, **kwargs)


class ArchiveE2EContractTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='archive_e2e_contract_')
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def assert_status(self, result, status, *codes):
        self.assertEqual(result['status'], status, result['issue_counts'])
        self.assertIs(result['passed'], status == 'COMPLETE')
        self.assertTrue(REPORT_KEYS <= result.keys())
        self.assertTrue(METRIC_KEYS <= result['metrics'].keys())
        self.assertEqual(result['schema'], 'FTDI-Core-E2E-Archive-Audit-v1')
        self.assertEqual(result['mode'], 'ARCHIVE_AWARE_READ_ONLY')
        self.assertLessEqual(len(result['issues']), 50)
        for lane in result['lanes'].values():
            self.assertTrue(LANE_KEYS <= lane.keys())
            self.assertTrue({'archived', 'resident'} <= lane['dispositions'].keys())
        for issue in result['issues']:
            self.assertIn(issue['code'], result['issue_counts'])
            self.assertEqual(issue['severity'],
                             'NOTICE' if issue['code'] in NOTICE_CODES else 'FATAL')
        self.assertEqual(result['fatal_issue_count'], sum(
            count for code, count in result['issue_counts'].items() if code not in NOTICE_CODES))
        self.assertEqual(result['notice_count'], sum(
            count for code, count in result['issue_counts'].items() if code in NOTICE_CODES))
        for code in codes:
            self.assertIn(code, result['issue_counts'])
            self.assertGreater(result['issue_counts'][code], 0)
        if status == 'INCONSISTENT':
            self.assertGreater(result['fatal_issue_count'], 0)
        else:
            self.assertEqual(result['fatal_issue_count'], 0)
            self.assertFalse(set(result['issue_counts']) - NOTICE_CODES)
        if status == 'COMPLETE':
            self.assertTrue(INVARIANT_KEYS <= result['invariants'].keys())
            self.assertTrue(all(value is True for value in result['invariants'].values()))
            self.assertIs(result['snapshot_consistency']['verified'], True)
        return result

    def complete(self, **kwargs):
        return self.assert_status(audit_fixture(self.f, **kwargs), 'COMPLETE',
                                  'SOURCE_MANIFEST_ABSENT')

    def inconsistent(self, *codes, **kwargs):
        return self.assert_status(audit_fixture(self.f, **kwargs), 'INCONSISTENT', *codes)

    def test_two_segments_and_resident_suffix(self):
        self.f.archived()
        result = self.complete()
        metrics = result['metrics']
        for key in ('matched_effects', 'expected_events_total', 'consumer_effects'):
            self.assertEqual(metrics[key], 12)
        published = sum('outbox' in item for number in range(2)
                        for item in read_segment(self.f.segment_path(number)))
        self.assertEqual(metrics['archived_events'], published)
        self.assertEqual(metrics['archived_events_matched'], published)
        self.assertEqual(metrics['archived_segments'], 2)
        self.assertEqual(metrics['archived_records_parsed'], 8)
        self.assertEqual(metrics['producer_outbox'], 4)
        self.assertFalse(any(i['severity'] == 'FATAL' for i in result['issues']))

    def test_fully_archived_and_legacy_rejection(self):
        self.f.archived(limits=(6, 6))
        self.assertEqual(sql(self.f.db(), 'SELECT COUNT(*) AS n FROM outbox')[0]['n'], 0)
        self.complete()
        old = e2e.reconcile(self.f.root, self.f.consumer, report=False)
        self.assertEqual(old['status'], 'INCONSISTENT')
        self.assertIs(old['passed'], False)
        self.assertGreater(old['metrics']['archived_records_unsupported'], 0)

    def test_never_pruned_parity_for_pending_retry_and_complete(self):
        self.f.ingest(8)
        self.assertEqual(sql(self.f.db(),
                            "SELECT name FROM sqlite_master WHERE name='archive_segments'"), [])
        for status in ('PENDING_DELIVERY', 'RETRY_REQUIRED', 'COMPLETE'):
            with self.subTest(status=status):
                if status == 'RETRY_REQUIRED':
                    rows = self.f.deliver(ack=False)
                elif status == 'COMPLETE':
                    self.f.ack(rows)
                result = self.assert_status(audit_fixture(self.f), status,
                                            'SOURCE_MANIFEST_ABSENT')
                old = e2e.reconcile(self.f.root, self.f.consumer, report=False)
                self.assertEqual(result['status'], old['status'])
                self.assertIs(result['passed'], old['passed'])

    def test_empty_manifest_with_zero_cursor_is_legal(self):
        self.f.ingest(8)
        self.assertEqual(self.f.prune()['archived_new'], 0)
        self.assertEqual(self.f.segments(), [])
        # The real no-op prune leaves an absent cursor, which the contract treats
        # as zero. Also exercise an explicitly persisted zero.
        sql(self.f.db(), "INSERT OR REPLACE INTO meta(k,v) VALUES('archived_records','0')")
        self.assert_status(audit_fixture(self.f), 'PENDING_DELIVERY', 'SOURCE_MANIFEST_ABSENT')
        self.f.deliver()
        self.complete()

    def test_interleaved_market_sequences_across_archive_boundary(self):
        self.f.ingest(8, order=[1, 2, 3, 5, 4, 6, 7, 8])
        self.f.deliver()
        self.f.prune(4)
        archived = {item['outbox']['seq'] for item in read_segment(self.f.segment_path())}
        resident = {row['seq'] for row in sql(self.f.db(), 'SELECT seq FROM outbox')}
        self.assertEqual(archived, {1, 2, 3, 5})
        self.assertEqual(resident, {4, 6, 7, 8})
        result = self.complete()
        self.assertEqual(result['metrics']['orphan_consumer_effects'], 0)

    def test_archived_prefix_pending_suffix_then_resume_and_prune(self):
        self.f.ingest(12)
        self.f.deliver(limit=4)
        self.f.prune(4)
        result = self.assert_status(audit_fixture(self.f), 'PENDING_DELIVERY',
                                    'SOURCE_MANIFEST_ABSENT')
        self.assertEqual(result['metrics']['remote_missing'], 8)
        self.f.deliver()
        self.f.prune(5)
        self.complete()

    def test_archived_prefix_consumer_commit_without_ack_then_recover(self):
        self.f.ingest(12)
        self.f.deliver(limit=4)
        self.f.prune(4)
        rows = self.f.deliver(ack=False)
        result = self.assert_status(audit_fixture(self.f), 'RETRY_REQUIRED',
                                    'SOURCE_MANIFEST_ABSENT')
        self.assertEqual(result['metrics']['remote_committed_unacked'], 8)
        self.f.ack(rows)
        self.complete()

    def test_missing_segment(self):
        self.f.archived()
        self.f.segment_path().unlink()
        self.inconsistent('ARCHIVE_SEGMENT_MISSING')

    def test_missing_first_manifest_row(self):
        self.f.archived()
        sql(self.f.db(), 'DELETE FROM archive_segments WHERE start_file_index=0')
        self.inconsistent('ARCHIVE_SEGMENT_CONTINUITY')

    def test_cursor_integer_mismatch_and_invalid_text(self):
        self.f.archived()
        for value, code in [('9', 'ARCHIVE_CURSOR_MISMATCH'),
                            ('abc', 'ARCHIVE_CURSOR_INVALID')]:
            with self.subTest(value=value):
                sql(self.f.db(), "UPDATE meta SET v=? WHERE k='archived_records'", (value,))
                self.inconsistent(code)

    def test_missing_manifest_table_with_positive_cursor(self):
        self.f.archived()
        sql(self.f.db(), 'DROP TABLE archive_segments')
        self.inconsistent('ARCHIVE_MANIFEST_MISSING')

    def test_truncated_and_bit_flipped_compressed_bytes(self):
        self.f.archived()
        path = self.f.segment_path()
        original = path.read_bytes()
        flipped = bytearray(original)
        flipped[len(flipped) // 2] ^= 1
        for label, data in [('truncated', original[:-8]), ('flipped', bytes(flipped))]:
            with self.subTest(corruption=label):
                path.write_bytes(data)
                self.inconsistent('ARCHIVE_SHA256_MISMATCH')

    def test_garbage_with_matching_manifest_hash_is_reported(self):
        self.f.archived()
        self.f.segment_path().write_bytes(b'not a gzip stream\x00\xff')
        self.f.update_segment_hash()
        self.inconsistent('ARCHIVE_SEGMENT_UNREADABLE')

    def test_manifest_record_count_does_not_match_parsed_records(self):
        self.f.archived()
        self.f.rewrite_segment(lambda items: items.pop())
        self.inconsistent('ARCHIVE_MANIFEST_COUNT_MISMATCH')

    @staticmethod
    def change_payload(record):
        payload = json.loads(record['payload'])
        payload['price'] = '9999.75'
        record['payload'] = canonical(payload)
        record['raw_sha256'] = sha256(record['payload'].encode('utf-8'))

    def test_self_consistent_raw_forgery_requires_source_anchor(self):
        self.f.archived()
        self.f.rewrite_segment(lambda items: self.change_payload(items[0]))
        legacy = retention.audit(self.f.root, 'BTC', self.f.consumer)
        self.assertIs(legacy['archive_continuity_and_hashes'], True)
        self.inconsistent('SOURCE_ANCHOR_MISMATCH')

    def test_self_consistent_outbox_forgery_requires_raw_and_consumer_binding(self):
        self.f.archived()
        self.f.rewrite_segment(lambda items: self.change_payload(items[0]['outbox']))
        self.inconsistent('ARCHIVE_RAW_OUTBOX_BINDING_MISMATCH',
                          'CONSUMER_ARCHIVE_CONTENT_CONFLICT')

    def test_unknown_record_field(self):
        self.f.archived()
        self.f.rewrite_segment(lambda items: items[0].update(unrecognized=True))
        self.inconsistent('ARCHIVE_RECORD_UNRECOGNIZED')

    def test_noncanonical_record_line(self):
        self.f.archived()
        self.f.rewrite_segment(lambda items: None, canonical_lines=False)
        self.inconsistent('ARCHIVE_RECORD_UNRECOGNIZED')

    def test_archived_record_must_be_acknowledged(self):
        self.f.archived()
        self.f.rewrite_segment(lambda items: items[0]['outbox'].update(acknowledged=False))
        self.inconsistent('ARCHIVED_RECORD_NOT_ACKED')

    def test_unknown_manifest_column(self):
        self.f.archived()
        sql(self.f.db(), 'ALTER TABLE archive_segments ADD COLUMN surprise TEXT')
        self.inconsistent('ARCHIVE_MANIFEST_UNRECOGNIZED')

    def test_archived_consumer_effect_deleted(self):
        self.f.archived()
        sql(self.f.consumer, 'DELETE FROM effects WHERE seq=2')
        self.inconsistent('ARCHIVED_ACK_WITHOUT_CONSUMER_EFFECT')

    def test_archived_consumer_payload_tampered(self):
        self.f.archived()
        sql(self.f.consumer, "UPDATE effects SET payload='{}' WHERE seq=2")
        result = self.inconsistent('CONSUMER_EFFECT_TAMPERED')
        self.assertGreater(result['metrics']['invalid_consumer_rows'], 0)

    def test_extra_valid_consumer_effect_is_orphan(self):
        self.f.archived()
        item = read_segment(self.f.segment_path())[0]['outbox']
        payload = json.loads(item['payload'])
        payload['source_seq'] = 300
        digest = sha256(canonical(payload).encode())
        event = {'lane': item['lane'], 'seq': 300, 'epoch': 0, 'payload': payload,
                 'payload_sha256': digest,
                 'event_id': event_identity(item['lane'], 300, 0, digest)}
        with closing(EffectStore(self.f.consumer)) as store:
            store.accept([event])
        result = self.inconsistent('ORPHAN_CONSUMER_EFFECT')
        self.assertEqual(result['metrics']['orphan_consumer_effects'], 1)

    def test_forged_resident_ack_without_effect(self):
        self.f.ingest(12)
        self.f.deliver(limit=4)
        self.f.prune(4)
        sql(self.f.db(), 'UPDATE outbox SET delivered=1 WHERE seq=5')
        result = self.inconsistent()
        permitted = {'PRODUCER_ACK_WITHOUT_CONSUMER_EFFECT',
                     'PRODUCER_ACK_LEDGER_INCONSISTENT'}
        self.assertTrue(permitted & result['issue_counts'].keys(), result['issue_counts'])

    def test_duplicate_identity_in_different_segments(self):
        self.f.archived()
        first = read_segment(self.f.segment_path())[0]

        def duplicate(items):
            replacement = deepcopy(first)
            replacement['id'] = items[0]['id']
            replacement['file_index'] = items[0]['file_index']
            items[0] = replacement

        self.f.rewrite_segment(duplicate, number=1)
        self.inconsistent('PRODUCER_DUPLICATE_IDENTITY')

    def test_archived_quarantine_cannot_disappear_from_audit(self):
        # The invalid fourth/final line leaves no later sequence waiting for it.
        self.f.ingest(4, invalid_last=True)
        self.assertEqual(len(self.f.deliver()), 3)
        self.assertEqual(self.f.prune(4)['archived_new'], 4)
        self.assertIs(local_audit.audit(self.f.root)['passed'], True)
        self.inconsistent('ARCHIVED_QUARANTINE_PRESENT')

    def test_archive_and_source_resource_limits(self):
        from ftdi_core.archive_e2e import ArchiveLimits
        self.f.archived(n=18, limits=(15,))
        before = self.f.snapshot()
        cases = [
            ('max_compressed_bytes', 100, 'ARCHIVE_SEGMENT_TOO_LARGE'),
            ('max_line_bytes', 100, 'ARCHIVE_LINE_TOO_LONG'),
            ('max_records_per_segment', 5, 'ARCHIVE_TOO_MANY_RECORDS'),
            ('max_source_line_bytes', 50, 'SOURCE_LINE_TOO_LONG'),
        ]
        for name, value, code in cases:
            with self.subTest(limit=name):
                self.inconsistent(code, limits=ArchiveLimits(**{name: value}))
                self.assertEqual(before, self.f.snapshot())
        with self.assertRaises(ValueError):
            ArchiveLimits(max_line_bytes=0)

    def test_limits_are_frozen_and_all_fields_require_positive_ints(self):
        from ftdi_core.archive_e2e import ArchiveLimits
        defaults = ArchiveLimits()
        expected = {
            'max_compressed_bytes': 32 * 1024**2,
            'max_decompressed_bytes': 128 * 1024**2,
            'max_line_bytes': 1024**2,
            'max_records_per_segment': 20000,
            'max_source_line_bytes': 1024**2,
            'max_events_per_lane': 100000000,
            'max_metadata_bytes': 4 * 1024**2,
            'max_unreferenced_overlap_files': 16,
            'max_archive_dir_entries': 100000,
        }
        self.assertEqual({field.name: getattr(defaults, field.name)
                          for field in fields(defaults)}, expected)
        with self.assertRaises(FrozenInstanceError):
            defaults.max_line_bytes = 2
        for name in expected:
            for bad in (0, -1, True, False, 1.0, '1', None):
                with self.subTest(field=name, invalid=bad):
                    with self.assertRaises(ValueError):
                        ArchiveLimits(**{name: bad})

    def test_decompression_bomb_stays_below_eight_mib_peak(self):
        from ftdi_core.archive_e2e import ArchiveLimits
        self.f.archived()
        with self.f.segment_path().open('wb') as raw:
            with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as zipped:
                chunk = b'x' * (64 * 1024)
                for _ in range(512):
                    zipped.write(chunk)
        self.f.update_segment_hash()
        limits = ArchiveLimits(max_decompressed_bytes=1024**2, max_line_bytes=4 * 1024**2)
        tracemalloc.start()
        try:
            result = audit_fixture(self.f, limits=limits)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assert_status(result, 'INCONSISTENT', 'ARCHIVE_DECOMPRESSED_TOO_LARGE')
        self.assertLess(peak, 8 * 1024**2, f'audit allocated {peak} bytes at peak')

    def crash_prune(self, stage):
        process = subprocess.run(
            [sys.executable, '-W', 'error::ResourceWarning', '-m',
             'ftdi_core.adapters.s070041.retention', '--outdir', str(self.f.root),
             '--lane', 'BTC', '--mode', 'archive', '--limit', '8',
             '--consumer-db', str(self.f.consumer), '--crash-stage', stage],
            cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(process.returncode, 86, process.stderr)

    def test_crash_before_commit_orphan_overlap_and_conflicting_orphan(self):
        self.f.ingest(12)
        self.f.deliver()
        self.crash_prune('after_archive_before_commit')
        result = self.complete()
        self.assertIn('UNREFERENCED_ARCHIVE_FILE', result['issue_counts'])
        self.assertEqual(self.f.prune(5)['archived_new'], 5)
        result = self.complete()
        self.assertIn('UNREFERENCED_ARCHIVE_FILE', result['issue_counts'])
        orphan = self.f.root / 'archives' / 'BTC' / 'raw_000000000000_000000000008.jsonl.gz'
        records = read_segment(orphan)
        self.change_payload(records[2])
        self.change_payload(records[2]['outbox'])
        # An unregistered file has no manifest row to update.
        write_segment(orphan, records)
        self.inconsistent('ARCHIVE_UNREFERENCED_CONTENT_CONFLICT')

    def test_crash_after_commit_is_complete(self):
        self.f.ingest(12)
        self.f.deliver()
        self.crash_prune('after_commit')
        result = self.complete()
        self.assertEqual(result['metrics']['archived_records'], 8)

    def test_read_only_repeatable_report_and_no_new_sidecars(self):
        self.f.archived()
        report_dir = self.f.base / 'reports'
        report_dir.mkdir()
        report = report_dir / 'audit.json'
        before = self.f.snapshot()
        first = self.complete(report_path=report)
        self.assertEqual(json.loads(report.read_text(encoding='utf-8')), first)
        self.assertEqual(before, self.f.snapshot())
        second = self.complete(report_path=report)
        self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True))
        self.assertEqual(json.loads(report.read_text(encoding='utf-8')), second)
        self.assertEqual(before, self.f.snapshot())
        self.assertEqual([p.name for p in report_dir.iterdir()], ['audit.json'])

    def test_missing_consumer_is_not_created_and_no_report_written(self):
        from ftdi_core.archive_e2e import reconcile_archive_aware
        self.f.ingest(4)
        missing = self.f.consumers / 'missing.sqlite3'
        report = self.f.base / 'should-not-exist.json'
        before = self.f.snapshot()
        with self.assertRaises(FileNotFoundError):
            reconcile_archive_aware(self.f.root, missing, report_path=report)
        self.assertFalse(missing.exists())
        self.assertFalse(report.exists())
        self.assertEqual(before, self.f.snapshot())

    def test_report_path_cannot_overwrite_evidence(self):
        self.f.archived()
        before = self.f.snapshot()
        # Check every expressly protected evidence class as well as both trees.
        forbidden = [
            self.f.root / 'lanes' / 'report.json',
            self.f.root / 'archives' / 'report.json',
            self.f.consumer, self.f.db(), self.f.source,
            self.f.root / 'lane_registry.json',
            Path(str(self.f.consumer) + '-wal'),
            Path(str(self.f.consumer) + '-shm'),
            Path(str(self.f.consumer) + '-journal'),
            self.f.consumers,
        ]
        for path in forbidden:
            with self.subTest(report_path=path.name):
                with self.assertRaises(ValueError):
                    audit_fixture(self.f, report_path=path)
                self.assertEqual(before, self.f.snapshot())

    def test_archived_healthy_lane_isolated_from_blocked_lane(self):
        self.assertFalse(self.f.ingest(12, blocked_eth=True)['complete'])
        self.f.deliver('BTC')
        self.f.deliver('ETH')
        self.f.prune(8)
        result = self.inconsistent()
        btc = result['lanes']['BTC']
        self.assertEqual(btc['status'], 'COMPLETE')
        self.assertFalse(set(btc['issue_counts']) - NOTICE_CODES)
        gap_codes = {'SOURCE_NOT_EXHAUSTED', 'PENDING_GAP_UNRESOLVED',
                     'LANE_WAITING_FOR_MISSING'}
        eth_codes = result['lanes']['ETH']['issue_counts']
        self.assertTrue(gap_codes & eth_codes.keys(), eth_codes)
        for code in gap_codes & eth_codes.keys():
            self.assertIn(code, result['issue_counts'])

    def test_source_mutation_after_prune(self):
        self.f.archived()
        original = self.f.source.read_bytes()
        changed = original.replace(b'2500.25', b'2500.26', 1)
        self.assertNotEqual(original, changed)
        self.f.source.write_bytes(changed)
        self.inconsistent('SOURCE_FINGERPRINT_MISMATCH')

    def test_consumer_must_be_independent_of_producer(self):
        from ftdi_core.archive_e2e import reconcile_archive_aware
        self.f.archived()
        before = self.f.snapshot()
        with self.assertRaises(ValueError):
            reconcile_archive_aware(self.f.root, self.f.db())
        self.assertEqual(before, self.f.snapshot())

    def test_relocation_uses_only_canonical_files_in_copied_root(self):
        self.f.archived()
        original_root = self.f.root
        original_segment = self.f.segment_path()
        self.f.root = self.f.base / 'relocated'
        shutil.copytree(original_root, self.f.root)
        self.inconsistent('ARCHIVE_PATH_RELOCATED')
        result = self.complete(allow_relocated_archives=True)
        self.assertIn('ARCHIVE_PATH_RELOCATED_ALLOWED', result['issue_counts'])
        original_segment.write_bytes(b'the old path is not evidence for the copy')
        result = self.complete(allow_relocated_archives=True)
        self.assertIn('ARCHIVE_PATH_RELOCATED_ALLOWED', result['issue_counts'])
        self.f.segment_path().unlink()
        self.inconsistent('ARCHIVE_SEGMENT_MISSING', allow_relocated_archives=True)

    def test_stored_archive_basename_must_be_canonical(self):
        self.f.archived()
        path = self.f.segment_path().with_name('wrong.jsonl.gz')
        sql(self.f.db(), 'UPDATE archive_segments SET archive_path=? WHERE start_file_index=0',
            (str(path),))
        self.inconsistent('ARCHIVE_PATH_UNRECOGNIZED')

    def test_duplicate_stored_archive_path_is_conflict(self):
        self.f.archived()
        sql(self.f.db(), 'UPDATE archive_segments SET archive_path=?',
            (str(self.f.segment_path()),))
        self.inconsistent('ARCHIVE_PATH_CONFLICT')

    def test_registered_archive_tmp_sibling_is_conflict(self):
        self.f.archived()
        Path(str(self.f.segment_path()) + '.tmp').write_bytes(b'unfinished')
        self.inconsistent('ARCHIVE_PATH_CONFLICT')

    def test_registered_archive_replaced_by_directory_is_conflict(self):
        self.f.archived()
        path = self.f.segment_path()
        path.unlink()
        path.mkdir()
        self.inconsistent('ARCHIVE_PATH_CONFLICT')

    def test_prepared_source_provenance_and_manifest_tamper(self):
        archive = self.f.sources / 'BTCUSDT-aggTrades-2024-06-01.zip'
        rows = ''.join(f'{i},69000.12,0.001,100,101,1717200000000000,false,True\n'
                       for i in range(1, 7))
        with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as zipped:
            info = zipfile.ZipInfo('BTCUSDT-aggTrades-2024-06-01.csv',
                                   date_time=(2024, 6, 1, 0, 0, 0))
            zipped.writestr(info, rows)
        prepared = prepare_archive(archive, self.f.sources / 'prepared')
        self.f.source = Path(prepared['output'])
        self.f.replay()
        self.f.deliver()
        self.f.prune(4)
        result = self.assert_status(audit_fixture(self.f), 'COMPLETE')
        provenance = result['lanes']['BTC']['provenance']['source_manifest']
        self.assertIs(provenance['jsonl_sha256_matches_computed'], True)
        self.assertIs(provenance['archive_sha256_verified_by_this_audit'], False)
        manifest = Path(prepared['manifest'])
        data = json.loads(manifest.read_text(encoding='utf-8'))
        data['jsonl_sha256'] = '0' * 64
        manifest.write_text(json.dumps(data), encoding='utf-8')
        self.inconsistent('SOURCE_MANIFEST_MISMATCH')

    def test_synthetic_source_manifest_absent_is_notice(self):
        self.f.archived()
        result = self.complete()
        self.assertIsNone(result['lanes']['BTC']['provenance']['source_manifest'])
        notices = [issue for issue in result['issues'] if issue['code'] == 'SOURCE_MANIFEST_ABSENT']
        self.assertTrue(notices)
        self.assertTrue(all(issue['severity'] == 'NOTICE' for issue in notices))

    def test_open_consumer_wal_snapshot_is_verified(self):
        self.f.ingest(12)
        with closing(EffectStore(self.f.consumer)) as store:
            store.db.execute('PRAGMA wal_autocheckpoint=0')
            self.f.deliver(store=store)
            self.f.prune(8)
            wal = Path(str(self.f.consumer) + '-wal')
            self.assertTrue(wal.is_file())
            self.assertGreater(wal.stat().st_size, 32)
            before = self.f.snapshot()
            result = self.complete()
            self.assertIs(result['snapshot_consistency']['verified'], True)
            self.assertEqual(before, self.f.snapshot())

    def test_windows_handles_released_after_success_failure_and_exception(self):
        from ftdi_core.archive_e2e import reconcile_archive_aware
        self.f.archived()
        for scenario in ('complete', 'inconsistent', 'exception'):
            with self.subTest(scenario=scenario):
                copied = self.f.base / ('copy_' + scenario)
                consumer = self.f.base / ('consumer_' + scenario + '.sqlite3')
                shutil.copytree(self.f.root, copied)
                shutil.copy2(self.f.consumer, consumer)
                if scenario == 'exception':
                    consumer.write_bytes(b'not a SQLite database')
                    with self.assertRaises(sqlite3.DatabaseError):
                        reconcile_archive_aware(copied, consumer, allow_relocated_archives=True)
                else:
                    if scenario == 'inconsistent':
                        sql(consumer, 'DELETE FROM effects WHERE seq=1')
                    result = reconcile_archive_aware(copied, consumer, allow_relocated_archives=True)
                    self.assert_status(result,
                                       'COMPLETE' if scenario == 'complete' else 'INCONSISTENT',
                                       'ARCHIVE_PATH_RELOCATED_ALLOWED' if scenario == 'complete'
                                       else 'ARCHIVED_ACK_WITHOUT_CONSUMER_EFFECT')
                # No gc.collect(), retries, sleeps, or delayed teardown to hide leaks.
                shutil.rmtree(copied)
                consumer.unlink()
                self.assertFalse(copied.exists())
                self.assertFalse(consumer.exists())

    def cli(self, command='audit-e2e-archive', *, consumer=None, report=None):
        args = [sys.executable, '-W', 'error::ResourceWarning', '-m', 'ftdi_core', command,
                '--outdir', str(self.f.root), '--consumer-db', str(consumer or self.f.consumer)]
        if report is not None:
            args.extend(['--report', str(report)])
        return subprocess.run(args, cwd=PROJECT_ROOT, capture_output=True,
                              text=True, encoding='utf-8', timeout=30)

    def test_cli_complete_report_legacy_tamper_and_missing_consumer(self):
        self.f.archived()
        result = self.cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_status(json.loads(result.stdout), 'COMPLETE', 'SOURCE_MANIFEST_ABSENT')
        self.assertEqual(list(self.f.root.rglob('END_TO_END_ARCHIVE_AUDIT.json')), [])
        report_dir = self.f.base / 'cli_reports'
        report_dir.mkdir()
        report = report_dir / 'audit.json'
        result = self.cli(report=report)
        self.assertEqual(result.returncode, 0, result.stderr)
        parsed = json.loads(result.stdout)
        self.assert_status(parsed, 'COMPLETE', 'SOURCE_MANIFEST_ABSENT')
        self.assertEqual(json.loads(report.read_text(encoding='utf-8')), parsed)
        old = self.cli('audit-e2e')
        self.assertEqual(old.returncode, 2, old.stderr)
        self.assertEqual(json.loads(old.stdout)['status'], 'INCONSISTENT')
        self.assertIs(json.loads(old.stdout)['passed'], False)
        path = self.f.segment_path()
        path.write_bytes(path.read_bytes()[:-8])
        bad = self.cli()
        self.assertEqual(bad.returncode, 2, bad.stderr)
        self.assert_status(json.loads(bad.stdout), 'INCONSISTENT', 'ARCHIVE_SHA256_MISMATCH')
        missing = self.f.consumers / 'absent.sqlite3'
        absent_report = report_dir / 'absent.json'
        absent = self.cli(consumer=missing, report=absent_report)
        self.assertEqual(absent.returncode, 3, absent.stderr)
        self.assertIn('FTDI_CORE_FAIL_CLOSED:', absent.stderr)
        self.assertFalse(missing.exists())
        self.assertFalse(absent_report.exists())
        self.assertEqual(list(self.f.root.rglob('END_TO_END_ARCHIVE_AUDIT.json')), [])

    def test_one_real_localhost_http_delivery_then_archive(self):
        self.f.ingest(12)
        self.assertIs(local_lab(self.f.root, self.f.consumer, batch=4)['pass'], True)
        self.f.prune(8)
        self.complete()


if __name__ == '__main__':
    unittest.main()

