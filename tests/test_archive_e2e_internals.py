"""Implementation-aware tests for ftdi_core.archive_e2e (complements the black-box
contract suite in test_archive_e2e.py, whose fixtures are reused here).

These cover what a black-box suite cannot inject deterministically: evidence that
changes *while* the audit runs, fingerprints that cannot be taken, and hostile
inputs aimed at specific parsing stages.
"""
from __future__ import annotations

from contextlib import closing
import gzip
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock

from ftdi_core import archive_e2e
from ftdi_core.archive_e2e import ArchiveLimits, reconcile_archive_aware
from ftdi_core.adapters.s070041 import multi_lane
from ftdi_core.adapters.s070041.durable_stream import canonical
from ftdi_core.adapters.s070041.external_delivery import EffectStore

from test_archive_e2e import Fixture, NOTICE_CODES, read_segment, sha256, sql, write_segment


class InternalsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='archive_e2e_internal_')
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def audit(self, **kwargs):
        return reconcile_archive_aware(self.f.root, self.f.consumer, **kwargs)

    def assert_inconsistent(self, result, *codes):
        self.assertEqual(result['status'], 'INCONSISTENT', result['issue_counts'])
        self.assertIs(result['passed'], False)
        for code in codes:
            self.assertIn(code, result['issue_counts'])
        return result

    def assert_complete(self, result):
        self.assertEqual(result['status'], 'COMPLETE', result['issue_counts'])
        self.assertFalse(set(result['issue_counts']) - NOTICE_CODES)
        self.assertTrue(all(result['invariants'].values()), result['invariants'])
        return result

    def during_audit(self, action):
        """Run ``action`` after all lanes were read but before the consumer pass."""
        original = archive_e2e._Auditor._consumer_pass

        def wrapped(auditor):
            action()
            return original(auditor)
        return mock.patch.object(archive_e2e._Auditor, '_consumer_pass', wrapped)

    # -- snapshot consistency -------------------------------------------
    def test_quiescent_databases_are_opened_immutable(self):
        self.f.archived()
        result = self.assert_complete(self.audit())
        snapshot = result['snapshot_consistency']
        self.assertEqual(sorted(snapshot['immutable_databases']), ['consumer', 'lane:BTC'])
        self.assertEqual(snapshot['wal_databases'], [])

    def test_concurrent_consumer_write_blocks_complete(self):
        self.f.archived()

        def write():  # any committed write, even to an unrelated table, invalidates the snapshot
            with closing(sqlite3.connect(self.f.consumer)) as db, db:
                db.execute('CREATE TABLE IF NOT EXISTS noise(x)')
                db.execute('INSERT INTO noise VALUES (1)')
        with self.during_audit(write):
            result = self.audit()
        self.assert_inconsistent(result, 'CONCURRENT_MODIFICATION_DETECTED')
        self.assertIs(result['snapshot_consistency']['verified'], False)
        self.assertIs(result['invariants']['snapshot_consistent'], False)

    def test_concurrent_producer_write_blocks_complete(self):
        self.f.archived()

        def write():
            # A no-op UPDATE (v=v) leaves every byte unchanged in SQLite and is rightly
            # not a modification. source_filename is not validated by lane checks, so
            # only snapshot fingerprinting can catch this real change.
            sql(self.f.db(), "UPDATE meta SET v=v || '.changed' WHERE k='source_filename'")
        with self.during_audit(write):
            self.assert_inconsistent(self.audit(), 'CONCURRENT_MODIFICATION_DETECTED')

    def test_source_touched_without_content_change_blocks_complete(self):
        self.f.archived()

        def touch():
            info = os.stat(self.f.source)
            os.utime(self.f.source, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
        with self.during_audit(touch):
            self.assert_inconsistent(self.audit(), 'CONCURRENT_MODIFICATION_DETECTED')

    def test_new_archive_file_during_audit_blocks_complete(self):
        self.f.archived()

        def drop():
            (self.f.root / 'archives' / 'BTC' / 'raw_000000000100_000000000101.jsonl.gz').write_bytes(b'x')
        with self.during_audit(drop):
            self.assert_inconsistent(self.audit(), 'CONCURRENT_MODIFICATION_DETECTED')

    def test_wal_reader_detects_commit_through_data_version(self):
        self.f.ingest(12)
        with closing(EffectStore(self.f.consumer)) as store:
            store.db.execute('PRAGMA wal_autocheckpoint=0')
            self.f.deliver(store=store)
            self.f.prune(8)

            def write():
                store.db.execute('CREATE TABLE IF NOT EXISTS noise(x)')
                store.db.commit()
            with self.during_audit(write):
                result = self.audit()
        self.assert_inconsistent(result, 'CONCURRENT_MODIFICATION_DETECTED')
        self.assertIn('consumer', result['snapshot_consistency']['wal_databases'])
        methods = {i.get('method') for i in result['issues'] if i['code'] == 'CONCURRENT_MODIFICATION_DETECTED'}
        self.assertIn('data_version', methods)

    def test_unreadable_fingerprint_is_snapshot_unverifiable(self):
        self.f.archived()
        original = archive_e2e._fingerprint
        calls = {'n': 0}
        consumer = str(self.f.consumer.resolve())

        def flaky(path, content):
            if str(path) == consumer:
                calls['n'] += 1
                if calls['n'] > 1:
                    return ['UNREADABLE', 'PermissionError']
            return original(path, content)
        with mock.patch.object(archive_e2e, '_fingerprint', flaky):
            result = self.audit()
        self.assert_inconsistent(result, 'SNAPSHOT_UNVERIFIABLE')

    def test_exception_mid_audit_releases_every_handle(self):
        self.f.archived()
        with mock.patch.object(archive_e2e._Auditor, '_consumer_pass', side_effect=RuntimeError('boom')):
            with self.assertRaises(RuntimeError):
                self.audit()
        shutil.rmtree(self.f.root)
        self.f.consumer.unlink()
        self.f.source.unlink()

    # -- identity domains / sequences -----------------------------------
    def test_raw_id_must_increase_with_file_index(self):
        self.f.archived()

        def swap(items):
            items[0]['id'], items[1]['id'] = items[1]['id'], items[0]['id']
        self.f.rewrite_segment(swap)
        self.assert_inconsistent(self.audit(), 'RAW_ID_ORDER_VIOLATION')

    def test_out_of_order_market_seq_does_not_trip_raw_id_check(self):
        self.f.ingest(8, order=[1, 2, 3, 5, 4, 6, 7, 8])
        self.f.deliver()
        self.f.prune(4)
        result = self.assert_complete(self.audit())
        self.assertNotIn('RAW_ID_ORDER_VIOLATION', result['issue_counts'])
        self.assertEqual(result['lanes']['BTC']['first_seq'], 1)
        self.assertEqual(result['lanes']['BTC']['next_seq'], 9)

    def test_lane_next_seq_tampering_is_gap_or_out_of_range(self):
        self.f.archived()
        sql(self.f.db(), 'UPDATE lanes SET next_seq = next_seq + 1')
        self.assert_inconsistent(self.audit(), 'PRODUCER_SEQUENCE_GAP')
        sql(self.f.db(), 'UPDATE lanes SET next_seq = next_seq - 2')
        self.assert_inconsistent(self.audit(), 'PRODUCER_SEQUENCE_OUT_OF_RANGE')

    def test_resident_journal_tampering(self):
        self.f.archived()
        sql(self.f.db(), "UPDATE raw_journal SET payload = payload || ' ' WHERE file_index = 9")
        self.assert_inconsistent(self.audit(), 'RESIDENT_JOURNAL_TAMPERED')

    def test_resident_self_consistent_forgery_needs_source_and_binding(self):
        self.f.archived()
        row = sql(self.f.db(), 'SELECT payload FROM raw_journal WHERE file_index = 9')[0]
        event = json.loads(row['payload'])
        event['qty'] = '7.5'
        forged = json.dumps(event, separators=(',', ':'))
        sql(self.f.db(), 'UPDATE raw_journal SET payload=?, raw_sha256=? WHERE file_index = 9',
            (forged, sha256(forged.encode())))
        self.assert_inconsistent(self.audit(), 'SOURCE_ANCHOR_MISMATCH', 'RESIDENT_RAW_OUTBOX_BINDING_MISMATCH')

    def test_consumer_without_unique_identity_index_is_unrecognized(self):
        self.f.archived()
        rows = sql(self.f.consumer, 'SELECT * FROM effects')
        weak = self.f.consumers / 'weak.sqlite3'
        with closing(sqlite3.connect(weak)) as db, db:
            db.execute('CREATE TABLE effects(event_id TEXT, lane TEXT, seq INTEGER, epoch INTEGER, '
                       'payload_sha256 TEXT, payload TEXT)')
            db.executemany('INSERT INTO effects VALUES (:event_id,:lane,:seq,:epoch,:payload_sha256,:payload)', rows)
        result = reconcile_archive_aware(self.f.root, weak)
        self.assert_inconsistent(result, 'CONSUMER_SCHEMA_UNRECOGNIZED')
        self.assertIs(result['invariants']['consumer_schema_recognized'], False)

    # -- legitimate source formats ---------------------------------------
    def _rewrite_source(self, transform):
        multi_lane.make_synthetic(self.f.source, 10, None, 'BTCUSDT')
        self.f.source.write_bytes(transform(self.f.source.read_bytes()))
        self.f.replay()
        self.f.deliver()
        self.f.prune(6)

    def test_crlf_source_is_legitimate(self):
        self._rewrite_source(lambda data: data.replace(b'\n', b'\r\n'))
        self.assert_complete(self.audit())

    def test_source_without_final_newline_is_legitimate(self):
        self._rewrite_source(lambda data: data.rstrip(b'\n'))
        self.assert_complete(self.audit())

    # -- hostile archive bytes -------------------------------------------
    def _replace_segment_bytes(self, data, number=0):
        self.f.segment_path(number).write_bytes(data)
        self.f.update_segment_hash(number)

    def test_deeply_nested_record_is_a_finding_not_a_crash(self):
        self.f.archived()
        line = b'[' * 200_000 + b']' * 200_000
        self._replace_segment_bytes(gzip.compress(line + b'\n', mtime=0))
        self.assert_inconsistent(self.audit(), 'ARCHIVE_RECORD_UNRECOGNIZED')

    def test_second_gzip_member_and_trailing_bytes_are_unreadable(self):
        self.f.archived()
        original = self.f.segment_path().read_bytes()
        for tail in (gzip.compress(b'{}\n', mtime=0), b'\x00garbage'):
            with self.subTest(tail=tail[:4]):
                self._replace_segment_bytes(original + tail)
                self.assert_inconsistent(self.audit(), 'ARCHIVE_SEGMENT_UNREADABLE')

    def test_non_utf8_record_is_unrecognized(self):
        self.f.archived()
        self._replace_segment_bytes(gzip.compress(b'\xff\xfe\n', mtime=0))
        self.assert_inconsistent(self.audit(), 'ARCHIVE_RECORD_UNRECOGNIZED')

    def test_index_gap_inside_segment(self):
        self.f.archived()

        def renumber(items):
            items[1]['file_index'] += 1
        self.f.rewrite_segment(renumber)
        self.assert_inconsistent(self.audit(), 'ARCHIVE_INDEX_GAP')

    def test_outbox_presence_must_match_outcome(self):
        self.f.archived()
        self.f.rewrite_segment(lambda items: items[0].update(outcome='DUPLICATE_IDENTICAL'))
        self.assert_inconsistent(self.audit(), 'ARCHIVE_RECORD_UNRECOGNIZED')

    def test_unreadable_segment_effects_are_unverifiable_not_orphans(self):
        self.f.archived()
        path = self.f.segment_path()
        data = bytearray(path.read_bytes())
        data[len(data) // 2] ^= 1
        path.write_bytes(bytes(data))
        result = self.assert_inconsistent(self.audit(), 'ARCHIVE_SHA256_MISMATCH')
        self.assertNotIn('ORPHAN_CONSUMER_EFFECT', result['issue_counts'])
        self.assertEqual(result['metrics']['orphan_consumer_effects'], 0)
        self.assertNotIn('PRODUCER_SEQUENCE_GAP', result['issue_counts'])  # cascade, not a real gap
        self.assertEqual(result['metrics']['unverifiable_lane_effects'], 12)
        self.assertIs(result['lanes']['BTC']['verifiable'], False)

    def test_too_many_overlapping_unregistered_files(self):
        self.f.archived()
        directory = self.f.root / 'archives' / 'BTC'
        for name in ('raw_000000000000_000000000003.jsonl.gz', 'raw_000000000001_000000000002.jsonl.gz'):
            (directory / name).write_bytes(self.f.segment_path().read_bytes())
        result = self.audit(limits=ArchiveLimits(max_unreferenced_overlap_files=1))
        self.assert_inconsistent(result, 'ARCHIVE_UNREFERENCED_CONTENT_CONFLICT')

    def test_archive_directory_must_be_a_directory(self):
        self.f.archived()
        directory = self.f.root / 'archives' / 'BTC'
        moved = self.f.base / 'moved_archives'
        shutil.move(str(directory), str(moved))
        directory.write_bytes(b'not a directory')
        self.assert_inconsistent(self.audit(), 'ARCHIVE_PATH_CONFLICT')

    def test_report_replaces_existing_file_atomically(self):
        self.f.archived()
        reports = self.f.base / 'reports'
        reports.mkdir()
        target = reports / 'r.json'
        target.write_text('stale', encoding='utf-8')
        result = self.audit(report_path=target)
        self.assertEqual(json.loads(target.read_text(encoding='utf-8')), result)
        self.assertEqual(sorted(p.name for p in reports.iterdir()), ['r.json'])


if __name__ == '__main__':
    unittest.main()
