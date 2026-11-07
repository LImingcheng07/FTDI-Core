"""Regression tests for defects found by the independent adversarial review (Codex, 2026-11-07).

C1-C3 and C6 were confirmed against the earlier implementation with proof-of-concept
scripts; each test reproduces the scenario and pins the fail-closed outcome.  Fixtures are
reused from the black-box contract suite (test_archive_e2e.py).
"""
from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import zipfile

from ftdi_core import archive_e2e
from ftdi_core.archive_e2e import ArchiveLimits, reconcile_archive_aware
from ftdi_core.adapters.s070041 import multi_lane
from ftdi_core.adapters.s070041.external_delivery import EffectStore
from ftdi_core.preparation import prepare_archive

from test_archive_e2e import Fixture, NOTICE_CODES, PROJECT_ROOT, sql


class ReviewRegressionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='archive_e2e_review_')
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
        """Run ``action`` after every lane has been read, before the consumer pass."""
        original = archive_e2e._Auditor._consumer_pass

        def wrapped(auditor):
            action()
            return original(auditor)
        return mock.patch.object(archive_e2e._Auditor, '_consumer_pass', wrapped)

    # C1: DUPLICATE_IDENTICAL was trusted without evidence -------------------------------
    def test_c1_archived_quarantine_relabelled_as_duplicate(self):
        self.f.ingest(4, invalid_last=True)
        self.f.deliver()
        self.assertEqual(self.f.prune(4)['archived_new'], 4)

        def relabel(items):
            for item in items:
                if item['outcome'] == 'QUARANTINED_INVALID':
                    item['outcome'] = 'DUPLICATE_IDENTICAL'
        items = self.f.rewrite_segment(relabel)
        self.assertIn('DUPLICATE_IDENTICAL', {item['outcome'] for item in items})
        result = self.assert_inconsistent(self.audit(), 'DISPOSITION_CLAIM_UNVERIFIED')
        self.assertNotIn('ARCHIVED_QUARANTINE_PRESENT', result['issue_counts'])

    def test_c1_resident_quarantine_relabelled_as_duplicate(self):
        self.f.ingest(4, invalid_last=True)
        self.f.deliver()
        sql(self.f.db(), "UPDATE disposition SET outcome='DUPLICATE_IDENTICAL' "
                         "WHERE outcome='QUARANTINED_INVALID'")
        self.assert_inconsistent(self.audit(), 'DISPOSITION_CLAIM_UNVERIFIED')

    def test_c1_first_event_hidden_behind_duplicate_label(self):
        self.f.archived()

        def hide_first(items):
            items[0]['outcome'] = 'DUPLICATE_IDENTICAL'
            del items[0]['outbox']
        self.f.rewrite_segment(hide_first)
        sql(self.f.db(), 'UPDATE archive_segments SET outbox_count = outbox_count - 1 '
                         'WHERE start_file_index = 0')
        sql(self.f.consumer, 'DELETE FROM effects WHERE seq = 1')
        result = self.assert_inconsistent(self.audit(), 'DISPOSITION_CLAIM_UNVERIFIED')
        self.assertEqual(result['metrics']['consumer_effects'], 11)

    def test_c1_legitimate_duplicate_is_complete_resident_then_archived(self):
        multi_lane.make_synthetic(self.f.source, 10, None, 'BTCUSDT')
        lines = self.f.source.read_bytes().splitlines(keepends=True)
        self.f.source.write_bytes(b''.join(lines[:5] + [lines[2]] + lines[5:]))  # seq 3 repeated at index 5
        self.f.replay()
        self.f.deliver()
        self.f.prune(4)  # original seq 3 archived; duplicate (file_index 5) still resident
        first = self.assert_complete(self.audit())
        self.assertEqual(first['lanes']['BTC']['dispositions']['resident'].get('DUPLICATE_IDENTICAL'), 1)
        self.f.prune(4)  # the duplicate is archived too
        second = self.assert_complete(self.audit())
        self.assertEqual(second['lanes']['BTC']['dispositions']['archived'].get('DUPLICATE_IDENTICAL'), 1)
        self.assertEqual(second['metrics']['source_lines_anchored'], 11)
        self.assertEqual(second['metrics']['matched_effects'], 10)

    # C2: same-size rewrite after reading with the mtime restored -------------------------
    def _rewrite_keep_mtime(self, path):
        info = os.stat(path)
        data = bytearray(path.read_bytes())
        data[len(data) // 2] ^= 1
        path.write_bytes(bytes(data))
        os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))

    def test_c2_archive_rewritten_after_read_with_restored_mtime(self):
        self.f.archived()
        with self.during_audit(lambda: self._rewrite_keep_mtime(self.f.segment_path())):
            result = self.audit()
        self.assert_inconsistent(result, 'CONCURRENT_MODIFICATION_DETECTED')
        self.assertTrue(any(issue.get('method') == 'content' for issue in result['issues']))

    def test_c2_source_rewritten_after_read_with_restored_mtime(self):
        self.f.archived()
        with self.during_audit(lambda: self._rewrite_keep_mtime(self.f.source)):
            result = self.audit()
        self.assert_inconsistent(result, 'CONCURRENT_MODIFICATION_DETECTED')
        self.assertIs(result['snapshot_consistency']['verified'], False)

    # C3 + C7: links/junctions -----------------------------------------------------------
    def _link_directory(self, link, target):
        if os.name == 'nt':
            done = subprocess.run(['cmd', '/c', 'mklink', '/J', str(link), str(target)],
                                  capture_output=True, text=True)
            if done.returncode != 0:
                self.skipTest(f'cannot create junction: {done.stderr.strip()}')
        else:
            try:
                os.symlink(target, link, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f'cannot create symlink: {exc}')

    def test_c3_report_cannot_overwrite_archive_through_junction_and_c7_no_outside_listing(self):
        self.f.archived()
        external = self.f.base / 'external_archives'
        shutil.move(str(self.f.root / 'archives'), str(external))
        self._link_directory(self.f.root / 'archives', external)
        self.addCleanup(lambda: os.path.lexists(self.f.root / 'archives') and os.rmdir(self.f.root / 'archives'))
        segment_real = sorted((external / 'BTC').iterdir())[0]
        before = segment_real.read_bytes()
        for target in (segment_real, self.f.root / 'archives' / 'BTC' / 'report.json', external / 'report.json'):
            with self.subTest(report_path=str(target)):
                with self.assertRaises(ValueError):
                    self.audit(report_path=target)
        self.assertEqual(segment_real.read_bytes(), before)
        self.assertFalse((external / 'report.json').exists())
        listed = []
        real_scandir = os.scandir

        def recording_scandir(path='.'):
            listed.append(os.path.normcase(os.path.realpath(path)))
            return real_scandir(path)
        with mock.patch.object(archive_e2e.os, 'scandir', recording_scandir):
            result = self.audit()
        self.assert_inconsistent(result, 'ARCHIVE_PATH_CONFLICT')
        outside = os.path.normcase(os.path.realpath(external))
        self.assertFalse([path for path in listed if path.startswith(outside)], listed)

    # C4: views in place of evidence tables ------------------------------------------------
    def test_c4_recursive_view_instead_of_table_is_rejected_quickly(self):
        self.f.archived()
        sql(self.f.db(), 'DROP TABLE raw_journal')
        sql(self.f.db(), "CREATE VIEW raw_journal AS WITH RECURSIVE endless(n) AS "
                         "(SELECT 1 UNION ALL SELECT n + 1 FROM endless) SELECT n AS id, n AS file_index, "
                         "'' AS raw_sha256, '' AS payload, 0 AS processed, 0 AS received_ns FROM endless")
        script = ('import json, sys\n'
                  'from ftdi_core.archive_e2e import reconcile_archive_aware\n'
                  'r = reconcile_archive_aware(sys.argv[1], sys.argv[2])\n'
                  'print(json.dumps({"status": r["status"], "codes": sorted(r["issue_counts"])}))\n')
        # Subprocess + timeout: a regression must fail this test, not hang the whole suite.
        done = subprocess.run([sys.executable, '-c', script, str(self.f.root), str(self.f.consumer)],
                              cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        outcome = json.loads(done.stdout)
        self.assertEqual(outcome['status'], 'INCONSISTENT')
        self.assertIn('PRODUCER_SCHEMA_UNRECOGNIZED', outcome['codes'])

    def test_c4_view_instead_of_consumer_table_is_rejected(self):
        self.f.archived()
        rows = sql(self.f.consumer, 'SELECT * FROM effects')
        fake = self.f.consumers / 'view.sqlite3'
        with closing(sqlite3.connect(fake)) as db, db:
            db.execute('CREATE TABLE real_effects(event_id TEXT PRIMARY KEY, lane TEXT, seq INTEGER, '
                       'epoch INTEGER, payload_sha256 TEXT, payload TEXT, UNIQUE(lane, seq, epoch))')
            db.executemany('INSERT INTO real_effects VALUES '
                           '(:event_id, :lane, :seq, :epoch, :payload_sha256, :payload)', rows)
            db.execute('CREATE VIEW effects AS SELECT * FROM real_effects')
        self.assert_inconsistent(reconcile_archive_aware(self.f.root, fake), 'CONSUMER_SCHEMA_UNRECOGNIZED')

    # C5: integers beyond Python's str->int digit limit -----------------------------------
    def test_c5_oversized_meta_integers_are_findings_not_exceptions(self):
        self.f.archived()
        huge = '9' * 5000
        sql(self.f.db(), "UPDATE meta SET v=? WHERE k='archived_records'", (huge,))
        self.assert_inconsistent(self.audit(), 'ARCHIVE_CURSOR_INVALID')
        sql(self.f.db(), "UPDATE meta SET v=? WHERE k='next_file_index'", (huge,))
        self.assert_inconsistent(self.audit(), 'PRODUCER_META_INVALID')

    # C6: never create SQLite sidecars ------------------------------------------------------
    def test_c6_wal_without_shm_is_refused_without_creating_files(self):
        self.f.ingest(12)
        copies = self.f.base / 'copied_consumer'
        copies.mkdir()
        with closing(EffectStore(self.f.consumer)) as store:
            store.db.execute('PRAGMA wal_autocheckpoint=0')
            self.f.deliver(store=store)
            self.f.prune(8)
            for suffix in ('', '-wal'):
                shutil.copyfile(f'{self.f.consumer}{suffix}', copies / f'effects.sqlite3{suffix}')
        self.assertGreater((copies / 'effects.sqlite3-wal').stat().st_size, 0)
        before = sorted(path.name for path in copies.iterdir())
        result = reconcile_archive_aware(self.f.root, copies / 'effects.sqlite3')
        self.assert_inconsistent(result, 'SNAPSHOT_UNVERIFIABLE')
        self.assertEqual(sorted(path.name for path in copies.iterdir()), before)

    def test_c6_rollback_journal_is_refused_without_touching_files(self):
        self.f.archived()
        Path(f'{self.f.consumer}-journal').write_bytes(b'')
        before = sorted(path.name for path in self.f.consumers.iterdir())
        self.assert_inconsistent(self.audit(), 'SNAPSHOT_UNVERIFIABLE')
        self.assertEqual(sorted(path.name for path in self.f.consumers.iterdir()), before)

    # C8: report size bounded by known outcomes -------------------------------------------
    def test_c8_unknown_disposition_labels_are_aggregated(self):
        self.f.archived()
        with closing(sqlite3.connect(self.f.db())) as db, db:
            db.executemany('INSERT INTO disposition VALUES (?, ?, NULL)',
                           ((100000 + i, f'UNKNOWN_{i}') for i in range(3000)))
        result = self.assert_inconsistent(self.audit(), 'PRODUCER_DISPOSITION_UNRECOGNIZED')
        resident = result['lanes']['BTC']['dispositions']['resident']
        self.assertEqual(resident['UNRECOGNIZED'], 3000)
        self.assertLessEqual(len(resident), len(archive_e2e.KNOWN_OUTCOMES) + 1)
        self.assertEqual(result['issue_counts']['PRODUCER_DISPOSITION_UNRECOGNIZED'], 1)

    def test_archive_directory_entry_limit(self):
        self.f.archived()
        (self.f.root / 'archives' / 'BTC' / 'notes.txt').write_text('x', encoding='utf-8')
        result = self.audit(limits=ArchiveLimits(max_archive_dir_entries=2))
        self.assert_inconsistent(result, 'ARCHIVE_PATH_CONFLICT')

    # Found on the full-day real-data run: cascade findings must not misdirect diagnosis ----
    def test_hidden_middle_event_reports_gap_but_not_cascading_manifest_mismatch(self):
        archive = self.f.sources / 'BTCUSDT-aggTrades-2024-06-01.zip'
        rows = ''.join(f'{i},69000.12,0.001,100,101,1717200000000000,false,True\n' for i in range(1, 9))
        with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as zipped:
            zipped.writestr(zipfile.ZipInfo('BTCUSDT-aggTrades-2024-06-01.csv', date_time=(2024, 6, 1, 0, 0, 0)), rows)
        self.f.source = Path(prepare_archive(archive, self.f.sources / 'prepared', limit=0)['output'])
        self.f.replay()
        self.f.deliver()
        self.f.prune(6)

        def hide(items):
            items[2]['outcome'] = 'DUPLICATE_IDENTICAL'
            del items[2]['outbox']
        self.f.rewrite_segment(hide)
        sql(self.f.db(), 'UPDATE archive_segments SET outbox_count = outbox_count - 1')
        sql(self.f.consumer, 'DELETE FROM effects WHERE seq = 3')
        result = self.assert_inconsistent(self.audit(), 'DISPOSITION_CLAIM_UNVERIFIED', 'PRODUCER_SEQUENCE_GAP')
        self.assertNotIn('SOURCE_MANIFEST_MISMATCH', result['issue_counts'])
        self.assertIsNone(result['lanes']['BTC']['first_seq'])


if __name__ == '__main__':
    unittest.main()
