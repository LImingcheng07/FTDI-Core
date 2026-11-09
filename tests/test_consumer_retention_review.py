"""Regressions for the six findings of the independent correctness review of consumer retention v2
(Codex, 2026-11-07). Each scenario was confirmed against the earlier implementation."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

import test_consumer_retention as contract   # module import: no duplicate test discovery
from test_archive_e2e import Fixture, PROJECT_ROOT, sql
from ftdi_core import consumer_retention as cr
from ftdi_core.adapters.s070041 import multi_lane
from ftdi_core.adapters.s070041.durable_stream import canonical
from ftdi_core.adapters.s070041.external_delivery import envelope, event_identity
from ftdi_core.archive_e2e import ArchiveLimits, reconcile_archive_aware



def _remove_directory_link(link):
    """Remove a directory junction (Windows: rmdir) or symlink (POSIX: unlink) without touching its target."""
    if os.path.islink(link):
        os.unlink(link)
    elif os.path.lexists(link):
        os.rmdir(link)

class RetentionReviewRegressionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='retention_review_')
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def prepared(self, n=6):
        self.f.ingest(n)
        rows = self.f.deliver()
        cr.migrate_consumer_v2(self.f.consumer, apply=True)
        return rows[0]['lane'], [envelope(r) for r in rows]

    def compact(self, lane, watermark, size=3, **kwargs):
        return cr.compact_consumer(self.f.consumer, lane=lane, content_watermark=watermark,
                                   ignore_producer_window=True, segment_size=size, apply=True, **kwargs)

    def counts(self):
        return {'hot': sql(self.f.consumer, 'SELECT COUNT(*) n FROM effects')[0]['n'],
                'digests': sql(self.f.consumer, 'SELECT COUNT(*) n FROM effect_digests')[0]['n'],
                'segments': sql(self.f.consumer, 'SELECT COUNT(*) n FROM effect_segments')[0]['n']}

    def oversize(self, lane, seq):
        """Rewrite one valid effect so that its archive line exceeds the 1 MiB audit line limit."""
        row = sql(self.f.consumer, 'SELECT payload FROM effects WHERE seq = ?', (seq,))[0]
        payload = json.loads(row['payload'])
        payload['padding'] = 'a' * (1024 * 1024)
        text = canonical(payload)
        digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
        sql(self.f.consumer, 'UPDATE effects SET payload = ?, payload_sha256 = ?, event_id = ? WHERE seq = ?',
            (text, digest, event_identity(lane, seq, 0, digest), seq))

    # R1: a damaged retention_state must not re-open a compacted identity
    def test_r1_missing_retention_state_cannot_admit_a_second_effect(self):
        lane, events = self.prepared()
        self.compact(lane, 6)
        sql(self.f.consumer, 'DELETE FROM retention_state')
        with cr.TieredEffectStore(self.f.consumer) as store:
            with self.assertRaises(cr.TierIntegrityError):
                store.accept([contract.changed_event(events[0])])
            with self.assertRaises(cr.TierIntegrityError):
                store.accept([events[0]])
        row = sql(self.f.consumer, 'SELECT * FROM effect_segments WHERE start_seq = 1')[0]
        with self.assertRaises(sqlite3.IntegrityError):      # trigger now also guards digest-backed identities
            sql(self.f.consumer, 'INSERT INTO effects VALUES (?, ?, 1, 0, ?, ?)', ('x' * 64, lane, '0' * 64, '{}'))
        self.assertEqual(self.counts()['hot'], 0)
        self.assertEqual(row['start_seq'], 1)

    # R2 + R4: never write (or reuse) a segment the bounded reader cannot read back
    def test_r2_oversized_line_refused_even_with_identical_existing_file(self):
        lane, _ = self.prepared(2)
        self.oversize(lane, 1)
        rows = sql(self.f.consumer, 'SELECT event_id, lane, seq, epoch, payload_sha256, payload FROM effects '
                                    'WHERE seq = 1')
        target = cr.archive_root(self.f.consumer) / cr.segment_relative_path(1, 0, 1, 1)
        target.parent.mkdir(parents=True)
        target.write_bytes(cr.segment_bytes(rows))
        before = self.counts()
        for apply in (False, True):
            with self.subTest(apply=apply), self.assertRaisesRegex(ValueError, 'audit limits'):
                cr.compact_consumer(self.f.consumer, lane=lane, content_watermark=1, ignore_producer_window=True,
                                    segment_size=1, apply=apply)
        self.assertEqual(self.counts(), before)

    def test_r4_oversized_line_in_a_later_segment_refused_before_any_commit(self):
        lane, _ = self.prepared(2)
        self.oversize(lane, 2)
        before = self.counts()
        for apply in (False, True):
            with self.subTest(apply=apply), self.assertRaises(ValueError):
                cr.compact_consumer(self.f.consumer, lane=lane, content_watermark=2, ignore_producer_window=True,
                                    segment_size=1, apply=apply)
        self.assertEqual(self.counts(), before)
        self.assertEqual(list(cr.archive_root(self.f.consumer).rglob('*.gz')), [])

    def test_r4_compressed_size_limit(self):
        lane, _ = self.prepared(3)
        rows = sql(self.f.consumer, 'SELECT event_id, lane, seq, epoch, payload_sha256, payload FROM effects '
                                    'ORDER BY seq')
        data = cr.segment_bytes(rows)
        cr.validate_segment_bytes(data, lane=lane, epoch=0, start_seq=1, end_seq=3)
        with self.assertRaisesRegex(ValueError, 'compressed bytes'):
            cr.validate_segment_bytes(data, lane=lane, epoch=0, start_seq=1, end_seq=3,
                                      limits=ArchiveLimits(max_compressed_bytes=100))

    # R3: archive directories reached through a junction/symlink are refused everywhere
    def _junction(self, link, target):
        if os.name == 'nt':
            done = subprocess.run(['cmd', '/c', 'mklink', '/J', str(link), str(target)], capture_output=True, text=True)
            if done.returncode:
                self.skipTest('cannot create junction')
        else:
            os.symlink(target, link, target_is_directory=True)
        self.addCleanup(_remove_directory_link, link)

    def test_r3_redirected_lane_directory_refused_by_compaction(self):
        lane, _ = self.prepared()
        outside = self.f.base / 'outside'
        outside.mkdir()
        cr.archive_root(self.f.consumer).mkdir()
        self._junction(cr.archive_root(self.f.consumer) / 'lane-1', outside)
        before = self.counts()
        for apply in (False, True):
            with self.subTest(apply=apply), self.assertRaisesRegex(ValueError, 'redirected'):
                cr.compact_consumer(self.f.consumer, lane=lane, content_watermark=6, ignore_producer_window=True,
                                    segment_size=3, apply=apply)
        self.assertEqual(self.counts(), before)
        self.assertEqual(list(outside.iterdir()), [])

    def test_r3_redirected_lane_directory_fails_audit_and_rehydrate(self):
        lane, _ = self.prepared()
        self.compact(lane, 6)
        lane_dir = cr.archive_root(self.f.consumer) / 'lane-1'
        moved = self.f.base / 'moved_lane'
        shutil.move(str(lane_dir), str(moved))
        self._junction(lane_dir, moved)
        report = reconcile_archive_aware(self.f.root, self.f.consumer)
        self.assertEqual(report['status'], 'INCONSISTENT')
        self.assertIn('CONSUMER_SEGMENT_PATH_INVALID', report['issue_counts'])
        with self.assertRaisesRegex(ValueError, 'redirected'):
            cr.rehydrate_consumer(self.f.consumer, lane=lane, apply=True)
        self.assertEqual(self.counts()['hot'], 0)

    # R5: rehydration verifies digests of every segment before the first change
    def test_r5_rehydrate_preflight_checks_digests(self):
        lane, _ = self.prepared()
        self.compact(lane, 6)
        sql(self.f.consumer, 'UPDATE effect_digests SET payload_sha256 = ? WHERE seq = 1', (b'x' * 32,))
        before = self.counts()
        for apply in (False, True):
            with self.subTest(apply=apply), self.assertRaisesRegex(ValueError, 'digest tier disagrees'):
                cr.rehydrate_consumer(self.f.consumer, lane=lane, apply=apply)
        self.assertEqual(self.counts(), before)

    # R6: a crashed lane's file can no longer collide with another lane's id
    def test_r6_crash_does_not_free_the_lane_id_for_another_lane(self):
        eth = self.f.sources / 'eth.jsonl'
        multi_lane.make_synthetic(self.f.source, 6, None, 'BTCUSDT')
        multi_lane.make_synthetic(eth, 6, None, 'ETHUSDT')
        self.f.replay([f'BTC={self.f.source}', f'ETH={eth}'])
        btc = self.f.deliver('BTC')[0]['lane']
        eth_lane = self.f.deliver('ETH')[0]['lane']
        cr.migrate_consumer_v2(self.f.consumer, apply=True)
        script = ('import sys\nfrom ftdi_core.consumer_retention import compact_consumer\n'
                  'compact_consumer(sys.argv[1], lane=sys.argv[2], content_watermark=3, ignore_producer_window=True, '
                  'segment_size=3, apply=True, crash_stage="after_segment_file")\n')
        done = subprocess.run([sys.executable, '-c', script, str(self.f.consumer), btc], cwd=PROJECT_ROOT,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 86, done.stderr)
        self.compact(eth_lane, 3)                         # previously refused: same lane-1 file name
        self.compact(btc, 3)                              # retry reuses the identical orphan bytes
        ids = {r['lane']: r['lane_id'] for r in sql(self.f.consumer, 'SELECT * FROM lane_ids')}
        self.assertNotEqual(ids[btc], ids[eth_lane])
        report = reconcile_archive_aware(self.f.root, self.f.consumer)
        self.assertEqual(report['status'], 'COMPLETE', report['issue_counts'])
        self.assertEqual(report['metrics']['consumer_compacted_effects'], 6)


if __name__ == '__main__':
    unittest.main()
