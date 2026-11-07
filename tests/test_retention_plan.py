"""Task 002 read-only retention planner (dry run).  Fixtures reuse the archive contract suite."""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from ftdi_core.retention_plan import DIGEST_ROW_BYTES_ESTIMATE, plan_consumer_retention

from test_archive_e2e import Fixture, PROJECT_ROOT, sql


class RetentionPlanTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='retention_plan_')
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def plan(self, **kwargs):
        return plan_consumer_retention(self.f.consumer, **kwargs)

    def lane(self, result):
        self.assertEqual(len(result['lanes']), 1, result['lanes'])
        return result['lanes'][0]

    def test_fully_delivered_lane_without_producer_evidence(self):
        self.f.ingest(30)
        self.f.deliver()
        result = self.plan()
        self.assertEqual(result['status'], 'PLANNED')
        self.assertEqual(result['mode'], 'DRY_RUN_READ_ONLY')
        self.assertIs(result['destructive_actions_performed'], False)
        lane = self.lane(result)
        self.assertEqual((lane['first_seq'], lane['max_seq'], lane['rows']), (1, 30, 30))
        self.assertTrue(lane['contiguous'])
        self.assertEqual(lane['proposed_content_watermark'], 30)
        self.assertEqual(lane['rows_at_or_below_watermark'], 30)
        self.assertGreater(lane['payload_bytes_at_or_below_watermark'], 30 * 100)
        self.assertEqual(lane['estimated_digest_tier_bytes'], 30 * DIGEST_ROW_BYTES_ESTIMATE)
        self.assertIsNone(lane['producer_low_unacked'])

    def test_producer_window_keeps_unacked_retries_hot(self):
        self.f.ingest(30)
        self.f.deliver(limit=20)               # consumer + producer ACK for seq 1..20
        self.f.deliver(limit=5, ack=False)     # consumer has 21..25, producer has not ACKed them
        lane = self.lane(self.plan(producer_root=self.f.root))
        self.assertEqual(lane['max_seq'], 25)
        self.assertEqual(lane['producer_low_unacked'], 21)
        self.assertEqual(lane['proposed_content_watermark'], 20)
        self.assertEqual(lane['rows_at_or_below_watermark'], 20)

    def test_all_acked_and_archived_producer_uses_next_seq(self):
        self.f.archived(n=12, limits=(6, 6))
        lane = self.lane(self.plan(producer_root=self.f.root))
        self.assertEqual(lane['producer_low_unacked'], 13)
        self.assertEqual(lane['proposed_content_watermark'], 12)

    def test_hole_caps_the_watermark(self):
        self.f.ingest(20)
        self.f.deliver()
        sql(self.f.consumer, 'DELETE FROM effects WHERE seq = 8')
        lane = self.lane(self.plan())
        self.assertFalse(lane['contiguous'])
        self.assertEqual(lane['first_missing_seq'], 8)
        self.assertEqual(lane['contiguous_high'], 7)
        self.assertEqual(lane['proposed_content_watermark'], 7)
        self.assertEqual(lane['rows_at_or_below_watermark'], 7)

    def test_margin_and_nothing_to_compact(self):
        self.f.ingest(10)
        self.f.deliver()
        self.assertEqual(self.lane(self.plan(margin=3))['proposed_content_watermark'], 7)
        result = self.plan(margin=10)
        self.assertEqual(result['status'], 'NOTHING_TO_COMPACT')
        self.assertIsNone(self.lane(result)['proposed_content_watermark'])
        self.assertEqual(result['totals']['rows_at_or_below_watermark'], 0)
        with self.assertRaises(ValueError):
            self.plan(margin=-1)

    def test_consumer_lane_unknown_to_producer_is_blocked(self):
        self.f.ingest(5)
        self.f.deliver()
        foreign = self.f.base / 'other_run'
        other = Fixture.__new__(Fixture)
        other.base, other.root, other.source = self.f.base, foreign, self.f.sources / 'eth.jsonl'
        from ftdi_core.adapters.s070041 import multi_lane
        multi_lane.make_synthetic(other.source, 5, None, 'ETHUSDT')
        other.replay([f'ETH={other.source}'])
        result = self.plan(producer_root=foreign)
        self.assertEqual(result['status'], 'BLOCKED')
        lane = self.lane(result)
        self.assertIn('lane not registered in the producer root', lane['blockers'])
        self.assertIsNone(lane['proposed_content_watermark'])
        self.assertEqual(result['producer_lanes_without_consumer_effects'], ['ETH'])

    def test_unrecognized_consumer_schema_is_blocked(self):
        self.f.ingest(5)
        self.f.deliver()
        rows = sql(self.f.consumer, 'SELECT * FROM effects')
        fake = self.f.consumers / 'view.sqlite3'
        with closing(sqlite3.connect(fake)) as db, db:
            db.execute('CREATE TABLE real(event_id TEXT, lane TEXT, seq INTEGER, epoch INTEGER, '
                       'payload_sha256 TEXT, payload TEXT)')
            db.executemany('INSERT INTO real VALUES (:event_id,:lane,:seq,:epoch,:payload_sha256,:payload)', rows)
            db.execute('CREATE VIEW effects AS SELECT * FROM real')
        result = plan_consumer_retention(fake)
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertEqual(result['blockers'][0]['code'], 'CONSUMER_SCHEMA_UNRECOGNIZED')

    def test_read_only_and_report_rules(self):
        self.f.archived(n=12, limits=(6,))
        reports = self.f.base / 'reports'
        reports.mkdir()
        before = self.f.snapshot()
        result = self.plan(producer_root=self.f.root, report_path=reports / 'plan.json')
        self.assertEqual(json.loads((reports / 'plan.json').read_text(encoding='utf-8')), result)
        self.assertEqual(before, self.f.snapshot())  # no evidence byte or sidecar changed
        for bad in (self.f.consumer, Path(f'{self.f.consumer}-wal'), self.f.root / 'lanes' / 'plan.json',
                    self.f.root / 'archives' / 'BTC' / 'plan.json'):
            with self.subTest(report=str(bad)):
                with self.assertRaises(ValueError):
                    self.plan(producer_root=self.f.root, report_path=bad)
        self.assertEqual(before, self.f.snapshot())
        missing = self.f.consumers / 'missing.sqlite3'
        with self.assertRaises(FileNotFoundError):
            plan_consumer_retention(missing)
        self.assertFalse(missing.exists())

    def test_open_consumer_wal_is_not_a_concurrent_modification(self):
        """Regression (full-day run): a consumer process that was terminated leaves WAL frames and -shm with
        no open connection; the read-only reader rebuilds -shm, which is not a concurrent modification."""
        self.f.ingest(12)
        rows = self.f.rows()
        events = Path(self.f.base) / 'events.json'
        from ftdi_core.adapters.s070041.external_delivery import envelope
        events.write_text(json.dumps([envelope(r) for r in rows]), encoding='utf-8')
        writer = ('import json, os, sys\n'
                  'from ftdi_core.adapters.s070041.external_delivery import EffectStore\n'
                  'store = EffectStore(sys.argv[1])\n'
                  'store.db.execute("PRAGMA wal_autocheckpoint=0")\n'
                  'store.accept(json.load(open(sys.argv[2], encoding="utf-8")))\n'
                  'os._exit(0)  # terminated like lab-e2e terminates the consumer: WAL and -shm stay behind\n')
        done = subprocess.run([sys.executable, '-c', writer, str(self.f.consumer), str(events)],
                              cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.f.ack(rows)
        self.assertGreater(Path(f'{self.f.consumer}-wal').stat().st_size, 0)
        self.assertTrue(Path(f'{self.f.consumer}-shm').exists())
        result = self.plan(producer_root=self.f.root)
        self.assertTrue(result['snapshot_stable'], result['blockers'])
        self.assertEqual(result['status'], 'PLANNED')

    def test_handles_released_for_windows(self):
        self.f.archived(n=12, limits=(6,))
        self.plan(producer_root=self.f.root)
        shutil.rmtree(self.f.root)
        self.f.consumer.unlink()

    def test_cli_exit_codes(self):
        self.f.ingest(6)
        self.f.deliver()

        def run(*args):
            return subprocess.run([sys.executable, '-m', 'ftdi_core', 'plan-consumer-retention', *args],
                                  cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=60)
        ok = run('--consumer-db', str(self.f.consumer), '--producer-root', str(self.f.root))
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertEqual(json.loads(ok.stdout)['status'], 'PLANNED')
        no_registry = run('--consumer-db', str(self.f.consumer), '--producer-root', str(self.f.base / 'nowhere'))
        self.assertEqual(no_registry.returncode, 3)
        missing = run('--consumer-db', str(self.f.consumers / 'missing.sqlite3'))
        self.assertEqual(missing.returncode, 3)
        self.assertIn('FTDI_CORE_FAIL_CLOSED', missing.stderr)


if __name__ == '__main__':
    unittest.main()
