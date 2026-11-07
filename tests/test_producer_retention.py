"""Core `archive-producer`: read-only dry run by default, chunked --apply, explicit stop reasons (ADR-003)."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import tempfile
import unittest

from test_archive_e2e import Fixture, evidence_snapshot, sql
from ftdi_core import consumer_retention as cr
from ftdi_core.archive_e2e import reconcile_archive_aware
from ftdi_core.cli import main
from ftdi_core.producer_retention import archive_producer


class ArchiveProducerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='archive_producer_')
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def run_archive(self, **kwargs):
        kwargs.setdefault('limit', 100)
        return archive_producer(self.f.root, kwargs.pop('lane', 'BTC'), consumer_db=self.f.consumer, **kwargs)

    def archived(self, alias='BTC'):
        rows = sql(self.f.db(alias), "SELECT v FROM meta WHERE k = 'archived_records'")
        return int(rows[0]['v']) if rows else 0

    def test_dry_run_changes_nothing_then_apply_in_chunks(self):
        self.f.ingest(12)
        self.f.deliver()
        before = evidence_snapshot(self.f.root, self.f.consumers, self.f.sources)
        plan = self.run_archive(limit=8)
        self.assertEqual(evidence_snapshot(self.f.root, self.f.consumers, self.f.sources), before)
        self.assertFalse((self.f.root / 'archives').exists())
        self.assertEqual((plan['status'], plan['would_archive'], plan['stopped'], plan['changed']),
                         ('DRY_RUN', 8, 'LIMIT_REACHED', False))
        self.assertEqual(plan['receipts'], {'consumer_hot_rows': 8, 'consumer_digests': 0})
        done = self.run_archive(limit=8, chunk=3, apply=True)
        self.assertEqual((done['status'], done['archived_new'], done['archived_total'], done['stopped']),
                         ('ARCHIVED', 8, 8, 'LIMIT_REACHED'))
        self.assertEqual([s['records'] for s in done['segments']], [3, 3, 2])
        self.assertTrue(all(s['archive_file'].startswith('archives/BTC/') for s in done['segments']))
        self.assertEqual(len(self.f.segments()), 3)
        report = reconcile_archive_aware(self.f.root, self.f.consumer)
        self.assertEqual(report['status'], 'COMPLETE', report['issue_counts'])
        self.assertEqual(report['metrics']['archived_events_matched'], 8)

    def test_stops_before_unacknowledged_events_and_resumes(self):
        self.f.ingest(12)
        self.f.deliver(limit=5)
        plan = self.run_archive()
        self.assertEqual((plan['would_archive'], plan['stopped']), (5, 'NOT_ACKNOWLEDGED'))
        done = self.run_archive(apply=True)
        self.assertEqual((done['archived_new'], done['stopped']), (5, 'NOT_ACKNOWLEDGED'))
        again = self.run_archive(apply=True)
        self.assertEqual((again['status'], again['archived_new'], again['changed']), ('NOTHING_ARCHIVABLE', 0, False))
        self.f.deliver()
        rest = self.run_archive(apply=True)
        self.assertEqual((rest['archived_new'], rest['archived_total'], rest['stopped']), (7, 12, 'END_OF_JOURNAL'))
        self.assertEqual(self.run_archive()['stopped'], 'END_OF_JOURNAL')
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], 'COMPLETE')

    def test_missing_receipt_refused_by_dry_run_and_apply(self):
        self.f.ingest(12)
        self.f.deliver()
        sql(self.f.consumer, 'DELETE FROM effects WHERE seq = 4')
        before = evidence_snapshot(self.f.root, self.f.consumers)
        with self.assertRaisesRegex(ValueError, 'receipt missing'):
            self.run_archive()
        self.assertEqual(evidence_snapshot(self.f.root, self.f.consumers), before)
        counts = sql(self.f.db(), 'SELECT (SELECT COUNT(*) FROM outbox) o, (SELECT COUNT(*) FROM raw_journal) r')
        with self.assertRaisesRegex(ValueError, r'receipt missing.*archived 0 events'):
            self.run_archive(apply=True)
        # The adapter may add its own indexes; no event, ACK or cursor changes and no archive file.
        self.assertEqual(sql(self.f.db(), 'SELECT (SELECT COUNT(*) FROM outbox) o, (SELECT COUNT(*) FROM raw_journal) r'),
                         counts)
        self.assertEqual(self.archived(), 0)
        self.assertEqual(list((self.f.root / 'archives').rglob('*.gz')) if (self.f.root / 'archives').exists() else [], [])

    def test_partial_progress_before_a_refusal_is_reported(self):
        self.f.ingest(12)
        self.f.deliver()
        sql(self.f.consumer, 'DELETE FROM effects WHERE seq = 5')
        with self.assertRaisesRegex(ValueError, r'archived 3 events in 1 committed segments'):
            self.run_archive(chunk=3, apply=True)
        self.assertEqual(self.archived(), 3)
        self.assertEqual(len(self.f.segments()), 1)

    def test_compacted_consumer_digests_are_receipts(self):
        self.f.ingest(12)
        lane = self.f.deliver()[0]['lane']
        cr.migrate_consumer_v2(self.f.consumer, apply=True)
        cr.compact_consumer(self.f.consumer, lane=lane, content_watermark=8, producer_root=self.f.root,
                            segment_size=4, apply=True)
        plan = self.run_archive()
        self.assertEqual(plan['receipts'], {'consumer_hot_rows': 4, 'consumer_digests': 8})
        done = self.run_archive(apply=True)
        self.assertEqual((done['archived_new'], done['stopped']), (12, 'END_OF_JOURNAL'))
        report = reconcile_archive_aware(self.f.root, self.f.consumer)
        self.assertEqual(report['status'], 'COMPLETE', report['issue_counts'])
        self.assertEqual(report['metrics']['consumer_compacted_effects'], 8)

    def test_tampered_digest_is_refused_by_dry_run(self):
        self.f.ingest(12)
        lane = self.f.deliver()[0]['lane']
        cr.migrate_consumer_v2(self.f.consumer, apply=True)
        cr.compact_consumer(self.f.consumer, lane=lane, content_watermark=8, producer_root=self.f.root,
                            segment_size=4, apply=True)
        sql(self.f.consumer, 'UPDATE effect_digests SET payload_sha256 = ? WHERE seq = 2', (b'y' * 32,))
        with self.assertRaisesRegex(ValueError, 'receipt missing'):
            self.run_archive()

    def test_source_mutation_refused(self):
        self.f.ingest(12)
        self.f.deliver()
        self.f.source.write_bytes(self.f.source.read_bytes() + b'\n')
        for apply in (False, True):
            with self.assertRaisesRegex(ValueError, 'fingerprint'):
                self.run_archive(apply=apply)
        self.assertEqual(self.archived(), 0)

    def test_pending_sequence_gap_is_reported(self):
        self.f.ingest(12, blocked_eth=True)
        self.f.deliver(alias='ETH')
        plan = self.run_archive(lane='ETH')
        self.assertEqual(plan['stopped'], 'PENDING_SEQUENCE_GAP')
        done = self.run_archive(lane='ETH', apply=True)
        self.assertEqual((done['archived_new'], done['stopped']), (plan['would_archive'], 'PENDING_SEQUENCE_GAP'))

    def test_argument_validation(self):
        self.f.ingest(4)
        for kwargs in ({'limit': 0}, {'limit': True}, {'chunk': 0}, {'chunk': 20001}, {'lane': 'XRP'}):
            with self.assertRaises(ValueError):
                self.run_archive(**kwargs)
        with self.assertRaisesRegex(ValueError, 'consumer-db'):
            archive_producer(self.f.root, 'BTC', consumer_db=self.f.consumers / 'absent.sqlite3')
        self.assertFalse((self.f.consumers / 'absent.sqlite3').exists())

    def test_cli_dry_run_apply_and_fail_closed(self):
        self.f.ingest(12)
        self.f.deliver()
        base = ['archive-producer', '--outdir', str(self.f.root), '--lane', 'BTC', '--consumer-db',
                str(self.f.consumer), '--limit', '12']
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(base), 0)
        self.assertEqual(json.loads(out.getvalue())['status'], 'DRY_RUN')
        self.assertEqual(self.archived(), 0)
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(base + ['--apply']), 0)
        self.assertEqual(json.loads(out.getvalue())['archived_new'], 12)
        err = io.StringIO()
        with redirect_stderr(err), redirect_stdout(io.StringIO()):
            self.assertEqual(main(base[:-4] + ['--consumer-db', str(self.f.consumers / 'nope.sqlite3')]), 3)
        self.assertIn('FTDI_CORE_FAIL_CLOSED', err.getvalue())


if __name__ == '__main__':
    unittest.main()
