"""Producer archive with a compacted (v2) consumer: digest rows are valid independent receipts (ADR-007 phase 2)."""
from __future__ import annotations

import tempfile
import unittest

from test_archive_e2e import Fixture, sql
from ftdi_core import consumer_retention as cr
from ftdi_core.archive_e2e import reconcile_archive_aware


class TieredProducerPruneTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='tiered_prune_')
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)
        self.f.ingest(12)
        self.lane = self.f.deliver()[0]['lane']
        cr.migrate_consumer_v2(self.f.consumer, apply=True)

    def test_consumer_compacted_first_then_producer_archived(self):
        cr.compact_consumer(self.f.consumer, lane=self.lane, content_watermark=8, producer_root=self.f.root,
                            segment_size=4, apply=True)
        result = self.f.prune(8)
        self.assertEqual(result['archived_new'], 8)
        report = reconcile_archive_aware(self.f.root, self.f.consumer)
        self.assertEqual(report['status'], 'COMPLETE', report['issue_counts'])
        self.assertEqual(report['metrics']['consumer_compacted_effects'], 8)
        self.assertEqual(report['metrics']['archived_events_matched'], 8)

    def test_tampered_digest_is_not_a_receipt(self):
        cr.compact_consumer(self.f.consumer, lane=self.lane, content_watermark=8, producer_root=self.f.root,
                            segment_size=4, apply=True)
        sql(self.f.consumer, 'UPDATE effect_digests SET payload_sha256 = ? WHERE seq = 3', (b'x' * 32,))
        outbox_before = sql(self.f.db(), 'SELECT COUNT(*) n FROM outbox')[0]['n']
        with self.assertRaisesRegex(ValueError, 'receipt missing'):
            self.f.prune(8)
        self.assertEqual(sql(self.f.db(), 'SELECT COUNT(*) n FROM outbox')[0]['n'], outbox_before)
        self.assertEqual(sql(self.f.db(), "SELECT v FROM meta WHERE k = 'archived_records'"), [])

    def test_missing_digest_and_missing_hot_row_is_not_a_receipt(self):
        cr.compact_consumer(self.f.consumer, lane=self.lane, content_watermark=8, producer_root=self.f.root,
                            segment_size=4, apply=True)
        sql(self.f.consumer, 'DELETE FROM effect_digests WHERE seq = 5')
        with self.assertRaisesRegex(ValueError, 'receipt missing'):
            self.f.prune(8)

    def test_hot_rows_remain_valid_receipts_on_a_migrated_consumer(self):
        self.assertEqual(self.f.prune(12)['archived_new'], 12)
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], 'COMPLETE')


if __name__ == '__main__':
    unittest.main()
