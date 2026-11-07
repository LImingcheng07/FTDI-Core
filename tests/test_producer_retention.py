"""Core `archive-producer`: mandatory pre-audit, read-only dry run, chunked --apply, explicit stop reasons
(ADR-003). Includes regressions for the independent review of the producer archive (findings P1-P8)."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from test_archive_e2e import Fixture, evidence_snapshot, sql
from ftdi_core import consumer_retention as cr
from ftdi_core import producer_retention as pr
from ftdi_core.adapters.s070041 import retention
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
        return archive_producer(self.f.root, kwargs.pop('lane', 'BTC'), consumer_db=kwargs.pop('consumer', self.f.consumer),
                                **kwargs)

    def archived(self, alias='BTC'):
        rows = sql(self.f.db(alias), "SELECT v FROM meta WHERE k = 'archived_records'")
        return int(rows[0]['v']) if rows else 0

    def counts(self):
        return sql(self.f.db(), 'SELECT (SELECT COUNT(*) FROM outbox) o, (SELECT COUNT(*) FROM delivery) d, '
                                '(SELECT COUNT(*) FROM raw_journal) r')

    def gz_files(self):
        archives = self.f.root / 'archives'
        return sorted(p.name for p in archives.rglob('*.gz')) if archives.exists() else []

    def test_dry_run_changes_nothing_then_apply_in_chunks(self):
        self.f.ingest(12)
        self.f.deliver()
        before = evidence_snapshot(self.f.root, self.f.consumers, self.f.sources)
        plan = self.run_archive(limit=8)
        self.assertEqual(evidence_snapshot(self.f.root, self.f.consumers, self.f.sources), before)
        self.assertFalse((self.f.root / 'archives').exists())
        self.assertEqual((plan['status'], plan['would_archive'], plan['stopped'], plan['changed']),
                         ('DRY_RUN', 8, 'LIMIT_REACHED', False))
        self.assertEqual(plan['pre_audit']['status'], 'COMPLETE')
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
        self.assertEqual((plan['pre_audit']['status'], plan['would_archive'], plan['stopped']),
                         ('PENDING_DELIVERY', 5, 'NOT_ACKNOWLEDGED'))
        done = self.run_archive(apply=True)
        self.assertEqual((done['archived_new'], done['stopped']), (5, 'NOT_ACKNOWLEDGED'))
        again = self.run_archive(apply=True)
        self.assertEqual((again['status'], again['archived_new'], again['changed']), ('NOTHING_ARCHIVABLE', 0, False))
        self.f.deliver()
        rest = self.run_archive(apply=True)
        self.assertEqual((rest['archived_new'], rest['archived_total'], rest['stopped']), (7, 12, 'END_OF_JOURNAL'))
        self.assertEqual(self.run_archive()['stopped'], 'END_OF_JOURNAL')
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], 'COMPLETE')

    def test_consumer_committed_but_unacked_is_archivable_up_to_the_unacked_event(self):
        self.f.ingest(12)
        self.f.deliver(limit=6)
        self.f.deliver(limit=2, ack=False)   # RETRY_REQUIRED: consumer has 7-8, producer ACK lost
        plan = self.run_archive()
        self.assertEqual((plan['pre_audit']['status'], plan['would_archive'], plan['stopped']),
                         ('RETRY_REQUIRED', 6, 'NOT_ACKNOWLEDGED'))
        self.assertEqual(self.run_archive(apply=True)['archived_new'], 6)

    def test_missing_receipt_refused_by_dry_run_and_apply(self):
        self.f.ingest(12)
        self.f.deliver()
        sql(self.f.consumer, 'DELETE FROM effects WHERE seq = 4')
        before = evidence_snapshot(self.f.root, self.f.consumers)
        with self.assertRaisesRegex(ValueError, 'INCONSISTENT.*PRODUCER_ACK_WITHOUT_CONSUMER_EFFECT'):
            self.run_archive()
        self.assertEqual(evidence_snapshot(self.f.root, self.f.consumers), before)
        with self.assertRaisesRegex(ValueError, 'PRODUCER_ACK_WITHOUT_CONSUMER_EFFECT'):
            self.run_archive(apply=True)
        self.assertEqual(evidence_snapshot(self.f.root, self.f.consumers), before)
        # The adapter enforces the same rule on its own (direct module use).
        with self.assertRaisesRegex(ValueError, 'receipt missing'):
            retention.prune(self.f.root, 'BTC', 12, consumer_db=self.f.consumer)
        self.assertEqual(self.archived(), 0)
        self.assertEqual(self.gz_files(), [])

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

    def test_tampered_digest_is_refused(self):
        self.f.ingest(12)
        lane = self.f.deliver()[0]['lane']
        cr.migrate_consumer_v2(self.f.consumer, apply=True)
        cr.compact_consumer(self.f.consumer, lane=lane, content_watermark=8, producer_root=self.f.root,
                            segment_size=4, apply=True)
        sql(self.f.consumer, 'UPDATE effect_digests SET payload_sha256 = ? WHERE seq = 2', (b'y' * 32,))
        for apply in (False, True):
            with self.assertRaisesRegex(ValueError, 'CONSUMER_DIGEST_MISMATCH'):
                self.run_archive(apply=apply)
        self.assertEqual(self.archived(), 0)

    def test_source_mutation_refused(self):
        self.f.ingest(12)
        self.f.deliver()
        self.f.source.write_bytes(self.f.source.read_bytes() + b'\n')
        for apply in (False, True):
            with self.assertRaisesRegex(ValueError, 'SOURCE_FINGERPRINT_MISMATCH'):
                self.run_archive(apply=apply)
        self.assertEqual(self.archived(), 0)

    def test_unresolved_gap_in_any_lane_refuses(self):
        self.f.ingest(12, blocked_eth=True)
        self.f.deliver()
        self.f.deliver(alias='ETH')
        for lane in ('ETH', 'BTC'):
            with self.assertRaisesRegex(ValueError, 'PENDING_GAP_UNRESOLVED'):
                self.run_archive(lane=lane)
        self.assertEqual((self.archived('BTC'), self.archived('ETH')), (0, 0))

    # Regressions for the independent review (P1-P8).

    def test_p1_two_outbox_rows_on_one_raw_record_are_never_deleted(self):
        self.f.ingest(2)
        self.f.deliver(limit=1)
        sql(self.f.db(), 'UPDATE outbox SET raw_id = (SELECT raw_id FROM outbox WHERE seq = 1) WHERE seq = 2')
        before = self.counts()
        with self.assertRaisesRegex(ValueError, 'Several outbox or delivery rows'):
            retention.prune(self.f.root, 'BTC', 2, consumer_db=self.f.consumer)
        with self.assertRaises(ValueError):
            self.run_archive(apply=True)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.gz_files(), [])

    def test_p1_delete_count_mismatch_rolls_back(self):
        self.f.ingest(4)
        self.f.deliver()
        real = retention.collect_archivable

        def drop_outbox(db, start, limit):
            items = real(db, start, limit)
            items[1].pop('outbox')   # pretend raw 1 had no outbox row: its outbox would be deleted unarchived
            return items
        before = self.counts()
        with mock.patch.object(retention, 'collect_archivable', drop_outbox):
            # caught by the in-transaction revalidation (or, failing that, by the delete counts)
            with self.assertRaisesRegex(ValueError, 'Rows (changed since archive was prepared|deleted differ)'):
                retention.prune(self.f.root, 'BTC', 4, consumer_db=self.f.consumer)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.archived(), 0)

    def test_p2_ack_for_another_lane_is_not_archived(self):
        self.f.ingest(4)
        self.f.deliver()
        sql(self.f.db(), "UPDATE delivery SET lane = 'OTHER' WHERE seq = 2")
        with self.assertRaisesRegex(ValueError, 'External ACK evidence mismatch'):
            retention.prune(self.f.root, 'BTC', 4, consumer_db=self.f.consumer)
        with self.assertRaisesRegex(ValueError, 'INCONSISTENT'):
            self.run_archive(apply=True)
        self.assertEqual(self.archived(), 0)
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], 'INCONSISTENT')

    def test_p3_producer_db_is_not_an_independent_consumer(self):
        self.f.ingest(4)
        self.f.deliver()
        for apply in (False, True):
            with self.assertRaisesRegex(ValueError, 'independent'):
                self.run_archive(consumer=self.f.db(), apply=apply)
        with self.assertRaisesRegex(ValueError, 'independent'):
            retention.prune(self.f.root, 'BTC', 4, consumer_db=self.f.db())
        self.assertEqual(self.archived(), 0)

    def test_p4_write_during_dry_run_is_detected(self):
        self.f.ingest(4)
        self.f.deliver()
        real = retention.collect_archivable

        def concurrent_writer(db, start, limit):
            with sqlite3.connect(self.f.db()) as writer:
                writer.execute("INSERT INTO meta(k, v) VALUES ('concurrent', '1')")
            writer.close()
            return real(db, start, limit)
        with mock.patch.object(pr, 'collect_archivable', concurrent_writer):
            with self.assertRaisesRegex(ValueError, 'changed during the dry run'):
                self.run_archive()

    def test_p5_segment_collision_is_predicted_and_progress_reported(self):
        self.f.ingest(4)
        self.f.deliver()
        target = self.f.root / 'archives' / 'BTC' / 'raw_000000000002_000000000004.jsonl.gz'
        target.parent.mkdir(parents=True)
        target.write_bytes(b'not the segment')
        with self.assertRaisesRegex(ValueError, 'collision with different content'):
            self.run_archive(chunk=2)
        with self.assertRaisesRegex(ValueError, r'collision.*committed by this run: 2 events in 1 segments; '
                                                r'archived_total is now 2'):
            self.run_archive(chunk=2, apply=True)
        self.assertEqual(self.archived(), 2)
        self.assertEqual(target.read_bytes(), b'not the segment')
        report = reconcile_archive_aware(self.f.root, self.f.consumer)
        self.assertEqual(report['status'], 'COMPLETE', report['issue_counts'])

    def test_p5_identical_existing_segment_is_reused_by_both(self):
        self.f.ingest(4)
        self.f.deliver()
        with sqlite3.connect(self.f.db()) as db:   # write the exact file a crashed prune would have left
            db.row_factory = sqlite3.Row
            items = retention.collect_archivable(db, 0, 4)
        db.close()
        retention.export_gzip(self.f.root / 'archives' / 'BTC' / 'raw_000000000000_000000000004.jsonl.gz', items)
        self.assertEqual(self.run_archive()['would_archive'], 4)
        self.assertEqual(self.run_archive(apply=True)['archived_new'], 4)
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], 'COMPLETE')

    def test_p6_failure_after_a_commit_reports_committed_progress(self):
        self.f.ingest(6)
        self.f.deliver()
        real, calls = retention.prune, []

        def fail_after_second_commit(*args, **kwargs):
            result = real(*args, **kwargs)
            calls.append(result)
            if len(calls) == 2:
                raise OSError('simulated failure after the commit')
            return result
        with mock.patch.object(pr, 'prune', fail_after_second_commit):
            with self.assertRaisesRegex(ValueError, r'committed by this run: 4 events in 2 segments; '
                                                    r'archived_total is now 4'):
                self.run_archive(chunk=2, apply=True)
        self.assertEqual(self.archived(), 4)

    def test_p8_incomplete_schema_releases_the_database_handle(self):
        self.f.ingest(4)
        self.f.deliver()
        sql(self.f.db(), 'DROP TABLE outbox')
        try:
            retention.prune(self.f.root, 'BTC', 4, consumer_db=self.f.consumer)
        except sqlite3.OperationalError as exc:
            frames = exc.__traceback__   # keep the failing frames (and their locals) alive, unlike assertRaises
        else:
            self.fail('incomplete schema accepted')
        self.f.db().unlink()   # Windows: fails with WinError 32 if the connection leaked
        del frames

    # Regressions for the follow-up review (Q1-Q7).

    def test_q1_trigger_in_producer_db_refuses(self):
        self.f.ingest(4)
        self.f.deliver()
        sql(self.f.db(), 'CREATE TRIGGER extra AFTER DELETE ON raw_journal WHEN OLD.id = 1 BEGIN '
                         'DELETE FROM raw_journal WHERE id = 4; END')
        before = self.counts()
        for apply in (False, True):
            with self.assertRaisesRegex(ValueError, 'triggers'):
                self.run_archive(apply=apply)
        with self.assertRaisesRegex(ValueError, 'triggers'):
            retention.prune(self.f.root, 'BTC', 4, consumer_db=self.f.consumer)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.archived(), 0)

    def test_q1_table_count_invariant_rolls_back(self):
        self.f.ingest(4)
        self.f.deliver()
        real = retention.table_counts
        calls = []

        def drift(db):   # the second count (after the deletes) reports one raw row too many removed
            counts = real(db)
            calls.append(counts)
            return counts if len(calls) == 1 else (counts[0] - 1,) + counts[1:]
        before = self.counts()
        with mock.patch.object(retention, 'table_counts', drift):
            with self.assertRaisesRegex(ValueError, 'Rows deleted differ'):
                retention.prune(self.f.root, 'BTC', 4, consumer_db=self.f.consumer)
        self.assertEqual(self.counts(), before)

    def test_q2_invalid_delivered_flag_is_not_archived(self):
        self.f.ingest(4)
        self.f.deliver()
        sql(self.f.db(), 'UPDATE outbox SET delivered = 2 WHERE seq = 1')
        with self.assertRaisesRegex(ValueError, 'delivered flag'):
            retention.prune(self.f.root, 'BTC', 4, consumer_db=self.f.consumer)
        with self.assertRaises(ValueError):
            self.run_archive(apply=True)
        self.assertEqual(self.archived(), 0)

    def test_q3_records_the_audit_could_not_read_are_refused_before_any_change(self):
        for field, value in (('received_ns', -1), ('received_ns', b'not-json')):
            with self.subTest(field=field, value=value):
                f = Fixture(tempfile.mkdtemp(prefix='q3_', dir=self.f.base))
                f.ingest(4)
                f.deliver()
                sql(f.db(), f'UPDATE raw_journal SET {field} = ? WHERE file_index = 2', (value,))
                for apply in (False, True):
                    with self.assertRaisesRegex(ValueError, r'record file_index=2 (would not be readable|cannot be)'):
                        archive_producer(f.root, 'BTC', consumer_db=f.consumer, limit=4, apply=apply)
                self.assertEqual(sql(f.db(), "SELECT v FROM meta WHERE k = 'archived_records'"), [])
                self.assertFalse((f.root / 'archives').exists())

    def test_q4_overlapping_unreferenced_segment_file_is_refused(self):
        self.f.ingest(4)
        self.f.deliver()
        stray = self.f.root / 'archives' / 'BTC' / 'raw_000000000000_000000000002.jsonl.gz'
        stray.parent.mkdir(parents=True)
        stray.write_bytes(b'garbage')
        for apply in (False, True):
            with self.assertRaisesRegex(ValueError, 'overlaps the range'):
                self.run_archive(apply=apply)
        self.assertEqual(self.archived(), 0)
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], 'COMPLETE')

    def test_q5_blocked_temp_path_is_predicted(self):
        self.f.ingest(4)
        self.f.deliver()
        (self.f.root / 'archives' / 'BTC' / 'raw_000000000000_000000000004.jsonl.gz.tmp').mkdir(parents=True)
        for apply in (False, True):
            with self.assertRaisesRegex(ValueError, 'temp path is blocked'):
                self.run_archive(apply=apply)
        self.assertEqual(self.archived(), 0)

    def test_q6_content_change_with_same_size_and_mtime_during_dry_run(self):
        self.f.ingest(4)
        self.f.deliver()
        real, db_file = retention.collect_archivable, self.f.db()

        def silent_rewrite(db, start, limit):
            info = os.stat(db_file)
            with open(db_file, 'r+b') as handle:   # header bytes 72..91 are reserved (zero)
                handle.seek(80)
                handle.write(b'')
            os.utime(db_file, ns=(info.st_atime_ns, info.st_mtime_ns))
            return real(db, start, limit)
        with mock.patch.object(pr, 'collect_archivable', silent_rewrite):
            with self.assertRaisesRegex(ValueError, 'changed during the dry run'):
                self.run_archive()

    def test_q7_change_between_pre_audit_and_apply_refuses(self):
        self.f.ingest(4)
        self.f.deliver()
        real = pr.reconcile_archive_aware

        def audit_then_tamper(*args, **kwargs):
            report = real(*args, **kwargs)
            sql(self.f.db(), "INSERT INTO meta(k, v) VALUES ('tampered', '1')")
            return report
        with mock.patch.object(pr, 'reconcile_archive_aware', audit_then_tamper):
            with self.assertRaisesRegex(ValueError, 'changed during the pre-audit'):
                self.run_archive(apply=True)
        self.assertEqual(self.archived(), 0)

    def test_q8_ack_changed_between_segment_write_and_delete_rolls_back(self):
        self.f.ingest(4)
        self.f.deliver()
        real = retention.export_gzip

        def change_ack(path, items):   # a writer violating the maintenance window, inside prune
            result = real(path, items)
            sql(self.f.db(), "UPDATE delivery SET lane = 'wrong' WHERE seq = 1")
            return result
        with mock.patch.object(retention, 'export_gzip', change_ack):
            with self.assertRaisesRegex(ValueError, 'ACK evidence changed'):
                self.run_archive(apply=True)
        self.assertEqual(self.archived(), 0)
        self.assertEqual(self.counts()[0]['o'], 4)
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], 'INCONSISTENT')

    def test_q8_disposition_or_payload_changed_before_delete_rolls_back(self):
        for statement in ("UPDATE disposition SET detail = 'changed' WHERE raw_id = 2",
                          "UPDATE raw_journal SET received_ns = received_ns + 1 WHERE id = 2",
                          "UPDATE outbox SET payload = payload || ' ' WHERE seq = 2"):
            with self.subTest(statement=statement):
                f = Fixture(tempfile.mkdtemp(prefix='q8_', dir=self.f.base))
                f.ingest(4)
                f.deliver()
                real = retention.export_gzip

                def change(path, items, f=f, statement=statement):
                    result = real(path, items)
                    sql(f.db(), statement)
                    return result
                with mock.patch.object(retention, 'export_gzip', change):
                    with self.assertRaisesRegex(ValueError, 'changed since archive was prepared'):
                        retention.prune(f.root, 'BTC', 4, consumer_db=f.consumer)
                self.assertEqual(sql(f.db(), "SELECT v FROM meta WHERE k = 'archived_records'"), [])

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
