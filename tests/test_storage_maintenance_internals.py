"""vacuum-db internals: logical digest, swap rollback, concurrent change, databases in use (ADR-010)."""
from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from test_archive_e2e import Fixture, sql
from ftdi_core import consumer_retention as cr
from ftdi_core import storage_maintenance as sm
from ftdi_core.archive_e2e import reconcile_archive_aware
from ftdi_core.storage_maintenance import logical_digest, vacuum_db


def digest_of(path):
    with closing(sqlite3.connect(path)) as db:
        return logical_digest(db)


class LogicalDigestTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='digest_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    def make(self, name, rows, extra=()):
        path = self.base / name
        with closing(sqlite3.connect(path)) as db, db:
            db.execute('CREATE TABLE effects(event_id TEXT PRIMARY KEY, payload BLOB, n)')   # n: no affinity
            db.executemany('INSERT INTO effects VALUES (?, ?, ?)', rows)
            for statement in extra:
                db.execute(statement)
        return path

    def test_independent_of_insertion_order_and_rowids(self):
        rows = [('a', b'\x00\x01', 1.5), ('b', None, 2.0), ('c', b'', -0.0)]
        first = self.make('one.db', rows)
        second = self.make('two.db', list(reversed(rows)))
        self.assertEqual(digest_of(first), digest_of(second))

    def test_sensitive_to_rows_types_and_schema(self):
        base = digest_of(self.make('base.db', [('a', b'x', 1.0)]))
        variants = {
            'value': self.make('v1.db', [('a', b'y', 1.0)]),
            'type text vs blob': self.make('v2.db', [('a', 'x', 1.0)]),
            'float vs int': self.make('v3.db', [('a', b'x', 1)]),
            'extra row': self.make('v4.db', [('a', b'x', 1.0), ('b', b'x', 1.0)]),
            'index': self.make('v5.db', [('a', b'x', 1.0)], ['CREATE INDEX i ON effects(n)']),
            'trigger': self.make('v6.db', [('a', b'x', 1.0)],
                                 ["CREATE TRIGGER t BEFORE INSERT ON effects BEGIN SELECT RAISE(ABORT, 'x'); END"]),
            'view': self.make('v7.db', [('a', b'x', 1.0)], ['CREATE VIEW w AS SELECT * FROM effects']),
        }
        digests = {name: digest_of(path) for name, path in variants.items()}
        for name, value in digests.items():
            self.assertNotEqual(value, base, name)
        self.assertEqual(len(set(digests.values())), len(digests))


class VacuumInternalsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='vacuum_int_')
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)
        self.f.ingest(40)
        self.lane = self.f.deliver()[0]['lane']
        cr.migrate_consumer_v2(self.f.consumer, apply=True)
        cr.compact_consumer(self.f.consumer, lane=self.lane, content_watermark=30, producer_root=self.f.root,
                            segment_size=10, apply=True)
        self.digest = digest_of(self.f.consumer)
        self.raw = self.f.consumer.read_bytes()

    def files(self):
        return sorted(p.name for p in self.f.consumers.iterdir())

    def assert_untouched(self):
        self.assertEqual(digest_of(self.f.consumer), self.digest)
        self.assertEqual(self.files(), ['effects.sqlite3', 'effects.sqlite3.archive'])

    def test_failed_final_rename_is_undone(self):
        real, calls = os.rename, []

        def flaky(source, target):
            calls.append((source, target))
            if str(source).endswith('.tmp'):
                raise PermissionError('simulated: file in use')
            return real(source, target)
        with mock.patch.object(sm.os, 'rename', flaky):
            with self.assertRaisesRegex(ValueError, 'every rename was undone'):
                vacuum_db(self.f.consumer, apply=True)
        self.assert_untouched()
        self.assertEqual(self.f.consumer.read_bytes(), self.raw)

    def test_failed_undo_reports_where_the_original_is(self):
        real = os.rename
        state = {'fail_undo': False}

        def flaky(source, target):
            if str(source).endswith('.tmp'):
                state['fail_undo'] = True
                raise PermissionError('simulated: file in use')
            if state['fail_undo']:
                raise PermissionError('simulated: undo blocked')
            return real(source, target)
        with mock.patch.object(sm.os, 'rename', flaky):
            with self.assertRaisesRegex(ValueError, r'could not be undone.*the original database is at .*pre-vacuum-'):
                vacuum_db(self.f.consumer, apply=True)
        kept = [p for p in self.f.consumers.iterdir() if '.pre-vacuum-' in p.name and not p.name.endswith(('-wal', '-shm'))]
        self.assertEqual(len(kept), 1)
        self.assertEqual(digest_of(kept[0]), self.digest)   # nothing lost: the original is intact under its kept name
        self.assertFalse(any(p.name.endswith('.tmp') for p in self.f.consumers.iterdir()))

    def test_change_while_rebuilding_is_refused(self):
        real = sm._inspect

        def write_then_inspect(path):
            if str(path).endswith('.tmp'):
                with closing(sqlite3.connect(self.f.consumer)) as db, db:
                    db.execute("INSERT INTO consumer_meta(k, v) VALUES ('concurrent', '1')")
            return real(path)
        with mock.patch.object(sm, '_inspect', write_then_inspect):
            with self.assertRaisesRegex(ValueError, 'changed during vacuum'):
                vacuum_db(self.f.consumer, apply=True)
        self.assertEqual(self.files(), ['effects.sqlite3', 'effects.sqlite3.archive'])
        self.assertEqual(sql(self.f.consumer, "SELECT v FROM consumer_meta WHERE k = 'concurrent'"), [{'v': '1'}])

    def test_reader_with_open_transaction_blocks(self):
        reader = sqlite3.connect(self.f.consumer)
        try:
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM effects').fetchone()
            with sqlite3.connect(self.f.consumer) as writer:   # leave WAL frames behind the reader's snapshot
                writer.execute("INSERT INTO consumer_meta(k, v) VALUES ('w', '1')")
            writer.close()
            with self.assertRaisesRegex(ValueError, 'database in use'):
                vacuum_db(self.f.consumer, apply=True)
        finally:
            reader.close()
        self.assertFalse(any('.pre-vacuum-' in p.name or p.name.endswith('.tmp') for p in self.f.consumers.iterdir()))

    @unittest.skipUnless(os.name == 'nt', 'Windows file-sharing semantics')
    def test_idle_open_connection_on_windows_is_refused_without_loss(self):
        idle = sqlite3.connect(self.f.consumer)
        try:
            idle.execute('SELECT 1').fetchone()
            with self.assertRaisesRegex(ValueError, 'every rename was undone'):
                vacuum_db(self.f.consumer, apply=True)
        finally:
            idle.close()
        self.assertEqual(digest_of(self.f.consumer), self.digest)
        self.assertFalse(any('.pre-vacuum-' in p.name or p.name.endswith('.tmp') for p in self.f.consumers.iterdir()))

    def test_existing_kept_name_is_never_overwritten(self):
        frozen = (2026, 11, 7, 12, 0, 0, 5, 311, 0)
        kept = Path(f'{self.f.consumer}.pre-vacuum-20261107T120000Z')
        kept.write_bytes(b'someone else\'s file')
        with mock.patch.object(sm.time, 'gmtime', return_value=frozen):
            with self.assertRaisesRegex(ValueError, 'name collision'):
                vacuum_db(self.f.consumer, apply=True)
        self.assertEqual(kept.read_bytes(), b'someone else\'s file')
        self.assertEqual(self.f.consumer.read_bytes(), self.raw)
        kept.unlink()

    def test_insufficient_space_is_refused_before_any_change(self):
        usage = mock.Mock(return_value=mock.Mock(free=1024))
        with mock.patch.object(sm.shutil, 'disk_usage', usage):
            with self.assertRaisesRegex(ValueError, 'not enough free space'):
                vacuum_db(self.f.consumer, apply=True)
        self.assertEqual(self.f.consumer.read_bytes(), self.raw)

    def test_audit_and_tiered_consumer_after_vacuum(self):
        before = reconcile_archive_aware(self.f.root, self.f.consumer)['status']
        done = vacuum_db(self.f.consumer, apply=True)
        self.assertLess(done['bytes_after'], done['bytes_before'])
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], before)
        self.assertEqual(digest_of(done['kept_original']), self.digest)
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'compacted range'):   # trigger survives the rebuild
            sql(self.f.consumer, 'INSERT INTO effects(event_id, lane, seq, epoch, payload_sha256, payload) '
                                 "VALUES ('x', ?, 5, 0, 'h', '{}')", (self.lane,))


if __name__ == '__main__':
    unittest.main()
