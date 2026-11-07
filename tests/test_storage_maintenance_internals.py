"""vacuum-db internals (ADR-010): logical digest framing and ordering, exclusive lock, verified backup before the
first write, sidecar/link validation, failure handling. Includes regressions for the independent review V1-V9."""
from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from test_archive_e2e import Fixture, sql
from ftdi_core import consumer_retention as cr
from ftdi_core import storage_maintenance as sm
from ftdi_core.archive_e2e import reconcile_archive_aware
from ftdi_core.cli import main
from ftdi_core.storage_maintenance import logical_digest, vacuum_db


def digest_of(path):
    # immutable: these databases have no WAL frames here, and mode=ro would leave -wal/-shm sidecars behind
    with closing(sqlite3.connect(f'{Path(path).as_uri()}?mode=ro&immutable=1', uri=True)) as db:
        return logical_digest(db)


def make_db(path, statements, rows=()):
    with closing(sqlite3.connect(path)) as db, db:
        for statement in statements:
            db.execute(statement)
        for table, values in rows:
            db.execute(f'INSERT INTO {table} VALUES ({",".join("?" * len(values))})', values)
    return path


class LogicalDigestTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='digest_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.n = 0

    def db(self, statements, rows=()):
        self.n += 1   # numbered names: Windows file names are case-insensitive
        return make_db(self.base / f'd{self.n}.db', statements, rows)

    def test_independent_of_insertion_order(self):
        table = ['CREATE TABLE effects(event_id TEXT PRIMARY KEY, payload BLOB, n)']
        rows = [('effects', ('a', b'\x00\x01', 1.5)), ('effects', ('b', None, 2)), ('effects', ('c', b'', 'x'))]
        self.assertEqual(digest_of(self.db(table, rows)), digest_of(self.db(table, list(reversed(rows)))))

    def test_v8_order_ties_under_collations_and_numeric_equality(self):
        for column, values in (('x TEXT COLLATE NOCASE', ('a', 'A')), ('x TEXT COLLATE RTRIM', ('a', 'a ')),
                               ('x', (1, 1.0))):
            with self.subTest(column=column):
                statements = [f'CREATE TABLE t({column})']
                first = self.db(statements, [('t', (v,)) for v in values])
                second = self.db(statements, [('t', (v,)) for v in reversed(values)])
                self.assertEqual(digest_of(first), digest_of(second))

    def test_v4_a_row_never_looks_like_a_table_boundary(self):
        statements = ['CREATE TABLE effects(a, b, c)', 'CREATE TABLE z(a, b, c)']
        moved_a = self.db(statements, [('effects', ('TABLE', 'z', 3))])
        moved_b = self.db(statements, [('z', ('TABLE', 'z', 3))])
        self.assertNotEqual(digest_of(moved_a), digest_of(moved_b))

    def test_sensitive_to_values_types_and_schema(self):
        table = 'CREATE TABLE effects(event_id TEXT PRIMARY KEY, payload BLOB, n)'
        base = digest_of(self.db([table], [('effects', ('a', b'x', 1.0))]))
        variants = {
            'value': self.db([table], [('effects', ('a', b'y', 1.0))]),
            'text vs blob': self.db([table], [('effects', ('a', 'x', 1.0))]),
            'float vs int': self.db([table], [('effects', ('a', b'x', 1))]),
            'null vs empty': self.db([table], [('effects', ('a', b'', 1.0))]),
            'extra row': self.db([table], [('effects', ('a', b'x', 1.0)), ('effects', ('b', b'x', 1.0))]),
            'index': self.db([table, 'CREATE INDEX i ON effects(n)'], [('effects', ('a', b'x', 1.0))]),
            'trigger': self.db([table, "CREATE TRIGGER t BEFORE INSERT ON effects WHEN NEW.event_id = 'never' "
                                       "BEGIN SELECT RAISE(ABORT, 'x'); END"],
                               [('effects', ('a', b'x', 1.0))]),
            'view': self.db([table, 'CREATE VIEW w AS SELECT * FROM effects'], [('effects', ('a', b'x', 1.0))]),
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

    def entries(self):
        return sorted(p.name for p in self.f.consumers.iterdir() if not p.name.endswith('-shm'))

    def kept(self):
        return [p for p in self.f.consumers.iterdir() if '.pre-vacuum-' in p.name]

    def assert_untouched(self):
        self.assertEqual(self.f.consumer.read_bytes(), self.raw)
        self.assertEqual(self.entries(), ['effects.sqlite3', 'effects.sqlite3.archive'])

    def test_success_keeps_a_verified_copy_and_everything_works(self):
        before = reconcile_archive_aware(self.f.root, self.f.consumer)['status']
        done = vacuum_db(self.f.consumer, apply=True)
        self.assertLess(done['bytes_after'], done['bytes_before'])
        self.assertEqual(Path(done['kept_original']).read_bytes(), self.raw)
        self.assertEqual(digest_of(self.f.consumer), self.digest)
        self.assertEqual(reconcile_archive_aware(self.f.root, self.f.consumer)['status'], before)
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'compacted range'):   # trigger survives
            sql(self.f.consumer, 'INSERT INTO effects(event_id, lane, seq, epoch, payload_sha256, payload) '
                                 "VALUES ('x', ?, 5, 0, 'h', '{}')", (self.lane,))
        self.assertEqual(sorted(p.name for p in self.f.consumers.iterdir()),
                         sorted(['effects.sqlite3', 'effects.sqlite3.archive', Path(done['kept_original']).name]))

    def test_any_other_open_connection_refuses(self):
        idle = sqlite3.connect(self.f.consumer)
        try:
            idle.execute('SELECT 1 FROM effects LIMIT 1').fetchall()   # idle, no transaction
            with self.assertRaisesRegex(ValueError, 'database in use'):
                vacuum_db(self.f.consumer, apply=True)
        finally:
            idle.close()
        self.assert_untouched()

    def test_other_process_holding_the_database_refuses(self):
        code = ('import sqlite3, sys; c = sqlite3.connect(sys.argv[1]); '
                'c.execute("SELECT COUNT(*) FROM sqlite_master").fetchall(); '
                'print("ready", flush=True); sys.stdin.readline(); c.close()')
        holder = subprocess.Popen([sys.executable, '-c', code, str(self.f.consumer)], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(holder.stdout.readline().strip(), 'ready')
            with self.assertRaisesRegex(ValueError, 'database in use'):
                vacuum_db(self.f.consumer, apply=True)
        finally:
            holder.communicate('\n', timeout=30)
        self.assert_untouched()

    def test_corrupt_backup_copy_refuses_before_the_original_is_touched(self):
        real = sm._copy_exclusive

        def corrupting(source, target):
            result = real(source, target)
            with open(target, 'r+b') as handle:
                handle.seek(200)
                handle.write(b'\xff')
            return result
        with mock.patch.object(sm, '_copy_exclusive', corrupting):
            with self.assertRaisesRegex(ValueError, 'backup copy differs'):
                vacuum_db(self.f.consumer, apply=True)
        self.assert_untouched()   # our own corrupt copy was removed, the original untouched

    def test_interruption_inside_the_copy_never_deletes_an_unproven_file(self):
        real = sm._copy_exclusive

        def interrupted(source, target):
            real(source, target)
            raise KeyboardInterrupt   # identity of the new file never returned to the caller
        with mock.patch.object(sm, '_copy_exclusive', interrupted):
            with self.assertRaises(KeyboardInterrupt):
                vacuum_db(self.f.consumer, apply=True)
        self.assertEqual(len(self.kept()), 1)   # left in place: never delete what cannot be proven ours
        self.assertEqual(self.kept()[0].read_bytes(), self.raw)
        self.assertEqual(self.f.consumer.read_bytes(), self.raw)

    def test_v6_failure_after_vacuum_reports_the_verified_backup(self):
        real, calls = sm.logical_digest, []

        def wrong_after(db):
            calls.append(1)
            value = real(db)
            return value if len(calls) < 3 else '0' * 64   # 1 before, 2 backup check, 3 after VACUUM
        with mock.patch.object(sm, 'logical_digest', wrong_after):
            with self.assertRaisesRegex(ValueError, r'verified backup of the original is kept at .*pre-vacuum-'):
                vacuum_db(self.f.consumer, apply=True)
        self.assertEqual(len(self.kept()), 1)
        self.assertEqual(self.kept()[0].read_bytes(), self.raw)
        self.assertEqual(digest_of(self.f.consumer), self.digest)   # VACUUM itself kept every row

    def test_v1_sidecar_hard_linked_to_the_database_refuses_before_any_write(self):
        os.link(self.f.consumer, f'{self.f.consumer}-wal')
        try:
            for apply in (False, True):
                with self.assertRaisesRegex(ValueError, 'hard link'):
                    vacuum_db(self.f.consumer, apply=apply)
            self.assertEqual(self.f.consumer.read_bytes(), self.raw)
        finally:
            os.unlink(f'{self.f.consumer}-wal')

    def test_v1_sidecar_hard_linked_to_an_unrelated_file_refuses(self):
        unrelated = self.f.consumers / 'unrelated.bin'
        unrelated.write_bytes(b'precious' * 100)
        os.link(unrelated, f'{self.f.consumer}-shm')
        try:
            with self.assertRaisesRegex(ValueError, 'hard-linked'):
                vacuum_db(self.f.consumer, apply=True)
            self.assertEqual(unrelated.read_bytes(), b'precious' * 100)
        finally:
            os.unlink(f'{self.f.consumer}-shm')
            unrelated.unlink()
        self.assert_untouched()

    def test_v1_non_wal_file_at_the_wal_name_is_left_alone(self):
        sentinel = Path(f'{self.f.consumer}-wal')
        sentinel.write_bytes(b'not a write-ahead log, but somebody else\'s file')
        try:
            for apply in (False, True):
                with self.assertRaisesRegex(ValueError, 'not a SQLite WAL file'):
                    vacuum_db(self.f.consumer, apply=apply)
            self.assertEqual(sentinel.read_bytes(), b'not a write-ahead log, but somebody else\'s file')
        finally:
            sentinel.unlink()
        self.assert_untouched()

    @unittest.skipUnless(os.name == 'nt', 'directory junctions are a Windows feature')
    def test_v7_parent_junction_refuses(self):
        junction = self.f.base / 'junction_alias'
        done = subprocess.run(['cmd', '/c', 'mklink', '/J', str(junction), str(self.f.consumers)],
                              capture_output=True, text=True)
        if done.returncode != 0:
            self.skipTest(f'cannot create a junction here: {done.stderr.strip()}')
        try:
            with self.assertRaisesRegex(ValueError, 'link or junction'):
                vacuum_db(junction / 'effects.sqlite3', apply=True)
        finally:
            os.rmdir(junction)
        self.assert_untouched()

    def test_v3_existing_or_late_name_is_never_overwritten_or_deleted(self):
        frozen = (2026, 11, 7, 12, 0, 0, 5, 311, 0)
        kept = Path(f'{self.f.consumer}.pre-vacuum-20261107T120000Z')
        kept.write_bytes(b'someone else\'s file')
        with mock.patch.object(sm.time, 'gmtime', return_value=frozen):
            with self.assertRaisesRegex(ValueError, 'name collision'):
                vacuum_db(self.f.consumer, apply=True)
            self.assertEqual(kept.read_bytes(), b'someone else\'s file')
            kept.unlink()
            real = sm._copy_exclusive

            def late(source, target):   # the name appears after the collision check
                Path(target).write_bytes(b'appeared meanwhile')
                return real(source, target)
            with mock.patch.object(sm, '_copy_exclusive', late):
                with self.assertRaises(FileExistsError):
                    vacuum_db(self.f.consumer, apply=True)
        self.assertEqual(kept.read_bytes(), b'appeared meanwhile')
        kept.unlink()
        self.assert_untouched()

    def test_insufficient_space_is_refused_before_any_change(self):
        usage = mock.Mock(return_value=mock.Mock(free=1024))
        with mock.patch.object(sm.shutil, 'disk_usage', usage):
            with self.assertRaisesRegex(ValueError, 'not enough free space'):
                vacuum_db(self.f.consumer, apply=True)
        self.assert_untouched()

    def test_v9_cli_refusals_exit_3(self):
        for args in (['vacuum-db', '--db', str(self.f.consumers / 'absent.sqlite3'), '--apply'],
                     ['vacuum-db', '--db', str(self.f.consumers), '--apply']):
            with self.subTest(args=args[2]):
                with mock.patch('sys.stderr'), mock.patch('sys.stdout'):
                    self.assertEqual(main(args), 3)
        self.assert_untouched()


if __name__ == '__main__':
    unittest.main()
