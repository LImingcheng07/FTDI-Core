"""Independent black-box tests of docs/STORAGE_MAINTENANCE.md.

Never inspect storage_maintenance's implementation.  Import its public function
only when exercising it, so the fixture self-checks also run before it exists.
The sole implementation-dependent probe is the explicitly permitted shutil hook.

Interpretations: sqlite_master excludes physical rootpage numbers, as specified
by the logical-digest contract. Missing paths also permit FileNotFoundError,
the explicit exception in the written contract. No golden digest bytes
are invented: the canonical type-tag framing is not specified by the contract.
"""
from __future__ import annotations

from collections import namedtuple
from contextlib import closing, redirect_stderr, redirect_stdout
import importlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from test_archive_e2e import Fixture, sql, evidence_snapshot
from ftdi_core.adapters.s070041.external_delivery import EffectStore, envelope
from ftdi_core.archive_e2e import reconcile_archive_aware
from ftdi_core.consumer_retention import (
    TieredEffectStore, compact_consumer, migrate_consumer_v2,
)
from ftdi_core.producer_retention import archive_producer
from ftdi_core.service import replay


def vacuum(path, **kwargs):
    from ftdi_core.storage_maintenance import vacuum_db
    return vacuum_db(path, **kwargs)


def directory_bytes(directory):
    """Exact bytes and entry names, including empty directories; ignore only shm."""
    return {
        path.relative_to(directory).as_posix(): (
            path.read_bytes() if path.is_file() else None
        )
        for path in sorted(Path(directory).rglob('*'))
        if not path.name.endswith('-shm')
    }


def wal_size(path):
    wal = Path(f'{path}-wal')
    return wal.stat().st_size if wal.exists() else 0


def content_from_connection(db):
    """Full schema and full sorted rows, independent of the tool's digest."""
    schema = tuple(tuple(row) for row in db.execute(
        'SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name'
    ).fetchall())
    tables = {}
    for name in sorted(row[1] for row in schema if row[0] == 'table'):
        quoted = '"' + name.replace('"', '""') + '"'
        columns = db.execute(f'PRAGMA table_info({quoted})').fetchall()
        order = ', '.join(str(i) for i in range(1, len(columns) + 1))
        tables[name] = tuple(tuple(row) for row in db.execute(
            f'SELECT * FROM {quoted} ORDER BY {order}'
        ).fetchall())
    pragmas = {
        key: db.execute(f'PRAGMA {key}').fetchone()[0]
        for key in ('user_version', 'application_id', 'journal_mode')
    }
    return {'schema': schema, 'tables': tables, 'pragmas': pragmas}


def logical_content(path):
    # immutable avoids creating sidecars on a quiescent WAL-mode DB. A DB with
    # committed WAL frames must instead be opened with ordinary read-only mode.
    uri = Path(path).resolve().as_uri() + '?mode=ro'
    if wal_size(path) <= 32:
        uri += '&immutable=1'
    with closing(sqlite3.connect(uri, uri=True)) as db:
        result = content_from_connection(db)
    # immutable readers report DELETE even when the persisted header is WAL.
    # Read the two journal-format bytes without opening a mutating connection.
    with Path(path).open('rb') as stream:
        header = stream.read(20)
    result['pragmas']['journal_mode'] = (
        'wal' if header[18:20] == b'\x02\x02' else 'delete'
    )
    return result


def cli_call(path, *, apply=False):
    from ftdi_core.cli import main
    args = ['vacuum-db', '--db', str(path)]
    if apply:
        args.append('--apply')
    with io.StringIO() as stdout, io.StringIO() as stderr:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            try:
                code = main(args)
            except SystemExit as exc:
                code = exc.code
        return code, stdout.getvalue(), stderr.getvalue()


class FixtureBase(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='vacuum_contract_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name) / 'evidence_\u91cf\u5316'
        self.base.mkdir()
        self.f = Fixture(self.base)

    def compacted(self):
        self.assertTrue(self.f.ingest(60)['complete'])
        rows = self.f.deliver()
        self.assertEqual(len(rows), 60)
        self.events = [envelope(row) for row in rows]
        self.original_effect = sql(
            self.f.consumer, 'SELECT * FROM effects WHERE seq=1'
        )[0]
        self.assertEqual(migrate_consumer_v2(
            self.f.consumer, apply=True)['status'], 'MIGRATED')
        result = compact_consumer(
            self.f.consumer, lane=self.events[0]['lane'],
            content_watermark=54, producer_root=self.f.root,
            segment_size=18, apply=True,
        )
        self.assertEqual(result['status'], 'COMPACTED')
        self.assertEqual(result['rows'], 54)
        self.assertGreater(sql(self.f.consumer, 'PRAGMA freelist_count')[0]
                           ['freelist_count'], 0)
        return self.f.consumer

    def archived(self):
        self.assertTrue(self.f.ingest(60)['complete'])
        self.assertEqual(len(self.f.deliver()), 60)
        # Exercise both fixture prune and the public Core archive front-end.
        self.assertEqual(self.f.prune(18)['archived_new'], 18)
        result = archive_producer(
            self.f.root, 'BTC', consumer_db=self.f.consumer,
            limit=36, chunk=18, apply=True,
        )
        self.assertEqual(result['status'], 'ARCHIVED')
        self.assertEqual(result['archived_new'], 36)
        self.assertGreater(sql(self.f.db(), 'PRAGMA freelist_count')[0]
                           ['freelist_count'], 0)
        return self.f.db()

    def audit_status(self):
        return reconcile_archive_aware(self.f.root, self.f.consumer)['status']

    def wal_copy(self):
        """Copy committed main+WAL while writer is alive, then close the writer.

        The copy has real frames but no open handles, so Windows must support
        apply successfully. The idle-handle test is deliberately separate.
        """
        self.f.ingest(12)
        self.f.deliver(limit=6)
        destination = self.base / 'wal_copy'
        destination.mkdir()
        path = destination / 'effects.sqlite3'
        with closing(EffectStore(self.f.consumer)) as writer:
            writer.db.execute('PRAGMA wal_autocheckpoint=0')
            self.f.deliver(limit=6, store=writer)
            self.assertFalse(writer.db.in_transaction)
            expected = content_from_connection(writer.db)
            self.assertEqual(len(expected['tables']['effects']), 12)
            uri = self.f.consumer.resolve().as_uri() + '?immutable=1'
            with closing(sqlite3.connect(uri, uri=True)) as main_only:
                self.assertEqual(main_only.execute(
                    'SELECT COUNT(*) FROM effects').fetchone()[0], 6)
            self.assertGreater(wal_size(self.f.consumer), 32)
            shutil.copyfile(self.f.consumer, path)
            shutil.copyfile(f'{self.f.consumer}-wal', f'{path}-wal')
        self.assertGreater(wal_size(path), 32)
        return path, expected


class FixtureSelfChecks(FixtureBase):
    """These checks run even when storage_maintenance is not implemented yet."""

    def test_compacted_fixture_has_freed_pages_and_complete_audit(self):
        path = self.compacted()
        content = logical_content(path)
        self.assertEqual(len(content['tables']['effects']), 6)
        self.assertEqual(len(content['tables']['effect_digests']), 54)
        self.assertTrue(any(row[0] == 'trigger' for row in content['schema']))
        self.assertEqual(self.audit_status(), 'COMPLETE')

    def test_archived_fixture_has_freed_pages_and_complete_audit(self):
        path = self.archived()
        content = logical_content(path)
        self.assertEqual(len(content['tables']['raw_journal']), 6)
        self.assertEqual(len(content['tables']['outbox']), 6)
        self.assertEqual(self.audit_status(), 'COMPLETE')

    def test_wal_fixture_contains_rows_absent_from_main_file(self):
        path, expected = self.wal_copy()
        self.assertEqual(logical_content(path), expected)


class StorageMaintenanceContractTests(FixtureBase):
    def assert_digest(self, result):
        self.assertRegex(result['logical_digest'], r'\A[0-9a-fA-F]{64}\Z')

    def assert_dry_run(self, path, kind, *, require_free=True):
        expected = logical_content(path)
        before = directory_bytes(self.base)
        with closing(sqlite3.connect(
                path.resolve().as_uri() + '?mode=ro' +
                ('&immutable=1' if wal_size(path) <= 32 else ''), uri=True)) as db:
            pages = {key: db.execute(f'PRAGMA {key}').fetchone()[0]
                     for key in ('page_size', 'page_count', 'freelist_count')}
        # Include the independent observer in the read-only assertion too.
        self.assertEqual(directory_bytes(self.base), before)
        results = []
        for surface in ('api', 'cli'):
            with self.subTest(surface=surface):
                if surface == 'api':
                    result = vacuum(str(path))  # Omitted apply must mean dry run.
                else:
                    code, stdout, stderr = cli_call(path)
                    self.assertEqual(code, 0, stderr)
                    self.assertEqual(stderr, '')
                    self.assertTrue(stdout.isascii(), repr(stdout))
                    result = json.loads(stdout)
                self.assertEqual(result['status'], 'DRY_RUN')
                self.assertEqual(result['kind'], kind)
                self.assertEqual(result['db'], str(path))
                self.assertIs(result['changed'], False)
                self.assertIs(result['apply_required'], True)
                self.assert_digest(result)
                for key, value in pages.items():
                    self.assertEqual(result[key], value)
                if require_free:
                    self.assertGreater(result['freelist_count'], 0)
                self.assertEqual(result['reclaimable_bytes_estimate'],
                                 pages['freelist_count'] * pages['page_size'])
                self.assertEqual(result['file_bytes'], path.stat().st_size)
                self.assertEqual(result['wal_bytes'], wal_size(path))
                self.assertGreaterEqual(result['space_needed_bytes'],
                    (pages['page_count'] - pages['freelist_count']) * pages['page_size'])
                self.assertGreaterEqual(result['space_free_bytes'], 0)
                self.assertEqual(directory_bytes(self.base), before)
                self.assertEqual(logical_content(path), expected)
                self.assertEqual(directory_bytes(self.base), before)
                results.append(result)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['logical_digest'], results[1]['logical_digest'])
        return results[0]

    def assert_vacuumed(self, result, path, kind, expected, *, bytes_before=None):
        self.assertEqual(result['status'], 'VACUUMED')
        self.assertEqual(result['kind'], kind)
        self.assertEqual(result['db'], str(path))
        self.assertIs(result['changed'], True)
        self.assert_digest(result)
        self.assertEqual(result['bytes_after'], path.stat().st_size)
        kept = Path(result['kept_original'])
        self.assertEqual(kept.parent.resolve(), path.parent.resolve())
        self.assertRegex(kept.name, r'\A' + re.escape(path.name) +
                         r'\.pre-vacuum-\d{8}T\d{6}Z\Z')
        self.assertTrue(kept.is_file())
        # With WAL, the kept main file can grow during the required checkpoint.
        # bytes_before means the main-file size before invoking the command.
        self.assertEqual(result['bytes_before'],
                         kept.stat().st_size if bytes_before is None else bytes_before)
        self.assertIsInstance(result['note'], str)
        self.assertTrue(result['note'].strip())
        self.assertEqual(logical_content(kept), expected)
        self.assertEqual(logical_content(path), expected)
        with closing(sqlite3.connect(
                path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0],
                             expected['pragmas']['journal_mode'])
        return kept

    def assert_refused(self, path, *, readable=False, byte_identical=True,
                       modes=(True,), message=None, exception=ValueError):
        expected = logical_content(path) if readable else None
        before = directory_bytes(self.base)
        for apply in modes:
            for surface in ('api', 'cli'):
                with self.subTest(apply=apply, surface=surface):
                    if surface == 'api':
                        with self.assertRaises(exception) as caught:
                            vacuum(str(path), apply=apply)
                        if message:
                            self.assertIn(message, str(caught.exception).lower())
                    else:
                        code, stdout, stderr = cli_call(path, apply=apply)
                        self.assertEqual(code, 3, (stdout, stderr))
                        self.assertIn('FTDI_CORE_FAIL_CLOSED', stderr)
                        if message:
                            self.assertIn(message, stderr.lower())
                        if stdout.strip():
                            self.assertTrue(stdout.isascii())
                            json.loads(stdout)
                    after = directory_bytes(self.base)
                    self.assertEqual(set(after) - set(before), set(),
                                     'Refusal left new files/directories')
                    if byte_identical:
                        self.assertEqual(after, before)
                    if readable:
                        self.assertTrue(path.is_file(), 'Refusal lost PATH')
                        self.assertEqual(logical_content(path), expected)

    def test_dry_run_compacted_consumer_api_and_cli(self):
        self.assert_dry_run(self.compacted(), 'consumer')

    def test_dry_run_archived_producer_api_and_cli(self):
        self.assert_dry_run(self.archived(), 'producer')

    def test_dry_run_v1_consumer_api_and_cli(self):
        self.f.ingest(12)
        self.f.deliver()
        self.assert_dry_run(self.f.consumer, 'consumer', require_free=False)

    def test_dry_run_with_committed_wal_preserves_every_byte(self):
        path, _ = self.wal_copy()
        self.assert_dry_run(path, 'consumer', require_free=False)

    def test_dry_run_wal_without_shm_is_refused_and_creates_nothing(self):
        # Contract clarified after this suite was written (docs/STORAGE_MAINTENANCE.md, "Dry run"): a missing -shm
        # is never created; the dry run refuses and points to --apply. Adjusted by Claude; the original version
        # expected the file to be created.
        path, expected = self.wal_copy()
        self.assertFalse(Path(f'{path}-shm').exists())
        before = directory_bytes(self.base)
        for surface in ('api', 'cli'):
            with self.subTest(surface=surface):
                if surface == 'api':
                    with self.assertRaisesRegex(ValueError, r'WAL frames without -shm.*--apply'):
                        vacuum(str(path))
                else:
                    code, stdout, stderr = cli_call(path)
                    self.assertEqual(code, 3)
                    self.assertIn('FTDI_CORE_FAIL_CLOSED', stderr)
                    self.assertEqual(stdout, '')
                self.assertFalse(Path(f'{path}-shm').exists())
                self.assertEqual(directory_bytes(self.base), before)
        self.assertEqual(logical_content(path), expected)

    def test_apply_compacted_consumer_preserves_audit_tiers_and_guard(self):
        path = self.compacted()
        before_status = self.audit_status()
        self.assertEqual(before_status, 'COMPLETE')
        expected = logical_content(path)
        self.assertEqual(wal_size(path), 0)
        original_bytes = path.read_bytes()
        protected = evidence_snapshot(self.f.root, self.f.sources,
                                      Path(f'{path}.archive'))
        dry = vacuum(str(path))
        result = vacuum(str(path), apply=True)
        kept = self.assert_vacuumed(result, path, 'consumer', expected)
        self.assertEqual(result['bytes_before'], len(original_bytes))
        self.assertLess(result['bytes_after'], result['bytes_before'])
        self.assertEqual(kept.read_bytes(), original_bytes)
        self.assertEqual(result['logical_digest'], dry['logical_digest'])
        self.assertEqual(vacuum(str(path))['logical_digest'], dry['logical_digest'])
        self.assertEqual(vacuum(str(kept))['logical_digest'], dry['logical_digest'])
        self.assertEqual(self.audit_status(), before_status)
        self.assertTrue(kept.is_file())  # Audit must tolerate the kept file.
        with closing(TieredEffectStore(path)) as store:
            duplicate = store.accept(self.events[:54])
        self.assertEqual(duplicate['duplicates'], 54)
        self.assertEqual(duplicate['inserted'], 0)
        self.assertEqual(duplicate['accepted'], 54)
        self.assertEqual(logical_content(path), expected)
        row = self.original_effect
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'compacted'):
            sql(path, 'INSERT INTO effects '
                '(event_id,lane,seq,epoch,payload_sha256,payload) VALUES (?,?,?,?,?,?)',
                tuple(row[key] for key in
                      ('event_id', 'lane', 'seq', 'epoch', 'payload_sha256', 'payload')))
        self.assertEqual(logical_content(path), expected)
        self.assertEqual(evidence_snapshot(self.f.root, self.f.sources,
                                          Path(f'{path}.archive')), protected)

    def test_apply_producer_preserves_audit_and_resume_without_new_rows(self):
        path = self.archived()
        before_status = self.audit_status()
        self.assertEqual(before_status, 'COMPLETE')
        expected = logical_content(path)
        original_bytes = path.read_bytes()
        protected = evidence_snapshot(self.f.sources, self.f.consumers,
                                      self.f.root / 'archives')
        registry = (self.f.root / 'lane_registry.json').read_bytes()
        dry = vacuum(str(path))
        result = vacuum(str(path), apply=True)
        kept = self.assert_vacuumed(result, path, 'producer', expected)
        self.assertLess(result['bytes_after'], result['bytes_before'])
        self.assertEqual(kept.read_bytes(), original_bytes)
        self.assertEqual(result['logical_digest'], dry['logical_digest'])
        self.assertEqual(self.audit_status(), before_status)
        self.assertEqual(evidence_snapshot(self.f.sources, self.f.consumers,
                                          self.f.root / 'archives'), protected)
        self.assertEqual((self.f.root / 'lane_registry.json').read_bytes(), registry)
        resumed = replay(self.f.root, [f'BTC={self.f.source}'], batch=4, max_pending=60)
        self.assertTrue(resumed['complete'])
        after = logical_content(path)
        for table in ('outbox', 'raw_journal'):
            self.assertEqual(len(after['tables'][table]), len(expected['tables'][table]))
            self.assertEqual(after['tables'][table], expected['tables'][table])
        self.assertEqual(self.audit_status(), before_status)

    def test_apply_cli_outputs_ascii_json_and_keeps_original(self):
        path = self.compacted()
        expected = logical_content(path)
        code, stdout, stderr = cli_call(path, apply=True)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(stderr, '')
        self.assertTrue(stdout.isascii(), repr(stdout))
        result = json.loads(stdout)
        self.assert_vacuumed(result, path, 'consumer', expected)
        self.assertLess(result['bytes_after'], result['bytes_before'])

    def test_apply_includes_rows_that_exist_only_in_wal(self):
        path, expected = self.wal_copy()
        self.assertGreater(wal_size(path), 32)
        bytes_before = path.stat().st_size
        # Apply independently: a dry-run refusal must not mask this guarantee.
        result = vacuum(str(path), apply=True)
        self.assert_vacuumed(result, path, 'consumer', expected,
                             bytes_before=bytes_before)
        self.assertEqual(result['logical_digest'],
                         vacuum(str(path))['logical_digest'])
        self.assertEqual(len(logical_content(path)['tables']['effects']), 12)

    def test_apply_wal_with_shm_preserves_all_rows_and_dry_run_digest(self):
        path, expected = self.wal_copy()
        # A read-only SQLite reader establishes coordination state and closes.
        # Committed frames remain, without a writer handle blocking Windows swap.
        self.assertEqual(logical_content(path), expected)
        self.assertTrue(Path(f'{path}-shm').is_file())
        self.assertGreater(wal_size(path), 32)
        bytes_before = path.stat().st_size
        dry = vacuum(str(path))
        result = vacuum(str(path), apply=True)
        self.assert_vacuumed(result, path, 'consumer', expected,
                             bytes_before=bytes_before)
        self.assertEqual(result['logical_digest'], dry['logical_digest'])

    def test_apply_preserves_nonzero_user_version_and_application_id(self):
        self.f.ingest(12)
        self.f.deliver()
        path = self.f.consumer
        sql(path, 'PRAGMA user_version=37')
        sql(path, 'PRAGMA application_id=1179927625')
        expected = logical_content(path)
        self.assertEqual(expected['pragmas']['user_version'], 37)
        self.assertEqual(expected['pragmas']['application_id'], 1179927625)
        self.assert_vacuumed(vacuum(str(path), apply=True), path, 'consumer', expected)

    def test_digest_changes_for_rows_and_additive_schema(self):
        self.f.ingest(12)
        self.f.deliver(limit=6)
        first = vacuum(str(self.f.consumer))
        self.f.deliver(limit=6)
        second = vacuum(str(self.f.consumer))
        self.assertNotEqual(first['logical_digest'], second['logical_digest'])
        migrate_consumer_v2(self.f.consumer, apply=True)
        third = vacuum(str(self.f.consumer))
        self.assertNotEqual(second['logical_digest'], third['logical_digest'])
        for result in (first, second, third):
            self.assert_digest(result)

    def test_refuses_non_ftdi_sqlite_database(self):
        path = self.base / 'unrelated.sqlite3'
        sql(path, 'CREATE TABLE unrelated(value TEXT)')
        sql(path, 'INSERT INTO unrelated VALUES (?)', ('keep me',))
        self.assert_refused(path, readable=True, modes=(False, True))

    def test_refuses_non_sqlite_file(self):
        path = self.base / 'not_sqlite.db'
        path.write_bytes(b'not a SQLite database\x00\xff\n')
        self.assert_refused(path, modes=(False, True))

    def test_refuses_missing_path_without_creating_it(self):
        self.assert_refused(self.base / 'missing.sqlite3', modes=(False, True),
                            exception=(ValueError, FileNotFoundError))

    def test_refuses_directory(self):
        path = self.base / 'directory.sqlite3'
        path.mkdir()
        (path / 'sentinel').write_bytes(b'preserve this directory')
        self.assert_refused(path, modes=(False, True))

    def test_refuses_rollback_journal_without_recovering_or_deleting_it(self):
        path = self.compacted()
        Path(f'{path}-journal').write_bytes(b'journal sentinel: do not touch')
        self.assert_refused(path, readable=True, modes=(False, True))

    def test_refuses_wal_reader_pinning_checkpoint(self):
        self.f.ingest(12)
        self.f.deliver(limit=6)
        path = self.f.consumer
        with closing(EffectStore(path)) as writer:
            writer.db.execute('PRAGMA wal_autocheckpoint=0')
            with closing(sqlite3.connect(path)) as reader:
                reader.execute('BEGIN')
                self.assertEqual(reader.execute(
                    'SELECT COUNT(*) FROM effects').fetchone()[0], 6)
                self.f.deliver(limit=6, store=writer)
                self.assertGreater(wal_size(path), 32)
                self.assertTrue(reader.in_transaction)
                # Step 1 may fold some WAL pages before discovering the pinned
                # reader. Only logical equality and scratch cleanup are promised.
                self.assert_refused(path, readable=True, byte_identical=False,
                                    message='database in use')
                self.assertEqual(reader.execute(
                    'SELECT COUNT(*) FROM effects').fetchone()[0], 6)
                reader.rollback()
        self.assertEqual(len(logical_content(path)['tables']['effects']), 12)

    def test_refuses_insufficient_space_without_changing_database(self):
        module = importlib.import_module('ftdi_core.storage_maintenance')
        if not hasattr(module, 'shutil') or not hasattr(module.shutil, 'disk_usage'):
            self.skipTest('Implementation does not import shutil.disk_usage; '
                          'the only authorized fault-injection hook is unavailable')
        path = self.compacted()
        usage = namedtuple('Usage', 'total used free')(1 << 30, 1 << 30, 0)
        with patch('ftdi_core.storage_maintenance.shutil.disk_usage',
                   return_value=usage) as disk_usage:
            self.assert_refused(path, readable=True)
            self.assertTrue(disk_usage.called)

    def test_kept_original_same_second_collision_is_not_overwritten(self):
        self.skipTest('No public clock/timestamp injection is specified; a real-time '
                      'same-second collision cannot be controlled deterministically '
                      'without inspecting or patching undocumented implementation')

    @unittest.skipUnless(os.name == 'nt', 'Requires native Windows open-handle semantics')
    def test_windows_idle_connection_never_loses_path_or_rows(self):
        path = self.compacted()
        expected = logical_content(path)
        with closing(sqlite3.connect(path)) as idle:
            idle.execute('SELECT * FROM effects').fetchall()
            self.assertFalse(idle.in_transaction)
            before = directory_bytes(self.base)
            result = None
            try:
                result = vacuum(str(path), apply=True)
            except ValueError:
                self.assertTrue(path.is_file(), 'Failed Windows rename lost PATH')
                self.assertEqual(directory_bytes(self.base), before)
            self.assertTrue(path.is_file())
            self.assertEqual(logical_content(path), expected)
        if result is not None:
            self.assert_vacuumed(result, path, 'consumer', expected)
        self.assertEqual(logical_content(path), expected)

    def test_second_apply_keeps_two_distinct_originals(self):
        path = self.compacted()
        expected = logical_content(path)
        first = vacuum(str(path), apply=True)
        kept_first = self.assert_vacuumed(first, path, 'consumer', expected)
        first_bytes = kept_first.read_bytes()
        now_bytes = path.read_bytes()
        # Names have second precision; collision refusal is separately specified.
        # Cross a full second rather than make this test depend on machine speed.
        time.sleep(1.1)
        second = vacuum(str(path), apply=True)
        kept_second = self.assert_vacuumed(second, path, 'consumer', expected)
        self.assertNotEqual(kept_first, kept_second)
        self.assertEqual(kept_first.read_bytes(), first_bytes)
        self.assertEqual(kept_second.read_bytes(), now_bytes)
        self.assertEqual(first['logical_digest'], second['logical_digest'])
        self.assertEqual(len(list(path.parent.glob(path.name + '.pre-vacuum-*'))), 2)


if __name__ == '__main__':
    unittest.main()
