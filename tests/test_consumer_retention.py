"""Adversarial black-box contract tests for docs/CONSUMER_RETENTION_V2.md.

Neither consumer implementation module is read, inspected, mocked, or patched.
Only documented APIs and independent SQL/file observations are used.

Interpretations where the contract needs clarification:
* Refusal preconditions are checked before any segment commits, even when the
  offending row or file belongs to a later segment; both dry run and apply refuse.
* A partial rehydrate target inside a segment restores that entire segment:
  C becomes the greatest retained segment end not greater than the target.
* CLI refusal diagnostics are on stderr; ASCII JSON is required on successful
  and dry-run stdout. An empty refusal stdout is permitted.
* Report fields not specified by the contract are not guessed. Exact operation
  counts are checked in SQL/files and the documented audit/accept metrics.
* The v2 contract supersedes the older proposed design's schema/table names.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from copy import deepcopy
import gzip
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import zlib

from test_archive_e2e import Fixture, sql, evidence_snapshot
from ftdi_core.adapters.s070041.external_delivery import (
    Conflict, EffectStore, envelope,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EFFECT_COLUMNS = ("event_id", "lane", "seq", "epoch", "payload_sha256", "payload")
V2_COLUMNS = {
    "consumer_meta": ("k", "v"),
    "lane_ids": ("lane_id", "lane"),
    "retention_state": ("lane", "epoch", "first_seq", "content_watermark",
                        "digest_watermark", "segments", "chain_head"),
    "effect_digests": ("lane_id", "epoch", "seq", "payload_sha256"),
    "effect_segments": ("lane", "epoch", "start_seq", "end_seq", "record_count",
                        "relative_path", "sha256", "prev_chain", "chain"),
}


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def chain_hash(segment):
    return sha256(canonical([
        "FTDI-Consumer-Segment-v1", segment["prev_chain"], segment["lane"],
        segment["epoch"], segment["start_seq"], segment["end_seq"],
        segment["record_count"], segment["sha256"],
    ]).encode("utf-8"))


def archive_root(consumer):
    return Path(str(consumer) + ".archive")


def relative_segment(lane_id, start, end, epoch=0):
    return (f"lane-{lane_id}/effects_e{epoch}_"
            f"{start:012d}_{end:012d}.jsonl.gz")


def gzip_records(records):
    with io.BytesIO() as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as stream:
            stream.write("".join(canonical(row) + "\n" for row in records).encode("utf-8"))
        return raw.getvalue()


def unpack_records(data):
    # gzip.decompress accepts concatenated members: explicitly forbid them.
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    plain = decoder.decompress(data) + decoder.flush()
    if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise AssertionError("expected exactly one complete gzip member")
    if data[:3] != b"\x1f\x8b\x08" or data[3] & 8 or data[4:8] != b"\0" * 4:
        raise AssertionError("expected gzip mtime=0 and empty filename")
    records = [json.loads(line) for line in plain.splitlines()]
    expected = "".join(canonical(row) + "\n" for row in records).encode("utf-8")
    if plain != expected:
        raise AssertionError("noncanonical JSONL or missing final newline")
    return records


def effects(consumer):
    return sql(consumer, "SELECT * FROM effects ORDER BY lane,seq,epoch,event_id")


def segments(consumer):
    return sql(consumer, "SELECT * FROM effect_segments ORDER BY lane,epoch,start_seq")


def database_rows(consumer):
    """Read schema and all rows without creating SQLite sidecars."""
    uri = Path(consumer).resolve().as_uri() + "?mode=ro&immutable=1"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        schema = db.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        ).fetchall()
        tables = [row[1] for row in schema if row[0] == "table"]
        rows = {}
        for name in tables:
            quoted = '"' + name.replace('"', '""') + '"'
            rows[name] = sorted(db.execute("SELECT * FROM " + quoted).fetchall(), key=repr)
        return schema, rows


def snapshot(f):
    """Actual bytes, inventory hashes, and logical rows: no count-only comparison."""
    return (
        evidence_snapshot(f.base),
        {path.relative_to(f.base).as_posix(): path.read_bytes()
         for path in sorted(f.base.rglob("*")) if path.is_file()},
        database_rows(f.consumer),
        tuple(path.relative_to(f.base).as_posix()
              for path in sorted(f.base.rglob("*")) if path.is_dir()),
    )


def event_from_row(row):
    result = dict(row)
    result["payload"] = json.loads(result["payload"])
    return result


def changed_event(event, *, seq=None):
    event = deepcopy(event)
    if seq is not None:
        event["seq"] = seq
        event["payload"]["source_seq"] = seq
    else:
        event["payload"]["price"] = "2501.75"
    event["payload_sha256"] = sha256(canonical(event["payload"]).encode("utf-8"))
    event["event_id"] = sha256(canonical([
        "FTDI-S07-004.1-envelope-v1", event["lane"], event["seq"],
        event["epoch"], event["payload_sha256"],
    ]).encode("utf-8"))
    return event


@contextmanager
def fixture_case():
    with tempfile.TemporaryDirectory(prefix="consumer_retention_contract_") as temporary:
        container = Path(temporary).resolve()
        case = container / "case"
        case.mkdir()
        try:
            yield Fixture(case)
        finally:
            # No GC, delayed cleanup, sleeps, retries, or ignore_errors to hide leaks.
            resolved = case.resolve()
            if resolved.parent != container:
                raise AssertionError("refusing cleanup outside this temporary fixture")
            shutil.rmtree(resolved)
            if resolved.exists():
                raise AssertionError("fixture handles prevented immediate deletion")


def retention_api():
    # Lazy import allows helper-only validation while implementation is in flight.
    return importlib.import_module("ftdi_core.consumer_retention")


class HelperTests(unittest.TestCase):
    def test_canonical_gzip_roundtrip_and_member_rejection(self):
        records = [{"payload": "Unicode: \u6d4b\u8bd5", "seq": 1}, {"seq": 2}]
        first = gzip_records(records)
        self.assertEqual(first, gzip_records(records))
        self.assertEqual(unpack_records(first), records)
        with self.assertRaises(AssertionError):
            unpack_records(first + first)

    def test_fixture_snapshot_envelope_and_handle_cleanup(self):
        with fixture_case() as f:
            self.assertTrue(f.ingest(6)["complete"])
            original = [envelope(row) for row in f.rows()]
            self.assertEqual(len(f.deliver()), 6)
            self.assertEqual([event_from_row(row) for row in effects(f.consumer)], original)
            before = snapshot(f)
            self.assertEqual(snapshot(f), before)
            self.assertEqual(f.prune(3)["archived_new"], 3)

    def test_changed_event_is_valid_v1_input(self):
        with fixture_case() as f:
            f.ingest(2)
            f.deliver()
            original = event_from_row(effects(f.consumer)[0])
            with closing(EffectStore(f.consumer)) as store:
                with self.assertRaises(Conflict):
                    store.accept([changed_event(original)])
                result = store.accept([changed_event(original, seq=3)])
                self.assertEqual((result["accepted"], result["inserted"],
                                  result["duplicates"]), (1, 1, 0))



class ConsumerRetentionContractTests(unittest.TestCase):
    def setUp(self):
        self.context = fixture_case()
        self.f = self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)

    def prepare(self, n=12, *, ack=True, migrate=True, prune=0):
        self.assertTrue(self.f.ingest(n)["complete"])
        self.producer_rows = self.f.deliver(ack=ack)
        self.assertEqual(len(self.producer_rows), n)
        self.original = effects(self.f.consumer)
        self.events = [event_from_row(row) for row in self.original]
        self.lane = self.original[0]["lane"]
        self.assertEqual([row["seq"] for row in self.original], list(range(1, n + 1)))
        if prune:
            self.assertEqual(self.f.prune(prune)["archived_new"], prune)
        if migrate:
            self.assertEqual(retention_api().migrate_consumer_v2(
                self.f.consumer, apply=True)["status"], "MIGRATED")

    def compact(self, watermark=12, **kwargs):
        options = dict(lane=self.lane, content_watermark=watermark,
                       producer_root=self.f.root, segment_size=5, apply=True)
        options.update(kwargs)
        return retention_api().compact_consumer(self.f.consumer, **options)

    def compact_ok(self, watermark=12, **kwargs):
        result = self.compact(watermark, **kwargs)
        self.assertEqual(result["status"], "COMPACTED", result)
        return result

    def audit(self, status="COMPLETE", *codes, root=None):
        from ftdi_core.archive_e2e import reconcile_archive_aware
        before = snapshot(self.f)
        try:
            report = reconcile_archive_aware(root or self.f.root, self.f.consumer)
        finally:
            self.assertEqual(snapshot(self.f), before, "audit modified evidence bytes/rows")
        diagnostic = {key: report[key] for key in
                      ("status", "issue_counts", "fatal_issue_count", "metrics")}
        self.assertEqual(report["status"], status, diagnostic)
        self.assertIs(report["passed"], status == "COMPLETE")
        for code in codes:
            self.assertIn(code, report["issue_counts"], diagnostic)
            self.assertGreater(report["issue_counts"][code], 0)
            if code == "UNREFERENCED_CONSUMER_SEGMENT_FILE":
                notices = [issue for issue in report["issues"] if issue["code"] == code]
                self.assertTrue(notices)
                self.assertTrue(all(issue["severity"] == "NOTICE" for issue in notices))
        if status == "COMPLETE":
            self.assertEqual(report["fatal_issue_count"], 0)
            for name in ("expected_events_total", "consumer_effects", "matched_effects"):
                self.assertEqual(report["metrics"][name], len(self.original), (name, report))
        else:
            self.assertGreater(report["fatal_issue_count"], 0)
        return report

    def assert_accept(self, result, events, *, inserted, duplicates):
        expected = {
            "accepted": len(events), "inserted": inserted, "duplicates": duplicates,
            "batch_digest": sha256("".join(e["event_id"] + "\n"
                                         for e in events).encode("ascii")),
        }
        self.assertEqual(result, expected)

    def assert_manifest(self, watermark, expected_ranges):
        db = self.f.consumer
        manifest = segments(db)
        self.assertEqual([(r["start_seq"], r["end_seq"]) for r in manifest], expected_ranges)
        lane_rows = sql(db, "SELECT * FROM lane_ids WHERE lane=?", (self.lane,))
        self.assertEqual(len(lane_rows), 1)
        lane_id = lane_rows[0]["lane_id"]
        previous = "0" * 64
        archived_rows = []
        for segment in manifest:
            first, last = segment["start_seq"], segment["end_seq"]
            self.assertEqual(segment["lane"], self.lane)
            self.assertEqual(segment["epoch"], 0)
            self.assertEqual(segment["record_count"], last - first + 1)
            self.assertLessEqual(segment["record_count"], 20000)
            relative = relative_segment(lane_id, first, last)
            self.assertEqual(segment["relative_path"], relative)
            data = (archive_root(db) / relative).read_bytes()
            self.assertEqual(segment["sha256"], sha256(data))
            self.assertEqual(segment["prev_chain"], previous)
            self.assertEqual(segment["chain"], chain_hash(segment))
            previous = segment["chain"]
            records = unpack_records(data)
            self.assertEqual([r["seq"] for r in records], list(range(first, last + 1)))
            for record in records:
                self.assertEqual(set(record), set(EFFECT_COLUMNS))
                self.assertEqual(record["payload_sha256"],
                                 sha256(record["payload"].encode("utf-8")))
            archived_rows.extend(records)
        self.assertEqual(archived_rows, [r for r in self.original if r["seq"] <= watermark])
        digests = sql(db, "SELECT * FROM effect_digests ORDER BY seq")
        self.assertEqual(digests, [
            {"lane_id": lane_id, "epoch": 0, "seq": r["seq"],
             "payload_sha256": hashlib.sha256(r["payload"].encode("utf-8")).digest()}
            for r in self.original if r["seq"] <= watermark
        ])
        self.assertEqual(effects(db), [r for r in self.original if r["seq"] > watermark])
        self.assertEqual(sql(db, "SELECT * FROM retention_state"), [{
            "lane": self.lane, "epoch": 0, "first_seq": self.original[0]["seq"],
            "content_watermark": watermark, "digest_watermark": None,
            "segments": len(manifest), "chain_head": previous,
        }])
        return manifest

    def cli(self, command, *args):
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
                   PYTHONWARNINGS="error::ResourceWarning")
        return subprocess.run(
            [sys.executable, "-B", "-W", "error::ResourceWarning", "-m", "ftdi_core",
             "consumer-retention", command, "--consumer-db", str(self.f.consumer),
             *map(str, args)],
            cwd=PROJECT_ROOT, env=env, capture_output=True, timeout=30,
        )

    def assert_cli(self, process, status=None, *, code=0):
        self.assertEqual(process.returncode, code,
                         (process.stdout.decode("ascii", errors="backslashreplace"),
                          process.stderr.decode("ascii", errors="backslashreplace")))
        self.assertNotIn(b"ResourceWarning", process.stderr)
        if status is not None:
            parsed = json.loads(process.stdout.decode("ascii"))
            self.assertEqual(parsed["status"], status, parsed)
            return parsed
        if process.stdout:
            json.loads(process.stdout.decode("ascii"))
        if code == 3:
            self.assertIn(b"FTDI_CORE_FAIL_CLOSED:", process.stderr)

    def test_migration_dry_run_schema_apply_and_idempotence(self):
        self.prepare(migrate=False)
        before = snapshot(self.f)
        self.assertEqual(retention_api().migrate_consumer_v2(self.f.consumer)["status"], "DRY_RUN")
        self.assertEqual(snapshot(self.f), before)
        self.assertEqual(retention_api().migrate_consumer_v2(
            self.f.consumer, apply=True)["status"], "MIGRATED")
        self.assertEqual(effects(self.f.consumer), self.original)
        for table, columns in V2_COLUMNS.items():
            info = sql(self.f.consumer, "PRAGMA table_info(" + table + ")")
            self.assertEqual(tuple(row["name"] for row in info), columns)
        objects = {row["name"]: row for row in sql(
            self.f.consumer, "SELECT type,name,sql FROM sqlite_master")}
        for table in V2_COLUMNS:
            self.assertEqual(objects[table]["type"], "table")
        self.assertEqual(objects["effects_guard_compacted"]["type"], "trigger")
        self.assertIn("WITHOUT ROWID", objects["effect_digests"]["sql"].upper())
        self.assertEqual(sql(self.f.consumer, "SELECT * FROM consumer_meta"),
                         [{"k": "schema", "v": "FTDI-Consumer-v2"}])
        for table in ("retention_state", "effect_segments", "effect_digests"):
            self.assertEqual(sql(self.f.consumer, "SELECT * FROM " + table), [])
        before = snapshot(self.f)
        self.assertEqual(retention_api().migrate_consumer_v2(
            self.f.consumer, apply=True)["status"], "ALREADY_V2")
        self.assertEqual(snapshot(self.f), before)

    def migration_refusal(self, scenario, apply):
        self.prepare(migrate=False)
        if scenario == "view":
            sql(self.f.consumer, "ALTER TABLE effects RENAME TO stored_effects")
            sql(self.f.consumer, "CREATE VIEW effects AS SELECT * FROM stored_effects")
        else:
            sql(self.f.consumer, "ALTER TABLE effects RENAME TO old_effects")
            sql(self.f.consumer, """CREATE TABLE effects(
                event_id TEXT PRIMARY KEY, lane TEXT NOT NULL,
                seq INTEGER NOT NULL, epoch INTEGER NOT NULL,
                payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL)""")
            sql(self.f.consumer, "INSERT INTO effects SELECT * FROM old_effects")
            sql(self.f.consumer, "DROP TABLE old_effects")
        before = snapshot(self.f)
        with self.assertRaises(ValueError):
            retention_api().migrate_consumer_v2(self.f.consumer, apply=apply)
        self.assertEqual(snapshot(self.f), before)

    def test_compact_dry_run_and_three_segment_contents(self):
        self.prepare()
        before = snapshot(self.f)
        self.assertEqual(self.compact(apply=False)["status"], "DRY_RUN")
        self.assertEqual(snapshot(self.f), before)
        self.compact_ok()
        self.assert_manifest(12, [(1, 5), (6, 10), (11, 12)])
        report = self.audit()
        self.assertEqual(report["metrics"]["consumer_compacted_effects"], 12)
        self.assertEqual(report["metrics"]["consumer_segments"], 3)
        self.assertEqual(report["metrics"]["consumer_segments_parsed"], 3)

    def test_second_compaction_extends_existing_chain(self):
        self.prepare()
        self.compact_ok(5)
        old = self.assert_manifest(5, [(1, 5)])
        data = (archive_root(self.f.consumer) / old[0]["relative_path"]).read_bytes()
        self.compact_ok(12)
        manifest = self.assert_manifest(12, [(1, 5), (6, 10), (11, 12)])
        self.assertEqual(manifest[0], old[0])
        self.assertEqual(manifest[1]["prev_chain"], old[0]["chain"])
        self.assertEqual((archive_root(self.f.consumer) /
                          manifest[0]["relative_path"]).read_bytes(), data)
        self.audit()

    def test_both_producer_and_consumer_archives_are_reconciled(self):
        self.prepare(prune=8)
        self.compact_ok()
        report = self.audit()
        self.assertEqual(report["metrics"]["archived_records"], 8)
        self.assertEqual(report["metrics"]["consumer_compacted_effects"], 12)

    def test_non_one_first_seq_controls_compacted_count(self):
        self.assertTrue(self.f.ingest(12, order=list(range(5, 13)))["complete"])
        self.assertEqual(len(self.f.deliver()), 8)
        self.original = effects(self.f.consumer)
        self.events = [event_from_row(row) for row in self.original]
        self.lane = self.original[0]["lane"]
        self.assertEqual(retention_api().migrate_consumer_v2(
            self.f.consumer, apply=True)["status"], "MIGRATED")
        self.compact_ok()
        self.assert_manifest(12, [(5, 9), (10, 12)])
        report = self.audit()
        self.assertEqual(report["metrics"]["consumer_compacted_effects"], 12 - 5 + 1)
        self.assertEqual(report["metrics"]["consumer_segments"], 2)
        self.assertEqual(report["metrics"]["consumer_segments_parsed"], 2)

    def compaction_refusal(self, scenario, apply):
        self.prepare(migrate=scenario != "v1", ack=scenario not in ("unacked_equal", "unacked_above"))
        kwargs = {}
        watermark = 12
        if scenario in ("equal_c", "below_c"):
            self.compact_ok(5)
            watermark = 5 if scenario == "equal_c" else 4
        elif scenario == "hole":
            sql(self.f.consumer, "DELETE FROM effects WHERE seq=7")
        elif scenario == "beyond_range":
            watermark = 13
        elif scenario == "payload":
            sql(self.f.consumer, "UPDATE effects SET payload=? WHERE seq=7", ('{}',))
        elif scenario == "event_id":
            sql(self.f.consumer, "UPDATE effects SET event_id=? WHERE seq=7", ("0" * 64,))
        elif scenario == "noncanonical":
            row = self.original[6]
            sql(self.f.consumer, "UPDATE effects SET payload=? WHERE seq=7",
                (json.dumps(json.loads(row["payload"]), indent=2),))
        elif scenario == "no_producer_window":
            kwargs["producer_root"] = None
        elif scenario in ("unacked_equal", "unacked_above"):
            self.f.ack(self.producer_rows[:5])
            watermark = 6 if scenario == "unacked_equal" else 7
        elif scenario == "unregistered_lane":
            (self.f.root / "lane_registry.json").write_text("{}", encoding="ascii")
        elif scenario == "file_collision":
            # Use an assigned id, not an assumed implementation allocation order.
            sql(self.f.consumer, "INSERT INTO lane_ids(lane_id,lane) VALUES(?,?)",
                (17, self.lane))
            path = archive_root(self.f.consumer) / relative_segment(17, 6, 10)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"preexisting different bytes in the SECOND segment")
        elif scenario == "unsupported_epoch":
            kwargs["epoch"] = 1
        elif scenario == "empty_lane":
            sql(self.f.consumer, "DELETE FROM effects")
        before = snapshot(self.f)
        with self.assertRaises(ValueError):
            self.compact(watermark, apply=apply, **kwargs)
        after = snapshot(self.f)
        self.assertEqual(after[0], before[0],
                         f"refusal changed files; hot_rows={len(effects(self.f.consumer))}")
        self.assertEqual(after, before, "refusal changed fixture evidence")

    def test_explicit_ignore_window_allows_compaction(self):
        self.prepare(ack=False)
        self.compact_ok(producer_root=None, ignore_producer_window=True)
        self.assert_manifest(12, [(1, 5), (6, 10), (11, 12)])

    def test_watermark_strictly_below_low_unacked_is_allowed(self):
        self.prepare(ack=False)
        self.f.ack(self.producer_rows[:5])
        self.compact_ok(5)
        self.assert_manifest(5, [(1, 5)])

    def test_trigger_blocks_raw_insert_and_adapter_batch_rolls_back(self):
        self.prepare()
        self.compact_ok(5)
        original = self.original[0]
        before_rows = database_rows(self.f.consumer)
        with self.assertRaises(sqlite3.IntegrityError):
            sql(self.f.consumer, "INSERT INTO effects VALUES(?,?,?,?,?,?)",
                tuple(original[name] for name in EFFECT_COLUMNS))
        self.assertEqual(database_rows(self.f.consumer), before_rows)
        with closing(EffectStore(self.f.consumer)) as store:
            with self.assertRaises(sqlite3.IntegrityError):
                store.accept([changed_event(self.events[0], seq=13), self.events[0]])
        self.assertEqual(database_rows(self.f.consumer), before_rows)
        self.assertEqual(len(effects(self.f.consumer)), 7)

    def test_tiered_compacted_replay_exact_duplicate_counts(self):
        self.prepare()
        self.compact_ok()
        before = database_rows(self.f.consumer)
        with closing(retention_api().TieredEffectStore(self.f.consumer)) as store:
            self.assert_accept(store.accept(self.events), self.events, inserted=0, duplicates=12)
        self.assertEqual(database_rows(self.f.consumer), before)
        self.assertEqual(effects(self.f.consumer), [])

    def test_tiered_compacted_payload_conflict_rolls_back_prior_insert(self):
        self.prepare()
        self.compact_ok(5)
        before = database_rows(self.f.consumer)
        with closing(retention_api().TieredEffectStore(self.f.consumer)) as store:
            with self.assertRaises(Conflict):
                store.accept([changed_event(self.events[0], seq=13),
                              changed_event(self.events[0])])
        self.assertEqual(database_rows(self.f.consumer), before)

    def test_tiered_missing_digest_rolls_back_prior_insert(self):
        self.prepare()
        self.compact_ok(5)
        sql(self.f.consumer, "DELETE FROM effect_digests WHERE seq=3")
        before = database_rows(self.f.consumer)
        with closing(retention_api().TieredEffectStore(self.f.consumer)) as store:
            with self.assertRaises(retention_api().TierIntegrityError):
                store.accept([changed_event(self.events[0], seq=13), self.events[2]])
        self.assertEqual(database_rows(self.f.consumer), before)
        self.assertEqual(len(effects(self.f.consumer)), 7)

    def test_tiered_above_watermark_matches_v1_insert_duplicate_and_conflict(self):
        self.prepare()
        self.compact_ok(5)
        fresh = changed_event(self.events[0], seq=13)
        batch = [self.events[5], fresh, fresh]
        with closing(retention_api().TieredEffectStore(self.f.consumer)) as store:
            self.assert_accept(store.accept(batch), batch, inserted=1, duplicates=2)
        before = database_rows(self.f.consumer)
        with closing(retention_api().TieredEffectStore(self.f.consumer)) as store:
            with self.assertRaises(Conflict):
                store.accept([changed_event(self.events[0], seq=14),
                              changed_event(self.events[5])])
        self.assertEqual(database_rows(self.f.consumer), before)
        self.assertEqual(len(effects(self.f.consumer)), 8)

    def test_tiered_v1_differential_batches_validation_and_rollback(self):
        self.prepare(migrate=False)
        other = self.f.consumers / "v1_reference.sqlite3"
        shutil.copy2(self.f.consumer, other)
        fresh = changed_event(self.events[0], seq=13)
        bad_identity = dict(fresh, event_id="0" * 64)
        bad_epoch = dict(fresh, epoch=1)
        cases = [
            (self.events[:3], None),
            ([fresh, fresh, self.events[0]], None),
            ([changed_event(fresh, seq=14), changed_event(self.events[0])], Conflict),
            ([], ValueError), ({}, ValueError), (tuple(self.events[:1]), ValueError),
            ([bad_identity], ValueError), ([bad_epoch], ValueError),
            ([changed_event(fresh, seq=14), bad_identity], ValueError),
            ([self.events[0]] * 1025, ValueError),
        ]
        with closing(EffectStore(other)) as reference, closing(
                retention_api().TieredEffectStore(self.f.consumer)) as tiered:
            for batch, error in cases:
                with self.subTest(batch_size=len(batch), error=error):
                    if error:
                        with self.assertRaises(error):
                            reference.accept(deepcopy(batch))
                        with self.assertRaises(error):
                            tiered.accept(deepcopy(batch))
                    else:
                        self.assertEqual(tiered.accept(deepcopy(batch)),
                                         reference.accept(deepcopy(batch)))
        self.assertEqual(effects(self.f.consumer), effects(other))
        self.assertEqual(len(effects(self.f.consumer)), 13)
        self.assertEqual(sql(self.f.consumer,
                             "SELECT name FROM sqlite_master WHERE name='consumer_meta'"), [])

    def test_end_to_end_fresh_producer_replays_same_source_as_duplicates(self):
        from ftdi_core.delivery import local_lab
        self.prepare()
        self.compact_ok()
        before = database_rows(self.f.consumer)
        new_root = self.f.base / "new_producer"
        old_root = self.f.root
        self.f.root = new_root
        try:
            self.assertTrue(self.f.replay()["complete"])
        finally:
            self.f.root = old_root
        result = local_lab(new_root, self.f.consumer, batch=4)
        delivery = result["delivery"]
        self.assertEqual(delivery["acknowledged"], 12)
        self.assertEqual(delivery["events_sent"], 12)
        self.assertEqual(delivery["remote_duplicates"], 12)
        self.assertEqual(delivery["batches"], 3)
        self.assertEqual(sql(new_root / "lanes/BTC/state.sqlite3",
                             "SELECT COUNT(*) AS n FROM outbox WHERE delivered=0"), [{"n": 0}])
        self.assertEqual(database_rows(self.f.consumer), before)
        self.assertEqual(effects(self.f.consumer), [])
        self.audit(root=new_root)

    def test_rehydrate_dry_run_and_full_row_for_row_restore_keeps_files(self):
        self.prepare()
        self.compact_ok()
        files = {p: p.read_bytes() for p in archive_root(self.f.consumer).rglob("*")
                 if p.is_file()}
        before = snapshot(self.f)
        result = retention_api().rehydrate_consumer(self.f.consumer, lane=self.lane)
        self.assertEqual(result["status"], "DRY_RUN")
        self.assertEqual(snapshot(self.f), before)
        result = retention_api().rehydrate_consumer(self.f.consumer, lane=self.lane, apply=True)
        self.assertEqual(result["status"], "REHYDRATED")
        self.assertEqual(effects(self.f.consumer), self.original)
        for table in ("retention_state", "effect_digests", "effect_segments"):
            self.assertEqual(sql(self.f.consumer, "SELECT * FROM " + table), [])
        self.assertEqual({p: p.read_bytes() for p in files}, files)
        report = self.audit("COMPLETE", "UNREFERENCED_CONSUMER_SEGMENT_FILE")
        self.assertEqual(report["issue_counts"]["UNREFERENCED_CONSUMER_SEGMENT_FILE"], 3)
        self.assertEqual(report["metrics"]["consumer_compacted_effects"], 0)
        self.assertEqual(report["metrics"]["consumer_segments"], 0)

    def test_partial_rehydrate_rounds_down_to_segment_boundary(self):
        self.prepare()
        self.compact_ok()
        before_files = evidence_snapshot(archive_root(self.f.consumer))
        result = retention_api().rehydrate_consumer(
            self.f.consumer, lane=self.lane, to_watermark=7, apply=True)
        self.assertEqual(result["status"], "REHYDRATED")
        self.assert_manifest(5, [(1, 5)])
        self.assertEqual(len(effects(self.f.consumer)), 7)
        self.assertEqual(evidence_snapshot(archive_root(self.f.consumer)), before_files)
        report = self.audit("COMPLETE", "UNREFERENCED_CONSUMER_SEGMENT_FILE")
        self.assertEqual(report["issue_counts"]["UNREFERENCED_CONSUMER_SEGMENT_FILE"], 2)
        self.assertEqual(report["metrics"]["consumer_compacted_effects"], 5)
        self.assertEqual(report["metrics"]["consumer_segments"], 1)

    def test_partial_rehydrate_at_exact_boundary_restores_newest_segment(self):
        self.prepare()
        self.compact_ok()
        before_files = evidence_snapshot(archive_root(self.f.consumer))
        result = retention_api().rehydrate_consumer(
            self.f.consumer, lane=self.lane, to_watermark=10, apply=True)
        self.assertEqual(result["status"], "REHYDRATED")
        self.assert_manifest(10, [(1, 5), (6, 10)])
        self.assertEqual(len(effects(self.f.consumer)), 2)
        self.assertEqual(evidence_snapshot(archive_root(self.f.consumer)), before_files)

    def crash(self, stage):
        process = self.cli("compact", "--lane", "BTC", "--watermark", 12,
                           "--producer-root", self.f.root, "--segment-size", 5,
                           "--apply", "--crash-stage", stage)
        self.assert_cli(process, code=86)

    def test_crash_after_segment_file_orphan_is_notice_and_retry_reuses_bytes(self):
        self.prepare()
        before = database_rows(self.f.consumer)
        self.crash("after_segment_file")
        # A normal open performs crash recovery before comparing logical state.
        self.assertEqual(effects(self.f.consumer), self.original)
        self.assertEqual(database_rows(self.f.consumer), before)
        files = list(archive_root(self.f.consumer).rglob("*.jsonl.gz"))
        self.assertEqual(len(files), 1)
        orphan = files[0].read_bytes()
        self.assertEqual(unpack_records(orphan), self.original[:5])
        self.assertEqual(sql(self.f.consumer, "SELECT * FROM retention_state"), [])
        with self.subTest(stage="orphan audit before retry"):
            report = self.audit("COMPLETE", "UNREFERENCED_CONSUMER_SEGMENT_FILE")
            self.assertEqual(report["issue_counts"]["UNREFERENCED_CONSUMER_SEGMENT_FILE"], 1)
            self.assertEqual(report["metrics"]["consumer_compacted_effects"], 0)
        self.compact_ok()
        self.assertEqual(files[0].read_bytes(), orphan)
        self.assert_manifest(12, [(1, 5), (6, 10), (11, 12)])
        report = self.audit()
        self.assertNotIn("UNREFERENCED_CONSUMER_SEGMENT_FILE", report["issue_counts"])

    def test_crash_after_commit_keeps_one_atomic_segment(self):
        self.prepare()
        self.crash("after_commit")
        self.assertEqual(effects(self.f.consumer), self.original[5:])
        self.assert_manifest(5, [(1, 5)])
        report = self.audit()
        self.assertEqual(report["metrics"]["consumer_compacted_effects"], 5)
        self.assertEqual(report["metrics"]["consumer_segments"], 1)
        self.assertEqual(report["metrics"]["consumer_segments_parsed"], 1)

    def corrupt_and_audit(self, scenario, code):
        self.prepare(prune=4)
        self.compact_ok()
        manifest = segments(self.f.consumer)
        middle = manifest[1]
        path = archive_root(self.f.consumer) / middle["relative_path"]
        if scenario == "flipped_byte":
            content = bytearray(path.read_bytes())
            content[len(content) // 2] ^= 1
            path.write_bytes(content)
        elif scenario == "missing_segment":
            path.unlink()
        elif scenario == "missing_manifest":
            sql(self.f.consumer, "DELETE FROM effect_segments WHERE start_seq=6")
        elif scenario == "chain":
            sql(self.f.consumer, "UPDATE effect_segments SET chain=? WHERE start_seq=6",
                ("f" * 64,))
        elif scenario == "path":
            sql(self.f.consumer, "UPDATE effect_segments SET relative_path=? WHERE start_seq=6",
                ("lane-999/noncanonical.jsonl.gz",))
        elif scenario == "digest":
            sql(self.f.consumer, "UPDATE effect_digests SET payload_sha256=? WHERE seq=7",
                (b"\0" * 32,))
        elif scenario == "missing_digest":
            sql(self.f.consumer, "DELETE FROM effect_digests WHERE seq=7")
        elif scenario == "extra_digest":
            sql(self.f.consumer, """INSERT INTO effect_digests
                SELECT lane_id,epoch,13,payload_sha256 FROM effect_digests WHERE seq=1""")
        elif scenario in ("trigger", "overlap"):
            sql(self.f.consumer, "DROP TRIGGER effects_guard_compacted")
            if scenario == "overlap":
                sql(self.f.consumer, "INSERT INTO effects VALUES(?,?,?,?,?,?)",
                    tuple(self.original[0][key] for key in EFFECT_COLUMNS))
        elif scenario == "forged":
            # Forge ALL consumer evidence consistently; unchanged producer is the oracle.
            first = manifest[0]
            first_path = archive_root(self.f.consumer) / first["relative_path"]
            records = unpack_records(first_path.read_bytes())
            replacement = changed_event(event_from_row(records[0]))
            replacement["payload"] = canonical(replacement["payload"])
            records[0] = replacement
            first_path.write_bytes(gzip_records(records))
            sql(self.f.consumer, "UPDATE effect_digests SET payload_sha256=? WHERE seq=1",
                (bytes.fromhex(replacement["payload_sha256"]),))
            previous = "0" * 64
            for segment in manifest:
                segment_path = archive_root(self.f.consumer) / segment["relative_path"]
                segment["sha256"] = sha256(segment_path.read_bytes())
                segment["prev_chain"] = previous
                segment["chain"] = chain_hash(segment)
                sql(self.f.consumer, """UPDATE effect_segments
                    SET sha256=?,prev_chain=?,chain=? WHERE start_seq=?""",
                    (segment["sha256"], previous, segment["chain"], segment["start_seq"]))
                previous = segment["chain"]
            sql(self.f.consumer, "UPDATE retention_state SET chain_head=?", (previous,))
        report = self.audit("INCONSISTENT", code)
        if scenario == "overlap":
            self.assertIn("CONSUMER_SCHEMA_UNRECOGNIZED", report["issue_counts"])
        if scenario == "forged":
            for internal_error in ("CONSUMER_DIGEST_MISMATCH", "CONSUMER_CHAIN_MISMATCH",
                                   "CONSUMER_SEGMENT_SHA256_MISMATCH",
                                   "CONSUMER_SEGMENT_RECORD_INVALID"):
                self.assertNotIn(internal_error, report["issue_counts"])

    def test_cli_migrate_compact_rehydrate_dry_runs_and_success(self):
        self.prepare(migrate=False)
        before = snapshot(self.f)
        self.assert_cli(self.cli("migrate"), "DRY_RUN")
        self.assertEqual(snapshot(self.f), before)
        self.assert_cli(self.cli("migrate", "--apply"), "MIGRATED")
        self.assert_cli(self.cli("migrate", "--apply"), "ALREADY_V2")
        options = ("--lane", "BTC", "--watermark", 12, "--producer-root", self.f.root,
                   "--segment-size", 5)
        before = snapshot(self.f)
        self.assert_cli(self.cli("compact", *options), "DRY_RUN")
        self.assertEqual(snapshot(self.f), before)
        self.assert_cli(self.cli("compact", *options, "--apply"), "COMPACTED")
        self.assert_manifest(12, [(1, 5), (6, 10), (11, 12)])
        before = snapshot(self.f)
        self.assert_cli(self.cli("rehydrate", "--lane", self.lane), "DRY_RUN")
        self.assertEqual(snapshot(self.f), before)
        self.assert_cli(self.cli("rehydrate", "--lane", self.lane, "--apply"), "REHYDRATED")
        self.assertEqual(effects(self.f.consumer), self.original)

    def test_cli_compact_exact_scope_with_ignore_window(self):
        self.prepare()
        self.assert_cli(self.cli("compact", "--lane", self.lane, "--watermark", 12,
                                 "--ignore-producer-window", "--segment-size", 5,
                                 "--apply"), "COMPACTED")
        self.assert_manifest(12, [(1, 5), (6, 10), (11, 12)])

    def cli_refusal(self, command, apply):
        self.prepare(migrate=command != "migrate")
        args = []
        if command == "migrate":
            sql(self.f.consumer, "ALTER TABLE effects RENAME TO stored_effects")
            sql(self.f.consumer, "CREATE VIEW effects AS SELECT * FROM stored_effects")
        elif command == "compact":
            args = ["--lane", "BTC", "--watermark", 13, "--producer-root", self.f.root]
        else:
            self.compact_ok()
            path = archive_root(self.f.consumer) / segments(self.f.consumer)[-1]["relative_path"]
            path.unlink()
            args = ["--lane", self.lane]
        flags = ["--apply"] if apply else []
        before = snapshot(self.f)
        self.assert_cli(self.cli(command, *args, *flags), code=3)
        self.assertEqual(snapshot(self.f), before)


def _install_cases():
    # Each scenario is a separately reported unittest, with its own temp fixture.
    for scenario in ("view", "missing_identity_unique"):
        for apply in (False, True):
            def test(self, scenario=scenario, apply=apply):
                self.migration_refusal(scenario, apply)
            suffix = "_apply" if apply else "_dry_run"
            setattr(ConsumerRetentionContractTests, "test_migration_refuses_" + scenario + suffix, test)
    for scenario in (
        "v1", "equal_c", "below_c", "hole", "beyond_range", "payload", "event_id",
        "noncanonical", "no_producer_window", "unacked_equal", "unacked_above",
        "unregistered_lane", "file_collision", "unsupported_epoch", "empty_lane",
    ):
        for apply in (False, True):
            def test(self, scenario=scenario, apply=apply):
                self.compaction_refusal(scenario, apply)
            suffix = "_apply" if apply else "_dry_run"
            setattr(ConsumerRetentionContractTests, "test_compaction_refuses_" + scenario + suffix, test)
    for scenario, code in {
        "flipped_byte": "CONSUMER_SEGMENT_SHA256_MISMATCH",
        "missing_segment": "CONSUMER_SEGMENT_MISSING",
        "missing_manifest": "CONSUMER_SEGMENT_CONTINUITY",
        "chain": "CONSUMER_CHAIN_MISMATCH",
        "path": "CONSUMER_SEGMENT_PATH_INVALID",
        "digest": "CONSUMER_DIGEST_MISMATCH",
        "missing_digest": "CONSUMER_DIGEST_MISSING",
        "extra_digest": "CONSUMER_DIGEST_EXTRA",
        "trigger": "CONSUMER_SCHEMA_UNRECOGNIZED",
        "overlap": "CONSUMER_TIER_OVERLAP",
        "forged": "CONSUMER_TIER_CONTENT_CONFLICT",
    }.items():
        def test(self, scenario=scenario, code=code):
            self.corrupt_and_audit(scenario, code)
        setattr(ConsumerRetentionContractTests, "test_audit_detects_" + scenario, test)
    for command in ("migrate", "compact", "rehydrate"):
        for apply in (False, True):
            def test(self, command=command, apply=apply):
                self.cli_refusal(command, apply)
            suffix = "_apply" if apply else "_dry_run"
            setattr(ConsumerRetentionContractTests,
                    "test_cli_" + command + "_refusal_exit_3" + suffix, test)


_install_cases()


if __name__ == "__main__":
    unittest.main()


