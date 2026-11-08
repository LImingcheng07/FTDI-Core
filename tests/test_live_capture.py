"""Independent black-box acceptance tests for docs/LIVE_CAPTURE.md.

Only the public live API is imported; no live implementation is inspected or
patched. Run with unittest discovery (-s tests), not the user-site tests package.

Strict readings / contract ambiguities deliberately made visible here:
* A conflict discovered after emission still excludes that sequence and its
  successors from the sealed prefix (section 3's final sentence).
* A consecutive REST page may include buffered sequences: the contract requires
  overlap comparison but does not specify rejecting a page longer than limit.
* Retry-After's storage key and recovery report nesting are unspecified; check
  their semantic contents without assuming undocumented key names.
* CLI precondition diagnostics may be stderr-only; any nonempty stdout must be
  ASCII JSON. Missing directories are preconditions; existing unsealed ones
  are verification findings.
* A malformed REST page is validated as a whole before any of its records
  enter the event prefix. Limit integer capacities reject bool and float.
* The manifest's 'lane (the 4 scope fields)' is read as a named-field object;
  a missing URL port, even though a protocol default exists, is refused.
* local_lab is the explicitly requested E2E exception to FakeVenue-only traffic:
  its independent consumer uses 127.0.0.1, never an external endpoint.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from dataclasses import FrozenInstanceError, fields
import hashlib
import io
import json
from pathlib import Path
import shutil
import socket
import tempfile
import unittest
from unittest.mock import patch

from fakevenue import FakeVenue, core_event, rest_trade, trade
from ftdi_core.live import CaptureLimits, capture_session, seal_session, verify_session


SYMBOL = "BTCUSDT"
MANIFEST = "SESSION_MANIFEST.json"
DATA_FILES = ("events.jsonl", "raw_ws.jsonl", "raw_rest.jsonl", "clock.jsonl")
COUNTS = {
    "ws_messages", "ws_duplicates", "rest_pages", "rest_records",
    "rest_duplicates", "text_variants", "before_anchor", "connections",
    "reconnects", "clock_samples", "gaps_repaired",
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def json_lines(values):
    return "".join(canonical(value) + "\n" for value in values).encode("utf-8")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_info(data):
    return {"bytes": len(data), "sha256": digest(data), "lines": len(data.splitlines())}


def snapshot(directory):
    """Capture filenames, directories and bytes, including unexpected sidecars."""
    directory = Path(directory)
    if not directory.exists():
        return None
    return {
        str(path.relative_to(directory)): path.read_bytes() if path.is_file() else None
        for path in directory.rglob("*")
    }


def read_rows(path):
    return [json.loads(line) for line in Path(path).read_bytes().splitlines()]


def fast_limits(**changes):
    values = dict(gap_wait_ms=20, gap_timeout_s=1.0, rest_min_interval_s=0,
                  clock_interval_s=3600, ws_read_timeout_s=2.0, max_reconnects=2)
    values.update(changes)
    return CaptureLimits(**values)


def contains_value(value, wanted):
    if isinstance(value, dict):
        return any(contains_value(v, wanted) for v in value.values())
    if isinstance(value, list):
        return any(contains_value(v, wanted) for v in value)
    return value == wanted or (isinstance(value, str) and str(wanted) in value)


def recovery_summary(report):
    """The contract names the facts, but does not specify their nesting."""
    if isinstance(report.get("would_seal"), dict):
        return report["would_seal"]
    if report.get("would_seal") is not True:
        raise AssertionError(f"missing would_seal report: {report!r}")
    return report


class LiveContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ftdi_live_contract_")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.capture_root = self.base / "captures"
        self.allowed_ports = set()
        self.allow_consumer = False
        original_create = socket.create_connection
        original_connect = socket.socket.connect

        def check(address):
            self.assertIsInstance(address, tuple, "unexpected non-IP connection")
            self.assertEqual(address[0], "127.0.0.1", "non-loopback network forbidden")
            if not self.allow_consumer:
                self.assertIn(address[1], self.allowed_ports, "not a FakeVenue port")

        def create(address, *args, **kwargs):
            check(address)
            return original_create(address, *args, **kwargs)

        def connect(sock, address):
            check(address)
            return original_connect(sock, address)

        self.network = ExitStack()
        self.addCleanup(self.network.close)
        self.network.enter_context(patch("socket.create_connection", side_effect=create))
        self.network.enter_context(patch("socket.socket.connect", new=connect))

    @contextmanager
    def venue(self, **kwargs):
        with FakeVenue(**kwargs) as venue:
            for url in (venue.ws_url, venue.rest_url):
                self.allowed_ports.add(int(url.rsplit(":", 1)[1]))
            yield venue

    def capture(self, venue, *, max_events=30, limits=None, **kwargs):
        result = capture_session(
            self.capture_root, symbol=SYMBOL, ws_url=venue.ws_url,
            rest_url=venue.rest_url, max_events=max_events,
            max_seconds=kwargs.pop("max_seconds", 4.0),
            limits=limits or fast_limits(), **kwargs)
        path = Path(result["session_dir"])
        self.assertEqual(path.parent, self.capture_root / SYMBOL)
        self.assertRegex(path.name, r"^session-\d{8}T\d{6}Z(?:-\d+)?$")
        manifest = json.loads((path / MANIFEST).read_bytes())
        self.assertEqual({k: v for k, v in result.items() if k != "session_dir"}, manifest)
        self.assert_files(path, manifest)
        return result, path

    def assert_files(self, path, manifest):
        self.assertEqual((path / MANIFEST).read_bytes(), json_lines([manifest]))
        self.assertEqual(set(manifest["files"]), set(DATA_FILES))
        for name in DATA_FILES:
            data = (path / name).read_bytes()
            self.assertEqual(manifest["files"][name], file_info(data), name)
            self.assertEqual(data, json_lines(read_rows(path / name)), name)
        self.assertFalse(list(path.glob("*.part")))

    def assert_events(self, result, path, first, last, reason):
        expected = [core_event(seq) for seq in range(first, last + 1)]
        self.assertEqual(result["stop_reason"], reason)
        self.assertEqual((path / "events.jsonl").read_bytes(), json_lines(expected))
        self.assertEqual(result["event_count"], len(expected))
        self.assertEqual(result["first_seq"], first if expected else None)
        self.assertEqual(result["last_seq"], last if expected else None)

    def verify_readonly(self, path, *, previous=None, status="VERIFIED", codes=()):
        before = snapshot(self.base)
        try:
            result = verify_session(path, previous=previous)
        finally:
            self.assertEqual(snapshot(self.base), before, "verification mutated evidence")
        self.assertEqual(result["status"], status, result)
        self.assertIsInstance(result["metrics"], dict)
        self.assertIsInstance(result["issue_counts"], dict)
        for code in codes:
            self.assertGreater(result["issue_counts"].get(code, 0), 0, result)
        return result

    def sealed_fixture(self, n=5):
        with self.venue(connections=[[("trades", 1, n)]]) as venue:
            result, path = self.capture(venue, max_events=n)
        self.assert_events(result, path, 1, n, "MAX_EVENTS")
        return result, path

    def copy_session(self, source, name):
        # Keep session_id valid while locating each adversarial copy separately.
        target = self.base / name / source.name
        shutil.copytree(source, target)
        return target

    def rewrite_manifest(self, path, **changes):
        manifest = json.loads((path / MANIFEST).read_bytes())
        manifest.update(changes)
        (path / MANIFEST).write_bytes(json_lines([manifest]))
        return manifest

    def rewrite_file(self, path, name, data, **changes):
        (path / name).write_bytes(data)
        manifest = json.loads((path / MANIFEST).read_bytes())
        manifest["files"][name] = file_info(data)
        manifest.update(changes)
        (path / MANIFEST).write_bytes(json_lines([manifest]))

    def unsealed_copy(self, source, label="killed", *, events=None):
        target = self.base / label / source.name
        target.mkdir(parents=True)
        for name in DATA_FILES:
            data = (source / name).read_bytes()
            if name == "events.jsonl" and events is not None:
                data = json_lines(events)
            # A crash can leave a final partial write in every stream.
            (target / (name + ".part")).write_bytes(data + b'{"torn":')
        # Adjusted by Claude after the contract added CAPTURE_START.json (written when a capture starts, so a
        # killed capture always has it; recovery reads the previous-session link and endpoints from it).
        (target / "CAPTURE_START.json").write_bytes((source / "CAPTURE_START.json").read_bytes())
        self.assertFalse((target / MANIFEST).exists())
        return target

    def cli(self, args, expected, *, json_required=True):
        from ftdi_core.cli import main
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            try:
                code = main(args)
            except SystemExit as exc:
                code = exc.code
        self.assertEqual(code, expected, stderr.getvalue())
        output = stdout.getvalue()
        self.assertTrue(output.isascii(), repr(output))
        parsed = json.loads(output) if output.strip() else None
        if json_required:
            self.assertIsInstance(parsed, dict, output)
        if expected == 3:
            self.assertIn("FTDI_CORE_FAIL_CLOSED:", stderr.getvalue() + output)
        return parsed

    def test_endpoint_refusals_before_any_socket(self):
        ws, rest = "ws://127.0.0.1:12345", "http://127.0.0.1:12346"
        cases = [dict(), dict(ws_url=ws), dict(rest_url=rest)]
        for key, good, scheme in (("ws_url", ws, "ws"), ("rest_url", rest, "http")):
            bad_urls = [
                f"{scheme}://localhost:12345", f"{scheme}://[::1]:12345",
                f"{scheme}://192.0.2.1:12345", f"{scheme}://example.invalid:12345",
                f"{scheme}://127.0.0.2:12345", f"{scheme}://127.1:12345",
                f"{scheme}://2130706433:12345",
                good + "/", good + "/path", good + "?x=1", good + "#fragment",
                good + "?", good + "#",
                good.replace("://", "://user:password@"),
                good.replace("://", "://user@"),
                good.replace(scheme + ":", ("wss" if scheme == "ws" else "https") + ":", 1),
                good.replace(scheme + ":", ("http" if scheme == "ws" else "ws") + ":", 1),
                f"{scheme}://127.0.0.1", f"{scheme}://127.0.0.1:0",
                f"{scheme}://127.0.0.1:65536", f"{scheme}://127.0.0.1:abc",
                f"{scheme}://127.0.0.1:", "",
            ]
            for bad in bad_urls:
                case = dict(ws_url=ws, rest_url=rest)
                case[key] = bad
                cases.append(case)
        cases += [
            dict(public_network=True, ws_url=ws, rest_url=rest),
            dict(public_network=True, ws_url=ws),
            dict(public_network=True, rest_url=rest),
            dict(public_network=True, ws_url="wss://data-stream.binance.vision",
                 rest_url="https://data-api.binance.vision"),
        ]
        for options in cases:
            with self.subTest(options=options):
                before = snapshot(self.base)
                with patch("socket.create_connection", side_effect=AssertionError("socket opened")) as create, \
                     patch("socket.socket.connect", side_effect=AssertionError("socket connected")) as connect, \
                     patch("socket.socket", side_effect=AssertionError("socket allocated")) as allocate:
                    with self.assertRaises(ValueError):
                        capture_session(self.capture_root, symbol=SYMBOL,
                                        max_events=1, max_seconds=1, **options)
                    create.assert_not_called()
                    connect.assert_not_called()
                    allocate.assert_not_called()
                self.assertEqual(snapshot(self.base), before)

    def test_limits_are_frozen_keyword_only_and_validate_capacities(self):
        defaults = CaptureLimits()
        expected = dict(queue_size=10000, max_pending=100000, gap_wait_ms=200,
                        gap_timeout_s=10.0, max_rest_pages_per_gap=10,
                        rest_min_interval_s=0.35, max_reconnects=5,
                        ws_read_timeout_s=30.0, max_message_bytes=1048576,
                        max_rest_body_bytes=2097152, clock_interval_s=60.0,
                        fsync_every=256)
        self.assertEqual({f.name: getattr(defaults, f.name) for f in fields(defaults)}, expected)
        with self.assertRaises(FrozenInstanceError):
            defaults.queue_size = 1
        with self.assertRaises(TypeError):
            CaptureLimits(10)
        for key in ("queue_size", "max_pending", "max_rest_pages_per_gap",
                    "max_message_bytes", "max_rest_body_bytes", "fsync_every"):
            for value in (0, -1, True, 1.5):
                with self.subTest(field=key, value=value):
                    with self.assertRaises(ValueError):
                        CaptureLimits(**{key: value})

    def test_happy_path_fifty_canonical_events_and_manifest(self):
        with self.venue(connections=[[("trades", 1, 50)]]) as venue:
            result, path = self.capture(venue, max_events=50)
            self.assertEqual(venue.handshakes[0]["path"], "/ws/btcusdt@aggTrade")
            self.assertFalse(venue.protocol_errors)
        self.assert_events(result, path, 1, 50, "MAX_EVENTS")
        self.assertEqual(result["schema"], "FTDI-Live-Session-v1")
        self.assertEqual(result["session_id"], path.name)
        self.assertEqual(result["symbol"], SYMBOL)
        self.assertEqual(result["lane"], {k: core_event(1)[k]
                                         for k in ("source", "symbol", "venue", "channel")})
        self.assertEqual(result["chain"], "FIRST")
        self.assertIsNone(result["previous"])
        self.assertEqual(result["sealed_by"], "capture")
        self.assertIs(result["public_network"], False)
        self.assertEqual(result["endpoints"], {"ws": venue.ws_url, "rest": venue.rest_url})
        for key in ("software_version", "started_utc", "sealed_utc", "limits"):
            self.assertTrue(result[key])
        self.assertTrue(COUNTS <= result["counts"].keys())
        for value in result["counts"].values():
            self.assertIs(type(value), int)
            self.assertGreaterEqual(value, 0)
        raw = read_rows(path / "raw_ws.jsonl")
        self.assertGreaterEqual(len(raw), 50)
        self.assertEqual([json.loads(row["text"]) for row in raw[:50]],
                         [trade(i) for i in range(1, 51)])
        self.assertEqual([row["n"] for row in raw], list(range(len(raw))))
        self.assertEqual(result["counts"]["ws_messages"], len(raw))
        for row in raw:
            self.assertTrue({"conn", "n", "read_wall_ns", "read_perf_ns",
                             "queue_depth", "text"} <= row.keys())
            self.assertEqual(row["conn"], 1)
            for key in ("n", "read_wall_ns", "read_perf_ns", "queue_depth"):
                self.assertIs(type(row[key]), int)
                self.assertGreaterEqual(row[key], 0)
        self.assertEqual([r["read_perf_ns"] for r in raw],
                         sorted(r["read_perf_ns"] for r in raw))
        clocks = read_rows(path / "clock.jsonl")
        self.assertGreaterEqual(len(clocks), 1)
        self.assertEqual(result["counts"]["clock_samples"], len(clocks))
        for row in clocks:
            self.assertTrue({"t0_wall_ns", "t0_perf_ns", "venue_ms",
                             "t1_wall_ns", "t1_perf_ns"} <= row.keys())
            self.assertGreaterEqual(row["t1_perf_ns"], row["t0_perf_ns"])
        rest = read_rows(path / "raw_rest.jsonl")
        for row in rest:
            self.assertTrue({"n", "kind", "url", "status", "t0_wall_ns", "t0_perf_ns",
                             "t1_wall_ns", "t1_perf_ns", "body", "error"} <= row.keys())
            self.assertIn(row["kind"], ("time", "aggTrades"))
            self.assertEqual(row["status"], 200)
            self.assertIsNone(row["error"])
        self.verify_readonly(path)

    def test_identical_written_and_buffered_duplicates(self):
        script = [("trades", 1, 5), ("trade", 3), ("trade", 7),
                  ("trade", 7), ("trade", 6), ("trades", 8, 10)]
        with self.venue(connections=[script]) as venue:
            result, path = self.capture(venue, max_events=10)
        self.assert_events(result, path, 1, 10, "MAX_EVENTS")
        self.assertEqual(result["counts"]["ws_duplicates"], 2)
        self.assertEqual(result["counts"]["ws_messages"], 12)
        self.verify_readonly(path)

    def test_buffered_ws_conflict_stops_before_conflicting_sequence(self):
        script = [("trades", 1, 5), ("trade", 7),
                  ("trade_with", 7, {"p": "99999"}), ("trades", 6, 10)]
        with self.venue(connections=[script]) as venue:
            result, path = self.capture(venue, max_events=10)
        self.assert_events(result, path, 1, 5, "HALTED_CONFLICT")
        self.verify_readonly(path)

    def test_already_written_conflict_truncates_to_safe_prefix(self):
        script = [("trades", 1, 8), ("trade_with", 6, {"p": "99999"}), ("trade", 9)]
        with self.venue(connections=[script]) as venue:
            result, path = self.capture(venue, max_events=20)
        self.assert_events(result, path, 1, 5, "HALTED_CONFLICT")
        self.verify_readonly(path)

    def test_rest_repairs_gap_without_skipping(self):
        with self.venue(connections=[[("trades", 1, 10), ("trades", 16, 30)]],
                        rest_available=(1, 30)) as venue:
            result, path = self.capture(venue)
            requests = [q for p, q in venue.requests if p == "/api/v3/aggTrades"]
        self.assert_events(result, path, 1, 30, "MAX_EVENTS")
        self.assertGreaterEqual(result["counts"]["gaps_repaired"], 1)
        self.assertGreaterEqual(result["counts"]["rest_records"], 5)
        self.assertGreaterEqual(result["counts"]["rest_pages"], 1)
        self.assertEqual(int(requests[0]["fromId"]), 11)
        self.assertEqual(int(requests[0]["limit"]), 5)
        self.assertTrue(all(q["symbol"] == SYMBOL and 1 <= int(q["limit"]) <= 1000
                            for q in requests))
        self.verify_readonly(path)

    def test_empty_rest_gap_is_blocked(self):
        with self.venue(connections=[[("trades", 1, 10), ("trades", 16, 30)]]) as venue:
            result, path = self.capture(venue, limits=fast_limits(max_rest_pages_per_gap=2))
        self.assert_events(result, path, 1, 10, "GAP_BLOCKED")
        self.assertEqual(result["counts"]["rest_records"], 0)
        self.assertGreaterEqual(result["counts"]["rest_pages"], 1)
        self.verify_readonly(path)

    def test_rest_conflict_with_buffered_ws(self):
        # Normal FakeVenue pages respect limit and cannot reach the buffered
        # boundary. Materialize the same rest_overrides in a scripted overlap
        # page, without relying on the scheduling of reader and REST threads.
        overrides = {8: {"p": "99999"}}
        body = [rest_trade(i) for i in range(6, 9)]
        for record in body:
            record.update(overrides.get(record["a"], {}))
        with self.venue(connections=[[("trades", 1, 5), ("trade", 8)]],
                        rest_overrides=overrides,
                        rest_queue=[(200, {}, canonical(body))]) as venue:
            result, path = self.capture(venue, max_events=20)
        self.assert_events(result, path, 1, 7, "HALTED_CONFLICT")
        self.assertGreaterEqual(result["counts"]["rest_records"], 1)
        self.verify_readonly(path)

    def test_nonconsecutive_rest_page_is_format_error(self):
        body = canonical([rest_trade(6), rest_trade(8)])
        with self.venue(connections=[[("trades", 1, 5), ("trade", 10)]],
                        rest_queue=[(200, {}, body)]) as venue:
            result, path = self.capture(venue, max_events=20)
        self.assert_events(result, path, 1, 5, "FORMAT_ERROR")

    def test_http_429_and_418_stop_and_preserve_retry_after(self):
        for status in (429, 418):
            with self.subTest(status=status):
                marker = "731"
                with self.venue(connections=[[("trades", 1, 5), ("trade", 10)]],
                                rest_queue=[(status, {"Retry-After": marker},
                                             '{"code":-1003}')]) as venue:
                    result, path = self.capture(venue, max_events=20)
                    requests = [q for p, q in venue.requests if p == "/api/v3/aggTrades"]
                self.assert_events(result, path, 1, 5, "RATE_LIMITED")
                responses = [r for r in read_rows(path / "raw_rest.jsonl")
                             if r["status"] == status]
                self.assertEqual(len(responses), 1)
                self.assertTrue(contains_value(responses[0], marker), responses)
                self.assertEqual(len(requests), 1, "rate-limited request was retried")

    def test_reconnect_repairs_missing_range(self):
        scripts = [[("trades", 1, 10), ("drop",)], [("trades", 15, 25)]]
        with self.venue(connections=scripts, rest_available=(1, 25)) as venue:
            result, path = self.capture(venue, max_events=25)
        self.assert_events(result, path, 1, 25, "MAX_EVENTS")
        self.assertGreaterEqual(result["counts"]["reconnects"], 1)
        self.assertGreaterEqual(result["counts"]["connections"], 2)
        self.assertGreaterEqual(result["counts"]["rest_records"], 4)
        raw = read_rows(path / "raw_ws.jsonl")
        self.assertEqual({r["conn"] for r in raw}, {1, 2})
        self.assertEqual([r["n"] for r in raw], list(range(len(raw))))
        self.verify_readonly(path)

    def test_reconnect_budget_exhausted(self):
        with self.venue(connections=[[("trades", 1, 3), ("drop",)], [("drop",)]]) as venue:
            result, path = self.capture(venue, max_events=20,
                                        limits=fast_limits(max_reconnects=1))
        self.assert_events(result, path, 1, 3, "CONNECTION_FAILED")
        self.assertEqual(result["counts"]["reconnects"], 1)
        self.verify_readonly(path)

    def test_fragmented_message_and_matching_pong(self):
        payload = b"contract-ping\x00\xff"
        script = [("fragmented", 1, 4), ("ping", payload), ("sleep", 0.1),
                  ("trades", 2, 3)]
        with self.venue(connections=[script]) as venue:
            result, path = self.capture(venue, max_events=3)
        self.assert_events(result, path, 1, 3, "MAX_EVENTS")
        self.assertIn((1, payload), venue.pongs)
        self.assertFalse(venue.protocol_errors)
        self.assertEqual(result["counts"]["ws_messages"], 3)
        self.verify_readonly(path)

    def test_invalid_frames_preserve_prior_events(self):
        cases = {
            "wrong_symbol": ("trade_with", 4, {"s": "ETHUSDT"}),
            "missing_field": ("trade_with", 4, {"q": None}),
            "float_id": ("trade_with", 4, {"a": 4.0}),
            "boolean_id": ("trade_with", 4, {"a": True}),
            "negative_price": ("trade_with", 4, {"p": "-1"}),
            "binary": ("binary", b"not text"),
            "oversize": ("oversize", 1024),
            "masked_server": ("masked", 4),
            "invalid_json": ("text", "{broken"),
        }
        for label, bad in cases.items():
            with self.subTest(frame=label):
                # A valid trade fits below 512 bytes; the bad text does not.
                limits = fast_limits(max_message_bytes=512)
                with self.venue(connections=[[("trades", 1, 3), bad]]) as venue:
                    result, path = self.capture(venue, max_events=10, limits=limits)
                self.assert_events(result, path, 1, 3, "FORMAT_ERROR")
                self.verify_readonly(path)
                if bad[0] in ("trade_with", "text"):
                    raw = read_rows(path / "raw_ws.jsonl")
                    self.assertGreaterEqual(len(raw), 4, "invalid text lost from raw evidence")

    def test_text_variants_keep_first_origin_text(self):
        # Consecutive over-return ensures REST overlaps already buffered WS seq 8.
        body = [rest_trade(i) for i in range(6, 9)]
        body[-1]["p"] = body[-1]["p"].rstrip("0")
        # The next gap prevents MAX_EVENTS from stopping before REST's overlap
        # record has been compared; normal REST then supplies missing seq 9.
        with self.venue(connections=[[("trades", 1, 5), ("trade", 8), ("trade", 10)]],
                        rest_available=(1, 10),
                        rest_queue=[(200, {}, canonical(body))]) as venue:
            result, path = self.capture(venue, max_events=10)
        self.assert_events(result, path, 1, 10, "MAX_EVENTS")
        self.assertGreaterEqual(result["counts"]["text_variants"], 1)
        self.verify_readonly(path)

    def test_pending_overflow_keeps_contiguous_prefix(self):
        with self.venue(connections=[[("trade", 1), ("trades", 4, 7)]]) as venue:
            result, path = self.capture(venue, limits=fast_limits(max_pending=2))
        self.assert_events(result, path, 1, 1, "PENDING_OVERFLOW")
        self.verify_readonly(path)

    def test_max_seconds_seals_empty_session(self):
        with self.venue(connections=[[]]) as venue:
            result, path = self.capture(venue, max_seconds=0.15)
        self.assert_events(result, path, 1, 0, "MAX_SECONDS")
        self.verify_readonly(path)

    def test_previous_session_catchup_and_manifest_hash_chain(self):
        first, previous = self.sealed_fixture(5)
        previous_hash = digest((previous / MANIFEST).read_bytes())
        with self.venue(connections=[[("trade", 3), ("trades", 9, 12)]],
                        rest_available=(1, 12)) as venue:
            result, path = self.capture(venue, max_events=7, previous_session=previous)
        self.assert_events(result, path, 6, 12, "MAX_EVENTS")
        self.assertEqual(result["chain"], "CONTIGUOUS")
        self.assertEqual(result["previous"], {
            "session_id": first["session_id"], "last_seq": 5,
            "manifest_sha256": previous_hash})
        self.assertEqual(result["counts"]["before_anchor"], 1)
        self.assertGreaterEqual(result["counts"]["rest_records"], 3)
        self.verify_readonly(path, previous=previous)
        # Copies retain the directory basename/session_id, so only the stated
        # chain fact changes. Exercise every expressly pinned previous field.
        for field, value in (("session_id", "session-20000101T000000Z"),
                             ("last_seq", 4), ("manifest_sha256", "0" * 64)):
            with self.subTest(chain_field=field):
                copied = self.copy_session(path, "chain_" + field)
                changed = dict(result["previous"])
                changed[field] = value
                self.rewrite_manifest(copied, previous=changed)
                self.verify_readonly(copied, previous=previous, status="INVALID",
                                     codes=("CHAIN_MISMATCH",))
        copied = self.copy_session(path, "wrong_chain")
        self.rewrite_manifest(copied, chain="FIRST")
        self.verify_readonly(copied, previous=previous, status="INVALID",
                             codes=("CHAIN_MISMATCH",))
        # Semantically unchanged JSON still changes the pinned manifest bytes.
        previous_copy = self.copy_session(previous, "tampered_previous")
        path = self.copy_session(path, "previous_was_changed")
        with (previous_copy / MANIFEST).open("ab") as stream:
            stream.write(b" ")
        self.verify_readonly(path, previous=previous_copy, status="INVALID", codes=("CHAIN_MISMATCH",))

    def test_verify_every_integrity_code_on_independent_copies(self):
        _, source = self.sealed_fixture()
        cases = [
            ("UNSEALED", "unsealed"),
            ("MANIFEST_INVALID", "bad_manifest"),
            ("MANIFEST_INVALID", "wrong_schema"),
            ("FILE_MISSING", "missing_file"),
            ("FILE_MISMATCH", "byte_flip"),
            ("EVENT_INVALID", "invalid_event"),
            ("EVENT_SEQUENCE_GAP", "sequence_gap"),
            ("EVENT_WITHOUT_RAW_EVIDENCE", "no_evidence"),
            ("RAW_CONFLICT", "raw_conflict"),
            ("RAW_UNPARSEABLE", "raw_json"),
            ("RAW_UNPARSEABLE", "raw_shape"),
        ]
        for code, mutation in cases:
            with self.subTest(mutation=mutation):
                path = self.copy_session(source, mutation)
                if mutation == "unsealed":
                    (path / MANIFEST).unlink()
                elif mutation == "bad_manifest":
                    (path / MANIFEST).write_bytes(b"{")
                elif mutation == "wrong_schema":
                    self.rewrite_manifest(path, schema="wrong")
                elif mutation == "missing_file":
                    (path / "clock.jsonl").unlink()
                elif mutation == "byte_flip":
                    data = bytearray((path / "events.jsonl").read_bytes())
                    data[10] ^= 1
                    (path / "events.jsonl").write_bytes(data)
                elif mutation in ("invalid_event", "sequence_gap", "no_evidence"):
                    rows = read_rows(path / "events.jsonl")
                    changes = {}
                    if mutation == "invalid_event":
                        rows[2]["price"] = "-1"
                    elif mutation == "sequence_gap":
                        del rows[2]
                        changes["event_count"] = len(rows)
                    else:
                        rows[2]["price"] = "123456.78"
                    self.rewrite_file(path, "events.jsonl", json_lines(rows), **changes)
                else:
                    rows = read_rows(path / "raw_ws.jsonl")
                    if mutation == "raw_conflict":
                        record = json.loads(rows[2]["text"])
                        record["p"] = "123456.78"
                        rows[2]["text"] = canonical(record)
                    elif mutation == "raw_shape":
                        rows[2] = {"unexpected": True}
                    data = json_lines(rows)
                    if mutation == "raw_json":
                        data = b"{broken}\n" + b"".join(data.splitlines(keepends=True)[1:])
                    self.rewrite_file(path, "raw_ws.jsonl", data)
                self.verify_readonly(path, status="INVALID", codes=(code,))
        self.verify_readonly(source)

    def test_verify_changed_price_cannot_be_authorized_by_rehashed_manifest(self):
        _, source = self.sealed_fixture()
        path = self.copy_session(source, "forged")
        rows = read_rows(path / "events.jsonl")
        rows[1]["price"] = "98765"
        self.rewrite_file(path, "events.jsonl", json_lines(rows))
        report = self.verify_readonly(path, status="INVALID")
        self.assertTrue({"EVENT_WITHOUT_RAW_EVIDENCE", "RAW_CONFLICT"} &
                        {k for k, v in report["issue_counts"].items() if v > 0}, report)
        self.assertFalse(report["issue_counts"].get("FILE_MISMATCH", 0))

    def test_recovery_dry_run_apply_and_refuse_already_sealed(self):
        _, source = self.sealed_fixture()
        path = self.unsealed_copy(source)
        before = snapshot(self.base)
        report = seal_session(path)
        self.assertEqual(snapshot(self.base), before)
        summary = recovery_summary(report)
        self.assertEqual({k: summary[k] for k in ("first_seq", "last_seq", "event_count")},
                         {"first_seq": 1, "last_seq": 5, "event_count": 5})
        part_bytes = {p.name: p.read_bytes() for p in path.glob("*.part")}
        seal_session(path, apply=True)
        manifest = json.loads((path / MANIFEST).read_bytes())
        self.assertEqual(manifest["sealed_by"], "recovery")
        self.assertEqual(manifest["stop_reason"], "RECOVERED")
        self.assertEqual(manifest["event_count"], 5)
        self.assertEqual((path / "events.jsonl").read_bytes(), json_lines(core_event(i) for i in range(1, 6)))
        self.assertEqual(manifest["recovered_from"],
                         {n: {"bytes": len(b), "sha256": digest(b)} for n, b in part_bytes.items()})
        for name, data in part_bytes.items():
            self.assertEqual((path / name).read_bytes(), data)
        for name in DATA_FILES:
            data = (path / name).read_bytes()
            self.assertEqual(data, (source / name).read_bytes())
            self.assertEqual(manifest["files"][name], file_info(data))
        self.verify_readonly(path, codes=("LEFTOVER_PART_FILE",))
        before = snapshot(self.base)
        with self.assertRaises(ValueError):
            seal_session(path, apply=True)
        self.assertEqual(snapshot(self.base), before)

    def test_recovery_stops_at_first_unsupported_event(self):
        _, source = self.sealed_fixture()
        events = [core_event(i) for i in range(1, 6)]
        events[2]["price"] = "123456"
        path = self.unsealed_copy(source, events=events)
        before = snapshot(self.base)
        summary = recovery_summary(seal_session(path))
        self.assertEqual(summary["event_count"], 2)
        self.assertEqual(snapshot(self.base), before)
        seal_session(path, apply=True)
        self.assertEqual((path / "events.jsonl").read_bytes(),
                         json_lines(core_event(i) for i in (1, 2)))
        self.verify_readonly(path)

    def test_replay_delivery_archive_aware_audit_complete(self):
        from ftdi_core.archive_e2e import reconcile_archive_aware
        from ftdi_core.delivery import local_lab
        from ftdi_core.service import replay
        from test_archive_e2e import sql

        result, path = self.sealed_fixture(12)
        run_root, consumer = self.base / "run", self.base / "consumer.sqlite3"
        events = path / "events.jsonl"
        self.assertIs(replay(run_root, [f"LIVE={events}"])["complete"], True)
        self.allow_consumer = True
        self.assertIs(local_lab(run_root, consumer)["pass"], True)
        report = reconcile_archive_aware(run_root, consumer)
        self.assertEqual(report["status"], "COMPLETE", report)
        self.assertEqual(report["metrics"]["matched_effects"], result["event_count"])
        rows = sql(consumer, "SELECT seq FROM effects ORDER BY seq")
        self.assertEqual([row["seq"] for row in rows], list(range(1, 13)))

    def test_cli_capture_success_failure_and_verify_exit_codes(self):
        # Exercise ensure_ascii at the CLI boundary with an actual Unicode path.
        self.capture_root = self.base / "captures-\u4f1a\u8bdd"
        for bad, expected, reason in ((False, 0, "MAX_EVENTS"), (True, 2, "FORMAT_ERROR")):
            with self.subTest(bad_frame=bad):
                script = [("trades", 1, 3)]
                if bad:
                    script.append(("trade_with", 4, {"p": "-1"}))
                with self.venue(connections=[script]) as venue:
                    result = self.cli([
                        "live-capture", "--symbol", SYMBOL, "--capture-dir", str(self.capture_root),
                        "--ws-url", venue.ws_url, "--rest-url", venue.rest_url,
                        "--max-events", "10" if bad else "3", "--max-seconds", "3",
                    ], expected)
                self.assertEqual(result["stop_reason"], reason)
                path = Path(result["session_dir"])
                self.assert_events(result, path, 1, 3, reason)
                self.assertEqual(self.cli(["verify-session", "--session", str(path)], 0)["status"],
                                 "VERIFIED")
                (path / "events.jsonl").write_bytes(b"tampered\n")
                before = snapshot(self.base)
                self.assertEqual(self.cli(["verify-session", "--session", str(path)], 2)["status"],
                                 "INVALID")
                self.assertEqual(snapshot(self.base), before)

    def test_cli_recovery_and_all_three_precondition_exit_codes(self):
        _, source = self.sealed_fixture()
        path = self.unsealed_copy(source)
        before = snapshot(self.base)
        summary = recovery_summary(self.cli(["seal-session", "--session", str(path)], 0))
        self.assertEqual(summary["event_count"], 5)
        self.assertEqual(snapshot(self.base), before)
        self.cli(["seal-session", "--session", str(path), "--apply"], 0)
        self.verify_readonly(path)
        commands = [
            ["live-capture", "--symbol", SYMBOL, "--capture-dir", str(self.base / "refused"),
             "--ws-url", "ws://localhost:12345", "--rest-url", "http://127.0.0.1:12346"],
            ["verify-session", "--session", str(self.base / "missing")],
            ["seal-session", "--session", str(path), "--apply"],
        ]
        for args in commands:
            with self.subTest(command=args[0]):
                before = snapshot(self.base)
                with patch("socket.create_connection", side_effect=AssertionError("network")) as create, \
                     patch("socket.socket.connect", side_effect=AssertionError("network")) as connect:
                    self.cli(args, 3, json_required=False)
                    create.assert_not_called()
                    connect.assert_not_called()
                self.assertEqual(snapshot(self.base), before)


if __name__ == "__main__":
    unittest.main()

