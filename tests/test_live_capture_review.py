"""Regressions for the independent review of live capture (L1-L12, docs/LIVE_CAPTURE.md)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

from fakevenue import FakeVenue, _frame, core_event, rest_trade, trade
from ftdi_core.live import CaptureLimits, capture_session, seal_session, verify_session
from ftdi_core.live import capture as capture_module
from ftdi_core.live.rest import RateLimited

FAST = dict(gap_wait_ms=20, gap_timeout_s=1.0, rest_min_interval_s=0, clock_interval_s=3600, ws_read_timeout_s=2.0,
            max_reconnects=1)


def lines(path):
    return [json.loads(line) for line in Path(path).read_bytes().decode('utf-8').splitlines()]


class RawServer:
    """A loopback TCP server with a custom per-connection behaviour (for cases the fake venue cannot script)."""

    def __init__(self, behaviour):
        self.behaviour = behaviour
        self.stop = threading.Event()
        self.threads = []

    def __enter__(self):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(8)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()
        return self

    def _accept(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except OSError:
                return
            thread = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            thread.start()
            self.threads.append(thread)

    def _serve(self, conn):
        try:
            self.behaviour(conn, self.stop)
        except OSError:
            pass
        finally:
            conn.close()

    def __exit__(self, *exc):
        self.stop.set()
        self.listener.close()
        self.thread.join(5)
        for thread in self.threads:
            thread.join(5)


def silent(conn, stop):
    while not stop.wait(0.05):
        pass


def trickle(conn, stop):
    conn.recv(4096)
    while not stop.wait(0.05):
        conn.sendall(b'X')


def ws_handshake(conn):
    import base64
    import hashlib
    request = b''
    while b'\r\n\r\n' not in request:
        request += conn.recv(4096)
    key = [l.split(b':', 1)[1].strip() for l in request.split(b'\r\n') if l.lower().startswith(b'sec-websocket-key')][0]
    accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
    conn.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                 b'Sec-WebSocket-Accept: ' + accept + b'\r\n\r\n')


def empty_fragments(conn, stop):
    ws_handshake(conn)
    conn.sendall(_frame(0x1, b'{', fin=False))
    for _ in range(3000):
        conn.sendall(_frame(0x0, b'', fin=False))
    while not stop.wait(0.05):
        pass


class LiveReviewRegressions(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='live_review_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    def capture(self, ws_url, rest_url, **kwargs):
        limits = CaptureLimits(**{**FAST, **kwargs.pop('limits', {})})
        kwargs.setdefault('max_events', 50)
        kwargs.setdefault('max_seconds', 10.0)
        result = capture_session(self.base / 'captures', symbol='BTCUSDT', ws_url=ws_url, rest_url=rest_url,
                                 limits=limits, **kwargs)
        return result, Path(result['session_dir'])

    def assert_verified(self, path):
        report = verify_session(path)
        self.assertEqual(report['status'], 'VERIFIED', report)
        return report

    def test_l1_messages_received_before_the_stop_are_kept_and_checked(self):
        real = capture_module.parse_ws

        def slow(text, symbol):
            time.sleep(0.05)   # the reader receives the conflicting delivery while seq 1-3 are processed
            return real(text, symbol)
        script = [('trades', 1, 3), ('trade_with', 1, {'p': '99999.00000000'})]
        with FakeVenue(connections=[script]) as venue:
            with mock.patch.object(capture_module, 'parse_ws', slow):
                result, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        texts = [json.loads(r['text']) for r in lines(path / 'raw_ws.jsonl')]
        self.assertIn('99999.00000000', [t['p'] for t in texts])   # evidence not lost
        self.assertEqual(result['stop_reason'], 'HALTED_CONFLICT')
        self.assertEqual(result['event_count'], 0)                 # seq 1 is ambiguous: nothing from it on
        self.assert_verified(path)

    def test_l1_the_message_that_overflows_the_queue_is_kept(self):
        real = capture_module.parse_ws

        def slow(text, symbol):
            time.sleep(0.05)
            return real(text, symbol)
        with FakeVenue(connections=[[('trades', 1, 30)]]) as venue:
            with mock.patch.object(capture_module, 'parse_ws', slow):
                result, path = self.capture(venue.ws_url, venue.rest_url, limits={'queue_size': 2})
        self.assertEqual(result['stop_reason'], 'READER_OVERFLOW')
        raw = lines(path / 'raw_ws.jsonl')
        self.assertGreaterEqual(len(raw), result['interpreted']['raw_ws'] + 3)   # 2 queued + 1 spilled
        self.assertEqual([json.loads(r['text'])['a'] for r in raw], list(range(1, len(raw) + 1)))
        self.assert_verified(path)

    def test_l2_a_malformed_page_does_not_hide_a_conflicting_record(self):
        body = json.dumps([{**rest_trade(1), 'p': '1.00000000'}, {'a': 'broken'}])
        with FakeVenue(connections=[[('trades', 1, 3), ('trade', 6)]], rest_queue=[(200, {}, body)]) as venue:
            result, path = self.capture(venue.ws_url, venue.rest_url)
        self.assertEqual(result['stop_reason'], 'HALTED_CONFLICT')
        self.assertEqual(result['event_count'], 0)
        report = self.assert_verified(path)
        self.assertGreater(report['issue_counts'].get('RAW_AFTER_STOP', 0), 0)

    def test_l3_max_events_reached_inside_a_page_with_a_later_conflict(self):
        page = json.dumps([rest_trade(2), {**rest_trade(3), 'p': '1.00000000'}])   # longer than requested
        with FakeVenue(connections=[[('trade', 1), ('trade', 3)]], rest_queue=[(200, {}, page)]) as venue:
            result, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        self.assertEqual(result['stop_reason'], 'HALTED_CONFLICT')
        self.assertEqual((result['first_seq'], result['last_seq']), (1, 2))
        self.assert_verified(path)

    def test_l4_a_silent_rest_server_cannot_hold_the_capture(self):
        with FakeVenue(connections=[[('hold',)]]) as venue, RawServer(silent) as rest:
            started = time.perf_counter()
            result, path = self.capture(venue.ws_url, f'http://127.0.0.1:{rest.port}', max_seconds=1.0)
            elapsed = time.perf_counter() - started
        self.assertEqual(result['stop_reason'], 'MAX_SECONDS')
        self.assertLess(elapsed, 1.0 + 2.0 + 1.5)   # deadline + final sample budget + slack
        self.assert_verified(path)

    def test_l4_a_trickling_handshake_is_bounded(self):
        with RawServer(trickle) as ws, FakeVenue() as venue:
            started = time.perf_counter()
            result, _ = self.capture(f'ws://127.0.0.1:{ws.port}', venue.rest_url, max_seconds=1.0,
                                     limits={'ws_read_timeout_s': 0.3, 'max_reconnects': 0})
            elapsed = time.perf_counter() - started
        self.assertEqual(result['stop_reason'], 'CONNECTION_FAILED')
        self.assertLess(elapsed, 3.0)

    def test_l5_a_time_answer_without_server_time_is_harmless(self):
        real = capture_module.RestClient.server_time

        def empty(self, budget_s=None):
            status, body, stamps = real(self, budget_s)
            return status, '{}', stamps
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            with mock.patch.object(capture_module.RestClient, 'server_time', empty):
                result, path = self.capture(venue.ws_url, venue.rest_url, max_events=5)
        self.assertEqual((result['stop_reason'], result['counts']['clock_samples']), ('MAX_EVENTS', 0))
        self.assert_verified(path)

    def test_l6_hostile_manifests_never_crash_verification(self):
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        for label, change in (('huge last_seq', {'last_seq': 10 ** 30}), ('list stop_reason', {'stop_reason': []}),
                              ('negative count', {'counts': {**json.loads((path / 'SESSION_MANIFEST.json').read_text(
                                  encoding='utf-8'))['counts'], 'ws_messages': -1}}),
                              ('dict session_id', {'session_id': {}})):
            with self.subTest(label=label):
                copy = self.base / label.replace(' ', '_') / path.name
                shutil.copytree(path, copy)
                manifest = json.loads((copy / 'SESSION_MANIFEST.json').read_text(encoding='utf-8'))
                manifest.update(change)
                (copy / 'SESSION_MANIFEST.json').write_bytes(json.dumps(manifest).encode())
                report = verify_session(copy)
                self.assertEqual(report['status'], 'INVALID')

    def test_l7_an_oversized_rest_body_is_a_format_error_with_evidence(self):
        with FakeVenue(connections=[[('trades', 1, 2), ('trade', 5)]], rest_queue=[(200, {}, 'x' * 300)]) as venue:
            result, path = self.capture(venue.ws_url, venue.rest_url, limits={'max_rest_body_bytes': 128})
        self.assertEqual(result['stop_reason'], 'FORMAT_ERROR')
        agg = [r for r in lines(path / 'raw_rest.jsonl') if r['kind'] == 'aggTrades']
        self.assertTrue(agg[0]['body_base64'])
        self.assert_verified(path)

    def test_l8_capture_start_and_recovered_parts_are_checked(self):
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        copy = self.base / 'start' / path.name
        shutil.copytree(path, copy)
        start = json.loads((copy / 'CAPTURE_START.json').read_text(encoding='utf-8'))
        start['symbol'] = 'ETHUSDT'
        data = json.dumps(start).encode()
        (copy / 'CAPTURE_START.json').write_bytes(data)
        manifest = json.loads((copy / 'SESSION_MANIFEST.json').read_text(encoding='utf-8'))
        import hashlib
        manifest['capture_start_sha256'] = hashlib.sha256(data).hexdigest()
        (copy / 'SESSION_MANIFEST.json').write_bytes(json.dumps(manifest).encode())
        self.assertIn('MANIFEST_INVALID', verify_session(copy)['issue_counts'])
        recovered = self.unsealed(path, 'recover')
        seal_session(recovered, apply=True)
        self.assert_verified(recovered)
        with open(recovered / 'raw_ws.jsonl.part', 'ab') as handle:
            handle.write(b'tampered')
        self.assertIn('FILE_MISMATCH', verify_session(recovered)['issue_counts'])

    def unsealed(self, path, label, *, keep_final=()):
        target = self.base / label / path.name
        target.mkdir(parents=True)
        shutil.copy(path / 'CAPTURE_START.json', target / 'CAPTURE_START.json')
        for name in ('events.jsonl', 'raw_ws.jsonl', 'raw_rest.jsonl', 'clock.jsonl'):
            shutil.copy(path / name, target / (name if name in keep_final else f'{name}.part'))
        return target

    def test_l9_recovery_refuses_corrupt_raw_numbering(self):
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        target = self.unsealed(path, 'bad_n')
        rows = lines(target / 'raw_ws.jsonl.part')
        rows[1]['n'] = 7
        (target / 'raw_ws.jsonl.part').write_bytes(''.join(json.dumps(r, sort_keys=True, separators=(',', ':'),
                                                                      ensure_ascii=False) + '\n' for r in rows).encode())
        before = sorted(p.name for p in target.iterdir())
        with self.assertRaisesRegex(ValueError, 'not consecutive'):
            seal_session(target, apply=True)
        self.assertEqual(sorted(p.name for p in target.iterdir()), before)

    def test_l10_recovery_after_an_interrupted_seal(self):
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        target = self.unsealed(path, 'half_renamed', keep_final=('events.jsonl', 'raw_ws.jsonl'))
        sealed = seal_session(target, apply=True)
        self.assertEqual(sealed['event_count'], 3)
        self.assertEqual(set(sealed['recovered_from']), {'raw_rest.jsonl.part', 'clock.jsonl.part'})
        self.assert_verified(target)

    def test_l11_empty_continuation_frames_are_bounded(self):
        with RawServer(empty_fragments) as ws, FakeVenue() as venue:
            result, _ = self.capture(f'ws://127.0.0.1:{ws.port}', venue.rest_url, max_seconds=5.0)
        self.assertEqual(result['stop_reason'], 'FORMAT_ERROR')
        self.assertIn('frames', result['stop_detail'])

    # -- second review (M1-M7) ---------------------------------------------------------------------

    def test_m1_conflicts_inside_an_unusable_body_still_count(self):
        conflicting = json.dumps([{**rest_trade(1), 'p': '1.00000000'}])
        for label, body, limits in (('not utf-8', conflicting.encode()[:-1] + b'\xff]', {}),
                                    ('too large', conflicting[:-1] + ',' + ' ' * 400 + ']', {'max_rest_body_bytes': 200})):
            with self.subTest(label=label):
                with FakeVenue(connections=[[('trades', 1, 2), ('trade', 4)]], rest_queue=[(200, {}, body)]) as venue:
                    result, path = self.capture(venue.ws_url, venue.rest_url, limits=limits)
                self.assertEqual(result['stop_reason'], 'HALTED_CONFLICT', result['stop_detail'])
                self.assertEqual(result['event_count'], 0)
                self.assert_verified(path)
                time.sleep(1.05)   # next session directory name

    def test_m2_a_session_changed_during_composition_is_refused(self):
        from ftdi_core.live import compose as compose_module
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        real = compose_module.verify_session

        def verify_then_change_original(session, previous=None):
            report = real(session, previous=previous)
            # a writer appends to the ORIGINAL session while it is being composed (and leaves it changed)
            (path / 'clock.jsonl').write_bytes((path / 'clock.jsonl').read_bytes() + b'\n')
            return report
        with mock.patch.object(compose_module, 'verify_session', verify_then_change_original):
            with self.assertRaisesRegex(ValueError, 'changed during composition'):
                compose_module.compose_sessions([path], self.base / 'chain')
        self.assertFalse((self.base / 'chain').exists())

    def test_n1_a_transient_change_of_an_original_never_reaches_the_output(self):
        from ftdi_core.live import compose as compose_module
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        original = (path / 'events.jsonl').read_bytes()
        forged = original.replace(b'"67002.02000000"', b'"99999"')
        self.assertNotEqual(forged, original)
        original_manifest = (path / 'SESSION_MANIFEST.json').read_bytes()
        manifest = json.loads(original_manifest)
        import hashlib
        manifest['files']['events.jsonl'] = {'bytes': len(forged), 'sha256': hashlib.sha256(forged).hexdigest(),
                                             'lines': forged.count(b'\n')}
        forged_manifest = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode() + b'\n'
        real_verify, real_write = compose_module.verify_session, compose_module._write_events

        def verify_then_forge(session, previous=None):
            report = real_verify(session, previous=previous)
            # after verification: events and manifest forged consistently
            (path / 'events.jsonl').write_bytes(forged)
            (path / 'SESSION_MANIFEST.json').write_bytes(forged_manifest)
            return report

        def write_then_restore(*args, **kwargs):
            try:
                return real_write(*args, **kwargs)
            finally:   # restored before the final fingerprint
                (path / 'events.jsonl').write_bytes(original)
                (path / 'SESSION_MANIFEST.json').write_bytes(original_manifest)
        with mock.patch.object(compose_module, 'verify_session', verify_then_forge), \
                mock.patch.object(compose_module, '_write_events', write_then_restore):
            compose_module.compose_sessions([path], self.base / 'chain')
        self.assertEqual((self.base / 'chain' / 'events.jsonl').read_bytes(), original)
        self.assertEqual(sorted(p.name for p in (self.base / 'chain').iterdir()), ['CHAIN_MANIFEST.json', 'events.jsonl'])

    def test_n2_loopback_port_80_sessions_verify(self):
        from ftdi_core.live.session import options_problem
        from ftdi_core.live.limits import CaptureLimits as Limits
        from dataclasses import asdict
        limits = asdict(Limits())
        self.assertIsNone(options_problem({'ws': 'ws://127.0.0.1:80', 'rest': 'http://127.0.0.1:80'}, False, limits))
        for port in ('65536', '99999', '0'):
            self.assertIsNotNone(options_problem({'ws': f'ws://127.0.0.1:{port}', 'rest': 'http://127.0.0.1:2'},
                                                 False, limits))
        from ftdi_core.live.endpoints import resolve
        self.assertEqual(resolve('ws://127.0.0.1:80', 'http://127.0.0.1:80', False).rest.base, 'http://127.0.0.1:80')

    def test_n5_recovery_refuses_what_would_not_verify(self):
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        target = self.unsealed(path, 'bad_base64')
        rows = lines(target / 'raw_rest.jsonl.part')
        rows.append({'n': len(rows), 'kind': 'aggTrades', 'url': 'x', 'status': 200, 't0_wall_ns': 1, 't0_perf_ns': 1,
                     't1_wall_ns': 2, 't1_perf_ns': 2, 'body': None, 'body_base64': '!!not base64!!', 'error': 'x'})
        (target / 'raw_rest.jsonl.part').write_bytes(''.join(
            json.dumps(r, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n' for r in rows).encode())
        before = sorted(p.name for p in target.iterdir())
        with self.assertRaisesRegex(ValueError, 'would not verify'):
            seal_session(target, apply=True)
        self.assertEqual(sorted(p.name for p in target.iterdir()), before)
        self.assertFalse(any(p.name.endswith('.recovery-check') for p in target.parent.iterdir()))

    def test_n6_a_failing_chain_manifest_write_leaves_no_output(self):
        from ftdi_core.live import compose as compose_module
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        with mock.patch.object(compose_module, 'write_new_json', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(OSError, 'disk full'):
                compose_module.compose_sessions([path], self.base / 'chain')
        self.assertFalse((self.base / 'chain').exists())

    def test_m3_a_body_trickled_after_the_headers_is_bounded(self):
        from ftdi_core.live.endpoints import Endpoint
        from ftdi_core.live.rest import RestClient

        def headers_then_trickle(conn, stop):
            conn.recv(4096)
            conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n'
                         b'Connection: close\r\n\r\n')
            while not stop.wait(0.05):
                conn.sendall(b' ')
        records = []
        with RawServer(headers_then_trickle) as server:
            client = RestClient(Endpoint('http', '127.0.0.1', server.port), symbol='BTCUSDT', recorder=records.append,
                                min_interval_s=0, max_body_bytes=4096, timeout_s=8.0)
            started = time.perf_counter()
            status, body, _ = client.server_time(budget_s=0.5)
            elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 1.5)
        self.assertIsNone(body)
        self.assertIn('no complete answer', records[0]['error'])

    def test_m5_recovery_validates_the_start_record_before_writing(self):
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        for label, change in (('bad previous', {'previous': {'last_seq': 0}}), ('bad start time', {'started_utc': []})):
            with self.subTest(label=label):
                target = self.unsealed(path, label.replace(' ', '_'))
                start = json.loads((target / 'CAPTURE_START.json').read_text(encoding='utf-8'))
                start.update(change)
                (target / 'CAPTURE_START.json').write_bytes(json.dumps(start).encode())
                before = sorted(p.name for p in target.iterdir())
                with self.assertRaisesRegex(ValueError, 'cannot recover|would not be valid'):
                    seal_session(target, apply=True)
                self.assertEqual(sorted(p.name for p in target.iterdir()), before)

    def test_m6_impossible_options_are_invalid_even_when_consistent(self):
        import hashlib
        with FakeVenue(connections=[[('trades', 1, 3)]]) as venue:
            _, path = self.capture(venue.ws_url, venue.rest_url, max_events=3)
        for label, change in (('public flag', {'public_network': 'garbage'}), ('limits', {'limits': None}),
                              ('endpoints', {'endpoints': {'ws': 'ws://example.org:1', 'rest': 'http://127.0.0.1:2'}})):
            with self.subTest(label=label):
                copy = self.base / label.replace(' ', '_') / path.name
                shutil.copytree(path, copy)
                start = json.loads((copy / 'CAPTURE_START.json').read_text(encoding='utf-8'))
                start.update(change)
                data = json.dumps(start).encode()
                (copy / 'CAPTURE_START.json').write_bytes(data)
                manifest = json.loads((copy / 'SESSION_MANIFEST.json').read_text(encoding='utf-8'))
                manifest.update(change, capture_start_sha256=hashlib.sha256(data).hexdigest())
                (copy / 'SESSION_MANIFEST.json').write_bytes(json.dumps(manifest).encode())
                self.assertIn('MANIFEST_INVALID', verify_session(copy)['issue_counts'])

    def test_m7_a_final_rate_limit_after_another_stop_is_noted(self):
        real, calls = capture_module.RestClient.server_time, []

        def limited_at_end(self, budget_s=None):
            calls.append(1)
            if len(calls) >= 2:
                raise RateLimited(429, '30')
            return real(self, budget_s)
        script = [('trades', 1, 2), ('text', 'not json')]
        with FakeVenue(connections=[script]) as venue:
            with mock.patch.object(capture_module.RestClient, 'server_time', limited_at_end):
                result, _ = self.capture(venue.ws_url, venue.rest_url)
        self.assertEqual(result['stop_reason'], 'FORMAT_ERROR')
        self.assertIn('rate-limited', result['stop_detail'])

    def test_l12_a_rate_limited_final_clock_sample_is_reported(self):
        real, calls = capture_module.RestClient.server_time, []

        def limited_at_end(self, budget_s=None):
            calls.append(1)
            if len(calls) >= 2:
                self.recorder({'kind': 'time', 'url': 'x', 'status': 429, 't0_wall_ns': 1, 't0_perf_ns': 1,
                               't1_wall_ns': 2, 't1_perf_ns': 2, 'body': '', 'body_base64': None, 'error': '429'})
                raise RateLimited(429, '30')
            return real(self, budget_s)
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            with mock.patch.object(capture_module.RestClient, 'server_time', limited_at_end):
                result, path = self.capture(venue.ws_url, venue.rest_url, max_events=5)
        self.assertEqual(result['stop_reason'], 'RATE_LIMITED')
        self.assertEqual(result['event_count'], 5)
        self.assert_verified(path)


if __name__ == '__main__':
    unittest.main()
