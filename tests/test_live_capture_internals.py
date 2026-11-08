"""Live capture internals: handshake proof, overflows, read timeout, interruption, clock evidence, a real kill."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from fakevenue import FakeVenue, core_event
from ftdi_core.live import CaptureLimits, capture_session, seal_session, verify_session
from ftdi_core.live import capture as capture_module
from ftdi_core.timeprov import OffsetSample, estimate_offset

ROOT = Path(__file__).resolve().parents[1]
FAST = dict(gap_wait_ms=20, gap_timeout_s=1.0, rest_min_interval_s=0, clock_interval_s=3600, ws_read_timeout_s=2.0,
            max_reconnects=2)


def lines(path):
    data = Path(path).read_bytes()
    return [json.loads(line) for line in data.decode('utf-8').splitlines()]


class LiveInternalsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='live_int_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    def capture(self, venue, **kwargs):
        limits = CaptureLimits(**{**FAST, **kwargs.pop('limits', {})})
        kwargs.setdefault('max_events', 50)
        kwargs.setdefault('max_seconds', 10.0)
        result = capture_session(self.base / 'captures', symbol='BTCUSDT', ws_url=venue.ws_url,
                                 rest_url=venue.rest_url, limits=limits, **kwargs)
        return result, Path(result['session_dir'])

    def assert_prefix(self, path, first, last):
        self.assertEqual(lines(path / 'events.jsonl'), [core_event(i) for i in range(first, last + 1)])

    def test_handshake_without_a_valid_accept_is_refused(self):
        with FakeVenue(connections=[[('trades', 1, 5)]], bad_accept=True) as venue:
            result, path = self.capture(venue)
        self.assertEqual(result['stop_reason'], 'FORMAT_ERROR')
        self.assertIn('handshake', result['stop_detail'])
        self.assertEqual(result['event_count'], 0)
        self.assertEqual(verify_session(path)['status'], 'VERIFIED')

    def test_handshake_sends_a_fresh_key_and_version_13(self):
        with FakeVenue(connections=[[('trades', 1, 2), ('drop',)], [('trades', 3, 4)]], rest_available=(1, 4)) as venue:
            self.capture(venue, max_events=4)
            keys = [h['headers']['sec-websocket-key'] for h in venue.handshakes]
            versions = {h['headers']['sec-websocket-version'] for h in venue.handshakes}
        self.assertEqual(len(keys), 2)
        self.assertEqual(len(set(keys)), 2)
        self.assertEqual(versions, {'13'})

    def test_reader_overflow_stops_without_dropping(self):
        real = capture_module.parse_ws

        def slow(text, symbol):
            time.sleep(0.02)
            return real(text, symbol)
        with FakeVenue(connections=[[('trades', 1, 200)]]) as venue:
            with mock.patch.object(capture_module, 'parse_ws', slow):
                result, path = self.capture(venue, max_events=200, limits={'queue_size': 5})
        self.assertEqual(result['stop_reason'], 'READER_OVERFLOW')
        events = lines(path / 'events.jsonl')
        self.assertEqual(events, [core_event(i) for i in range(1, len(events) + 1)])
        self.assertEqual(verify_session(path)['status'], 'VERIFIED')

    def test_pending_overflow(self):
        with FakeVenue(connections=[[('trades', 1, 3), ('trades', 10, 20)]]) as venue:
            result, path = self.capture(venue, limits={'max_pending': 4})
        self.assertEqual(result['stop_reason'], 'PENDING_OVERFLOW')
        self.assert_prefix(path, 1, 3)
        self.assertEqual(verify_session(path)['status'], 'VERIFIED')

    def test_silent_connection_times_out_and_reconnects(self):
        scripts = [[('trades', 1, 5), ('hold',)], [('trades', 6, 10)]]
        with FakeVenue(connections=scripts) as venue:
            result, path = self.capture(venue, max_events=10, limits={'ws_read_timeout_s': 0.3})
        self.assertEqual(result['stop_reason'], 'MAX_EVENTS')
        self.assertEqual(result['counts']['reconnects'], 1)
        self.assert_prefix(path, 1, 10)

    def test_interrupt_seals_what_was_written(self):
        real = capture_module._Capture._write
        calls = []

        def interrupting(self, event):
            calls.append(1)
            if len(calls) == 6:
                raise KeyboardInterrupt
            return real(self, event)
        with FakeVenue(connections=[[('trades', 1, 20)]]) as venue:
            with mock.patch.object(capture_module._Capture, '_write', interrupting):
                result, path = self.capture(venue)
        self.assertEqual(result['stop_reason'], 'INTERRUPTED')
        self.assert_prefix(path, 1, 5)
        self.assertEqual(verify_session(path)['status'], 'VERIFIED')

    def test_clock_samples_feed_the_task003_offset_model(self):
        with FakeVenue(connections=[[('trades', 1, 5)]], server_offset_ms=600) as venue:
            result, path = self.capture(venue, max_events=5)
        samples = [OffsetSample(s['t0_wall_ns'], s['venue_ms'], s['t1_wall_ns']) for s in lines(path / 'clock.jsonl')]
        self.assertGreaterEqual(len(samples), 2)
        estimate = estimate_offset(samples)
        self.assertLess(abs(estimate.offset_ms - 600), 50)   # the fake venue's clock runs 600 ms ahead
        raw = lines(path / 'raw_ws.jsonl')
        self.assertTrue(all(r['read_perf_ns'] > 0 and r['read_wall_ns'] > 0 for r in raw))
        self.assertEqual([r['read_perf_ns'] for r in raw], sorted(r['read_perf_ns'] for r in raw))

    def test_no_file_handles_left_after_an_unexpected_error(self):
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            with mock.patch.object(capture_module._Capture, '_accept', side_effect=RuntimeError('boom')):
                with self.assertRaisesRegex(RuntimeError, 'boom'):
                    self.capture(venue)
        session = next((self.base / 'captures' / 'BTCUSDT').iterdir())
        for part in session.glob('*.part'):
            os.rename(part, part.with_suffix('.moved'))   # fails on Windows if a handle were still open

    def test_a_killed_capture_is_recovered_and_verified(self):
        code = (
            'import sys; sys.path[:0] = [sys.argv[1]]\n'
            'from ftdi_core.live import CaptureLimits, capture_session\n'
            'capture_session(sys.argv[2], symbol="BTCUSDT", ws_url=sys.argv[3], rest_url=sys.argv[4], '
            'max_events=100000, max_seconds=60, limits=CaptureLimits(fsync_every=1, rest_min_interval_s=0, '
            'clock_interval_s=3600))\n')
        script = [('trades', 1, 40), ('hold',)]
        with FakeVenue(connections=[script]) as venue:
            child = subprocess.Popen([sys.executable, '-c', code, str(ROOT), str(self.base / 'captures'),
                                      venue.ws_url, venue.rest_url], stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
            try:
                deadline = time.time() + 30
                part = None
                while time.time() < deadline:
                    found = list((self.base / 'captures').glob('BTCUSDT/*/events.jsonl.part'))
                    if found and found[0].read_bytes().count(b'\n') >= 40:
                        part = found[0]
                        break
                    time.sleep(0.05)
                self.assertIsNotNone(part, 'capture did not write 40 events')
            finally:
                child.kill()
                child.wait(30)
        session = part.parent
        self.assertFalse((session / 'SESSION_MANIFEST.json').exists())
        plan = seal_session(session)
        self.assertEqual(plan['would_seal'], {'first_seq': 1, 'last_seq': 40, 'event_count': 40})
        sealed = seal_session(session, apply=True)
        self.assertEqual((sealed['stop_reason'], sealed['sealed_by']), ('RECOVERED', 'recovery'))
        report = verify_session(session)
        self.assertEqual(report['status'], 'VERIFIED', report)
        self.assertEqual(report['issue_counts'].get('LEFTOVER_PART_FILE'), 4)
        self.assert_prefix(session, 1, 40)


if __name__ == '__main__':
    unittest.main()
