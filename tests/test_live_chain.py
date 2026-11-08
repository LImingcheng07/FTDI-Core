"""Task 004 phase 2: composing a chain of sealed sessions for replay, and latency evidence of a session."""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import unittest

from fakevenue import FakeVenue, core_event
from ftdi_core.archive_e2e import reconcile_archive_aware
from ftdi_core.delivery import local_lab
from ftdi_core.live import CaptureLimits, capture_session
from ftdi_core.live.compose import compose_sessions
from ftdi_core.live.latency import analyze_session
from ftdi_core.service import replay

FAST = CaptureLimits(gap_wait_ms=20, gap_timeout_s=1.0, rest_min_interval_s=0, clock_interval_s=3600,
                     ws_read_timeout_s=2.0, max_reconnects=1)


def lines(path):
    return [json.loads(line) for line in Path(path).read_bytes().decode('utf-8').splitlines()]


def effects(consumer):
    with closing(sqlite3.connect(f'{Path(consumer).as_uri()}?mode=ro', uri=True)) as db:
        return db.execute('SELECT COUNT(*) FROM effects').fetchone()[0]


class ChainTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='live_chain_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    def session(self, script, *, max_events, previous=None, rest_available=(0, -1), max_seconds=10.0):
        with FakeVenue(connections=[script], rest_available=rest_available) as venue:
            result = capture_session(self.base / 'captures', symbol='BTCUSDT', ws_url=venue.ws_url,
                                     rest_url=venue.rest_url, max_events=max_events, max_seconds=max_seconds,
                                     previous_session=previous, limits=FAST)
        time.sleep(1.05)   # one session directory name per second
        return Path(result['session_dir']), result

    def chain(self):
        s1, r1 = self.session([('trades', 1, 20)], max_events=20)
        s2, r2 = self.session([('hold',)], max_events=5, previous=s1, max_seconds=0.5)   # quiet market: empty
        s3, r3 = self.session([('trades', 25, 40)], max_events=20, previous=s1, rest_available=(21, 40))
        self.assertEqual((r1['event_count'], r2['event_count'], r3['event_count']), (20, 0, 20))
        return [s1, s2, s3]

    def test_chain_composes_replays_delivers_and_audits(self):
        sessions = self.chain()
        composed = compose_sessions(sessions, self.base / 'chain1')
        self.assertEqual((composed['first_seq'], composed['last_seq'], composed['event_count']), (1, 40, 40))
        events = self.base / 'chain1' / 'events.jsonl'
        self.assertEqual(lines(events), [core_event(i) for i in range(1, 41)])
        run, consumer = self.base / 'run1', self.base / 'consumer' / 'effects.sqlite3'
        consumer.parent.mkdir()
        self.assertTrue(replay(run, [f'LIVE={events}'], batch=16)['complete'])
        self.assertTrue(local_lab(run, consumer, batch=16)['pass'])
        report = reconcile_archive_aware(run, consumer)
        self.assertEqual(report['status'], 'COMPLETE', report['issue_counts'])
        self.assertEqual(report['metrics']['matched_effects'], 40)
        # The chain grows; it is re-composed from its head into a new run: no new effect for known events.
        s4, _ = self.session([('trades', 41, 50)], max_events=10, previous=sessions[2])
        compose_sessions(sessions + [s4], self.base / 'chain2')
        run2 = self.base / 'run2'
        self.assertTrue(replay(run2, [f'LIVE={self.base / "chain2" / "events.jsonl"}'], batch=16)['complete'])
        lab = local_lab(run2, consumer, batch=16)
        self.assertTrue(lab['pass'])
        self.assertEqual(lab['delivery']['remote_duplicates'], 40)
        self.assertEqual(effects(consumer), 50)
        self.assertEqual(reconcile_archive_aware(run2, consumer)['status'], 'COMPLETE')

    def test_refusals(self):
        sessions = self.chain()
        with self.assertRaisesRegex(ValueError, 'not VERIFIED in this chain'):
            compose_sessions([sessions[2], sessions[0]], self.base / 'wrong_order')
        with self.assertRaisesRegex(ValueError, 'not VERIFIED in this chain'):
            compose_sessions([sessions[0], sessions[2], sessions[2]], self.base / 'repeated')
        tampered = self.base / 'tampered' / sessions[0].name
        shutil.copytree(sessions[0], tampered)
        data = bytearray((tampered / 'events.jsonl').read_bytes())
        data[10] ^= 1
        (tampered / 'events.jsonl').write_bytes(bytes(data))
        with self.assertRaisesRegex(ValueError, 'FILE_MISMATCH'):
            compose_sessions([tampered], self.base / 'tampered_out')
        (self.base / 'exists').mkdir()
        with self.assertRaisesRegex(ValueError, 'already exists'):
            compose_sessions(sessions, self.base / 'exists')
        with self.assertRaisesRegex(ValueError, 'empty'):
            compose_sessions([sessions[1]], self.base / 'only_empty')
        for name in ('wrong_order', 'repeated', 'tampered_out', 'only_empty'):
            self.assertFalse((self.base / name).exists())


class LatencyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='live_latency_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    def test_recent_events_give_trusted_small_latencies(self):
        script = []
        for seq in range(1, 31):   # venue time E taken when the message is sent (never ahead of the send)
            script += [('trade_now', seq), ('sleep', 0.02)]
        with FakeVenue(connections=[script]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=30, max_seconds=10.0, limits=FAST)
        report = analyze_session(result['session_dir'])
        self.assertEqual(report['events_first_delivered_by_ws'], 30)
        self.assertEqual(report['status'], 'ESTIMATED', report)
        self.assertGreaterEqual(report['trusted'], 20)
        self.assertIsInstance(report['latency_percentiles']['p50_ms'], float)
        self.assertLess(abs(report['latency_percentiles']['p50_ms']), 200)
        self.assertFalse(report['certified'])

    def test_without_an_offset_estimate_there_is_no_latency(self):
        with FakeVenue(connections=[[('trades', 1, 25)]]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=25, max_seconds=10.0, limits=FAST)
        from ftdi_core.timeprov import EventTimes, classify_events, percentile_report
        raw = lines(Path(result['session_dir']) / 'raw_ws.jsonl')
        events = [EventTimes(json.loads(r['text'])['E'], r['read_wall_ns'], r['read_perf_ns'], r['queue_depth'])
                  for r in raw]
        report = percentile_report(classify_events(events, lambda event: None))   # no sample near any event
        self.assertEqual(report['status'], 'NO_CLOCK_MODEL')
        self.assertIsNone(report['latency_percentiles']['p50_ms'])


if __name__ == '__main__':
    unittest.main()
