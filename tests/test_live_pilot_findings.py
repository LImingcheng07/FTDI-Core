"""Regressions for the findings of the real-network pilot (docs/REAL_LIVE_PILOT_RESULTS.md), v0.4.1."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from fakevenue import FakeVenue
from ftdi_core.archive_e2e import reconcile_archive_aware
from ftdi_core.delivery import local_lab
from ftdi_core.live import CaptureLimits, capture_session
from ftdi_core.live import capture as capture_module
from ftdi_core.live.compose import compose_sessions
from ftdi_core.live.endpoints import Endpoint
from ftdi_core.live.rest import RestClient
from ftdi_core.service import replay
from ftdi_core.timeprov import EventTimes, OffsetEstimate, classify_events, percentile_report

MS = 1_000_000
FAST = CaptureLimits(gap_wait_ms=20, gap_timeout_s=1.0, rest_min_interval_s=0, clock_interval_s=3600,
                     ws_read_timeout_s=2.0, max_reconnects=1)


class PilotFindingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='live_pilot_')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)

    def test_p1_clock_sample_timing_excludes_connection_setup(self):
        records = []
        with FakeVenue() as venue:
            client = RestClient(Endpoint('http', '127.0.0.1', int(venue.rest_url.rsplit(':', 1)[1])), symbol='BTCUSDT',
                                recorder=records.append, min_interval_s=0, max_body_bytes=4096)
            import ftdi_core.live.rest as rest_module

            class SlowConnect(rest_module._HTTP):
                def connect(self):
                    time.sleep(0.3)   # e.g. a TLS handshake over a long path
                    super().connect()
            with mock.patch.object(rest_module, '_HTTP', SlowConnect):
                status, body, stamps = client.server_time()
        self.assertEqual(status, 200)
        rtt_ms = (stamps['t1_wall_ns'] - stamps['t0_wall_ns']) / MS
        self.assertLess(rtt_ms, 150, 'the connection setup must not be part of the clock-sample round trip')
        self.assertEqual((records[0]['t0_wall_ns'], records[0]['t1_wall_ns']), (stamps['t0_wall_ns'], stamps['t1_wall_ns']))

    @unittest.skipUnless(sys.platform == 'win32', 'the coarse wall clock is a Windows property')
    def test_p2_read_and_clock_stamps_use_the_precise_wall_clock(self):
        from ftdi_core.live import clocks, rest
        self.assertEqual(clocks.WALL_CLOCK['implementation'], 'GetSystemTimePreciseAsFileTime()')
        tick = 15_625_000

        def coarse():   # what time.time_ns() does on Windows with the default system timer
            return time.perf_counter_ns() // tick * tick + 1_700_000_000_000_000_000
        with mock.patch.object(time, 'time_ns', coarse):
            pairs = []
            for _ in range(400):
                pairs.append(capture_module._read_stamps())
                time.sleep(0.0005)
            records = []
            with FakeVenue() as venue:
                client = RestClient(Endpoint('http', '127.0.0.1', int(venue.rest_url.rsplit(':', 1)[1])),
                                    symbol='BTCUSDT', recorder=records.append, min_interval_s=0, max_body_bytes=4096)
                _, _, stamps = client.server_time()
        offsets = [wall - perf for perf, wall in pairs]
        self.assertLess((max(offsets) - min(offsets)) / MS, 2.0, 'read stamps follow the quantised clock')
        # the clock-sample stamp is real wall time (the patched coarse clock would give an unrelated value)
        self.assertLess(abs(stamps['t0_wall_ns'] - clocks.wall_time_ns()) / 1e9, 5.0)
        self.assertIs(rest.wall_time_ns, clocks.wall_time_ns)
        self.assertIs(capture_module.wall_time_ns, clocks.wall_time_ns)

    def test_p3_jitter_is_not_a_wall_clock_step_but_a_lasting_shift_is(self):
        estimate = OffsetEstimate(offset_ms=0.0, uncertainty_ms=2.0, at_wall_ns=1_700_000_000_000 * MS, rtt_ms=1.0,
                                  samples_used=1, samples_rejected=0)

        def events(shifts):
            out, t = [], 1_700_000_000_000 * MS
            for i, shift in enumerate(shifts):
                read = t + i * 300 * MS
                out.append(EventTimes(venue_event_ms=(read - 40 * MS) // MS, read_wall_ns=read + int(shift * MS),
                                      read_perf_ns=i * 300 * MS, queue_depth_at_read=0))
            return out
        jitter = classify_events(events([0, 0, 7, 0, -6, 0, 0, 4]), estimate)
        self.assertFalse(any('WALL_CLOCK_STEP' in e.tags for e in jitter))
        # a read whose stamps disagree with its neighbours by more than the tolerance is excluded on its own
        self.assertEqual([('READ_STAMP_OUTLIER' in e.tags) for e in jitter], [False, False, True, False, True] + [False] * 3)
        self.assertEqual([e.trusted for e in jitter], [True, True, False, True, False, True, True, True])
        # the pilot's pattern: short runs of 3-4 reads delayed by ~5.6 ms, then back to the level
        runs = classify_events(events([0] * 10 + [-5.6] * 4 + [0] * 10 + [-3.5] * 3 + [0] * 25), estimate)
        self.assertFalse(any('WALL_CLOCK_STEP' in e.tags for e in runs))
        self.assertEqual([i for i, e in enumerate(runs) if not e.trusted], [10, 11, 12, 13])
        # a deviating last read cannot be confirmed as a step, but is not trusted either
        last = classify_events(events([0] * 6 + [9]), estimate)
        self.assertEqual([e.trusted for e in last], [True] * 6 + [False])
        self.assertEqual(last[-1].tags, ('READ_STAMP_OUTLIER',))
        step = classify_events(events([0, 0, 0, 30, 30, 30, 30, 30]), estimate)
        self.assertEqual([('WALL_CLOCK_STEP' in e.tags) for e in step], [False] * 3 + [True] * 5)
        self.assertFalse(any('READ_STAMP_OUTLIER' in e.tags for e in step))

    def test_p4_a_report_without_trusted_samples_says_so(self):
        estimate = OffsetEstimate(offset_ms=0.0, uncertainty_ms=380.0, at_wall_ns=0, rtt_ms=760.0, samples_used=1,
                                  samples_rejected=0)
        events = [EventTimes(venue_event_ms=1000 + i, read_wall_ns=(1040 + i) * MS, read_perf_ns=i * MS,
                             queue_depth_at_read=0) for i in range(50)]
        report = percentile_report(classify_events(events, estimate))
        self.assertEqual(report['status'], 'NO_TRUSTED_SAMPLES')
        self.assertEqual(report['trusted'], 0)

    def test_p5_a_composed_chain_has_a_source_manifest_the_audit_accepts(self):
        with FakeVenue(connections=[[('trades', 1, 30)]]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=30, max_seconds=10.0, limits=FAST)
        chain = compose_sessions([result['session_dir']], self.base / 'chain')
        manifest = json.loads((self.base / 'chain' / 'source_manifest.json').read_text(encoding='utf-8'))
        self.assertEqual((manifest['records'], manifest['source_seq_first'], manifest['source_seq_last'],
                          manifest['jsonl_sha256']), (30, 1, 30, chain['files']['events.jsonl']['sha256']))
        run, consumer = self.base / 'run', self.base / 'consumer' / 'effects.sqlite3'
        consumer.parent.mkdir()
        replay(run, [f'LIVE={self.base / "chain" / "events.jsonl"}'], batch=16)
        local_lab(run, consumer, batch=16)
        report = reconcile_archive_aware(run, consumer)
        self.assertEqual(report['status'], 'COMPLETE', report['issue_counts'])
        self.assertNotIn('SOURCE_MANIFEST_ABSENT', report['issue_counts'])
        declared = report['lanes']['LIVE']['provenance']['source_manifest']
        self.assertTrue(declared['jsonl_sha256_matches_computed'])
        self.assertEqual(declared['source_kind'], 'LIVE_SESSION_CHAIN')

    def test_p6_capture_start_records_the_clocks_and_v040_starts_stay_valid(self):
        from ftdi_core.live.session import START, start_problem
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=5, max_seconds=10.0, limits=FAST)
        start = json.loads((Path(result['session_dir']) / START).read_text(encoding='utf-8'))
        from ftdi_core.live.clocks import INTERVAL_CLOCK, WALL_CLOCK
        self.assertEqual(start['clocks'], {'wall': WALL_CLOCK, 'interval': INTERVAL_CLOCK})
        self.assertIsNone(start_problem(start))
        v040 = {k: v for k, v in start.items() if k != 'clocks'}
        self.assertIsNone(start_problem(v040), 'a v0.4.0 CAPTURE_START (no clocks) stays valid')
        for bad in (None, [], {}, {'wall': WALL_CLOCK}, {'wall': WALL_CLOCK, 'interval': INTERVAL_CLOCK, 'x': 1},
                    {'wall': {'implementation': 'x'}, 'interval': INTERVAL_CLOCK},
                    {'wall': {'implementation': 1, 'resolution_ns': 100}, 'interval': INTERVAL_CLOCK},
                    {'wall': {'implementation': 'x', 'resolution_ns': 0}, 'interval': INTERVAL_CLOCK},
                    {'wall': {'implementation': 'x', 'resolution_ns': 1.5}, 'interval': INTERVAL_CLOCK},
                    {'wall': {'implementation': 'x', 'resolution_ns': True}, 'interval': INTERVAL_CLOCK}):
            self.assertEqual(start_problem(dict(start, clocks=bad)), 'clocks', bad)
        self.assertEqual(start_problem(dict(start, unknown=1)), 'fields or schema')

    def test_p7_latency_uses_the_session_wall_clock_resolution(self):
        from ftdi_core.live.latency import analyze_session, session_wall_resolution_ms
        from ftdi_core.live.clocks import WALL_CLOCK
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=5, max_seconds=10.0, limits=FAST)
        report = analyze_session(result['session_dir'])
        self.assertEqual(report['session_wall_clock'], {'implementation': WALL_CLOCK['implementation'],
                                                        'resolution_ms': WALL_CLOCK['resolution_ns'] / 1e6})
        old = self.base / 'v040'
        old.mkdir()
        (old / 'CAPTURE_START.json').write_text(json.dumps({'schema': 'FTDI-Live-Capture-Start-v1'}), encoding='utf-8')
        resolution, implementation = session_wall_resolution_ms(old)
        if sys.platform == 'win32':
            self.assertEqual(resolution, 15.625)
            self.assertIn('GetSystemTimeAsFileTime', implementation)
        else:
            self.assertIsNone(resolution)


if __name__ == '__main__':
    unittest.main()
