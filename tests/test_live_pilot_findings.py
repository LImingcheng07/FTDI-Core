"""Regressions for the findings of the real-network pilot (docs/REAL_LIVE_PILOT_RESULTS.md), v0.4.1."""
from __future__ import annotations

import json
from pathlib import Path
import socket
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
from ftdi_core.timeprov import (EventTimes, OffsetEstimate, OffsetSample, TimePolicy, classify_events,
                             estimate_offset, percentile_report)

MS = 1_000_000
BASE = 1_700_000_000_000 * MS
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
        self.assertIs(rest.stamp_pair, clocks.stamp_pair)
        self.assertIs(capture_module.stamp_pair, clocks.stamp_pair)

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
        from ftdi_core.live.latency import session_samples
        samples = session_samples(result['session_dir'])
        self.assertTrue(samples)
        self.assertTrue(all(type(s.t0_perf_ns) is int and type(s.t1_perf_ns) is int for s in samples),
                        'the interval stamps of the clock samples reach the model')
        old = self.base / 'v040'
        old.mkdir()
        (old / 'CAPTURE_START.json').write_text(json.dumps({'schema': 'FTDI-Live-Capture-Start-v1'}), encoding='utf-8')
        resolution, implementation = session_wall_resolution_ms(old)
        # whatever machine analyses it (review F5: on Linux the analysing machine's 1 ns was used)
        self.assertEqual(resolution, 15.625)
        self.assertIn('GetSystemTimeAsFileTime', implementation)

    # -- review of v0.4.1 (Codex, round 1): F1-F4 ------------------------------------------------------------
    @staticmethod
    def _reads(levels_ms, spacing_ms=10, lag_ms=40):
        """Reads ``spacing_ms`` apart, ``lag_ms`` after the venue event, wall-minus-interval level shifted per read."""
        out = []
        for i, level in enumerate(levels_ms):
            perf = 5_000 * MS + i * spacing_ms * MS
            wall = BASE + perf + int(level * MS)
            out.append(EventTimes(venue_event_ms=(BASE + perf - lag_ms * MS) // MS, read_wall_ns=wall,
                                  read_perf_ns=perf, queue_depth_at_read=0))
        return out

    @staticmethod
    def _sample_estimate(level_ms, at_perf_ns=5_000 * MS, **kw):
        """Estimate from a clock sample taken at interval time ``at_perf_ns`` with the given level (offset 0)."""
        wall = BASE + at_perf_ns + int(level_ms * MS)
        sample = OffsetSample(wall - MS // 2, (wall + MS // 2) // MS, wall + MS // 2, at_perf_ns - MS // 2,
                              at_perf_ns + MS // 2)
        return estimate_offset([sample], local_resolution_ms=0.0001, **kw)

    def test_p8_each_read_is_checked_against_the_clock_sample_it_is_corrected_with(self):
        estimate = self._sample_estimate(0)
        self.assertEqual(estimate.at_perf_ns, 5_000 * MS)
        stable = classify_events(self._reads([0] * 30), estimate)
        self.assertTrue(all(e.trusted for e in stable), [e.tags for e in stable if not e.trusted])
        # the wall clock steps by 30 ms after the sample: reads from the step on are not corrected by it
        after = classify_events(self._reads([0] * 10 + [30] * 20), estimate)
        self.assertEqual([('WALL_CLOCK_STEP' in e.tags) for e in after], [False] * 10 + [True] * 20)
        # the sample was taken after the step (at the new level): the reads before it are the stepped ones
        new_level = self._sample_estimate(30, at_perf_ns=5_000 * MS + 25 * 10 * MS)
        before = classify_events(self._reads([0] * 10 + [30] * 20), new_level)
        self.assertEqual([e.trusted for e in before], [False] * 10 + [True] * 20)
        self.assertTrue(all('WALL_CLOCK_STEP' in e.tags for e in before[:10]))
        # one jittered read is an outlier; the measured level difference is part of the uncertainty
        jitter = classify_events(self._reads([0] * 5 + [9] + [0] * 5 + [3] + [0] * 5), estimate)
        self.assertEqual([i for i, e in enumerate(jitter) if not e.trusted], [5])
        self.assertEqual(jitter[5].tags, ('READ_STAMP_OUTLIER',))
        self.assertAlmostEqual(jitter[11].uncertainty_ms - jitter[10].uncertainty_ms, 3.0, delta=0.01)
        # a sample during which the wall clock moved against the interval clock is not usable
        wall = BASE + 5_000 * MS
        moved = OffsetSample(wall, wall // MS, wall + 12 * MS, 5_000 * MS, 5_000 * MS + 2 * MS)
        self.assertIsNone(estimate_offset([moved]))
        self.assertEqual(estimate_offset([moved, OffsetSample(wall, wall // MS, wall + 2 * MS, 5_000 * MS,
                                                              5_002 * MS)]).samples_rejected, 1)
        with self.assertRaises(ValueError):
            OffsetSample(wall, wall // MS, wall + 2 * MS, 5_000 * MS, None)
        reversed_perf = OffsetSample(wall, wall // MS, wall + 2 * MS, 5_000 * MS, 4_998 * MS)
        self.assertIsNone(estimate_offset([reversed_perf]), 'interval stamps running backwards')

    def test_p9_an_absurd_clock_resolution_makes_the_session_invalid_not_the_analysis_crash(self):
        from ftdi_core.live.latency import analyze_session
        from ftdi_core.live.session import MANIFEST, START, sha256_bytes, start_problem, verify_session
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=5, max_seconds=10.0, limits=FAST)
        session = Path(result['session_dir'])
        start = json.loads((session / START).read_bytes().decode('utf-8'))
        for resolution, ok in ((1, True), (10 ** 9, True), (10 ** 9 + 1, False), (10 ** 400, False)):
            start['clocks']['wall']['resolution_ns'] = resolution
            self.assertEqual(start_problem(start) is None, ok, resolution)
        start['clocks']['wall']['resolution_ns'] = 100
        for implementation, ok in (('x', True), ('', False), ('x' * 200, True), ('x' * 201, False)):
            start['clocks']['wall']['implementation'] = implementation
            self.assertEqual(start_problem(start) is None, ok, len(implementation))
        # end to end (Codex's reproduction): a coherent session whose start record claims 10**400 ns
        start['clocks']['wall'] = {'implementation': 'x', 'resolution_ns': 10 ** 400}
        raw = json.dumps(start).encode('utf-8')
        (session / START).write_bytes(raw)
        manifest = json.loads((session / MANIFEST).read_bytes().decode('utf-8'))
        manifest['capture_start_sha256'] = sha256_bytes(raw)
        (session / MANIFEST).write_text(json.dumps(manifest), encoding='utf-8')
        report = verify_session(session)
        self.assertEqual(report['status'], 'INVALID')
        self.assertIn({'code': 'MANIFEST_INVALID', 'detail': 'CAPTURE_START.json: clocks'}, report['samples'])
        with self.assertRaisesRegex(ValueError, 'not VERIFIED'):
            analyze_session(session)

    def test_p10_a_jittered_first_read_is_an_outlier_not_the_reference(self):
        plain = OffsetEstimate(offset_ms=0.0, uncertainty_ms=2.0, at_wall_ns=BASE + 5_000 * MS, rtt_ms=1.0,
                               samples_used=1, samples_rejected=0)          # no interval stamps: heuristic rule
        for estimate in (plain, self._sample_estimate(0)):
            for first in (9, -9):
                evidence = classify_events(self._reads([first] + [0] * 50), estimate)
                self.assertEqual(evidence[0].tags, ('READ_STAMP_OUTLIER',), (first, estimate))
                self.assertTrue(all(e.trusted for e in evidence[1:]), (first, [e.tags for e in evidence[1:4]]))
        # a real step after two reads is still a step for the heuristic rule
        early = classify_events(self._reads([0, 0] + [30] * 30), plain)
        self.assertEqual([('WALL_CLOCK_STEP' in e.tags) for e in early], [False, False] + [True] * 30)

    def test_p11_slow_drift_within_the_policy_is_not_a_step_but_faster_drift_is(self):
        def reads(ppm):   # one read every 10 s, the wall clock drifting against the interval clock
            return [EventTimes(venue_event_ms=(BASE + i * 10_000 * MS - 40 * MS) // MS,
                               read_wall_ns=BASE + i * 10_000 * MS + i * 10_000 * MS * ppm // 1_000_000,
                               read_perf_ns=i * 10_000 * MS, queue_depth_at_read=0) for i in range(50)]

        def fresh(ppm, with_perf):   # an estimate taken at the read (Codex's reproduction)
            def estimate(event):
                i = event.read_perf_ns // (10_000 * MS)
                drift_ms = i * 10_000 * ppm / 1_000_000
                return OffsetEstimate(-drift_ms, 2.0, event.read_wall_ns, 1.0, 1, 0,
                                      event.read_perf_ns if with_perf else None)
            return estimate
        for ppm in (50, -50, 90):
            for with_perf in (False, True):
                evidence = classify_events(reads(ppm), fresh(ppm, with_perf))
                self.assertTrue(all(e.trusted for e in evidence), (ppm, with_perf, [e.tags for e in evidence][18:24]))
                self.assertTrue(all(abs(e.latency_ms - 40) < 0.01 for e in evidence))
        evidence = classify_events(reads(400), fresh(400, False))
        self.assertTrue(any('WALL_CLOCK_STEP' in e.tags for e in evidence), 'drift far beyond the policy')
        # a read 100 s after its sample, the wall clock having drifted 80 ppm (8 ms, within tolerance + drift bound):
        # trusted, with the drift bound and the measured difference in its uncertainty
        estimate = self._sample_estimate(0)
        perf = 5_000 * MS + 100_000 * MS
        read = EventTimes(venue_event_ms=(BASE + perf - 40 * MS) // MS, read_wall_ns=BASE + perf + 8 * MS,
                          read_perf_ns=perf, queue_depth_at_read=0)
        evidence = classify_events([read], estimate)[0]
        self.assertEqual(evidence.tags, ())
        self.assertTrue(evidence.trusted)
        self.assertAlmostEqual(evidence.uncertainty_ms, estimate.uncertainty_ms + 10.0 + 8.0, delta=0.01)

    def test_p12_name_resolution_and_connect_are_bounded_by_the_deadline(self):
        from ftdi_core.live import netconnect
        from ftdi_core.live.wsclient import ConnectionClosed, WebSocket
        original = socket.getaddrinfo

        def stalled(*args, **kw):
            time.sleep(1.5)
            return original(*args, **kw)
        with FakeVenue() as venue:
            port = int(venue.rest_url.rsplit(':', 1)[1])
            records = []
            client = RestClient(Endpoint('http', '127.0.0.1', port), symbol='BTCUSDT', recorder=records.append,
                                min_interval_s=0, max_body_bytes=4096)
            with mock.patch.object(netconnect.socket, 'getaddrinfo', stalled):
                begin = time.perf_counter()
                status, body, _ = client.server_time(budget_s=0.3)
                rest_elapsed = time.perf_counter() - begin
                begin = time.perf_counter()
                with self.assertRaises(ConnectionClosed):
                    WebSocket.connect(Endpoint('ws', '127.0.0.1', int(venue.ws_url.rsplit(':', 1)[1])), '/ws/x',
                                      timeout=0.3, max_message=4096)
                ws_elapsed = time.perf_counter() - begin
            self.assertIsNone(status)
            self.assertIn('name resolution', records[0]['error'])
            self.assertLess(rest_elapsed, 1.0)
            self.assertLess(ws_elapsed, 1.0)
            # set-up that returns after the budget is a failed attempt, not a late answer (Codex's reproduction)
            import ftdi_core.live.rest as rest_module

            class SlowConnect(rest_module._HTTP):
                def connect(self):
                    time.sleep(0.2)
                    super().connect()
            records.clear()
            with mock.patch.object(rest_module, '_HTTP', SlowConnect):
                status, body, _ = client.server_time(budget_s=0.05)
            self.assertIsNone(status)
            self.assertIsNone(body)
            self.assertIn('SetupTimeout', records[0]['error'])

            # the socket connects in time but the set-up as a whole (e.g. a slow TLS handshake) ends after the
            # budget: deterministic failure, not a request racing the abort timer
            class SlowHandshake(rest_module._HTTP):
                def connect(self):
                    super().connect()
                    time.sleep(0.2)
            records.clear()
            with mock.patch.object(rest_module, '_HTTP', SlowHandshake):
                status, body, _ = client.server_time(budget_s=0.05)
            self.assertIsNone(status)
            self.assertIn('SetupTimeout', records[0]['error'])

    # -- review of v0.4.1 (Codex, round 2): R2-1..R2-5 ------------------------------------------------------
    def test_p13_the_heuristic_does_not_depend_on_the_direction_of_a_shift(self):
        plain = OffsetEstimate(offset_ms=0.0, uncertainty_ms=2.0, at_wall_ns=BASE + 5_000 * MS, rtt_ms=1.0,
                               samples_used=1, samples_rejected=0)
        # an early step after two consistent reads, both directions (Codex: the downward one was trusted)
        for size in (9, -9, 30, -30):
            evidence = classify_events(self._reads([0, 0] + [size] * 30), plain)
            self.assertEqual([e.trusted for e in evidence], [True, True] + [False] * 30, size)
            self.assertTrue(all('WALL_CLOCK_STEP' in e.tags for e in evidence[2:]), size)
        # mirrored sequences give mirrored decisions
        for levels in ([0, 0, 7, 0, -6, 0, 0, 4], [9] + [0] * 30, [0] * 10 + [-5.6] * 4 + [0] * 25,
                       [0, 0, 0, 30, 30, 30, 30, 30], [0] * 6 + [9]):
            up = classify_events(self._reads(levels), plain)
            down = classify_events(self._reads([-v for v in levels]), plain)
            self.assertEqual([e.tags for e in up], [e.tags for e in down], levels)
        # sparse reads (120 s apart) drifting at +-90 ppm against the interval clock, fresh estimates: all trusted
        for ppm in (90, -90):
            reads = [EventTimes(venue_event_ms=(BASE + i * 120_000 * MS - 40 * MS) // MS,
                                read_wall_ns=BASE + i * 120_000 * MS + i * 120_000 * MS * ppm // 1_000_000,
                                read_perf_ns=i * 120_000 * MS, queue_depth_at_read=0) for i in range(30)]

            def fresh(event, ppm=ppm):
                drift_ms = event.read_perf_ns / MS * ppm / 1_000_000
                return OffsetEstimate(-drift_ms, 2.0, event.read_wall_ns, 1.0, 1, 0)
            evidence = classify_events(reads, fresh)
            self.assertTrue(all(e.trusted for e in evidence), (ppm, [e.tags for e in evidence][:4]))
        # majority, not unanimity: within a window spread over the tolerance (levels 0 and 4 ms), a read at -3 ms
        # agrees with the reference and with half of the window, and is trusted
        spread = classify_events(self._reads([0, 4] * 15 + [-3] + [0, 4] * 10), plain)
        self.assertTrue(all(e.trusted for e in spread), [(i, e.tags) for i, e in enumerate(spread) if not e.trusted])
        # no two consecutive reads agree: no reference, nothing trusted; a single read has nothing to disagree with
        alternating = classify_events(self._reads([0, 9] * 10), plain)
        self.assertTrue(all(e.tags == ('READ_STAMP_OUTLIER',) for e in alternating))
        self.assertEqual(classify_events(self._reads([0]), plain)[0].tags, ())

    def test_p14_integers_beyond_64_bits_make_the_session_invalid_not_the_analysis_crash(self):
        from ftdi_core.live.events import FormatError, parse_ws
        from ftdi_core.live.latency import analyze_session
        from ftdi_core.live.session import MANIFEST, facts_of, verify_session
        from ftdi_core.live import session as session_module
        self.assertTrue(session_module._is_int(2 ** 63 - 1))
        self.assertFalse(session_module._is_int(2 ** 63))
        message = json.dumps({'e': 'aggTrade', 'E': 2 ** 63, 's': 'BTCUSDT', 'a': 1, 'p': '1.0', 'q': '1.0', 'f': 1,
                              'l': 1, 'T': 1, 'm': True, 'M': True})
        with self.assertRaises(FormatError):
            parse_ws(message, 'BTCUSDT')
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=5, max_seconds=10.0, limits=FAST)
        session = Path(result['session_dir'])
        lines = [json.loads(line) for line in (session / 'clock.jsonl').read_text(encoding='utf-8').splitlines()]
        self.assertTrue(lines)
        for line in lines:   # Codex's reproduction: coherent, consistent stamps far beyond 64 bits
            line['t0_perf_ns'] = 10 ** 400
            line['t1_perf_ns'] = 10 ** 400 + 1_000_000
            line['t1_wall_ns'] = line['t0_wall_ns'] + 1_000_000
        data = ''.join(json.dumps(line) + '\n' for line in lines).encode('utf-8')
        (session / 'clock.jsonl').write_bytes(data)
        manifest = json.loads((session / MANIFEST).read_bytes().decode('utf-8'))
        manifest['files']['clock.jsonl'] = facts_of(data)
        (session / MANIFEST).write_text(json.dumps(manifest), encoding='utf-8')
        report = verify_session(session)
        self.assertEqual(report['status'], 'INVALID')
        with self.assertRaisesRegex(ValueError, 'not VERIFIED'):
            analyze_session(session)

    def test_p15_connection_setup_falls_back_reports_and_starts_nothing_after_the_deadline(self):
        import types
        from ftdi_core.live import netconnect
        real = socket

        def stand_in(getaddrinfo, make_socket=real.socket):   # replaces only netconnect's view of the socket module
            return types.SimpleNamespace(getaddrinfo=getaddrinfo, socket=make_socket, SOCK_STREAM=real.SOCK_STREAM)
        with FakeVenue() as venue:
            port = int(venue.rest_url.rsplit(':', 1)[1])
            # R2-3: the first address's family is unavailable locally (socket construction fails): the next is tried
            created = []

            def make_socket(family, kind, proto):
                created.append(family)
                if family == real.AF_INET6:
                    raise OSError(10047, 'address family not supported (simulated)')
                return real.socket(family, kind, proto)
            addresses = [(real.AF_INET6, real.SOCK_STREAM, 6, '', ('::1', port, 0, 0)),
                         (real.AF_INET, real.SOCK_STREAM, 6, '', ('127.0.0.1', port))]
            with mock.patch.object(netconnect, 'socket', stand_in(lambda *a, **k: addresses, make_socket)):
                sock = netconnect.connect('127.0.0.1', port, time.perf_counter() + 2.0)
            sock.close()
            self.assertEqual(created, [real.AF_INET6, real.AF_INET])
            # R2-4: a non-OSError failure of the lookup is a recorded resolution failure, not a KeyError
            def broken(*args, **kw):
                raise UnicodeError('label too long (simulated)')
            records = []
            client = RestClient(Endpoint('http', '127.0.0.1', port), symbol='BTCUSDT', recorder=records.append,
                                min_interval_s=0, max_body_bytes=4096)
            with mock.patch.object(netconnect, 'socket', stand_in(broken)):
                with self.assertRaisesRegex(OSError, 'UnicodeError'):
                    netconnect.connect('127.0.0.1', port, time.perf_counter() + 2.0)
                status, _, _ = client.server_time(budget_s=1.0)
            self.assertIsNone(status)
            self.assertIn('UnicodeError', records[0]['error'])
            # R2-5: an expired deadline starts no lookup and no worker
            calls = []

            def counting(*args, **kw):
                calls.append(args)
                return addresses[1:]
            workers = sum(t.name == 'ftdi-resolve' for t in threading.enumerate())
            with mock.patch.object(netconnect, 'socket', stand_in(counting)):
                with self.assertRaises(netconnect.SetupTimeout):
                    netconnect.connect('127.0.0.1', port, time.perf_counter() - 1.0)
            self.assertEqual(calls, [])
            self.assertEqual(sum(t.name == 'ftdi-resolve' for t in threading.enumerate()), workers)

    def test_p16_no_trusted_read_disagrees_with_its_reference(self):
        """Safety invariant over many random level sequences (fixed seed): jitter, runs, steps, ramps, drift."""
        import random
        policy = TimePolicy()
        tolerance_ns, rate = policy.wall_step_tolerance_ms * MS, policy.drift_ppm * 1e-6
        # a ramp faster than the drift bound with dense reads: each read agrees with its neighbours
        ramp = [i * 10 * 0.001 for i in range(2000)]   # 1000 ppm at one read per 10 ms (0.01 ms per read)
        plain = OffsetEstimate(offset_ms=0.0, uncertainty_ms=2.0, at_wall_ns=BASE + 5_000 * MS, rtt_ms=1.0,
                               samples_used=1, samples_rejected=0)
        evidence = classify_events(self._reads(ramp), plain)
        self.assertTrue(any('WALL_CLOCK_STEP' in e.tags for e in evidence), 'a 1000 ppm ramp is not drift')
        rng = random.Random(20261109)
        for case in range(300):
            levels, level = [], 0.0
            for _ in range(rng.randint(1, 400)):
                kind = rng.random()
                if kind < 0.05:
                    level += rng.choice((-1, 1)) * rng.uniform(3, 40)        # a step
                slope = rng.choice((0, 0, 0, 0.0005, -0.0005, 0.05, -0.05))  # ms per read (10 ms apart)
                level += slope
                jitter = rng.uniform(-9, 9) if rng.random() < 0.1 else rng.uniform(-0.5, 0.5)
                levels.append(level + jitter)
            reads = self._reads(levels)
            # heuristic: a trusted read agrees with one of the two reads of the first agreeing pair
            evidence = classify_events(reads, plain)
            points = [(r.read_perf_ns, r.read_wall_ns - r.read_perf_ns) for r in reads]

            def agree(a, b):
                return abs(a[1] - b[1]) <= tolerance_ns + rate * abs(a[0] - b[0])
            pair = next((k for k in range(len(points) - 1) if agree(points[k], points[k + 1])), None)
            for k, e in enumerate(evidence):
                if e.trusted and len(points) > 1:
                    self.assertIsNotNone(pair, (case, levels))
                    self.assertTrue(agree(points[k], points[pair]) or agree(points[k], points[pair + 1]),
                                    (case, k, levels))
            # per-read model: a trusted read agrees with its clock sample (level 0 at the first read's time)
            estimate = self._sample_estimate(0)
            for k, e in enumerate(classify_events(reads, estimate)):
                if e.trusted:
                    moved = abs(points[k][1] - (estimate.at_wall_ns - estimate.at_perf_ns))
                    self.assertLessEqual(moved, tolerance_ns + rate * abs(points[k][0] - estimate.at_perf_ns),
                                         (case, k))

    # -- independent review of v0.4.1 (round 3): F1-F8 ------------------------------------------------------
    def test_p17_wall_and_interval_stamps_stay_paired_when_the_thread_waits_for_the_gil(self):
        from ftdi_core.live import clocks
        from ftdi_core.live import rest as rest_module
        offset = 1_700_000_000_000 * MS
        rng = __import__('random').Random(7)

        def delayed_wall():   # the value is exact, but getting the GIL back afterwards sometimes takes 5 ms
            value = time.perf_counter_ns() + offset
            if rng.random() < 0.3:
                time.sleep(0.005)
            return value
        with mock.patch.object(clocks, 'wall_time_ns', delayed_wall):
            pairs = [clocks.stamp_pair() for _ in range(40)]
            reads = [capture_module._read_stamps() for _ in range(40)]
            with FakeVenue() as venue:
                client = RestClient(Endpoint('http', '127.0.0.1', int(venue.rest_url.rsplit(':', 1)[1])),
                                    symbol='BTCUSDT', recorder=lambda r: None, min_interval_s=0, max_body_bytes=4096)
                stamps = [client.server_time()[2] for _ in range(10)]
        errors = [abs(wall - perf - offset) / MS for wall, perf in pairs]
        errors += [abs(wall - perf - offset) / MS for perf, wall in reads]
        errors += [abs(st[f't{k}_wall_ns'] - st[f't{k}_perf_ns'] - offset) / MS for st in stamps for k in (0, 1)]
        self.assertLess(max(errors), 0.5, 'wall and interval stamps of one instant must agree')

    def test_p18_the_error_bound_covers_a_wall_clock_moving_during_the_sample_and_unchecked_reads(self):
        # wall clock 4.9 ms back during a request of 10 ms on the interval clock (review F3)
        wall = BASE + 5_000 * MS
        moving = OffsetSample(wall, wall // MS + 5, wall + 5_100_000, 5_000 * MS, 5_010 * MS)
        self.assertEqual(moving.rtt_bound_ms, 10.0)
        self.assertAlmostEqual(moving.level_spread_ms, 4.9)
        normal = OffsetSample(wall, wall // MS + 3, wall + 6 * MS, 5_000 * MS, 5_006 * MS)
        estimate = estimate_offset([moving, normal], venue_resolution_ms=1.0, local_resolution_ms=0.0001)
        self.assertEqual(estimate.rtt_ms, 6.0, 'selection by the larger of both round trips')
        alone = estimate_offset([moving], venue_resolution_ms=1.0, local_resolution_ms=0.0001)
        self.assertAlmostEqual(alone.uncertainty_ms, 10.0 / 2 + 4.9 / 2 + 1.0 + 0.0001, places=6)
        # a read without an interval stamp cannot be checked against a sample that has them (review F6)
        reads = self._reads([12] * 5)
        reads[2] = EventTimes(reads[2].venue_event_ms, reads[2].read_wall_ns, None, 0)
        evidence = classify_events(reads, self._sample_estimate(0))
        self.assertEqual(evidence[2].tags, ('WALL_CLOCK_UNVERIFIED',))
        self.assertFalse(any(e.trusted for e in evidence))

    def test_p19_sessions_analyse_text_separators_bounded_server_times_and_new_samples(self):
        from ftdi_core.live.latency import analyze_session, nearest_estimator
        from ftdi_core.live.session import verify_session
        # review F1: U+2028 / U+0085 inside a message (sent raw, not escaped) must not split a line of the evidence
        from fakevenue import trade
        text = json.dumps(dict(trade(1), note='a\u2028b\x85c'), ensure_ascii=False, separators=(',', ':'))
        self.assertIn('\u2028', text)
        with FakeVenue(connections=[[('text', text), ('trades', 2, 5)]]) as venue:
            result = capture_session(self.base / 'c', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=5, max_seconds=10.0, limits=FAST)
        self.assertEqual(verify_session(result['session_dir'])['status'], 'VERIFIED')
        self.assertEqual(analyze_session(result['session_dir'])['events_first_delivered_by_ws'], 5)
        # review F4: a server time beyond 64 bits is not written as a clock sample, the session stays VERIFIED
        with FakeVenue(connections=[[('trades', 1, 5)]], server_offset_ms=2 ** 63) as venue:
            result = capture_session(self.base / 'd', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                     max_events=5, max_seconds=10.0, limits=FAST)
        self.assertEqual(result['counts']['clock_samples'], 0)
        self.assertEqual(verify_session(result['session_dir'])['status'], 'VERIFIED')
        # review F8: after a wall-clock step, a newer sample taken under the new wall clock corrects the reads
        def sample(level_ms, at_perf):
            wall = BASE + at_perf + int(level_ms * MS)
            return OffsetSample(wall - MS // 2, (wall + MS // 2) // MS, wall + MS // 2, at_perf - MS // 2,
                                at_perf + MS // 2)
        old, new = sample(0, 5_000 * MS), sample(30, 5_000 * MS + 200 * MS)
        old_better = OffsetSample(old.t0_wall_ns + MS // 4, old.venue_ms, old.t1_wall_ns - MS // 4,
                                  old.t0_perf_ns + MS // 4, old.t1_perf_ns - MS // 4)   # lower round trip
        estimate = nearest_estimator([old_better, new], TimePolicy(), 0.0001)
        reads = self._reads([0] * 10 + [30] * 20)
        evidence = classify_events(reads, estimate)
        self.assertTrue(all(e.trusted for e in evidence), [(i, e.tags) for i, e in enumerate(evidence) if not e.trusted])

    def test_p20_the_minimum_interval_between_rest_calls_counts_against_the_budget(self):
        with FakeVenue() as venue:
            client = RestClient(Endpoint('http', '127.0.0.1', int(venue.rest_url.rsplit(':', 1)[1])), symbol='BTCUSDT',
                                recorder=lambda r: None, min_interval_s=0.35, max_body_bytes=4096)
            client.server_time()
            import ftdi_core.live.rest as rest_module
            seen = []
            real_timer = rest_module.threading.Timer

            def timer(interval, *args, **kw):
                seen.append(interval)
                return real_timer(interval, *args, **kw)
            with mock.patch.object(rest_module.threading, 'Timer', timer):
                begin = time.perf_counter()
                status, _, _ = client.server_time(budget_s=0.5)
                elapsed = time.perf_counter() - begin
        self.assertEqual(status, 200)
        self.assertLess(seen[0], 0.5 - 0.25, 'the network budget is what remains after the wait')
        self.assertLess(elapsed, 0.6)

    def test_p21_the_sample_giving_the_read_the_smallest_uncertainty_is_used(self):
        """Verification pass of the review: a 2 ms sample from 250 s ago was chosen over a 10 ms sample from 1 s
        ago (minimum round trip), adding 25 ms of drift bound and excluding the read."""
        from ftdi_core.live.latency import nearest_estimator

        def sample(at_perf, rtt_ms):
            wall = BASE + at_perf
            half = int(rtt_ms * MS) // 2
            return OffsetSample(wall - half, wall // MS, wall + half, at_perf - half, at_perf + half)
        read_perf = 5_000 * MS + 251_000 * MS
        old, recent = sample(5_000 * MS + 1_000 * MS, 2.0), sample(read_perf - 1_000 * MS, 10.0)
        read = EventTimes(venue_event_ms=(BASE + read_perf - 40 * MS) // MS, read_wall_ns=BASE + read_perf,
                          read_perf_ns=read_perf, queue_depth_at_read=0)
        policy = TimePolicy()
        chosen = nearest_estimator([old, recent], policy, 0.0001)(read)
        self.assertEqual(chosen.at_perf_ns, recent.t0_perf_ns + 5 * MS, 'the recent sample')
        evidence = classify_events([read], nearest_estimator([old, recent], policy, 0.0001))[0]
        self.assertTrue(evidence.trusted, evidence.tags)
        self.assertLess(evidence.uncertainty_ms, 7.0)
        only_old = classify_events([read], nearest_estimator([old], policy, 0.0001))[0]
        self.assertIn('OFFSET_UNCERTAIN', only_old.tags)
        # a sample under the read's wall clock is preferred to one whose total would be smaller but whose wall
        # clock differs by more than the tolerance (6 ms): the read is corrected, not tagged
        def leveled(at_perf, rtt_ms, level_ms):
            wall = BASE + at_perf + int(level_ms * MS)
            half = int(rtt_ms * MS) // 2
            return OffsetSample(wall - half, (wall - int(level_ms * MS)) // MS, wall + half, at_perf - half,
                                at_perf + half)
        precise_other = leveled(read_perf - 500 * MS, 2.0, 0)     # u about 2 + 6 ms moved
        slower_same = leveled(read_perf - 500 * MS, 16.0, 6)      # u about 9 ms, same wall clock
        shifted = EventTimes(read.venue_event_ms, read.read_wall_ns + 6 * MS, read.read_perf_ns, 0)
        evidence = classify_events([shifted], nearest_estimator([precise_other, slower_same], policy, 0.0001))[0]
        self.assertTrue(evidence.trusted, evidence.tags)
        self.assertAlmostEqual(evidence.latency_ms, 40.0, delta=1.5)

    def test_p22_a_body_cut_short_by_the_connection_is_not_an_answer(self):
        """First run on Linux: there a shut-down or closed socket ends the body read with EOF and http.client returns
        the partial body (Windows raised ConnectionAbortedError). An orderly close after part of the announced body
        gives the same EOF on every platform."""
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def serve():
            conn, _ = listener.accept()
            with conn:
                conn.recv(4096)
                conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n'
                             b'Connection: close\r\n\r\n[{"a":1}]')
        server = threading.Thread(target=serve, daemon=True)
        server.start()
        records = []
        client = RestClient(Endpoint('http', '127.0.0.1', listener.getsockname()[1]), symbol='BTCUSDT',
                            recorder=records.append, min_interval_s=0, max_body_bytes=4096)
        status, body, _ = client.agg_trades(1, 10, budget_s=2.0)
        server.join(2)
        self.assertIsNone(body, 'a truncated body must not be used')
        self.assertIsNone(records[0]['body'])
        self.assertIn('IncompleteRead', records[0]['error'])

    def test_p23_the_cli_sets_the_clock_interval_and_public_mode_bounds_it(self):
        """The remote-host pilot needed samples every 5 s; the CLI had no option for it (v0.4.2)."""
        import contextlib
        import io
        from ftdi_core.cli import main
        from ftdi_core.live.limits import limits_problem
        from dataclasses import asdict
        with FakeVenue(connections=[[('trades', 1, 5)]]) as venue:
            with contextlib.redirect_stdout(io.StringIO()) as out:
                code = main(['live-capture', '--symbol', 'BTCUSDT', '--capture-dir', str(self.base / 'c'),
                             '--ws-url', venue.ws_url, '--rest-url', venue.rest_url, '--max-events', '5',
                             '--max-seconds', '10', '--clock-interval-s', '2.5'])
        self.assertEqual(code, 0)
        session = Path(json.loads(out.getvalue())['session_dir'])
        start = json.loads((session / 'CAPTURE_START.json').read_text(encoding='utf-8'))
        self.assertEqual(start['limits']['clock_interval_s'], 2.5)
        # against the public venue: at least 1 s, refused before any socket, and a session claiming less is invalid
        with self.assertRaisesRegex(ValueError, 'clock_interval_s'):
            capture_session(self.base / 'p', symbol='BTCUSDT', public_network=True, max_seconds=1.0,
                            limits=CaptureLimits(clock_interval_s=0.5))
        self.assertFalse((self.base / 'p').exists() and any((self.base / 'p').rglob('*')))
        self.assertIsNone(limits_problem(asdict(CaptureLimits(clock_interval_s=1.0)), True))
        self.assertIn('clock_interval_s', limits_problem(asdict(CaptureLimits(clock_interval_s=0.5)), True))
        self.assertIsNone(limits_problem(asdict(CaptureLimits(clock_interval_s=0.5)), False))

    def test_p24_raw_evidence_reaches_the_os_before_anything_derived_from_it(self):
        """First Linux run: a capture killed between two buffer flushes left event 40 on disk without its raw
        message (recovery correctly sealed 39). Raw files are now unbuffered: an appended raw line is in the file at
        once, while derived files may still be buffered."""
        from ftdi_core.live.session import SessionFiles
        directory = self.base / 's'
        directory.mkdir()
        files = SessionFiles(directory)
        try:
            files.append('raw_ws.jsonl', {'n': 0, 'text': 'x'})
            files.append('raw_rest.jsonl', {'n': 0, 'body': 'y'})
            files.append('events.jsonl', {'source_seq': 1})
            self.assertEqual((directory / 'raw_ws.jsonl.part').read_bytes().count(b'\n'), 1)
            self.assertEqual((directory / 'raw_rest.jsonl.part').read_bytes().count(b'\n'), 1)
            files.sync()
            self.assertEqual((directory / 'events.jsonl.part').read_bytes().count(b'\n'), 1)
        finally:
            files.close()


if __name__ == '__main__':
    unittest.main()
