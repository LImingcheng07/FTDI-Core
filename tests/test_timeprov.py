"""Task 003: timestamp provenance (pure functions, deterministic, no network)."""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from ftdi_core.timeprov import (EventTimes, OffsetSample, TimePolicy, analyze_pilot_db, assess_clocks,
                                classify_events, estimate_offset, nearest_rank, percentile_report, uncertainty_at)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MS = 1_000_000
BASE_NS = 1_793_970_617_871_293_000


ALIGNED_NS = (BASE_NS // MS) * MS   # whole milliseconds, so expected offsets are exact


def sample(t0_ms, venue_ms, rtt_ms):
    return OffsetSample(ALIGNED_NS + t0_ms * MS, venue_ms, ALIGNED_NS + (t0_ms + rtt_ms) * MS)


class OffsetTests(unittest.TestCase):
    def test_minimum_rtt_sample_wins_with_exact_uncertainty(self):
        venue_now = BASE_NS // MS + 600          # venue clock 600 ms ahead of local wall
        samples = [sample(0, venue_now + 40, 80), sample(1000, venue_now + 1000 + 10, 20),
                   sample(2000, venue_now + 2000 + 100, 200)]
        estimate = estimate_offset(samples, venue_resolution_ms=1.0, local_resolution_ms=1.0)
        self.assertEqual(estimate.rtt_ms, 20)
        self.assertAlmostEqual(estimate.offset_ms, 600.0, places=6)   # venue - mid of the 20 ms sample
        self.assertAlmostEqual(estimate.uncertainty_ms, 10 + 1 + 1)
        self.assertEqual((estimate.samples_used, estimate.samples_rejected), (3, 0))

    def test_rtt_limit_and_invalid_samples(self):
        policy = TimePolicy(max_rtt_ms=50)
        self.assertIsNone(estimate_offset([sample(0, 1, 80)], local_resolution_ms=1.0, policy=policy))
        estimate = estimate_offset([sample(0, 1, 80), sample(5, 1, 30)], local_resolution_ms=1.0, policy=policy)
        self.assertEqual(estimate.samples_rejected, 1)
        with self.assertRaises(ValueError):
            OffsetSample(BASE_NS + 5, 1, BASE_NS)            # wall stepped backwards during the request
        with self.assertRaises(ValueError):
            OffsetSample(BASE_NS, 1.5, BASE_NS + 1)

    def test_uncertainty_grows_with_age(self):
        estimate = estimate_offset([sample(0, 1, 20)], venue_resolution_ms=1.0, local_resolution_ms=1.0)
        policy = TimePolicy(drift_ppm=100)
        later = estimate.at_wall_ns + 60_000 * MS          # one minute later
        self.assertAlmostEqual(uncertainty_at(estimate, later, policy), estimate.uncertainty_ms + 6.0)


class ClassificationTests(unittest.TestCase):
    def setUp(self):
        venue_ms = BASE_NS // MS + 600
        self.estimate = estimate_offset([sample(0, venue_ms + 5, 10)], venue_resolution_ms=1.0, local_resolution_ms=1.0)

    def events(self, latencies_ms, queue=0, perf=True):
        """Events whose true latency is given, with the local clock 600 ms behind the venue."""
        out = []
        for i, latency in enumerate(latencies_ms):
            read_wall = BASE_NS + (i * 10 + 20) * MS
            venue_event = read_wall // MS + 600 - latency
            out.append(EventTimes(int(venue_event), read_wall, read_wall if perf else None, queue))
        return out

    def test_without_offset_everything_is_uncorrected(self):
        evidences = classify_events(self.events([50, 60]), None)
        self.assertTrue(all('CLOCK_UNCORRECTED' in e.tags and e.latency_ms is None and not e.trusted
                            for e in evidences))
        self.assertEqual(percentile_report(evidences)['status'], 'NO_CLOCK_MODEL')

    def test_corrected_latencies_and_minimum_samples(self):
        latencies = [40 + (i % 50) for i in range(150)]
        evidences = classify_events(self.events(latencies), self.estimate)
        self.assertTrue(all(e.trusted for e in evidences), {t for e in evidences for t in e.tags})
        self.assertAlmostEqual(evidences[0].latency_ms, 40, delta=1.0)
        report = percentile_report(evidences)
        self.assertEqual(report['status'], 'ESTIMATED')
        self.assertIsInstance(report['latency_percentiles']['p50_ms'], float)
        self.assertIsInstance(report['latency_percentiles']['p95_ms'], float)
        self.assertIn('insufficient samples', report['latency_percentiles']['p99_ms'])
        self.assertIs(report['certified'], False)

    def test_negative_beyond_uncertainty_makes_report_inconclusive(self):
        evidences = classify_events(self.events([40] * 95 + [-200] * 5), self.estimate)
        report = percentile_report(evidences)
        self.assertEqual(report['tag_counts']['NEGATIVE_BEYOND_UNCERTAINTY'], 5)
        self.assertEqual(report['status'], 'INCONCLUSIVE')
        self.assertIsNone(report['latency_percentiles']['p50_ms'])

    def test_queue_depth_rules(self):
        unknown = classify_events(self.events([40], queue=None), self.estimate)[0]
        queued = classify_events(self.events([40], queue=3), self.estimate)[0]
        self.assertIn('QUEUE_DEPTH_UNKNOWN', unknown.tags)
        self.assertIn('QUEUED_READ', queued.tags)
        self.assertFalse(unknown.trusted or queued.trusted)

    def test_wall_clock_step_invalidates_following_samples(self):
        events = self.events([40, 40, 40, 40])
        stepped = [events[0], events[1],
                   EventTimes(events[2].venue_event_ms, events[2].read_wall_ns + 30 * MS, events[2].read_perf_ns, 0),
                   EventTimes(events[3].venue_event_ms, events[3].read_wall_ns + 30 * MS, events[3].read_perf_ns, 0)]
        evidences = classify_events(stepped, self.estimate)
        self.assertEqual([('WALL_CLOCK_STEP' in e.tags) for e in evidences], [False, False, True, True])

    def test_stale_and_uncertain_offsets(self):
        late = [EventTimes(e.venue_event_ms + 400_000, e.read_wall_ns + 400_000 * MS, None, 0)
                for e in self.events([40])]
        evidence = classify_events(late, self.estimate, TimePolicy(max_offset_age_s=300))[0]
        self.assertIn('OFFSET_STALE', evidence.tags)
        tight = classify_events(self.events([40]), self.estimate, TimePolicy(max_offset_uncertainty_ms=1.0))[0]
        self.assertIn('OFFSET_UNCERTAIN', tight.tags)

    def test_nearest_rank(self):
        self.assertEqual(nearest_rank(list(range(1, 101)), 50), 50)
        self.assertEqual(nearest_rank(list(range(1, 101)), 99), 99)
        self.assertEqual(nearest_rank([7], 99), 7)


class ClockAndPilotTests(unittest.TestCase):
    def test_clock_assessment_flags_coarse_interval_clocks(self):
        clocks = assess_clocks()
        self.assertTrue(clocks['perf_counter']['ok_for_intervals'])
        coarse = clocks['monotonic']['resolution_s'] > TimePolicy().max_interval_clock_resolution_s
        self.assertEqual('LOW_RESOLUTION_CLOCK' in clocks['monotonic']['tags'], coarse)

    def pilot_db(self, base):
        """A synthetic DB with the S07-005 observations schema: local clock 600 ms behind the venue."""
        path = Path(base) / 'capture.sqlite3'
        with closing(sqlite3.connect(path)) as db, db:
            db.execute('CREATE TABLE observations (idx INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER NOT NULL, '
                       'origin TEXT NOT NULL, wire TEXT NOT NULL, wall_ns INTEGER NOT NULL, mono_ns INTEGER NOT NULL, '
                       'exchange_E_ms INTEGER, prev_hash TEXT NOT NULL, row_hash TEXT NOT NULL)')
            for i in range(200):
                venue = 1_793_970_618_470 + i * 100
                delay = 20 + (i * 37) % 400
                wall_ns = (venue - 600 + delay) * MS + 123
                mono_ns = 672_188_312 * MS + ((i * 100 + delay) // 16) * 16 * MS
                wire = json.dumps({'E': venue, 'T': venue, 'a': 4108738720 + i})
                db.execute('INSERT INTO observations(seq, origin, wire, wall_ns, mono_ns, exchange_E_ms, prev_hash, '
                           'row_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                           (4108738720 + i, 'ws', wire, wall_ns, mono_ns, venue, '0', '0'))
        return path

    def test_pilot_analysis_is_read_only_and_never_yields_latency(self):
        with tempfile.TemporaryDirectory() as t:
            path = self.pilot_db(t)
            before = (path.read_bytes(), sorted(p.name for p in Path(t).iterdir()))
            result = analyze_pilot_db(path)
            self.assertEqual(before, (path.read_bytes(), sorted(p.name for p in Path(t).iterdir())))
            self.assertEqual(result['observations'], 200)
            self.assertTrue(result['seq_contiguous'])
            self.assertAlmostEqual(result['lower_envelope_ms'], -580.0, delta=0.01)
            self.assertEqual(result['negative_raw_deltas'], 200)
            self.assertEqual(result['venue_E_minus_T_ms']['max'], 0)
            self.assertTrue(result['monotonic_steps_all_integer_ms'])
            self.assertEqual(result['evaluation']['status'], 'NO_CLOCK_MODEL')
            self.assertEqual(result['evaluation']['trusted'], 0)
            self.assertIsNone(result['evaluation']['latency_percentiles']['p99_ms'])
            with self.assertRaises(ValueError):
                analyze_pilot_db(path, report_path=path)
            with self.assertRaises(FileNotFoundError):
                analyze_pilot_db(Path(t) / 'missing.sqlite3')

    def test_cli(self):
        with tempfile.TemporaryDirectory() as t:
            path = self.pilot_db(t)
            report = Path(t) / 'clock.json'
            done = subprocess.run([sys.executable, '-m', 'ftdi_core', 'analyze-clock-evidence', '--pilot-db',
                                   str(path), '--report', str(report)], cwd=PROJECT_ROOT, capture_output=True,
                                  text=True, timeout=60)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(json.loads(done.stdout), json.loads(report.read_text(encoding='utf-8')))
            missing = subprocess.run([sys.executable, '-m', 'ftdi_core', 'analyze-clock-evidence', '--pilot-db',
                                      str(Path(t) / 'nope.sqlite3')], cwd=PROJECT_ROOT, capture_output=True,
                                     text=True, timeout=60)
            self.assertEqual(missing.returncode, 3)


if __name__ == '__main__':
    unittest.main()
