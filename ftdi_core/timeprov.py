"""FTDI-Core Task 003: timestamp provenance — pure evaluation functions (ADR-008).

Design: ``docs/TASK003_TIME_PROVENANCE_DESIGN.md``.  No network, no live capture, no credentials.

Conventions
-----------
* ``offset_ms`` (theta) is what must be ADDED to the local wall clock to obtain venue time:
  ``theta = venue_ms - local_wall_mid_ms`` from a Cristian sample ``(t0, venue_ms, t1)``.
* Latency estimate for an event: ``L = read_wall_ms + theta - venue_event_ms`` with ``+/- u(t)``.
* Same-host intervals use ``time.perf_counter_ns`` only; ``time.monotonic`` on Windows is
  ``GetTickCount64`` (15.6 ms) and is unusable for latency.
* Nothing here certifies production latency; reports always state their assumptions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
import time
from pathlib import Path

from .archive_e2e import _norm, _Snapshot, _Unopenable, _write_report

REPORT_SCHEMA = 'FTDI-Core-Clock-Evidence-v1'
ASSUMPTIONS = [
    "The venue's server-time clock and its event-time clock are assumed coherent (not externally verifiable).",
    'Cross-clock latency is an estimate bounded by the stated uncertainty; it is not a certified one-way delay.',
    'Samples carrying any exclusion tag are never included in percentiles.',
]
EXCLUDING_TAGS = frozenset({'CLOCK_UNCORRECTED', 'OFFSET_STALE', 'OFFSET_UNCERTAIN', 'NEGATIVE_BEYOND_UNCERTAINTY',
                            'WALL_CLOCK_STEP', 'QUEUED_READ', 'QUEUE_DEPTH_UNKNOWN'})


@dataclass(frozen=True)
class TimePolicy:
    max_interval_clock_resolution_s: float = 0.001
    max_offset_uncertainty_ms: float = 20.0
    max_offset_age_s: float = 300.0
    drift_ppm: float = 100.0
    max_rtt_ms: float = 1000.0
    wall_step_tolerance_ms: float = 5.0
    inconclusive_negative_fraction: float = 0.01
    min_samples_p50: int = 20
    min_samples_p95: int = 100
    min_samples_p99: int = 1000


@dataclass(frozen=True)
class OffsetSample:
    """Cristian sample: local wall before the request, venue server time, local wall after."""
    t0_wall_ns: int
    venue_ms: int
    t1_wall_ns: int

    def __post_init__(self):
        if not all(type(v) is int for v in (self.t0_wall_ns, self.venue_ms, self.t1_wall_ns)):
            raise ValueError('offset sample fields must be integers')
        if self.t1_wall_ns < self.t0_wall_ns:
            raise ValueError('offset sample has t1 < t0 (wall clock stepped backwards during the request)')

    @property
    def rtt_ms(self):
        return (self.t1_wall_ns - self.t0_wall_ns) / 1e6


@dataclass(frozen=True)
class OffsetEstimate:
    offset_ms: float
    uncertainty_ms: float
    at_wall_ns: int
    rtt_ms: float
    samples_used: int
    samples_rejected: int


@dataclass(frozen=True)
class EventTimes:
    venue_event_ms: int
    read_wall_ns: int
    read_perf_ns: int | None = None
    queue_depth_at_read: int | None = None


@dataclass(frozen=True)
class LatencyEvidence:
    raw_delta_ms: float
    latency_ms: float | None
    uncertainty_ms: float | None
    tags: tuple
    trusted: bool


def assess_clocks(policy=TimePolicy()):
    """Implementation/resolution of the local clocks and whether each may time intervals."""
    out = {}
    for name in ('time', 'perf_counter', 'monotonic'):
        info = time.get_clock_info(name)
        ok = info.monotonic and info.resolution <= policy.max_interval_clock_resolution_s
        out[name] = {'implementation': info.implementation, 'resolution_s': info.resolution,
                     'monotonic': info.monotonic, 'adjustable': info.adjustable, 'ok_for_intervals': ok,
                     'tags': [] if ok or not info.monotonic else ['LOW_RESOLUTION_CLOCK']}
    return out


def estimate_offset(samples, *, venue_resolution_ms=1.0, local_resolution_ms=None, policy=TimePolicy()):
    """Minimum-RTT Cristian estimate, or None if no sample is usable."""
    if local_resolution_ms is None:
        local_resolution_ms = time.get_clock_info('time').resolution * 1000
    usable = [s for s in samples if s.rtt_ms <= policy.max_rtt_ms]
    if not usable:
        return None
    best = min(usable, key=lambda s: (s.rtt_ms, s.t0_wall_ns))
    # Integer nanoseconds first: epoch-scale floats would lose sub-microsecond precision.
    offset_ms = (best.venue_ms * 2_000_000 - (best.t0_wall_ns + best.t1_wall_ns)) / 2_000_000
    return OffsetEstimate(offset_ms=offset_ms,
                          uncertainty_ms=best.rtt_ms / 2 + venue_resolution_ms + local_resolution_ms,
                          at_wall_ns=(best.t0_wall_ns + best.t1_wall_ns) // 2, rtt_ms=best.rtt_ms,
                          samples_used=len(usable), samples_rejected=len(samples) - len(usable))


def uncertainty_at(estimate, wall_ns, policy=TimePolicy()):
    age_ms = abs(wall_ns - estimate.at_wall_ns) / 1e6
    return estimate.uncertainty_ms + policy.drift_ppm * 1e-6 * age_ms


def classify_events(events, estimate, policy=TimePolicy()):
    """One LatencyEvidence per event; exclusion tags make a sample untrusted."""
    evidences, stepped, previous = [], False, None
    for event in events:
        tags = []
        raw = (event.read_wall_ns - event.venue_event_ms * 1_000_000) / 1e6
        if previous is not None and event.read_perf_ns is not None and previous.read_perf_ns is not None:
            disagreement = abs((event.read_wall_ns - previous.read_wall_ns) - (event.read_perf_ns - previous.read_perf_ns)) / 1e6
            if disagreement > policy.wall_step_tolerance_ms:
                stepped = True
        if stepped:
            tags.append('WALL_CLOCK_STEP')
        if event.queue_depth_at_read is None:
            tags.append('QUEUE_DEPTH_UNKNOWN')
        elif event.queue_depth_at_read > 0:
            tags.append('QUEUED_READ')
        latency = uncertainty = None
        if estimate is None:
            tags.append('CLOCK_UNCORRECTED')
        else:
            if abs(event.read_wall_ns - estimate.at_wall_ns) / 1e9 > policy.max_offset_age_s:
                tags.append('OFFSET_STALE')
            uncertainty = uncertainty_at(estimate, event.read_wall_ns, policy)
            if uncertainty > policy.max_offset_uncertainty_ms:
                tags.append('OFFSET_UNCERTAIN')
            latency = raw + estimate.offset_ms
            if latency < -uncertainty:
                tags.append('NEGATIVE_BEYOND_UNCERTAINTY')
        trusted = latency is not None and not (EXCLUDING_TAGS & set(tags))
        evidences.append(LatencyEvidence(raw, latency, uncertainty, tuple(tags), trusted))
        previous = event
    return evidences


def nearest_rank(values, p):
    """Nearest-rank percentile (deterministic, no interpolation)."""
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def percentile_report(evidences, policy=TimePolicy()):
    corrected = [e for e in evidences if e.latency_ms is not None]
    trusted = [e for e in evidences if e.trusted]
    counts = {}
    for e in evidences:
        for tag in e.tags:
            counts[tag] = counts.get(tag, 0) + 1
    negative = counts.get('NEGATIVE_BEYOND_UNCERTAINTY', 0)
    if not corrected:
        status = 'NO_CLOCK_MODEL'
    elif negative / len(corrected) > policy.inconclusive_negative_fraction:
        status = 'INCONCLUSIVE'
    else:
        status = 'ESTIMATED'
    percentiles = {}
    for p, minimum in ((50, policy.min_samples_p50), (95, policy.min_samples_p95), (99, policy.min_samples_p99)):
        key = f'p{p}_ms'
        if status != 'ESTIMATED' or len(trusted) < minimum:
            percentiles[key] = f'insufficient samples (need {minimum} trusted)' if status == 'ESTIMATED' else None
        else:
            percentiles[key] = round(nearest_rank([e.latency_ms for e in trusted], p), 3)
    return {'status': status, 'samples': len(evidences), 'corrected': len(corrected), 'trusted': len(trusted),
            'tag_counts': dict(sorted(counts.items())), 'latency_percentiles': percentiles,
            'max_included_uncertainty_ms': round(max((e.uncertainty_ms for e in trusted), default=0.0), 3),
            'assumptions': ASSUMPTIONS, 'certified': False}


def _summary(values):
    return {'min': round(min(values), 3), 'p50': round(nearest_rank(values, 50), 3),
            'p95': round(nearest_rank(values, 95), 3), 'max': round(max(values), 3)}


def analyze_pilot_db(path, policy=TimePolicy(), report_path=None):
    """Read-only retro-analysis of an S07-005 ``capture.sqlite3`` (observations table)."""
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    target = None
    if report_path is not None:
        target = Path(report_path).resolve()
        evidence = {_norm(Path(f'{path}{suffix}')) for suffix in ('', '-wal', '-shm', '-journal')}
        if target.is_dir() or not target.parent.is_dir() or _norm(target) in evidence:
            raise ValueError(f'invalid report path: {target}')
    try:
        snapshot = _Snapshot(path, 'pilot')
    except _Unopenable as exc:
        raise ValueError(f'refusing read-only open of the pilot DB ({exc})') from None
    try:
        rows = snapshot.conn.execute('SELECT idx, seq, wall_ns, mono_ns, exchange_E_ms, wire '
                                     'FROM observations ORDER BY idx').fetchall()
    finally:
        snapshot.close()
    if not rows:
        raise ValueError('pilot DB has no observations')
    events, e_minus_t, mono_steps = [], [], []
    for row in rows:
        wire = json.loads(row['wire'])
        events.append(EventTimes(venue_event_ms=row['exchange_E_ms'], read_wall_ns=row['wall_ns']))
        if isinstance(wire.get('T'), int):
            e_minus_t.append(row['exchange_E_ms'] - wire['T'])
    for a, b in zip(rows, rows[1:]):
        if b['mono_ns'] > a['mono_ns']:
            mono_steps.append((b['mono_ns'] - a['mono_ns']) / 1e6)
    raw = [(e.read_wall_ns - e.venue_event_ms * 1_000_000) / 1e6 for e in events]
    floor = min(raw)
    span_wall = (rows[-1]['wall_ns'] - rows[0]['wall_ns']) / 1e6
    span_mono = (rows[-1]['mono_ns'] - rows[0]['mono_ns']) / 1e6
    report = percentile_report(classify_events(events, None, policy), policy)
    seqs = [row['seq'] for row in rows]
    result = {
        'schema': REPORT_SCHEMA, 'source': str(path), 'mode': 'READ_ONLY_RETRO_ANALYSIS',
        'observations': len(rows), 'seq_first': seqs[0], 'seq_last': seqs[-1],
        'seq_contiguous': seqs == list(range(seqs[0], seqs[0] + len(seqs))),
        'raw_wall_minus_venue_ms': _summary(raw), 'negative_raw_deltas': sum(r < 0 for r in raw),
        'lower_envelope_ms': round(floor, 3), 'samples_within_10ms_of_envelope': sum(r - floor < 10 for r in raw),
        'venue_E_minus_T_ms': _summary(e_minus_t) if e_minus_t else None,
        'monotonic_min_positive_step_ms': round(min(mono_steps), 3) if mono_steps else None,
        'monotonic_steps_all_integer_ms': all(abs(s - round(s)) < 1e-9 for s in mono_steps),
        'wall_minus_monotonic_drift_ms': round(span_wall - span_mono, 3),
        'local_clocks_now': assess_clocks(policy),
        'evaluation': report,
        'interpretation': [
            'No offset estimate was recorded, so every sample is CLOCK_UNCORRECTED: no latency or percentile exists.',
            f'The lower envelope ({floor:.1f} ms) bounds the local-wall-minus-venue offset plus the minimum one-way delay; '
            'negative raw differences are clock offset, not negative latency.',
            'Receive times were taken in the same loop that persisted each message (read time, not arrival time) and '
            'without queue depth, so even with an offset model these samples would be excluded.',
        ],
        'policy': asdict(policy),
    }
    if target is not None:
        _write_report(target, result)
    return result
