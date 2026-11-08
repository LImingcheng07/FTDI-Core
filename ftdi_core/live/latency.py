"""Task 003 latency evidence for a verified live session (ADR-008 rules; nothing is certified)."""
from __future__ import annotations

import json
from pathlib import Path

from ..timeprov import (EventTimes, OffsetSample, TimePolicy, assess_clocks, estimate_offset, classify_events,
                        percentile_report)
from .events import FormatError, parse_ws
from .session import MANIFEST, START, split_lines, verify_session

V040_WALL_RESOLUTION_MS = 15.625   # sessions without a clocks record: the coarsest supported wall clock
                                   # (time.time_ns() on Windows: GetSystemTimeAsFileTime), whatever analyses them

SCHEMA = 'FTDI-Core-Live-Latency-v1'


def _lines(path):
    data = Path(path).read_bytes()
    return [json.loads(line) for line in split_lines(data)]   # newline only: U+2028 etc. may be inside a line


def nearest_estimator(samples, policy, local_resolution_ms=None):
    """event -> minimum-RTT estimate among the samples within ``max_offset_age_s`` of the event's read time,
    preferring samples taken under the same wall clock as the read (level within tolerance plus drift), so that a
    sample taken after a wall-clock step corrects the reads after it."""
    usable = sorted((s for s in samples if s.rtt_bound_ms <= policy.max_rtt_ms), key=lambda s: s.t0_wall_ns)
    window_ns = int(policy.max_offset_age_s * 1e9)
    tolerance_ns, rate = policy.wall_step_tolerance_ms * 1e6, policy.drift_ppm * 1e-6

    def estimate(event):
        near = [s for s in usable if abs((s.t0_wall_ns + s.t1_wall_ns) // 2 - event.read_wall_ns) <= window_ns]
        if not near:
            return None
        if event.read_perf_ns is not None:
            level = event.read_wall_ns - event.read_perf_ns
            same = [s for s in near if s.level_ns is not None and abs(level - s.level_ns)
                    <= tolerance_ns + rate * abs(event.read_perf_ns - (s.t0_perf_ns + s.t1_perf_ns) // 2)]
            found = estimate_offset(same, local_resolution_ms=local_resolution_ms, policy=policy) if same else None
            if found is not None:
                return found
        return estimate_offset(near, local_resolution_ms=local_resolution_ms, policy=policy)
    return estimate


def session_samples(session):
    """The clock samples of a session, with their interval stamps (wall clock checked against them per read)."""
    return [OffsetSample(s['t0_wall_ns'], s['venue_ms'], s['t1_wall_ns'], s['t0_perf_ns'], s['t1_perf_ns'])
            for s in _lines(Path(session) / 'clock.jsonl') if s['t1_wall_ns'] >= s['t0_wall_ns']]


def session_wall_resolution_ms(session):
    """Resolution of the wall clock the capture used (recorded since v0.4.1; v0.4.0 used ``time.time_ns``)."""
    start = json.loads((Path(session) / START).read_bytes().decode('utf-8'))
    clocks = start.get('clocks')
    if clocks:
        return clocks['wall']['resolution_ns'] / 1e6, clocks['wall']['implementation']
    return V040_WALL_RESOLUTION_MS, 'time.time_ns (no clocks record: GetSystemTimeAsFileTime resolution assumed)'


def analyze_session(session_dir, *, policy=TimePolicy()):
    """Latency evidence of the WS messages that first delivered each written event (read-only)."""
    session = Path(session_dir)
    report = verify_session(session)
    if report['status'] != 'VERIFIED':
        raise ValueError(f'session is not VERIFIED: {sorted(report["issue_counts"])}')
    manifest = json.loads((session / MANIFEST).read_bytes().decode('utf-8'))
    first, last = manifest['first_seq'], manifest['last_seq']
    samples = session_samples(session)
    events, seen, rest_only = [], set(), 0
    for record in _lines(session / 'raw_ws.jsonl'):
        try:
            venue = parse_ws(record['text'], manifest['symbol'])
        except FormatError:
            continue
        seq = venue['a']
        if first is None or not first <= seq <= last or seq in seen:
            continue   # outside the session, or a later duplicate delivery
        seen.add(seq)
        events.append(EventTimes(venue_event_ms=venue['E'], read_wall_ns=record['read_wall_ns'],
                                 read_perf_ns=record['read_perf_ns'], queue_depth_at_read=record['queue_depth']))
    if first is not None:
        rest_only = (last - first + 1) - len(seen)
    resolution_ms, wall_clock = session_wall_resolution_ms(session)
    evidences = classify_events(events, nearest_estimator(samples, policy, resolution_ms), policy)
    result = percentile_report(evidences, policy)
    result.update(schema=SCHEMA, session=str(session), session_id=manifest['session_id'],
                  public_network=manifest['public_network'], events_in_session=manifest['event_count'],
                  events_first_delivered_by_ws=len(events), events_recovered_by_rest_only=rest_only,
                  clock_samples=len(samples), clocks=assess_clocks(policy),
                  session_wall_clock={'implementation': wall_clock, 'resolution_ms': resolution_ms},
                  definition='latency = (read_wall + offset) - venue E, offset = venue clock minus local wall clock; '
                             'read time is taken immediately after '
                             'the WebSocket read in a reader thread that does not persist (queue depth recorded)')
    return result
