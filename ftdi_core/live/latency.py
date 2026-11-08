"""Task 003 latency evidence for a verified live session (ADR-008 rules; nothing is certified)."""
from __future__ import annotations

import json
from pathlib import Path

from ..timeprov import (EventTimes, OffsetSample, TimePolicy, assess_clocks, estimate_offset, classify_events,
                        percentile_report)
from .events import FormatError, parse_ws
from .session import MANIFEST, verify_session

SCHEMA = 'FTDI-Core-Live-Latency-v1'


def _lines(path):
    data = Path(path).read_bytes()
    return [json.loads(line) for line in data.decode('utf-8').splitlines()]


def nearest_estimator(samples, policy):
    """event -> minimum-RTT estimate among the samples within ``max_offset_age_s`` of the event's read time."""
    usable = sorted((s for s in samples if s.rtt_ms <= policy.max_rtt_ms), key=lambda s: s.t0_wall_ns)
    window_ns = int(policy.max_offset_age_s * 1e9)

    def estimate(event):
        near = [s for s in usable if abs((s.t0_wall_ns + s.t1_wall_ns) // 2 - event.read_wall_ns) <= window_ns]
        return estimate_offset(near, policy=policy) if near else None
    return estimate


def analyze_session(session_dir, *, policy=TimePolicy()):
    """Latency evidence of the WS messages that first delivered each written event (read-only)."""
    session = Path(session_dir)
    report = verify_session(session)
    if report['status'] != 'VERIFIED':
        raise ValueError(f'session is not VERIFIED: {sorted(report["issue_counts"])}')
    manifest = json.loads((session / MANIFEST).read_bytes().decode('utf-8'))
    first, last = manifest['first_seq'], manifest['last_seq']
    samples = [OffsetSample(s['t0_wall_ns'], s['venue_ms'], s['t1_wall_ns']) for s in _lines(session / 'clock.jsonl')
               if s['t1_wall_ns'] >= s['t0_wall_ns']]
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
    evidences = classify_events(events, nearest_estimator(samples, policy), policy)
    result = percentile_report(evidences, policy)
    result.update(schema=SCHEMA, session=str(session), session_id=manifest['session_id'],
                  public_network=manifest['public_network'], events_in_session=manifest['event_count'],
                  events_first_delivered_by_ws=len(events), events_recovered_by_rest_only=rest_only,
                  clock_samples=len(samples), clocks=assess_clocks(policy),
                  definition='latency = (read_wall_ns - offset) - venue E; read time is taken immediately after '
                             'the WebSocket read in a reader thread that does not persist (queue depth recorded)')
    return result
