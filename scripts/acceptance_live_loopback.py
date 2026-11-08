"""FTDI-Core v0.4 live-pipeline acceptance on 127.0.0.1 (simulated venue; no network beyond loopback).

    python scripts/acceptance_live_loopback.py --workdir C:\\ftdi_live_acceptance

Three chained capture sessions against a scripted venue with real-world faults (a gap repaired by REST, a dropped
connection, duplicate deliveries), each verified from its raw evidence; the chain is composed, replayed,
delivered to an independent consumer process and audited archive-aware; a longer chain is re-composed into a new
run (duplicates only); latency evidence is computed under the ADR-008 rules. Exit 0 = all checks passed; evidence:
<workdir>/live_acceptance_evidence.json. Needs the repository's tests/ directory (the simulated venue).
"""
from __future__ import annotations

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import time

PROJECT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT), str(PROJECT / 'tests')]

from fakevenue import FakeVenue, core_event  # noqa: E402
from ftdi_core import __version__  # noqa: E402
from ftdi_core.archive_e2e import reconcile_archive_aware  # noqa: E402
from ftdi_core.delivery import local_lab  # noqa: E402
from ftdi_core.live import CaptureLimits, capture_session, verify_session  # noqa: E402
from ftdi_core.live.compose import compose_sessions  # noqa: E402
from ftdi_core.live.latency import analyze_session  # noqa: E402
from ftdi_core.service import replay  # noqa: E402

LIMITS = CaptureLimits(gap_wait_ms=50, gap_timeout_s=5.0, rest_min_interval_s=0, clock_interval_s=0.5,
                       ws_read_timeout_s=5.0, max_reconnects=3)
summary = {'checks': {}, 'sessions': []}


def check(name, ok, detail=None):
    summary['checks'][name] = {'ok': bool(ok), 'detail': detail}
    print(f'[{"PASS" if ok else "FAIL"}] {name}' + (f': {detail}' if detail is not None else ''), flush=True)
    if not ok:
        finish(False, name)


def finish(ok, reason=None):
    summary.update(result='PASS' if ok else 'FAIL', failure=reason, finished=time.strftime('%Y-%m-%d %H:%M:%S'))
    (WORK / 'live_acceptance_evidence.json').write_bytes(
        (json.dumps(summary, indent=2, ensure_ascii=False) + '\n').encode('utf-8'))
    print(f'LIVE ACCEPTANCE {summary["result"]}' + (f' ({reason})' if reason else ''), flush=True)
    raise SystemExit(0 if ok else 1)


def live_script(first, last, *, gap=None, duplicate_every=50):
    """Trades first..last stamped by the venue when sent, a missing range ``gap``, periodic duplicate deliveries."""
    script = []
    for seq in range(first, last + 1):
        if gap and gap[0] <= seq <= gap[1]:
            continue
        script.append(('trade_now', seq))
        if seq % duplicate_every == 0:
            script.append(('trade_now', seq))
        if seq % 10 == 0:
            script.append(('sleep', 0.005))
    return script


def capture(scripts, *, max_events, previous=None, rest_available):
    with FakeVenue(connections=scripts, rest_available=rest_available) as venue:
        result = capture_session(WORK / 'captures', symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url,
                                 max_events=max_events, max_seconds=60.0, previous_session=previous, limits=LIMITS)
    path = Path(result['session_dir'])
    report = verify_session(path, previous=previous)
    summary['sessions'].append({k: result[k] for k in ('session_id', 'first_seq', 'last_seq', 'event_count',
                                                       'stop_reason', 'chain')} | {
        'counts': result['counts'], 'verify': report['status']})
    print(json.dumps(summary['sessions'][-1]), flush=True)
    time.sleep(1.1)   # one session directory name per second
    return path, result, report


parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
parser.add_argument('--workdir', type=Path, required=True, help='new directory')
args = parser.parse_args()
WORK = args.workdir.resolve()
if WORK.exists():
    raise SystemExit(f'--workdir must not exist yet: {WORK}')
WORK.mkdir(parents=True)
summary.update(started=time.strftime('%Y-%m-%d %H:%M:%S'), ftdi_core_version=__version__,
               python=sys.version.split()[0], sqlite=sqlite3.sqlite_version)

# Session 1: a gap (101..140 never sent by WS) repaired by REST, duplicates in the stream.
s1, r1, v1 = capture([live_script(1, 1000, gap=(101, 140))], max_events=1000, rest_available=(1, 1000))
check('session 1: gap repaired by REST, VERIFIED', v1['status'] == 'VERIFIED' and r1['stop_reason'] == 'MAX_EVENTS'
      and r1['counts']['gaps_repaired'] >= 1 and r1['counts']['rest_records'] >= 40 and r1['counts']['ws_duplicates'] > 0,
      r1['counts'])
# Session 2: continues at 1001; the first connection drops after 1500, the second resumes at 1600.
s2, r2, v2 = capture([live_script(1001, 1500) + [('drop',)], live_script(1600, 2000)], max_events=1000,
                     previous=s1, rest_available=(1, 2000))
check('session 2: reconnect, catch-up, chain CONTIGUOUS, VERIFIED',
      v2['status'] == 'VERIFIED' and r2['chain'] == 'CONTIGUOUS' and r2['first_seq'] == 1001 and r2['last_seq'] == 2000
      and r2['counts']['reconnects'] >= 1, r2['counts'])
events = [json.loads(line) for path in (s1, s2) for line in (path / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
check('events 1..2000 equal the venue ground truth exactly (gap and reconnect range included)',
      events == [core_event(i) for i in range(1, 2001)])

chain1 = compose_sessions([s1, s2], WORK / 'chain1')
check('chain 1 composed 1..2000', (chain1['first_seq'], chain1['last_seq'], chain1['event_count']) == (1, 2000, 2000))
run1, consumer = WORK / 'run1', WORK / 'consumer' / 'effects.sqlite3'
consumer.parent.mkdir()
check('replay of chain 1 complete', replay(run1, [f'LIVE={WORK / "chain1" / "events.jsonl"}'], batch=256)['complete'])
lab = local_lab(run1, consumer, batch=256)
check('lab-e2e (separate consumer process) passes', lab['pass'], lab['delivery'])
audit = reconcile_archive_aware(run1, consumer)
check('archive-aware audit COMPLETE', audit['status'] == 'COMPLETE' and audit['metrics']['matched_effects'] == 2000,
      audit['metrics']['matched_effects'])

# Session 3 and a longer chain re-composed from its head into a new run: duplicates only for known events.
s3, r3, v3 = capture([live_script(2001, 2500)], max_events=500, previous=s2, rest_available=(1, 2500))
check('session 3 VERIFIED', v3['status'] == 'VERIFIED' and r3['last_seq'] == 2500)
compose_sessions([s1, s2, s3], WORK / 'chain2')
run2 = WORK / 'run2'
replay(run2, [f'LIVE={WORK / "chain2" / "events.jsonl"}'], batch=256)
lab2 = local_lab(run2, consumer, batch=256)
with closing(sqlite3.connect(f'{consumer.as_uri()}?mode=ro', uri=True)) as db:
    effects = db.execute('SELECT COUNT(*) FROM effects').fetchone()[0]
check('re-composed chain: 2000 duplicates, 500 new effects', lab2['pass'] and
      lab2['delivery']['remote_duplicates'] == 2000 and effects == 2500, lab2['delivery'])
check('archive-aware audit of the longer chain COMPLETE', reconcile_archive_aware(run2, consumer)['status'] == 'COMPLETE')

latency = analyze_session(s1)
summary['latency_session_1'] = {k: latency[k] for k in ('status', 'samples', 'trusted', 'tag_counts',
                                                        'latency_percentiles', 'events_recovered_by_rest_only')}
check('latency evidence follows ADR-008 (never certified)', latency['certified'] is False and
      latency['status'] in ('ESTIMATED', 'INCONCLUSIVE', 'NO_CLOCK_MODEL', 'NO_TRUSTED_SAMPLES'),
      summary['latency_session_1'])
finish(True)
