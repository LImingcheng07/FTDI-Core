"""Compose a verified chain of sealed sessions into one immutable replay source (Task 004 phase 2)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil

from .. import __version__
from .events import canonical, check_event, lane, loads
from .session import DATA_FILES, MANIFEST, START, file_facts, sha256_bytes, utc_iso, verify_session, write_new_json

SCHEMA = 'FTDI-Live-Chain-v1'


def compose_sessions(sessions, output_dir):
    """Verify ``sessions`` (in order; empty sessions allowed) as one contiguous chain and write
    ``<output_dir>/events.jsonl`` + ``CHAIN_MANIFEST.json``. ``output_dir`` must not exist yet."""
    sessions = [Path(s) for s in sessions]
    if not sessions:
        raise ValueError('at least one session is required')
    output = Path(output_dir)
    if output.exists():
        raise ValueError(f'output directory already exists: {output}')
    before = [_fingerprint(session) for session in sessions]
    entries, symbol, last_nonempty, expected = [], None, None, None
    for index, session in enumerate(sessions):
        report = verify_session(session, previous=last_nonempty)
        if report['status'] != 'VERIFIED':
            raise ValueError(f'session {index + 1} ({session.name}) is not VERIFIED in this chain: '
                             f'{sorted(code for code in report["issue_counts"] if code not in report["notice_codes"])}')
        data = (session / MANIFEST).read_bytes()
        manifest = json.loads(data.decode('utf-8'))
        if symbol is None:
            symbol = manifest['symbol']
        elif manifest['symbol'] != symbol:
            raise ValueError(f'session {session.name} has another symbol')
        if index > 0 and last_nonempty is None:
            # every session before was empty: this one must still link to the same predecessor
            if manifest['previous'] != entries[0]['previous']:
                raise ValueError(f'session {session.name} does not continue the chain')
        if manifest['event_count']:
            if expected is not None and manifest['first_seq'] != expected:
                raise ValueError(f'session {session.name} does not start at {expected}')
            expected = manifest['last_seq'] + 1
            last_nonempty = session
        entries.append({'session_id': manifest['session_id'], 'path': str(session.resolve()),
                        'manifest_sha256': sha256_bytes(data), 'previous': manifest['previous'],
                        'first_seq': manifest['first_seq'], 'last_seq': manifest['last_seq'],
                        'event_count': manifest['event_count'], 'stop_reason': manifest['stop_reason'],
                        'events_sha256': manifest['files']['events.jsonl']['sha256']})
    nonempty = [e for e in entries if e['event_count']]
    if not nonempty:
        raise ValueError('all sessions are empty; nothing to compose')
    output.mkdir(parents=True)
    try:
        facts, count = _write_events(output / 'events.jsonl', entries, nonempty, symbol)
        if [_fingerprint(session) for session in sessions] != before:
            raise ValueError('a session changed during composition; refused')
    except BaseException:
        shutil.rmtree(output)   # only the directory this call created; no session file is touched
        raise
    manifest = {'schema': SCHEMA, 'symbol': symbol, 'lane': lane(symbol), 'created_utc': utc_iso(),
                'software_version': __version__, 'first_seq': nonempty[0]['first_seq'],
                'last_seq': nonempty[-1]['last_seq'], 'event_count': count,
                'chain_head': {'session_id': entries[0]['session_id'], 'previous': entries[0]['previous']},
                'sessions': entries, 'files': {'events.jsonl': facts}}
    write_new_json(output / 'CHAIN_MANIFEST.json', manifest)
    return {**manifest, 'output_dir': str(output)}


def _fingerprint(session):
    """SHA-256 of every file a verification reads (to detect a change between verification and copying)."""
    out = {}
    for name in (MANIFEST, START) + DATA_FILES:
        path = Path(session) / name
        out[name] = sha256_bytes(path.read_bytes()) if path.is_file() else None
    return out


def _write_events(target, entries, nonempty, symbol):
    """Concatenate the verified events and re-check the output line by line."""
    with open(target, 'xb') as handle:
        for entry in entries:
            data = (Path(entry['path']) / 'events.jsonl').read_bytes()
            if sha256_bytes(data) != entry['events_sha256']:
                raise ValueError(f'events of {entry["session_id"]} changed during composition')
            handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    data = Path(target).read_bytes()
    lines = data.decode('utf-8').splitlines()
    first, last = nonempty[0]['first_seq'], nonempty[-1]['last_seq']
    count = sum(e['event_count'] for e in entries)
    if len(lines) != count or last - first + 1 != count:
        raise ValueError('composed events do not cover the declared range; refused')
    for index, line in enumerate(lines):
        event = loads(line)
        check_event(event, symbol)
        if canonical(event) != line or event['source_seq'] != first + index:
            raise ValueError(f'composed line {index} is not the expected event; refused')
    return file_facts(target), count
