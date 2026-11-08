"""Session directory, sealing, read-only verification and recovery (docs/LIVE_CAPTURE.md §4-§6)."""
from __future__ import annotations

import base64
import binascii
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time

from .. import __version__

from .events import FormatError, bytes_records_lenient, canonical, check_event, check_symbol, compare, core_event, \
    lane, loads, parse_rest_page, parse_ws, rest_records_lenient, ws_record_lenient
from .limits import limits_problem

SCHEMA = 'FTDI-Live-Session-v1'
START_SCHEMA = 'FTDI-Live-Capture-Start-v1'
DATA_FILES = ('events.jsonl', 'raw_ws.jsonl', 'raw_rest.jsonl', 'clock.jsonl')
MANIFEST = 'SESSION_MANIFEST.json'
START = 'CAPTURE_START.json'
NORMAL_STOPS = ('MAX_EVENTS', 'MAX_SECONDS', 'INTERRUPTED')
STOP_REASONS = frozenset(NORMAL_STOPS + ('GAP_BLOCKED', 'HALTED_CONFLICT', 'FORMAT_ERROR', 'READER_OVERFLOW',
                                         'PENDING_OVERFLOW', 'RATE_LIMITED', 'CONNECTION_FAILED', 'RECOVERED'))
COUNT_KEYS = ('ws_messages', 'ws_duplicates', 'rest_pages', 'rest_records', 'rest_duplicates', 'text_variants',
              'before_anchor', 'connections', 'reconnects', 'clock_samples', 'gaps_repaired')
RAW_WS_KEYS = frozenset({'conn', 'n', 'read_wall_ns', 'read_perf_ns', 'queue_depth', 'text'})
RAW_REST_KEYS = frozenset({'n', 'kind', 'url', 'status', 't0_wall_ns', 't0_perf_ns', 't1_wall_ns', 't1_perf_ns',
                           'body', 'body_base64', 'error'})
CLOCK_KEYS = frozenset({'t0_wall_ns', 't0_perf_ns', 'venue_ms', 't1_wall_ns', 't1_perf_ns'})
NOTICE_CODES = frozenset({'LEFTOVER_PART_FILE', 'RAW_AFTER_STOP'})
MANIFEST_KEYS = frozenset({'schema', 'session_id', 'symbol', 'lane', 'software_version', 'started_utc', 'sealed_utc',
                           'sealed_by', 'first_seq', 'last_seq', 'event_count', 'previous', 'chain', 'stop_reason',
                           'stop_detail', 'files', 'counts', 'endpoints', 'public_network', 'limits',
                           'capture_start_sha256', 'interpreted'})
START_KEYS = frozenset({'schema', 'symbol', 'lane', 'started_utc', 'software_version', 'endpoints', 'public_network',
                        'limits', 'max_events', 'max_seconds', 'previous'})
START_OPTIONAL = frozenset({'clocks'})   # since v0.4.1: the wall and interval clocks the capture used


def utc_compact():
    return time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())


def utc_iso():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def facts_of(data):
    return {'bytes': len(data), 'sha256': sha256_bytes(data), 'lines': data.count(b'\n')}


def file_facts(path):
    return facts_of(Path(path).read_bytes())


def write_new_json(path, obj):
    """Write a JSON file that must not exist yet: temp file, fsync, atomic rename."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(f'{path.name} already exists')
    temp = path.with_name(path.name + '.tmp')
    with open(temp, 'xb') as handle:
        handle.write((canonical(obj) + '\n').encode('utf-8'))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def create_session_dir(capture_dir, symbol):
    base = Path(capture_dir) / symbol
    base.mkdir(parents=True, exist_ok=True)
    stem = f'session-{utc_compact()}'
    for k in range(1000):
        candidate = base / (stem if k == 0 else f'{stem}-{k}')
        try:
            os.mkdir(candidate)
            return candidate
        except FileExistsError:
            continue
    raise ValueError('cannot allocate a session directory name')


def previous_link(previous_dir):
    """(link dict for the manifest, previous manifest) of a verified, non-empty session."""
    report = verify_session(previous_dir)
    if report['status'] != 'VERIFIED':
        raise ValueError(f'previous session is not VERIFIED ({sorted(report["issue_counts"])}); refused')
    data = (Path(previous_dir) / MANIFEST).read_bytes()
    manifest = json.loads(data)
    if manifest['last_seq'] is None:
        raise ValueError('previous session is empty; chain from the last non-empty session')
    return {'session_id': manifest['session_id'], 'last_seq': manifest['last_seq'],
            'manifest_sha256': sha256_bytes(data)}, manifest


# -- raw evidence (shared by sealing, verification and recovery) ----------------------------------

MAX_INT64 = 2 ** 63 - 1   # clocks, trade ids and SQLite integers are 64-bit; larger values are not evidence


def _is_int(value, *, nullable=False):
    return (value is None and nullable) or (type(value) is int and 0 <= value <= MAX_INT64)


def split_lines(data):
    """Complete lines of a file that must end with a newline (or be empty)."""
    if data and not data.endswith(b'\n'):
        raise FormatError('file does not end with a newline')
    return [line.decode('utf-8') for line in data.split(b'\n')[:-1]] if data else []


def raw_ws_record(line):
    obj = loads(line)
    if (not isinstance(obj, dict) or set(obj) != RAW_WS_KEYS or not isinstance(obj['text'], str)
            or not all(_is_int(obj[k]) for k in ('conn', 'n', 'read_wall_ns', 'read_perf_ns', 'queue_depth'))):
        raise FormatError('raw_ws line has an unexpected shape')
    return obj


def raw_rest_record(line):
    obj = loads(line)
    if (not isinstance(obj, dict) or set(obj) != RAW_REST_KEYS or obj['kind'] not in ('aggTrades', 'time')
            or not isinstance(obj['url'], str) or not _is_int(obj['status'], nullable=True)
            or not all(_is_int(obj[k]) for k in ('n', 't0_wall_ns', 't0_perf_ns', 't1_wall_ns', 't1_perf_ns'))
            or not all(obj[k] is None or isinstance(obj[k], str) for k in ('body', 'body_base64', 'error'))):
        raise FormatError('raw_rest line has an unexpected shape')
    return obj


def clock_record(line):
    obj = loads(line)
    if not isinstance(obj, dict) or set(obj) != CLOCK_KEYS or not all(_is_int(obj[k]) for k in CLOCK_KEYS):
        raise FormatError('clock line has an unexpected shape')
    return obj


def parse_raw(name, data, parser):
    """Records of a raw file with consecutive ``n`` (when present); raises FormatError on any bad line."""
    records = []
    for index, line in enumerate(split_lines(data)):
        try:
            record = parser(line)
        except FormatError as exc:
            raise FormatError(f'{name} line {index}: {exc}') from None
        if 'n' in record and record['n'] != index:
            raise FormatError(f'{name} line {index}: n is not consecutive')
        records.append(record)
    return records


def evidence_index(ws_records, rest_records, symbol, interpreted, issue):
    """seq -> events derived from every raw record (lenient). Records the capture interpreted must be valid;
    records after the stop (not interpreted) are evidence but may be malformed (notice).
    Returns (evidence, number of REST records)."""
    evidence, rest_count = {}, 0
    for index, record in enumerate(ws_records):
        venue = ws_record_lenient(record['text'], symbol)
        if venue is None:
            issue('RAW_UNPARSEABLE' if index < interpreted['raw_ws'] else 'RAW_AFTER_STOP', f'raw_ws line {index}')
            continue
        evidence.setdefault(venue['a'], []).append(core_event(venue, symbol))
    for index, record in enumerate(rest_records):
        if record['kind'] != 'aggTrades' or record['status'] != 200:
            continue
        if record['body'] is None:
            if record['body_base64'] is None:
                continue
            issue('RAW_UNPARSEABLE' if index < interpreted['raw_rest'] else 'RAW_AFTER_STOP',
                  f'raw_rest line {index}: body not UTF-8 or too large')
            try:
                venues = bytes_records_lenient(base64.b64decode(record['body_base64'], validate=True))
            except (binascii.Error, ValueError):
                issue('RAW_UNPARSEABLE', f'raw_rest line {index}: body_base64 is not base64')
                continue
            for venue in venues:
                rest_count += 1
                evidence.setdefault(venue['a'], []).append(core_event(venue, symbol))
            continue
        venues, problem = rest_records_lenient(record['body'])
        if problem:
            issue('RAW_UNPARSEABLE' if index < interpreted['raw_rest'] else 'RAW_AFTER_STOP',
                  f'raw_rest line {index}: {problem}')
        for venue in venues:
            rest_count += 1
            evidence.setdefault(venue['a'], []).append(core_event(venue, symbol))
    return evidence, rest_count


def first_conflict(events, evidence):
    """Smallest seq of ``events`` (dict seq -> event) contradicted by any raw record, or None."""
    conflicting = [seq for seq, event in events.items()
                   if any(compare(event, other) == 'conflict' for other in evidence.get(seq, ()))]
    return min(conflicting) if conflicting else None


class SessionFiles:
    """Append-only ``.part`` files of a running capture."""

    def __init__(self, directory):
        self.dir = Path(directory)
        self.handles = {}
        self.lines = Counter()
        try:
            for name in DATA_FILES:
                self.handles[name] = open(self.dir / f'{name}.part', 'xb')
        except BaseException:
            self.close()
            raise

    def append(self, name, obj):
        self.handles[name].write((canonical(obj) + '\n').encode('utf-8'))
        self.lines[name] += 1

    def sync(self):
        for handle in self.handles.values():
            if not handle.closed:
                handle.flush()
                os.fsync(handle.fileno())

    def close(self):
        for handle in self.handles.values():
            if not handle.closed:
                handle.close()

    def read(self, name):
        handle = self.handles[name]
        handle.flush()
        return (self.dir / f'{name}.part').read_bytes()

    def seal(self, fields):
        self.sync()
        self.close()
        for name in DATA_FILES:
            if (self.dir / name).exists():
                raise FileExistsError(f'{name} already exists')
        for name in DATA_FILES:
            os.rename(self.dir / f'{name}.part', self.dir / name)
        return finish_manifest(self.dir, fields)


def finish_manifest(directory, fields):
    directory = Path(directory)
    manifest = dict(fields)
    manifest.update(schema=SCHEMA, session_id=directory.name, sealed_utc=utc_iso(), software_version=__version__,
                    files={name: file_facts(directory / name) for name in DATA_FILES},
                    capture_start_sha256=sha256_bytes((directory / START).read_bytes()))
    write_new_json(directory / MANIFEST, manifest)
    return manifest


# -- verification ---------------------------------------------------------------------------------

def _str(value, nullable=False):
    return isinstance(value, str) or (nullable and value is None)


def manifest_problem(m, directory):
    """Shape and type problems of a manifest (MANIFEST_INVALID); never raises."""
    if not isinstance(m, dict) or set(m) - {'recovered_from'} != MANIFEST_KEYS or m.get('schema') != SCHEMA:
        return 'fields or schema'
    if not all(_str(m[k]) for k in ('session_id', 'symbol', 'sealed_utc', 'stop_reason', 'stop_detail', 'sealed_by',
                                    'chain', 'software_version', 'capture_start_sha256')) \
            or not _str(m['started_utc'], nullable=True):
        return 'field types'
    try:
        check_symbol(m['symbol'])
    except ValueError:
        return 'symbol'
    if m['session_id'] != Path(directory).name or m['lane'] != lane(m['symbol']):
        return 'session id or lane'
    if m['stop_reason'] not in STOP_REASONS or m['sealed_by'] not in ('capture', 'recovery'):
        return 'stop reason or sealed_by'
    recovery = m['sealed_by'] == 'recovery'
    if recovery != (m['stop_reason'] == 'RECOVERED') or recovery != ('recovered_from' in m):
        return 'recovery fields'
    first, last, count = m['first_seq'], m['last_seq'], m['event_count']
    if not _is_int(count):
        return 'event_count'
    if count == 0:
        if first is not None or last is not None:
            return 'empty session with sequence numbers'
    elif not (_is_int(first) and _is_int(last) and first <= last):
        return 'first_seq / last_seq'
    if not isinstance(m['files'], dict) or set(m['files']) != set(DATA_FILES) or not all(
            isinstance(f, dict) and set(f) == {'bytes', 'sha256', 'lines'} for f in m['files'].values()):
        return 'files'
    if not isinstance(m['counts'], dict) or set(m['counts']) != set(COUNT_KEYS) or not all(
            _is_int(v, nullable=recovery) for v in m['counts'].values()):
        return 'counts'
    interpreted = m['interpreted']
    if not isinstance(interpreted, dict) or set(interpreted) != {'raw_ws', 'raw_rest'} or not all(
            _is_int(v) for v in interpreted.values()):
        return 'interpreted'
    if m['chain'] not in ('FIRST', 'CONTIGUOUS'):
        return 'chain'
    previous = m['previous']
    if previous is not None and (not isinstance(previous, dict)
                                 or set(previous) != {'session_id', 'last_seq', 'manifest_sha256'}
                                 or not _str(previous['session_id']) or not _is_int(previous['last_seq'])
                                 or not _str(previous['manifest_sha256'])):
        return 'previous'
    problem = options_problem(m['endpoints'], m['public_network'], m['limits'])
    if problem:
        return problem
    if recovery and (not isinstance(m['recovered_from'], dict) or not all(
            isinstance(k, str) and k.endswith('.part') and k[:-5] in DATA_FILES and isinstance(v, dict)
            and set(v) == {'bytes', 'sha256'} for k, v in m['recovered_from'].items())):
        return 'recovered_from'
    return None


def chain_problem(m):
    """Coherence of the chain statement with the recorded link (CHAIN_MISMATCH)."""
    previous = m['previous']
    if previous is None:
        return None if m['chain'] == 'FIRST' else 'chain CONTIGUOUS without a previous session'
    if m['chain'] != 'CONTIGUOUS':
        return 'chain FIRST although a previous session is recorded'
    if m['first_seq'] is not None and m['first_seq'] != previous['last_seq'] + 1:
        return 'first_seq does not continue the previous session'
    return None


PUBLIC_ENDPOINTS = {'ws': 'wss://data-stream.binance.vision', 'rest': 'https://data-api.binance.vision'}


def options_problem(endpoints, public_network, limits):
    """Endpoints, network mode and limits must be ones a capture can actually have used."""
    if type(public_network) is not bool:
        return 'public_network'
    if not isinstance(endpoints, dict) or set(endpoints) != {'ws', 'rest'}:
        return 'endpoints'
    if public_network:
        if endpoints != PUBLIC_ENDPOINTS:
            return 'endpoints differ from the fixed public endpoints'
    else:
        for key, scheme in (('ws', 'ws'), ('rest', 'http')):
            match = isinstance(endpoints[key], str) and re.fullmatch(rf'{scheme}://127\.0\.0\.1:([1-9][0-9]{{0,4}})',
                                                                    endpoints[key])
            if not match or int(match.group(1)) > 65535:
                return 'endpoints are not loopback endpoints'
    return limits_problem(limits, public_network)


MAX_CLOCK_RESOLUTION_NS = 1_000_000_000   # a clock coarser than 1 s cannot serve as capture evidence


def _clocks_ok(clocks):
    return (isinstance(clocks, dict) and set(clocks) == {'wall', 'interval'}
            and all(isinstance(c, dict) and set(c) == {'implementation', 'resolution_ns'}
                    and isinstance(c['implementation'], str) and 0 < len(c['implementation']) <= 200
                    and type(c['resolution_ns']) is int and 1 <= c['resolution_ns'] <= MAX_CLOCK_RESOLUTION_NS
                    for c in clocks.values()))


def start_problem(start, m=None):
    """CAPTURE_START.json must be well formed and describe the same capture as the manifest."""
    if not isinstance(start, dict) or not START_KEYS <= set(start) <= START_KEYS | START_OPTIONAL \
            or start['schema'] != START_SCHEMA:
        return 'fields or schema'
    if 'clocks' in start and not _clocks_ok(start['clocks']):
        return 'clocks'
    try:
        check_symbol(start['symbol'])
    except ValueError:
        return 'symbol'
    if start['lane'] != lane(start['symbol']) or not _str(start['started_utc']) or not _str(start['software_version']):
        return 'lane, start time or version'
    if type(start['max_events']) is not int or start['max_events'] < 1 or \
            type(start['max_seconds']) not in (int, float) or not start['max_seconds'] > 0:
        return 'capture bounds'
    previous = start['previous']
    if previous is not None and (not isinstance(previous, dict)
                                 or set(previous) != {'session_id', 'last_seq', 'manifest_sha256'}
                                 or not _str(previous['session_id']) or not _is_int(previous['last_seq'])
                                 or not _str(previous['manifest_sha256'])):
        return 'previous'
    problem = options_problem(start['endpoints'], start['public_network'], start['limits'])
    if problem or m is None:
        return problem
    if start['symbol'] != m['symbol'] or start['lane'] != m['lane'] or start['previous'] != m['previous']:
        return 'symbol, lane or previous differ from the manifest'
    if m['sealed_by'] == 'capture' and (start['endpoints'] != m['endpoints']
                                        or start['public_network'] != m['public_network']
                                        or start['limits'] != m['limits'] or start['started_utc'] != m['started_utc']):
        return 'endpoints, limits or start time differ from the manifest'
    return None


def verify_session(session_dir, *, previous=None):
    """Read-only verification of a sealed session. Never creates or changes a file; never raises on content."""
    directory = Path(session_dir)
    if not directory.is_dir():
        raise FileNotFoundError(f'session directory not found: {directory}')
    issues, samples = Counter(), []
    metrics = {'events': 0, 'raw_ws_messages': 0, 'raw_rest_records': 0, 'clock_samples': 0, 'chain_checked': False}

    def issue(code, detail=None):
        issues[code] += 1
        if len(samples) < 20:
            samples.append({'code': code, 'detail': detail})

    def report():
        fatal = [code for code in issues if code not in NOTICE_CODES]
        return {'schema': 'FTDI-Live-Session-Verification-v1', 'session': str(directory),
                'status': 'INVALID' if fatal else 'VERIFIED', 'issue_counts': dict(sorted(issues.items())),
                'notice_codes': sorted(NOTICE_CODES), 'samples': samples, 'metrics': metrics}

    manifest_path = directory / MANIFEST
    if not manifest_path.is_file():
        issue('UNSEALED', 'no SESSION_MANIFEST.json')
        return report()
    try:
        m = json.loads(manifest_path.read_bytes().decode('utf-8'))
        problem = manifest_problem(m, directory)
    except (ValueError, UnicodeDecodeError, RecursionError, TypeError, KeyError) as exc:
        problem = f'unreadable: {type(exc).__name__}'
    if problem:
        issue('MANIFEST_INVALID', problem)
        return report()
    symbol = m['symbol']
    problem = chain_problem(m)
    if problem:
        issue('CHAIN_MISMATCH', problem)
    contents = {}
    for name in DATA_FILES + (START,):
        path = directory / name
        if not path.is_file():
            issue('FILE_MISSING', name)
            continue
        data = path.read_bytes()
        if name == START:
            if sha256_bytes(data) != m['capture_start_sha256']:
                issue('FILE_MISMATCH', name)
            try:
                problem = start_problem(json.loads(data.decode('utf-8')), m)
            except (ValueError, UnicodeDecodeError, RecursionError, TypeError, KeyError):
                problem = 'unreadable'
            if problem:
                issue('MANIFEST_INVALID', f'{START}: {problem}')
        elif m['files'][name] != facts_of(data):
            issue('FILE_MISMATCH', name)
        contents[name] = data
    recovered = m.get('recovered_from', {})
    for leftover in sorted(directory.glob('*.part')):
        issue('LEFTOVER_PART_FILE', leftover.name)
        if leftover.name not in recovered:
            issue('FILE_MISMATCH', f'{leftover.name} is not recorded in recovered_from')
    for name, facts in recovered.items():
        path = directory / name
        if not path.is_file():
            issue('FILE_MISSING', name)
            continue
        data = path.read_bytes()
        if {'bytes': len(data), 'sha256': sha256_bytes(data)} != facts:
            issue('FILE_MISMATCH', name)
            continue
        final = contents.get(name[:-5])
        complete = data[:data.rfind(b'\n') + 1]
        if final is not None and name != 'events.jsonl.part' and final != complete:
            issue('FILE_MISMATCH', f'{name[:-5]} is not the complete lines of {name}')
        if final is not None and name == 'events.jsonl.part' and not complete.startswith(final):
            issue('FILE_MISMATCH', 'events.jsonl is not a prefix of events.jsonl.part')

    parsed = {}
    for name, parser in (('raw_ws.jsonl', raw_ws_record), ('raw_rest.jsonl', raw_rest_record),
                         ('clock.jsonl', clock_record)):
        if name not in contents:
            parsed[name] = []
            continue
        try:
            parsed[name] = parse_raw(name, contents[name], parser)
        except (FormatError, UnicodeDecodeError) as exc:
            issue('RAW_UNPARSEABLE', str(exc))
            parsed[name] = []
    metrics['raw_ws_messages'] = len(parsed['raw_ws.jsonl'])
    metrics['clock_samples'] = len(parsed['clock.jsonl'])
    if any(v > len(parsed[f'{k}.jsonl']) for k, v in m['interpreted'].items()):
        issue('MANIFEST_INVALID', 'interpreted exceeds the raw files')
    evidence, metrics['raw_rest_records'] = evidence_index(parsed['raw_ws.jsonl'], parsed['raw_rest.jsonl'], symbol,
                                                           m['interpreted'], issue)

    if 'events.jsonl' in contents:
        try:
            lines = split_lines(contents['events.jsonl'])
        except (FormatError, UnicodeDecodeError) as exc:
            issue('EVENT_INVALID', str(exc))
            lines = []
        first, count = m['first_seq'], m['event_count']
        contiguous = len(lines) == count and (count == 0 or m['last_seq'] - first + 1 == count)
        for index, line in enumerate(lines):
            try:
                event = loads(line)
                if canonical(event) != line:
                    raise FormatError('line is not canonical JSON')
                check_event(event, symbol)
            except FormatError as exc:
                issue('EVENT_INVALID', f'line {index}: {exc}')
                contiguous = False
                continue
            seq = event['source_seq']
            if first is None or seq != first + index:
                contiguous = False
            found = evidence.get(seq, ())
            if not any(compare(event, other) != 'conflict' for other in found):
                issue('EVENT_WITHOUT_RAW_EVIDENCE', seq)
            if any(compare(event, other) == 'conflict' for other in found):
                issue('RAW_CONFLICT', seq)
            metrics['events'] += 1
        if not contiguous:
            issue('EVENT_SEQUENCE_GAP', {'first_seq': first, 'last_seq': m['last_seq'], 'lines': len(lines)})

    if previous is not None:
        metrics['chain_checked'] = True
        try:
            data = (Path(previous) / MANIFEST).read_bytes()
            prev = json.loads(data.decode('utf-8'))
            prev_ok = verify_session(previous)['status'] == 'VERIFIED'
            link = {'session_id': prev.get('session_id'), 'last_seq': prev.get('last_seq'),
                    'manifest_sha256': sha256_bytes(data)}
        except (OSError, ValueError, UnicodeDecodeError, AttributeError, RecursionError) as exc:
            issue('CHAIN_MISMATCH', f'previous session unreadable: {type(exc).__name__}')
        else:
            if not prev_ok or m['previous'] != link or prev.get('symbol') != symbol:
                issue('CHAIN_MISMATCH', 'previous session differs from the recorded link or is not VERIFIED')
    return report()


# -- recovery -------------------------------------------------------------------------------------

def seal_session(session_dir, *, apply=False):
    """Seal a killed capture from its complete lines; the ``.part`` files are kept.

    Also handles a capture killed between the renames of sealing (some data files already final)."""
    directory = Path(session_dir)
    if not directory.is_dir():
        raise FileNotFoundError(f'session directory not found: {directory}')
    if (directory / MANIFEST).exists():
        raise ValueError('session is already sealed')
    try:
        start_bytes = (directory / START).read_bytes()
        start = json.loads(start_bytes.decode('utf-8'))
        problem = start_problem(start)
    except (OSError, ValueError, KeyError, TypeError, UnicodeDecodeError, RecursionError) as exc:
        problem = f'{type(exc).__name__}: {exc}'
    if problem:
        raise ValueError(f'{START} missing or invalid ({problem}); cannot recover')
    symbol, previous = start['symbol'], start['previous']
    parts, complete = {}, {}
    for name in DATA_FILES:
        part, final = directory / f'{name}.part', directory / name
        if part.is_file() and final.exists():
            raise ValueError(f'both {name} and {name}.part exist; refused')
        if part.is_file():
            data = part.read_bytes()
            parts[name] = data
            complete[name] = data[:data.rfind(b'\n') + 1]
        elif final.is_file():
            complete[name] = final.read_bytes()   # already renamed by an interrupted seal: complete by construction
            if complete[name] and not complete[name].endswith(b'\n'):
                raise ValueError(f'{name} does not end with a newline; refused')
        else:
            raise ValueError(f'{name}.part missing; cannot recover')
    try:
        ws = parse_raw('raw_ws.jsonl', complete['raw_ws.jsonl'], raw_ws_record)
        rest = parse_raw('raw_rest.jsonl', complete['raw_rest.jsonl'], raw_rest_record)
        parse_raw('clock.jsonl', complete['clock.jsonl'], clock_record)
    except (FormatError, UnicodeDecodeError) as exc:
        raise ValueError(f'corrupt raw evidence ({exc}); refusing to guess') from None
    interpreted = {'raw_ws': 0, 'raw_rest': 0}   # unknown after a kill: all raw lines are treated as evidence only
    evidence, _ = evidence_index(ws, rest, symbol, interpreted, lambda code, detail=None: None)
    expected = previous['last_seq'] + 1 if previous else None
    kept, written = [], {}
    for line in split_lines(complete['events.jsonl']):
        try:
            event = loads(line)
            if canonical(event) != line:
                break
            check_event(event, symbol)
        except FormatError:
            break
        seq = event['source_seq']
        if (expected is not None and seq != expected) or seq in written:
            break
        found = evidence.get(seq, ())
        if not any(compare(event, other) != 'conflict' for other in found):
            break
        kept.append(line)
        written[seq] = event
        expected = seq + 1
    cut = first_conflict(written, evidence)
    if cut is not None:
        kept = [line for line in kept if loads(line)['source_seq'] < cut]
    first = loads(kept[0])['source_seq'] if kept else None
    last = loads(kept[-1])['source_seq'] if kept else None
    would = {'first_seq': first, 'last_seq': last, 'event_count': len(kept)}
    if not apply:
        return {'status': 'DRY_RUN', 'session_dir': str(directory), 'would_seal': would, 'changed': False,
                'apply_required': True}
    finals = {'events.jsonl': ''.join(f'{line}\n' for line in kept).encode('utf-8'),
              'raw_ws.jsonl': complete['raw_ws.jsonl'], 'raw_rest.jsonl': complete['raw_rest.jsonl'],
              'clock.jsonl': complete['clock.jsonl']}
    fields = _recovery_fields(start, symbol, previous, kept, first, last, cut, ws, rest, complete, interpreted, parts)
    candidate = {**fields, 'schema': SCHEMA, 'session_id': directory.name, 'sealed_utc': utc_iso(),
                 'software_version': __version__, 'files': {name: facts_of(data) for name, data in finals.items()},
                 'capture_start_sha256': sha256_bytes(start_bytes)}
    problem = manifest_problem(candidate, directory) or start_problem(start, candidate)
    if problem:
        raise ValueError(f'the recovered session would not be valid ({problem}); nothing written')
    if 'events.jsonl' not in parts and (directory / 'events.jsonl').read_bytes() != finals['events.jsonl']:
        raise ValueError('events.jsonl was already sealed with other content; refused')
    _verify_on_copy(directory, start_bytes, finals, parts, fields)
    for name, data in finals.items():
        if name in parts:   # files already final (an interrupted seal) stay as they are
            _write_new(directory / name, data)
    manifest = finish_manifest(directory, fields)
    return {**manifest, 'session_dir': str(directory)}


def _write_new(path, data):
    with open(path, 'xb') as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _verify_on_copy(directory, start_bytes, finals, parts, fields):
    """Build the recovered session in a private sibling directory and run verify_session on it; nothing is
    written into the real session unless that copy is VERIFIED. The copy is removed afterwards."""
    check_root = Path(tempfile.mkdtemp(prefix=f'.{directory.name}.recovery-check-', dir=directory.parent))
    try:   # mkdtemp created a fresh, uniquely named directory owned by this call: only it is removed
        copy = check_root / directory.name
        copy.mkdir()
        _write_new(copy / START, start_bytes)
        for name, data in finals.items():
            _write_new(copy / name, data)
        for name, data in parts.items():
            _write_new(copy / f'{name}.part', data)
        finish_manifest(copy, fields)
        report = verify_session(copy)
    finally:
        shutil.rmtree(check_root, ignore_errors=True)
    if report['status'] != 'VERIFIED':
        fatal = sorted(code for code in report['issue_counts'] if code not in NOTICE_CODES)
        raise ValueError(f'the recovered session would not verify ({fatal}); nothing written')


def _recovery_fields(start, symbol, previous, kept, first, last, cut, ws, rest, complete, interpreted, parts):
    counts = dict.fromkeys(COUNT_KEYS)
    counts.update(ws_messages=len(ws), rest_pages=sum(r['kind'] == 'aggTrades' for r in rest),
                  clock_samples=complete['clock.jsonl'].count(b'\n'), connections=max((r['conn'] for r in ws), default=0))
    return {'symbol': symbol, 'lane': lane(symbol), 'started_utc': start['started_utc'], 'sealed_by': 'recovery',
            'first_seq': first, 'last_seq': last, 'event_count': len(kept), 'previous': previous,
            'chain': 'CONTIGUOUS' if previous else 'FIRST', 'stop_reason': 'RECOVERED',
            'stop_detail': 'sealed from the complete lines of an interrupted capture'
                           + (f'; cut before conflicting seq {cut}' if cut is not None else ''),
            'counts': counts, 'endpoints': start['endpoints'], 'public_network': start['public_network'],
            'limits': start['limits'], 'interpreted': interpreted,
            'recovered_from': {f'{name}.part': {'bytes': len(data), 'sha256': sha256_bytes(data)}
                               for name, data in parts.items()}}
