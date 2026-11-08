"""Session directory, sealing, read-only verification and recovery (docs/LIVE_CAPTURE.md §4-§6)."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import time

from .. import __version__
from .events import FormatError, canonical, check_event, check_symbol, compare, core_event, lane, loads, \
    parse_rest_page, parse_ws

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
                           'body', 'error'})
CLOCK_KEYS = frozenset({'t0_wall_ns', 't0_perf_ns', 'venue_ms', 't1_wall_ns', 't1_perf_ns'})
NOTICE_CODES = frozenset({'LEFTOVER_PART_FILE', 'RAW_STOP_MESSAGE'})
MANIFEST_KEYS = frozenset({'schema', 'session_id', 'symbol', 'lane', 'software_version', 'started_utc', 'sealed_utc',
                           'sealed_by', 'first_seq', 'last_seq', 'event_count', 'previous', 'chain', 'stop_reason',
                           'files', 'counts', 'endpoints', 'public_network', 'limits', 'capture_start_sha256',
                           'stop_detail'})


def utc_compact():
    return time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())


def utc_iso():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def file_facts(path):
    data = Path(path).read_bytes()
    return {'bytes': len(data), 'sha256': sha256_bytes(data), 'lines': data.count(b'\n')}


def _json_bytes(obj):
    return (canonical(obj) + '\n').encode('utf-8')


def write_new_json(path, obj):
    """Write a JSON file that must not exist yet: temp file, fsync, atomic rename."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(f'{path.name} already exists')
    temp = path.with_name(path.name + '.tmp')
    with open(temp, 'xb') as handle:
        handle.write(_json_bytes(obj))
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

    def seal(self, fields):
        self.sync()
        self.close()
        for name in DATA_FILES:
            final = self.dir / name
            if final.exists():
                raise FileExistsError(f'{name} already exists')
            os.rename(self.dir / f'{name}.part', final)
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

def _lines(data):
    if data and not data.endswith(b'\n'):
        raise FormatError('file does not end with a newline')
    return [line.decode('utf-8') for line in data.split(b'\n')[:-1]] if data else []


def _is_int(value, *, nullable=False):
    return (value is None and nullable) or (type(value) is int and value >= 0)


def _raw_ws_record(line):
    obj = loads(line)
    if (not isinstance(obj, dict) or set(obj) != RAW_WS_KEYS or not isinstance(obj['text'], str)
            or not all(_is_int(obj[k]) for k in ('conn', 'n', 'read_wall_ns', 'read_perf_ns', 'queue_depth'))):
        raise FormatError('raw_ws line has an unexpected shape')
    return obj


def _raw_rest_record(line):
    obj = loads(line)
    if (not isinstance(obj, dict) or set(obj) != RAW_REST_KEYS or obj['kind'] not in ('aggTrades', 'time')
            or not isinstance(obj['url'], str) or not _is_int(obj['status'], nullable=True)
            or not all(_is_int(obj[k]) for k in ('n', 't0_wall_ns', 't0_perf_ns', 't1_wall_ns', 't1_perf_ns'))
            or not (obj['body'] is None or isinstance(obj['body'], str))
            or not (obj['error'] is None or isinstance(obj['error'], str))):
        raise FormatError('raw_rest line has an unexpected shape')
    return obj


def _clock_record(line):
    obj = loads(line)
    if not isinstance(obj, dict) or set(obj) != CLOCK_KEYS or not all(_is_int(obj[k]) for k in CLOCK_KEYS):
        raise FormatError('clock line has an unexpected shape')
    return obj


def manifest_problem(m, directory):
    if not isinstance(m, dict) or set(m) - {'recovered_from'} != MANIFEST_KEYS or m.get('schema') != SCHEMA:
        return 'fields or schema'
    try:
        check_symbol(m['symbol'])
    except ValueError:
        return 'symbol'
    if m['session_id'] != Path(directory).name or m['lane'] != lane(m['symbol']):
        return 'session id or lane'
    if m['stop_reason'] not in STOP_REASONS or m['sealed_by'] not in ('capture', 'recovery') or             not isinstance(m['stop_detail'], str):
        return 'stop reason or sealed_by'
    if (m['sealed_by'] == 'recovery') != (m['stop_reason'] == 'RECOVERED') or \
            (m['sealed_by'] == 'recovery') != ('recovered_from' in m):
        return 'recovery fields'
    first, last, count = m['first_seq'], m['last_seq'], m['event_count']
    if type(count) is not int or count < 0:
        return 'event_count'
    if count == 0:
        if first is not None or last is not None:
            return 'empty session with sequence numbers'
    elif not (_is_int(first) and _is_int(last) and first <= last):
        return 'first_seq / last_seq'
    if not isinstance(m['files'], dict) or set(m['files']) != set(DATA_FILES):
        return 'files'
    if not isinstance(m['counts'], dict) or set(m['counts']) != set(COUNT_KEYS):
        return 'counts'
    previous = m['previous']
    if m['chain'] not in ('FIRST', 'CONTIGUOUS'):
        return 'chain'
    if previous is not None and (not isinstance(previous, dict)
                                 or set(previous) != {'session_id', 'last_seq', 'manifest_sha256'}
                                 or not isinstance(previous['session_id'], str) or not _is_int(previous['last_seq'])
                                 or not isinstance(previous['manifest_sha256'], str)):
        return 'previous'
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


def verify_session(session_dir, *, previous=None):
    """Read-only verification of a sealed session. Never creates or changes a file."""
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

    for leftover in sorted(directory.glob('*.part')):
        issue('LEFTOVER_PART_FILE', leftover.name)
    manifest_path = directory / MANIFEST
    if not manifest_path.is_file():
        issue('UNSEALED', 'no SESSION_MANIFEST.json')
        return report()
    try:
        m = json.loads(manifest_path.read_bytes().decode('utf-8'))
    except (ValueError, UnicodeDecodeError) as exc:
        issue('MANIFEST_INVALID', f'unreadable: {exc}')
        return report()
    problem = manifest_problem(m, directory)
    if problem:
        issue('MANIFEST_INVALID', problem)
        return report()
    symbol, stop = m['symbol'], m['stop_reason']
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
        expected = (m['files'][name] if name != START else None)
        if expected is not None and expected != {'bytes': len(data), 'sha256': sha256_bytes(data),
                                                 'lines': data.count(b'\n')}:
            issue('FILE_MISMATCH', name)
        if name == START and sha256_bytes(data) != m['capture_start_sha256']:
            issue('FILE_MISMATCH', name)
        contents[name] = data

    # raw evidence index: seq -> list of events derived from raw records
    evidence = {}
    tolerated_tail = stop in ('FORMAT_ERROR', 'RECOVERED')
    if 'raw_ws.jsonl' in contents:
        try:
            lines = _lines(contents['raw_ws.jsonl'])
        except (FormatError, UnicodeDecodeError) as exc:
            issue('RAW_UNPARSEABLE', f'raw_ws: {exc}')
            lines = []
        for index, line in enumerate(lines):
            try:
                record = _raw_ws_record(line)
                if record['n'] != index:
                    raise FormatError('raw_ws n is not consecutive')
            except FormatError as exc:
                issue('RAW_UNPARSEABLE', f'raw_ws line {index}: {exc}')
                continue
            metrics['raw_ws_messages'] += 1
            try:
                venue = parse_ws(record['text'], symbol)
            except FormatError as exc:
                if tolerated_tail and index == len(lines) - 1:
                    issue('RAW_STOP_MESSAGE')
                else:
                    issue('RAW_UNPARSEABLE', f'raw_ws line {index}: {exc}')
                continue
            evidence.setdefault(venue['a'], []).append(core_event(venue, symbol))
    if 'raw_rest.jsonl' in contents:
        try:
            lines = _lines(contents['raw_rest.jsonl'])
        except (FormatError, UnicodeDecodeError) as exc:
            issue('RAW_UNPARSEABLE', f'raw_rest: {exc}')
            lines = []
        agg_indexes = []
        records = []
        for index, line in enumerate(lines):
            try:
                record = _raw_rest_record(line)
                if record['n'] != index:
                    raise FormatError('raw_rest n is not consecutive')
            except FormatError as exc:
                issue('RAW_UNPARSEABLE', f'raw_rest line {index}: {exc}')
                continue
            records.append((index, record))
            if record['kind'] == 'aggTrades':
                agg_indexes.append(index)
        last_agg = agg_indexes[-1] if agg_indexes else None
        for index, record in records:
            if record['kind'] != 'aggTrades' or record['status'] != 200 or record['body'] is None:
                continue
            try:
                page = parse_rest_page(record['body'])
            except FormatError as exc:
                if tolerated_tail and index == last_agg:
                    issue('RAW_STOP_MESSAGE')
                else:
                    issue('RAW_UNPARSEABLE', f'raw_rest line {index}: {exc}')
                continue
            for venue in page:
                metrics['raw_rest_records'] += 1
                evidence.setdefault(venue['a'], []).append(core_event(venue, symbol))
    if 'clock.jsonl' in contents:
        try:
            for index, line in enumerate(_lines(contents['clock.jsonl'])):
                try:
                    _clock_record(line)
                    metrics['clock_samples'] += 1
                except FormatError as exc:
                    issue('RAW_UNPARSEABLE', f'clock line {index}: {exc}')
        except (FormatError, UnicodeDecodeError) as exc:
            issue('RAW_UNPARSEABLE', f'clock: {exc}')

    if 'events.jsonl' in contents:
        seqs = []
        try:
            lines = _lines(contents['events.jsonl'])
        except (FormatError, UnicodeDecodeError) as exc:
            issue('EVENT_INVALID', str(exc))
            lines = []
        for index, line in enumerate(lines):
            try:
                event = loads(line)
                if canonical(event) != line:
                    raise FormatError('line is not canonical JSON')
                check_event(event, symbol)
            except FormatError as exc:
                issue('EVENT_INVALID', f'line {index}: {exc}')
                continue
            seq = event['source_seq']
            seqs.append(seq)
            found = evidence.get(seq, [])
            if not any(compare(event, other) != 'conflict' for other in found):
                issue('EVENT_WITHOUT_RAW_EVIDENCE', seq)
            if any(compare(event, other) == 'conflict' for other in found):
                issue('RAW_CONFLICT', seq)
        metrics['events'] = len(seqs)
        first, last = m['first_seq'], m['last_seq']
        expected_seqs = list(range(first, last + 1)) if m['event_count'] else []
        if seqs != expected_seqs or len(lines) != m['event_count'] or len(expected_seqs) != m['event_count']:
            issue('EVENT_SEQUENCE_GAP', {'first_seq': first, 'last_seq': last, 'found': len(seqs)})

    if previous is not None:
        metrics['chain_checked'] = True
        try:
            data = (Path(previous) / MANIFEST).read_bytes()
            prev = json.loads(data.decode('utf-8'))
            prev_ok = verify_session(previous)['status'] == 'VERIFIED'
            link = {'session_id': prev.get('session_id'), 'last_seq': prev.get('last_seq'),
                    'manifest_sha256': sha256_bytes(data)}
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            issue('CHAIN_MISMATCH', f'previous session unreadable: {exc}')
        else:
            if not prev_ok or m['previous'] != link or prev.get('symbol') != symbol:
                issue('CHAIN_MISMATCH', 'previous session differs from the recorded link or is not VERIFIED')
    return report()


# -- recovery -------------------------------------------------------------------------------------

def _complete(data):
    end = data.rfind(b'\n') + 1
    return data[:end]


def seal_session(session_dir, *, apply=False):
    """Seal a killed capture from its complete lines; the ``.part`` files are kept."""
    directory = Path(session_dir)
    if not directory.is_dir():
        raise FileNotFoundError(f'session directory not found: {directory}')
    if (directory / MANIFEST).exists():
        raise ValueError('session is already sealed')
    try:
        start = json.loads((directory / START).read_bytes().decode('utf-8'))
        if start.get('schema') != START_SCHEMA:
            raise ValueError('wrong schema')
        symbol = check_symbol(start['symbol'])
    except (OSError, ValueError, KeyError, UnicodeDecodeError) as exc:
        raise ValueError(f'{START} missing or invalid ({exc}); cannot recover') from None
    parts, complete = {}, {}
    for name in DATA_FILES:
        path = directory / f'{name}.part'
        if not path.is_file():
            raise ValueError(f'{name}.part missing; cannot recover')
        if (directory / name).exists():
            raise ValueError(f'{name} already exists; refused')
        data = path.read_bytes()
        parts[name] = data
        complete[name] = _complete(data)
    parsers = {'raw_ws.jsonl': _raw_ws_record, 'raw_rest.jsonl': _raw_rest_record, 'clock.jsonl': _clock_record}
    parsed = {}
    for name, parser in parsers.items():
        parsed[name] = []
        for index, line in enumerate(_lines(complete[name])):
            try:
                parsed[name].append(parser(line))
            except FormatError as exc:
                raise ValueError(f'{name}.part line {index} is corrupt ({exc}); refusing to guess') from None
    evidence = {}
    for record in parsed['raw_ws.jsonl']:
        try:
            venue = parse_ws(record['text'], symbol)
        except FormatError:
            continue
        evidence.setdefault(venue['a'], []).append(core_event(venue, symbol))
    for record in parsed['raw_rest.jsonl']:
        if record['kind'] == 'aggTrades' and record['status'] == 200 and record['body'] is not None:
            try:
                for venue in parse_rest_page(record['body']):
                    evidence.setdefault(venue['a'], []).append(core_event(venue, symbol))
            except FormatError:
                continue
    previous = start.get('previous')
    expected = previous['last_seq'] + 1 if previous else None
    kept = []
    for line in _lines(complete['events.jsonl']):
        try:
            event = loads(line)
            if canonical(event) != line:
                break
            check_event(event, symbol)
        except FormatError:
            break
        seq = event['source_seq']
        if expected is not None and seq != expected:
            break
        found = evidence.get(seq, [])
        if not found or any(compare(event, other) == 'conflict' for other in found):
            break
        kept.append(line)
        expected = seq + 1
    first = loads(kept[0])['source_seq'] if kept else None
    last = loads(kept[-1])['source_seq'] if kept else None
    would = {'first_seq': first, 'last_seq': last, 'event_count': len(kept)}
    if not apply:
        return {'status': 'DRY_RUN', 'session_dir': str(directory), 'would_seal': would, 'changed': False,
                'apply_required': True}
    finals = {'events.jsonl': ''.join(f'{line}\n' for line in kept).encode('utf-8'),
              'raw_ws.jsonl': complete['raw_ws.jsonl'], 'raw_rest.jsonl': complete['raw_rest.jsonl'],
              'clock.jsonl': complete['clock.jsonl']}
    for name, data in finals.items():
        with open(directory / name, 'xb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    ws = parsed['raw_ws.jsonl']
    counts = dict.fromkeys(COUNT_KEYS)
    counts.update(ws_messages=len(ws), rest_pages=sum(r['kind'] == 'aggTrades' for r in parsed['raw_rest.jsonl']),
                  clock_samples=len(parsed['clock.jsonl']), connections=max((r['conn'] for r in ws), default=0))
    fields = {'symbol': symbol, 'lane': lane(symbol), 'started_utc': start.get('started_utc'),
              'sealed_by': 'recovery', 'first_seq': first, 'last_seq': last, 'event_count': len(kept),
              'previous': previous, 'chain': 'CONTIGUOUS' if previous else 'FIRST', 'stop_reason': 'RECOVERED',
              'stop_detail': 'sealed from the complete lines of an interrupted capture',
              'counts': counts, 'endpoints': start.get('endpoints'), 'public_network': start.get('public_network'),
              'limits': start.get('limits'),
              'recovered_from': {f'{name}.part': {'bytes': len(data), 'sha256': sha256_bytes(data)}
                                 for name, data in parts.items()}}
    manifest = finish_manifest(directory, fields)
    return {**manifest, 'session_dir': str(directory)}
