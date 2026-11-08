"""Live capture engine (docs/LIVE_CAPTURE.md §3): reader thread -> bounded queue -> validating writer -> sealed session."""
from __future__ import annotations

from dataclasses import asdict
import queue
import random
import threading
import time
from pathlib import Path

from .. import __version__
from .endpoints import resolve
from .clocks import INTERVAL_CLOCK, WALL_CLOCK, stamp_pair
from .limits import CaptureLimits
from .events import FormatError, check_symbol, compare, core_event, lane, loads, parse_rest_page, parse_ws
from .rest import RateLimited, RestClient, UnusableBody
from .session import (MAX_INT64, START, START_SCHEMA, SessionFiles, create_session_dir, evidence_index, first_conflict,
                      parse_raw, previous_link, raw_rest_record, raw_ws_record, utc_iso, write_new_json)
from .wsclient import ConnectionClosed, WebSocket, WebSocketError


def _read_stamps():
    """(interval stamp, wall stamp) of a read that just returned: the interval clock immediately, then the precise
    wall clock mapped to that instant (``clocks.stamp_pair``; ``time.time_ns`` is quantised on Windows)."""
    perf = time.perf_counter_ns()
    wall, _ = stamp_pair(perf)
    return perf, wall


class _Stop(Exception):
    def __init__(self, reason, detail=''):
        super().__init__(f'{reason}: {detail}')
        self.reason, self.detail = reason, detail


def _reader(ws, items, overflow, stop, conn, spill):
    """Read messages; stamp them immediately after the read; never drop one (a message that does not fit into
    the full queue is kept in ``spill`` and the reader stops)."""
    while not stop.is_set():
        try:
            text = ws.recv()
        except ConnectionClosed as exc:
            item = ('closed', conn, str(exc))
        except WebSocketError as exc:
            item = ('protocol', conn, str(exc))
        except Exception as exc:  # pragma: no cover - defensive: surface any reader failure
            item = ('protocol', conn, f'{type(exc).__name__}: {exc}')
        else:
            perf, wall = _read_stamps()
            depth = items.qsize()
            try:
                items.put_nowait(('msg', conn, text, wall, perf, depth))
            except queue.Full:
                spill.append(('msg', conn, text, wall, perf, depth))
                overflow.set()
                return
            continue
        try:
            items.put_nowait(item)
        except queue.Full:
            overflow.set()
        return


class _Capture:
    def __init__(self, directory, *, symbol, endpoints, limits, max_events, max_seconds, previous):
        self.dir, self.symbol, self.endpoints, self.limits = directory, symbol, endpoints, limits
        self.max_events, self.max_seconds = max_events, max_seconds
        self.previous_link, self.previous = previous
        self.files = SessionFiles(directory)
        self.rest = RestClient(endpoints.rest, symbol=symbol, recorder=self._record_rest,
                               min_interval_s=limits.rest_min_interval_s, max_body_bytes=limits.max_rest_body_bytes)
        self.counts = dict.fromkeys(('ws_messages', 'ws_duplicates', 'rest_pages', 'rest_records', 'rest_duplicates',
                                     'text_variants', 'before_anchor', 'connections', 'reconnects', 'clock_samples',
                                     'gaps_repaired'), 0)
        self.written = {}
        self.offsets = {}
        self.pending = {}
        self.first_seq = self.expected = None
        if self.previous_link:
            self.first_seq = self.expected = self.previous_link['last_seq'] + 1
        self.gap_since = None
        self.gap_pages = 0
        self.catch_up = None
        self.ws_n = self.rest_n = 0
        self.interpreted = {'raw_ws': 0, 'raw_rest': 0}   # prefix of raw lines interpreted before the stop
        self.unsynced = 0
        self.last_clock = None
        self.deadline = None

    # -- recording -------------------------------------------------------------------------------
    def _record_rest(self, record):
        self.files.append('raw_rest.jsonl', {'n': self.rest_n, **record})
        self.rest_n += 1

    def _interpreted(self, name, n):
        if self.interpreted[name] == n:
            self.interpreted[name] = n + 1

    def _budget(self):
        return max(0.05, self.deadline - time.perf_counter())

    def _clock_sample(self, budget_s=None):
        """A server-time sample; any failure except a rate limit is recorded and ignored."""
        self.last_clock = time.perf_counter()
        n = self.rest_n
        try:
            status, body, stamps = self.rest.server_time(self._budget() if budget_s is None else budget_s)
        except UnusableBody:
            return
        self._interpreted('raw_rest', n)
        if status != 200 or body is None:
            return
        try:
            obj = loads(body)
            venue_ms = obj.get('serverTime') if isinstance(obj, dict) else None
        except FormatError:
            return
        if type(venue_ms) is int and 0 <= venue_ms <= MAX_INT64 and stamps['t1_wall_ns'] >= stamps['t0_wall_ns']:
            self.files.append('clock.jsonl', {'t0_wall_ns': stamps['t0_wall_ns'], 't0_perf_ns': stamps['t0_perf_ns'],
                                              'venue_ms': venue_ms, 't1_wall_ns': stamps['t1_wall_ns'],
                                              't1_perf_ns': stamps['t1_perf_ns']})
            self.counts['clock_samples'] += 1

    # -- sequencing ------------------------------------------------------------------------------
    def _write(self, event):
        self.offsets[event['source_seq']] = self.files.handles['events.jsonl'].tell()
        self.files.append('events.jsonl', event)
        self.written[event['source_seq']] = event
        self.expected = event['source_seq'] + 1
        self.unsynced += 1
        if self.unsynced >= self.limits.fsync_every:
            self.files.sync()
            self.unsynced = 0
        if len(self.written) >= self.max_events:
            raise _Stop('MAX_EVENTS')

    def _same(self, kept, event, origin):
        verdict = compare(kept, event)
        if verdict == 'conflict':
            raise _Stop('HALTED_CONFLICT', f'seq {event["source_seq"]} delivered with different content ({origin})')
        if verdict == 'text_variant':
            self.counts['text_variants'] += 1
        self.counts['ws_duplicates' if origin == 'ws' else 'rest_duplicates'] += 1

    def _accept(self, record, origin):
        event = core_event(record, self.symbol)
        seq = record['a']
        if self.first_seq is None:
            self.first_seq = self.expected = seq
        if seq < self.first_seq:
            self.counts['before_anchor'] += 1
            return
        if seq < self.expected:
            return self._same(self.written[seq], event, origin)
        if seq > self.expected:
            if seq in self.pending:
                return self._same(self.pending[seq], event, origin)
            if len(self.pending) >= self.limits.max_pending:
                raise _Stop('PENDING_OVERFLOW', f'{len(self.pending)} buffered events')
            self.pending[seq] = event
            if self.gap_since is None:
                self.gap_since = time.perf_counter()
                self.gap_pages = 0
            return
        self._write(event)
        if self.gap_since is not None and self.expected in self.pending:
            self.counts['gaps_repaired'] += 1   # the hole below the buffered records is filled
        while self.expected in self.pending:
            self._write(self.pending.pop(self.expected))
        if self.gap_since is not None:
            # another hole may remain above: its timer and page budget start now
            self.gap_since = time.perf_counter() if self.pending else None
            self.gap_pages = 0

    def _rest_page(self, limit):
        """One REST request from ``expected``; processes the records. Returns the number of records."""
        start = self.expected
        n = self.rest_n
        try:
            status, body, _ = self.rest.agg_trades(start, limit, self._budget())
        except UnusableBody as exc:
            raise _Stop('FORMAT_ERROR', f'REST aggTrades body unusable: {exc}') from None
        finally:
            self.counts['rest_pages'] += 1
        if status != 200 or body is None:
            self._interpreted('raw_rest', n)
            return 0
        page = parse_rest_page(body)
        self._interpreted('raw_rest', n)
        for offset, record in enumerate(page):
            if record['a'] != start + offset:
                raise _Stop('FORMAT_ERROR', f'REST page from {start} is not consecutive at position {offset}')
        for record in page:
            self.counts['rest_records'] += 1
            self._accept(record, 'rest')
        return len(page)

    def _request_catch_up(self):
        """Ask REST from ``expected`` once ``gap_wait_ms`` from now, unless WS advances or opens a gap first."""
        if self.expected is not None:
            self.catch_up = (time.perf_counter(), self.expected)

    def _repair(self):
        if self.gap_since is None:
            if self.catch_up is not None:
                since, start = self.catch_up
                if self.expected != start:
                    self.catch_up = None   # WS delivered the continuation itself
                elif (time.perf_counter() - since) * 1000 >= self.limits.gap_wait_ms and self.rest.ready():
                    self.catch_up = None
                    self._rest_page(1000)
            return
        self.catch_up = None   # a known gap is repaired by the gap rule
        open_s = time.perf_counter() - self.gap_since
        if open_s >= self.limits.gap_timeout_s:
            raise _Stop('GAP_BLOCKED', f'gap at {self.expected} open for {open_s:.1f} s')
        if open_s * 1000 < self.limits.gap_wait_ms or not self.rest.ready():
            return
        if self.gap_pages >= self.limits.max_rest_pages_per_gap:
            raise _Stop('GAP_BLOCKED', f'gap at {self.expected} not repaired by {self.gap_pages} REST pages')
        self.gap_pages += 1
        self._rest_page(min(1000, min(self.pending) - self.expected))

    # -- main loop -------------------------------------------------------------------------------
    def _connect(self, attempt):
        path = f'/ws/{self.symbol.lower()}@aggTrade'
        return WebSocket.connect(self.endpoints.ws, path, timeout=self.limits.ws_read_timeout_s,
                                 max_message=self.limits.max_message_bytes, deadline=self.deadline)

    def _backoff(self, k, deadline):
        delay = min(8.0, 0.5 * 2 ** k) * random.uniform(0.9, 1.1)
        end = min(time.perf_counter() + delay, deadline)
        while time.perf_counter() < end:
            self._repair()
            time.sleep(min(0.05, max(0.0, end - time.perf_counter())))

    def _consume(self, ws, items, overflow, deadline):
        while True:
            now = time.perf_counter()
            if now >= deadline:
                raise _Stop('MAX_SECONDS')
            if overflow.is_set():
                raise _Stop('READER_OVERFLOW', f'queue of {self.limits.queue_size} messages full')
            if self.last_clock is not None and now - self.last_clock >= self.limits.clock_interval_s:
                self._clock_sample()
            self._repair()
            try:
                item = items.get(timeout=0.05)
            except queue.Empty:
                continue
            kind = item[0]
            if kind == 'closed':
                return item[2]
            if kind == 'protocol':
                raise _Stop('FORMAT_ERROR', item[2])
            _, conn, text, wall, perf, depth = item
            self.files.append('raw_ws.jsonl', {'conn': conn, 'n': self.ws_n, 'read_wall_ns': wall,
                                               'read_perf_ns': perf, 'queue_depth': depth, 'text': text})
            self.ws_n += 1
            self.counts['ws_messages'] += 1
            try:
                record = parse_ws(text, self.symbol)
            except FormatError as exc:
                raise _Stop('FORMAT_ERROR', str(exc)) from None
            self._interpreted('raw_ws', self.ws_n - 1)
            self._accept(record, 'ws')

    def _drain(self, items, spill):
        """Record every WebSocket message already received but not interpreted (evidence after the stop)."""
        pending = []
        while items is not None:
            try:
                pending.append(items.get_nowait())
            except queue.Empty:
                break
        pending.extend(spill)
        spill.clear()
        for item in pending:
            if item[0] != 'msg':
                continue
            _, conn, text, wall, perf, depth = item
            self.files.append('raw_ws.jsonl', {'conn': conn, 'n': self.ws_n, 'read_wall_ns': wall,
                                               'read_perf_ns': perf, 'queue_depth': depth, 'text': text})
            self.ws_n += 1
            self.counts['ws_messages'] += 1

    def _conflict_cut(self):
        """Cut our unsealed events back before the smallest written seq that any raw record contradicts
        (the same lenient evidence rule as verify_session). Returns that seq or None."""
        ws = parse_raw('raw_ws.jsonl', self.files.read('raw_ws.jsonl'), raw_ws_record)
        rest = parse_raw('raw_rest.jsonl', self.files.read('raw_rest.jsonl'), raw_rest_record)
        evidence, _ = evidence_index(ws, rest, self.symbol, self.interpreted, lambda code, detail=None: None)
        cut = first_conflict(self.written, evidence)
        if cut is None:
            return None
        handle = self.files.handles['events.jsonl']
        handle.flush()
        handle.truncate(self.offsets[cut])
        handle.seek(0, 2)
        for seq in [s for s in self.written if s >= cut]:
            del self.written[seq]
        self.files.lines['events.jsonl'] = len(self.written)
        return cut

    def run(self):
        try:
            return self._run()
        except BaseException:
            self.files.close()   # unsealed: recoverable with seal_session; never leave handles open (Windows)
            raise

    def _run(self):
        started = time.perf_counter()
        deadline = self.deadline = started + self.max_seconds
        stop_reason, detail = None, ''
        ws = reader = None
        stop_reader = threading.Event()
        items, spill = None, []
        try:
            self._clock_sample()
            attempt = 0
            while True:
                if time.perf_counter() >= deadline:
                    raise _Stop('MAX_SECONDS')
                try:
                    ws = self._connect(attempt)
                except WebSocketError as exc:
                    raise _Stop('FORMAT_ERROR', f'handshake: {exc}') from None
                except ConnectionClosed as exc:
                    if attempt >= self.limits.max_reconnects:
                        raise _Stop('CONNECTION_FAILED', str(exc)) from None
                    self._backoff(attempt, deadline)
                    attempt += 1
                    self.counts['reconnects'] += 1
                    continue
                self.counts['connections'] += 1
                if self.counts['connections'] > 1 or self.previous_link:
                    self._request_catch_up()   # after the WS is connected: REST then covers up to its start
                items, overflow = queue.Queue(maxsize=self.limits.queue_size), threading.Event()
                stop_reader = threading.Event()
                reader = threading.Thread(target=_reader, args=(ws, items, overflow, stop_reader,
                                                                self.counts['connections'], spill), daemon=True)
                reader.start()
                reason = self._consume(ws, items, overflow, deadline)
                stop_reader.set()
                ws.close()
                reader.join(10)
                ws = reader = None
                if attempt >= self.limits.max_reconnects:
                    raise _Stop('CONNECTION_FAILED', reason)
                self._backoff(attempt, deadline)
                attempt += 1
                self.counts['reconnects'] += 1
        except _Stop as stop:
            stop_reason, detail = stop.reason, stop.detail
        except RateLimited as exc:
            stop_reason, detail = 'RATE_LIMITED', str(exc)
        except FormatError as exc:
            stop_reason, detail = 'FORMAT_ERROR', str(exc)
        except KeyboardInterrupt:
            stop_reason, detail = 'INTERRUPTED', 'KeyboardInterrupt'
        finally:
            stop_reader.set()
            if ws is not None:
                ws.close()
            if reader is not None:
                reader.join(10)
        self._drain(items, spill)
        if stop_reason != 'RATE_LIMITED':
            try:
                self._clock_sample(budget_s=2.0)
            except RateLimited as exc:
                if stop_reason in ('MAX_EVENTS', 'MAX_SECONDS', 'INTERRUPTED'):
                    detail = f'final clock sample after {stop_reason}: {exc}'
                    stop_reason = 'RATE_LIMITED'
                else:
                    detail = f'{detail}; final clock sample rate-limited: {exc}'
        cut = self._conflict_cut()
        if cut is not None:
            stop_reason, detail = 'HALTED_CONFLICT', (f'raw evidence contradicts written seq {cut}; events cut '
                                                      f'back before it (stop was {stop_reason}: {detail})')
        events = sorted(self.written)
        manifest = self.files.seal({
            'symbol': self.symbol, 'lane': lane(self.symbol), 'started_utc': self.started_utc, 'sealed_by': 'capture',
            'first_seq': events[0] if events else None, 'last_seq': events[-1] if events else None,
            'event_count': len(events), 'previous': self.previous_link,
            'chain': 'CONTIGUOUS' if self.previous_link else 'FIRST', 'stop_reason': stop_reason,
            'stop_detail': detail,
            'counts': self.counts, 'endpoints': {'ws': self.endpoints.ws.base, 'rest': self.endpoints.rest.base},
            'public_network': self.endpoints.public, 'limits': asdict(self.limits),
            'interpreted': dict(self.interpreted)})
        return {**manifest, 'session_dir': str(self.dir)}


def capture_session(capture_dir, *, symbol, ws_url=None, rest_url=None, public_network=False, max_events=10000,
                    max_seconds=600.0, previous_session=None, limits=None):
    """Capture public aggTrades into a new sealed session directory (docs/LIVE_CAPTURE.md)."""
    check_symbol(symbol)
    limits = CaptureLimits() if limits is None else limits
    if not isinstance(limits, CaptureLimits):
        raise ValueError('limits must be a CaptureLimits')
    if type(max_events) is not int or max_events < 1:
        raise ValueError('max_events must be a positive integer')
    if type(max_seconds) not in (int, float) or not max_seconds > 0:
        raise ValueError('max_seconds must be a positive number')
    endpoints = resolve(ws_url, rest_url, public_network)
    if endpoints.public and limits.rest_min_interval_s < 0.1:
        raise ValueError('rest_min_interval_s must be at least 0.1 against the public venue')
    link = previous = None
    if previous_session is not None:
        link, previous = previous_link(previous_session)
        if previous['symbol'] != symbol:
            raise ValueError('previous session has another symbol')
    directory = create_session_dir(capture_dir, symbol)
    started_utc = utc_iso()
    write_new_json(Path(directory) / START, {
        'schema': START_SCHEMA, 'symbol': symbol, 'lane': lane(symbol), 'started_utc': started_utc,
        'software_version': __version__,
        'endpoints': {'ws': endpoints.ws.base, 'rest': endpoints.rest.base}, 'public_network': endpoints.public,
        'limits': asdict(limits), 'max_events': max_events, 'max_seconds': max_seconds, 'previous': link,
        'clocks': {'wall': WALL_CLOCK, 'interval': INTERVAL_CLOCK}})
    capture = _Capture(directory, symbol=symbol, endpoints=endpoints, limits=limits, max_events=max_events,
                       max_seconds=max_seconds, previous=(link, previous))
    capture.started_utc = started_utc
    return capture.run()
