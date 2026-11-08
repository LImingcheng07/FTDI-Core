"""A scripted fake public venue on 127.0.0.1 for live-capture tests (no network beyond loopback).

    with FakeVenue(connections=[[('trades', 1, 50)], [('trades', 60, 80)]], rest_available=(1, 200)) as venue:
        capture_session(tmp, symbol='BTCUSDT', ws_url=venue.ws_url, rest_url=venue.rest_url, ...)

WebSocket (RFC 6455 server side, text frames, unmasked as servers must): connection k (1-based) plays
``connections[k-1]``; connections beyond the list hold idle. Script actions:

    ('trade', seq) | ('trades', first, last)      aggTrade frame(s) from ``trade(seq)``
    ('trade_with', seq, {field: value})           aggTrade frame with overridden / removed (value None) fields
    ('trade_now', seq)                            aggTrade frame whose venue time E is taken when it is sent
    ('text', str)                                 arbitrary text frame
    ('binary', bytes)                             binary frame
    ('fragmented', seq, parts)                    one aggTrade message split into ``parts`` frames
    ('ping', payload)                             server ping; pongs are recorded in ``venue.pongs``
    ('oversize', n)                               text frame of n bytes
    ('masked', seq)                               protocol violation: a masked server frame
    ('sleep', seconds)                            pause
    ('close',)                                    close frame (code 1000), then wait for the client's close
    ('drop',)                                     abrupt TCP close
    ('hold',)                                     stay idle until the venue stops (also the default at the end)

REST: ``GET /api/v3/aggTrades?symbol=&fromId=&limit=`` returns ``rest_trade(seq)`` for
``seq in [fromId, min(fromId + limit - 1, hi)]`` where ``rest_available = (lo, hi)``; an empty list if
``fromId > hi`` or ``fromId < lo``. ``rest_overrides = {seq: {field: value}}`` changes REST records only.
``rest_queue`` = list of ``(status, headers, body)`` served (in order) before normal aggTrades answers.
``GET /api/v3/time`` returns ``{"serverTime": now_ms + server_offset_ms}``. All requests are logged in
``venue.requests`` as ``(path, query_dict)``.
"""
from __future__ import annotations

import base64
import hashlib
import http.server
import json
import socket
import struct
import threading
import time
import urllib.parse

GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
BASE_MS = 1717200000000


def trade(seq, symbol='BTCUSDT'):
    """Deterministic venue aggTrade (WebSocket shape)."""
    return {'e': 'aggTrade', 'E': BASE_MS + seq * 10 + 3, 's': symbol, 'a': seq,
            'p': f'{67000 + seq % 97}.{seq % 100:02d}000000', 'q': f'0.{seq % 9 + 1:03d}00000',
            'f': 2 * seq, 'l': 2 * seq + 1, 'T': BASE_MS + seq * 10, 'm': seq % 2 == 0, 'M': True}


def rest_trade(seq):
    record = trade(seq)
    for key in ('e', 'E', 's'):
        del record[key]
    return record


def core_event(seq, symbol='BTCUSDT'):
    """The Core event the contract derives from ``trade(seq)`` (docs/LIVE_CAPTURE.md §2)."""
    t = trade(seq, symbol)
    return {'source': 'binance_spot_ws_aggtrade', 'symbol': symbol, 'venue': 'BINANCE_SPOT', 'channel': 'aggTrades',
            'source_seq': seq, 'event_time_ms': t['T'], 'event_time_native': t['T'], 'event_time_native_unit': 'ms',
            'price': t['p'], 'qty': t['q'], 'first_trade_id': t['f'], 'last_trade_id': t['l'],
            'is_buyer_maker': t['m'], 'is_best_match': t['M']}


def _compact(obj):
    return json.dumps(obj, separators=(',', ':'))


def _frame(opcode, payload, *, fin=True, mask=False):
    head = bytes([(0x80 if fin else 0) | opcode])
    n = len(payload)
    mask_bit = 0x80 if mask else 0
    if n < 126:
        head += bytes([mask_bit | n])
    elif n < 65536:
        head += bytes([mask_bit | 126]) + struct.pack('>H', n)
    else:
        head += bytes([mask_bit | 127]) + struct.pack('>Q', n)
    if mask:
        key = b'\x01\x02\x03\x04'
        payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
        head += key
    return head + payload


def _recv_exact(sock, n):
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError('eof')
        data += chunk
    return data


class _Connection:
    def __init__(self, venue, sock, number):
        self.venue, self.sock, self.number = venue, sock, number
        self.closed_by_client = threading.Event()
        self.lock = threading.Lock()

    def send(self, data):
        with self.lock:
            self.sock.sendall(data)

    def handshake(self):
        request = b''
        while b'\r\n\r\n' not in request:
            chunk = self.sock.recv(4096)
            if not chunk or len(request) > 16384:
                raise ConnectionError('bad handshake')
            request += chunk
        lines = request.split(b'\r\n\r\n')[0].decode('latin-1').split('\r\n')
        method, path, _ = lines[0].split(' ', 2)
        headers = {k.strip().lower(): v.strip() for k, v in (line.split(':', 1) for line in lines[1:] if ':' in line)}
        self.venue.handshakes.append({'path': path, 'headers': headers})
        expected_path = f'/ws/{self.venue.symbol.lower()}@aggTrade'
        if (method != 'GET' or path != expected_path or headers.get('upgrade', '').lower() != 'websocket'
                or 'upgrade' not in headers.get('connection', '').lower()
                or headers.get('sec-websocket-version') != '13' or 'sec-websocket-key' not in headers):
            self.sock.sendall(b'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n')
            raise ConnectionError('rejected handshake')
        key = headers['sec-websocket-key']
        accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
        if self.venue.bad_accept:
            accept = base64.b64encode(b'wrong' * 4).decode()
        self.sock.sendall(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                           f'Sec-WebSocket-Accept: {accept}\r\n\r\n').encode())

    def read_client(self):
        """Parse client frames: they must be masked; record pongs and close."""
        try:
            while not self.venue.stopping.is_set():
                b1, b2 = _recv_exact(self.sock, 2)
                opcode, masked, n = b1 & 0x0F, b2 & 0x80, b2 & 0x7F
                if n == 126:
                    n = struct.unpack('>H', _recv_exact(self.sock, 2))[0]
                elif n == 127:
                    n = struct.unpack('>Q', _recv_exact(self.sock, 8))[0]
                if not masked:
                    self.venue.protocol_errors.append('unmasked client frame')
                    return
                key = _recv_exact(self.sock, 4)
                payload = bytes(b ^ key[i % 4] for i, b in enumerate(_recv_exact(self.sock, n)))
                if opcode == 0xA:
                    self.venue.pongs.append((self.number, payload))
                elif opcode == 0x8:
                    self.venue.client_closes.append(self.number)
                    self.closed_by_client.set()
                    return
        except (OSError, ConnectionError, ValueError):
            self.closed_by_client.set()

    def play(self, script):
        for action in list(script) + [('hold',)]:
            if self.venue.stopping.is_set():
                return
            kind = action[0]
            if kind == 'trade':
                self.send(_frame(0x1, _compact(trade(action[1], self.venue.symbol)).encode()))
            elif kind == 'trades':
                for seq in range(action[1], action[2] + 1):
                    self.send(_frame(0x1, _compact(trade(seq, self.venue.symbol)).encode()))
            elif kind == 'trade_now':
                record = trade(action[1], self.venue.symbol)
                record['E'] = int(time.time() * 1000) + self.venue.server_offset_ms
                self.send(_frame(0x1, _compact(record).encode()))
            elif kind == 'trade_with':
                record = trade(action[1], self.venue.symbol)
                for key, value in action[2].items():
                    if value is None:
                        record.pop(key, None)
                    else:
                        record[key] = value
                self.send(_frame(0x1, _compact(record).encode()))
            elif kind == 'text':
                self.send(_frame(0x1, action[1].encode()))
            elif kind == 'binary':
                self.send(_frame(0x2, action[1]))
            elif kind == 'fragmented':
                data = _compact(trade(action[1], self.venue.symbol)).encode()
                parts = action[2]
                size = -(-len(data) // parts)
                chunks = [data[i:i + size] for i in range(0, len(data), size)]
                for index, chunk in enumerate(chunks):
                    opcode = 0x1 if index == 0 else 0x0
                    self.send(_frame(opcode, chunk, fin=index == len(chunks) - 1))
            elif kind == 'ping':
                self.send(_frame(0x9, action[1]))
            elif kind == 'oversize':
                self.send(_frame(0x1, b'x' * action[1]))
            elif kind == 'masked':
                self.send(_frame(0x1, _compact(trade(action[1], self.venue.symbol)).encode(), mask=True))
            elif kind == 'sleep':
                self.venue.stopping.wait(action[1])
            elif kind == 'close':
                self.send(_frame(0x8, struct.pack('>H', 1000)))
                self.closed_by_client.wait(5)
                return
            elif kind == 'drop':
                return
            elif kind == 'hold':
                while not self.venue.stopping.is_set() and not self.closed_by_client.is_set():
                    self.venue.stopping.wait(0.05)
                return
            else:
                raise ValueError(f'unknown action {action!r}')

    def run(self, script):
        reader = None
        try:
            self.handshake()
            reader = threading.Thread(target=self.read_client, daemon=True)
            reader.start()
            self.play(script)
        except (OSError, ConnectionError):
            pass
        finally:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
            if reader is not None:
                reader.join(5)


class FakeVenue:
    def __init__(self, *, symbol='BTCUSDT', connections=(), rest_available=(0, -1), rest_overrides=None,
                 rest_queue=(), server_offset_ms=0, bad_accept=False):
        self.symbol = symbol
        self.connections = [list(script) for script in connections]
        self.rest_available = rest_available
        self.rest_overrides = dict(rest_overrides or {})
        self.rest_queue = list(rest_queue)
        self.server_offset_ms = server_offset_ms
        self.bad_accept = bad_accept
        self.stopping = threading.Event()
        self.handshakes, self.pongs, self.client_closes, self.protocol_errors = [], [], [], []
        self.requests = []
        self._threads = []
        self._lock = threading.Lock()
        self._accepted = 0

    # -- WebSocket ----------------------------------------------------------------------------
    def _accept_loop(self):
        while not self.stopping.is_set():
            try:
                sock, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                self._accepted += 1
                number = self._accepted
            script = self.connections[number - 1] if number <= len(self.connections) else []
            connection = _Connection(self, sock, number)
            thread = threading.Thread(target=connection.run, args=(script,), daemon=True)
            thread.start()
            self._threads.append(thread)

    @property
    def ws_connections(self):
        return self._accepted

    # -- REST -----------------------------------------------------------------------------------
    def _handler(self):
        venue = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, headers=None):
                data = body if isinstance(body, bytes) else body.encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                parsed = urllib.parse.urlsplit(self.path)
                query = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
                venue.requests.append((parsed.path, query))
                if parsed.path == '/api/v3/time':
                    return self._send(200, _compact({'serverTime': int(time.time() * 1000) + venue.server_offset_ms}))
                if parsed.path != '/api/v3/aggTrades':
                    return self._send(404, '{"code":-1}')
                if venue.rest_queue:
                    status, headers, body = venue.rest_queue.pop(0)
                    return self._send(status, body, headers)
                try:
                    if query.get('symbol') != venue.symbol:
                        raise ValueError
                    start, limit = int(query['fromId']), int(query.get('limit', '500'))
                    if not 1 <= limit <= 1000:
                        raise ValueError
                except (KeyError, ValueError):
                    return self._send(400, '{"code":-1100}')
                lo, hi = venue.rest_available
                records = []
                if lo <= start <= hi:
                    for seq in range(start, min(start + limit - 1, hi) + 1):
                        record = rest_trade(seq)
                        record.update(venue.rest_overrides.get(seq, {}))
                        records.append(record)
                return self._send(200, _compact(records))

        return Handler

    # -- lifecycle -----------------------------------------------------------------------------
    def __enter__(self):
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.bind(('127.0.0.1', 0))
        self._listener.listen(8)
        self.ws_url = f'ws://127.0.0.1:{self._listener.getsockname()[1]}'
        self._http = http.server.ThreadingHTTPServer(('127.0.0.1', 0), self._handler())
        self._http.daemon_threads = True
        self.rest_url = f'http://127.0.0.1:{self._http.server_address[1]}'
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        self._http_thread = threading.Thread(target=self._http.serve_forever, kwargs={'poll_interval': 0.05},
                                             daemon=True)
        self._http_thread.start()
        return self

    def __exit__(self, *exc):
        self.stopping.set()
        self._listener.close()
        self._http.shutdown()
        self._http.server_close()
        self._accept_thread.join(5)
        self._http_thread.join(5)
        for thread in self._threads:
            thread.join(5)
        return False
