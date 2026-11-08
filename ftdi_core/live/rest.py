"""Read-only REST client for the two public market-data calls; every response is recorded before it is used."""
from __future__ import annotations

import base64
import http.client
import socket
import ssl
import threading
import time
import urllib.parse

from . import netconnect
from .clocks import wall_time_ns


class RateLimited(Exception):
    def __init__(self, status, retry_after):
        super().__init__(f'HTTP {status}, Retry-After={retry_after}')
        self.status, self.retry_after = status, retry_after


class UnusableBody(Exception):
    """A 200 response whose body is too large or not UTF-8 (kept as base64 evidence)."""


class _Keep:
    """Remember the connected socket: http.client detaches it from the connection for 'Connection: close'."""

    def connect(self):
        super().connect()
        self.kept_socket = self.sock


class _HTTP(_Keep, http.client.HTTPConnection):
    pass


class _HTTPS(_Keep, http.client.HTTPSConnection):
    pass


def _abort(conn):
    for sock in (getattr(conn, 'kept_socket', None), getattr(conn, 'sock', None)):
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


class RestClient:
    def __init__(self, endpoint, *, symbol, recorder, min_interval_s, max_body_bytes, timeout_s=8.0):
        self.endpoint, self.symbol, self.recorder = endpoint, symbol, recorder
        self.min_interval_s, self.max_body_bytes, self.timeout_s = min_interval_s, max_body_bytes, timeout_s
        self._last = None

    def _wait_interval(self):
        if self._last is not None:
            delay = self.min_interval_s - (time.perf_counter() - self._last)
            if delay > 0:
                time.sleep(delay)

    def ready(self):
        return self._last is None or time.perf_counter() - self._last >= self.min_interval_s

    def get(self, kind, path, query, budget_s=None):
        """(status or None, body text or None, stamps). Records every attempt.

        The whole call (connect, headers, body) is bounded by ``budget_s`` (default ``timeout_s``): a timer
        shuts the socket down when it expires. Raises RateLimited on 429/418, UnusableBody on a 200 response
        whose body is too large or not UTF-8."""
        self._wait_interval()
        budget = self.timeout_s if budget_s is None else max(0.05, min(self.timeout_s, budget_s))
        target = path + '?' + urllib.parse.urlencode(query) if query else path
        url = self.endpoint.base + target
        self._last = time.perf_counter()
        status = body = body_base64 = error = retry_after = None
        t0_wall = t0_perf = None
        deadline = time.perf_counter() + budget
        if self.endpoint.scheme == 'https':
            conn = _HTTPS(self.endpoint.host, self.endpoint.port, timeout=budget, context=ssl.create_default_context())
        else:
            conn = _HTTP(self.endpoint.host, self.endpoint.port, timeout=budget)
        # name resolution and TCP connect bounded by the deadline (getaddrinfo itself has no timeout)
        conn._create_connection = lambda address, *_args, **_kw: netconnect.connect(address[0], address[1], deadline)
        timer = threading.Timer(budget, _abort, args=(conn,))
        timer.daemon = True
        timer.start()
        try:
            conn.connect()   # TCP (+TLS) first: the stamps below bracket only the request and its answer
            netconnect.remaining(deadline)   # set-up that overran the budget is a failed attempt, not a late answer
            t0_wall, t0_perf = wall_time_ns(), time.perf_counter_ns()
            conn.request('GET', target, headers={'Accept': 'application/json', 'Connection': 'close',
                                                 'User-Agent': 'FTDI-Core-readonly'})
            response = conn.getresponse()
            status = response.status
            retry_after = response.getheader('Retry-After')
            data = response.read(self.max_body_bytes + 1)
            if len(data) > self.max_body_bytes:
                body_base64 = base64.b64encode(data[:self.max_body_bytes]).decode('ascii')
                error = f'body exceeds {self.max_body_bytes} bytes (first {self.max_body_bytes} kept as base64)'
            else:
                try:
                    body = data.decode('utf-8')
                except UnicodeDecodeError:
                    body_base64 = base64.b64encode(data).decode('ascii')
                    error = 'body is not UTF-8 (kept as base64)'
            if status in (418, 429):
                error = f'HTTP {status} Retry-After={retry_after}'
        except (OSError, http.client.HTTPException) as exc:
            error = f'{type(exc).__name__}: {exc}'
            if not timer.is_alive():
                error = f'no complete answer within {budget:.2f} s ({error})'
        finally:
            timer.cancel()
            conn.close()
        t1_wall, t1_perf = wall_time_ns(), time.perf_counter_ns()
        if t0_wall is None:   # the connection itself failed: the attempt spans the whole call
            t0_wall, t0_perf = t1_wall, t1_perf
        self.recorder({'kind': kind, 'url': url, 'status': status, 't0_wall_ns': t0_wall, 't0_perf_ns': t0_perf,
                       't1_wall_ns': t1_wall, 't1_perf_ns': t1_perf, 'body': body, 'body_base64': body_base64,
                       'error': error})
        if status in (418, 429):
            raise RateLimited(status, retry_after)
        if status == 200 and body is None and body_base64 is not None:
            raise UnusableBody(error)
        return status, (body if error is None else None), {'t0_wall_ns': t0_wall, 't0_perf_ns': t0_perf,
                                                          't1_wall_ns': t1_wall, 't1_perf_ns': t1_perf}

    def agg_trades(self, from_id, limit, budget_s=None):
        return self.get('aggTrades', '/api/v3/aggTrades', {'symbol': self.symbol, 'fromId': from_id, 'limit': limit},
                        budget_s)

    def server_time(self, budget_s=None):
        return self.get('time', '/api/v3/time', None, budget_s)
