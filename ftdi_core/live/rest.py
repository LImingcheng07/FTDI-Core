"""Read-only REST client for the two public market-data calls; every response is recorded before it is used."""
from __future__ import annotations

import http.client
import ssl
import time
import urllib.parse


class RateLimited(Exception):
    def __init__(self, status, retry_after):
        super().__init__(f'HTTP {status}, Retry-After={retry_after}')
        self.status, self.retry_after = status, retry_after


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

    def get(self, kind, path, query):
        """Returns (status or None, body text or None). Records every attempt. Raises RateLimited on 429/418."""
        self._wait_interval()
        target = path + '?' + urllib.parse.urlencode(query) if query else path
        url = self.endpoint.base + target
        t0_wall, t0_perf = time.time_ns(), time.perf_counter_ns()
        self._last = time.perf_counter()
        status = body = error = retry_after = None
        if self.endpoint.scheme == 'https':
            conn = http.client.HTTPSConnection(self.endpoint.host, self.endpoint.port, timeout=self.timeout_s,
                                               context=ssl.create_default_context())
        else:
            conn = http.client.HTTPConnection(self.endpoint.host, self.endpoint.port, timeout=self.timeout_s)
        try:
            conn.request('GET', target, headers={'Accept': 'application/json', 'Connection': 'close',
                                                 'User-Agent': 'FTDI-Core-readonly'})
            response = conn.getresponse()
            status = response.status
            retry_after = response.getheader('Retry-After')
            data = response.read(self.max_body_bytes + 1)
            if len(data) > self.max_body_bytes:
                error = f'body exceeds {self.max_body_bytes} bytes'
            else:
                try:
                    body = data.decode('utf-8')
                except UnicodeDecodeError:
                    error = 'body is not UTF-8'
            if status in (418, 429):
                error = f'HTTP {status} Retry-After={retry_after}'
        except (OSError, http.client.HTTPException) as exc:
            error = f'{type(exc).__name__}: {exc}'
        finally:
            conn.close()
        t1_wall, t1_perf = time.time_ns(), time.perf_counter_ns()
        self.recorder({'kind': kind, 'url': url, 'status': status, 't0_wall_ns': t0_wall, 't0_perf_ns': t0_perf,
                       't1_wall_ns': t1_wall, 't1_perf_ns': t1_perf, 'body': body, 'error': error})
        if status in (418, 429):
            raise RateLimited(status, retry_after)
        return status, (body if error is None else None), {'t0_wall_ns': t0_wall, 't0_perf_ns': t0_perf,
                                                          't1_wall_ns': t1_wall, 't1_perf_ns': t1_perf}

    def agg_trades(self, from_id, limit):
        return self.get('aggTrades', '/api/v3/aggTrades', {'symbol': self.symbol, 'fromId': from_id, 'limit': limit})

    def server_time(self):
        return self.get('time', '/api/v3/time', None)
