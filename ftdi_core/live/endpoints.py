"""The network rule (docs/LIVE_CAPTURE.md §1): loopback by default, the fixed public endpoints only on request."""
from __future__ import annotations

from dataclasses import dataclass
import re

PUBLIC_WS = ('wss', 'data-stream.binance.vision', 443)
PUBLIC_REST = ('https', 'data-api.binance.vision', 443)


@dataclass(frozen=True)
class Endpoint:
    scheme: str
    host: str
    port: int

    @property
    def base(self):
        """Loopback endpoints always name their port (also 80); the public ones are the fixed host names."""
        if self.host == '127.0.0.1':
            return f'{self.scheme}://{self.host}:{self.port}'
        default = {'ws': 80, 'http': 80, 'wss': 443, 'https': 443}[self.scheme]
        return f'{self.scheme}://{self.host}' + ('' if self.port == default else f':{self.port}')


@dataclass(frozen=True)
class Endpoints:
    ws: Endpoint
    rest: Endpoint
    public: bool


def _loopback(url, scheme, label):
    if not isinstance(url, str):
        raise ValueError(f'{label} must be a string')
    match = re.fullmatch(rf'{scheme}://127\.0\.0\.1:([1-9][0-9]{{0,4}})', url)
    if match is None or not 1 <= int(match.group(1)) <= 65535:
        raise ValueError(f'{label} must be exactly {scheme}://127.0.0.1:<port> (loopback only, no path, query, '
                         f'fragment or user; use public_network for the public venue)')
    return Endpoint(scheme, '127.0.0.1', int(match.group(1)))


def resolve(ws_url, rest_url, public_network):
    if type(public_network) is not bool:
        raise ValueError('public_network must be a boolean')
    if public_network:
        if ws_url is not None or rest_url is not None:
            raise ValueError('public_network uses the fixed public endpoints; do not pass URLs')
        return Endpoints(Endpoint(*PUBLIC_WS), Endpoint(*PUBLIC_REST), True)
    if ws_url is None or rest_url is None:
        raise ValueError('loopback mode needs both ws_url and rest_url (or public_network=True)')
    return Endpoints(_loopback(ws_url, 'ws', 'ws_url'), _loopback(rest_url, 'http', 'rest_url'), False)
