"""Connection setup bounded by a deadline, including name resolution.

``socket.create_connection`` resolves the host with ``getaddrinfo``, which has no timeout: a stalled resolver could
hold a capture past its deadline (review of v0.4.1, F4). Here the resolution runs in a daemon worker thread that
the caller waits for at most until the deadline (an abandoned lookup finishes or fails on its own and touches no
capture state); each address is then connected with the remaining time, and the deadline is checked again after
connecting.
"""
from __future__ import annotations

import socket
import threading
import time


class SetupTimeout(TimeoutError):
    """Name resolution or connection did not complete before the deadline."""


def remaining(deadline):
    left = deadline - time.perf_counter()
    if left <= 0:
        raise SetupTimeout('connection setup did not complete before the deadline')
    return left


def resolve(host, port, deadline):
    """``getaddrinfo`` for a TCP connection, abandoned (SetupTimeout) when the deadline passes first."""
    outcome = {}

    def work():
        try:
            outcome['addresses'] = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
        except OSError as exc:
            outcome['error'] = exc
    worker = threading.Thread(target=work, name='ftdi-resolve', daemon=True)
    worker.start()
    worker.join(remaining(deadline))
    if worker.is_alive():
        raise SetupTimeout(f'name resolution of {host!r} did not complete before the deadline')
    if 'error' in outcome:
        raise outcome['error']
    return outcome['addresses']


def connect(host, port, deadline):
    """A connected TCP socket (timeout set to the remaining time), or OSError / SetupTimeout."""
    last = None
    for family, kind, proto, _, address in resolve(host, port, deadline):
        sock = socket.socket(family, kind, proto)
        try:
            sock.settimeout(remaining(deadline))
            sock.connect(address)
            sock.settimeout(remaining(deadline))
            return sock
        except BaseException as exc:
            sock.close()
            if isinstance(exc, SetupTimeout) or not isinstance(exc, OSError):
                raise
            last = exc
    raise last if last is not None else OSError(f'no address for {host!r}')
