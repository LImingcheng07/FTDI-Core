"""Minimal RFC 6455 WebSocket client (text messages only) over the standard library.

Protocol violations raise ``WebSocketError`` (the capture stops with FORMAT_ERROR); a closed, dropped or silent
connection raises ``ConnectionClosed`` (the capture reconnects). Server pings are answered with pongs.
"""
from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
import struct
import threading

GUID = b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
MAX_HANDSHAKE = 16384


class WebSocketError(Exception):
    """The server violated the protocol or sent something a capture must not accept."""


class ConnectionClosed(Exception):
    """The connection ended (close frame, EOF, socket error or read timeout)."""


class WebSocket:
    def __init__(self, sock, buffered, max_message):
        self._sock = sock
        self._buffer = buffered
        self._max_message = max_message
        self._send_lock = threading.Lock()
        self._closing = False

    @classmethod
    def connect(cls, endpoint, path, *, timeout, max_message):
        try:
            raw = socket.create_connection((endpoint.host, endpoint.port), timeout=timeout)
        except OSError as exc:
            raise ConnectionClosed(f'connect failed: {exc}') from None
        sock = raw
        try:
            if endpoint.scheme == 'wss':
                sock = ssl.create_default_context().wrap_socket(raw, server_hostname=endpoint.host)
            key = base64.b64encode(os.urandom(16))
            host = endpoint.host if endpoint.port in (80, 443) else f'{endpoint.host}:{endpoint.port}'
            request = (f'GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                       f'Sec-WebSocket-Key: {key.decode()}\r\nSec-WebSocket-Version: 13\r\n'
                       'User-Agent: FTDI-Core-readonly\r\n\r\n').encode()
            sock.sendall(request)
            response = b''
            while b'\r\n\r\n' not in response:
                chunk = sock.recv(4096)
                if not chunk:
                    raise ConnectionClosed('connection closed during the handshake')
                response += chunk
                if len(response) > MAX_HANDSHAKE:
                    raise WebSocketError('handshake response too large')
            head, _, rest = response.partition(b'\r\n\r\n')
            lines = head.decode('latin-1').split('\r\n')
            if not lines[0].startswith('HTTP/1.1 101'):
                raise WebSocketError(f'handshake refused: {lines[0][:80]}')
            headers = {}
            for line in lines[1:]:
                name, sep, value = line.partition(':')
                if sep:
                    headers[name.strip().lower()] = value.strip()
            accept = base64.b64encode(hashlib.sha1(key + GUID).digest()).decode()
            if (headers.get('upgrade', '').lower() != 'websocket'
                    or 'upgrade' not in headers.get('connection', '').lower()
                    or headers.get('sec-websocket-accept') != accept):
                raise WebSocketError('handshake response does not prove a WebSocket upgrade')
            if headers.get('sec-websocket-extensions'):
                raise WebSocketError('server negotiated an extension that was not offered')
            return cls(sock, rest, max_message)
        except (ConnectionClosed, WebSocketError):
            sock.close()
            raise
        except socket.timeout:
            sock.close()
            raise ConnectionClosed('handshake timed out') from None
        except (OSError, ssl.SSLError) as exc:
            sock.close()
            raise ConnectionClosed(f'handshake failed: {exc}') from None

    # -- reading ---------------------------------------------------------------------------------
    def _read(self, n):
        while len(self._buffer) < n:
            try:
                chunk = self._sock.recv(max(65536, n - len(self._buffer)))
            except socket.timeout:
                raise ConnectionClosed('read timeout') from None
            except OSError as exc:
                raise ConnectionClosed(f'socket error: {exc}') from None
            if not chunk:
                raise ConnectionClosed('connection closed by the server (EOF)')
            self._buffer += chunk
        data, self._buffer = self._buffer[:n], self._buffer[n:]
        return data

    def _frame(self):
        b1, b2 = self._read(2)
        fin, rsv, opcode = b1 & 0x80, b1 & 0x70, b1 & 0x0F
        masked, length = b2 & 0x80, b2 & 0x7F
        if rsv:
            raise WebSocketError('reserved bits set (no extension was negotiated)')
        if masked:
            raise WebSocketError('masked frame from the server')
        if length == 126:
            length = struct.unpack('>H', self._read(2))[0]
        elif length == 127:
            length = struct.unpack('>Q', self._read(8))[0]
        if opcode >= 0x8 and (not fin or length > 125):
            raise WebSocketError('invalid control frame')
        if length > self._max_message:
            raise WebSocketError(f'frame of {length} bytes exceeds the message limit')
        return bool(fin), opcode, self._read(length)

    def recv(self):
        """The next complete text message (str)."""
        parts, size = [], 0
        while True:
            fin, opcode, payload = self._frame()
            if opcode == 0x9:
                self._send(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x8:
                code = struct.unpack('>H', payload[:2])[0] if len(payload) >= 2 else 1005
                self._reply_close(payload[:2])
                raise ConnectionClosed(f'close frame from the server (code {code})')
            if opcode == 0x2:
                raise WebSocketError('binary message (only text aggTrade messages are expected)')
            if opcode == 0x1:
                if parts:
                    raise WebSocketError('new text message inside a fragmented message')
            elif opcode == 0x0:
                if not parts:
                    raise WebSocketError('continuation frame without a message')
            else:
                raise WebSocketError(f'unknown opcode {opcode}')
            parts.append(payload)
            size += len(payload)
            if size > self._max_message:
                raise WebSocketError(f'message exceeds {self._max_message} bytes')
            if fin:
                try:
                    return b''.join(parts).decode('utf-8')
                except UnicodeDecodeError:
                    raise WebSocketError('text message is not UTF-8') from None

    # -- writing ---------------------------------------------------------------------------------
    def _send(self, opcode, payload):
        mask = os.urandom(4)
        n = len(payload)
        head = bytes([0x80 | opcode])
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack('>H', n)
        else:
            head += bytes([0x80 | 127]) + struct.pack('>Q', n)
        body = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        with self._send_lock:
            try:
                self._sock.sendall(head + mask + body)
            except OSError as exc:
                raise ConnectionClosed(f'send failed: {exc}') from None

    def _reply_close(self, code):
        if not self._closing:
            self._closing = True
            try:
                self._send(0x8, code)
            except ConnectionClosed:
                pass

    def close(self):
        """Send a close frame (best effort) and release the socket; unblocks a concurrent ``recv``."""
        self._reply_close(struct.pack('>H', 1000))
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()
