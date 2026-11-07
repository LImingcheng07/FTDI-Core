"""Core localhost-only consumer service: the S07-004.1 HTTP contract served by TieredEffectStore.

Identical request/response behaviour to ``ftdi_core.adapters.s070041.consumer_service`` on a v1
database; on a compacted (v2) database replays are answered from the digest tier, and a tier that
cannot prove a replay yields HTTP 503 (nothing committed, nothing ACKed).  Binds 127.0.0.1 only.
"""
from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path

from .adapters.s070041.consumer_service import MAX_BODY, _publish_ready_port
from .adapters.s070041.external_delivery import Conflict
from .consumer_retention import TieredEffectStore, TierIntegrityError


def serve(db_path, port=8765, ready_file=None, crash_after_commit=False):
    with TieredEffectStore(db_path) as store:
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, code, body):
                data = json.dumps(body, sort_keys=True).encode('utf-8')
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.path != '/health':
                    self.reply(404, {'error': 'not found'})
                    return
                self.reply(200, {'status': 'ok', 'effects': store.count()})

            def do_POST(self):
                if self.path != '/batch':
                    self.reply(404, {'error': 'not found'})
                    return
                try:
                    length = int(self.headers.get('Content-Length', '-1'))
                    if length <= 0 or length > MAX_BODY:
                        self.reply(413, {'error': 'invalid request size'})
                        return
                    body = json.loads(self.rfile.read(length))
                    answer = store.accept(body.get('events') if isinstance(body, dict) else None)
                    if crash_after_commit:
                        os._exit(86)  # effects committed, HTTP ACK deliberately lost
                    self.reply(200, answer)
                except Conflict as exc:
                    self.reply(409, {'error': str(exc)})
                except TierIntegrityError as exc:
                    self.reply(503, {'error': str(exc)})
                except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                    self.reply(400, {'error': str(exc)})

        server = HTTPServer(('127.0.0.1', port), Handler)
        if ready_file is not None:
            _publish_ready_port(ready_file, server.server_port)
        print('Consumer on 127.0.0.1:' + str(server.server_port), flush=True)
        try:
            server.serve_forever(poll_interval=.1)
        finally:
            server.server_close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Local-only HTTP effect consumer, tier-aware (non-trading)')
    parser.add_argument('--db', type=Path, required=True)
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--ready-file', type=Path)
    parser.add_argument('--crash-after-commit', action='store_true')
    args = parser.parse_args(argv)
    serve(args.db, args.port, args.ready_file, args.crash_after_commit)


if __name__ == '__main__':
    main()
