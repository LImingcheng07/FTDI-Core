#!/usr/bin/env python3
"""Localhost-only HTTP consumer service; its own durable SQLite effect DB."""
from __future__ import annotations
import argparse
import json
import os
import tempfile
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from .external_delivery import EffectStore, Conflict

MAX_BODY = 2*1024*1024


def _publish_ready_port(path, port):
    """Publish a complete port number in one atomic rename (Windows-safe).

    Never expose an existing empty/partially written readiness file. The temp
    file is closed before replacement, which also avoids WinError 32.
    """
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', encoding='ascii', dir=dest.parent,
            prefix=dest.name + '.', suffix='.tmp', delete=False
        ) as handle:
            temp_name = Path(handle.name)
            handle.write(str(port))
            handle.flush()
        os.replace(temp_name, dest)
    finally:
        if temp_name is not None:
            temp_name.unlink(missing_ok=True)


def serve(db_path,port=8765,ready_file=None,crash_after_commit=False):
    with EffectStore(db_path) as store:
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*a): pass
            def reply(self,code,body):
                data=json.dumps(body,sort_keys=True).encode('utf-8')
                self.send_response(code)
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            def do_GET(self):
                if self.path!='/health':
                    self.reply(404,{'error':'not found'});return
                self.reply(200,{'status':'ok','effects':store.count()})
            def do_POST(self):
                if self.path!='/batch':
                    self.reply(404,{'error':'not found'});return
                try:
                    length=int(self.headers.get('Content-Length','-1'))
                    if length<=0 or length>MAX_BODY:
                        self.reply(413,{'error':'invalid request size'});return
                    d=json.loads(self.rfile.read(length))
                    answer=store.accept(d.get('events') if isinstance(d,dict) else None)
                    if crash_after_commit:
                        os._exit(86) # External DB committed, HTTP ack deliberately lost.
                    self.reply(200,answer)
                except Conflict as e:
                    self.reply(409,{'error':str(e)})
                except (ValueError,KeyError,TypeError,json.JSONDecodeError) as e:
                    self.reply(400,{'error':str(e)})
        server=HTTPServer(('127.0.0.1',port),Handler)
        if ready_file is not None:
            _publish_ready_port(ready_file,server.server_port)
        print('Consumer on 127.0.0.1:'+str(server.server_port),flush=True)
        try:
            server.serve_forever(poll_interval=.1)
        finally:
            server.server_close()


def main(argv=None):
    p=argparse.ArgumentParser(description='Local-only HTTP effect consumer (non-trading)')
    p.add_argument('--db',type=Path,required=True)
    p.add_argument('--port',type=int,default=8765)
    p.add_argument('--ready-file',type=Path)
    p.add_argument('--crash-after-commit',action='store_true')
    a=p.parse_args(argv)
    serve(a.db,a.port,a.ready_file,a.crash_after_commit)

if __name__=='__main__':main()
