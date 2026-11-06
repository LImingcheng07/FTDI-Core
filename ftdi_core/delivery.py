"""Core v0.2 orchestration for the tested isolated S07-004.1 HTTP delivery adapter.

A localhost-only consumer is a *separate process with its own independent DB*.
No public HTTP exposure, trading, credentials, or silent archive operations.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError

from .adapters.s070041.outbox_publisher import publish
from .e2e import reconcile


def _validate_independent(root, consumer_db):
    root=Path(root)
    consumer_db=Path(consumer_db).resolve()
    registry=json.loads((root/'lane_registry.json').read_text(encoding='utf-8'))
    for alias in registry:
        if consumer_db == (root/'lanes'/alias/'state.sqlite3').resolve():
            raise ValueError('Consumer DB must be independent from producer journal')
    return registry


def _await_consumer_ready(ready: Path, proc, *, timeout=15.0):
    """Wait for a complete, valid port instead of only checking file existence.

    An older consumer might create an empty readiness file before writing the
    port. Windows can schedule the publisher in that interval. The current
    consumer also publishes the file atomically, so both sides are defensive.
    """
    deadline = time.monotonic() + timeout
    last_seen = 'not created'
    while True:
        code = proc.poll()
        if code is not None:
            details = proc.stderr.read()[-1000:] if proc.stderr else ''
            raise RuntimeError(
                f'Consumer exited before ready (exit={code}; readiness={last_seen}): {details}'
            )
        try:
            content = ready.read_text(encoding='ascii').strip()
        except FileNotFoundError:
            last_seen = 'not created'
        except (OSError, UnicodeError) as exc:
            last_seen = f'not readable ({type(exc).__name__})'
        else:
            last_seen = repr(content[:80]) if content else 'empty file'
            # A valid readiness file consists of a complete decimal TCP port.
            # Do not turn a zero-length file into an uncaught ValueError.
            if content.isascii() and content.isdecimal():
                port = int(content)
                if 1 <= port <= 65535:
                    return port
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f'Consumer readiness deadline exceeded ({timeout:g}s; last={last_seen})'
            )
        time.sleep(.025)


def local_lab(root: Path, consumer_db: Path, *, batch=256, max_batches=None, timeout=10.0):
    """Start consumer process on an OS-assigned loopback port, deliver, audit, stop.

    Can be rerun safely against the same producer/consumer databases.  An
    interrupted run can be resumed by running this command again.
    """
    root=Path(root)
    consumer_db=Path(consumer_db)
    _validate_independent(root,consumer_db)
    if batch<1 or batch>1024 or (max_batches is not None and max_batches<1):
        raise ValueError('Invalid batch or max_batches')
    with tempfile.TemporaryDirectory(prefix='ftdi_e2e_') as td:
        ready=Path(td)/'ready.txt'
        proc=subprocess.Popen([sys.executable,'-m','ftdi_core.adapters.s070041.consumer_service',
                               '--db',str(consumer_db.resolve()),'--port','0','--ready-file',str(ready)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            port=_await_consumer_ready(ready,proc,timeout=15.0)
            t0=time.perf_counter()
            result=publish(root,port,batch=batch,max_batches=max_batches,timeout=timeout)
            result['wall_seconds']=round(time.perf_counter()-t0,4)
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill();proc.wait(timeout=5)
            if proc.stderr:
                proc.stderr.close()
    audit=reconcile(root,consumer_db)
    outcome={'schema':'FTDI-Core-Lab-E2E-v1','delivery':result,'audit':audit,
             'pass':audit['passed'], 'mode':'LOCALHOST_TWO_INDEPENDENT_SQLITE_DATABASES'}
    (root/'E2E_LAB_REPORT.json').write_text(json.dumps(outcome,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    return outcome
