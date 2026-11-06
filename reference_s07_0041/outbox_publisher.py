#!/usr/bin/env python3
"""Localhost HTTP outbox publisher with duplicate-safe independent consumer ACK."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sqlite3
from contextlib import closing
import time
from urllib.request import Request,urlopen
from external_delivery import batch_digest,envelope


def fetch_pending(db,n,lane):
    db.row_factory=sqlite3.Row
    return db.execute('SELECT * FROM outbox WHERE delivered=0 AND lane=? ORDER BY seq LIMIT ?', (lane,n)).fetchall()


def commit_ack(db,rows):
    db.execute('BEGIN IMMEDIATE')
    try:
        for r in rows:
            old=db.execute('SELECT raw_id,raw_sha256,delivered FROM outbox WHERE lane=? AND seq=?',
                           (r['lane'],r['seq'])).fetchone()
            if old is None or old['raw_id']!=r['raw_id'] or old['raw_sha256']!=r['raw_sha256']:
                raise ValueError('Source outbox changed before ack; fail closed')
            db.execute('INSERT OR IGNORE INTO delivery(lane,seq,raw_id,raw_sha256) VALUES(?,?,?,?)',
                       (r['lane'],r['seq'],r['raw_id'],r['raw_sha256']))
            db.execute('UPDATE outbox SET delivered=1 WHERE lane=? AND seq=? AND raw_sha256=?',
                       (r['lane'],r['seq'],r['raw_sha256']))
        db.commit()
    except BaseException:
        db.rollback()
        raise


def publish(root,port,*,batch=256,max_batches=None,crash_before_ack=False,timeout=10.0):
    if batch<1 or batch>1024:
        raise ValueError('batch must be in [1,1024]')
    registry_path=Path(root)/'lane_registry.json'
    registry=json.loads(registry_path.read_text(encoding='utf-8'))
    totals={'batches':0,'events_sent':0,'acknowledged':0,'remote_duplicates':0,'lanes':{}}
    for alias in sorted(registry):
        path=Path(root)/'lanes'/alias/'state.sqlite3'
        with closing(sqlite3.connect(str(path),timeout=30)) as db:
            db.row_factory=sqlite3.Row
            db.execute('PRAGMA busy_timeout=30000')
            sent=0
            while max_batches is None or totals['batches']<max_batches:
                rows=fetch_pending(db,batch,registry[alias]['scope'])
                if not rows:break
                events=[envelope(r) for r in rows]
                req=Request(f'http://127.0.0.1:{port}/batch',
                            data=json.dumps({'events':events},separators=(',',':')).encode('utf-8'),
                            headers={'Content-Type':'application/json'},method='POST')
                with urlopen(req,timeout=timeout) as r:
                    response=json.loads(r.read())
                    if r.status!=200 or response.get('batch_digest')!=batch_digest(events) or response.get('accepted')!=len(events):
                        raise ValueError('Untrusted/incomplete remote ACK; no local commit')
                totals['remote_duplicates']+=int(response.get('duplicates',0))
                sent+=len(rows)
                totals['events_sent']+=len(rows)
                totals['batches']+=1
                if crash_before_ack:
                    os._exit(86) # Remote commit succeeded, local DB ACK is not committed.
                commit_ack(db,rows)
                totals['acknowledged']+=len(rows)
            totals['lanes'][alias]=sent
        if max_batches is not None and totals['batches']>=max_batches:break
    return totals


def main(argv=None):
    p=argparse.ArgumentParser(description='At-least-once delivery from isolated durable outboxes to localhost-only consumer')
    p.add_argument('--outdir',type=Path,required=True)
    p.add_argument('--port',type=int,default=8765)
    p.add_argument('--batch',type=int,default=256)
    p.add_argument('--max-batches',type=int)
    p.add_argument('--crash-before-ack',action='store_true')
    a=p.parse_args(argv)
    t=time.perf_counter()
    result=publish(a.outdir,a.port,batch=a.batch,max_batches=a.max_batches,crash_before_ack=a.crash_before_ack)
    result['wall_seconds']=round(time.perf_counter()-t,4)
    print(json.dumps(result,indent=2))
    (a.outdir/'PUBLISH_REPORT.json').write_text(json.dumps(result,indent=2),encoding='utf-8')

if __name__=='__main__':main()
