#!/usr/bin/env python3
"""S07-004.1 isolated durable lanes, one immutable JSONL file / SQLite DB per lane.

Single coordinator, round-robin scheduling. No live exchange IO. No trading.
"""
from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from durable_stream import StreamDB, scope_of, valid, sha256_file

ALIAS_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$')


def source_scope(path: Path) -> str:
    with path.open('rb') as f:
        first = f.readline()
    if not first.strip():
        raise ValueError(f'Empty source or first line: {path}')
    e = json.loads(first)
    if not isinstance(e, dict) or not valid(e):
        raise ValueError(f'First record has invalid schema: {path}')
    return scope_of(e)


def parse_sources(items):
    found = {}
    for item in items:
        if '=' not in item:
            raise ValueError('Use --lane ALIAS=path.jsonl')
        alias, path = item.split('=', 1)
        if not ALIAS_RE.fullmatch(alias) or alias in found:
            raise ValueError(f'Invalid or repeated lane alias: {alias!r}')
        src = Path(path).resolve()
        if not src.is_file():
            raise FileNotFoundError(src)
        found[alias] = {'path': src, 'scope': source_scope(src)}
    if not found:
        raise ValueError('At least one --lane is required')
    scopes = [v['scope'] for v in found.values()]
    if len(scopes) != len(set(scopes)):
        raise ValueError('Two lane aliases cannot refer to the same full scope')
    return found


def registry_path(root):
    return root/'lane_registry.json'


def register(root, sources):
    root.mkdir(parents=True, exist_ok=True)
    registry = {a:{'source':str(v['path']), 'scope':v['scope'], 'sha256':sha256_file(v['path'])}
                for a,v in sorted(sources.items())}
    path = registry_path(root)
    if path.exists():
        old = json.loads(path.read_text(encoding='utf-8'))
        if old != registry:
            raise ValueError('Lane registry differs; refusing source substitution or lane set modification')
    else:
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(registry, indent=2), encoding='utf-8')
        tmp.replace(path)
    return registry


def open_lane(root, alias, record, max_pending, max_backlog):
    folder = root/'lanes'/alias
    folder.mkdir(parents=True,exist_ok=True)
    return StreamDB(folder/'state.sqlite3',record['path'],
                    expected_scope=record['scope'],
                    max_pending=max_pending,max_backlog=max_backlog)


def run(root, sources, *, batch=128, max_pending=128, max_backlog=1024, max_rounds=None):
    registry = register(root,sources)
    opened = {}
    try:
        for alias in sorted(sources):
            opened[alias] = open_lane(root,alias,sources[alias],max_pending,max_backlog)
        rounds = 0
        while max_rounds is None or rounds < max_rounds:
            rounds += 1
            progress = 0
            # Round robin: no lane may consume the entire processing budget while other lanes wait.
            # Per-lane database/quotas remove cross-lane pending head-of-line blocking.
            for alias in sorted(opened):
                s = opened[alias]
                p = s.process_batch(batch)
                capacity = s.max_pending - s.count('pending')
                a = s.ingest_batch(min(batch,max(0,capacity))) if capacity > 0 else 0
                progress += p+a
            if progress == 0:
                break
        summary = {'experiment':'FTDI-S07-004.1','rounds':rounds,'config':{
                   'batch':batch,'max_pending_per_lane':max_pending,'max_backlog_per_lane':max_backlog},
                   'lanes':{}}
        for alias,s in opened.items():
            lane_st = s.db.execute('SELECT next_seq,integrity FROM lanes').fetchone()
            counts = {'source_ack':int(s.get_meta('next_file_index')),
                      'file_exhausted':int(s.get_meta('next_byte_offset')) == s.source.stat().st_size,
                      'raw_stored':s.count('raw_journal'),
                      'raw_unprocessed':s.count('raw_journal','processed=0'),
                      'pending':s.count('pending'),
                      'outbox_total':s.count('outbox'),
                      'outbox_acked':s.count('outbox','delivered=1'),
                      'outbox_unacked':s.count('outbox','delivered=0'),
                      'integrity':lane_st['integrity'] if lane_st else 'EMPTY',
                      'next_seq':lane_st['next_seq'] if lane_st else None,
                      'source_sha256':s.fingerprint,
                      'scope':json.loads(s.expected_scope),
                      'archived':int(s.get_meta('archived_records') or 0)}
            summary['lanes'][alias]=counts
        return summary
    finally:
        for s in opened.values():
            s.close()


def make_synthetic(path:Path, count=2000, gap_at=None, symbol='ETHUSDT'):
    if path.exists():
        raise ValueError(f'Output already exists: {path}')
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',encoding='utf-8',newline='\n') as f:
        for n in range(1,count+1):
            if n==gap_at: continue
            e={'source':'synthetic','symbol':symbol,'venue':'SYNTH','channel':'aggTrades',
               'source_seq':n,'event_time_ms':1717200000000+n,
               'price':'2500.25','qty':'0.5'}
            f.write(json.dumps(e,separators=(',',':'))+'\n')


def main(argv=None):
    ap=argparse.ArgumentParser(description='Partitioned per-lane durable replay; no exchange recovery')
    ap.add_argument('--lane',action='append',help='ALIAS=local JSONL; repeat for independent lanes')
    ap.add_argument('--outdir',type=Path,default=Path('runs/FTDI-S07-004.1'))
    ap.add_argument('--batch',type=int,default=128)
    ap.add_argument('--max-pending',type=int,default=128)
    ap.add_argument('--max-backlog',type=int,default=1024)
    ap.add_argument('--max-rounds',type=int)
    ap.add_argument('--synthetic-out',type=Path,help='Create synthetic ETHUSDT lane fixture only')
    ap.add_argument('--synthetic-count',type=int,default=2000)
    ap.add_argument('--synthetic-gap',type=int,default=2)
    args=ap.parse_args(argv)
    if args.synthetic_out:
        make_synthetic(args.synthetic_out,args.synthetic_count,args.synthetic_gap)
        print('SYNTHETIC ONLY:',args.synthetic_out)
        return 0
    if not args.lane:
        ap.error('Provide at least one --lane ALIAS=path.jsonl')
    if min(args.batch,args.max_pending,args.max_backlog)<1:
        ap.error('batch and capacities must be positive')
    sources = parse_sources(args.lane)
    r = run(args.outdir,sources,batch=args.batch,max_pending=args.max_pending,
            max_backlog=args.max_backlog,max_rounds=args.max_rounds)
    out=args.outdir/'INGEST_REPORT.json'
    out.write_text(json.dumps(r,indent=2,ensure_ascii=False),encoding='utf-8')
    print('INGEST',out)
    for alias,k in r['lanes'].items():
        print(f"{alias}: ack={k['source_ack']} outbox={k['outbox_total']} pending={k['pending']} "
              f"unprocessed={k['raw_unprocessed']} state={k['integrity']} eof={k['file_exhausted']}")
    return 0 if all(v['file_exhausted'] and v['raw_unprocessed']==0 and v['pending']==0 for v in r['lanes'].values()) else 2

if __name__=='__main__':
    raise SystemExit(main())
