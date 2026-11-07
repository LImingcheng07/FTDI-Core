"""Unified local development CLI (no trading and no automatic network access)."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
import sqlite3

from . import __version__
from .preparation import prepare_archive
from .service import replay
from .audit import audit
from .delivery import local_lab
from .e2e import reconcile
from .archive_e2e import reconcile_archive_aware
from .retention_plan import plan_consumer_retention
from .timeprov import analyze_pilot_db
from .consumer_retention import compact_consumer, migrate_consumer_v2, rehydrate_consumer


def _resolve_lane(args):
    if getattr(args, 'producer_root', None) is None:
        return args.lane
    from .archive_e2e import ArchiveLimits, _load_registry
    _, registry = _load_registry(Path(args.producer_root).resolve() / 'lane_registry.json', ArchiveLimits())
    if args.lane not in registry:
        raise ValueError(f'unknown registry alias: {args.lane}')
    return registry[args.lane]['scope']


def _consumer_retention(args):
    if args.action == 'migrate':
        return migrate_consumer_v2(args.consumer_db, apply=args.apply)
    lane = _resolve_lane(args)
    if args.action == 'compact':
        return compact_consumer(args.consumer_db, lane=lane, epoch=args.epoch, content_watermark=args.watermark,
                                producer_root=args.producer_root, ignore_producer_window=args.ignore_producer_window,
                                apply=args.apply, segment_size=args.segment_size, crash_stage=args.crash_stage)
    return rehydrate_consumer(args.consumer_db, lane=lane, epoch=args.epoch, to_watermark=args.to_watermark,
                              apply=args.apply)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog='python -m ftdi_core',
                                description=f'FTDI Core v{__version__}: local verified research runtime (no trading)')
    p.add_argument('--version',action='version',version=f'ftdi-core {__version__}')
    sub = p.add_subparsers(dest='cmd',required=True)
    a = sub.add_parser('prepare',help='Convert a verified Binance aggTrades ZIP into immutable Core JSONL')
    a.add_argument('--archive',type=Path,required=True)
    a.add_argument('--checksum',type=Path)
    a.add_argument('--expected-sha256')
    a.add_argument('--outdir',type=Path,required=True)
    a.add_argument('--symbol',default='BTCUSDT')
    a.add_argument('--limit',type=int,default=50000,help='0 = all records')
    b = sub.add_parser('replay',help='Resume isolated durable replay of one or more JSONL lanes')
    b.add_argument('--lane',action='append',required=True,help='ALIAS=/path/to/file.jsonl')
    b.add_argument('--outdir',type=Path,required=True)
    b.add_argument('--batch',type=int,default=128)
    b.add_argument('--max-pending',type=int,default=128)
    b.add_argument('--max-backlog',type=int,default=1024)
    b.add_argument('--max-rounds',type=int)
    c = sub.add_parser('audit',help='Recompute hashes/constraints for local committed data')
    c.add_argument('--outdir',type=Path,required=True)
    d = sub.add_parser('lab-e2e',help='Launch localhost-only independent consumer, deliver, reconcile')
    d.add_argument('--outdir',type=Path,required=True)
    d.add_argument('--consumer-db',type=Path,required=True)
    d.add_argument('--batch',type=int,default=256)
    d.add_argument('--max-batches',type=int)
    e = sub.add_parser('audit-e2e',help='Independently reconcile resident producer and consumer events')
    e.add_argument('--outdir',type=Path,required=True)
    e.add_argument('--consumer-db',type=Path,required=True)
    f = sub.add_parser('audit-e2e-archive',
                       help='Read-only archive-aware reconciliation: archived prefix + resident outbox vs consumer effects')
    f.add_argument('--outdir',type=Path,required=True)
    f.add_argument('--consumer-db',type=Path,required=True)
    f.add_argument('--report',type=Path,
                   help='Write the JSON report here (atomic; never inside lanes/ or archives/). Default: stdout only')
    f.add_argument('--allow-relocated-archives',action='store_true',
                   help='Accept archive_path values recorded under another run root; only in-root files are read')
    g = sub.add_parser('plan-consumer-retention',
                       help='Dry run (Task 002): what consumer retention could safely compact; deletes nothing')
    g.add_argument('--consumer-db',type=Path,required=True)
    g.add_argument('--producer-root',type=Path,help='Run root whose producer ACK window bounds the watermark')
    g.add_argument('--margin',type=int,default=0,help='Extra events kept below the proposed watermark')
    g.add_argument('--report',type=Path,help='Write the JSON plan here (atomic). Default: stdout only')
    r = sub.add_parser('consumer-retention', help='Consumer retention v2 (Task 002): migrate / compact / rehydrate; dry run unless --apply')
    rs = r.add_subparsers(dest='action', required=True)
    for action in ('migrate', 'compact', 'rehydrate'):
        q = rs.add_parser(action)
        q.add_argument('--consumer-db', type=Path, required=True)
        q.add_argument('--apply', action='store_true', help='Perform the change (default: dry run, writes nothing)')
        if action != 'migrate':
            q.add_argument('--lane', required=True, help='Registry alias with --producer-root, else the exact lane scope')
            q.add_argument('--epoch', type=int, default=0)
        if action == 'compact':
            q.add_argument('--watermark', type=int, required=True)
            window = q.add_mutually_exclusive_group(required=True)
            window.add_argument('--producer-root', type=Path)
            window.add_argument('--ignore-producer-window', action='store_true')
            q.add_argument('--segment-size', type=int, default=20000)
            q.add_argument('--crash-stage', choices=['after_segment_file', 'after_commit'], help='test only')
        if action == 'rehydrate':
            q.add_argument('--to-watermark', type=int)
            q.add_argument('--producer-root', type=Path, help='Only to resolve --lane as a registry alias')
    h = sub.add_parser('analyze-clock-evidence',
                       help='Read-only retro-analysis of a live-capture DB (Task 003); no network')
    h.add_argument('--pilot-db',type=Path,required=True)
    h.add_argument('--report',type=Path)
    args = p.parse_args(argv)
    try:
        if args.cmd == 'prepare':
            result = prepare_archive(args.archive,args.outdir,checksum=args.checksum,
                                     expected_sha256=args.expected_sha256,symbol=args.symbol,limit=args.limit)
            good = True
        elif args.cmd == 'replay':
            result = replay(args.outdir,args.lane,batch=args.batch,max_pending=args.max_pending,
                            max_backlog=args.max_backlog,max_rounds=args.max_rounds)
            good = result['complete']
        elif args.cmd == 'audit':
            result = audit(args.outdir)
            good = result['passed']
        elif args.cmd == 'lab-e2e':
            result = local_lab(args.outdir,args.consumer_db,batch=args.batch,max_batches=args.max_batches)
            good = result['pass']
        elif args.cmd == 'consumer-retention':
            result = _consumer_retention(args)
            good = True
        elif args.cmd == 'analyze-clock-evidence':
            result = analyze_pilot_db(args.pilot_db,report_path=args.report)
            good = True
        elif args.cmd == 'plan-consumer-retention':
            result = plan_consumer_retention(args.consumer_db,producer_root=args.producer_root,
                                             margin=args.margin,report_path=args.report)
            good = result['status'] != 'BLOCKED'
        elif args.cmd == 'audit-e2e-archive':
            result = reconcile_archive_aware(args.outdir,args.consumer_db,report_path=args.report,
                                             allow_relocated_archives=args.allow_relocated_archives)
            good = result['passed']
        else:
            result = reconcile(args.outdir,args.consumer_db)
            good = result['passed']
        # ASCII-only JSON: valid under any console/pipe encoding (GBK on Chinese Windows); a non-encodable
        # path character must never turn a finished audit into an exit-code-3 failure.
        print(json.dumps(result,ensure_ascii=True,indent=2))
        return 0 if good else 2
    except (ValueError,FileNotFoundError,FileExistsError,KeyError,RuntimeError,ConnectionError,OSError,sqlite3.Error) as exc:
        print('FTDI_CORE_FAIL_CLOSED: ' + str(exc).encode('ascii','backslashreplace').decode('ascii'),file=sys.stderr)
        return 3
