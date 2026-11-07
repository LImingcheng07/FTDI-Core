"""Unified local development CLI (no trading and no automatic network access)."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
import sqlite3

from .preparation import prepare_archive
from .service import replay
from .audit import audit
from .delivery import local_lab
from .e2e import reconcile
from .archive_e2e import reconcile_archive_aware
from .retention_plan import plan_consumer_retention


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog='python -m ftdi_core',description='FTDI Core v0.2: local verified research runtime')
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
        print(json.dumps(result,ensure_ascii=False,indent=2))
        return 0 if good else 2
    except (ValueError,FileNotFoundError,FileExistsError,KeyError,RuntimeError,ConnectionError,OSError,sqlite3.Error) as exc:
        print(f'FTDI_CORE_FAIL_CLOSED: {exc}',file=sys.stderr)
        return 3
