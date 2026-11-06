#!/usr/bin/env python3
"""Durable sequential streaming lab; no trading or live market feed."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import random
import sys
import time

from durable_stream import CrashPoint, STAGES, StreamDB, sha256_file


def make_demo(path: Path, count: int, seed: int = 20261106):
    if count < 1:
        raise ValueError('--count must be positive')
    if path.exists():
        with path.open('rb') as existing:
            found = sum(1 for _ in existing)
        if found != count:
            raise ValueError(f'Existing demo has {found} records, requested {count}; use a new --outdir')
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    tmp = path.with_suffix('.tmp')
    with tmp.open('w', encoding='utf-8', newline='\n') as f:
        t = 1717200000000
        cents = 6750000
        for i in range(count):
            t += 40 + rng.randrange(11)
            cents += rng.randrange(-20, 21)
            e = {'source':'synthetic','symbol':'BTCUSDT','venue':'SYNTH','channel':'aggTrades',
                 'source_seq':i + 1, 'event_time_ms':t,
                 'price':f'{cents/100:.2f}', 'qty':f'{0.01+(i % 100)*0.0001:.6f}'}
            f.write(json.dumps(e, separators=(',',':'))+'\n')
    tmp.replace(path)


def verify_manifest(path, manifest_path):
    if not manifest_path.exists():
        raise ValueError(f'Missing S07-002 manifest: {manifest_path}')
    m = json.loads(manifest_path.read_text(encoding='utf-8'))
    if m.get('official_checksum_verified') is not True:
        raise ValueError('Original archive checksum was not verified in S07-002 manifest')
    actual = sha256_file(path)
    if actual != m.get('data_jsonl_sha256'):
        raise ValueError('JSONL SHA256 mismatch against S07-002 manifest')
    return {'manifest_path':str(manifest_path.resolve()),
            'source_data_sha256':actual,'official_archive_checksum_inherited':True,
            'original_archive_sha256':m.get('archive_sha256')}


def acceptance(r):
    accounted = sum(r['dispositions'].values()) == r['raw_journal_records'] - r['raw_unprocessed']
    checks = {
        'source_reached_eof':r['source_file_exhausted'],
        'ingest_checkpoint_matches_journal':r['raw_journal_records']==r['source_records_acknowledged'],
        'all_raw_processed':r['raw_unprocessed']==0,
        'no_pending_at_end':r['pending']==0,
        'all_outbox_acknowledged':r['outbox_unacked']==0,
        'outbox_matches_delivery':r['outbox_records']==r['delivery_records'],
        'no_quarantine':r['quarantine_records']==0,
        'all_lanes_contiguous':all(x['integrity']=='CONTIGUOUS' for x in r['lanes']),
        'dispositions_accounted':accounted,
        'no_orphan_output':r['orphan_or_unprocessed_outputs']==0,
        'no_internal_delivery_conflicts':r['delivery_inconsistencies']==0,
        'journal_storage_hashes_valid':r['raw_journal_checksum_mismatches']==0,
        'outbox_payload_hashes_valid':r['outbox_content_mismatches']==0,
        'source_file_immutable_during_run':r['source_file_still_matches_initial_sha256'],
    }
    return checks


def write_report(result, outdir):
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir/'results.json').write_text(json.dumps(result, indent=2, ensure_ascii=False),encoding='utf-8')
    r = result['state']
    rows = ['# FTDI-S07-004 · 持久化流式与崩溃恢复实验', '',
            '**范围：本地历史 JSONL/合成数据的流式摄入、SQLite WAL、事务检查点、内部交付。没有真实交易所连接、没有实盘网络延迟、没有真实下单。**','',
            '| 指标 | 数量 |','|---|---:|']
    for key in ('source_records_acknowledged','raw_journal_records','raw_unprocessed',
                'pending','outbox_records','outbox_unacked','delivery_records','quarantine_records'):
        rows.append(f'| {key} | {r[key]} |')
    rows.extend(['',f"**整体内部一致性验收：{'通过' if result['acceptance_passed'] else '未通过或尚未完成'}**",'',
                 '| 检查 | 结果 |','|---|---|'])
    for k,v in result['checks'].items():
        rows.append(f"| {k} | {'通过' if v else '未通过'} |")
    rows.extend(['','## 各数据流状态','','| 来源 / 标的 / venue / channel | 下一个 ID | 流状态 | 待处理最小 ID |','|---|---:|---|---:|'])
    for lane in r['lanes']:
        rows.append(f"| {' / '.join(lane['scope'])} | {lane['next_expected_seq']} | {lane['integrity']} | {lane['lowest_pending_seq'] if lane['lowest_pending_seq'] is not None else '-'} |")
    rows.extend(['','## 关键边界','','- 外部下游交付的 exactly-once **尚未证明**；本版交付目标是**同一个 SQLite 数据库**中的 `delivery` 表。',
                 '- `WAL + synchronous=FULL` 面向单机进程崩溃与通常文件系统假设；无法保证硬件损坏、恶意修改、控制器撒谎或跨机器灾难。',
                 '- 原始文件内容由 SHA-256 绑定，历史真实性仍需 S07-002 校验的来源证明。',
                 '- 流开始以前的未知缺口无法从第一个观测 ID 自动证明；尾部无后续事件时无法知道交易所是否仍有缺失数据。',
                 '- 这次未集成 S07-003.2 的模拟 oracle；真实缺口保持待处理，不能无证据跨越。',
                 '- 墙钟与 CPU 为本地执行阶段耗时，不是实时市场行情 P99。',''])
    (outdir/'REPORT.md').write_text('\n'.join(rows),encoding='utf-8')


def main(argv=None):
    ap = argparse.ArgumentParser(description='FTDI-S07-004 durable streaming / crash restart lab (no trading)')
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument('--input',type=Path,help='S07-002 checksum-verified JSONL')
    src.add_argument('--demo',action='store_true',help='Synthetic file input, never real market data')
    ap.add_argument('--manifest',type=Path)
    ap.add_argument('--count',type=int,default=50000,help='Synthetic record count')
    ap.add_argument('--outdir',type=Path,default=Path('runs/FTDI-S07-004'))
    ap.add_argument('--batch',type=int,default=128)
    ap.add_argument('--max-backlog',type=int,default=8192)
    ap.add_argument('--max-pending',type=int,default=8192)
    ap.add_argument('--max-cycles',type=int,default=None,help='Stop early for resume test')
    ap.add_argument('--crash-stage',choices=STAGES,help='Abrupt os._exit(86); NEVER use on production databases')
    ap.add_argument('--crash-after',type=int,default=1)
    args=ap.parse_args(argv)
    if args.batch<1 or args.max_backlog<1 or args.max_pending<1 or (args.max_cycles is not None and args.max_cycles<1):
        ap.error('Batch/capacities/max-cycles must be positive')
    if args.demo:
        path=args.outdir/'synthetic_input.jsonl'
        make_demo(path,args.count)
        provenance={'mode':'SYNTHETIC', 'official_archive_checksum_inherited':False}
    else:
        path=args.input
        if not path.is_file():
            ap.error(f'Input missing: {path}')
        try:
            provenance=verify_manifest(path,args.manifest or path.parent.parent/'manifest.json')
        except (OSError,ValueError,KeyError,json.JSONDecodeError) as e:
            ap.error(str(e))
        provenance['mode']='S07-002-VERIFIED-HISTORY'
    start=time.perf_counter()
    cpu=time.process_time()
    with StreamDB(args.outdir/'state.sqlite3',path, max_pending=args.max_pending,
                  max_backlog=args.max_backlog,
                  crash=CrashPoint(args.crash_stage,args.crash_after)) as s:
        r=s.pump(batch=args.batch,max_cycles=args.max_cycles)
    checks=acceptance(r)
    result={'experiment_id':'FTDI-S07-004','source':str(path.resolve()),
            'provenance':provenance,'params':{'batch':args.batch,'max_backlog':args.max_backlog,
                                           'max_pending':args.max_pending},
            'state':r,'checks':checks,'acceptance_passed':all(checks.values()),
            'wall_seconds':round(time.perf_counter()-start,4),
            'process_cpu_seconds':round(time.process_time()-cpu,4)}
    write_report(result,args.outdir)
    print(f"FTDI-S07-004: ack={r['source_records_acknowledged']} delivered={r['delivery_records']} "
          f"unprocessed={r['raw_unprocessed']} pending={r['pending']} "
          f"ACCEPTANCE={'PASS' if result['acceptance_passed'] else 'NOT_YET'} "
          f"wall={result['wall_seconds']}s")
    print('Report:',(args.outdir/'REPORT.md').resolve())
    if not result['acceptance_passed']:
        return 2
    return 0

if __name__=='__main__':
    sys.exit(main())
