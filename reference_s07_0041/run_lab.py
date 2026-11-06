#!/usr/bin/env python3
"""Single-command reproducible S07-004.1 integration test using SYNTHETIC data only."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from multi_lane import make_synthetic,parse_sources,run
from external_delivery import EffectStore
from outbox_publisher import publish
from retention import prune,audit


def integration(root,count=2000,pending=32):
    root=Path(root)
    root.mkdir(parents=True,exist_ok=True)
    b=root/'synthetic_BTC.jsonl';e=root/'synthetic_ETH_missing_2.jsonl'
    if not b.exists():make_synthetic(b,count,None,'BTCUSDT')
    if not e.exists():make_synthetic(e,count,2,'ETHUSDT')
    src=parse_sources([f'BTC={b}',f'ETH={e}'])
    ingested=run(root,src,batch=64,max_pending=pending,max_backlog=256)
    ready=root/'consumer.ready'
    if ready.exists():ready.unlink()
    server=subprocess.Popen([sys.executable,str(Path(__file__).parent/'consumer_service.py'),
             '--db',str(root/'consumer.sqlite3'),'--port','0','--ready-file',str(ready)],
             stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 6.0
        port = None
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError('Local consumer exited before readiness')
            if ready.exists():
                content = ready.read_text(encoding='ascii').strip()
                if content.isdecimal() and 1 <= int(content) <= 65535:
                    port = int(content)
                    break
            time.sleep(.025)
        if port is None:raise RuntimeError('Local consumer did not become ready')
        pub=publish(root,port,batch=128)
    finally:
        if server.poll() is None:
            server.terminate()
            try:server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill();server.wait(timeout=5)
    with EffectStore(root/'consumer.sqlite3') as consumer:
        effect_count=consumer.count()
        effect_sha256=consumer.logical_sha256()
    archive_result=prune(root,'BTC',min(count,20000),True,root/'consumer.sqlite3')
    archive_audit=audit(root,'BTC',root/'consumer.sqlite3')
    binfo,einfo=ingested['lanes']['BTC'],ingested['lanes']['ETH']
    checks={'btc_complete':binfo['file_exhausted'] and binfo['outbox_total']+binfo['archived']==count and binfo['pending']==0,
            'eth_permanent_gap_safe':einfo['integrity']=='WAITING_FOR_MISSING' and einfo['pending']==pending and einfo['outbox_total']==1,
            'eth_did_not_block_btc':binfo['source_ack']==count,
            'external_effects_once':effect_count==count+1 and pub['acknowledged']<=count+1,
            'archive_requires_consumer_receipt':archive_audit['consumer_identities_missing']==0,
            'btc_archive_complete':archive_audit['archive_records']==count and archive_audit['resident_raw']==0}
    result={'experiment':'FTDI-S07-004.1','data':'SYNTHETIC ONLY','checks':checks,'pass':all(checks.values()),
            'ingest':ingested,'publisher':pub,'effect_count':effect_count,'consumer_digest':effect_sha256,
            'archive':archive_result,'audit':archive_audit}
    (root/'LAB_RESULTS.json').write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    (root/'LAB_REPORT.md').write_text('# FTDI-S07-004.1 一键综合实验\n\n**完全合成数据，非真实交易所接入。**\n\n'+
        '\n'.join(f'- {k}: {"PASS" if v else "FAIL"}' for k,v in checks.items())+
        f'\n\nBTC={count}，ETH 存在永久缺口，外部效果记录={effect_count}。\n\n',encoding='utf-8')
    return result


def main(argv=None):
    p=argparse.ArgumentParser(description='One-command synthetic integrated demo')
    p.add_argument('--outdir',type=Path,default=Path('runs/FTDI-S07-004.1-demo'))
    p.add_argument('--count',type=int,default=2000)
    p.add_argument('--pending',type=int,default=32)
    a=p.parse_args(argv)
    if min(a.count,a.pending)<1:p.error('count and pending must be positive')
    r=integration(a.outdir,a.count,a.pending)
    print('INTEGRATION', 'PASS' if r['pass'] else 'FAIL', 'report=',a.outdir/'LAB_REPORT.md')
    return 0 if r['pass'] else 2

if __name__=='__main__':raise SystemExit(main())
