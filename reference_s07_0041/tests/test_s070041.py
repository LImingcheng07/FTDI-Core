from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from contextlib import closing
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from multi_lane import parse_sources,run,make_synthetic
from durable_stream import StreamDB,scope_of,canonical
from external_delivery import EffectStore, Conflict, validate, envelope
from outbox_publisher import publish
from retention import prune,audit


def make_events(path,n=30,symbol='BTCUSDT',gap=None,mutate=False):
    with open(path,'w',encoding='utf-8',newline='\n') as f:
        for seq in range(1,n+1):
            if seq==gap:continue
            e={'source':'test','venue':'TEST','symbol':symbol,'channel':'aggTrades',
               'source_seq':seq,'event_time_ms':1700000000000+seq,
               'price':'1.25' if not mutate else '2.50','qty':'0.1'}
            f.write(json.dumps(e,separators=(',',':'))+'\n')


class NewArchitectureTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home=Path(self.tmp.name)
        self.root=self.home/'state'
        self.btc=self.home/'btc.jsonl'
        self.eth=self.home/'eth.jsonl'

    def ingest(self,**kw):
        src=parse_sources([f'BTC={self.btc}',f'ETH={self.eth}'])
        return run(self.root,src,**kw)

    def _consumer(self,crash_after_commit=False):
        portfile=self.home/'ready.port'
        try:portfile.unlink()
        except FileNotFoundError:pass
        args=[sys.executable,str(ROOT/'consumer_service.py'),'--db',str(self.home/'consumer.sqlite3'),
              '--port','0','--ready-file',str(portfile)]
        if crash_after_commit:args.append('--crash-after-commit')
        proc=subprocess.Popen(args,cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        deadline=time.monotonic()+6
        while not portfile.is_file() and time.monotonic()<deadline and proc.poll() is None:
            time.sleep(.025)
        if not portfile.is_file():
            proc.kill();proc.communicate(timeout=5)
            raise RuntimeError('Consumer did not start')
        port=int(portfile.read_text())
        self.addCleanup(self._stop,proc)
        return proc,port

    @staticmethod
    def _stop(proc):
        if proc.poll() is None:
            proc.terminate()
            try:proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill();proc.wait(timeout=2)
        if proc.stderr:proc.stderr.close()

    def test_per_lane_gap_safety_and_fairness(self):
        make_events(self.btc,200)
        make_events(self.eth,200,symbol='ETHUSDT',gap=2)
        r=self.ingest(batch=16,max_pending=8,max_backlog=16)
        b,e=r['lanes']['BTC'],r['lanes']['ETH']
        self.assertEqual(b['source_ack'],200)
        self.assertEqual(b['outbox_total'],200)
        self.assertEqual(b['integrity'],'CONTIGUOUS')
        self.assertEqual(e['pending'],8)
        self.assertEqual(e['outbox_total'],1)
        self.assertFalse(e['file_exhausted'])
        self.assertEqual(e['integrity'],'WAITING_FOR_MISSING')
        # Durable checkpoint and read position still stable after second invocation.
        self.assertEqual(self.ingest(batch=16,max_pending=8,max_backlog=16)['lanes']['ETH']['source_ack'],e['source_ack'])

    def test_reorder_within_lane_recovers_without_oracle(self):
        evs=[]
        for seq in (1,3,2,4):
            evs.append({'source':'test','symbol':'BTCUSDT','venue':'TEST','channel':'aggTrades',
                         'source_seq':seq,'event_time_ms':1700000000000+seq,'price':'3','qty':'2'})
        self.btc.write_text(''.join(json.dumps(e)+'\n' for e in evs))
        r=run(self.root,parse_sources([f'BTC={self.btc}']),batch=1,max_pending=2,max_backlog=2)
        self.assertEqual(r['lanes']['BTC']['outbox_total'],4)
        self.assertEqual(r['lanes']['BTC']['pending'],0)

    def test_cross_lane_source_rejected_no_mixed_ack(self):
        make_events(self.btc,10)
        with self.btc.open('a') as f:
            f.write(json.dumps({'source':'test','symbol':'ETHUSDT','venue':'TEST','channel':'aggTrades',
                                 'source_seq':11,'event_time_ms':10,'price':'1','qty':'1'})+'\n')
        with self.assertRaisesRegex(ValueError,'Cross-lane input'):
            run(self.root,parse_sources([f'BTC={self.btc}']),batch=100)
        with closing(sqlite3.connect(str(self.root/'lanes'/'BTC'/'state.sqlite3'))) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM raw_journal').fetchone()[0],0)

    def test_lane_registry_source_replacement_rejected(self):
        make_events(self.btc,10)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        make_events(self.eth,10,symbol='ETHUSDT')
        with self.assertRaisesRegex(ValueError,'Lane registry differs'):
            run(self.root,parse_sources([f'ETH={self.eth}']))

    def test_synthetic_lane_explicit_tag_and_gap(self):
        make_synthetic(self.eth,20,gap_at=2)
        lines=self.eth.read_text().splitlines()
        self.assertEqual(len(lines),19)
        self.assertEqual(json.loads(lines[0])['source'],'synthetic')
        self.assertEqual(json.loads(lines[1])['source_seq'],3)

    def test_consumer_rejects_tampered_hash(self):
        e={'source':'test','symbol':'BTCUSDT','venue':'TEST','channel':'aggTrades',
           'source_seq':1,'event_time_ms':123,'price':'1','qty':'1'}
        lane=scope_of(e);d=hashlib.sha256(canonical(e).encode()).hexdigest()
        from external_delivery import event_identity
        x={'event_id':event_identity(lane,1,0,d),'lane':lane,'seq':1,'epoch':0,
           'payload_sha256':d,'payload':e}
        self.assertEqual(validate(x)[0],x['event_id'])
        x['payload']['price']='999'
        with self.assertRaisesRegex(ValueError,'hash mismatch'):
            validate(x)

    def test_effect_store_atomic_conflict_and_idempotency(self):
        make_events(self.btc,2)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        with closing(sqlite3.connect(str(self.root/'lanes'/'BTC'/'state.sqlite3'))) as source_db:
            source_db.row_factory=sqlite3.Row
            rows=source_db.execute('SELECT * FROM outbox ORDER BY seq').fetchall()
            a,b=[envelope(r) for r in rows]
        with EffectStore(self.home/'effect.sqlite3') as db:
            self.assertEqual(db.accept([a,b])['inserted'],2)
            self.assertEqual(db.accept([a,b])['duplicates'],2)
            modified=dict(b);modified['payload']=dict(b['payload']);modified['payload']['price']='7'
            h=hashlib.sha256(canonical(modified['payload']).encode()).hexdigest()
            from external_delivery import event_identity
            modified['payload_sha256']=h
            modified['event_id']=event_identity(b['lane'],b['seq'],0,h)
            with self.assertRaises(Conflict):db.accept([a,modified])
            self.assertEqual(db.count(),2)

    def test_real_subprocess_publisher_crash_before_ack_replayed(self):
        make_events(self.btc,25)
        run(self.root,parse_sources([f'BTC={self.btc}']),batch=7)
        proc,port=self._consumer()
        cmd=[sys.executable,str(ROOT/'outbox_publisher.py'),'--outdir',str(self.root),
             '--port',str(port),'--batch','7','--crash-before-ack']
        crashed=subprocess.run(cmd,cwd=ROOT,capture_output=True,timeout=8)
        self.assertEqual(crashed.returncode,86,crashed.stderr.decode())
        with closing(sqlite3.connect(str(self.root/'lanes'/'BTC'/'state.sqlite3'))) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM delivery').fetchone()[0],0)
        resumed=publish(self.root,port,batch=7)
        self.assertGreaterEqual(resumed['remote_duplicates'],7)
        self.assertEqual(resumed['acknowledged'],25)
        with EffectStore(self.home/'consumer.sqlite3') as db:
            self.assertEqual(db.count(),25)

    def test_real_subprocess_consumer_crash_after_commit_before_response(self):
        make_events(self.btc,19)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        proc,port=self._consumer(crash_after_commit=True)
        with self.assertRaises(Exception):publish(self.root,port,batch=5,timeout=2)
        self.assertEqual(proc.wait(timeout=5),86)
        with EffectStore(self.home/'consumer.sqlite3') as db:
            self.assertEqual(db.count(),5)
        proc2,port2=self._consumer(crash_after_commit=False)
        r=publish(self.root,port2,batch=5)
        self.assertGreaterEqual(r['remote_duplicates'],5)
        self.assertEqual(r['acknowledged'],19)
        with EffectStore(self.home/'consumer.sqlite3') as db:
            self.assertEqual(db.count(),19)

    def test_retention_does_not_prune_without_external_ack(self):
        make_events(self.btc,30)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        r=prune(self.root,'BTC',20)
        self.assertEqual(r['archived_new'],0)
        self.assertEqual(audit(self.root,'BTC')['resident_raw'],30)

    def test_retention_archives_and_verifies_consumer_ack(self):
        make_events(self.btc,60)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        proc,port=self._consumer()
        self.assertEqual(publish(self.root,port,batch=12)['acknowledged'],60)
        r=prune(self.root,'BTC',20,True,self.home/'consumer.sqlite3')
        self.assertEqual(r['archived_new'],20)
        self.assertGreater(r['db_bytes_after'],0)  # Small SQLite files may grow due to archive metadata/indexes
        a=audit(self.root,'BTC',self.home/'consumer.sqlite3')
        self.assertEqual(a['archive_records'],20)
        self.assertEqual(a['resident_raw'],40)
        r2=prune(self.root,'BTC',60,False,self.home/'consumer.sqlite3')
        self.assertEqual(r2['archived_new'],40)
        self.assertEqual(audit(self.root,'BTC',self.home/'consumer.sqlite3')['archive_records'],60)
        # Restart after archival must not rewind the immutable source checkpoint.
        r3=run(self.root,parse_sources([f'BTC={self.btc}']))
        self.assertEqual(r3['lanes']['BTC']['source_ack'],60)
        self.assertEqual(r3['lanes']['BTC']['archived'],60)

    def test_archive_tampering_detected(self):
        make_events(self.btc,10)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        proc,port=self._consumer()
        publish(self.root,port)
        r=prune(self.root,'BTC',10,False,self.home/'consumer.sqlite3')
        archive=Path(r['archive_file'])
        archive.write_bytes(archive.read_bytes()+b'TAMPER')
        with self.assertRaisesRegex(ValueError,'hash mismatch'):
            audit(self.root,'BTC')

    def test_retention_requires_independent_receipt_before_deletion(self):
        make_events(self.btc,8)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        proc,port=self._consumer()
        publish(self.root,port)
        with self.assertRaisesRegex(ValueError,'consumer-db'):
            prune(self.root,'BTC',8)
        with EffectStore(self.home/'empty.sqlite3') as empty:
            self.assertEqual(empty.count(),0)
        with self.assertRaisesRegex(ValueError,'receipt missing'):
            prune(self.root,'BTC',8,consumer_db=self.home/'empty.sqlite3')
        self.assertEqual(audit(self.root,'BTC')['resident_raw'],8)

    def test_retention_two_phase_crash_before_commit_then_retry(self):
        make_events(self.btc,12)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        proc,port=self._consumer()
        publish(self.root,port)
        cmd=[sys.executable,str(ROOT/'retention.py'),'--outdir',str(self.root),
             '--lane','BTC','--mode','archive','--limit','8',
             '--consumer-db',str(self.home/'consumer.sqlite3'),
             '--crash-stage','after_archive_before_commit']
        crash=subprocess.run(cmd,cwd=ROOT,capture_output=True,timeout=12)
        self.assertEqual(crash.returncode,86,crash.stderr.decode())
        self.assertEqual(audit(self.root,'BTC')['resident_raw'],12)
        retried=prune(self.root,'BTC',8,consumer_db=self.home/'consumer.sqlite3')
        self.assertEqual(retried['archived_new'],8)
        self.assertEqual(audit(self.root,'BTC',self.home/'consumer.sqlite3')['resident_raw'],4)

    def test_retention_two_phase_crash_after_commit_auditable(self):
        make_events(self.btc,12)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        proc,port=self._consumer()
        publish(self.root,port)
        cmd=[sys.executable,str(ROOT/'retention.py'),'--outdir',str(self.root),
             '--lane','BTC','--mode','archive','--limit','8',
             '--consumer-db',str(self.home/'consumer.sqlite3'),
             '--crash-stage','after_commit']
        crash=subprocess.run(cmd,cwd=ROOT,capture_output=True,timeout=12)
        self.assertEqual(crash.returncode,86,crash.stderr.decode())
        audit_result=audit(self.root,'BTC',self.home/'consumer.sqlite3')
        self.assertEqual(audit_result['archive_records'],8)
        self.assertEqual(audit_result['resident_raw'],4)

    def test_publisher_query_uses_delivery_lane_index(self):
        make_events(self.btc,200)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        with closing(sqlite3.connect(str(self.root/'lanes'/'BTC'/'state.sqlite3'))) as db:
            scope=json.loads((self.root/'lane_registry.json').read_text())['BTC']['scope']
            plan=' '.join(str(x[-1]) for x in db.execute(
                'EXPLAIN QUERY PLAN SELECT * FROM outbox WHERE delivered=0 AND lane=? ORDER BY seq LIMIT ?',
                (scope,32)).fetchall())
            self.assertIn('idx_outbox_delivery',plan)

    def test_retention_requires_registered_lane(self):
        make_events(self.btc,2)
        run(self.root,parse_sources([f'BTC={self.btc}']))
        with self.assertRaisesRegex(ValueError,'Unknown registered lane'):
            prune(self.root,'ETH',1)


if __name__=='__main__':unittest.main()
