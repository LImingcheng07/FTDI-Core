import json
from pathlib import Path
import sqlite3
import tempfile
from contextlib import closing
import unittest
from ftdi_core.service import replay
from ftdi_core.audit import audit
from ftdi_core.adapters.s070041.multi_lane import make_synthetic
from ftdi_core.adapters.s070041.external_delivery import EffectStore,envelope,Conflict,event_identity
from ftdi_core.adapters.s070041.outbox_publisher import commit_ack
from ftdi_core.adapters.s070041.durable_stream import canonical


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.state=self.root/'state';self.btc=self.root/'btc.jsonl';self.eth=self.root/'eth.jsonl'

    def test_replay_and_integrity_audit(self):
        make_synthetic(self.btc,75,None,'BTCUSDT')
        r=replay(self.state,[f'BTC={self.btc}'])
        self.assertTrue(r['complete'])
        self.assertEqual(r['lanes']['BTC']['outbox_total'],75)
        self.assertTrue(audit(self.state)['passed'])
    def test_replay_resume_no_duplicates(self):
        make_synthetic(self.btc,150,None,'BTCUSDT')
        a=replay(self.state,[f'BTC={self.btc}'],batch=10,max_rounds=2)
        self.assertFalse(a['complete'])
        b=replay(self.state,[f'BTC={self.btc}'],batch=10)
        self.assertTrue(b['complete'])
        self.assertEqual(b['lanes']['BTC']['outbox_total'],150)
        c=replay(self.state,[f'BTC={self.btc}'],batch=10)
        self.assertEqual(c['lanes']['BTC']['outbox_total'],150)
    def test_lane_gap_isolated_from_good_lane(self):
        make_synthetic(self.btc,250,None,'BTCUSDT')
        make_synthetic(self.eth,250,2,'ETHUSDT')
        r=replay(self.state,[f'BTC={self.btc}',f'ETH={self.eth}'],batch=10,max_pending=7)
        self.assertFalse(r['complete'])
        self.assertEqual(r['lanes']['BTC']['outbox_total'],250)
        self.assertEqual(r['lanes']['ETH']['pending'],7)
        self.assertEqual(r['lanes']['ETH']['outbox_total'],1)
        self.assertEqual(r['lanes']['ETH']['integrity'],'WAITING_FOR_MISSING')
        self.assertFalse(audit(self.state)['passed'])
    def test_source_modification_rejected(self):
        make_synthetic(self.btc,20,None,'BTCUSDT')
        replay(self.state,[f'BTC={self.btc}'])
        self.btc.write_text(self.btc.read_text()+'\n',encoding='utf-8')
        with self.assertRaises(ValueError):replay(self.state,[f'BTC={self.btc}'])
    def test_duplicate_scope_rejected(self):
        make_synthetic(self.btc,10,None,'BTCUSDT')
        with self.assertRaises(ValueError):replay(self.state,[f'A={self.btc}',f'B={self.btc}'])
    def test_conflicting_lane_registry_rejected(self):
        make_synthetic(self.btc,10,None,'BTCUSDT')
        make_synthetic(self.eth,10,None,'ETHUSDT')
        replay(self.state,[f'BTC={self.btc}'])
        with self.assertRaises(ValueError):replay(self.state,[f'BTC={self.btc}',f'ETH={self.eth}'])
    def test_audit_detects_raw_tampering(self):
        make_synthetic(self.btc,10,None,'BTCUSDT')
        replay(self.state,[f'BTC={self.btc}'])
        with closing(sqlite3.connect(self.state/'lanes/BTC/state.sqlite3')) as db, db:
            db.execute("UPDATE raw_journal SET payload='{}' WHERE file_index=0")
        x=audit(self.state)
        self.assertFalse(x['passed'])
        self.assertFalse(x['lanes']['BTC']['checks']['journal_checksum'])
    def test_audit_detects_outbox_tampering(self):
        make_synthetic(self.btc,10,None,'BTCUSDT')
        replay(self.state,[f'BTC={self.btc}'])
        with closing(sqlite3.connect(self.state/'lanes/BTC/state.sqlite3')) as db, db:
            db.execute("UPDATE outbox SET payload='{}' WHERE seq=1")
        x=audit(self.state)
        self.assertFalse(x['passed'])
        self.assertFalse(x['lanes']['BTC']['checks']['outbox_checksum'])
    def test_external_store_idempotent_ack(self):
        make_synthetic(self.btc,5,None,'BTCUSDT')
        replay(self.state,[f'BTC={self.btc}'])
        path=self.state/'lanes/BTC/state.sqlite3'
        with closing(sqlite3.connect(path)) as db, db:
            db.row_factory=sqlite3.Row
            rows=db.execute('SELECT * FROM outbox ORDER BY seq').fetchall()
            evs=[envelope(r) for r in rows]
            with EffectStore(self.root/'consumer.sqlite3') as sink:
                a=sink.accept(evs)
                b=sink.accept(evs)  # Response lost, producer safely resends.
                self.assertEqual((a['inserted'],b['duplicates'],sink.count()),(5,5,5))
            commit_ack(db,rows)
        with closing(sqlite3.connect(path)) as check, check:
            self.assertEqual(check.execute('SELECT COUNT(*) FROM delivery').fetchone()[0],5)
    def test_external_conflict_rejected(self):
        make_synthetic(self.btc,2,None,'BTCUSDT')
        replay(self.state,[f'BTC={self.btc}'])
        with closing(sqlite3.connect(self.state/'lanes/BTC/state.sqlite3')) as db, db:
            db.row_factory=sqlite3.Row
            ev=envelope(db.execute('SELECT * FROM outbox ORDER BY seq LIMIT 1').fetchone())
        with EffectStore(self.root/'consumer.sqlite3') as sink:
            sink.accept([ev])
            import copy,hashlib
            malicious=copy.deepcopy(ev)
            malicious['payload']['price']='99999'
            malicious['payload_sha256']=hashlib.sha256(canonical(malicious['payload']).encode()).hexdigest()
            malicious['event_id']=event_identity(ev['lane'],ev['seq'],ev['epoch'],malicious['payload_sha256'])
            with self.assertRaises(Conflict):sink.accept([malicious])
            self.assertEqual(sink.count(),1)
    def test_missing_lane_fails(self):
        with self.assertRaises(ValueError):replay(self.state,[])
