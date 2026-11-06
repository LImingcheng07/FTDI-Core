"""Core v0.2 delivery integration and fault windows (no external network)."""
from __future__ import annotations

from contextlib import closing
from unittest import mock
from threading import Thread
from io import StringIO
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from ftdi_core.service import replay
from ftdi_core.delivery import local_lab, _await_consumer_ready
from ftdi_core.e2e import reconcile
from ftdi_core.adapters.s070041.external_delivery import EffectStore, envelope, event_identity
from ftdi_core.adapters.s070041.multi_lane import make_synthetic
from ftdi_core.adapters.s070041.durable_stream import canonical


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        self.base=Path(self.t.name)
        self.root=self.base/'state'
        self.consumer=self.base/'consumer.sqlite3'
        self.source=self.base/'btc.jsonl'

    def ingest(self,n=40):
        make_synthetic(self.source,n,None,'BTCUSDT')
        result=replay(self.root,[f'BTC={self.source}'],batch=13)
        self.assertTrue(result['complete'])

    def outbox(self):
        with closing(sqlite3.connect(self.root/'lanes/BTC/state.sqlite3')) as db:
            db.row_factory=sqlite3.Row
            return [envelope(r) for r in db.execute('SELECT * FROM outbox ORDER BY seq')]

    def test_ready_file_initially_empty_is_waited_for(self):
        """Regression: Windows saw a zero-length ready.txt before its port."""
        ready = self.base / 'empty-ready.txt'
        ready.write_text('', encoding='ascii')
        proc = mock.Mock()
        proc.poll.return_value = None
        proc.stderr = StringIO('')
        def fill():
            time.sleep(.08)
            ready.write_text('51641', encoding='ascii')
        writer = Thread(target=fill)
        writer.start()
        try:
            self.assertEqual(_await_consumer_ready(ready, proc, timeout=2), 51641)
        finally:
            writer.join(timeout=2)
            self.assertFalse(writer.is_alive())

    def test_dead_consumer_with_empty_ready_file_reports_runtime_error(self):
        ready = self.base / 'empty-dead-ready.txt'
        ready.write_text('', encoding='ascii')
        proc = mock.Mock()
        proc.poll.return_value = 7
        proc.stderr = StringIO('simulated process startup error')
        with self.assertRaisesRegex(RuntimeError, 'exit=7'):
            _await_consumer_ready(ready, proc, timeout=.1)

    def test_invalid_ready_file_times_out_with_diagnostic(self):
        ready = self.base / 'bad-ready.txt'
        ready.write_text('not-a-port', encoding='ascii')
        proc = mock.Mock()
        proc.poll.return_value = None
        proc.stderr = StringIO('')
        with self.assertRaisesRegex(RuntimeError, 'readiness deadline exceeded'):
            _await_consumer_ready(ready, proc, timeout=.07)

    def test_zero_effects_is_incomplete(self):
        self.ingest(10)
        with EffectStore(self.consumer):pass
        r=reconcile(self.root,self.consumer)
        self.assertEqual(r['status'],'PENDING_DELIVERY')
        self.assertEqual(r['metrics']['remote_missing'],10)
        self.assertEqual(r['metrics']['producer_acked'],0)

    def test_one_command_e2e_and_repeat_no_duplicate(self):
        self.ingest(58)
        a=local_lab(self.root,self.consumer,batch=11)
        self.assertTrue(a['pass'])
        self.assertEqual(a['delivery']['acknowledged'],58)
        self.assertEqual(a['audit']['metrics']['matched_effects'],58)
        b=local_lab(self.root,self.consumer,batch=11)
        self.assertTrue(b['pass'])
        self.assertEqual(b['delivery']['events_sent'],0)
        self.assertEqual(b['audit']['metrics']['consumer_effects'],58)

    def test_partial_delivery_then_resume(self):
        self.ingest(24)
        a=local_lab(self.root,self.consumer,batch=7,max_batches=1)
        self.assertEqual(a['audit']['status'],'PENDING_DELIVERY')
        self.assertEqual(a['delivery']['acknowledged'],7)
        self.assertEqual(a['audit']['metrics']['producer_acked'],7)
        b=local_lab(self.root,self.consumer,batch=7)
        self.assertTrue(b['pass'])
        self.assertEqual(b['audit']['metrics']['matched_effects'],24)

    def test_remote_commit_ack_lost_then_retries(self):
        self.ingest(17)
        with EffectStore(self.consumer) as remote:
            remote.accept(self.outbox())
        mid=reconcile(self.root,self.consumer)
        self.assertEqual(mid['status'],'RETRY_REQUIRED')
        self.assertEqual(mid['metrics']['remote_committed_unacked'],17)
        finish=local_lab(self.root,self.consumer,batch=5)
        self.assertTrue(finish['pass'])
        self.assertEqual(finish['delivery']['remote_duplicates'],17)
        self.assertEqual(finish['audit']['metrics']['consumer_effects'],17)

    def test_tampered_consumer_effect_detected(self):
        self.ingest(11)
        self.assertTrue(local_lab(self.root,self.consumer)['pass'])
        with closing(sqlite3.connect(self.consumer)) as db, db:
            db.execute("UPDATE effects SET payload='{}' WHERE seq=3")
        a=reconcile(self.root,self.consumer)
        self.assertEqual(a['status'],'INCONSISTENT')
        self.assertGreater(a['metrics']['invalid_consumer_rows'],0)

    def test_consumer_only_orphan_detected(self):
        self.ingest(11)
        self.assertTrue(local_lab(self.root,self.consumer)['pass'])
        event=self.outbox()[0]
        event['seq']=300
        event['payload']['source_seq']=300
        import hashlib
        event['payload_sha256']=hashlib.sha256(canonical(event['payload']).encode()).hexdigest()
        event['event_id']=event_identity(event['lane'],event['seq'],0,event['payload_sha256'])
        with EffectStore(self.consumer) as remote:
            remote.accept([event])
        a=reconcile(self.root,self.consumer)
        self.assertEqual(a['status'],'INCONSISTENT')
        self.assertEqual(a['metrics']['orphan_consumer_effects'],1)

    def test_forged_local_receipt_detected(self):
        self.ingest(9)
        with EffectStore(self.consumer):pass
        path=self.root/'lanes/BTC/state.sqlite3'
        with closing(sqlite3.connect(path)) as db, db:
            db.execute('UPDATE outbox SET delivered=1 WHERE seq=1')
        a=reconcile(self.root,self.consumer)
        self.assertEqual(a['status'],'INCONSISTENT')
        self.assertEqual(a['metrics']['producer_receipt_inconsistencies'],1)

    def test_two_isolated_lanes_one_consumer(self):
        eth=self.base/'eth.jsonl'
        make_synthetic(self.source,20,None,'BTCUSDT')
        make_synthetic(eth,15,None,'ETHUSDT')
        self.assertTrue(replay(self.root,[f'BTC={self.source}',f'ETH={eth}'])['complete'])
        a=local_lab(self.root,self.consumer,batch=8)
        self.assertTrue(a['pass'])
        self.assertEqual(a['audit']['metrics']['matched_effects'],35)

    def test_blocked_lane_cannot_pass_complete_audit(self):
        eth=self.base/'eth.jsonl'
        make_synthetic(self.source,20,None,'BTCUSDT')
        make_synthetic(eth,20,2,'ETHUSDT')
        self.assertFalse(replay(self.root,[f'BTC={self.source}',f'ETH={eth}'],max_pending=5)['complete'])
        a=local_lab(self.root,self.consumer,batch=8)
        self.assertFalse(a['pass'])
        self.assertFalse(a['audit']['producer_local_audit_passed'])

    def test_archived_prefix_requires_archive_aware_audit(self):
        self.ingest(5)
        self.assertTrue(local_lab(self.root,self.consumer)['pass'])
        # Simulate a completed-prune marker: the v0.2 resident-only audit
        # must NOT certify any archived prefix without replaying archives.
        with closing(sqlite3.connect(self.root/'lanes/BTC/state.sqlite3')) as db,db:
            db.execute("INSERT INTO meta(k,v) VALUES('archived_records','1')")
        r=reconcile(self.root,self.consumer)
        self.assertEqual(r['status'],'INCONSISTENT')
        self.assertEqual(r['metrics']['archived_records_unsupported'],1)

    def test_disallow_same_consumer_database(self):
        self.ingest(3)
        path=self.root/'lanes/BTC/state.sqlite3'
        with self.assertRaises(ValueError):local_lab(self.root,path)
        with self.assertRaises(ValueError):reconcile(self.root,path)

    def test_nonexistent_consumer_does_not_get_created_by_audit(self):
        self.ingest(3)
        with self.assertRaises(FileNotFoundError):reconcile(self.root,self.consumer)
        self.assertFalse(self.consumer.exists())

    def test_cli_e2e(self):
        self.ingest(12)
        c=subprocess.run([sys.executable,'-m','ftdi_core','lab-e2e','--outdir',str(self.root),
                          '--consumer-db',str(self.consumer),'--batch','5'],
                         text=True,capture_output=True,timeout=30)
        self.assertEqual(c.returncode,0,c.stderr)
        self.assertTrue(json.loads(c.stdout)['pass'])
        a=subprocess.run([sys.executable,'-m','ftdi_core','audit-e2e','--outdir',str(self.root),
                          '--consumer-db',str(self.consumer)],text=True,capture_output=True,timeout=30)
        self.assertEqual(a.returncode,0,a.stderr)
        self.assertEqual(json.loads(a.stdout)['status'],'COMPLETE')

    def test_publisher_abrupt_exit_after_consumer_commit(self):
        self.ingest(17)
        # A real independently running consumer process; publisher is killed by os._exit.
        ready=self.base/'ready.txt'
        consumer=subprocess.Popen([sys.executable,'-m','ftdi_core.adapters.s070041.consumer_service',
                                   '--db',str(self.consumer),'--port','0','--ready-file',str(ready)],
                                  stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
        try:
            port=_await_consumer_ready(ready,consumer,timeout=10)
            publisher=subprocess.run([sys.executable,'-m','ftdi_core.adapters.s070041.outbox_publisher',
                                      '--outdir',str(self.root),'--port',str(port),
                                      '--batch','20','--crash-before-ack'],
                                     capture_output=True,text=True,timeout=20)
            self.assertEqual(publisher.returncode,86,publisher.stderr)
        finally:
            if consumer.poll() is None:
                consumer.terminate();consumer.wait(timeout=5)
            if consumer.stderr:consumer.stderr.close()
        mid=reconcile(self.root,self.consumer)
        self.assertEqual(mid['status'],'RETRY_REQUIRED')
        self.assertEqual(mid['metrics']['remote_committed_unacked'],17)
        finish=local_lab(self.root,self.consumer,batch=7)
        self.assertTrue(finish['pass'])
        self.assertEqual(finish['delivery']['remote_duplicates'],17)

    def test_consumer_crash_after_db_commit_before_http_ack(self):
        self.ingest(12)
        ready=self.base/'ready.txt'
        consumer=subprocess.Popen([sys.executable,'-m','ftdi_core.adapters.s070041.consumer_service',
                                   '--db',str(self.consumer),'--port','0','--ready-file',str(ready),
                                   '--crash-after-commit'],
                                  stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
        try:
            port=_await_consumer_ready(ready,consumer,timeout=10)
            publisher=subprocess.run([sys.executable,'-m','ftdi_core.adapters.s070041.outbox_publisher',
                                      '--outdir',str(self.root),'--port',str(port),
                                      '--batch','20'],capture_output=True,text=True,timeout=20)
            self.assertNotEqual(publisher.returncode,0)
            consumer.wait(timeout=5)
            self.assertEqual(consumer.returncode,86)
        finally:
            if consumer.poll() is None:
                consumer.terminate();consumer.wait(timeout=5)
            if consumer.stderr:consumer.stderr.close()
        mid=reconcile(self.root,self.consumer)
        self.assertEqual(mid['status'],'RETRY_REQUIRED')
        self.assertEqual(mid['metrics']['remote_committed_unacked'],12)
        done=local_lab(self.root,self.consumer,batch=8)
        self.assertTrue(done['pass'])
        self.assertEqual(done['delivery']['remote_duplicates'],12)

if __name__=='__main__':unittest.main()
