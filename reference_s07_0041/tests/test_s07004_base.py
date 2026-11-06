from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from durable_stream import StreamDB, CrashPoint, canonical, valid, STAGES
from run_stream import acceptance, verify_manifest, make_demo


def ev(i, **overrides):
    d={'source':'test','venue':'TEST','symbol':'BTCUSDT','channel':'aggTrades',
       'source_seq':i,'event_time_ms':1700000000000+i,'price':'123.50','qty':'0.1'}
    d.update(overrides)
    return d


def write_fixture(path, events):
    with Path(path).open('w',encoding='utf-8') as f:
        for x in events:
            f.write(json.dumps(x,ensure_ascii=False,separators=(',',':'))+'\n')


class DurableStreamTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder=Path(self.tmp.name)
        self.source=self.folder/'source.jsonl'
        self.db=self.folder/'stream.sqlite3'

    def open_db(self, **kw):
        return StreamDB(self.db,self.source,**kw)

    def test_schema_validation(self):
        self.assertTrue(valid(ev(1)))
        self.assertFalse(valid(ev(1,price='NaN')))
        self.assertFalse(valid(ev(1,qty='-1')))
        self.assertFalse(valid(ev(True)))
        self.assertFalse(valid(ev(1,event_time_ms='123')))
        self.assertFalse(valid(ev(1,source='')))

    def test_ordinary_exactly_once_internal(self):
        write_fixture(self.source,[ev(x) for x in range(1,1001)])
        with self.open_db() as s:
            r=s.pump(batch=17)
            self.assertEqual((r['raw_journal_records'],r['delivery_records']),(1000,1000))
            self.assertTrue(all(acceptance(r).values()))
            firsthash=r['delivery_logical_sha256']
        with self.open_db() as s:
            r=s.pump(batch=31)
            self.assertEqual(r['delivery_logical_sha256'],firsthash)
            self.assertEqual(r['delivery_records'],1000)

    def test_resume_after_partial_cycles(self):
        write_fixture(self.source,[ev(x) for x in range(1,301)])
        with self.open_db() as s:
            r=s.pump(batch=11,max_cycles=4)
            self.assertFalse(r['source_file_exhausted'])
        with self.open_db() as s:
            r=s.pump(batch=37)
            self.assertTrue(all(acceptance(r).values()))
            self.assertEqual(r['delivery_records'],300)

    def test_reorder_without_oracle(self):
        write_fixture(self.source,[ev(x) for x in (1,3,5,2,4,6)])
        with self.open_db() as s:
            r=s.pump(batch=2)
            self.assertEqual(r['delivery_records'],6)
            self.assertTrue(all(acceptance(r).values()))
            seqs=[r[0] for r in s.db.execute('SELECT seq FROM delivery ORDER BY seq')]
            self.assertEqual(seqs,list(range(1,7)))

    def test_gap_blocks_output_fail_closed(self):
        write_fixture(self.source,[ev(x) for x in (1,3,4,5)])
        with self.open_db() as s:
            r=s.pump(batch=2)
            self.assertEqual(r['delivery_records'],1)
            self.assertEqual(r['pending'],3)
            self.assertEqual(r['lanes'][0]['next_expected_seq'],2)
            self.assertEqual(r['lanes'][0]['integrity'],'WAITING_FOR_MISSING')
            self.assertFalse(all(acceptance(r).values()))

    def test_full_pending_stops_ack_instead_of_skipping(self):
        write_fixture(self.source,[ev(x) for x in (1,3,4,2,5)])
        with self.open_db(max_pending=2,max_backlog=2) as s:
            r=s.pump(batch=1)
            self.assertEqual(r['delivery_records'],1)
            self.assertEqual(r['pending'],2)
            self.assertLess(r['source_records_acknowledged'],5)
            self.assertEqual(r['lanes'][0]['integrity'],'WAITING_FOR_MISSING')
            self.assertFalse(all(acceptance(r).values()))

    def test_sufficient_pending_drains_on_expected(self):
        write_fixture(self.source,[ev(x) for x in (1,3,4,2,5)])
        with self.open_db(max_pending=3,max_backlog=3) as s:
            r=s.pump(batch=1)
            self.assertEqual(r['delivery_records'],5)
            self.assertEqual(r['pending'],0)
            self.assertTrue(all(acceptance(r).values()))

    def test_duplicate_equal(self):
        write_fixture(self.source,[ev(x) for x in (1,2,2,3)])
        with self.open_db() as s:
            r=s.pump()
            self.assertEqual(r['delivery_records'],3)
            self.assertEqual(r['dispositions'].get('DUPLICATE_IDENTICAL'),1)
            self.assertTrue(all(acceptance(r).values()))

    def test_conflict_fails_closed(self):
        write_fixture(self.source,[ev(1),ev(2),ev(2,price='999'),ev(3)])
        with self.open_db() as s:
            r=s.pump()
            self.assertEqual(r['delivery_records'],2)
            self.assertEqual(r['lanes'][0]['integrity'],'CONFLICT')
            self.assertGreater(r['quarantine_records'],0)
            self.assertEqual(r['dispositions'].get('QUARANTINED_CONFLICT'),1)

    def test_invalid_quarantined(self):
        write_fixture(self.source,[ev(1),ev(2,price='-1'),ev(3)])
        with self.open_db() as s:
            r=s.pump()
            self.assertEqual(r['quarantine_records'],1)
            self.assertEqual(r['pending'],1)
            self.assertEqual(r['delivery_records'],1)

    def test_independent_lanes(self):
        write_fixture(self.source,[ev(1),ev(3),ev(1,symbol='ETHUSDT'),ev(2,symbol='ETHUSDT')])
        with self.open_db() as s:
            r=s.pump()
            self.assertEqual(r['delivery_records'],3)
            self.assertEqual(len(r['lanes']),2)
            self.assertEqual({x['scope'][1]:x['integrity'] for x in r['lanes']},
                             {'BTCUSDT':'WAITING_FOR_MISSING','ETHUSDT':'CONTIGUOUS'})

    def test_demo_count_change_rejected(self):
        make_demo(self.source,12)
        with self.assertRaisesRegex(ValueError,'Existing demo has'):
            make_demo(self.source,13)

    def test_source_change_rejected(self):
        write_fixture(self.source,[ev(1)])
        with self.open_db() as s: s.pump()
        write_fixture(self.source,[ev(1),ev(2)])
        with self.assertRaisesRegex(ValueError,'Source or schema mismatch'):
            self.open_db()

    def test_failed_init_releases_sqlite_file(self):
        # On Windows unlink() fails with WinError 32 when __init__ leaks a
        # sqlite3.Connection. A failed constructor must not require GC to close.
        write_fixture(self.source, [ev(1)])
        with self.open_db() as s:
            s.pump()
        write_fixture(self.source, [ev(1), ev(2)])
        with self.assertRaisesRegex(ValueError, 'Source or schema mismatch'):
            self.open_db()
        self.db.unlink()
        self.assertFalse(self.db.exists())

    def test_missing_source_during_init_releases_sqlite_file(self):
        # Missing source also fails after opening the SQLite connection.
        with self.assertRaises(FileNotFoundError):
            self.open_db()
        self.db.unlink()
        self.assertFalse(self.db.exists())

    def test_manifest_guard(self):
        write_fixture(self.source,[ev(1)])
        p=self.folder/'manifest.json'
        p.write_text(json.dumps({'official_checksum_verified':False,'data_jsonl_sha256':'bad'}))
        with self.assertRaises(ValueError):verify_manifest(self.source,p)
        p.write_text(json.dumps({'official_checksum_verified':True,'data_jsonl_sha256':'bad'}))
        with self.assertRaises(ValueError):verify_manifest(self.source,p)
        p.write_text(json.dumps({'official_checksum_verified':True,'data_jsonl_sha256':hashlib.sha256(self.source.read_bytes()).hexdigest()}))
        self.assertTrue(verify_manifest(self.source,p)['official_archive_checksum_inherited'])

    def test_synthetic_fixture_deterministic(self):
        make_demo(self.source,200)
        digest=hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.source.unlink()
        make_demo(self.source,200)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(),digest)

    def test_bounded_backlog_and_resume(self):
        write_fixture(self.source,[ev(x) for x in range(1,51)])
        with self.open_db(max_backlog=2,max_pending=3) as s:
            r=s.pump(batch=10)
            self.assertEqual(r['delivery_records'],50)
            self.assertTrue(all(acceptance(r).values()))

    def test_source_offset_matches_bytes(self):
        write_fixture(self.source,[ev(x) for x in range(1,21)])
        with self.open_db() as s:
            s.ingest_batch(7)
            pos=int(s.get_meta('next_byte_offset'))
            with self.source.open('rb') as f:
                f.seek(pos)
                self.assertEqual(json.loads(f.readline())['source_seq'],8)
            self.assertEqual(s.get_meta('next_file_index'),'7')
            self.assertEqual(s.count('raw_journal'),7)

    def test_transaction_raw_rollback_on_before_commit(self):
        # Same crash as process matrix, tested in subprocess to preserve os._exit.
        write_fixture(self.source,[ev(x) for x in range(1,41)])
        code='''from durable_stream import StreamDB,CrashPoint\nwith StreamDB(r"{}",r"{}",crash=CrashPoint("before_raw_commit")) as s:s.pump(batch=5)'''.format(self.db,self.source)
        p=subprocess.run([sys.executable,'-c',code],cwd=ROOT,capture_output=True)
        self.assertEqual(p.returncode,86,p.stderr.decode())
        with self.open_db() as s:
            self.assertEqual(s.count('raw_journal'),0)
            r=s.pump(batch=6)
            self.assertEqual(r['delivery_records'],40)

    def test_abrupt_crash_matrix(self):
        write_fixture(self.source,[ev(x) for x in range(1,221)])
        baseline=None
        for stage in STAGES:
            location=self.folder/stage
            location.mkdir()
            db=location/'journal.sqlite3'
            code=('from durable_stream import StreamDB,CrashPoint\n'
                  'with StreamDB(r"{}",r"{}",crash=CrashPoint("{}",2)) as s:s.pump(batch=13)'
                 ).format(db,self.source,stage)
            proc=subprocess.run([sys.executable,'-c',code],cwd=ROOT,capture_output=True)
            self.assertEqual(proc.returncode,86,f'{stage}: {proc.stderr.decode()}')
            with StreamDB(db,self.source) as s:
                r=s.pump(batch=17)
                self.assertTrue(all(acceptance(r).values()),f'{stage}: {r}')
                self.assertEqual(r['delivery_records'],220)
                if baseline is None: baseline=r['delivery_logical_sha256']
                self.assertEqual(r['delivery_logical_sha256'],baseline,f'{stage}: logical output changed')
                self.assertEqual(s.count('disposition'),220)

    def test_empty_file(self):
        self.source.write_text('')
        with self.open_db() as s:
            r=s.pump()
            self.assertEqual(r['delivery_records'],0)
            self.assertTrue(r['source_file_exhausted'])

    def test_bad_json_prevents_checkpoint(self):
        self.source.write_text(json.dumps(ev(1))+'\nBAD JSON\n',encoding='utf-8')
        with self.open_db() as s:
            with self.assertRaisesRegex(ValueError,'Malformed source line'):
                s.ingest_batch(10)
            self.assertEqual(s.count('raw_journal'),0)
            self.assertEqual(s.get_meta('next_file_index'),'0')

    def test_pending_duplicate_identical_before_gap_resolution(self):
        write_fixture(self.source,[ev(x) for x in (1,3,3,2,4)])
        with self.open_db() as s:
            r=s.pump(batch=1)
            self.assertEqual(r['delivery_records'],4)
            self.assertEqual(r['dispositions']['DUPLICATE_IDENTICAL'],1)
            self.assertTrue(all(acceptance(r).values()))

    def test_internal_delivery_ack_atomic_against_crashes(self):
        write_fixture(self.source,[ev(x) for x in range(1,30)])
        with self.open_db() as s:
            s.ingest_batch(29)
            s.process_batch(29)
            self.assertEqual(s.count('outbox','delivered=0'),29)
            self.assertEqual(s.count('delivery'),0)
        with self.open_db() as s:
            s.deliver_batch(11)
            s.deliver_batch(11)
            s.deliver_batch(11)
            self.assertEqual(s.count('delivery'),29)
            self.assertEqual(s.count('outbox','delivered=0'),0)
            s.deliver_batch(29)
            self.assertEqual(s.count('delivery'),29)

    def test_integrity_audit_detects_journal_tampering(self):
        write_fixture(self.source,[ev(x) for x in range(1,5)])
        with self.open_db() as s:
            s.pump()
            s.db.execute("UPDATE raw_journal SET payload=? WHERE file_index=0", ('{}',))
            s.db.commit()
            r=s.report()
            self.assertEqual(r['raw_journal_checksum_mismatches'],1)
            self.assertFalse(all(acceptance(r).values()))

    def test_integrity_audit_detects_outbox_tampering(self):
        write_fixture(self.source,[ev(x) for x in range(1,5)])
        with self.open_db() as s:
            s.pump()
            s.db.execute("UPDATE outbox SET payload=? WHERE seq=1", ('{}',))
            s.db.commit()
            r=s.report()
            self.assertEqual(r['outbox_content_mismatches'],1)
            self.assertFalse(all(acceptance(r).values()))

    def test_verified_source_jsonl_is_streamed(self):
        write_fixture(self.source,[ev(x) for x in range(1,111)])
        manifest=self.folder/'manifest.json'
        manifest.write_text(json.dumps({'official_checksum_verified':True,
                                         'data_jsonl_sha256':hashlib.sha256(self.source.read_bytes()).hexdigest()}))
        self.assertTrue(verify_manifest(self.source,manifest)['official_archive_checksum_inherited'])
        with self.open_db() as s:
            r=s.pump(batch=3)
            self.assertEqual(r['source_byte_offset'],self.source.stat().st_size)
            self.assertEqual(r['delivery_records'],110)

if __name__=='__main__': unittest.main()
