import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zipfile
from ftdi_core.preparation import parse_csv_row,prepare_archive,sha256_file


def csvrow(i):
    return f'{i},69000.12,0.001,100,101,1717200000000000,false\n'


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.archive=self.root/'BTCUSDT-aggTrades-2024-06-01.zip'

    def write_zip(self,rows=None,member='BTCUSDT-aggTrades-2024-06-01.csv'):
        if rows is None:rows=[csvrow(i) for i in range(1,5)]
        with zipfile.ZipFile(self.archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr(member,''.join(rows))
        digest=sha256_file(self.archive)
        checksum=self.root/'check.sha256'
        checksum.write_text(digest+'  '+self.archive.name+'\n',encoding='utf-8')
        return checksum

    def test_prepare_with_independent_checksum(self):
        ck=self.write_zip()
        result=prepare_archive(self.archive,self.root/'out',checksum=ck)
        self.assertEqual(result['records'],4)
        self.assertTrue(result['independent_checksum_provided'])
        self.assertEqual(result['source_seq_gaps'],0)
        out=Path(result['output']);es=[json.loads(x) for x in out.read_text().splitlines()]
        self.assertEqual(es[0]['source_seq'],1)
        self.assertEqual(es[0]['event_time_native_unit'],'us')
        self.assertEqual(es[0]['event_time_ms'],1717200000000)
        self.assertEqual(es[0]['first_trade_id'],100)
        self.assertFalse(es[0]['is_buyer_maker'])
        self.assertEqual(sha256_file(out),result['jsonl_sha256'])
    def test_eighth_csv_boolean_preserved(self):
        self.write_zip([csvrow(1).strip() + ',True\n'])
        result=prepare_archive(self.archive,self.root/'out')
        e=json.loads(Path(result['output']).read_text().strip())
        self.assertIs(e['is_best_match'],True)

    def test_invalid_eighth_boolean_rejected(self):
        self.write_zip([csvrow(1).strip() + ',invalid\n'])
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')

    def test_limit(self):
        self.write_zip()
        self.assertEqual(prepare_archive(self.archive,self.root/'out',limit=2)['records'],2)
    def test_full_limit_zero(self):
        self.write_zip()
        self.assertEqual(prepare_archive(self.archive,self.root/'out',limit=0)['records'],4)
    def test_checksum_mismatch_is_fail_closed(self):
        ck=self.write_zip()
        ck.write_text('0'*64+'  '+self.archive.name)
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out',checksum=ck)
    def test_wrong_expected_sha256(self):
        self.write_zip()
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out',expected_sha256='f'*64)
    def test_refuses_overwrite(self):
        self.write_zip()
        prepare_archive(self.archive,self.root/'out')
        with self.assertRaises(FileExistsError):prepare_archive(self.archive,self.root/'out')
    def test_detects_but_does_not_hide_missing_ids(self):
        self.write_zip([csvrow(1),csvrow(3),csvrow(7)])
        result=prepare_archive(self.archive,self.root/'out')
        self.assertEqual(result['source_seq_gaps'],4)
        self.assertFalse(result['contiguous_in_file'])
    def test_duplicate_input_id_rejected(self):
        self.write_zip([csvrow(1),csvrow(1)])
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')
        self.assertFalse((self.root/'out'/'source_manifest.json').exists())
    def test_malformed_csv_rejected(self):
        self.write_zip(['not,an,aggTrade\n'])
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')
    def test_nested_member_rejected(self):
        self.write_zip(member='subdir/trades.csv')
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')
    def test_too_many_csv_members_rejected(self):
        self.write_zip()
        with zipfile.ZipFile(self.archive,'a') as z:z.writestr('extra.csv',csvrow(9))
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')
    def test_invalid_symbol_rejected(self):
        self.write_zip()
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out',symbol='btc-usdt')
    def test_symbol_mismatch_filename_rejected(self):
        self.write_zip()
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out',symbol='ETHUSDT')

    def test_blank_line_rejected(self):
        self.write_zip([csvrow(1),'\n',csvrow(2)])
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')

    def test_repeated_header_rejected(self):
        self.write_zip([csvrow(1),'a,p,q,f,l,T,m\n'])
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')

    def test_negative_limit_rejected(self):
        self.write_zip()
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out',limit=-5)
    def test_zero_price_rejected(self):
        e=csvrow(1).split(',');e[1]='0'
        self.write_zip([','.join(e)])
        with self.assertRaises(ValueError):prepare_archive(self.archive,self.root/'out')
    def test_header_is_ignored(self):
        self.write_zip(['a,p,q,f,l,T,m\n',csvrow(1)])
        self.assertEqual(prepare_archive(self.archive,self.root/'out')['records'],1)
