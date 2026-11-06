import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from ftdi_core.adapters.s070041.multi_lane import make_synthetic

class CLITests(unittest.TestCase):
    def test_cli_replay_and_audit(self):
        with tempfile.TemporaryDirectory() as t:
            d=Path(t);src=d/'events.jsonl';state=d/'state'
            make_synthetic(src,15)
            c=subprocess.run([sys.executable,'-m','ftdi_core','replay','--lane',f'BTC={src}',
                              '--outdir',str(state)],capture_output=True,text=True,timeout=20)
            self.assertEqual(c.returncode,0,c.stderr)
            d1=json.loads(c.stdout);self.assertTrue(d1['complete'])
            q=subprocess.run([sys.executable,'-m','ftdi_core','audit','--outdir',str(state)],
                             capture_output=True,text=True,timeout=20)
            self.assertEqual(q.returncode,0,q.stderr)
            self.assertTrue(json.loads(q.stdout)['passed'])
    def test_cli_incomplete_returns_2(self):
        with tempfile.TemporaryDirectory() as t:
            d=Path(t);src=d/'events.jsonl';state=d/'state'
            make_synthetic(src,50,2)
            p=subprocess.run([sys.executable,'-m','ftdi_core','replay','--lane',f'ETH={src}',
                              '--outdir',str(state),'--max-pending','5'],
                             capture_output=True,text=True,timeout=20)
            self.assertEqual(p.returncode,2,p.stderr)
    def test_cli_bad_input_returns_3(self):
        with tempfile.TemporaryDirectory() as t:
            p=subprocess.run([sys.executable,'-m','ftdi_core','replay','--lane','A=missing.jsonl',
                              '--outdir',t],capture_output=True,text=True,timeout=20)
            self.assertEqual(p.returncode,3,p.stderr)
