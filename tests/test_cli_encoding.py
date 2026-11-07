"""CLI output must survive non-ASCII paths on Windows consoles/pipes (GBK code page).

Regression: with ``ensure_ascii=False`` a path character outside the console encoding made
``print`` raise UnicodeEncodeError after the work had finished, turning success into exit 3.
"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import test_timeprov  # module import: avoids re-discovering its TestCase classes here

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CliEncodingTests(unittest.TestCase):
    def test_non_gbk_path_characters_do_not_break_cli_output(self):
        with tempfile.TemporaryDirectory() as t:
            directory = Path(t) / 'run_量化_\U0001F600'
            directory.mkdir()
            db = test_timeprov.ClockAndPilotTests.pilot_db(None, directory)
            done = subprocess.run([sys.executable, '-m', 'ftdi_core', 'analyze-clock-evidence', '--pilot-db', str(db)],
                                  cwd=PROJECT_ROOT, capture_output=True, timeout=60)
            self.assertEqual(done.returncode, 0, done.stderr)
            text = done.stdout.decode('ascii')                     # ASCII-only by contract
            self.assertIn('\U0001F600', json.loads(text)['source'])
            missing = subprocess.run([sys.executable, '-m', 'ftdi_core', 'analyze-clock-evidence', '--pilot-db',
                                      str(directory / 'nope.sqlite3')], cwd=PROJECT_ROOT, capture_output=True, timeout=60)
            self.assertEqual(missing.returncode, 3)
            self.assertIn('FTDI_CORE_FAIL_CLOSED', missing.stderr.decode('ascii'))


if __name__ == '__main__':
    unittest.main()
