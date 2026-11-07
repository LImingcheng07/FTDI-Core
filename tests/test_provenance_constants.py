"""Provenance constants must not drift: every maintained copy of the original ZIP SHA-256 must
be the full 64-hex value verified against the owner's archive and companion checksum file."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
ZIP_SHA256 = '78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9'
# Frozen baseline evidence and owner-provided historical notes are kept byte-identical on purpose;
# their truncated value is documented in docs/SOURCE_PROVENANCE.md.
FROZEN = {'PACKAGE_MANIFEST.json', 'DEVKIT_MANIFEST.json', 'FTDI_CLAUDE_CODE_HANDOFF.md'}
TOKEN = re.compile(r'78085fc5[0-9a-f]*')


class ProvenanceConstantTests(unittest.TestCase):
    def test_every_maintained_zip_sha256_is_complete_and_identical(self):
        self.assertRegex(ZIP_SHA256, r'^[0-9a-f]{64}$')
        found = 0
        for path in sorted(ROOT.rglob('*')):
            rel = path.relative_to(ROOT)
            if (not path.is_file() or path.suffix not in {'.md', '.py', '.json', '.txt'}
                    or rel.parts[0] in {'.git', 'runs', 'data', 'release'} or '__pycache__' in rel.parts
                    or rel.as_posix() in FROZEN or rel.parts[:2] == ('docs', 'handoff')
                    or path.name == Path(__file__).name):
                continue
            text = path.read_text(encoding='utf-8', errors='replace')
            if rel.as_posix() == 'docs/SOURCE_PROVENANCE.md':
                text = text.replace('truncated 62-character value', '')
            for token in TOKEN.findall(text):
                found += 1
                self.assertEqual(token, ZIP_SHA256, f'{rel}: {token} ({len(token)} chars)')
        self.assertGreaterEqual(found, 5)


if __name__ == '__main__':
    unittest.main()
