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

    def test_hash_pinned_text_files_have_lf_line_endings(self):
        """Manifests pin raw bytes: a CRLF working copy (e.g. from Path.write_text on Windows) would
        not match what a clean checkout produces under the eol=lf rules in .gitattributes."""
        offenders = []
        for path in sorted(ROOT.rglob('*')):
            rel = path.relative_to(ROOT)
            if (path.is_file() and path.suffix in {'.json', '.toml', '.txt', '.md', '.py'}
                    and rel.parts[0] not in {'.git', 'runs', 'data'} and '__pycache__' not in rel.parts
                    and b'\r\n' in path.read_bytes()):
                offenders.append(rel.as_posix())
        self.assertEqual(offenders, [])


if __name__ == '__main__':
    unittest.main()
