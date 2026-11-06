# FTDI-Core v0.2 Windows consumer readiness hotfix

## Observed failure

`test_remote_commit_ack_lost_then_retries` raised
`ValueError: invalid literal for int() with base 10: ''` in
`ftdi_core/delivery.py` when the parent saw `ready.txt` before the child
finished writing its ephemeral HTTP port.

## Changes

- Both consumer service entrypoints write the port to a separate temporary
  file and use `os.replace` after closing the file. Readers never observe an
  in-progress write from the patched consumer.
- Core's `local_lab` waits for a valid complete numeric port (not just file
  existence), checks process exit, and reports useful timeout errors.
- The S07 compatibility run_lab gets the same defensive wait.
- Core crash tests use the shared readiness helper; three additional regression
  tests check empty file, terminated child, and invalid-file timeout.
- No event processing, recovery, DB schema, or archival format is changed.

## On Windows (existing project)

Option A (recommended): use the small HOTFIX ZIP. Extract its contents
**into the existing FTDI-ClaudeCode-DevKit-v0.2 project directory** with
"Replace the files in the destination" enabled. Preserve your own uncommitted
edits by checking `git status` and `git diff` first.

Option B: extract the full Windows-fixed ZIP into a new directory and
continue work there (do not overwrite your live runs or existing databases).

Run in project root:

```powershell
python -W error::ResourceWarning -m unittest discover -s tests -q
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Verify-FTDI.ps1
```

Expected: **65 Core + 45 compatibility = 110 tests**.
Current automated verification was on Linux; the specific Windows timing
issue was reported by a user and needs native Windows confirmation.
This patch is source-only: it does not touch existing SQLite data files.

## Release manifest note

`PACKAGE_MANIFEST.json` and `DEVKIT_MANIFEST.json` are rebuilt for this
patched distribution, and original source hashes from the earlier v0.2
archive remain available in the earlier ZIP. This patch is not claimed to be
byte-for-byte equivalent to the unpatched v0.2 release.
