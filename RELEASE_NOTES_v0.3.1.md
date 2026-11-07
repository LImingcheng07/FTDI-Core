# FTDI-Core v0.3.1 — release notes

Date: 2026-11-07. Patch release on top of v0.3.0 (`RELEASE_NOTES_v0.3.0.md`). Platform verified: native
Windows 11 Pro, Python 3.11.9, SQLite 3.45.1. Status: **local research prototype; not production certified;
not a trading program.** Stdlib only, no outbound network.

## New: `vacuum-db` — reclaim disk space after retention (ADR-010)

```powershell
python -m ftdi_core vacuum-db --db C:\ftdi\consumer\effects.sqlite3            # dry run
python -m ftdi_core vacuum-db --db C:\ftdi\consumer\effects.sqlite3 --apply    # rebuild
```

Retention deletes rows but SQLite only reuses the freed pages, so a compacted consumer DB stayed at its full
size. `vacuum-db` holds an exclusive SQLite lock for the whole operation (any other open connection, even idle, is
a refusal), makes a byte copy `<db>.pre-vacuum-<UTC timestamp>` and verifies it (SHA-256 re-read and the logical
digest: schema, indexes, triggers and every row of every table), and only then runs SQLite's transactional
`VACUUM` in place and re-verifies integrity, settings and the digest. The database path never disappears; **the
copy is never deleted by FTDI** — the owner removes it after verifying. Links, junctions, hard links and foreign
sidecar files are refused before any write. Contract: `docs/STORAGE_MAINTENANCE.md`.

An independent review of a first rename-based design (9 findings, incl. one critical: a checkpoint through a
hard-linked `-wal` could truncate the only original) led to this in-place design. The rename windows (path
missing, late commit lost, foreign temp file deleted) no longer exist by construction; the other findings have
regression tests that fail on the first design (mutation-checked).

## Changed

* `scripts/acceptance_full_day.py` reclaims space with `vacuum-db` (dry run and apply on a copy of the compacted
  consumer) and audits the result, instead of a manual VACUUM on a backup copy.

## Compatibility

No schema, contract or format change. v0.3.0 data are used as-is; a vacuumed database is an ordinary SQLite file
with identical content (rowids of tables without an INTEGER PRIMARY KEY may be renumbered; FTDI does not use them).
Rollback: stop all programs using the database and copy the kept file back over it.

## Verification

See `docs/RELEASE_GATES_V03.md` (section v0.3.1).
