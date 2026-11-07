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
size. `vacuum-db` rebuilds one producer lane DB or consumer DB with `VACUUM INTO`, proves that the rebuilt file
has exactly the same logical content (schema, indexes, triggers and every row of every table — a
layout-independent digest), checks integrity, and swaps it in by renames that are undone on any failure. **The
original file is never deleted**: it is kept as `<db>.pre-vacuum-<UTC timestamp>`; the owner removes it after
verifying. Refused when the database is in use, a rollback journal is present, space is insufficient, the file is
not an FTDI database, or it changed during the rebuild. Contract: `docs/STORAGE_MAINTENANCE.md`.

## Changed

* `scripts/acceptance_full_day.py` reclaims space with `vacuum-db` (dry run and apply on a copy of the compacted
  consumer) and audits the result, instead of a manual VACUUM on a backup copy.

## Compatibility

No schema, contract or format change. v0.3.0 data are used as-is; a vacuumed database is an ordinary SQLite file
with identical content (rowids of tables without an INTEGER PRIMARY KEY may be renumbered; FTDI does not use them).
Rollback: rename the kept original back to the database name.

## Verification

See `docs/RELEASE_GATES_V03.md` (section v0.3.1).
