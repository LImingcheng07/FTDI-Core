# Storage maintenance — `vacuum-db` contract (v0.3.1)

Status: contract for implementation and black-box tests. ADR-010.

## Purpose

Retention (`archive-producer`, `consumer-retention compact`) deletes rows, but SQLite only reuses the freed
pages; the file shrinks only after a VACUUM. `vacuum-db` rebuilds one FTDI database file **without changing a
single logical row**, proves that, and **never deletes the original**: the original file is kept next to the
new one under a unique name, and the owner decides when to delete it.

## Command

```
python -m ftdi_core vacuum-db --db PATH [--apply]
```

Exit codes: 0 = dry run or success; 3 = refused / failed closed (`FTDI_CORE_FAIL_CLOSED: ...` on stderr, nothing
changed except what the error message states). JSON on stdout (ASCII only).

Python API: `ftdi_core.storage_maintenance.vacuum_db(path, *, apply=False) -> dict`; refusals raise `ValueError`
(or `FileNotFoundError` for a missing file).

## Accepted databases

Only FTDI databases are accepted (anything else is refused with `ValueError`):

* **producer lane DB**: has the tables `meta`, `raw_journal`, `outbox`, `delivery`, `disposition`, `pending`;
  reported as `"kind": "producer"`;
* **consumer DB**: has the table `effects`; reported as `"kind": "consumer"` (v1 or schema v2).

The path must be an existing regular file (not a directory, not a symlink/junction target outside its
directory). A rollback journal `PATH-journal` present → refused (a hot journal must be recovered by its writer).

## Dry run (default)

Strictly read-only: no file is created, changed or deleted. Only an **existing** `-shm` coordination file may be
updated by SQLite when the database has WAL frames; a missing `-shm` is never created: with WAL frames and no
`-shm` the dry run is refused (`ValueError`, exit 3) and suggests `--apply`, which first recovers and folds the
WAL with a normal connection (logical content unchanged). Output fields:

| field | meaning |
|---|---|
| `status` | `"DRY_RUN"` |
| `kind` | `"producer"` or `"consumer"` |
| `db` | the path as given |
| `page_size`, `page_count`, `freelist_count` | from SQLite |
| `file_bytes` | current size of the database file (WAL not included) |
| `wal_bytes` | size of `PATH-wal` (0 if absent) |
| `reclaimable_bytes_estimate` | `freelist_count * page_size` (a lower bound; VACUUM also defragments) |
| `space_needed_bytes` | free space required in the directory to apply (≥ `(page_count - freelist_count) * page_size`) |
| `space_free_bytes` | free space in the directory |
| `logical_digest` | SHA-256 over schema and every row of every table (see below) |
| `apply_required` | `true` |
| `changed` | `false` |

## Apply

`vacuum-db` holds an **exclusive SQLite lock for the whole operation** (`locking_mode=EXCLUSIVE`): if any other
connection of any process has the database open, even idle, the command is refused (`database in use`), and
while it runs nobody else can read or write the database. The original file is changed only by SQLite's own
transactional `VACUUM`, after a byte copy of it has been made **and verified**.

Steps, in this order. A refusal before step 5 leaves the database at `PATH` with identical logical content
(byte-identical if it had no WAL frames; step 3 folds WAL frames into the file) and removes only the copy this
run created:

1. Path checks: a regular file, not a link, not reached through a link or junction (its real path equals the
   given path), exactly one hard link; `PATH-journal` absent; `PATH-wal` / `PATH-shm`, when present, regular
   files with one hard link each that are not the database itself, and a non-empty `PATH-wal` must start with
   the SQLite WAL magic. Otherwise refused before any SQLite open.
2. Open `PATH`, take the exclusive lock (refused if impossible), check the database kind, compute the logical
   digest.
3. Fold the WAL into the file (`wal_checkpoint(TRUNCATE)`); check free space: in the directory for one copy of
   the file plus the VACUUM working space (one compacted copy), and for SQLite's temporary database in the
   system temp directory (one compacted copy; added to the directory requirement when both are on one device).
4. Create `PATH.pre-vacuum-<UTC yyyymmddTHHMMSSZ>` exclusively (an existing name is never overwritten), copy the
   file into it, fsync, re-read both files and compare SHA-256, and verify that the copy's logical digest equals
   step 2. Any difference → refused, the copy removed.
5. `VACUUM` in place, fold its WAL, `PRAGMA integrity_check` must be `ok`, `user_version`, `application_id`,
   `page_size`, `auto_vacuum` unchanged, logical digest equal to step 2.
6. Release the lock.

If step 5 fails, the error names the verified copy; `PATH` is a consistent SQLite database because VACUUM is
transactional. Nothing is restored automatically.

Success output: `status: "VACUUMED"`, `kind`, `db`, `bytes_before`, `bytes_after` (file sizes),
`logical_digest` (unchanged), `kept_original` (path of the verified copy), `changed: true`, and a `note` that the
kept copy is never deleted by FTDI. Command-line usage errors (missing `--db`) exit 2 (argparse).

## Logical digest

SHA-256 (domain `FTDI-logical-digest-v2`) over self-delimiting records of four kinds: every `sqlite_master` row
`(type, name, tbl_name, sql)` of the main schema sorted by `(type, name)` (automatic indexes and
`sqlite_sequence` included), then per table in name order a header (name, column count), every row ordered by
`typeof(c), c COLLATE BINARY` for all columns, and an end record with the row count. Values are type-tagged
(NULL, INTEGER, REAL as exact hex, TEXT and BLOB length-prefixed). Independent of page layout and rowids; any
changed, added, removed or moved row, table, index, view or trigger changes it. The only residual ordering tie
(REAL `0.0` vs `-0.0`) can make equal databases look different (refusal), never the reverse.

## Guarantees the tests must be able to observe

* Dry run: every file in the directory byte-identical before and after (except `-shm`), exit 0.
* After apply: `PATH` has the same logical content (all tables and rows, triggers, indexes), its size is ≤ the
  original's when rows had been deleted, the kept original has the same logical digest as the database before
  the command (and is byte-identical to it if it had no WAL frames), and FTDI keeps working on `PATH`:
  `audit-e2e-archive` gives the same status as before, a tier-aware consumer still answers replays as duplicates,
  the insert-guard trigger still blocks writes into compacted ranges, a producer replay resumes without new rows.
* Never: a state in which `PATH` is missing or contains different logical content; deletion of any file other
  than a copy this run created and can prove is its own (same file identity); writes through links or
  hard-linked sidecars; changes to archives, sources, registry or other databases.
* Refusals: non-FTDI database, missing file, directory, link/junction/hard link, foreign or hard-linked
  sidecar, invalid WAL file, rollback journal present, database open elsewhere (even idle), insufficient space,
  name collision of the kept copy (also when the name appears during the run), backup copy not identical.
