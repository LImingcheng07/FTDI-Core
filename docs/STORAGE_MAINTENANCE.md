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

Precondition (documented, as for every maintenance command): an exclusive maintenance window — no other process
has the database open. On Windows an open handle makes the final rename fail, which is reported as a refusal.

Steps, in this order; any failure stops with exit 3 and leaves the database at `PATH` with identical logical
content (byte-identical if it had no WAL frames before the command; step 1 folds WAL frames into the file):

1. Checkpoint the WAL into the main file (`wal_checkpoint(TRUNCATE)`); if another connection prevents a complete
   checkpoint → refused (`database in use`). Not enough free space (see `space_needed_bytes`) → refused.
2. Compute the logical digest and a content fingerprint of `PATH` (and `PATH-wal` if present).
3. `VACUUM INTO` a new temporary file in the same directory.
4. Verify the new file: `PRAGMA integrity_check` is `ok`; identical logical digest; identical `user_version`
   and `application_id`; the journal mode is restored to WAL if the original used WAL.
5. Re-check the fingerprint of `PATH`: any change since step 2 → refused (`changed during vacuum`), the temporary
   file is removed (it is the tool's own scratch file), the original is untouched.
6. Swap: rename `PATH` → `PATH.pre-vacuum-<UTC yyyymmddTHHMMSSZ>` (the **kept original**), move its `-wal`/`-shm`
   sidecars along with it, rename the new file → `PATH`. If any rename fails, every completed rename is undone so
   that `PATH` is the original again; the error says so.
7. Verify `PATH` again (read-only): logical digest identical to step 2.

Success output: `status: "VACUUMED"`, `kind`, `db`, `bytes_before`, `bytes_after` (file sizes),
`logical_digest` (unchanged), `kept_original` (path of the kept original), `changed: true`, and a `note` that
the kept original is never deleted by FTDI.

## Logical digest

SHA-256 over, in order: every `sqlite_master` row `(type, name, tbl_name, sql)` sorted by `(type, name)`
(automatic indexes included; `sqlite_sequence` included as a table), then for every table in name order its
name and all rows ordered by all columns, each row encoded canonically with SQLite type tags. Independent of
page layout and rowid assignment; any changed, added or removed row, table, index, view or trigger changes it.

## Guarantees the tests must be able to observe

* Dry run: every file in the directory byte-identical before and after (except `-shm`), exit 0.
* After apply: `PATH` has the same logical content (all tables and rows, triggers, indexes), its size is ≤ the
  original's when rows had been deleted, the kept original has the same logical digest as the database before
  the command (and is byte-identical to it if it had no WAL frames), and FTDI keeps working on `PATH`:
  `audit-e2e-archive` gives the same status as before, a tier-aware consumer still answers replays as duplicates,
  the insert-guard trigger still blocks writes into compacted ranges, a producer replay resumes without new rows.
* Never: a state in which `PATH` is missing or contains different logical content; deletion of any file other
  than the tool's own temporary file; changes to archives, sources, registry or other databases.
* Refusals: non-FTDI database, missing file, directory, rollback journal present, database in use, insufficient
  space, concurrent change, name collision of the temporary or kept file.
