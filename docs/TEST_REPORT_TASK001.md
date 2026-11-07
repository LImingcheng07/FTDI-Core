# Task 001 test report — archive-aware E2E audit (`audit-e2e-archive`)

Status: **development candidate on branch `feat/archive-e2e-audit`; not a release; not production certified.**
Executed 2026-11-07 on the owner's machine: **native Windows 11 Pro 10.0.26300, Python 3.11.9,
SQLite 3.45.1** (not a container). Base commit: `86def0d` (unmodified v0.2 Windows-ready baseline).

## Commands and results

```text
python -W error::ResourceWarning -m unittest discover -s tests -q
  -> Ran 152 tests ... OK
cd reference_s07_0041 && python -W error::ResourceWarning -m unittest discover -s tests -q
  -> Ran 45 tests ... OK
```

| Suite | Tests | Author | Result |
|---|---:|---|---|
| Pre-existing Core (v0.2 Windows-ready) | 65 | baseline | OK, unchanged |
| `tests/test_archive_e2e.py` — black-box contract suite | 48 | Codex, from `docs/ARCHIVE_E2E_AUDIT.md` only, without reading the implementation | OK |
| `tests/test_archive_e2e_internals.py` — injection/hostile-input tests needing implementation knowledge | 25 | Claude | OK |
| `tests/test_archive_e2e_review.py` — regressions for review findings C1–C8 | 14 | Claude | OK |
| **Core total** | **152** | | **OK** |
| Frozen S07-004.1 compatibility (`reference_s07_0041/`) | 45 | baseline | OK, unchanged |

Unchanged by this task (verified with `git status`): `reference_s07_0041/`, `ftdi_core/adapters/`,
`ftdi_core/e2e.py`, `ftdi_core/audit.py`, `ftdi_core/delivery.py`, every database schema, the data contract.
`ftdi_core/cli.py` gained one subcommand (13 added lines, no other change).

## Fail-closed evidence (CLI, native Windows)

| Scenario | Command | Exit | Status / findings |
|---|---|---:|---|
| Archived run (24 archived + 6 resident), v0.2 resident audit | `audit-e2e` | 2 | INCONSISTENT, `archived_records_unsupported=24` (unchanged v0.2 refusal) |
| Same run, archive-aware | `audit-e2e-archive --report r.json` | 0 | COMPLETE: 2 segments, matched 30, anchored 30, snapshot verified |
| One bit flipped in a segment | `audit-e2e-archive` | 2 | INCONSISTENT, `ARCHIVE_SHA256_MISMATCH` |
| Consumer DB path does not exist | `audit-e2e-archive` | 3 | `FTDI_CORE_FAIL_CLOSED`, file not created |

Every corruption class in the contract is covered by at least one automated test asserting
`INCONSISTENT` plus the specific finding code (see `docs/RELEASE_GATES_ARCHIVE_AUDIT_DRAFT.md`, gate A3).

## Defects found during development (all fixed, all with tests)

| Found by | Defect | Fix |
|---|---|---|
| Codex black-box test (decompression bomb, peak < 8 MiB) | `read(limit + 1)` pre-allocates the limit (32 MiB) for every segment regardless of file size | read `size + 1` after `fstat`; also lstat/fstat identity check |
| CLI demo | unreadable segment made its 12 consumer effects look like orphans (misleading diagnosis; status was already fail-closed) | lane marked unverifiable; effects counted as `unverifiable_lane_effects` |
| Codex adversarial review, PoC-confirmed | **C1** `DUPLICATE_IDENTICAL` trusted without evidence: relabelling a quarantined record, or hiding the first delivered event, produced **COMPLETE** | every duplicate claim must match a byte-identical published event with the same seq (`DISPOSITION_CLAIM_UNVERIFIED`) |
| same | **C2** same-size rewrite of source/archive after reading, mtime restored → COMPLETE with snapshot "verified" | post-audit content re-hash of every relied-upon file |
| same | **C3** report path through an `archives/` junction could overwrite an archive file | report path vetted literally and resolved against all evidence directories/files |
| same | **C4** recursive VIEW in place of `raw_journal` hung the audit | evidence tables must be plain `CREATE TABLE` with exact `pragma_table_xinfo` columns |
| same | **C5** 5000-digit meta integer escaped as `ValueError` | canonical integers limited to 18 digits |
| same | **C6** WAL without `-shm`: read-only open created `-shm` | refused (`SNAPSHOT_UNVERIFIABLE`); hot `-journal` refused too |
| same | **C7** snapshot listing enumerated an out-of-root junction target | redirected directories are never enumerated |
| same | **C8** thousands of unknown disposition labels inflated the report | known outcomes grouped, unknown aggregated |

Mutation check: with the C1, C2 or C5 fix disabled by `mock`, every corresponding regression test
fails; with the C4 fix disabled the recursive-view audit hangs (subprocess timeout), with it enabled it
returns `INCONSISTENT` immediately. The C4 consumer-view test is also caught by the unique-index check
(defence in depth). The adversarial review run ended early when the reviewer's service flagged its
own output; its findings were extracted from the run log and PoC scripts and reproduced here.

Legitimate data confirmed `COMPLETE` (no false positives): out-of-order market sequences across the
archive boundary, CRLF sources, sources without a final newline, legitimate duplicate lines (resident
and archived), multi-lane runs, WAL databases with an existing `-shm`, relocated roots with the explicit flag.

## Scale (synthetic data; the real 554,441-event ZIP is not bundled)

| Events | Archived / resident | `audit-e2e-archive` wall | Python peak (tracemalloc) | Status |
|---:|---|---:|---:|---|
| 50,000 | 50,000 / 0 (3 segments) | 6.28 s | 6.9 MiB | COMPLETE |
| 200,000 | 180,000 / 20,000 (9 segments) | 25.15 s* | 6.9 MiB | COMPLETE |

\* measured before the C1/C2 fixes; the 50k run is after them (~126 µs/event in both cases). Memory
does not grow with event count (the sequence bitmap is `next_file_index / 8` bytes). Linear
extrapolation to the full day suggests ~70 s; **this is an extrapolation, not a measurement**. Hot spot:
`json.dumps` canonicalisation (~12 calls per event), an optimisation candidate.

## Not verified / limits

- Full-day real archive audit (gate A11) — needs the owner's original ZIP and a run outside this sandbox.
- UNC network paths (`\\server\share`) are not supported by SQLite URI opening (same as v0.2).
- Power-loss, disk-full and long-running concurrency are out of scope; concurrency is detected, not prevented.
- No protection against an actor able to rewrite source, registry, producer DB, archives and consumer DB together.
