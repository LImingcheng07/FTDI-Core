# FTDI-Core archive-aware audit — release gates (DRAFT for the next minor release)

Status: **DRAFT. Development candidate on top of v0.2.0; not a release, not production certified.**
Scope: `audit-e2e-archive` (`docs/ARCHIVE_E2E_AUDIT.md`, ADR-006). The v0.2 gates in
`RELEASE_GATES_V02.md` continue to apply unchanged.

| # | Gate | Evidence required | Status (2026-11-07) |
|---|---|---|---|
| A1 | v0.2 behaviour unchanged: `audit-e2e`, `lab-e2e`, `e2e.py`, S07 adapters, `reference_s07_0041/`, schemas, data contract | `git diff` limited to new files + `cli.py` subcommand; 65 pre-existing Core tests and 45 frozen tests green | PASS (native Windows 11, Python 3.11.9) |
| A2 | Archived prefix + resident suffix reconciles per event (identity, payload SHA, canonical payload) against the consumer; market-seq interleaving accepted | contract tests 1–7, interleaving test | PASS |
| A3 | `COMPLETE` unreachable for: missing/partial/tampered/garbage segments, broken manifest tiling or cursor, missing manifest with positive cursor, self-consistent forgeries (raw, outbox, duplicates, unacked), archived quarantine, quarantine or delivered events relabelled `DUPLICATE_IDENTICAL`, missing/altered/orphan consumer effects, forged resident ACK, gaps, source mutation, views/virtual tables in place of evidence tables | contract tests 8–25, 33; internals tests; review regressions C1, C4 | PASS |
| A4 | Strictly read-only: no evidence byte or file changes (sidecars included); never-created consumer DB; report only to an explicit safe path | read-only snapshot tests, report-path tests, immutable-open test | PASS |
| A5 | Bounded resources on hostile archives (compressed, decompressed, line, record, metadata, overlap limits); no uncaught parser exceptions | limit tests, decompression bomb (< 8 MiB peak), deep nesting, multi-member gzip, non-UTF-8 | PASS |
| A6 | Concurrent modification or unverifiable fingerprints forbid `COMPLETE` | mid-audit producer/consumer/source/archive-dir changes, WAL `data_version`, unreadable fingerprint, same-size rewrite with restored mtime (C2), WAL without `-shm` / hot journal refused (C6) | PASS |
| A7 | Relocation needs explicit opt-in and never reads the stored old path; registered-path conflicts fail closed | relocation, basename, duplicate path, `.tmp` sibling, directory-replacement tests | PASS |
| A8 | Crash windows of the S07 prune remain auditable (orphan before commit, after commit) | subprocess crash tests | PASS |
| A9 | Windows handle hygiene on success, failure and exception paths | `rmtree` immediately after audits; ResourceWarning as error | PASS on native Windows |
| A10 | Independent verification | 48 black-box tests written from the contract by a separate agent (Codex) without reading the implementation; separate adversarial review found C1–C8, all fixed with regression tests and mutation-checked | PASS (see TEST_REPORT_TASK001.md) |
| A11 | Real-data scale run: full-day 554,441 events archived and audited `COMPLETE`, plus deliberate corruption runs `INCONSISTENT` | owner's original ZIP (read-only, SHA unchanged): 500,000 archived + 54,441 resident COMPLETE in 81 s / 37 MiB; bit flip and relabel forgery INCONSISTENT; relocation needs the flag (`docs/REAL_ARCHIVE_AUDIT_RESULTS.md`) | PASS (2026-11-07, native Windows) |
| A12 | Version bump, release notes and a new package manifest cut from the release commit | release process | **NOT YET DONE** (candidate manifest only) |

Still blocked for production (unchanged from v0.2): authenticated remote delivery, synchronized
timestamps, live WS + REST operation, long-term consumer effect retention (Task 002), backups and
power-loss tests, signatures/WORM for evidence.
