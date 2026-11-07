# FTDI-Core v0.3.0 — release gates and evidence

Release date 2026-11-07. Platform: native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1 (owner's machine,
run data on the NVMe SSD). Status: **released for local research use; not production certified.**
Supersedes `RELEASE_GATES_ARCHIVE_AUDIT_DRAFT.md` (kept as the Task 001 record); the v0.2 gates in
`RELEASE_GATES_V02.md` still apply.

| # | Gate | Evidence | Status |
|---|---|---|---|
| G1 | v0.2 compatibility: event contract, S07 envelope, producer schemas, `reference_s07_0041/`, `audit-e2e` refusal of archived prefixes, `PACKAGE_MANIFEST.json` byte-identical | 65 v0.2 tests + 45 frozen tests green; acceptance check "v0.2 refusal unchanged"; manifest v2 records the baseline SHA-256 `054b3c4c…88a7daff (unchanged since the imported v0.2 baseline commit `86def0d`)` | PASS |
| G2 | Archive-aware audit (Task 001): per-event reconciliation, fail closed on every forgery class, strictly read-only, bounded resources, concurrent change forbids COMPLETE, relocation opt-in | gates A1–A11 in `RELEASE_GATES_ARCHIVE_AUDIT_DRAFT.md`; `docs/ARCHIVE_E2E_AUDIT.md` | PASS |
| G3 | Consumer retention v2 (Task 002 phase 1): never forgets an identity, verified hash-chained segments, insert-guard trigger, tier-aware replay answers (duplicate / 409 / 503), exact rehydration, dry run by default | T2-1…T2-5 in `TASK002_CONSUMER_RETENTION_DESIGN.md`; `docs/CONSUMER_RETENTION_V2.md`; `docs/REAL_CONSUMER_RETENTION_RESULTS.md` | PASS |
| G4 | Producer archive in the Core CLI: dry run strictly read-only and equal to what `--apply` does; digest receipts; refusal before change; partial progress reported; explicit stop reason | `tests/test_producer_retention.py` (10), `tests/test_tiered_prune.py` (4); full-day dry run of 554,441 events in 47 s with the exact receipt split | PASS |
| G5 | Timestamp provenance (Task 003): no latency without a clock model, tagged exclusions, minimum counts | `tests/test_timeprov.py` (13); pilot re-analysis NO_CLOCK_MODEL (ADR-008) | PASS |
| G6 | Independent verification | black-box tests written from the contracts by a separate agent (Codex) without reading the implementation: 48 (audit) + 75 (retention); adversarial reviews: audit C1–C8, retention R1–R6, all fixed with mutation-checked regressions; `archive-producer` review: @@REVIEW3@@ | PASS |
| G7 | Full-day real data with the release code | `scripts/acceptance_full_day.py` on the owner's ZIP (read-only, SHA-256 unchanged) from commit `afa457c` (clean tree): **ACCEPTANCE PASS**, see below and `examples/V030_ACCEPTANCE_EVIDENCE.json` | PASS |
| G8 | Windows behaviour | all suites with `-W error::ResourceWarning`; directories removed immediately after audits and failures; executed natively, not emulated | PASS |
| G9 | Release process | version 0.3.0 in `ftdi_core/__init__.py`, `pyproject.toml`, service output; `RELEASE_NOTES_v0.3.0.md`; `release/PACKAGE_MANIFEST.v0.3.0.json` (schema v2) and `dist/FTDI-Core-v0.3.0-windows.zip` built by `scripts/build_release.py`, which refuses a dirty tree. The built ZIP is then extracted to a new directory and both suites and `Check-Release-Manifest.py` are run there; that result and the ZIP SHA-256 are recorded in the annotated local tag `v0.3.0` (not pushed), since this file is itself inside the ZIP | PASS (build inputs); post-build check: tag `v0.3.0` |
| G10 | Safety | no network beyond 127.0.0.1, no credentials, no trading; owner's ZIP read-only (SHA-256 before = after); the owner's own consumer data was not compacted; no history rewrite, no push | PASS |

## G7 — full-day acceptance (release code)

| Step | Exit | Wall (s) | Outcome |
|---|---:|---:|---|
| prepare (full day) | 0 | 10.31 | - |
| replay | 0 | 42.35 | - |
| lab-e2e (separate consumer process, localhost) | 0 | 128.98 | sent 554,441, ACKed 554,441, duplicates 0 |
| archive-producer dry run (300000) | 0 | 29.19 | status DRY_RUN; would_archive 300,000; archived_total 0; stopped LIMIT_REACHED; receipts consumer_hot_rows 300,000, consumer_digests 0 |
| archive-producer --apply (300000) | 0 | 47.64 | status ARCHIVED; archived_new 300,000; archived_total 300,000; stopped LIMIT_REACHED |
| v0.2 audit-e2e refuses the archived prefix | 2 | 32.61 | status INCONSISTENT; matched 254,441 |
| consumer-retention migrate | 0 | 0.33 | status MIGRATED |
| plan-consumer-retention | 0 | 0.72 | status PLANNED |
| consumer-retention compact (dry run) | 0 | 47.37 | status DRY_RUN; rows 500,000 |
| consumer-retention compact --apply | 0 | 137.98 | status COMPACTED; rows 500,000 |
| archive-producer dry run (200000) | 0 | 11.37 | status DRY_RUN; would_archive 200,000; archived_total 300,000; stopped LIMIT_REACHED; receipts consumer_hot_rows 0, consumer_digests 200,000 |
| archive-producer --apply (200000) | 0 | 29.41 | status ARCHIVED; archived_new 200,000; archived_total 500,000; stopped LIMIT_REACHED |
| audit-e2e-archive (producer and consumer compacted) | 0 | 86.93 | status COMPLETE; matched 554,441, compacted 500,000 |
| replay into a NEW producer run | 0 | 36.16 | - |
| restarted producer resends every event | 0 | 119.24 | sent 554,441, ACKed 554,441, duplicates 554,441 |
| tampered consumer segment | 2 | 83.74 | status INCONSISTENT; issues CONSUMER_SEGMENT_SHA256_MISMATCH 1; matched 554,441, compacted 500,000 |
| consumer-retention rehydrate --apply | 0 | 80.01 | status REHYDRATED |

| Check | Result |
|---|---|
| all events delivered | PASS |
| archive-producer dry run plans 300000 events, all receipted by consumer_hot_rows, changes nothing | PASS |
| archive-producer archived exactly 300000 events | PASS |
| v0.2 refusal unchanged | PASS |
| archive-producer dry run plans 200000 events, all receipted by consumer_digests, changes nothing | PASS |
| archive-producer archived exactly 200000 events | PASS |
| producer archive stops at the target | PASS |
| archive-aware audit COMPLETE | PASS |
| restart replay creates no new effect | PASS |
| tampering detected | PASS |
| legacy writer blocked by trigger | PASS |
| rehydration identical to the original table | PASS |

Source: 554,441 records, seq 3023902824…3024457264, gaps 0; ZIP SHA-256 `78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9` before = after. Commit `afa457c` (clean tree), ftdi-core 0.3.0, Python 3.11.9, SQLite 3.45.1; 2026-11-07 20:46:51 → 2026-11-07 21:02:28. Storage: consumer after compaction + VACUUM (copy) 60.9 MiB, consumer archive 50.8 MiB.

## Test inventory (277 Core + 45 frozen)

| File | Tests | Origin |
|---|---:|---|
| `test_cli`, `test_contracts`, `test_e2e`, `test_preparation`, `test_service` | 65 | v0.2 baseline, unchanged |
| `test_archive_e2e.py` | 48 | Codex, black-box from `ARCHIVE_E2E_AUDIT.md` |
| `test_archive_e2e_internals.py` | 25 | injection / hostile input |
| `test_archive_e2e_review.py` | 15 | regressions C1–C8 + real-data cascade |
| `test_consumer_retention.py` | 75 | Codex, black-box from `CONSUMER_RETENTION_V2.md` |
| `test_consumer_retention_review.py` | 8 | regressions R1–R6 |
| `test_producer_retention.py` | 10 | `archive-producer` |
| `test_tiered_prune.py` | 4 | digest receipts |
| `test_retention_plan.py` | 11 | planner |
| `test_timeprov.py` | 13 | time provenance |
| `test_cli_encoding.py`, `test_provenance_constants.py` | 3 | GBK console, pinned ZIP SHA |
| **Core total** | **277** | |
| `reference_s07_0041/tests` | 45 | frozen compatibility evidence |

## Still blocked for production

Authenticated remote delivery; a live market adapter with decoupled reader, queue depth and offset sampling;
synchronized timestamps and trusted latency; 24-hour operation; backups, restore and power-loss tests;
signatures/WORM for evidence; the digest watermark D and a VACUUM tool for real data; distributed snapshots
(audits assume a quiescent maintenance window).
