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
| G4 | Producer archive in the Core CLI: mandatory pre-audit (refuses unless COMPLETE / PENDING_DELIVERY / RETRY_REQUIRED); dry run changes no data or evidence file and predicts what `--apply` does in a maintenance window; independent consumer; digest receipts; exact delete counts; refusal before change; committed progress reported after any failure; explicit stop reason | `tests/test_producer_retention.py` (29, incl. regressions P1–P8, Q1–Q8), `tests/test_tiered_prune.py` (4); full-day acceptance below | PASS |
| G5 | Timestamp provenance (Task 003): no latency without a clock model, tagged exclusions, minimum counts | `tests/test_timeprov.py` (13); pilot re-analysis NO_CLOCK_MODEL (ADR-008) | PASS |
| G6 | Independent verification | black-box tests written from the contracts by a separate agent (Codex) without reading the implementation: 48 (audit) + 75 (retention); adversarial reviews: audit C1–C8, retention R1–R6, all fixed with mutation-checked regressions; `archive-producer` reviews: 8 findings P1–P8 (two HIGH: deletion of an unarchived outbox row on a corrupt ledger; archiving that erased an ACK inconsistency), all fixed; the follow-up review confirmed the fixes and found Q1–Q7 (trigger side effects, records the audit cannot read back, overlapping leftover files, blocked temp path, metadata-preserving writes, audit-to-apply window, invalid delivered flag), all fixed. The follow-up session was cut off by its provider before writing a final summary; its reproduction logs were evaluated directly. Re-running all its reproduction cases on the fixed code: every case is refused before any change or ends COMPLETE, except one that changed the ACK ledger inside `prune` between segment write and delete; that led to Q8 (every deleted row is revalidated against the archived record inside the transaction), fixed. Every regression fails on its pre-fix commit (mutation check) | PASS |
| G7 | Full-day real data with the release code | `scripts/acceptance_full_day.py` on the owner's ZIP (read-only, SHA-256 unchanged) from commit `@@COMMIT@@` (clean tree): **ACCEPTANCE PASS**, see below and `examples/V030_ACCEPTANCE_EVIDENCE.json` | PASS |
| G8 | Windows behaviour | all suites with `-W error::ResourceWarning`; directories removed immediately after audits and failures; executed natively, not emulated | PASS |
| G9 | Release process | version 0.3.0 in `ftdi_core/__init__.py`, `pyproject.toml`, service output; `RELEASE_NOTES_v0.3.0.md`; `release/PACKAGE_MANIFEST.v0.3.0.json` (schema v2) and `dist/FTDI-Core-v0.3.0-windows.zip` built by `scripts/build_release.py`, which refuses a dirty tree. The built ZIP is then extracted to a new directory and both suites and `Check-Release-Manifest.py` are run there; that result and the ZIP SHA-256 are recorded in the annotated local tag `v0.3.0` (not pushed), since this file is itself inside the ZIP | PASS (build inputs); post-build check: tag `v0.3.0` |
| G10 | Safety | no network beyond 127.0.0.1, no credentials, no trading; owner's ZIP read-only (SHA-256 before = after); the owner's own consumer data was not compacted; no history rewrite, no push | PASS |

## G7 — full-day acceptance (release code)

@@ACCEPTANCE_TABLE@@

## Test inventory (296 Core + 45 frozen)

| File | Tests | Origin |
|---|---:|---|
| `test_cli`, `test_contracts`, `test_e2e`, `test_preparation`, `test_service` | 65 | v0.2 baseline, unchanged |
| `test_archive_e2e.py` | 48 | Codex, black-box from `ARCHIVE_E2E_AUDIT.md` |
| `test_archive_e2e_internals.py` | 25 | injection / hostile input |
| `test_archive_e2e_review.py` | 15 | regressions C1–C8 + real-data cascade |
| `test_consumer_retention.py` | 75 | Codex, black-box from `CONSUMER_RETENTION_V2.md` |
| `test_consumer_retention_review.py` | 8 | regressions R1–R6 |
| `test_producer_retention.py` | 29 | `archive-producer` + review regressions P1–P8, Q1–Q8 |
| `test_tiered_prune.py` | 4 | digest receipts |
| `test_retention_plan.py` | 11 | planner |
| `test_timeprov.py` | 13 | time provenance |
| `test_cli_encoding.py`, `test_provenance_constants.py` | 3 | GBK console, pinned ZIP SHA |
| **Core total** | **296** | |
| `reference_s07_0041/tests` | 45 | frozen compatibility evidence |

## Still blocked for production

Authenticated remote delivery; a live market adapter with decoupled reader, queue depth and offset sampling;
synchronized timestamps and trusted latency; 24-hour operation; backups, restore and power-loss tests;
signatures/WORM for evidence; the digest watermark D and a VACUUM tool for real data; distributed snapshots
(audits assume a quiescent maintenance window).
