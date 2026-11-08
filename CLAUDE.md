# FTDI-Core: instructions for Claude Code

You are working on FTDI-Core v0.4.1 (Python 3.10+, stdlib-only research prototype), not a trading bot or production-certified market-data platform. Communicate with the project owner in Chinese unless asked otherwise.

**Governance (since 2026-11-06, see `FTDI_CLAUDE_CODE_HANDOFF.md`)**: Claude Code owns architecture, implementation, tests, refactoring and versioning decisions. Older steps that require approval from ChatGPT or a reviewer are superseded. The owner's explicit consent is still required for irreversible or high-risk actions: deleting real data/archives/consumer DBs, destructive schema migrations, `git push`/force/reset/history rewrites, credentials, accounts or trading. Work may be delegated to local agents (e.g. `codex exec`); verify delegated output before relying on it.

## First actions in every session
1. Read README_START_HERE.md, docs/ARCHITECTURE_V1.md, docs/ARCHITECTURE_V1_1.md, docs/ARCHITECTURE_V1_2.md, docs/DATA_CONTRACT_V1.md, docs/RELEASE_GATES_V03.md and docs/RELEASE_GATES_V04.md.
2. Run the **unmodified baseline** tests before implementation:
   - `python -W error::ResourceWarning -m unittest discover -s tests -q` (65 at the v0.2 release; 429 at v0.4.1, 1 skipped by design)
   - From `reference_s07_0041/`: `python -W error::ResourceWarning -m unittest discover -s tests -q` (45 tests at release)
3. Inspect `git status --short` if this is a Git repository. Never discard or overwrite the user's uncommitted changes.
4. Work on a narrowly scoped task; define the contract and acceptance tests before nontrivial edits. A user-site package named `tests` exists on the owner's machine: run suites with `discover -s tests`, not `-m unittest tests.x`.

## Safety and correctness invariants
- Market records are evidence. Source ZIP files, independent checksum files, and raw event payloads are immutable. Never silently normalize away original fields. Binance aggTrades historical CSV has **8 columns**; the Core contract preserves the eighth field.
- Sequence gaps, duplicates with conflicting content, checksum mismatch, source mutation, and pending overflow **fail closed**. Do not skip an unresolved gap to declare continuity or claim missing events have been recovered.
- Preserve per-lane isolation and transactional input offset/checkpoint contracts. A transaction ACK is not proof of a separate downstream business effect.
- End-to-end `COMPLETE` is permitted only when producer outbox, independent consumer effects, and producer ACK records agree on **every event identity and payload SHA**. A matching count alone is insufficient.
- The v0.2 auditor (`audit-e2e`) explicitly rejects archived prefixes; keep it that way. Archived prefixes are reconciled only by the separate read-only `audit-e2e-archive` (`docs/ARCHIVE_E2E_AUDIT.md`, ADR-006). Never weaken either gate to make tests pass, and never use the mutating legacy `retention.audit` as E2E evidence.
- SQLite WAL is not a general guarantee of cross-host exactly-once delivery. The localhost HTTP lab is unauthenticated and must not bind publicly.
- The program is read-only with respect to markets. No order placement, private API keys, account operations or trade execution, including as a supposed test.
- Offline replay and synthetic REST recovery are **not** evidence of real exchange availability, network P99 or venue-certified prefix completeness.
- Never delete the source ZIP, original logs, consumer effect DB, archives, or another user's state without explicit permission and a verified backup. Any retention/prune feature must default to **dry-run** and require a separate explicit destructive opt-in after exhaustive audit.
- Do not add surprise outbound network calls, telemetry, auto-updates, or package installation scripts. New dependencies require approval and a pinned, reviewed rationale.
- No external service tokens in source, tests, logs, commits or docs. `.env` and secrets are ignored by Git.

## Code and release practices
- Prefer small changes to `ftdi_core/` and tests; treat `reference_s07_0041/` as frozen compatibility evidence.
- Keep database schema/data-contract changes behind explicit migration and ADR; no silent format changes.
- Tests use stdlib unittest and must pass with ResourceWarning escalated. Also run the 45-test frozen compatibility suite.
- Add tests for success, partial/incomplete, crash/retry, tampering, restart, Windows file-handle cleanup, and source-fingerprint protection as appropriate.
- Git commit locally only after tests pass and review. Never `git push`, force-push, reset, amend, delete data, or change remotes without the owner's explicit request.
- Work with temporary test directories. Do not modify existing `runs/` or `data/` in-place for testing.
- Summarize changes in Chinese: files changed, exact test commands/counts/results, event-integrity impact, outstanding risks, rollback strategy. Avoid claiming Windows behavior verified unless actually executed on Windows.
- Any feature relying on live Binance connections remains experimental, separately gated, and disabled in offline tests.

## Repository map
- `ftdi_core/` official application and CLI
- `ftdi_core/adapters/s070041/` maintained compatibility adapters
- `reference_s07_0041/` original S07 verification and failure tests
- `tests/` Core integration tests
- `docs/` contracts, ADRs, release gates, real-data evidence
- `tasks/` bounded work orders for Claude Code
- `scripts/` Windows setup/verification and packaging

## Current state
**v0.4.1 released 2026-11-08** (tags `v0.3.0`, `v0.3.1`, `v0.4.0`, `v0.4.1`, local only). v0.4.1 fixes the live clock
evidence found by the owner-approved real-network pilot (`docs/REAL_LIVE_PILOT_RESULTS.md`, `RELEASE_NOTES_v0.4.1.md`):
precise Windows wall clock (`ftdi_core/live/clocks.py`), clock samples timed after connect, each read checked against
the wall-minus-interval level of its clock sample (`timeprov.classify_events`; heuristic `_stamp_levels` otherwise),
`READ_STAMP_OUTLIER`, status `NO_TRUSTED_SAMPLES`, chain `source_manifest.json`, deadline-bounded name resolution
(`ftdi_core/live/netconnect.py`). The pilot data in
`C:\ftdi\live*` is the owner's: never delete or compact it. v0.4.0 adds live read-only ingestion (Task 004, ADR-011, `docs/LIVE_CAPTURE.md`, `ftdi_core/live/`): `live-capture` into sealed sessions (stdlib WebSocket client; loopback unless `--public-network`), `verify-session`, `seal-session`, `compose-sessions` (verified chain -> one replay source), `analyze-clock-evidence --session`; simulated venue `tests/fakevenue.py`, loopback acceptance `scripts/acceptance_live_loopback.py`. v0.3.1 added `vacuum-db` (ADR-010, `docs/STORAGE_MAINTENANCE.md`). v0.3.0 (`RELEASE_NOTES_v0.3.0.md`, gates `docs/RELEASE_GATES_V03.md`,
manifest `release/PACKAGE_MANIFEST.v0.3.0.json`, ZIP built by `scripts/build_release.py`). Contents: Task 001 archive-aware
audit (`audit-e2e-archive`, ADR-006); Task 002 phase 1 consumer retention v2 (`consumer-retention`, `plan-consumer-retention`,
ADR-007); Core `archive-producer` (mandatory pre-audit, dry run unless `--apply`) whose prune accepts identical v2 digests as
receipts, so producer archive and consumer compaction run in either order; Task 003 timestamp provenance (`analyze-clock-evidence`, ADR-008; the
live pilot yields no latency); consumer checkpoint tuning (ADR-009). Full-day acceptance: `scripts/acceptance_full_day.py`
(evidence `examples/V030_ACCEPTANCE_EVIDENCE.json`, v0.3.1: `examples/V031_ACCEPTANCE_EVIDENCE.json`). The owner's own data was never compacted; doing so still needs explicit
consent. Keep run data on the NVMe SSD (C:), not the E: HDD.
Next: the owner's decision on a second, short real-network capture with v0.4.1 (each real-network capture needs the owner's consent; synchronise the Windows clock first), to obtain a trusted latency estimate; optionally cross-check a session against the official daily ZIP. Deferred: digest watermark D (small gain, replays of old identities would need segment reads). Later: authenticated delivery; backups/power-loss tests; signed
evidence.
