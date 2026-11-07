# FTDI-Core: instructions for Claude Code

You are working on FTDI-Core v0.2 (Python 3.10+, stdlib-only research prototype), not a trading bot or production-certified market-data platform. Communicate with the project owner in Chinese unless asked otherwise.

**Governance (since 2026-11-06, see `FTDI_CLAUDE_CODE_HANDOFF.md`)**: Claude Code owns architecture, implementation, tests, refactoring and versioning decisions. Older steps that require approval from ChatGPT or a reviewer are superseded. The owner's explicit consent is still required for irreversible or high-risk actions: deleting real data/archives/consumer DBs, destructive schema migrations, `git push`/force/reset/history rewrites, credentials, accounts or trading. Work may be delegated to local agents (e.g. `codex exec`); verify delegated output before relying on it.

## First actions in every session
1. Read README_START_HERE.md, docs/ARCHITECTURE_V1.md, docs/ARCHITECTURE_V1_1.md, docs/DATA_CONTRACT_V1.md and docs/RELEASE_GATES_V02.md.
2. Run the **unmodified baseline** tests before implementation:
   - `python -W error::ResourceWarning -m unittest discover -s tests -q` (65 at the v0.2 release; 154 after Task 001)
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
Task 001 (archive-aware E2E audit) is implemented on branch `feat/archive-e2e-audit` (`ftdi_core/archive_e2e.py`); release gates in `docs/RELEASE_GATES_ARCHIVE_AUDIT_DRAFT.md`. Full-day real-data audit passed (gate A11, `docs/REAL_ARCHIVE_AUDIT_RESULTS.md`). Still open: a release cut with version bump and manifest (A12). Next candidates: `tasks/002` (consumer dedup retention design, design-only until reviewed by the owner because it touches deletion) and `tasks/003` (time provenance).
