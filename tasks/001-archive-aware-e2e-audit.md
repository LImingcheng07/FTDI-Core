# Task 001: archive-aware end-to-end audit (read-only phase)

Status: READY FOR DESIGN REVIEW, NOT APPROVED FOR DESTRUCTIVE OPERATIONS.

## Problem
`ftdi_core/e2e.py` currently returns INCONSISTENT when producer history has an archived prefix, even if legacy archive integrity evidence exists. The v0.3 direction is a **read-only, archive-aware E2E reconciler** that verifies both resident outbox records and archived events against independent consumer effects. A false COMPLETE is unacceptable.

## Before coding
Read `ftdi_core/e2e.py`, `ftdi_core/adapters/s070041/retention.py`, `docs/ARCHITECTURE_V1_1.md`, `docs/DATA_CONTRACT_V1.md`, and existing tests. Map actual archive segment schema, digest definition, archive path relocation semantics, and consumer `effects` columns. Report risks and a proposed read-only interface before editing.

## Scope (allowed)
- Implement read-only reconciliation of an archive prefix **only when** every segment checksum, internal index, event hash, expected ACK evidence, and independent effect are fully verified.
- Retain v0.2's refusal semantics on missing, incomplete, conflicting, tampered, or unrecognized archive evidence. Every record's identity and payload must match; compare more than counts.
- Keep current resident-only `audit-e2e` behavior stable by default or introduce an explicit new audit mode/command. Document the compatibility choice.
- Add tests for valid archive + resident suffix, missing segment, tampered compressed bytes, forged ACK, missing consumer row, duplicate/conflicting identity, partial archive, restart, and an unrelated blocked lane.
- Write a new ADR/release gate for any new archived-audit trust assumption. No new external dependencies.

## Out of scope
- DELETE/PRUNE/VACUUM or rewriting existing archives.
- Deleting or shortening consumer idempotency retention; configuring automatic garbage collection.
- Orders, credentials, remote HTTP binding, live WS integration, or trading logic.
- Declaring cross-host delivery exactly-once or production-ready.

## Acceptance
- Both baseline suites pass with ResourceWarning as error.
- New archive test fixtures are small, deterministic, generated in temp directories, and corrupted versions always fail closed.
- Independent per-event reconciliation detects missing/extra/altered archived events and invalid remote receipts.
- `COMPLETE` is impossible if a source has an unresolved gap or archived prefix has unverifiable evidence.
- `git diff` documents changed API, read-only audit behavior, tests and explicit limitations.
