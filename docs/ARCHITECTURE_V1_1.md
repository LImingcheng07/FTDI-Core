# FTDI-Core v0.2 — Architecture addendum v1.1

Status: **ACCEPTED FOR OFFLINE LOCAL DEVELOPMENT**, **NOT PRODUCTION CERTIFIED**.

The established `FTDI-Core-Event-v1`, source normalization, and `FTDI-S07-004.1-envelope-v1` are unchanged. No database schema migration is required.

## Implemented flow

```text
Immutable source -> per-scope journal + checkpoint (T1)
                 -> gap-safe ordered outbox (T2)
                 -> at-least-once localhost HTTP publisher
                 -> independent consumer SQLite `effects` commit (T3)
                 -> HTTP response with full batch identity digest
                 -> producer `delivery` and `outbox.delivered=1` commit (T4)
                 -> independent resident-outbox content/receipt audit
```

`lab-e2e` starts the consumer as a **separate OS process** and always stops it after publishing. The consumer database is preserved. The outbox producer never treats the HTTP request as the effect: the consumer commits **first**. The producer only ACKs when the response describes exactly the expected digest and batch size. Both databases use SQLite WAL and FULL synchronous settings inherited from the verified S07-004.1 adapter.

## Four audit outcomes

| Status | Meaning | Exit code |
|---|---|---|
| COMPLETE | Every resident outbox event has matching consumer effect and matching local ACK; producer source/journal audit also passed | 0 |
| PENDING_DELIVERY | Valid source history still contains events not committed remotely | 2 |
| RETRY_REQUIRED | Consumer has committed event(s), but producer ACK not committed; safe resending expected | 2 |
| INCONSISTENT | Conflicting data, missing consumer receipt after producer ACK, orphan effect, invalid event hash, pending source gaps, or archived data outside this audit's scope | 2 |

For a fail-closed exception (missing DB, mismatched paths, unrecognized arguments) exit code `3` is returned. Recovery is *not* an excuse to write a false COMPLETE.

The auditor compares each **full event_id, lane, sequence, epoch, canonical payload SHA** across two independent databases, validates consumer self-consistency, and compares producer ACK-ledger rows. It collects bounded issue samples (up to 20), and emits aggregate mismatch counts. It does **not** simply compare totals.

## Crash windows tested

1. Consumer commits effects, but the publisher process exits before its ACK commit: `RETRY_REQUIRED`; resend; no duplicate effect.
2. Consumer commits effects, then process exits before HTTP response: `RETRY_REQUIRED`; restart server and publisher; no duplicate effect.
3. A partial batch allowance finishes before all source outbox events: `PENDING_DELIVERY`; rerun to reach COMPLETE.
4. An independently altered consumer payload: `INCONSISTENT`, no false pass.
5. An orphan valid effect or forged producer delivery flag: `INCONSISTENT`, no false pass.
6. A permanently missing event in one stream: the other stream may make progress, but the overall source audit is not COMPLETE.

## Explicit constraints

- **Not distributed exactly-once:** only the consumer's own SQLite effect table is idempotent. Arbitrary third-party actions (e.g., orders, API calls, side-effect outside this DB) are NOT covered.
- **Unauthenticated localhost HTTP:** do not bind publicly or use it across hosts. `127.0.0.1` is a local-only lab boundary and does not constitute a network security perimeter against malicious software on the same machine.
- **No distributed snapshot:** for strongest evidence, perform `audit-e2e` when producer and consumer are quiescent. SQLite transactions provide only per-database snapshots.
- **Prearchive scope:** after safe archival, the resident outbox may be empty while consumer effects remain. This auditor flags any archived prefix as unsupported rather than claiming COMPLETE; use `audit-e2e-archive` (read-only, archive-aware; `docs/ARCHIVE_E2E_AUDIT.md`, ADR-006) to reconcile archived prefixes. The legacy `retention.audit` mutates the producer DB and checks only event IDs; do not use it as E2E evidence.
- **Input prefix:** historical aggregate-ID continuity is not venue-independent certification of events before the first observed ID.
- **Clock trust:** neither wall-clock batch duration nor event-vs-local timestamp difference is trustworthy one-way exchange latency without clock calibration.
- **Backpressure:** SQLite file size and consumer effect retention can increase indefinitely; this release does not implement automatic cleanup or retention windows.

## Proposed v0.3 gates

Introduce archive-aware receipt reconciliation, consumer dedup snapshot/compaction without violating replay windows, versioned live-adapter provenance, robust metric collection and Windows-specific restart/failure QA. Only then consider remote authenticated delivery and sustained-network acceptance tests.
