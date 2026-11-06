# Migration plan and gates

## Provenance-oriented mapping

| Prior experiment | Core v0.1 use | Gate / concern |
|---|---|---|
| S07-002 | Original ZIP source and historical checksum chain | Keep original bytes immutable; no network rerequest |
| S07-003.2 | Gap wait/coalescing and hot/spill experiment reference | Do not graft ephemeral SQLite spill into durable raw journal |
| S07-004 / Windows fixed | Single-lane journal/crash safety source | Built into S07-004.1 compatibility; old test snapshot included |
| S07-004.1 | Per-lane durable DB, scheduler, external consumer, receipt-gated archive | Packaged in namespaced adapter; 45 legacy tests must pass |
| S07-005 | Read-only live WebSocket + bounded REST repair research | **Quarantined** until clock+network+data-contract integration gates pass |
| S07-005.1 | Time sync and reconnect audit | Not yet implemented here |

## Target milestone sequence

- **Core v0.1 [THIS RELEASE]**: baseline event and provenance contract, namespaced proven adapter, installable Python project, CLI and 44+ new tests, inherited 45 tests, verified real historical source replay and audit. Internal outbox remains unacked unless a separate consumer is started.
- **Core v0.2**: formal `SourcePort`, `JournalPort`, `RecoveryPort`, `PublisherPort`, `ReceiptStore` protocols; independently audited DB migration and live bridge; versioned event clock/evidence; orchestrated HTTP consumer.
- **Core v0.3**: full integration of dedicated live single/dual-lane ingress with durable raw-frame WAL, reconnect/timeouts/backoff, bounded REST recovery, and managed lifecycle.
- **Pre-production gates**: OS power failure and `ENOSPC` tests, Windows/NTFS native crash suite, multi-day live sessions, independent clock calibration and P99, security and rate limiting review, retention WORM/offsite audit, actual consumer replay under load.

## No automatic in-place migration

A Core JSONL with eighth flag has a different normalized hash than the historical S07-004.1 JSONL. Do not point an existing S07 database at the new normalized source. A migration tool must prove event equivalence under explicitly documented dropped/added fields, produce a signed/hash-linked ledger, and replay into a **new** database.
