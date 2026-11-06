# FTDI-Core Architecture Decision Records

## ADR-001 -- Version the S07-004.1 compatibility adapter (ACCEPTED)

**Context**: The previous S07 experiments succeeded independently but have overlapping/partly incompatible storage, timestamp, and delivery schemas.

**Decision**: Integrate the known S07-004.1 durable lane / external dedup engine behind an explicit `ftdi_core.adapters.s070041` compatibility namespace. Retain a byte-level copy of the old S07-004.1 source/tests in `reference_s07_0041/` for regression. Package the new application services, entry point and event contract separately. No silent in-place data migration, no unverified rewrite of proven SQLite transaction logic.

**Consequence**: Some temporary duplication is intentional. Both the new 44+ tests and old 45-test suite must stay green. A later native storage rewrite must compare logical outbox event IDs and fault invariants with this baseline.

## ADR-002 -- Separate source-byte identity, normalized event identity, receipt identity (ACCEPTED)

**Context**: Windows CRLF/LF and metadata augmentation already changed JSONL hashes. The original archive contains an eighth boolean ignored by S07-004.1.

**Decision**: Preserve `is_best_match` when supplied by source, and record original ZIP hash, normalized JSONL hash, canonical event-stream hash, and consumer event ID separately. Never claim a normalized file hashes to the original ZIP or to an older normalization format.

**Consequence**: Core-v0.1 normalized JSONL hashes intentionally differ from S07-004.1. This change is allowed only because the raw archive hash is independently retained and no automatic mutation of an existing S07 database occurs.

## ADR-003 -- No default destructive archiving or external effects in Core CLI (ACCEPTED)

**Context**: S07-004.1 proved a receipt-gated archive only with an independent consumer database. The S07-005 realtime adapter has not certified long-lived consumer ACK semantics.

**Decision**: Core v0.1 CLI exposes safe `prepare`, `replay`, `audit` commands. Cross-process publisher, consumer and retention remain executable as a versioned adapter with inherited tests but are not automatically enabled. A later CLI must require explicit `consumer_db`, archive audit and a dry-run plan.

**Consequence**: User can develop and audit without accidentally pruning source data or starting a trading service.

## ADR-004 -- Live ingress is a gated boundary, not a shortcut (ACCEPTED)

**Context**: Windows live WS proved 200 observed BTCUSDT events but 199/200 local minus exchange timestamp deltas were negative; real reconnect and 24h uptime are untested.

**Decision**: Design live-ingress interface in architecture but do not import or automatically start the S07-005 network loop in Core v0.1. Add live clock synchronization, reconnect/rest backfill and raw-frame persistence with independent tests before promotion.

**Consequence**: Production claims remain prohibited; no misleading `P99` measurement.

## ADR-005 -- Fail-closed hard capacity (ACCEPTED)

**Context**: A missing early source ID can fill durable pending. Silently dropping later events would destroy provenance.

**Decision**: Per-lane pending is capacity-bounded; exhausted lanes stop source ACK. Other registered lanes are scheduled fairly. A blocked result is a valid safety outcome, not a successful completed replay.

**Consequence**: Operators must handle unresolved blocked lanes and resource budgets. Shared disk-failure isolation is still a future goal.
