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

## ADR-006 -- Archive-aware E2E audit as a separate read-only mode anchored on source bytes (ACCEPTED for local development, 2026-11-07)

**Context**: `audit-e2e` (v0.2) refuses any lane with an archived prefix. The legacy `retention.audit` can read archives but (measured on Windows 11 / Python 3.11.9 / SQLite 3.45.1): creates `archive_segments` plus three indexes in the producer DB and creates a missing consumer DB file; checks only that each archived `event_id` exists in the consumer; follows the stored absolute `archive_path`; and accepts a self-consistent forgery (archive rewritten, manifest SHA updated). After a prune, archived `QUARANTINED_*` dispositions also vanish from `ftdi_core.audit`, flipping it from FAIL to PASS. Market `source_seq` values can legitimately interleave across the archive boundary (archived `[1,2,3,5]`, resident `[4,6,7,8]`). SQLite `mode=ro` creates `-wal`/`-shm` sidecars on a cleanly closed WAL database.

**Decision**:
1. New explicit mode `audit-e2e-archive` / `ftdi_core.archive_e2e.reconcile_archive_aware`; `audit-e2e`, `e2e.py`, the S07 adapters, `reference_s07_0041/`, all schemas and the data contract stay unchanged.
2. Trust anchors: the immutable source bytes are the primary anchor (every `file_index` in `[0, next_file_index)` must match an archived or resident `raw_sha256`); the consumer `effects` DB is the independent delivery evidence (exact `event_id`, `(lane, seq, epoch)`, `payload_sha256` and canonical payload). Registry/meta hashes, `archive_segments.archive_sha256`, archived `acknowledged: true` and `source_manifest.json` are producer-domain **claims**: cross-checked, never accepted as proof on their own.
3. `raw_journal.id`, `file_index` and market `seq` are separate identity domains: `id` must strictly increase with `file_index`; `file_index` must be contiguous; published `seq` values must be unique and contiguous up to `lanes.next_seq - 1`, in any archive/resident interleaving.
4. An archived `QUARANTINED_*` record makes the run `INCONSISTENT`. Archived events can never be `PENDING`/`RETRY`. Every disposition is a producer claim and is proven from evidence: `ORDERED_PUBLISHED` by the raw-to-outbox binding, `DUPLICATE_IDENTICAL` by a byte-identical published event with the same seq (otherwise `DISPOSITION_CLAIM_UNVERIFIED`). Without this, relabelling a quarantined record or a never-delivered event as a duplicate produced a false `COMPLETE` (found by the independent review).
5. Only the canonical in-root segment path is opened; the stored `archive_path` is compared as a string and never opened. A differing stored path is fatal unless `--allow-relocated-archives`. Path conflicts involving registered segments (duplicate stored paths, non-regular files, `.tmp` siblings, directories escaping the root) are fatal. Unregistered segment files are notices unless they overlap the archived range with different or unverifiable content (fatal).
6. Strictly read-only: `mode=ro` + `query_only`, `immutable=1` when quiescent (no WAL frames, no `-journal`); a database whose read-only open would itself create `-shm` or roll back a hot journal is refused (`SNAPSHOT_UNVERIFIABLE`). Evidence tables must be plain `CREATE TABLE` objects with the exact `pragma_table_xinfo` columns (a recursive view could otherwise hang the audit). The local checks of `ftdi_core.audit` are re-implemented over these read-only snapshots instead of calling `StreamDB`. The only write is an optional report to an explicit path outside `lanes/` and `archives/`, vetted literally and after resolving links/junctions.
7. Cross-database consistency: pre/post fingerprints of all evidence (SQLite main/`-wal`/`-journal` with SHA-256, registry bytes, sources, archive listings that never enumerate link targets), `PRAGMA data_version` on WAL databases, and a post-audit **content** re-hash of every source, source manifest and archive file relied upon (stat alone misses a same-size rewrite with a restored mtime); any change or unverifiable fingerprint forbids `COMPLETE`.
8. Hard resource bounds on compressed/decompressed bytes, line length, records, metadata size, directory entries, overlap checks, integer length (<= 18 digits) and report size (unknown labels aggregated); files are read as `size + 1` bytes, never `limit + 1` (`BufferedReader.read(n)` pre-allocates `n`).
9. Contract and finding codes: `docs/ARCHIVE_E2E_AUDIT.md`. Verification is independent by construction: 48 black-box tests were written from the contract by a separate agent (Codex) without reading the implementation, and a separate adversarial review found 8 defects (C1-C8), each fixed with a regression test in `tests/test_archive_e2e_review.py`.

**Consequences**: stricter than the resident-only audit (source anchoring, raw-to-outbox binding, disposition and sequence checks), so healthy runs still reach `COMPLETE` while self-consistent forgeries and hidden quarantine do not. Costs one extra sequential read per source, one indexed consumer lookup per published event, and SHA-256 of each SQLite file before and after. It does not defend against an actor able to rewrite source, registry, producer DB, archives and consumer DB together (no signatures/WORM), and does not certify events before the first observed sequence.

## ADR-007 -- Consumer idempotency retention by tiered, never-forgetting watermarks (PROPOSED, 2026-11-07)

**Context**: the consumer `effects` table is at once the business record, the idempotency memory and the conflict detector. Measured on the full day: 403.6 MiB for 554,441 events (~763 bytes/event), growing linearly. Deleting rows by age would re-open duplicate application and silent acceptance of conflicting replays (producer restored from a backup, or restarted from seq 1).

**Decision (proposed)**: per `(lane, epoch)`, retention moves evidence between three tiers and never forgets an identity's digest: hot rows (`seq > C`), a compact digest table (`D < seq <= C`), and verified hash-chained archive segments (`seq <= D`). `C` is bounded by the consumer's contiguous applied high and, for performance only, by the producer's low-unacked sequence; `D <= C` is an explicit owner choice. No TTL; holes, unknown lanes and other epochs are never covered. `accept()` answers replays from whichever tier holds the identity and refuses the batch (no ACK) if that tier is unreadable. The audit must reconcile compacted tiers before the first real compaction. Full specification, schema v2 proposal, failure matrix and gates: `docs/TASK002_CONSUMER_RETENTION_DESIGN.md`.

**Implemented now**: only the read-only planner `plan-consumer-retention` (dry run).

**Requires the owner's approval before implementation**: consumer schema v2 (additive tables), the destructive compactor and `rehydrate`. Until then nothing deletes or compacts consumer data.
