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

**v0.3 (2026-11-07)**: the "later CLI" exists as `archive-producer`: explicit independent `--consumer-db`, a mandatory `audit-e2e-archive` before planning or applying (refused unless COMPLETE, PENDING_DELIVERY or RETRY_REQUIRED), a read-only dry run applying the adapter's own rules (default), and `--apply` as the separate destructive opt-in. The adapter module entry point is unchanged but gained the review fixes (one outbox/delivery row per raw record, lane-checked ACK, exact delete counts, consumer independence).

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

## ADR-007 -- Consumer idempotency retention by tiered, never-forgetting watermarks (ACCEPTED for phase 1, 2026-11-07)

**Context**: the consumer `effects` table is at once the business record, the idempotency memory and the conflict detector. Measured on the full day: 403.6 MiB for 554,441 events (~763 bytes/event), growing linearly. Deleting rows by age would re-open duplicate application and silent acceptance of conflicting replays (producer restored from a backup, or restarted from seq 1).

**Decision (proposed)**: per `(lane, epoch)`, retention moves evidence between three tiers and never forgets an identity's digest: hot rows (`seq > C`), a compact digest table (`D < seq <= C`), and verified hash-chained archive segments (`seq <= D`). `C` is bounded by the consumer's contiguous applied high and, for performance only, by the producer's low-unacked sequence; `D <= C` is an explicit owner choice. No TTL; holes, unknown lanes and other epochs are never covered. `accept()` answers replays from whichever tier holds the identity and refuses the batch (no ACK) if that tier is unreadable. The audit must reconcile compacted tiers before the first real compaction. Full specification, schema v2 proposal, failure matrix and gates: `docs/TASK002_CONSUMER_RETENTION_DESIGN.md`.

**Implemented**: the read-only planner, and (owner approved on 2026-11-07) phase 1 per `docs/CONSUMER_RETENTION_V2.md`: additive schema v2 with an insert-guard trigger, dry-run-by-default `migrate`/`compact`/`rehydrate`, the tier-aware consumer used by `lab-e2e`, and the v2 extension of `audit-e2e-archive`. The digest watermark D and a tier-aware producer prune are phase 2. Compaction of the owner's real data still requires the owner's explicit consent.

**Phase 2, partial (v0.3.0)**: the producer prune (maintained adapter, `check_receipts`) accepts a v2 digest row with the identical payload SHA for the same lane/seq as an independent receipt, so producer archiving and consumer compaction can run in either order; a missing or altered digest refuses the chunk before any change (`tests/test_tiered_prune.py`, `tests/test_producer_retention.py`, full-day acceptance). The digest watermark D remains open.

## ADR-008 -- Timestamp provenance: no latency without a clock model (ACCEPTED for local development, 2026-11-07)

**Context**: the 200-event Windows live pilot had 199 negative `local wall - exchange E` values. Re-analysis (read-only) shows a lower envelope of -601.2 ms (local clock behind the venue), `E - T` of 0-1 ms, and receive times taken with `time.monotonic()`, which on this machine is `GetTickCount64` with 15.625 ms resolution, inside the same loop that persisted each message.

**Decision**: intervals use `perf_counter_ns` only; venue-to-local latency requires a Cristian/NTP-style offset estimate with an explicit, age-growing uncertainty; samples are tagged (`CLOCK_UNCORRECTED`, `OFFSET_STALE`, `OFFSET_UNCERTAIN`, `NEGATIVE_BEYOND_UNCERTAINTY`, `WALL_CLOCK_STEP`, `QUEUED_READ`, `QUEUE_DEPTH_UNKNOWN`) and any tag excludes them from percentiles; percentiles need minimum trusted counts (20/100/1000 for P50/P95/P99) and a report with >1 % negative-beyond-uncertainty samples is INCONCLUSIVE. Venue clock coherence is always stated as an assumption. Specification: `docs/TASK003_TIME_PROVENANCE_DESIGN.md`; implementation: `ftdi_core/timeprov.py`, CLI `analyze-clock-evidence`.

**Consequence**: the pilot yields no latency at all (status NO_CLOCK_MODEL). A future live adapter must timestamp reads in a reader decoupled from persistence and record queue depth and offset samples before any latency claim.

## ADR-009 -- Durability-neutral consumer checkpoint tuning; maintained adapter may diverge from the frozen reference (ACCEPTED, 2026-11-07)

**Context**: the full-day delivery was ~6x slower on the owner's machine because its data lived on a 7200 rpm SATA HDD (fsync ~8.4 ms vs 0.18 ms on the NVMe SSD) and the consumer's SHA `event_id` key dirties random index pages that WAL checkpoints write back (`docs/PERF_WINDOWS_DELIVERY.md`).

**Decision**: `ftdi_core/adapters/s070041/external_delivery.py` `EffectStore` sets `PRAGMA wal_autocheckpoint=16000`. Commits stay `synchronous=FULL`, so durability and the commit-before-ACK contract are unchanged. `synchronous=NORMAL` is rejected. The maintained adapter may carry such documented, tested, semantics-preserving changes; `reference_s07_0041/` stays byte-frozen as compatibility evidence.

**Consequence**: measured -37 % publish time on the HDD (10.77 s -> 6.80 s for 50k events) and -9 % on the SSD. Operational guidance: keep run data on an SSD and use `--batch 1024` for large historical deliveries.

## ADR-010 -- Space reclamation in place under an exclusive lock, after a verified copy (ACCEPTED, 2026-11-08)

**Context**: retention deletes rows, but SQLite only reuses freed pages; the full-day consumer stays ~400 MiB after compaction until a VACUUM (61 MiB). A first design rebuilt into a new file with `VACUUM INTO` and swapped it in by renames. An independent review (Codex, 9 findings) showed that any rename-based swap has windows in which the database path is missing or a late commit is lost, that a checkpoint through a hard-linked sidecar can destroy the only original, that the digest framing allowed a row to masquerade as a table boundary, and that ORDER BY ties made the digest order-dependent.

**Decision**: `vacuum-db` (dry run unless `--apply`) validates the path and sidecars before any SQLite open (no links, junctions or hard links; a non-empty `-wal`/`-shm` must be structurally a SQLite WAL/shared-memory file of this database: magic, version, page size, header checksum), accepts only schema objects FTDI itself creates (no views, virtual/FTS tables, foreign objects or generated columns, since VACUUM may renumber implicit rowids), then holds `locking_mode=EXCLUSIVE` for the whole operation: another connection holding a lock makes it refuse, nobody can open a transaction meanwhile, and the path/sidecar checks are repeated under the lock before the first write. A connection holding no lock is undetectable but cannot change anything. It folds the WAL, makes an exclusive byte copy `<db>.pre-vacuum-<UTC timestamp>`, re-reads both files and verifies the copy's logical digest, and only then runs SQLite's own transactional `VACUUM` in place and re-verifies integrity, settings and the logical digest (framed records, `typeof` + `BINARY` ordering). The database path never disappears; the copy is never deleted by FTDI. Contract: `docs/STORAGE_MAINTENANCE.md`.

A second independent review of this design found 7 issues (foreign sidecars with a plausible header, a sidecar link created between check and open, objects depending on rowids such as external-content FTS, generated columns missing from the digest, partial copies left after an I/O error, a late stat failure without the backup path, an over-stated idle-connection claim); all are fixed or documented with regression tests that fail on the previous design.

**Consequence**: needs free space for one copy plus VACUUM's working space. The owner deletes the copy after verifying, which is when space is actually reclaimed on disk overall. A failure inside VACUUM leaves a consistent database (SQLite transaction) and a named, verified copy.
