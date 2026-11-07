# Task 002 — Consumer idempotency retention (design)

Status: **PROPOSED (ADR-007)**. Design, specification and a **read-only planner** are implemented.
**Destructive compaction is NOT implemented** and requires the owner's explicit approval
(it deletes rows from the consumer DB and changes its schema).

## 1. Problem (measured)

Full day, 554,441 events (2026-11-07, `docs/REAL_ARCHIVE_AUDIT_RESULTS.md`): consumer DB **403.6 MiB ≈ 763
bytes/event**; the normalized source line is ≈ 360 bytes, so about half of each consumer row is the payload
and half is identity text plus two unique indexes (`event_id`, `(lane, seq, epoch)`). Growth is linear and
unbounded: ~150 GB/year for one BTCUSDT aggTrades stream.

The `effects` table currently serves three purposes at once:

1. **Business record**: the consumer's materialized event (payload text).
2. **Idempotency memory**: a replayed `(lane, seq, epoch)` is recognized and not applied twice.
3. **Conflict detection**: a *different* payload for an applied `(lane, seq, epoch)` is rejected (HTTP 409).

Retention must reduce storage without weakening 2 or 3, and without silently discarding 1.

## 2. Invariants (must hold before, during and after any retention step)

| # | Invariant |
|---|---|
| R1 | **Never forget.** Every applied `(lane, epoch, seq)` keeps a verifiable record of its `payload_sha256` forever, in exactly one tier: hot row, digest row, or verified archive segment. Retention moves evidence between tiers; it never deletes the last copy of an identity's digest. |
| R2 | **At most one effect.** A replay of an applied identity never creates a second effect, in any tier, across restarts. |
| R3 | **Conflicts stay visible.** A different payload for an applied identity is rejected in every tier. If the tier that holds the identity cannot be read or verified, the batch is **refused (no ACK)**, never accepted and never treated as a duplicate. |
| R4 | **No time-based deletion.** Watermarks come from verified contiguity and producer ACK evidence, never from age/TTL alone. |
| R5 | **Contiguous prefix only.** Compaction covers only `[first_seq, C]` of a `(lane, epoch)` where every seq has an applied effect. A hole above it must remain deliverable as a first-time event. |
| R6 | **Fail-safe destructive steps.** Dry-run by default; apply needs an explicit flag and owner approval; archive file written, fsynced and re-verified first, then digests + row deletion + watermark advance in **one** consumer transaction (an orphan archive file is preferable to data loss). |
| R7 | **Audit continuity.** `audit-e2e-archive` must reconcile the compacted tiers with the same per-event rigor (consumer-side archive-aware audit) before the first real compaction is allowed. |

## 3. Tiers and watermarks (per `(lane, epoch)`)

```text
seq:   first_seq ........ D ........ C ................ high
tier:  [ archived only  ][ digest   ][ hot effects rows (unchanged v1 table) ]
       payload+digest in  (seq,       full row: event_id, payload_sha256, payload
       verified segments  sha) rows
```

- **C — content watermark.** Rows with `seq ≤ C` have their payload only in verified archive segments; their
  digest `(seq, payload_sha256)` stays in a compact `WITHOUT ROWID` table (≈ 50 bytes/event, ~13× smaller).
- **D — digest watermark, `D ≤ C`.** Digests with `seq ≤ D` live only in the archive segments (each record
  carries its `payload_sha256`). This is what makes storage truly bounded; replays below D take a slow path.
- Holes, a different epoch, or an unknown lane are never covered by a watermark.

## 4. Replay classification in `accept()` (specification)

| Incoming `(lane, epoch, seq, payload_sha256)` | Decision |
|---|---|
| seq > C, no hot row | apply (new effect) |
| seq > C, hot row identical | duplicate: ACK, no effect |
| seq > C, hot row different | 409 conflict (unchanged v1 behaviour) |
| D < seq ≤ C, digest identical | duplicate: ACK, no effect |
| D < seq ≤ C, digest different | 409 conflict |
| D < seq ≤ C, **digest missing** | integrity violation: refuse batch (5xx), no ACK, flag for audit |
| seq ≤ D, archive record identical | duplicate (slow path; segment located by range, verified by SHA + chain before use) |
| seq ≤ D, archive record different | 409 conflict |
| seq ≤ D, **archive missing/unverifiable** | refuse batch (5xx), no ACK; never "assume duplicate" |
| epoch ≠ 0 | rejected as today (no verified epoch re-anchoring yet) |

A refused batch is safe: the producer has not ACKed, so it will retry; an operator must restore the tier.

## 5. Choosing watermarks

- `consumer_contiguous_high(lane, epoch)`: largest seq such that `[first_seq, seq]` is fully applied (computed
  read-only by the planner; a hole caps it).
- `producer_low_unacked(lane)`: smallest seq the producer may still resend in normal operation
  (`MIN(seq) FROM outbox WHERE delivered = 0`, or `lanes.next_seq` if all are ACKed). Archived producer events
  are ACKed by construction.
- **Proposed `C = min(consumer_contiguous_high, producer_low_unacked − 1) − margin`**, margin default 0 events.
  This keeps every *normal* retry (lost HTTP ACK) on the fast hot-row path. Correctness does **not** depend on
  it (R1–R3 hold for any C ≤ contiguous high); it only affects availability/performance of retries.
- **Abnormal replays** (producer restored from an older backup, or a fresh producer run from seq 1 against the
  same consumer) are answered from the digest tier (fast) or the archive (slow) and are never re-applied.
- `D` is an explicit owner choice ≤ C (e.g. keep 30 days of digests hot); still no TTL deletion: D only moves
  digests into already-verified archive segments.

## 6. Proposed consumer schema v2 (additive; requires owner approval)

Existing `effects` table untouched (v1 DBs keep working; "never compacted" = no v2 tables).

```sql
CREATE TABLE consumer_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);         -- schema = FTDI-Consumer-v2
CREATE TABLE retention_state(lane TEXT NOT NULL, epoch INTEGER NOT NULL,
    first_seq INTEGER NOT NULL, content_watermark INTEGER, digest_watermark INTEGER,
    chain_head TEXT, PRIMARY KEY(lane, epoch));
CREATE TABLE effect_digests(lane_id INTEGER NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL,
    payload_sha256 BLOB NOT NULL CHECK(length(payload_sha256) = 32),
    PRIMARY KEY(lane_id, epoch, seq)) WITHOUT ROWID;
CREATE TABLE lane_ids(lane_id INTEGER PRIMARY KEY, lane TEXT NOT NULL UNIQUE);
CREATE TABLE effect_archive_segments(lane TEXT NOT NULL, epoch INTEGER NOT NULL,
    start_seq INTEGER NOT NULL, end_seq INTEGER NOT NULL,          -- inclusive range
    record_count INTEGER NOT NULL, relative_path TEXT NOT NULL,    -- relative to the consumer archive root
    sha256 TEXT NOT NULL, prev_chain TEXT NOT NULL, chain TEXT NOT NULL,
    PRIMARY KEY(lane, epoch, start_seq));
```

Lessons from Task 001 built in: **relative** archive paths (no absolute-path relocation problem), a hash
**chain** `chain = SHA256(prev_chain | lane | epoch | start | end | count | sha256)` whose head can be exported
to an external witness, canonical one-member gzip with `mtime=0`, exact record schema, record count in the
manifest, and the bounded/strict reader from `archive_e2e`.

Migration: additive `CREATE TABLE` only, behind a versioned `consumer_meta.schema`; dual-read (v1 = no v2
tables). Rollback before the first compaction: drop the v2 tables (no data affected). Rollback after a
compaction: a `rehydrate` tool re-inserts rows from verified segments into `effects` (designed with the
compactor; tested on copies).

## 7. Audit extension (required before R7 is met)

`audit-e2e-archive` gains a consumer side mirroring the producer side: verify consumer segments (SHA, chain,
tiling of `[first_seq, C]`, strict record schema, limits), the digest tier against the segments for
`D < seq ≤ C`, and then compare every producer event with the consumer evidence in whichever tier holds it
(identity + `payload_sha256`). A missing tier, a digest/segment disagreement or a broken chain is fatal.

## 8. Failure matrix

| Failure | Required behaviour |
|---|---|
| Crash after archive file written, before the consumer transaction | No row deleted, watermark unchanged; orphan file reported, re-verified or overwritten identically next run |
| Crash after the consumer transaction | Rows gone, digests + segment + watermark present atomically; audit passes |
| Archive segment later corrupted or missing | Replays ≤ D refused (no ACK); replays in digest tier unaffected; audit INCONSISTENT |
| Digest row deleted/tampered | Replay in (D, C] refused as integrity violation; audit detects digest vs segment mismatch |
| Hole in consumer prefix | Planner caps C below the hole; compaction refuses to cross it |
| Producer restored from old backup / restarted from seq 1 | Identical events answered as duplicates from digests/archive; conflicting ones 409; never re-applied |
| Concurrent `accept()` during compaction | `BEGIN IMMEDIATE` serializes; accept sees the state either before or after the single compaction transaction |
| Disk full while writing a segment | Compaction aborts before any deletion; temp file removed |
| Wall-clock jumps | Irrelevant: no time-based decision anywhere |
| Consumer DB restored from an older backup | Watermarks/digests roll back together with rows (same DB); segments newer than the restored manifest are orphans and are never trusted |
| VACUUM | Separate explicit step after a verified compaction; never automatic |

## 9. Rollout gates

| Gate | Content | Status |
|---|---|---|
| T2-1 | This design + ADR-007 accepted by the owner | **PENDING owner decision** |
| T2-2 | Read-only planner `plan-consumer-retention` (dry-run evidence: contiguity, holes, producer ACK low-water mark, proposed C, reclaimable bytes) | DONE (see §10) |
| T2-3 | Consumer schema v2 + migration + `rehydrate`, tested on copies only | needs owner approval (schema change) |
| T2-4 | Consumer-side archive-aware audit (R7) | after T2-3 |
| T2-5 | Compactor `apply` (destructive), crash-injection and tamper tests, real-data run on a copy | needs owner approval (deletes rows) |

## 10. Read-only planner (implemented)

`python -m ftdi_core plan-consumer-retention --consumer-db C.sqlite3 [--producer-root RUN] [--report R.json]`

Per `(lane, epoch)` it reports: applied range, contiguity and the first hole, producer low-unacked seq (when
`--producer-root` is given), the proposed C, rows and payload bytes at or below C, and the estimated digest-tier
cost. It opens every database with the same read-only snapshot rules as `audit-e2e-archive` (`immutable=1`
when quiescent, refusing opens that would create sidecars) and writes nothing except the optional report.
Byte figures are estimates; reclaiming file space would additionally need an explicit VACUUM.
