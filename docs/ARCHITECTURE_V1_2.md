# FTDI-Core v0.3 — Architecture addendum v1.2

Status: **ACCEPTED FOR OFFLINE LOCAL DEVELOPMENT**, **NOT PRODUCTION CERTIFIED**. Extends
`ARCHITECTURE_V1.md` and `ARCHITECTURE_V1_1.md`; everything stated there still holds. The event contract
`FTDI-Core-Event-v1`, the S07-004.1 envelope and the producer schemas are unchanged.

## Data path with retention tiers

```text
Immutable source ZIP + checksum
  -> prepare: canonical JSONL (8 CSV fields kept) + source_manifest.json
  -> replay:  per-lane journal (raw) + gap-safe ordered outbox            [producer state.sqlite3, WAL, FULL]
  -> lab-e2e: localhost HTTP, at-least-once, batch digest
  -> consumer (separate process): commit effects BEFORE the HTTP answer   [consumer effects.sqlite3]
  -> producer: delivery ACK + outbox.delivered=1

Producer retention (archive-producer: audit-e2e-archive first, dry run by default; S07 adapter prune does the work)
  resident outbox prefix -> verified gzip segments (<run>/archives/<lane>/, SHA-256, manifest tiling)
  receipt per event: consumer hot row with the same event_id, OR consumer digest row with the same payload SHA

Consumer retention v2 (consumer-retention, dry run by default; additive schema)
  hot effects (seq > C) | digest tier: (lane_id, epoch, seq, sha256) for first_seq <= seq <= C
                         | content: hash-chained gzip segments in <consumer>.archive/ (never deleted)
  insert-guard trigger: no writer may insert into [first_seq, C]

Audit
  audit      producer journal/outbox integrity (v0.1)
  audit-e2e  resident-only, refuses archived prefixes (v0.2, unchanged)
  audit-e2e-archive  read-only, every event across source lines, raw journal, outbox (resident + archived),
                     consumer (hot + digest + segments), producer ACKs
```

## Invariants added in v0.3

1. **Never forget an identity.** Every applied `(lane, epoch, seq)` keeps either a hot row or a digest row.
   Replays of compacted identities are answered from the digest: identical → duplicate ACK, different → 409,
   unreadable tier → 503 without ACK. There is no TTL and no age-based deletion.
2. **Evidence before deletion.** Producer retention fsyncs each segment and re-checks its SHA-256; consumer
   retention additionally reads every segment back through the bounded audit reader. Both delete rows only
   inside the same transaction that registers the segments. Archive files are never deleted by any command.
3. **Read-only audit.** `audit-e2e-archive` opens SQLite through `mode=ro` snapshots, compares pre/post
   fingerprints, `data_version` and a content re-hash; any change or unverifiable state forbids `COMPLETE`.
4. **Fail closed on every tier.** Missing/altered segment, broken hash chain, digest mismatch, hole, unknown
   lane, oversize or linked directory → refusal or `INCONSISTENT`, never a silent skip.
5. **Either retention order.** A consumer digest with the identical payload SHA is a valid producer-prune
   receipt (ADR-007 phase 2, partial), so producer archiving and consumer compaction are independent
   maintenance steps.
6. **No latency without a clock model** (ADR-008).
7. **Retention never hides an inconsistency.** `archive-producer` runs `audit-e2e-archive` before planning or
   applying and refuses unless the run is `COMPLETE`, `PENDING_DELIVERY` or `RETRY_REQUIRED`; every record is
   validated by the auditor's own parser before it is written; archived rows are exactly the rows of the
   committed segment (each deleted row revalidated against its archived record, delete and table counts
   checked in the transaction, no triggers).

## Module map (new in v0.3)

| Module | Role |
|---|---|
| `ftdi_core/archive_e2e.py` | archive-aware read-only auditor (ADR-006) |
| `ftdi_core/consumer_retention.py` | schema v2, migrate / compact / rehydrate, `TieredEffectStore` (ADR-007) |
| `ftdi_core/consumer_service.py` | tier-aware localhost consumer used by `lab-e2e` |
| `ftdi_core/retention_plan.py` | read-only retention planner |
| `ftdi_core/producer_retention.py` | `archive-producer`: strict read-only dry run, chunked `--apply`, stop reason |
| `ftdi_core/timeprov.py` | timestamp provenance analysis (ADR-008) |
| `ftdi_core/adapters/s070041/retention.py` | producer prune; accepts digest receipts |
| `scripts/acceptance_full_day.py` | one-command full-day acceptance |
| `scripts/build_release.py`, `scripts/Check-Release-Manifest.py` | release manifest and ZIP |

## Still outside the architecture

Authenticated or cross-host delivery, a live market adapter (reader decoupled from persistence, queue depth,
offset sampling), the digest watermark D (moving digests into segments), automatic VACUUM, backups,
signatures/WORM evidence, distributed snapshots. Strong audits still assume a quiescent maintenance window.
