# Consumer retention v2 (Task 002, phase 1) — interface contract

Status: **implementation contract, local research use, NOT production certified.** Design rationale:
`docs/TASK002_CONSUMER_RETENTION_DESIGN.md`, ADR-007. Phase 1 implements the content watermark **C** only
(hot rows → digest tier + verified archive). The digest watermark D (moving digests into archives) is
reserved (`digest_watermark` column, always NULL in phase 1) and not implemented.

Module: `ftdi_core/consumer_retention.py`. Core consumer service: `ftdi_core/consumer_service.py`.

## 1. Schema v2 (additive; created only by an explicit migration)

The v1 `effects` table is unchanged. A DB without `consumer_meta` is v1 ("never migrated").

```sql
CREATE TABLE consumer_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
-- row ('schema', 'FTDI-Consumer-v2')
CREATE TABLE lane_ids(lane_id INTEGER PRIMARY KEY, lane TEXT NOT NULL UNIQUE);
CREATE TABLE retention_state(
    lane TEXT NOT NULL, epoch INTEGER NOT NULL,
    first_seq INTEGER NOT NULL,          -- first compacted (= first applied) seq of the lane
    content_watermark INTEGER NOT NULL,  -- C: seq in [first_seq, C] lives only in digests + archive
    digest_watermark INTEGER,            -- reserved, NULL in phase 1
    segments INTEGER NOT NULL,
    chain_head TEXT NOT NULL,
    PRIMARY KEY(lane, epoch));
CREATE TABLE effect_digests(
    lane_id INTEGER NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL,
    payload_sha256 BLOB NOT NULL CHECK(length(payload_sha256) = 32),
    PRIMARY KEY(lane_id, epoch, seq)) WITHOUT ROWID;
CREATE TABLE effect_segments(
    lane TEXT NOT NULL, epoch INTEGER NOT NULL, start_seq INTEGER NOT NULL, end_seq INTEGER NOT NULL,
    record_count INTEGER NOT NULL, relative_path TEXT NOT NULL, sha256 TEXT NOT NULL,
    prev_chain TEXT NOT NULL, chain TEXT NOT NULL,
    PRIMARY KEY(lane, epoch, start_seq));
CREATE TRIGGER effects_guard_compacted BEFORE INSERT ON effects
WHEN EXISTS (SELECT 1 FROM retention_state r WHERE r.lane = NEW.lane AND r.epoch = NEW.epoch
             AND NEW.seq BETWEEN r.first_seq AND r.content_watermark)
  OR EXISTS (SELECT 1 FROM lane_ids l JOIN effect_digests d ON d.lane_id = l.lane_id
             WHERE l.lane = NEW.lane AND d.epoch = NEW.epoch AND d.seq = NEW.seq)
BEGIN SELECT RAISE(ABORT, 'FTDI: insert into a compacted range'); END;
```

The trigger makes **any** writer (including the S07 adapter or the frozen reference consumer) fail instead
of inserting a second effect for a compacted identity; it also fires when only the digest row survives (a
damaged `retention_state` must not re-open a compacted identity).

## 2. Archive layout and record format

- Archive root: the sibling directory `<consumer_db>.archive/` (e.g. `effects.sqlite3.archive/`).
- Segment file: `lane-<lane_id>/effects_e<epoch>_<start_seq:012d>_<end_seq:012d>.jsonl.gz`; `relative_path`
  stores exactly this string with `/` separators (never absolute, never `..`). Ranges are **inclusive**;
  `record_count = end_seq - start_seq + 1`; at most 20000 records per segment.
- File bytes: one gzip member (`mtime=0`, empty filename); each line is `canonical(row) + "\n"` with
  `canonical = json.dumps(..., sort_keys=True, ensure_ascii=False, separators=(',', ':'))` and
  `row = {event_id, lane, seq, epoch, payload_sha256, payload}` exactly as stored in `effects`
  (`payload` is the stored canonical payload **text**); lines ordered by seq, contiguous.
- `sha256` = SHA-256 of the gzip file bytes.
- Hash chain per `(lane, epoch)`: the first segment has `prev_chain = "0" * 64`; every segment has
  `chain = SHA256(canonical(["FTDI-Consumer-Segment-v1", prev_chain, lane, epoch, start_seq, end_seq,
  record_count, sha256]))` (hex); `prev_chain` of segment k+1 = `chain` of segment k;
  `retention_state.chain_head` = chain of the last segment.

## 3. Python API (all return dicts; refusals raise `ValueError`, missing files `FileNotFoundError`)

```python
migrate_consumer_v2(consumer_db, *, apply=False)
compact_consumer(consumer_db, *, lane, epoch=0, content_watermark, producer_root=None,
                 ignore_producer_window=False, apply=False, segment_size=20000, crash_stage=None)
rehydrate_consumer(consumer_db, *, lane, epoch=0, to_watermark=None, apply=False)
TieredEffectStore(path)          # drop-in for external_delivery.EffectStore
TierIntegrityError(Exception)    # raised by accept() when a compacted tier cannot prove a replay
```

- **Default is dry run** (`apply=False`): nothing is written; the result describes what would happen
  (`status`: `DRY_RUN`, or `NOTHING_TO_DO`). With `apply=True`: `MIGRATED` / `ALREADY_V2`, `COMPACTED`,
  `REHYDRATED`.
- `migrate_consumer_v2`: requires a recognized v1 `effects` table (plain table, exact columns,
  UNIQUE(event_id) and UNIQUE(lane, seq, epoch)); idempotent (`ALREADY_V2`). Never touches `effects` rows.
- `compact_consumer` refuses (ValueError) when: the DB is not v2; `lane` has no effects; the watermark is
  not above the current C; the range `[current C + 1 (or the lane's first seq), content_watermark]` has a
  hole or exceeds the applied range; any row in it is invalid (event_id/payload hash/canonical text);
  neither `producer_root` nor `ignore_producer_window=True` is given; with `producer_root`, the watermark
  is ≥ the producer's low-unacked seq (`MIN(seq) WHERE delivered=0`, else `lanes.next_seq`) or the lane is
  not registered there; an existing segment file with the target name has different bytes (checked for
  every planned segment before anything is written, also in a dry run); `epoch != 0`.
  `lane` is the exact lane scope string (JSON array text, as stored in `effects.lane`).
- Every planned segment must be readable by the bounded audit reader (`ArchiveLimits` defaults: compressed
  size, decompressed size, line length, records); otherwise the operation is refused before anything is
  written (dry run included). Archive directories reached through a symlink/junction are refused.
- The lane id is allocated in its own committed transaction before any segment file is written.
- Apply procedure per segment, inside one `BEGIN IMMEDIATE` held for the whole segment: read and
  validate rows → write `.tmp`, fsync, re-read and verify every line → `os.replace` to the final name →
  insert digests, segment row, update/insert `retention_state` → delete exactly those `effects` rows →
  COMMIT. `crash_stage` (test only): `'after_segment_file'` = `os._exit(86)` after the final file exists
  but before COMMIT; `'after_commit'` = `os._exit(86)` right after COMMIT.
- `rehydrate_consumer` restores rows from verified segments, newest first, down to `to_watermark`
  (default: everything, i.e. `first_seq - 1`). A `to_watermark` inside a segment is rounded **down** to the
  segment boundary (the resulting C is the largest boundary <= `to_watermark`), so at least the requested
  range becomes hot again; values outside `[first_seq - 1, C - 1]` are refused. Every segment to restore is
  read (bounded, confined) and checked against its digests before the first change. Per segment, in one transaction: lower C to
  `start_seq - 1` (or drop the state row when nothing remains), insert the rows, delete their digests,
  delete the segment row, set `chain_head = prev_chain`. **Archive files are never deleted.** A rehydrated
  `effects` table is row-for-row identical to the pre-compaction table.
- Neither compaction nor rehydration runs VACUUM (separate, explicit, out of scope).

## 4. `TieredEffectStore.accept(events)` semantics

Same input validation, batch limits, return shape (`accepted`, `inserted`, `duplicates`, `batch_digest`)
and `Conflict` exception as `EffectStore.accept`, inside one `BEGIN IMMEDIATE`. For each event:

| Case | Result |
|---|---|
| lane/epoch has state and `first_seq <= seq <= C`, digest equals | duplicate (no effect) |
| same, digest differs | `Conflict` (HTTP 409), batch rolled back |
| same, digest row missing | `TierIntegrityError` (HTTP 503), batch rolled back, nothing ACKed |
| a digest row exists but `retention_state` does not cover the seq | `TierIntegrityError` (state damaged) |
| otherwise | exactly v1 behaviour (hot row: duplicate / Conflict / insert) |

On a v1 DB `TieredEffectStore` behaves exactly like `EffectStore`.
`ftdi_core.consumer_service` (same CLI flags as the adapter's consumer service) serves `TieredEffectStore`
and maps `TierIntegrityError` to 503; `lab-e2e` uses it.

## 5. CLI

```text
python -m ftdi_core consumer-retention migrate   --consumer-db DB [--apply]
python -m ftdi_core consumer-retention compact   --consumer-db DB --lane LANE --watermark N
        (--producer-root RUN | --ignore-producer-window) [--epoch 0] [--segment-size N] [--apply]
python -m ftdi_core consumer-retention rehydrate --consumer-db DB --lane LANE [--to-watermark N] [--apply]
```
`compact` also accepts `--crash-stage after_segment_file|after_commit` (test only; process exits with 86).
`LANE` is a registry alias when `--producer-root` is given, otherwise the exact lane scope string.
Exit codes: 0 = success or dry run; 3 = refusal / precondition failure (`FTDI_CORE_FAIL_CLOSED: ...`).
JSON on stdout (ASCII-only).

## 6. Operational constraints (phase 1)

- **Archive the producer before compacting the consumer** for the same range: the S07 producer prune
  (`retention.prune`) accepts only consumer *hot rows* as receipts, so after consumer compaction those
  producer rows can no longer be pruned (fail-closed, never unsafe). A tier-aware receipt check is phase 2.
- `lab-e2e` audits with `audit-e2e-archive` automatically when a producer lane has an archived prefix or the
  consumer is v2 (the resident-only `audit-e2e` refuses both by design); otherwise unchanged
  (`audit_mode` in the lab report).
- Run compaction in a maintenance window: each segment holds `BEGIN IMMEDIATE` and blocks consumer writes
  for its duration (busy_timeout 30 s).
- File space is reclaimed only by an explicit VACUUM, which is out of scope here.

## 7. `audit-e2e-archive` on a v2 consumer

v1 consumers are audited exactly as before. When `consumer_meta.schema = 'FTDI-Consumer-v2'`, the v2
objects (tables with exact columns, the trigger) must be recognized, and for every `retention_state` row:
segments tile `[first_seq, C]`, the chain verifies from genesis to `chain_head`, `relative_path` equals the
canonical name, files are read under the same limits/strictness as producer archives, every record is a
valid effect of that lane/epoch, every record's digest row exists and is equal, there are no digest rows
outside `[first_seq, C]`, and no hot `effects` row lies inside `[first_seq, C]`. Producer events in a
compacted range are matched against the digest tier (identity + payload SHA-256); digest rows count as
consumer effects for orphan detection, totals and the identity digest.

New FATAL finding codes: `CONSUMER_SEGMENT_MISSING`, `CONSUMER_SEGMENT_SHA256_MISMATCH`,
`CONSUMER_SEGMENT_UNREADABLE`, `CONSUMER_SEGMENT_CONTINUITY`, `CONSUMER_SEGMENT_PATH_INVALID`
(`relative_path` not the canonical name), `CONSUMER_CHAIN_MISMATCH`,
`CONSUMER_SEGMENT_RECORD_INVALID`, `CONSUMER_DIGEST_MISSING`, `CONSUMER_DIGEST_MISMATCH`,
`CONSUMER_DIGEST_EXTRA`, `CONSUMER_TIER_OVERLAP`, `CONSUMER_TIER_CONTENT_CONFLICT`
(malformed v2 objects or a missing trigger → `CONSUMER_SCHEMA_UNRECOGNIZED`).
New NOTICE: `UNREFERENCED_CONSUMER_SEGMENT_FILE`. New metrics: `consumer_compacted_effects`,
`consumer_segments`, `consumer_segments_parsed`. A fully compacted, consistent consumer audits `COMPLETE`.
