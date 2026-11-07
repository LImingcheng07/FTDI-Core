# FTDI-Core v0.3.0 — release notes

Date: 2026-11-07. Platform verified: native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1.
Status: **local research prototype; not production certified; not a trading program.** Stdlib only, no new
dependencies, no outbound network (localhost HTTP only). Chinese user guide: `docs/USER_GUIDE_zh.md`.

## What is new

### 1. Archive-aware end-to-end audit — `audit-e2e-archive` (Task 001, ADR-006)
Read-only reconciliation of **every** event across the source JSONL, the producer journal/outbox (resident
*and* archived gzip segments) and the independent consumer (hot rows *and* compacted tiers).

* Anchors every source line by byte offset and SHA; binds raw journal rows to outbox events; verifies
  `DUPLICATE_IDENTICAL` claims; sequence contiguity by bitmap.
* Opens SQLite only through read-only snapshots (`mode=ro`, `immutable=1` only when provably quiescent);
  refuses a WAL without `-shm` and a hot `-journal`. Pre/post fingerprints, `data_version` and a post-audit
  content re-hash: any change during the audit forbids `COMPLETE`.
* Bounded resources on hostile archives (compressed/decompressed/line/record/metadata/directory limits).
* Four statuses (`COMPLETE`, `PENDING_DELIVERY`, `RETRY_REQUIRED`, `INCONSISTENT`), exit codes 0/2/3, fatal
  vs NOTICE finding codes. Contract: `docs/ARCHIVE_E2E_AUDIT.md`.
* `lab-e2e` switches its final audit to the archive-aware mode automatically when the producer has an archived
  prefix or the consumer is schema v2 (`audit_mode` in the output).

### 2. Consumer retention v2 — `consumer-retention migrate|compact|rehydrate`, `plan-consumer-retention` (Task 002 phase 1, ADR-007)
The consumer `effects` table no longer has to grow forever, and it never forgets an identity.

* Additive schema v2: `lane_ids`, `effect_digests` (lane, epoch, seq, 32-byte payload SHA), segment registry
  with hash-chained gzip segments under `<consumer>.archive/`, and an insert-guard trigger
  `effects_guard_compacted` that blocks any writer (including the v0.2 adapter) from inserting into a
  compacted range.
* Dry run is the default; `--apply` is required for every change. Every segment is read back by the bounded
  audit reader before and inside the compaction transaction. Archive files are never deleted.
* The tier-aware consumer (`ftdi_core/consumer_service.py`, used by `lab-e2e`) answers replays from any tier:
  identical → duplicate ACK; different content → HTTP 409; unreadable tier → HTTP 503 (no ACK).
* `rehydrate` restores the hot rows byte-identically. Contract: `docs/CONSUMER_RETENTION_V2.md`.

### 3. Producer archive in the Core CLI — `archive-producer`; tier-aware receipts (ADR-003, ADR-007 phase 2 partial)
* New Core subcommand `archive-producer --outdir RUN --lane ALIAS --consumer-db DB [--limit N] [--apply]`.
  Previously the producer archive was reachable only through the S07 adapter module, which applies
  immediately. The Core command is a **dry run unless `--apply`**, and both first run `audit-e2e-archive`:
  unless it is `COMPLETE`, `PENDING_DELIVERY` or `RETRY_REQUIRED` the archive is refused, so archiving can never
  erase the evidence of an inconsistency. The dry run reads read-only snapshots (no data or evidence file
  changes; only SQLite's `-shm` coordination file may be touched), applies the adapter's own archivability and
  receipt rules unchanged, validates every record with the auditor's own archived-record parser (an archive the
  audit could not read back is never written), predicts segment-file collisions, overlapping leftover segment
  files and blocked temp paths, refuses if a database changed meanwhile (content fingerprints), and
  reports `would_archive`, the receipt sources and why it stops (`LIMIT_REACHED`, `NOT_ACKNOWLEDGED`,
  `END_OF_JOURNAL`, ...). `--apply` plans every chunk the same way and requires the committed segment to have
  exactly the planned SHA-256; after any failure it reports the progress actually committed (read back from the
  producer cursor).
* The adapter prune now also refuses several outbox/delivery rows on one raw record, an ACK for another lane,
  an invalid delivered flag, triggers in the producer DB, and a consumer DB that is a producer lane DB; it checks
  every row it deletes against the archived record (raw, disposition, outbox, ACK), exact delete counts and
  table counts inside its transaction, and releases its connection when initialisation
  fails. Direct use of the adapter module stays a low-level tool without the Core audit gate (documented).
* The producer prune accepts a consumer **digest row with the identical payload SHA** as an independent
  receipt, so producer archiving and consumer compaction can run in either order. A missing or altered
  receipt refuses the chunk before any change.

### 4. Timestamp provenance — `analyze-clock-evidence` (Task 003, ADR-008)
Read-only analysis of live-capture timestamps. Without a clock-offset model no latency or percentile is
reported; tagged samples are excluded; minimum trusted counts 20/100/1000 for P50/P95/P99. The earlier 200-event
pilot yields **no latency** (local clock ~0.6 s behind the venue; coarse `time.monotonic()`).

### 5. Windows performance (ADR-009)
The 6× slowdown on the owner's machine was the HDD (fsync 8.4 ms vs 0.18 ms on NVMe), not the code. Consumer
`wal_autocheckpoint=16000` (durability unchanged, `synchronous=FULL`): −37 % publish time on HDD, −9 % on SSD.
Guidance: keep run data on an SSD, use `--batch 1024` (`docs/PERF_WINDOWS_DELIVERY.md`).

### 6. Tooling
* `python -m ftdi_core --version`; ASCII-only JSON CLI output (fixes a GBK console crash).
* `scripts/acceptance_full_day.py`: one-command full-day acceptance on the user's own ZIP (read-only input,
  new work dir, evidence JSON).
* `scripts/build_release.py --version X.Y.Z`: per-file SHA-256 manifest `release/PACKAGE_MANIFEST.v<ver>.json`
  (schema v2, linked to the v0.2 baseline by SHA) and a deterministic ZIP; refuses a dirty tree and overwrites.
* `scripts/Check-Release-Manifest.py`: verifies the newest release manifest and reports MISSING / MISMATCH /
  EXTRA files; the v0.2 baseline can still be checked explicitly.

## Compatibility

* Event contract `FTDI-Core-Event-v1`, the S07-004.1 envelope, producer schemas and the frozen
  `reference_s07_0041/` are unchanged. `PACKAGE_MANIFEST.json` (v0.2 baseline) is byte-identical.
* `audit-e2e` (v0.2) behaves exactly as before, including its explicit refusal of archived prefixes.
* Consumer schema v2 is additive and opt-in (`consumer-retention migrate --apply`). An unmigrated consumer
  works as in v0.2. After migration and compaction, the v0.2 adapter can still read hot rows and is blocked by
  the trigger from writing into compacted ranges (fail closed, no silent duplicate).
* No data migration is performed automatically; nothing of the user's existing data is modified by upgrading.

## Verification

| Suite | Result |
|---|---|
| Core (`python -W error::ResourceWarning -m unittest discover -s tests`) | **296 / 296 OK** (65 v0.2 baseline + 231 new) |
| Frozen S07 compatibility (`reference_s07_0041/`) | **45 / 45 OK** |
| Independent black-box contract tests (Codex, written from the contracts only) | 48 audit + 75 retention, included above |
| Independent adversarial reviews | audit: C1–C8; retention: R1–R6; producer archive: P1–P8 and follow-ups Q1–Q8 — fixed with mutation-checked regressions (P7, slow tiny chunks, documented) |
| Full-day real data (554,441 events, owner's ZIP read-only) | `scripts/acceptance_full_day.py`: **ACCEPTANCE PASS** — `examples/V030_ACCEPTANCE_EVIDENCE.json` |

Release gates and evidence: `docs/RELEASE_GATES_V03.md`.

## Known limits (unchanged by this release)

Not production certified. No authenticated remote delivery, no live market adapter in this release, no
trusted latency or P99, no 24-hour uptime, no power-loss or backup/restore certification, no signatures or
WORM storage for evidence. `COMPLETE` proves the consistency of the supplied local evidence chain for the
observed range, not venue completeness before the first sequence. Compaction does not shrink the database file
until a VACUUM, which this release does not perform on real data.

## Upgrade and rollback

* Upgrade: extract the ZIP into a new directory; existing run directories and consumer DBs can be used as-is.
* Rollback: v0.2 can be used again on any producer run and on any consumer that was **not** compacted. For a
  compacted consumer, first `consumer-retention rehydrate --apply` with v0.3 (restores every hot row
  byte-identically), then v0.2 can read and write it normally. The additive v2 tables do not affect v0.2.
