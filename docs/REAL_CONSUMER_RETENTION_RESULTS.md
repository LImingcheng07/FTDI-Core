# Task 002 phase 1 — full-day real-data consumer retention (gate T2-5, generated copies)

Executed **2026-11-07** on the owner's machine (native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1) with
the final code (commit `cb8c519`, after the independent review fixes). Input: the owner's original
`BTCUSDT-aggTrades-2024-06-01.zip`, read-only (SHA-256 unchanged before/after). All databases, archives and
copies were generated in a new scratch directory on the NVMe SSD. **The owner's own consumer data was not
compacted.** Machine-readable evidence: `examples/REAL_CONSUMER_RETENTION_EVIDENCE.json`.

## Pipeline and results

| Step | Exit | Wall | Outcome |
|---|---:|---:|---|
| `prepare --limit 0` → `replay` | 0 | 9.9 s + 34.6 s | 554,441 events, contiguous |
| `lab-e2e --batch 1024` (separate tier-aware consumer process, localhost HTTP) | 0 | 110.5 s | COMPLETE; publish phase 55.0 s on NVMe (349 s earlier on the HDD) |
| S07 producer prune of 500,000 events (first: it needs consumer hot rows as receipts) | 0 | 40.0 s | 25 producer segments |
| `audit-e2e-archive` (producer archived, consumer v1) | 0 | 79.5 s | COMPLETE, 554,441 matched |
| `plan-consumer-retention --producer-root` | 0 | 0.8 s | PLANNED |
| `consumer-retention migrate` dry run → `--apply` | 0 | 0.2 s / 0.3 s | DRY_RUN → MIGRATED |
| `consumer-retention compact` dry run (every segment validated with the audit reader) | 0 | 46.2 s | 500,000 rows, 25 segments |
| `consumer-retention compact --apply` | 0 | 133.7 s | COMPACTED, 54,441 hot rows left |
| **`audit-e2e-archive` (producer AND consumer compacted)** | **0** | **85.8 s** | **COMPLETE**: 500,000 digest-tier effects, 25/25 consumer segments verified, 554,441 matched |
| New producer run from the same source → `lab-e2e` (**every event resent**) | 0 | 117.6 s | 554,441 sent/ACKed, **554,441 duplicates, 0 inserts**; hot rows 54,441 → 54,441; audit (auto archive-aware) COMPLETE |
| One bit flipped in a consumer segment (copy) | 2 | 83.6 s | INCONSISTENT, `CONSUMER_SEGMENT_SHA256_MISMATCH` |
| S07 adapter `EffectStore.accept` replay into a compacted range (copy) | — | — | blocked by the trigger: `FTDI: insert into a compacted range` |
| `rehydrate` dry run → `--apply` (copy) | 0 | 22.5 s / 76.3 s | REHYDRATED, 25 segments; **all 554,441 rows identical** to before compaction (content SHA-256 `07cec77c…a2574`); archive files kept |
| `audit-e2e-archive` (rehydrated copy vs restarted producer) | 0 | 69.8 s | COMPLETE (25 notices: archive files kept) |

## Storage

| | Size |
|---|---:|
| Consumer DB before compaction | 397.3 MiB |
| Consumer DB after compaction, before VACUUM | 404.6 MiB (deleted pages are only reclaimed by VACUUM) |
| Consumer DB after compaction + VACUUM (on a copy) | **60.9 MiB** (−85 %): 54,441 hot rows + 500,000 digests |
| Consumer archive (25 verified gzip segments, relative paths, hash chain) | 50.8 MiB |

## Observations
- Correctness is independent of the producer ACK window; the window only keeps normal retries on the hot path.
- Compaction is ~2× slower than before the review fixes because every segment is re-read by the bounded
  audit reader during planning and again inside its transaction ("never write an archive we cannot read
  back"). An optimization candidate, not a correctness issue.
- Phase-1 constraint confirmed: archive the producer before compacting the consumer for the same range.
  **Resolved in v0.3.0**: the producer prune accepts identical consumer digests as receipts; the v0.3.0
  acceptance compacts the consumer between two producer archive steps (`examples/V030_ACCEPTANCE_EVIDENCE.json`).
- VACUUM was run only on a copy; a VACUUM tool for real data is a separate, explicit step.
