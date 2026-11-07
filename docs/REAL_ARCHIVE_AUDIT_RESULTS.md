# Task 001 gate A11 — full-day real-data archive-aware audit

Executed **2026-11-07** on the owner's machine: **native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1**.
Input: the owner's original public Binance archive `BTCUSDT-aggTrades-2024-06-01.zip`, read in place,
**read-only**, from the owner's S07-002 lab directory. All artefacts were written to a new, git-ignored
work directory `runs/task001-real-2026-11-07/` (2.8 GB). Nothing else was modified.

Orchestration scripts (not in the repo): `%TEMP%\ftdi_codex\real_run.py`, `verify_final.py`.
Machine-readable summary: `examples/REAL_ARCHIVE_AUDIT_EVIDENCE.json`.

## Provenance

| Item | Value |
|---|---|
| ZIP SHA-256 (computed before and after the run) | `78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9`, unchanged |
| Companion checksum file (`.zip.zip`) | same value → `independent_checksum_provided: true` |
| Owner's source directory | 16 files, identical size and mtime before/after |
| Records (normalization `FTDI-CSV-AGGTRADES-v1`, 8 CSV fields kept) | 554,441, aggregate IDs 3023902824 … 3024457264, 0 gaps in file |
| Normalized JSONL SHA-256 | `1f157174bc3555276e596074c13e34b6fb11b8fa8f557654a3c88ec2c56fdb0a` |
| Canonical events SHA-256 | `c47af609dca14787ee9212f29eba978382aa41b0c00a8ff6f1a922fe44aea538` |

Found while verifying: four copies of the ZIP SHA in the project carried a **truncated 62-character
value** (`…268bc8a641…`, missing `bb`), including `docs/SOURCE_PROVENANCE.md` and
`scripts/build_release.py`. Both are corrected; `tests/test_provenance_constants.py` now pins the full
value. The frozen baseline `PACKAGE_MANIFEST.json` and the owner-provided handoff note are left
byte-identical and documented instead.

## Pipeline and results (exit codes are the CLI's)

| Step | Exit | Wall | Outcome |
|---|---:|---:|---|
| `prepare --limit 0` | 0 | 7.3 s | 554,441 events |
| `replay` (batch 1024) | 0 | 49.0 s | complete, contiguous |
| `lab-e2e` (separate consumer OS process, localhost HTTP, batch 512) | 0 | 407.0 s | COMPLETE: 1,083 POSTs, 554,441 sent/ACKed, 0 duplicates; publish phase 349.0 s, the rest is its built-in v0.2 audit |
| `audit-e2e` (v0.2, resident only) | 0 | 57.8 s | COMPLETE, 554,441 matched |
| `audit-e2e-archive` before any prune | 0 | 74.8 s | COMPLETE, 554,441 anchored and matched (parity) |
| S07 `retention` prune, 25 × 20,000 | 0 | 69.3 s | 500,000 archived in 25 segments (62.1 MiB), 54,441 resident |
| `audit-e2e` after prune | 2 | 20.0 s | INCONSISTENT, `archived_records_unsupported=500000` (v0.2 refusal, unchanged) |
| **`audit-e2e-archive` after prune** | **0** | **86.2 s** | **COMPLETE**: 25/25 segments parsed, 500,000 archived + 54,441 resident events matched, 554,441 source lines anchored, snapshot verified |
| copied run, no flag | 2 | 88.3 s | INCONSISTENT, `ARCHIVE_PATH_RELOCATED` × 25 |
| copied run, `--allow-relocated-archives` | 0 | 87.5 s | COMPLETE (only canonical in-root files read) |
| corruption A: one bit flipped in `raw_000000240000_000000260000.jsonl.gz` | 2 | 79.1 s | INCONSISTENT, `ARCHIVE_SHA256_MISMATCH` |
| corruption B: delivered event (file_index 60100, seq 3023962924) relabelled `DUPLICATE_IDENTICAL`, manifest SHA/count rewritten, consumer row deleted | 2 | 86.2 s | INCONSISTENT, `DISPOSITION_CLAIM_UNVERIFIED` + true `PRODUCER_SEQUENCE_GAP` |

Sizes after prune: producer DB 818.2 MiB (the S07 prune does not VACUUM by default), archives 62.1 MiB,
consumer DB 403.6 MiB, normalized JSONL 190.6 MiB.

## Change made because of this run

In corruption A the 20,000 events of the unreadable segment were missing from the sequence bitmap, so
the first run additionally reported `PRODUCER_SEQUENCE_GAP` and `SOURCE_MANIFEST_MISMATCH`; corruption B
also reported the manifest mismatch. Status was correct (fail-closed), but these cascade findings point at
the wrong evidence. Now a gap is reported only when every archived record was read, and the manifest's
sequence span is compared only for a verifiable, contiguous lane. Regression:
`test_hidden_middle_event_reports_gap_but_not_cascading_manifest_mismatch`.

## Final code re-run on the same real data

Re-run with the final code (after the cascade fix), process peak working set measured with
`GetProcessMemoryInfo` (whole Python process, interpreter and SQLite caches included):

| Run | Exit | Wall | Peak working set | Outcome |
|---|---:|---:|---:|---|
| healthy full day (500,000 archived + 54,441 resident) | 0 | 81.1 s | 37.0 MiB | **COMPLETE**, 554,441 anchored and matched, snapshot verified, no findings |
| corruption B present | 2 | 88.5 s | 36.9 MiB | INCONSISTENT: `DISPOSITION_CLAIM_UNVERIFIED`, `PRODUCER_SEQUENCE_GAP` (true hole at seq 3023962924); no cascading manifest finding |
| corruption B + one flipped bit in `raw_000000240000_000000260000.jsonl.gz` | 2 | 81.2 s | 36.7 MiB | INCONSISTENT: `ARCHIVE_SHA256_MISMATCH`, `DISPOSITION_CLAIM_UNVERIFIED`; no cascading gap/manifest findings |

## Observations and limits

- The archive-aware audit of the full day takes about as long as the v0.2 resident audit plus ~30 %,
  dominated by JSON canonicalisation; memory stays bounded (see peak working set above).
- The `lab-e2e` publish phase took 349 s here versus 59 s in the earlier Linux container run (same data,
  same batch size 512, same 1,083 POSTs): most likely per-batch `synchronous=FULL` fsync cost on Windows
  in both databases. Not investigated; outside Task 001 and a candidate for a later performance task.
  Neither figure is a latency or throughput certification.
- This proves local evidence consistency for the supplied interval only: not venue completeness before
  aggregate ID 3023902824, not live availability, not protection against an actor able to rewrite all
  evidence stores together.
