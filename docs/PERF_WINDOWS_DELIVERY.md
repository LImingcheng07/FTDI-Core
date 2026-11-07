# Windows delivery performance: root cause and guidance (2026-11-07)

## Observation
Full day (554,441 events, batch 512, 1,083 POSTs): `lab-e2e` publish phase **349 s** on the owner's Windows
machine versus **59 s** in an earlier Linux container run (`docs/REAL_ARCHIVE_AUDIT_RESULTS.md`).

## Investigation
1. An independent investigation (Codex, 56 runs, synthetic data in temp dirs on `C:`) could **not**
   reproduce it: 37–60 ms per 512-event batch. It ruled out Nagle/delayed-ACK stalls and found only modest
   gains from persistent connections, write coalescing and a larger WAL checkpoint threshold
   (report: `%TEMP%\ftdi_codex\perf\REPORT.md`, outside the repo).
2. The missing variable was the **storage device**: the full-day run lived on `E:`.

| Drive | Device | `os.fsync` of 4 KiB, p50 / max |
|---|---|---|
| `C:` | WD_BLACK SN770 NVMe SSD | 0.18 / 0.32 ms |
| `E:` | WDC WD20EZBX 7200 rpm SATA **HDD** | **8.36** / 23.9 ms (≈ one platter revolution) |

3. Same 50,000-event synthetic workload, fresh directories, separate consumer process, all audits `COMPLETE`:

| Drive | Variant | ms / batch | publish total |
|---|---|---:|---:|
| C: NVMe | baseline (batch 512) | 30.6 | 2.99 s |
| E: HDD | baseline (batch 512) | 109.9 | 10.77 s |
| E: HDD | `wal_autocheckpoint=16000` on the consumer | 69.1 | 6.77 s |
| E: HDD | batch 1024 | 179.3 / 1024 events | 8.79 s |
| E: HDD | batch 1024 + `wal_autocheckpoint=16000` | 114.1 / 1024 events | 5.59 s |

## Root cause
Every batch performs two `synchronous=FULL` commits (consumer, then producer ACK), and WAL checkpoints
write back the consumer's dirty B-tree pages. `effects.event_id` is a SHA-256 primary key, so each batch
dirties ~one random index page per event; on an HDD each written page costs seek time and every fsync
waits for the platter. The gap widens with database size (3.6× at 50k events, ~6× at the full day). On an
NVMe SSD the same work is cheap, which is why it did not reproduce on `C:` or on the Linux container.

## Changes (ADR-009)
- `EffectStore` sets `PRAGMA wal_autocheckpoint=16000` (fewer, larger checkpoints coalesce repeated page
  writes). Durability is unchanged: each commit is still fsynced in the WAL with `synchronous=FULL`.
  Re-measured with the shipped code: E: 10.77 s → **6.80 s**; C: 2.99 s → 2.73 s. This is a deliberate
  divergence of the maintained adapter from the frozen `reference_s07_0041/` copy (which is unchanged).
- Rejected: `synchronous=NORMAL` (an acknowledged consumer effect could be lost on power failure, breaking
  the end-to-end guarantee), and threading the stdlib server (SQLite connection thread affinity).

## Guidance
- Put run directories (`runs/`, `data/`, consumer DBs) on the NVMe SSD; this alone is worth ≥3.6×.
- Use `--batch 1024` for large historical deliveries (fewer commits; still at-least-once and idempotent).
- Not done: a schema-level fix (e.g. no random-key index) belongs to the consumer schema v2 of Task 002 and
  needs the owner's approval; persistent HTTP connections need reconnect/failure tests first.
