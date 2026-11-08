# Real-network pilot of the v0.4.0 live capture (2026-11-08)

Owner-approved, on the owner's machine (native Windows 11 Pro, Python 3.11.9), FTDI-Core v0.4.0 (commit
`16d7eae`), public read-only endpoints only (`wss://data-stream.binance.vision`, `https://data-api.binance.vision`),
no key, no order. Data in the owner's directory `C:\ftdi\live` (sessions), `C:\ftdi\live_chain1` (composed chain),
`C:\ftdi\live_run1` / `C:\ftdi\live_consumer` (replay and consumer). Summary without paths:
`examples/REAL_LIVE_PILOT_EVIDENCE.json`.

## Result: capture and audit work on the real network; latency evidence is not yet good enough

| Step | Outcome |
|---|---|
| Session 1, `live-capture --public-network --max-seconds 600` | `MAX_SECONDS`, **1,838 events** `4109678145..4109679982`, contiguous, 0 gaps, 0 duplicates, 1 connection, 11 clock samples |
| `verify-session` (session 1) | **VERIFIED** (every event re-derived from its raw WebSocket message) |
| Session 2, `--previous-session` session 1, 120 s | `MAX_SECONDS`, **642 events** starting exactly at `4109679983` (`chain CONTIGUOUS`); the 196 trades executed between the sessions were **recovered through the public REST endpoint** (1 gap repaired) |
| `verify-session --previous` (session 2) | **VERIFIED**, chain link checked |
| `compose-sessions` | 2,480 events `4109678145..4109680624` |
| `replay` → `lab-e2e` (separate consumer process) | 2,480 sent, 2,480 acknowledged, 0 duplicates |
| `audit-e2e-archive` | **COMPLETE**: 2,480 / 2,480 matched, 0 missing, 0 conflicts, every source line anchored (one notice `SOURCE_MANIFEST_ABSENT`: the composed chain has a `CHAIN_MANIFEST.json`, not a `source_manifest.json`) |
| `analyze-clock-evidence --session` | **no latency figure** (0 trusted samples): every sample `OFFSET_UNCERTAIN`; most also `WALL_CLOCK_STEP` |

## Why there is no latency figure (the rules worked; the evidence was weak)

1. **Clock samples too coarse.** Server-time round trips were 754–1,693 ms (median 935 ms): each sample opened
   a new TLS connection, and the timing included TCP and TLS setup. The best estimate (local clock ≈ 480 ms
   behind the venue) therefore carries about ±380 ms uncertainty, far above the 20 ms policy limit.
2. **The Windows clock was not synchronised**: `w32tm /query /status` reported Leap indicator 3 (not synchronised)
   and a root dispersion of 9.18 s.
3. **`WALL_CLOCK_STEP` was a measurement artefact, not a clock step.** Consecutive read stamps disagreed by up to
   ±8.8 ms, in short runs of 3-4 reads at about −3.5 / −5.6 ms that returned to the level, while over the whole
   596 s session wall and interval clocks differed by only 0.87 ms. Cause (measured afterwards on the same machine):
   CPython 3.11 implements `time.time_ns()` on Windows with `GetSystemTimeAsFileTime()` (nominal resolution
   15.625 ms; the effective step follows the system timer resolution). Read stamps close in time share one tick's
   error, hence the runs. The step detector, which reacted to a single disagreeing pair, flagged every later
   sample. (A first hypothesis — the reader losing the GIL between the two clock calls — was tested and rejected:
   under CPU contention the two calls stayed within 0.5 ms.) The clock samples' wall stamps were quantised the
   same way.

## Follow-up (v0.4.1)

Implemented in v0.4.1:

* Wall stamps (reads and clock samples) from `GetSystemTimePreciseAsFileTime()` on Windows (100 ns; measured
  wall-minus-interval spread 0.15 ms over 1 s against 1.27 ms for `time.time_ns()` at the time of measurement);
  the clocks used are recorded in `CAPTURE_START.json`; v0.4.0 sessions are analysed with the coarse resolution.
* Clock samples time only the request on an established connection (TCP/TLS setup excluded from the round trip).
* Each read is checked against the wall-minus-interval level of the clock sample that corrects it (the
  samples have always recorded interval stamps): the wall clock moving between sample and read excludes that read
  only; a smaller difference is added to its uncertainty. Without interval stamps on a sample, a step must be a
  lasting shift and a deviating read is excluded alone (`READ_STAMP_OUTLIER`). Re-analysing this pilot removes
  every `WALL_CLOCK_STEP` tag; 7 reads of session 1 are `READ_STAMP_OUTLIER` (figures in
  `examples/REAL_LIVE_PILOT_EVIDENCE.json`, `reanalysis_v041`).
* A composed chain carries a `source_manifest.json` the archive-aware audit checks (no `SOURCE_MANIFEST_ABSENT`).
* A latency report with 0 trusted samples has status `NO_TRUSTED_SAMPLES` (this pilot, re-analysed).
* Not changed: the Windows clock was not synchronised; a latency claim also needs a synchronised clock.
* Next: a short second pilot with v0.4.1 (owner's decision).
