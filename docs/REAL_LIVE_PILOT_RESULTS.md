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
   ±8.8 ms with alternating sign, while over the whole 596 s session wall and interval clocks differed by only
   0.87 ms. The reader took the wall and the interval stamp in two calls and can lose the GIL in between (5 ms
   switch interval) while the writer thread works; the step detector then flagged every later sample.

## Follow-up (v0.4.1)

* Clock samples on a persistent connection, timed only around the request on an established connection.
* Read stamps taken as a bracketed pair with a recorded uncertainty; the step detector requires a lasting shift,
  not single-pair jitter.
* A composed chain carries a source manifest the archive-aware audit recognises.
* A latency report with 0 trusted samples is labelled as such (not `ESTIMATED`).
* Then a short second pilot (owner's decision).
