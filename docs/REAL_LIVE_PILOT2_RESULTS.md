# Second real-network pilot, FTDI-Core v0.4.1 (2026-11-09)

Owner-approved, on the owner's machine (native Windows 11 Pro, Python 3.11.9), FTDI-Core v0.4.1 (commit `3fd4794`,
tag `v0.4.1`), public read-only endpoints only (`wss://data-stream.binance.vision`, `https://data-api.binance.vision`),
no key, no order. Data in the owner's directory `C:\ftdi\live2` (sessions, reports), `C:\ftdi\live2_chain`,
`C:\ftdi\live2_run`, `C:\ftdi\live2_consumer`. Summary without paths: `examples/REAL_LIVE_PILOT2_EVIDENCE.json`.
First pilot (v0.4.0): `docs/REAL_LIVE_PILOT_RESULTS.md`.

## Result: capture, chain and audit work again; the v0.4.1 clock fixes work; still no trusted latency

| Step | Outcome |
|---|---|
| Session 1, `live-capture --public-network --max-seconds 600` | `MAX_SECONDS`, **4,147 events** `4110203743..4110207889`, contiguous, 0 gaps, 0 duplicates, 1 connection, 11 clock samples |
| `verify-session` (session 1) | **VERIFIED** |
| Session 2, `--previous-session` session 1, 120 s | `MAX_SECONDS`, **1,413 events** from exactly `4110207890` (`chain CONTIGUOUS`); the **229 trades** executed between the sessions recovered through the public REST endpoint; 2 identical duplicate deliveries counted |
| `verify-session --previous` (session 2) | **VERIFIED**, chain link checked |
| `compose-sessions` | 5,560 events `4110203743..4110209302`, with `source_manifest.json` |
| `replay` → `lab-e2e` (separate consumer process) | 5,560 sent, 5,560 acknowledged, 0 duplicates |
| `audit-e2e-archive` | **COMPLETE**: 5,560 / 5,560 matched, 0 missing, 0 conflicts, every source line anchored, **no issue at all** (the v0.4.0 notice `SOURCE_MANIFEST_ABSENT` is gone; the declared chain hash matches) |
| `analyze-clock-evidence --session` (both) | `NO_TRUSTED_SAMPLES` (0 trusted): every read `OFFSET_UNCERTAIN`; 58 % also `QUEUED_READ`; **no `WALL_CLOCK_STEP`, no `READ_STAMP_OUTLIER`** |

## What the v0.4.1 fixes changed

| | Pilot 1 (v0.4.0) | Pilot 2 (v0.4.1) |
|---|---|---|
| Clock-sample round trip | 754–1,693 ms (median 935) — TCP+TLS set-up included | **43–81 ms** (median 64) — request only |
| Wall vs interval clock, per sample | stamps quantised to timer ticks, runs of −3.5 / −5.6 ms | level constant within **0.002 ms** over 13 minutes |
| `WALL_CLOCK_STEP` | 2,161 of 2,284 reads (artefact) | **0** |
| Best offset uncertainty | ≈ 380 ms | **22.5 ms** |
| Report status with 0 trusted samples | `ESTIMATED` (misleading) | `NO_TRUSTED_SAMPLES` |
| Audit notice `SOURCE_MANIFEST_ABSENT` | yes | no |

## Why there is still no trusted latency figure

1. **The round trip to the venue sets the limit.** A Cristian sample bounds the offset by half its round trip
   plus the venue's 1 ms resolution: the best of the 14 samples (43 ms) gives 22.5 ms, above the 20 ms policy
   limit (`max_offset_uncertainty_ms`). A trusted read needs a sample under about 38 ms round trip; from this
   location none came within reach. The rule worked as intended: no number was produced from evidence this weak.
2. **The Windows clock is still not synchronised** (w32tm Leap indicator 3 before and after; root dispersion
   9.17 s). The local clock was ≈ 750–800 ms behind the venue and ran about 48 ppm slow against it over 758 s (a
   least-squares fit through the 14 samples; residuals within ±10 ms, inside the samples' half round trips of
   21–41 ms). The model allows 100 ppm, so this is within policy, but a synchronised clock is still a precondition
   for any latency claim.

For orientation only — **not trusted, not certified, every value ± its stated uncertainty**: corrected latencies of
reads that were not queued had a median of about 39 ms (session 1, uncertainty median ±34 ms) and 50 ms (session 2,
±36 ms). `examples/REAL_LIVE_PILOT2_EVIDENCE.json` (`descriptive_untrusted`) has the distribution; these numbers
cannot be told apart from 0 ms or 80 ms at this uncertainty and must not be quoted as a latency.

## What would give a trusted figure

* Lower clock-sample round trips: run the capture from a host network-close to the venue, or take more samples
  (e.g. `clock_interval_s` 10 s instead of 60 s, still far below the venue's limits), so that a fast sample occurs
  more often; the per-read choice of the sample giving the smallest uncertainty then uses it.
* A synchronised local clock (`w32tm /resync` as administrator; better a stable NTP source).
* A tighter bound than Cristian's half round trip needs a different time source (e.g. PTP or a GPS-disciplined
  clock); out of scope for this research prototype.

None of this is needed for the data path itself: capture, chain, delivery and audit were complete and verified.
