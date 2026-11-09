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

## Third capture: clock sample every 5 s (2026-11-09, owner-approved)

Same released v0.4.1 code, started through the Python API with one non-default limit, `clock_interval_s=5` (the CLI
has no option for it; the limit is recorded in the session). 600 s, public read-only endpoints. Data in
`C:\ftdi\live3`, `C:\ftdi\live3_chain`, `C:\ftdi\live3_run`, `C:\ftdi\live3_consumer`; summary without paths
`examples/REAL_LIVE_PILOT3_EVIDENCE.json`.

| | Result |
|---|---|
| Capture | `MAX_SECONDS`, **4,683 events** `4110219777..4110224459`, contiguous, 0 gaps, 1 connection, **121 clock samples** |
| `verify-session` | **VERIFIED** |
| compose → replay → lab-e2e → `audit-e2e-archive` | 4,683 sent / acknowledged, 0 duplicates; audit **COMPLETE**, no issue |
| Clock-sample round trips | 42.7–90.2 ms, median 63.8 ms; the seven fastest 42.7–45.9 ms; **none below 38 ms** |
| Wall vs interval clock | level within **0.014 ms** over 600 s (no step, no outlier) |
| Latency report (released policy, 20 ms limit) | `NO_TRUSTED_SAMPLES` (best uncertainty ≈ 22.4 ms) |

Twelve times more samples did not produce a faster one: the round trip from this machine to the venue has a floor
of about 43 ms (the network path), not occasional slowness. With the Cristian method the offset uncertainty cannot
fall below about half of that plus 1 ms, i.e. ≈ 22.4 ms, above the 20 ms limit. **From this location the released
policy cannot produce a trusted latency figure, whatever the sampling rate.**

The Windows clock synchronised during the capture (w32tm: last successful sync 12:59:35, ≈ 539 s in; Leap indicator
0 afterwards), but a stripchart against the same NTP server still showed the local clock ≈ 0.88 s behind after the
capture, and the wall-minus-interval level did not change: the offset had not been corrected yet. This does not
affect the estimate (a constant offset is what the clock samples measure), but a latency claim needs the
correction applied (`w32tm /resync /force` as administrator) and, ideally, a capture after it.

**What-if, for the owner's decision only** (`examples/REAL_LIVE_PILOT3_EVIDENCE.json`,
`whatif_uncertainty_limit`; the released limit stays 20 ms and nothing here is certified):

| Uncertainty limit | Status | Trusted reads (of 4,683) | P50 | P95 | P99 |
|---|---|---:|---:|---:|---:|
| 20 ms (released) | `NO_TRUSTED_SAMPLES` | 0 | — | — | — |
| 25 ms | `ESTIMATED` | 904 | 39.7 ms | 42.4 ms | needs 1,000 trusted |
| 30 ms | `ESTIMATED` | 1,850 | 39.9 ms | 42.8 ms | 48.9 ms |

Each of these percentiles carries an uncertainty of up to the limit (±25 or ±30 ms): the true venue-to-local
latency is consistent with roughly 15–65 ms. The figures say the delivery from the venue's event time to the read
on this machine is of the order of tens of milliseconds, consistent with the ~43 ms network round trip; they do not
resolve it more finely. Raising the limit would be a policy change (ADR-008) for the owner to decide; a tighter
figure needs a host network-close to the venue or a better time source (e.g. PTP/GPS), not more sampling.

