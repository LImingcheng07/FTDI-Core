# FTDI-Core v0.4.1 — release notes

Date: 2026-11-08. Platform verified: native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1. Status: **local research
prototype; not production certified; not a trading program.** Corrective release for the clock evidence of live
capture, after the owner-approved real-network pilot of v0.4.0 (`docs/REAL_LIVE_PILOT_RESULTS.md`). Capture,
sequencing, gap repair, sealing, verification, composition and the historical pipeline are unchanged.

## What the pilot showed

On the real network (BTCUSDT, 2 chained sessions, 2,480 events, 196 recovered by REST between the sessions) the
capture, `verify-session`, `compose-sessions`, `replay` → `lab-e2e` and `audit-e2e-archive` (**COMPLETE**,
2,480 / 2,480) worked. The latency report rightly gave no figure, but for three reasons that v0.4.0 handled badly:

1. clock-sample round trips (754–1,693 ms) included TCP and TLS setup;
2. read stamps and clock samples used `time.time_ns()`, which on Windows (CPython < 3.13) is
   `GetSystemTimeAsFileTime()`, quantised to system-timer ticks (nominally 15.625 ms);
3. the step detector turned one pair of quantised stamps into `WALL_CLOCK_STEP` for every later read.

The local Windows clock was also not synchronised (w32tm Leap indicator 3), which this release cannot fix.

## Fixed

* **Precise wall clock** (`ftdi_core/live/clocks.py`): live read stamps and clock samples use
  `GetSystemTimePreciseAsFileTime()` on Windows (100 ns; wall-minus-interval spread 0.15 ms instead of 1.27 ms
  measured on the pilot machine), `time.time_ns()` elsewhere. `CAPTURE_START.json` records the clocks used
  (`clocks: {wall, interval}`).
* **Clock-sample timing** (`/api/v3/time`): the connection (TCP, TLS) is established first; `t0`/`t1` bracket only
  the request and its answer. A REST record whose connection failed has `t0 = t1`.
* **Wall-clock steps** (Task 003 rule, ADR-008 amendment): `WALL_CLOCK_STEP` needs a lasting shift of the
  wall-minus-interval level (the median of this and the next 20 reads departs from the median of the preceding
  20 by more than the 5 ms tolerance). A read that deviates without a lasting shift is excluded on its own with the
  new tag `READ_STAMP_OUTLIER`.
* **Report status `NO_TRUSTED_SAMPLES`** when latencies could be corrected but no sample is trusted (v0.4.0 said
  `ESTIMATED` with 0 trusted samples). The local clock resolution in the offset uncertainty is the session's
  recorded wall-clock resolution (15.625 ms assumed on Windows for v0.4.0 sessions); the report states it
  (`session_wall_clock`).
* **Chain source manifest**: `compose-sessions` also writes `source_manifest.json` (`FTDI-Core-SourceManifest-v1`,
  `source_kind: LIVE_SESSION_CHAIN`), so `audit-e2e-archive` checks the chain file's declared hash, count and range
  instead of noting `SOURCE_MANIFEST_ABSENT`.

Re-analysed read-only with these rules, the pilot sessions have no `WALL_CLOCK_STEP` tag, status
`NO_TRUSTED_SAMPLES` (`OFFSET_UNCERTAIN` from the slow clock samples; 88 + 12 `READ_STAMP_OUTLIER` reads from the
coarse clock), figures in `examples/REAL_LIVE_PILOT_EVIDENCE.json` (`reanalysis_v041`).

## Compatibility

* Sessions, chains and runs of v0.4.0 (including the owner's pilot data) are read and verified unchanged; nothing
  is migrated or rewritten. A v0.4.0 `CAPTURE_START.json` without `clocks` remains valid.
* Not forward compatible: `verify-session` of v0.4.0 rejects a session captured by v0.4.1 (its start record has the
  additional `clocks` field). Use v0.4.1 to verify new sessions.
* A composed chain directory has a third file, `source_manifest.json`. Chains composed by v0.4.0 stay valid; their
  runs keep the notice `SOURCE_MANIFEST_ABSENT`.
* Latency and clock-evidence reports (also `analyze-clock-evidence --pilot-db`) apply the new step rule; they may
  contain the new tag `READ_STAMP_OUTLIER`, the new status `NO_TRUSTED_SAMPLES` and the field `session_wall_clock`.
* No change to the historical pipeline, the delivery protocol, the consumer or its databases, retention,
  `archive-producer` or `vacuum-db`.

## Verification

See `docs/RELEASE_GATES_V04.md`, section v0.4.1.

## Not proven

A latency figure from the real network: that needs a v0.4.1 capture on the real network with a synchronised
local clock (the owner's decision). Sustained real-network operation. No latency claim is certified.
