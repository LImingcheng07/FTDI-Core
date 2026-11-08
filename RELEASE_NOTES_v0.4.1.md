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
  (`clocks: {wall, interval}`). Because the ctypes call releases the GIL, every wall stamp is read between two
  interval-clock reads and retried until they lie within 0.1 ms (`stamp_pair`; with a CPU-bound thread competing
  for the GIL: 0.016 ms level spread instead of 10.7 ms for wall-then-interval).
* **Clock-sample timing** (`/api/v3/time`): the connection (TCP, TLS) is established first; `t0`/`t1` bracket only
  the request and its answer. A REST record whose connection failed has `t0 = t1`.
* **Wall clock against interval clock** (Task 003 rule, ADR-008 amendment): each read is checked against the
  wall-minus-interval level of the clock sample that corrects it (5 ms tolerance plus the 100 ppm drift bound over
  the time between them). If the wall clock moved, the offset does not apply to that read (`WALL_CLOCK_STEP`, or
  the new tag `READ_STAMP_OUTLIER` if the read also departs from its neighbours); a smaller difference is added to
  the uncertainty. A clock sample during which the wall clock moved by more than the tolerance is discarded; for
  the others the round trip is the larger of the wall and interval measurement and half the movement is added to
  the uncertainty. Samples taken under the read's wall clock are preferred, so a sample after a wall-clock step
  corrects the reads after it. A read without an interval stamp cannot be checked (`WALL_CLOCK_UNVERIFIED`).
  Without interval stamps on the sample (other callers of the model), a heuristic on the reads: the reference
  starts with the first two agreeing reads; a read departs if it disagrees with both of them or with the
  majority of the last 20 accepted reads (drift allowed for per pair); a lasting shift needs the majority of
  the next 20 reads to depart; a deviating read is excluded alone and never used as a reference.
* **Bounded connection set-up**: name resolution (which has no timeout of its own) runs in a worker thread that
  is abandoned at the deadline, for REST and WebSocket; a REST connection set up after its budget is a failed
  attempt, not a late answer.
* **Clock records bounded**: `resolution_ns` must be an integer from 1 to 10^9 and `implementation` 1-200
  characters, otherwise the session is INVALID (an absurd value could otherwise crash the latency analysis).
* **Report status `NO_TRUSTED_SAMPLES`** when latencies could be corrected but no sample is trusted (v0.4.0 said
  `ESTIMATED` with 0 trusted samples). The local clock resolution in the offset uncertainty is the session's
  recorded wall-clock resolution (15.625 ms assumed for v0.4.0 sessions, which recorded none); the report states
  it (`session_wall_clock`).
* **REST budget**: the minimum interval between REST calls counts against the call's budget.
* **Chain source manifest**: `compose-sessions` also writes `source_manifest.json` (`FTDI-Core-SourceManifest-v1`,
  `source_kind: LIVE_SESSION_CHAIN`), so `audit-e2e-archive` checks the chain file's declared hash, count and range
  instead of noting `SOURCE_MANIFEST_ABSENT`.

Re-analysed read-only with these rules, the pilot sessions have no `WALL_CLOCK_STEP` tag, status
`NO_TRUSTED_SAMPLES` (only `OFFSET_UNCERTAIN` from the slow clock samples and legitimate `QUEUED_READ`),
figures in `examples/REAL_LIVE_PILOT_EVIDENCE.json` (`reanalysis_v041`).

The independent review (Codex) of the first v0.4.1 candidate found four defects, all fixed with regressions:
an absurd clock resolution passed verification and crashed the analysis (F1); a jittered first read became the
reference and tagged every later read (F2, inherited from v0.4.0); slow drift within the policy looked like a step
with sparse reads (F3); name resolution and connection set-up were not bounded by the deadline (F4, inherited).
Its second pass confirmed F1-F4 fixed and found seven more, also fixed: the reads-only heuristic depended on the
direction of an early step and paired medians of different reads (R2-1, now pairwise majority votes with drift per
pair); an absurd interval stamp in a VERIFIED session crashed the analysis (R2-2, every evidence integer and venue
integer is now bounded to 64 bits); a socket-construction error prevented trying the next address (R2-3); a
non-OSError in the resolver became a KeyError (R2-4); an expired deadline still started a resolver thread (R2-5);
the design document had the offset sign reversed and an outdated status (R2-6, R2-7). A third pass (by a separate
reviewer, Codex being unavailable) and our own check added: the heuristic also compares every read with its
reference pair, so a ramp faster than the drift bound cannot creep away from the reference while each read agrees
with its recent neighbours; a randomised safety test asserts that no trusted read disagrees with its reference.
The third reviewer found nine more, also fixed: evidence lines were split at U+2028/U+0085 by the analysis (a
VERIFIED session could not be analysed); REST stamps took the interval clock after the GIL-releasing wall clock
(now `stamp_pair`); a wall clock moving back during a sample shortened its round trip and understated the
uncertainty; a server time beyond 64 bits was written as a sample and made the session INVALID; v0.4.0 sessions
analysed on Linux used that machine's clock resolution; reads without interval stamps were not checked; the
minimum REST interval was not part of the budget; sample selection ignored the wall clock level; some comments
and this file's heuristic description were inaccurate.

## Compatibility

* Sessions, chains and runs of v0.4.0 (including the owner's pilot data) are read and verified unchanged; nothing
  is migrated or rewritten. A v0.4.0 `CAPTURE_START.json` without `clocks` remains valid.
* Not forward compatible: `verify-session` of v0.4.0 rejects a session captured by v0.4.1 (its start record has the
  additional `clocks` field). Use v0.4.1 to verify new sessions.
* A composed chain directory has a third file, `source_manifest.json`. Chains composed by v0.4.0 stay valid; their
  runs keep the notice `SOURCE_MANIFEST_ABSENT`.
* Latency and clock-evidence reports apply the new rules; they may contain the new tags `READ_STAMP_OUTLIER`,
  `WALL_CLOCK_UNVERIFIED`,
  the new status `NO_TRUSTED_SAMPLES` and the field `session_wall_clock`. `analyze-clock-evidence --pilot-db` has
  no interval stamps and is unchanged. The model's `OffsetSample` and `OffsetEstimate` gained optional fields
  (`t0_perf_ns`, `t1_perf_ns`; `at_perf_ns`); existing calls keep working.
* `verify-session` rejects integers outside 0..2^63−1 in evidence files and venue messages (never produced
  by a real capture; SQLite could not store them either).
* No change to the historical pipeline, the delivery protocol, the consumer or its databases, retention,
  `archive-producer` or `vacuum-db`.

## Verification

See `docs/RELEASE_GATES_V04.md`, section v0.4.1.

## Not proven

A latency figure from the real network: that needs a v0.4.1 capture on the real network with a synchronised
local clock (the owner's decision). Sustained real-network operation. No latency claim is certified.
