# FTDI-Core v0.4.0 — release gates and evidence

Release date 2026-11-08. Platform: native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1 (owner's machine). Status:
**released for local research use; not production certified.** All v0.3.x gates (`RELEASE_GATES_V03.md`) still
hold: the historical pipeline, retention and `vacuum-db` are unchanged.

| # | Gate | Evidence | Status |
|---|---|---|---|
| L1 | Capture writes only venue-delivered, contiguous, non-contradicted events; raw evidence complete (received messages never lost, also at the stop and on overflow) | `docs/LIVE_CAPTURE.md` §3; black-box `tests/test_live_capture.py` (26, Codex, from the contract); `tests/test_live_capture_internals.py` (9, incl. a real process kill); regressions `tests/test_live_capture_review.py` (25: L1–L12, M1–M7, N1–N6, O3) | PASS |
| L2 | Gaps never skipped (REST repair or `GAP_BLOCKED`), conflicts halt and are cut before sealing, every network call bounded by the capture deadline, 429/418 stop | same suites; loopback acceptance below | PASS |
| L3 | `verify-session` re-derives every event from raw evidence, read-only, never raises on content; `seal-session` recovers killed captures (also mid-seal) and refuses what would not verify | same suites | PASS |
| L4 | Chain → pipeline: `compose-sessions` (fingerprinted, re-checked) → unchanged `replay` → `lab-e2e` → `audit-e2e-archive` COMPLETE; a longer chain re-composed into a new run gives duplicates only | `tests/test_live_chain.py` (4); loopback acceptance | PASS |
| L5 | Latency only under ADR-008 rules (nearest minimum-RTT sample, exclusion tags, minimum counts), never certified | `tests/test_live_chain.py`; loopback acceptance (ESTIMATED on loopback, P99 withheld for lack of samples) | PASS |
| L6 | Network rule: loopback only unless `--public-network`, which selects exactly the two public market-data hosts; refused before any socket otherwise; no key, no private endpoint, no order | endpoint tests (black-box and review: 18 unusual URL forms) | PASS |
| L7 | Independent review | Codex: black-box suite from the contract; adversarial review 12 findings (L1–L12); verification 7 findings (M1–M7); third pass 6 findings (N1: a transient, consistently forged change of an original during composition — fixed by composing from private verified snapshots; N2/N4 loopback port handling; N3 retained `.part` files in fingerprints; N5 recovery verified on a private copy before writing; N6 cleanup on a failing chain-manifest write); fourth pass: N1–N6 confirmed fixed; remaining findings need a concurrent writer racing a maintenance command (junction or directory inserted mid-run, a session's evidence replaced coherently during the snapshot) and are outside the documented maintenance-window precondition and trust boundary (`docs/LIVE_CAPTURE.md` §8); output-inside-a-session is now refused, the recovery check directory is created with `mkdtemp`, cleanup covers every failure. Every regression fails on its pre-fix commit (mutation-checked) | PASS |
| L8 | No regression of the historical pipeline | full-day acceptance `scripts/acceptance_full_day.py` from the release code: **ACCEPTANCE PASS** from commit `bff6f9b` (clean tree), 554,441 events, owner's ZIP read-only (SHA-256 unchanged), incl. `vacuum-db` 404.6 → 60.9 MiB; `examples/V040_ACCEPTANCE_EVIDENCE.json` | PASS |
| L9 | Suites and package | Core 409 (1 skipped by design), frozen S07 45, `-W error::ResourceWarning`, native Windows 11; extracted release ZIP verified (recorded in tag `v0.4.0`) | PASS |

## Live pipeline acceptance on 127.0.0.1 (`scripts/acceptance_live_loopback.py`)

Simulated venue with real-world faults: three chained sessions (2,500 events), a 40-event gap repaired by REST, a
dropped connection with catch-up (99 events by REST), periodic duplicate deliveries.

| Session | Range | Events | Stop | Chain | WS messages | WS duplicates | REST records | Reconnects | Gaps repaired | Verify |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---|
| `session-20261108T141529Z` | 1..1000 | 1000 | MAX_EVENTS | FIRST | 980 | 19 | 40 | 0 | 1 | VERIFIED |
| `session-20261108T141531Z` | 1001..2000 | 1000 | MAX_EVENTS | CONTIGUOUS | 920 | 18 | 99 | 1 | 1 | VERIFIED |
| `session-20261108T141534Z` | 2001..2500 | 500 | MAX_EVENTS | CONTIGUOUS | 509 | 9 | 0 | 0 | 0 | VERIFIED |

| Check | Result |
|---|---|
| session 1: gap repaired by REST, VERIFIED | PASS |
| session 2: reconnect, catch-up, chain CONTIGUOUS, VERIFIED | PASS |
| events 1..2000 equal the venue ground truth exactly (gap and reconnect range included) | PASS |
| chain 1 composed 1..2000 | PASS |
| replay of chain 1 complete | PASS |
| lab-e2e (separate consumer process) passes | PASS |
| archive-aware audit COMPLETE | PASS |
| session 3 VERIFIED | PASS |
| re-composed chain: 2000 duplicates, 500 new effects | PASS |
| archive-aware audit of the longer chain COMPLETE | PASS |
| latency evidence follows ADR-008 (never certified) | PASS |

Latency evidence of session 1 (loopback, this machine only): status ESTIMATED, 661 trusted of 960 samples, excluded tags {'QUEUED_READ': 299}, P50 -0.115 ms, P95 0.335 ms, P99: insufficient samples (need 1000 trusted); 40 events recovered by REST only (no read time). ftdi-core 0.4.0, Python 3.11.9, SQLite 3.45.1, 2026-11-08 22:15:29 → 2026-11-08 22:15:37.

On loopback the true delay is below a millisecond while the offset uncertainty is about 1 ms (the venue time has 1 ms resolution), so a percentile may be slightly negative within its uncertainty: these numbers show that the ADR-008 rules work end to end; they are not a latency measurement.

Evidence: `examples/V040_LIVE_LOOPBACK_EVIDENCE.json` (commit `bff6f9b`).

## Not proven

Sustained real-network operation of the v0.4 collector (a real-network pilot is the owner's decision);
venue-to-user latency (the loopback figures measure this machine only); everything listed as not proven in v0.3.

## v0.4.1 (2026-11-09): clock evidence corrected after the real-network pilot

Corrective release (`RELEASE_NOTES_v0.4.1.md`) for the findings of the owner-approved real-network pilot
(`docs/REAL_LIVE_PILOT_RESULTS.md`). Gates L1–L9 above still hold for the unchanged capture, sequencing, sealing,
verification and composition; the gates below cover the change. Regressions: `tests/test_live_pilot_findings.py` (21: p1–p7 for the pilot
findings, p8–p21 for the review findings).

| # | Gate | Evidence | Status |
|---|---|---|---|
| P1 | Live read stamps and clock samples use the precise wall clock (`GetSystemTimePreciseAsFileTime()` on Windows); `CAPTURE_START.json` records the clocks; a v0.4.0 start record without them stays valid; malformed or absurd clock records (resolution outside 1..10^9 ns) make the session INVALID instead of crashing the analysis | p2 (a coarse `time.time_ns` patched in does not reach the stamps), p6, p9 | PASS |
| P2 | Clock-sample round trips exclude TCP/TLS setup | p1 (a 300 ms connect is not in the round trip) | PASS |
| P3 | Each read is corrected by the clock sample that gives it the smallest uncertainty, preferring samples under its own wall clock, and checked against that sample's wall-minus-interval level (tolerance + drift bound; the measured movement is in the uncertainty; a sample during which the wall clock moved is discarded, the larger of its wall and interval round trip and half its movement enter the uncertainty; a read without an interval stamp is `WALL_CLOCK_UNVERIFIED`). Without interval stamps on the sample, a heuristic: reference from the first agreeing pair, departure from both of them or from the majority of the last 20 accepted reads, lasting shift by majority of the next 20, drift per pair, direction-independent | p8, p18, p19, p21 (per-read model), p3, p10, p11, p13 (heuristic), p16 (randomised safety invariant: no trusted read disagrees with its sample or reference; 41 violations on the code before the reference-pair check, 0 now); `tests/test_timeprov.py` unchanged and passing | PASS |
| P4 | Status `NO_TRUSTED_SAMPLES` with 0 trusted samples; the offset uncertainty uses the session's recorded wall-clock resolution (15.625 ms assumed for v0.4.0 sessions, on any analysing machine) | p4, p7; read-only re-analysis of the pilot (`examples/REAL_LIVE_PILOT_EVIDENCE.json`, `reanalysis_v041`) | PASS |
| P5 | A composed chain has a `source_manifest.json` the archive-aware audit checks (hash matches, no `SOURCE_MANIFEST_ABSENT`) | p5; loopback acceptance below | PASS |
| P6 | Every fix is necessary for its regression: 52 single-fix reverts; 51 make their test fail, 1 is equivalent (outliers in the 20-read voting window are always a minority before the first step, so every vote is unchanged); unmutated passes (bytecode caching disabled, so a same-length edit cannot hit a stale `.pyc`) | mutation check on a detached worktree of `269ed0a` | PASS |
| P7 | Independent review | Codex: first pass 4 findings (unbounded clock resolution, jittered first read as reference, drift as a step, unbounded name resolution); second pass confirmed them fixed and found 7 more (direction-dependent heuristic, >64-bit integers, socket-construction fallback, resolver exceptions, worker after an expired deadline, two documentation errors); a third Codex pass was not possible (its service rejected the API key). Independent Claude reviewer: 9 findings (line separators in evidence, GIL between wall and interval stamp, round trip shortened by a wall clock moving back, server time beyond 64 bits, resolution of v0.4.0 sessions on other machines, reads without interval stamps, REST interval outside the budget, sample selection ignoring the wall clock, documentation) and a verification pass (all confirmed fixed, two limits documented, sample choice by the read's uncertainty added). Our own check: slow ramps in the heuristic (randomised safety test). Every finding has a regression | PASS |
| P8 | No regression of the historical pipeline | full-day acceptance from commit `269ed0a` (clean tree): **ACCEPTANCE PASS**, 554,441 events, owner's ZIP read-only (SHA-256 unchanged), incl. `vacuum-db` 404.6 → 60.9 MiB; `examples/V041_ACCEPTANCE_EVIDENCE.json` | PASS |
| P9 | Suites and package | Core 430 (1 skipped by design), frozen S07 45, `-W error::ResourceWarning`, native Windows 11; extracted release ZIP verified (recorded in tag `v0.4.1`) | PASS |
| P10 | Owner's pilot data untouched | the re-analysis only reads `C:\ftdi\live`; no file there created, changed or deleted | PASS |
| P11 | Every network call bounded by its deadline including name resolution (which has no timeout of its own); a REST set-up ending after the budget is a failed attempt; address fallback also when a socket cannot be created; any resolver failure is a recorded transport error; no worker after an expired deadline; the minimum REST interval counts against the budget | p12, p15, p20 | PASS |
| P12 | Wall and interval stamps of one instant stay paired although the ctypes wall-clock call releases the GIL (`stamp_pair`: interval clock read on both sides, retried until within 0.1 ms) | p17 (wall clock returning late 30 % of the time: error < 0.5 ms); measured with a CPU-bound thread competing for the GIL: level spread 0.016 ms, against 10.7 ms for wall-then-interval | PASS |
| P13 | A VERIFIED session can always be analysed | p14 (integers beyond 64 bits are INVALID), p19 (U+2028/U+0085 sent raw inside a message; a server time beyond 64 bits is not written as a sample) | PASS |

### Live pipeline acceptance on 127.0.0.1, v0.4.1 (`scripts/acceptance_live_loopback.py`)

| Session | Range | Events | Stop | Chain | WS messages | WS duplicates | REST records | Reconnects | Gaps repaired | Verify |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---|
| `session-20261108T180540Z` | 1..1000 | 1000 | MAX_EVENTS | FIRST | 980 | 19 | 40 | 0 | 1 | VERIFIED |
| `session-20261108T180542Z` | 1001..2000 | 1000 | MAX_EVENTS | CONTIGUOUS | 920 | 18 | 99 | 1 | 1 | VERIFIED |
| `session-20261108T180546Z` | 2001..2500 | 500 | MAX_EVENTS | CONTIGUOUS | 510 | 9 | 0 | 0 | 0 | VERIFIED |

| Check | Result |
|---|---|
| session 1: gap repaired by REST, VERIFIED | PASS |
| session 2: reconnect, catch-up, chain CONTIGUOUS, VERIFIED | PASS |
| events 1..2000 equal the venue ground truth exactly (gap and reconnect range included) | PASS |
| chain 1 composed 1..2000 | PASS |
| replay of chain 1 complete | PASS |
| lab-e2e (separate consumer process) passes | PASS |
| archive-aware audit COMPLETE | PASS |
| session 3 VERIFIED | PASS |
| re-composed chain: 2000 duplicates, 500 new effects | PASS |
| archive-aware audit of the longer chain COMPLETE | PASS |
| latency evidence follows ADR-008 (never certified) | PASS |

Latency evidence of session 1 (loopback, this machine only): status ESTIMATED, 730 trusted of 960 samples, excluded tags {'QUEUED_READ': 230}, P50 0.175 ms, P95 0.777 ms, P99: insufficient samples (need 1000 trusted); 40 events recovered by REST only (no read time). ftdi-core 0.4.1, Python 3.11.9, SQLite 3.45.1, 2026-11-09 02:05:40 → 2026-11-09 02:05:48.

Evidence: `examples/V041_LIVE_LOOPBACK_EVIDENCE.json` (commit `269ed0a`).

### Not proven (v0.4.1)

A latency figure from the real network (needs a v0.4.1 capture with a synchronised local clock: the owner's
decision); sustained real-network operation; everything listed as not proven for v0.4.0 and v0.3.

## v0.4.2 (2026-11-09): Linux verified; first trusted latency estimate

Release notes `RELEASE_NOTES_v0.4.2.md`. Gates L1–L9 and P1–P13 above still hold. Regressions:
`tests/test_live_pilot_findings.py` p22–p24.

| # | Gate | Evidence | Status |
|---|---|---|---|
| Q1 | A REST body shorter than its Content-Length is a transport error, never an answer (Linux returns it silently) | p22; `test_m3` passes on Linux (failed there before) | PASS |
| Q2 | Raw evidence reaches the OS before anything derived from it (raw files unbuffered, synced first) | p24; the killed-capture test on Linux: 0 failures in 30 runs (before: 2 in 12) | PASS |
| Q3 | `live-capture --clock-interval-s`; at least 1 s against the public venue (refused before any socket; a public session claiming less is INVALID) | p23; all six real sessions re-verified VERIFIED | PASS |
| Q4 | Every change is necessary for its regression | 5 single-change reverts, all caught (worktree of `943cb8d`, bytecode caching off) | PASS |
| Q5 | Linux | Ubuntu 24.04.5 LTS, Python 3.12.3: Core suite OK (4 Windows-only tests skip by design), frozen S07 45 OK; live loopback acceptance: 11/11 checks PASS; sessions 1..1000 VERIFIED, 1001..2000 VERIFIED, 2001..2500 VERIFIED; latency evidence of session 1 (loopback only) ESTIMATED, 189 of 960 trusted, P50 0.751 ms; ftdi-core 0.4.2, Python 3.12.3, SQLite 3.45.1; `examples/V042_LIVE_LOOPBACK_LINUX_EVIDENCE.json`; extracted release ZIP verified on Linux (recorded in tag `v0.4.2`) | PASS |
| Q6 | Real network on Linux | owner's cloud server, released v0.4.1 package: 10,870 events, VERIFIED, compose → replay → lab-e2e → audit COMPLETE on Linux; latency `ESTIMATED` (1,147 trusted, P50 18.5 / P95 19.7 / P99 20.0 ms, each ± up to 20 ms), independent cross-check P50 19.5 ms with the NTP-disciplined clock (`docs/REAL_LIVE_REMOTE_RESULTS.md`, `examples/REAL_LIVE_REMOTE_EVIDENCE.json`) | PASS |
| Q7 | No regression on Windows | full-day acceptance from commit `943cb8d` (clean tree): **ACCEPTANCE PASS**, 554,441 events, owner's ZIP read-only (SHA-256 unchanged), incl. `vacuum-db` 404.6 → 60.9 MiB; `examples/V042_ACCEPTANCE_EVIDENCE.json`; live loopback acceptance: 11/11 checks PASS; sessions 1..1000 VERIFIED, 1001..2000 VERIFIED, 2001..2500 VERIFIED; latency evidence of session 1 (loopback only) ESTIMATED, 574 of 960 trusted, P50 0.404 ms; ftdi-core 0.4.2, Python 3.11.9, SQLite 3.45.1; `examples/V042_LIVE_LOOPBACK_EVIDENCE.json` | PASS |
| Q8 | Suites and package | Windows: Core 433 (1 skipped by design), frozen S07 45, `-W error::ResourceWarning`; extracted release ZIP verified on Windows and Linux (recorded in tag `v0.4.2`) | PASS |

### Not proven (v0.4.2)

A certified latency (the estimate carries up to ±20 ms, from one 10-minute capture on one host); sustained
real-network operation; full-day historical acceptance on Linux (the owner's ZIP was not copied to the server).
