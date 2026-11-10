# Real-network captures on a Linux cloud server (2026-11-09)

Owner-provided Linux cloud server (Ubuntu 24.04.5 LTS, Python 3.12.3, 4 vCPU; other services running), released
FTDI-Core v0.4.1 package (verified against its manifest on the host, 149/149 files), started through the Python API
with `clock_interval_s=5` (v0.4.2 adds `--clock-interval-s` to the CLI). 600 s, public read-only endpoints, no key,
no order. The session was copied to the owner's PC (`C:\ftdi\remote_tx106`); the host keeps its copy under the
user's home directory. Summary without host address or paths: `examples/REAL_LIVE_REMOTE_EVIDENCE.json`.

## Result: the first trusted latency estimate

| Step | Outcome |
|---|---|
| Capture | `MAX_SECONDS`, **10,870 events** `4110331283..4110342152`, contiguous, 0 gaps, 1 connection, 121 clock samples |
| `verify-session` (on the host and again on the PC) | **VERIFIED** |
| compose → replay → lab-e2e → `audit-e2e-archive` (on the host, Linux) | 10,870 sent / acknowledged, 0 duplicates; audit **COMPLETE**, no issue |
| Clock samples | round trip 36.1–85.3 ms (median 55.8); **16 below 38 ms** (the PC: none in 121, floor 42.7 ms) |
| Host clock | chrony, stratum 3, 0.12 ms from its NTP sources; the venue clock was ≈ 1 ms ahead of it (median of all samples) |
| `analyze-clock-evidence --session` (released policy, 20 ms limit) | **`ESTIMATED`**: 1,147 trusted reads of 10,870; **P50 18.5 ms, P95 19.7 ms, P99 20.0 ms**; excluded: 8,411 `OFFSET_UNCERTAIN`, 6,162 `QUEUED_READ` |

## How far the figure can be trusted

* **Every trusted read carries an uncertainty of 19.1–20.0 ms** (half the round trip of its clock sample plus the
  venue's 1 ms resolution, drift and wall-clock movement). The absolute level of venue-to-host latency is therefore
  known only to about ±20 ms: the true value is consistent with roughly 0–40 ms. This is what the Cristian method
  can do over a 36 ms round trip; it is stated, not hidden.
* **No selection effect**: the 1,147 trusted reads (P50 18.46 ms) look like all 4,708 reads that were not queued
  (corrected P50 18.55 ms); trust depends only on which clock sample corrects a read, not on its latency.
* **Independent cross-check**: the host clock was disciplined by NTP and within about 1 ms of the venue clock, so
  the *uncorrected* difference read time − event time is an estimate that does not use the clock samples at all:
  P50 19.5 ms, P5–P99 19.3–20.8 ms. Its own error is the host-to-venue clock difference (chrony bounds its NTP
  sources by about 18 ms against UTC; the samples put the venue ≈ 1 ms ahead).
* **Physical plausibility**: half the fastest round trips is 18.0–18.9 ms; a one-way delivery of about the same is
  what a symmetric path gives.
* The **spread** is narrow (trusted P5–P99 17.6–20.0 ms): reads corrected by the same sample share its offset error,
  so differences between reads are measured much more precisely than the absolute level.
* 57 % of reads were `QUEUED_READ` (messages already waiting in the reader's queue, bursts of trades); their read
  time is not their arrival time and they are excluded, as the rules require.

In short: **from this host, venue event time to the read in the capture process is about 18–20 ms**, with an
absolute uncertainty of up to ±20 ms that two independent methods agree within. Not certified; a research estimate
with its evidence.


## Second capture: one hour in the evening (2026-11-09 22:00–23:00, owner-approved)

Released v0.4.2 package on the same host, started with the CLI:
`live-capture --symbol BTCUSDT --public-network --max-seconds 3600 --max-events 1000000 --clock-interval-s 5`.
Copy on the owner's PC: `C:\ftdi\remote_tx106_1h`; summary `examples/REAL_LIVE_REMOTE_1H_EVIDENCE.json`.

| | Result |
|---|---|
| Capture | `MAX_SECONDS`, **54,438 events** `4110589237..4110643674`, contiguous, 0 gaps, 1 connection, 0 reconnects, 718 clock samples |
| `verify-session`; compose → replay → lab-e2e → `audit-e2e-archive` (on the host) | **VERIFIED**; 54,438 sent / acknowledged, 0 duplicates; audit **COMPLETE**, 54,438 / 54,438, no issue |
| Latency report (released policy) | **`ESTIMATED`**: 955 trusted reads of 54,438; **P50 35.4 ms, P95 37.1 ms**; P99 withheld (needs 1,000 trusted reads) |
| Independent check (NTP-disciplined host clock, uncorrected, 21,033 unqueued reads) | P50 36.56 ms, P5–P99 36.43–38.05 ms |

Stable over the hour, also through the busy second half (3–5 times more trades):

| Window (min) | Reads | Trusted | Trusted P50 (ms) | Independent P50 (ms) |
|---|---:|---:|---:|---:|
| 0-10 min | 4,796 | 26 | 35.37 | 36.6 |
| 10-20 min | 3,690 | 107 | 35.49 | 36.55 |
| 20-30 min | 3,507 | 33 | 35.42 | 36.54 |
| 30-40 min | 15,310 | 63 | 36.17 | 36.56 |
| 40-50 min | 16,848 | 629 | 35.14 | 36.58 |
| 50-60 min | 10,287 | 97 | 36.89 | 36.55 |

**Why the evening figure (≈ 36 ms) is twice the afternoon one (≈ 19 ms).** Both methods agree within about 1 ms in
both captures, so the difference is real, not a measurement artefact. A connect-time probe from the host to every
address of both public hosts found three discrete levels — about 35, 51 and 67 ms — for every address, changing
from one connection to the next. The most likely explanation: each TCP connection takes one of several
network paths and keeps it. The
afternoon WebSocket connection had a fast path (one-way ≈ 19 ms), the evening one a path about 17 ms slower, for
its whole lifetime. The clock samples, each on a new connection, show the same levels (round trips here:
16 near 36 ms, 195 near 52 ms, 489 near 67 ms, 18 slower), and only the fast ones
can be trusted under the 20 ms limit.

So **venue-to-host latency from this host is about 19 ms or about 36 ms depending on the path the WebSocket
connection gets**, each with an uncertainty of up to ±20 ms. A capture that wants the fast path could open the
stream, check its delivery delay against the clock samples, and reconnect while it is slow; not implemented (an
operational choice for the owner).


## Path selection (2026-11-10, v0.4.3)

**Experiment** (before the feature): 12 WebSocket connections in a row, 10 s each, delivery delay on the host's
NTP-disciplined clock: 3 connections at **18.9–20.2 ms**, 9 at **35.2–38.7 ms**; within a connection the delay is
steady (minimum and median within 0.2 ms); one server address gave the fast path once and the slow path twice.
The path is a property of the connection.

**Feature** (`docs/LIVE_CAPTURE.md` §3a, ADR-012): `--path-probes K --path-probe-s W` opens probe connections next to
the active one (make-before-break), compares the delivery level of both over the same window and keeps the faster.

**Validation on the real network** (`examples/REAL_LIVE_PATH_SELECTION_EVIDENCE.json`; clock sample every 5 s):

| Run | Build | Length | Probes × window | Events | Connections | Switches | Kept level | REST | Reconnects | Verify |
|---|---|---|---|---:|---:|---:|---|---:|---:|---|
| 1 | dev | 600 s | 8 × 10 s | 4,737 | 9 | 0 | 19.1 ms (first connection already fast) | 0 | 0 | VERIFIED |
| 2 | dev | 300 s | 8 × 10 s | 2,868 | 9 | 1 | 35.8 ms (no fast connection among 9) | 0 | 0 | VERIFIED |
| 3 | dev | 300 s | 16 × 8 s | 1,909 | 15 | 1 | **35.0 → 19.6 ms** | 0 | 1 | VERIFIED |
| 4 | v0.4.3 | 300 s | 16 × 8 s | 2,117 | 16 | 1 | **35.2 → 18.9 ms** | 0 | 0 | VERIFIED |

* No run needed REST or lost a trade: the overlap delivered trades twice (391, 918, 722 and 783
  identical duplicates), all sessions VERIFIED. The run-3 reconnect was the first connection's set-up (unchanged
  behaviour), not path selection.
* The per-connection levels that `analyze-clock-evidence` re-derives from the raw evidence equal the levels the
  capture decided on (run 4: 35.2, 37.4, 36.1, 36.7, 35.3, 36.7, 35.3, 18.9, 36.4, 18.9, 36.9, 35.0, 37.7, 19.1, 35.2, 37.3 ms).
* Over all runs and the experiment, 13 of 61 connections were fast (≈ 21 %): run 2 had none among 9 (≈ 12 % at that
  rate). Path selection can only keep a fast path it finds; more probes make finding one more likely.
* **Run 3 found a defect, fixed in v0.4.3:** two probe set-ups hung until their 30 s timeout inside the writer
  thread; for 30 s no clock sample was taken and reads queued up (depth 479; largest clock-sample gap 34.4 s). Data
  was never at risk, but the latency evidence of that time was lost. Probes now connect in the background, bounded
  by one window. Run 4 had a failed probe set-up too, without effect: largest gap 5.2 s (the interval), maximum
  queue depth 48 (ordinary bursts).
* Latency reports (released policy): run 1 P50 18.2 ms, run 4 P50 18.2 ms on the kept fast path (82 trusted reads in
  5 minutes; P95/P99 need more); each value ± up to 20 ms, not certified.

## Found on the way: first run on Linux

The first test run on Linux (before this capture) found two defects that never showed on Windows; both are fixed in
v0.4.2 with regressions:

* A REST body cut short by the connection: on Linux a shut-down socket ends the read with EOF and `http.client`
  returns the partial body without an error (Windows raises). It is now a recorded transport error, never an answer.
* A capture killed between two buffer flushes could leave an event on disk without its raw message (2 of 12 runs).
  Recovery correctly sealed only the evidenced events, so nothing false was accepted; but "raw evidence first" held
  only inside Python's buffers. Raw files are now unbuffered and synced first.

Two tests also needed portable clean-up of directory links (symlink on Linux, junction on Windows).
