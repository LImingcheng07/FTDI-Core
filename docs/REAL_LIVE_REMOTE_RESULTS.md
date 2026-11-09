# Real-network capture on a Linux cloud server (2026-11-09)

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

## Found on the way: first run on Linux

The first test run on Linux (before this capture) found two defects that never showed on Windows; both are fixed in
v0.4.2 with regressions:

* A REST body cut short by the connection: on Linux a shut-down socket ends the read with EOF and `http.client`
  returns the partial body without an error (Windows raises). It is now a recorded transport error, never an answer.
* A capture killed between two buffer flushes could leave an event on disk without its raw message (2 of 12 runs).
  Recovery correctly sealed only the evidenced events, so nothing false was accepted; but "raw evidence first" held
  only inside Python's buffers. Raw files are now unbuffered and synced first.

Two tests also needed portable clean-up of directory links (symlink on Linux, junction on Windows).
