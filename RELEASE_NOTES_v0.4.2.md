# FTDI-Core v0.4.2 — release notes

Date: 2026-11-09. Platforms verified: native Windows 11 Pro (Python 3.11.9, SQLite 3.45.1) and, for the first time,
Linux (Ubuntu 24.04.5 LTS, Python 3.12.3). Status: **local research prototype; not production certified; not a
trading program.**

## New

* **Linux verified.** Both test suites pass on Ubuntu 24.04 (Windows-only tests skip there by design), the live
  pipeline acceptance on 127.0.0.1 passes, and a real-network capture ran end to end on a Linux cloud server
  (capture, verify, compose, replay, independent consumer, archive-aware audit COMPLETE).
* **`live-capture --clock-interval-s S`** (default 60): seconds between server-time clock samples. More samples make a
  fast round trip, and so a small offset uncertainty, more likely. Against the public venue at least 1 s; a public
  session whose start record or manifest claims less is INVALID.
* **First trusted latency estimate** (`docs/REAL_LIVE_REMOTE_RESULTS.md`): on the owner's Linux cloud server, with
  a clock sample every 5 s, 16 of 121 samples had a round trip below 38 ms; the report is `ESTIMATED` with 1,147
  trusted reads: P50 18.5 ms, P95 19.7 ms, P99 20.0 ms, each with an uncertainty of up to ±20 ms. An independent
  check with the NTP-disciplined host clock (uncorrected read time − event time) gives P50 19.5 ms. From the owner's
  PC the round trip never fell below 42.7 ms (third capture, `docs/REAL_LIVE_PILOT2_RESULTS.md`), so the 20 ms limit
  cannot be met there.

## Fixed (found by the first Linux run)

* **A REST body cut short is not an answer.** On Linux a shut-down or closed socket ends the body read with EOF and
  `http.client` returns the partial body without an error (Windows raises). A body shorter than its Content-Length is
  now a recorded transport error (`IncompleteRead`), never used.
* **Raw evidence reaches the operating system before anything derived from it.** A capture killed between two
  buffer flushes could leave an event on disk without its raw message (2 of 12 runs on Linux). Recovery correctly
  sealed only the evidenced events — nothing false was accepted — but "raw first" held only in Python's buffers.
  The raw files are now unbuffered (one write per received message) and are synced first.
* Two tests cleaned up directory links with `rmdir`, which fails for a symlink on Linux.

## Compatibility

* All six real sessions captured so far verify unchanged (re-verified with this version). A public-mode session
  with `clock_interval_s` below 1 s would now be INVALID; none exists.
* No change to schemas, the historical pipeline, delivery, the consumer, retention, `archive-producer` or `vacuum-db`.

## Verification

See `docs/RELEASE_GATES_V04.md`, section v0.4.2.

## Not proven

A certified latency: the estimate is a research figure with a stated uncertainty of up to ±20 ms, from one 10-minute
capture on one host. Sustained real-network operation. Linux full-day acceptance with the owner's historical ZIP
(the ZIP was not copied to the server).
