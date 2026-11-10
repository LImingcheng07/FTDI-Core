# FTDI-Core v0.4.3 — release notes

Date: 2026-11-10. Platforms verified: Windows 11 Pro (Python 3.11.9) and Ubuntu 24.04.5 LTS (Python 3.12.3).
Status: **local research prototype; not production certified; not a trading program.**

## New: path selection (`docs/LIVE_CAPTURE.md` §3a, ADR-012)

On the owner's Linux cloud server each WebSocket connection keeps one network path, and the venue-to-host delivery
delay is either ≈ 19 ms or ≈ 36 ms depending on the connection (experiment: 3 of 12 fast; same server address both
ways). `live-capture --path-probes K --path-probe-s W` (off by default):

* opens a probe connection next to the active one (make-before-break: both deliver, duplicates are identical, so
  there is no gap and no REST request), compares the delivery level of both over the same window (10th percentile
  of read time − venue event time of unqueued reads; offset-free) and keeps the faster; up to `K` probes;
* a probe that cannot connect or closes is given up; if the active connection closes while a probe streams, the
  probe takes over; probes connect in the background and never hold up the capture;
* `CAPTURE_START.json` records `path_selection: {probes, window_s}` only when enabled; the decisions are returned by
  the capture and re-derivable from the raw evidence; `analyze-clock-evidence --session` reports every connection's
  delivery level (`connections`).

Real network (`docs/REAL_LIVE_REMOTE_RESULTS.md`, `examples/REAL_LIVE_PATH_SELECTION_EVIDENCE.json`): four runs, all
VERIFIED, no REST, no lost trade; two switched from ≈ 35 ms to ≈ 19 ms, one started on the fast path and kept it, one
found no fast connection among 9. Run 3 exposed probe set-ups that blocked the capture for 30 s; fixed (background
set-up) and confirmed in run 4.

## Test safety

A mutation check showed that a test whose public-mode refusal was broken could start a real read-only capture
against the public endpoint (stopped by the test timeout after up to 5 minutes; the data was deleted). The live test
suite now has a loopback-only guard (`tests/fakevenue.py`): connection set-up to anything but 127.0.0.1 fails at once.

## Compatibility

Default sessions are unchanged (no new field; readable by v0.4.1/v0.4.2). Sessions captured with path selection
carry the optional `path_selection` start field and are verified by v0.4.3 or later. Latency reports gained the
field `connections`. No change to the historical pipeline, delivery, consumer, retention or `vacuum-db`.

## Verification

See `docs/RELEASE_GATES_V04.md`, section v0.4.3.

## Historical pipeline

Unchanged since v0.4.2 (byte-identical outside `ftdi_core/live` except the version string and two `live-capture`
arguments). The full-day acceptance was not re-run for this version: the owner's original ZIP is no longer at its
location. See gate R7.

## Not proven

That a fast path exists at every time or on every host; a certified latency; sustained operation.
