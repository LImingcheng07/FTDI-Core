# Task 003 — Trustworthy timestamp provenance (design)

Status: **ACCEPTED for local development (ADR-008)**. Contract, pure evaluation functions, deterministic
tests and a read-only retro-analysis of the 200-event live pilot are implemented. **No live capture, no
network access, no credentials.** A fresh live capture is a separate, later step.

## 1. Evidence from the 200-event Windows live pilot (re-analysed read-only, 2026-11-07)

Source: owner's S07-005 run `runs/live-btc/capture.sqlite3` (analysed on a byte-identical copy):

| Finding | Value | Meaning |
|---|---|---|
| `local wall − exchange E`: min / p50 / max | −601.2 / −460.9 / +230.8 ms; 199 of 200 negative | raw cross-clock differences, not latency |
| Lower envelope | −601.2 ms; 70 samples within 10 ms of it | local wall clock behind the venue clock by ≥ 601 ms + the (unknown) minimum one-way delay |
| `E − T` | 0 … 1 ms | for aggTrade, event time ≈ trade time |
| `time.monotonic()` on this machine | `GetTickCount64()`, resolution 15.625 ms, observed step 16 ms | the pilot's `mono_ns` cannot resolve intervals below ~16 ms |
| `time.perf_counter()` | `QueryPerformanceCounter()`, 100 ns | the only suitable interval clock on Windows |
| Receive timestamp definition | taken after `recv()` in the same loop that persists each message | "read time", inflated by queueing behind persistence; not arrival time |

Conclusion: the negative values are explained by an unmeasured clock offset; the pilot supports **no**
latency, P50/P95/P99 or "faster than light" claim. Reproduce with
`python -m ftdi_core analyze-clock-evidence --pilot-db <capture.sqlite3>` (read-only).

## 2. Clocks and their only legitimate uses

| Clock | Source | Legitimate use | Never use for |
|---|---|---|---|
| Venue `E`/`T` | exchange | ordering hints, cross-clock evidence | local latency without an offset model |
| Local wall | `time.time_ns()` (adjustable; resolution 1–15.6 ms on Windows) | joining with external clocks after offset estimation | intervals (it can step) |
| Local interval | `time.perf_counter_ns()` (monotonic, 100 ns on Windows) | durations between local stages | absolute time |
| `time.monotonic_ns()` on Windows | `GetTickCount64`, 15.6 ms | coarse timeouts only | any latency/interval metric |

Every session records `time.get_clock_info()` for each clock; a clock whose resolution exceeds the policy
limit (default 1 ms) is tagged `LOW_RESOLUTION_CLOCK` and cannot produce interval metrics.

## 3. Timestamps per event (contract)

| Field | Clock | Stage |
|---|---|---|
| `venue_event_ms`, `venue_trade_ms` | venue | as delivered |
| `read_wall_ns`, `read_perf_ns` | local wall + interval | immediately after the frame is read, in a reader that does **not** persist inline |
| `journal_commit_perf_ns` | interval | after the raw-frame journal commit |
| `publish_perf_ns`, `consumer_commit_*`, `ack_perf_ns` | interval (same host) | later pipeline stages |
| `queue_depth_at_read` | count | frames already buffered when this one was read (0 = read as it arrived, as far as observable) |

Same-host stage durations (read → commit → publish → ACK) use only `perf` timestamps and are valid with the
clock's resolution as their uncertainty. Cross-host or venue-to-local quantities need §4.

## 4. Offset model and latency evidence

- **Offset sample** (Cristian): local wall `t0`, venue server time `s`, local wall `t1`. Offset estimate
  `θ = s − (t0 + t1)/2`; uncertainty `u = (t1 − t0)/2 + venue resolution + local wall resolution`. With
  interval stamps (since v0.4.1) the round trip is the larger of the wall and the interval measurement, and half
  of the wall clock's movement against the interval clock during the request is added (a wall clock moving back
  during the request would otherwise shorten `t1 − t0` and understate `u`).
- **Estimate**: the minimum-RTT sample within a window (NTP-style clock filter); samples with RTT above a
  limit are discarded. For live sessions (since v0.4.1) each read uses the single sample in the window that gives
  it the smallest uncertainty `u(t)` (including the drift bound over the sample's age and the measured movement
  below), preferring samples under the read's wall clock. Uncertainty grows with age: `u(t) = u0 + ρ · |t − t_est|`, default drift bound
  ρ = 100 ppm. An estimate older than `max_age` is stale.
- **Assumption, always stated**: the venue's server-time clock and its event-time clock are coherent. This is
  not verifiable from outside and is listed in every report.
- **Latency estimate** for an event: `L = (read_wall + θ) − venue_event_ms` (θ is venue minus local, so
  `read_wall + θ` is the read time on the venue clock), reported with `±u(t)`.
- **Wall clock against the interval clock** (since v0.4.1). The *level* of a stamp pair is wall minus interval
  time. When the clock sample carries interval stamps (live capture records them), each read is checked against
  the sample that corrects it: `Δ = |level(read) − level(sample)|`. `Δ` above the tolerance (5 ms) plus
  `ρ · |t_read − t_sample|` means the wall clock moved between the sample and the read, and the offset does not
  apply (`WALL_CLOCK_STEP`, or `READ_STAMP_OUTLIER` if the read also departs from its neighbouring reads);
  otherwise `Δ` is added to `u(t)`. A sample during which the level moved by more than the tolerance is
  discarded. Without interval stamps on the sample, a heuristic on the reads alone applies (table below); in that
  case reads without an interval stamp are not checked at all (as before v0.4.1: the model cannot check them).

| Tag | Condition | Effect |
|---|---|---|
| `CLOCK_UNCORRECTED` | no offset estimate | only the raw difference is reported; never a latency |
| `OFFSET_STALE` | estimate older than `max_age` | excluded from percentiles |
| `OFFSET_UNCERTAIN` | `u(t)` above the policy limit (default 20 ms) | excluded from percentiles |
| `NEGATIVE_BEYOND_UNCERTAINTY` | `L < −u(t)` | clock model or timestamp definition is wrong; excluded and counted; many → report INCONCLUSIVE |
| `WALL_CLOCK_STEP` | sample with interval stamps: `Δ` above tolerance + `ρ·age` and the read agrees with its neighbours (the clock moved between sample and read). Sample without them (heuristic): the reference starts with the first two consecutive reads that agree (levels within tolerance + `ρ`·elapsed); a read departs when it disagrees with both of them (catches slow ramps) or with the majority of the preceding 20 accepted reads; a lasting shift when the majority of this and the next 20 reads depart as well | sample with interval stamps: that read is excluded; heuristic: every read from that point is excluded |
| `WALL_CLOCK_UNVERIFIED` | (since v0.4.1) the sample has interval stamps but the read has none, so its wall clock cannot be checked against the sample | excluded |
| `READ_STAMP_OUTLIER` | (since v0.4.1) the read departs without a lasting shift (jitter, a short run of quantised stamps, a deviating last read), or comes before the first agreeing pair; with interval stamps on the sample only if `Δ` is above the limit as well | that read alone is excluded; it is never a reference for later reads |
| `QUEUED_READ` | `queue_depth_at_read > 0` (the reader found earlier messages still waiting) | read time ≠ arrival time; excluded from latency, kept for throughput |
| `LOW_RESOLUTION_CLOCK` | interval clock resolution above the limit | no interval metrics |

## 5. Percentile reporting rules

(since v0.4.1) Status `NO_TRUSTED_SAMPLES` when latencies could be corrected but no sample is trusted.


P50/P95/P99 are computed **only** over samples without exclusion tags and only when the trusted count is at
least `min_samples` (defaults: 20 for P50, 100 for P95, 1000 for P99; otherwise `"insufficient samples"`).
A report always states: trusted/total counts, counts per tag, the maximum uncertainty of the included
samples, the clock implementations, and the coherence assumption. If more than 1 % of corrected samples are
`NEGATIVE_BEYOND_UNCERTAINTY`, the whole report is `INCONCLUSIVE`. Nothing here certifies production latency.

## 6. Implemented now

- `ftdi_core/timeprov.py`: clock assessment, offset estimation, latency classification, percentile report,
  and the read-only pilot analysis (pure functions, stdlib only, no network).
- CLI `analyze-clock-evidence --pilot-db PATH [--report R.json]` (opens the DB `mode=ro&immutable=1`).
- Deterministic tests in `tests/test_timeprov.py`.
- Since v0.4.0 the live adapter (Task 004, `docs/LIVE_CAPTURE.md`): reader thread with `perf_counter_ns`, wall
  stamp and queue depth per read; server-time samples with wall and interval stamps; `analyze-clock-evidence
  --session` (`ftdi_core/live/latency.py`). Since v0.4.1 the precise Windows wall clock and the per-read check
  against the sample (§4); regressions in `tests/test_live_pilot_findings.py`.

## 7. Next steps (as written for v0.3; items 1 and 2 are done by the live adapter, see §6)

1. A live adapter must implement §3: decoupled reader thread with `perf_counter_ns`, queue depth, raw-frame
   journal commit timestamps.
2. Offset sampling against the venue's public server-time endpoint (read-only, rate-limited) before, during
   and after a capture. This is a network action and will be run only on the owner's machine.
3. Then a fresh live capture, evaluated with §4–§5.
