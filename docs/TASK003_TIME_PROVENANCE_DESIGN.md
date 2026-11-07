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
  `θ = s − (t0 + t1)/2`; uncertainty `u = (t1 − t0)/2 + venue resolution + local wall resolution`.
- **Estimate**: the minimum-RTT sample within a window (NTP-style clock filter); samples with RTT above a
  limit are discarded. Uncertainty grows with age: `u(t) = u0 + ρ · |t − t_est|`, default drift bound
  ρ = 100 ppm. An estimate older than `max_age` is stale.
- **Assumption, always stated**: the venue's server-time clock and its event-time clock are coherent. This is
  not verifiable from outside and is listed in every report.
- **Latency estimate** for an event: `L = (read_wall − θ) − venue_event_ms`, reported with `±u(t)`.

| Tag | Condition | Effect |
|---|---|---|
| `CLOCK_UNCORRECTED` | no offset estimate | only the raw difference is reported; never a latency |
| `OFFSET_STALE` | estimate older than `max_age` | excluded from percentiles |
| `OFFSET_UNCERTAIN` | `u(t)` above the policy limit (default 20 ms) | excluded from percentiles |
| `NEGATIVE_BEYOND_UNCERTAINTY` | `L < −u(t)` | clock model or timestamp definition is wrong; excluded and counted; many → report INCONCLUSIVE |
| `WALL_CLOCK_STEP` | wall and perf disagree between consecutive reads by more than the tolerance | offset invalidated from that point |
| `QUEUED_READ` | `queue_depth_at_read > 0`, or the read followed the previous one faster than the interval-clock resolution | read time ≠ arrival time; excluded from latency, kept for throughput |
| `LOW_RESOLUTION_CLOCK` | interval clock resolution above the limit | no interval metrics |

## 5. Percentile reporting rules

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

## 7. Next steps (not done)

1. A live adapter must implement §3: decoupled reader thread with `perf_counter_ns`, queue depth, raw-frame
   journal commit timestamps.
2. Offset sampling against the venue's public server-time endpoint (read-only, rate-limited) before, during
   and after a capture. This is a network action and will be run only on the owner's machine.
3. Then a fresh live capture, evaluated with §4–§5.
