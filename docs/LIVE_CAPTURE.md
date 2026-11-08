# Live capture to sealed sessions — contract (Task 004 phase 1)

Status: implemented (v0.4.0). The session format `FTDI-Live-Session-v1` is first released with v0.4.0; sessions
written by pre-release development builds are not supported. Design and rationale: `docs/TASK004_LIVE_INGEST_DESIGN.md`.
Phase 1 ends at a **verified sealed session** whose `events.jsonl` is replayed by the unchanged pipeline
(`replay` → `lab-e2e` → `audit-e2e-archive`). The run-chain audit across sessions and the latency report are
phase 2.

Read-only public market data only. No API key, no private endpoint, no order, no `POST`. Standard library only.

## 1. Endpoints and the network rule

| Mode | WebSocket | REST | Selected by |
|---|---|---|---|
| loopback (default) | `ws://127.0.0.1:<port>` | `http://127.0.0.1:<port>` | `ws_url` and `rest_url` both given |
| public | `wss://data-stream.binance.vision` | `https://data-api.binance.vision` | `public_network=True` and **no** URLs given |

Anything else (another host, `localhost`, `::1`, another scheme, a path, query, user info, only one URL, URLs
together with `public_network=True`) → `ValueError` **before any socket is opened**. Paths used:
WebSocket `/ws/<symbol lower-case>@aggTrade`; REST `GET /api/v3/aggTrades?symbol=<SYMBOL>&fromId=<n>&limit=<k>`
(`1 <= k <= 1000`) and `GET /api/v3/time`. TLS (public mode) verifies the certificate and host name with the
default context. `symbol` matches `[A-Z0-9]{2,24}`.

## 2. Event mapping (Core event contract, `docs/DATA_CONTRACT_V1.md`)

A WebSocket text frame must be one JSON object with exactly the venue fields of an aggTrade:
`e == "aggTrade"`, `E` (int ≥ 0), `s == <SYMBOL>`, `a`, `f`, `l`, `T` (int ≥ 0, `f <= l`), `p`, `q` (strings,
finite positive decimals), `m`, `M` (booleans). Extra keys are allowed and ignored. A REST aggTrades body is a
JSON array of objects with `a, p, q, f, l, T, m, M` (same rules, no `e/E/s`). Integers are JSON integers
(not booleans, not floats). The Core event is, for both origins:

```
{"source": "binance_spot_ws_aggtrade", "symbol": SYMBOL, "venue": "BINANCE_SPOT", "channel": "aggTrades",
 "source_seq": a, "event_time_ms": T, "event_time_native": T, "event_time_native_unit": "ms",
 "price": p, "qty": q, "first_trade_id": f, "last_trade_id": l, "is_buyer_maker": m, "is_best_match": M}
```

`price`/`qty` keep the exact text of the **first** origin seen for that `a`. Two records of the same `a` are
**identical** when `f, l, T, m, M` are equal and `p`, `q` are equal as decimals; a textual difference of equal
values is counted as `text_variants`. Any other difference is a **conflict**.

## 3. Capture algorithm

`capture_session(capture_dir, *, symbol, ws_url=None, rest_url=None, public_network=False, max_events=10000,
max_seconds=600.0, previous_session=None, limits=CaptureLimits()) -> dict`

* A reader thread receives WebSocket messages and, immediately after each read, records `read_perf_ns`
  (`time.perf_counter_ns()`), `read_wall_ns` (the precise wall clock: `GetSystemTimePreciseAsFileTime()` on Windows,
  `time.time_ns()` elsewhere — CPython before 3.13 implements `time.time_ns()` on Windows with the coarse
  `GetSystemTimeAsFileTime()`; the wall clock is read between two interval-clock reads and mapped to
  `read_perf_ns`, retried up to 8 times until both lie within 0.1 ms, because the ctypes call releases the GIL)
  and `queue_depth` (messages already waiting in
  the bounded queue) and enqueues the message. A full queue stops the capture (`READER_OVERFLOW`); messages are
  never dropped (the one that did not fit is kept). The writer (calling thread) does all validation and
  persistence. When the capture stops, every message already received but not yet interpreted (still queued, or
  kept on overflow) is appended to `raw_ws` as evidence after the stop.
* Every WebSocket text message is appended to `raw_ws` **before** it is interpreted; every REST response (any
  status) to `raw_rest` before it is interpreted.
* `expected` = next `source_seq` to write. Without `previous_session` the first valid record seen (WS or REST)
  anchors the session (`first_seq`). With `previous_session` (a sealed, verified, **non-empty** session of the
  same symbol; otherwise `ValueError` before any socket) `expected = previous.last_seq + 1` from the start, that is
  `first_seq`, and one REST catch-up request `fromId=expected` is made `gap_wait_ms` after the WebSocket is
  connected, unless WebSocket records advanced `expected` or opened a gap meanwhile.
* Record with `a < first_seq` (only possible with an anchor): counted `before_anchor`, compared with nothing,
  not written. `first_seq <= a < expected`: must be identical to the written event (duplicate) else conflict.
  `a == expected`: written; then buffered records continue the sequence. `a > expected`: buffered (at most
  `limits.max_pending` distinct seqs, else `PENDING_OVERFLOW`); identical re-delivery of a buffered seq is a
  duplicate, a different one a conflict.
* Gap repair: while a gap is open (buffered records exist, or `previous_session` requires catch-up) and it has
  been open at least `limits.gap_wait_ms`, the writer requests REST `fromId=expected`,
  `limit=min(1000, missing)`, at most once per `limits.rest_min_interval_s`. A non-empty page must start exactly
  at `expected` and be consecutive, else `FORMAT_ERROR`. Its records are processed like WS records (so a record
  that differs from a buffered WS record is a conflict). An empty or short page never authorises a skip. If the
  gap is still open after `limits.gap_timeout_s`, or after `limits.max_rest_pages_per_gap` pages for one gap →
  `GAP_BLOCKED`.
* Connection loss (close frame, EOF, socket error, read timeout `limits.ws_read_timeout_s`): reconnect after
  `min(8, 0.5·2^k)` s ± 10 % jitter, at most `limits.max_reconnects` times (`CONNECTION_FAILED` after that); the
  resulting gap is repaired by the rule above; after every reconnect one deferred REST catch-up request
  `fromId=expected` is made by the same rule as at the start (an empty answer just means nothing new). Each
  connection has an id (`conn`, from 1). A message split into more than 1024 frames → `FORMAT_ERROR`.
* Clock samples: `GET /api/v3/time` at start, every `limits.clock_interval_s`, and at the end (budget 2 s); the
  stamps `t0`/`t1` bracket only the request and its answer on an already established connection (TCP and TLS set-up
  excluded); each of `t0` and `t1` is a wall/interval pair taken the same way as the read stamps;
  failures (transport errors, non-200, a body without an integer `serverTime`) are recorded and do not stop the
  capture.
* Deadlines: every WebSocket handshake completes within `ws_read_timeout_s` and before the capture deadline;
  every REST call (name resolution, connect, headers, body) completes within 8 s and before the capture deadline,
  else it is aborted and recorded as a transport error. The minimum interval between REST calls
  (`rest_min_interval_s`, never shortened) is waited for first and counts against the call's budget; the network
  part gets what remains, at least 0.05 s. Name resolution has no timeout of its own: it runs in a
  worker thread that is abandoned at the deadline (since v0.4.1); a connection set up after the budget is a
  failed attempt, not a late answer. A capture therefore ends at most a few seconds after
  `max_seconds` (the final clock sample has its own 2 s budget).
* HTTP 429 or 418 on any REST call → `RATE_LIMITED` (the `Retry-After` value is recorded in `error`, nothing is
  retried). At the final clock sample it turns `MAX_EVENTS` / `MAX_SECONDS` / `INTERRUPTED` into `RATE_LIMITED`;
  after any other stop the first cause stays and `stop_detail` notes the rate limit. Other HTTP errors on aggTrades: retried like an empty page. A 200
  aggTrades response whose body is larger than `max_rest_body_bytes` or not UTF-8 → `FORMAT_ERROR` (the received
  bytes are kept, base64, in `body_base64`).
* Stop reasons: `MAX_EVENTS` (exactly `max_events` written), `MAX_SECONDS`, `INTERRUPTED`
  (KeyboardInterrupt), `GAP_BLOCKED`, `HALTED_CONFLICT`, `FORMAT_ERROR` (invalid WS message, invalid REST body or
  page, binary or oversized message), `READER_OVERFLOW`, `PENDING_OVERFLOW`, `RATE_LIMITED`,
  `CONNECTION_FAILED`. Every stop seals the session with the contiguous events written so far (possibly none).
  Nothing past a gap or a conflict is ever written to `events.jsonl`.
* **Conflict cut at sealing**: before sealing, all raw evidence of the session is scanned with the rule of §5
  (every well-formed record counts, also inside a malformed page, inside a body kept as `body_base64` — truncated
  or not UTF-8: the complete records at its start are salvaged — and after the stop). If any raw record
  contradicts a written event, that seq is ambiguous: `events.jsonl` is cut back to the events **before** the
  smallest such seq (the raw files keep both deliveries) and the stop reason becomes `HALTED_CONFLICT`. A sealed
  session can therefore never show `RAW_CONFLICT`.

`CaptureLimits` (frozen dataclass, all fields keyword, validated): `queue_size=10000`, `max_pending=100000`,
`gap_wait_ms=200`, `gap_timeout_s=10.0`, `max_rest_pages_per_gap=10`, `rest_min_interval_s=0.35`
(≥ 0.1 in public mode; ≥ 0 in loopback mode), `max_reconnects=5`, `ws_read_timeout_s=30.0`,
`max_message_bytes=1048576`, `max_rest_body_bytes=2097152`, `clock_interval_s=60.0`, `fsync_every=256`.

## 4. Session directory and files

`<capture_dir>/<SYMBOL>/session-<UTC yyyymmddTHHMMSSZ>[-<k>]/` (a fresh directory; `-<k>` only if the name
exists). While capturing, files carry the suffix `.part`; sealing renames them. Every line is canonical JSON
(`sort_keys`, compact separators, `ensure_ascii=False`) + `\n`.

| File | One line per | Fields |
|---|---|---|
| `events.jsonl` | Core event, ascending contiguous `source_seq` | §2 |
| `raw_ws.jsonl` | WebSocket text message as received | `conn`, `n` (0-based, session-wide), `read_wall_ns`, `read_perf_ns`, `queue_depth`, `text` |
| `raw_rest.jsonl` | REST response | `n`, `kind` (`aggTrades`/`time`), `url`, `status`, `t0_wall_ns`, `t0_perf_ns`, `t1_wall_ns`, `t1_perf_ns`, `body` (text, or `null` on transport error or unusable body), `body_base64` (received bytes when the body is too large or not UTF-8, else `null`), `error` (string or `null`) |
| `clock.jsonl` | successful time sample | `t0_wall_ns`, `t0_perf_ns`, `venue_ms`, `t1_wall_ns`, `t1_perf_ns` |
| `CAPTURE_START.json` | — | written when the session starts: `schema` (`FTDI-Live-Capture-Start-v1`), `symbol`, `lane`, `started_utc`, `software_version`, `endpoints`, `public_network`, `limits`, `max_events`, `max_seconds`, `previous` (as in the manifest), and since v0.4.1 `clocks` (`{wall, interval}`: `implementation` (1-200 characters), `resolution_ns` (integer 1..10^9)); v0.4.0 sessions without `clocks` remain valid. Since v0.4.1 every integer of the evidence files and of venue messages (stamps, counters, ids, `E`, `T`) must be in 0..2^63−1 |
| `SESSION_MANIFEST.json` | — | below; written last, atomically |

Files are flushed and `fsync`ed every `limits.fsync_every` events and at sealing.

`SESSION_MANIFEST.json` (`schema: "FTDI-Live-Session-v1"`): `session_id` (directory name), `symbol`, `lane`
(the 4 scope fields), `software_version`, `started_utc`, `sealed_utc`, `sealed_by` (`capture`/`recovery`),
`first_seq`, `last_seq` (both `null` when empty), `event_count`, `previous` (`null` or `{session_id, last_seq,
manifest_sha256}`), `chain` (`FIRST` without previous, `CONTIGUOUS` when `first_seq == previous.last_seq + 1` or
the session is empty), `stop_reason`, `stop_detail` (human-readable cause, may be empty), `files` (`{name: {bytes, sha256, lines}}` for the four data files),
`counts` (`ws_messages`, `ws_duplicates`, `rest_pages`, `rest_records`, `rest_duplicates`, `text_variants`,
`before_anchor`, `connections`, `reconnects`, `clock_samples`, `gaps_repaired`), `endpoints` (`ws`, `rest`),
`public_network`, `limits`, `capture_start_sha256`, `interpreted` (`{raw_ws: k, raw_rest: j}`: the first `k`
resp. `j` raw lines were interpreted by the capture; later lines are evidence received after the stop), and for
recovery `recovered_from`
(`{name.part: {bytes, sha256}}`; counts that cannot be derived from the files are `null`).

The return value of `capture_session` is the manifest plus `session_dir`.

## 5. Verification (read-only)

`verify_session(session_dir, *, previous=None) -> dict` with `status` `VERIFIED` or `INVALID`, `issue_counts`
(code → count, notices included, as in `audit-e2e-archive`), `notice_codes` and `metrics`. It never creates or changes a file. Codes:

| Code | Condition |
|---|---|
| `UNSEALED` | no manifest (status `INVALID`) |
| `MANIFEST_INVALID` | manifest unreadable, wrong schema, wrong field types, inconsistent fields, options no capture can have used (`public_network` not a boolean, endpoints other than the loopback forms or the fixed public pair, `limits` not a valid `CaptureLimits` for the mode, invalid capture bounds), or `CAPTURE_START.json` describing another capture (symbol, lane, previous; for a capture-sealed session also endpoints, limits, start time) |
| `FILE_MISSING` / `FILE_MISMATCH` | a data file, `CAPTURE_START.json` or a recorded `.part` file absent, or bytes / SHA-256 / line count differ from the manifest; after recovery a raw file that is not the complete lines of its `.part`, `events.jsonl` not a prefix of it, or a `.part` file not recorded |
| `EVENT_INVALID` | an events line is not a canonical Core event of §2 for this symbol |
| `EVENT_SEQUENCE_GAP` | `source_seq` not contiguous `first_seq..last_seq`, or count ≠ `event_count` |
| `EVENT_WITHOUT_RAW_EVIDENCE` | no `raw_ws` message or `raw_rest` aggTrades record with that `a` that is identical (§2) |
| `RAW_CONFLICT` | a raw record (any well-formed record, also inside a malformed page, a `body_base64` body or after the stop) with `a` in `[first_seq, last_seq]` not identical to the event |
| `RAW_UNPARSEABLE` | a raw line is not valid JSON of the documented shape, `n` is not consecutive, or an **interpreted** message/page is not a valid aggTrade record |
| `CHAIN_MISMATCH` | `chain` incoherent with the recorded `previous` (`FIRST` with a previous, `CONTIGUOUS` without, `first_seq ≠ previous.last_seq + 1`); with `previous`: it is not `VERIFIED`, or its manifest SHA-256 / id / last_seq differ from the recorded link |

Notices (counted, do not change the status): `LEFTOVER_PART_FILE` (a `.part` file kept after recovery),
`RAW_AFTER_STOP` (a raw message or page after the interpreted prefix that is not a valid aggTrade record: it caused
the stop or arrived after it, and is kept as evidence; its well-formed records still count for `RAW_CONFLICT`).
Verification never raises on file content; it only raises `FileNotFoundError` for a missing directory.

## 6. Recovery of an unsealed session

`seal_session(session_dir, *, apply=False) -> dict`: for a directory with `.part` files and no manifest (a killed
capture). Dry run (default) changes nothing and reports `would_seal` (`first_seq`, `last_seq`, `event_count`).
Apply writes the final files with the **complete lines** only: `events.jsonl` = the longest prefix of complete,
valid, contiguous events that are supported by raw evidence, cut before any seq that raw evidence contradicts
(§3); the raw files = their complete lines. The `.part` files are **kept** (never deleted) and listed in
`recovered_from`. `stop_reason` = `RECOVERED`; `interpreted` = `{raw_ws: 0, raw_rest: 0}` (unknown after a kill).
A capture killed between the renames of sealing (some data files already final, the others `.part`) is recovered
the same way. Refused (`ValueError`, nothing written): a manifest already exists; `CAPTURE_START.json` missing or
inconsistent; a data file both final and `.part`, or neither; a complete raw line that is malformed or whose `n`
is not consecutive; a recovered session that would not verify: the complete result (start record, final files,
`.part` copies, manifest) is first built in a private sibling directory `.<session>.recovery-check` and checked
with `verify_session` itself (a uniquely named directory created with `mkdtemp`, owned by this call); only a
`VERIFIED` result is written into the session directory, and the check directory is removed.

## 7. CLI

```
python -m ftdi_core live-capture --symbol BTCUSDT --capture-dir DIR (--public-network | --ws-url URL --rest-url URL)
                                [--max-events N] [--max-seconds S] [--previous-session DIR]
python -m ftdi_core verify-session --session DIR [--previous DIR]
python -m ftdi_core seal-session --session DIR [--apply]
```

ASCII JSON on stdout. Exit codes: `live-capture` 0 when sealed with `MAX_EVENTS`, `MAX_SECONDS` or
`INTERRUPTED`, 2 when sealed with any other stop reason; `verify-session` 0 `VERIFIED`, 2 `INVALID`; all three 3 on
a precondition failure (`FTDI_CORE_FAIL_CLOSED: ...`, nothing created).

## 8. What a sealed session proves, and what not

All maintenance commands (`seal-session`, `compose-sessions`) assume that no other process writes the session,
capture or output directories while they run (the same maintenance-window precondition as for every FTDI
retention command). Within that precondition the guarantees above are exact. Outside it, a concurrent writer can
at most make a command refuse, or, if it can write a session's events, raw evidence and manifest coherently, give it
different but self-consistent content — which no local verification can distinguish from genuine evidence (the
trust boundary stated for every FTDI audit: no signatures or WORM storage).

It proves that every event in `events.jsonl` was delivered by the venue (raw evidence) and that the range
`first_seq..last_seq` is contiguous with no conflicting delivery. It does **not** prove that the venue's stream
before `first_seq` or after `last_seq` was captured, nor any latency (phase 2, Task 003 rules).

## 9. Phase 2: from sessions to the audited pipeline, and latency evidence

**Chain of sessions.** Each capture after the first is started with `--previous-session` (the last non-empty
sealed session); it continues exactly at `previous.last_seq + 1` (REST catch-up) or seals empty/`GAP_BLOCKED`, so a
chain never skips a sequence number. A fresh chain (no previous) starts a new, independent range.

`compose_sessions(sessions, output_dir)` / `python -m ftdi_core compose-sessions --session S1 --session S2 ... --output DIR`
first copies every session into a private snapshot inside the new output directory (which must lie outside every
session directory), verifies the **snapshots** against their predecessors in the given order (empty sessions
allowed in between) and composes from the same snapshots: the output always equals verified session content, and a
change of an original after its snapshot was taken cannot reach it; it refuses any session that
is not `VERIFIED` in this chain, and refuses if any file of any original session (including retained `.part`
files) differs at the end from its state before the snapshot; it re-checks every output line (canonical Core
event, contiguous over the declared range), removes the snapshots, removes its own output on any refusal or error,
and writes a **new** directory with `events.jsonl` (the
concatenation of the sessions' events, contiguous), `source_manifest.json` (since v0.4.1: the
`FTDI-Core-SourceManifest-v1` record `prepare` writes for historical files, `source_kind: LIVE_SESSION_CHAIN`, so the
archive-aware audit checks the declared hash, count and range) and `CHAIN_MANIFEST.json` (`schema: "FTDI-Live-Chain-v1"`,
symbol, lane, first/last seq, count, per session id, path, manifest SHA-256, events SHA-256, range and stop reason,
and the SHA-256 of the output). The output is an ordinary immutable source:

```
python -m ftdi_core replay --lane LIVE=DIR\events.jsonl --outdir RUN
python -m ftdi_core lab-e2e --outdir RUN --consumer-db CONSUMER
python -m ftdi_core audit-e2e-archive --outdir RUN --consumer-db CONSUMER
```

When the chain grows, compose it again **from its head** into a new directory and replay it into a **new** run
directory with the same consumer: known events are duplicates (no new effect), the new run covers every effect of
the lane, and the archive-aware audit is `COMPLETE`.

**Latency evidence.** `python -m ftdi_core analyze-clock-evidence --session DIR [--report FILE]` applies the
ADR-008 rules (`docs/TASK003_TIME_PROVENANCE_DESIGN.md`) to the WebSocket message that **first** delivered each
event (events recovered only by REST have no read time and are counted separately). Each event uses the
minimum-RTT clock sample within `max_offset_age_s` of its read time, preferring samples taken under the same wall
clock as the read (their wall-minus-interval levels agree), so that a sample taken after a wall-clock step corrects
the reads after it. The round trip of a sample is the larger of its wall and interval measurement. The local clock
resolution in the uncertainty is the session's recorded wall-clock resolution (15.625 ms assumed for v0.4.0
sessions, which recorded none, whatever machine analyses them). A
report with corrected but no trusted samples has status `NO_TRUSTED_SAMPLES`. Clock samples carry interval
stamps, so each read is checked against the wall-minus-interval level of the sample that corrects it (a moved wall
clock excludes that read; a smaller difference is added to its uncertainty). Tags (`QUEUED_READ`, `OFFSET_UNCERTAIN`, ...)
exclude samples; percentiles need 20 / 100 / 1000 trusted samples; nothing is certified.
