# Live capture to sealed sessions — contract (Task 004 phase 1)

Status: contract for implementation and black-box tests. Design and rationale: `docs/TASK004_LIVE_INGEST_DESIGN.md`.
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

* A reader thread receives WebSocket messages and, immediately after each read, records `read_wall_ns`
  (`time.time_ns()`), `read_perf_ns` (`time.perf_counter_ns()`) and `queue_depth` (messages already waiting in
  the bounded queue) and enqueues the message. A full queue stops the capture (`READER_OVERFLOW`); messages are
  never dropped. The writer (calling thread) does all validation and persistence.
* Every WebSocket text message is appended to `raw_ws` **before** it is interpreted; every REST response (any
  status) to `raw_rest` before it is interpreted.
* `expected` = next `source_seq` to write. Without `previous_session` the first valid record seen (WS or REST)
  anchors the session (`first_seq`). With `previous_session` (a sealed, verified, **non-empty** session of the
  same symbol; otherwise `ValueError` before any socket) `expected = previous.last_seq + 1` from the start, that is
  `first_seq`, and one REST catch-up request `fromId=expected` is made at the start.
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
  resulting gap is repaired by the rule above; after every reconnect one REST catch-up request `fromId=expected`
  is made (an empty answer just means nothing new). Each connection has an id (`conn`, from 1).
* Clock samples: `GET /api/v3/time` at start, every `limits.clock_interval_s`, and at the end; failures are
  recorded and do not stop the capture.
* HTTP 429 or 418 on any REST call → `RATE_LIMITED` (the `Retry-After` value is recorded, nothing is retried).
  Other HTTP errors on aggTrades: retried like an empty page.
* Stop reasons: `MAX_EVENTS` (exactly `max_events` written), `MAX_SECONDS`, `INTERRUPTED`
  (KeyboardInterrupt), `GAP_BLOCKED`, `HALTED_CONFLICT`, `FORMAT_ERROR` (invalid WS message, invalid REST body or
  page, binary or oversized message), `READER_OVERFLOW`, `PENDING_OVERFLOW`, `RATE_LIMITED`,
  `CONNECTION_FAILED`. Every stop seals the session with the contiguous events written so far (possibly none).
  Nothing past a gap or a conflict is ever written to `events.jsonl`. A conflict for an **already written** seq
  makes that seq ambiguous: at sealing, `events.jsonl` is cut back to the events **before** it (the raw files keep
  both deliveries), so `last_seq = conflicting seq − 1`.

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
| `raw_rest.jsonl` | REST response | `n`, `kind` (`aggTrades`/`time`), `url`, `status`, `t0_wall_ns`, `t0_perf_ns`, `t1_wall_ns`, `t1_perf_ns`, `body` (text, or `null` on transport error), `error` (string or `null`) |
| `clock.jsonl` | successful time sample | `t0_wall_ns`, `t0_perf_ns`, `venue_ms`, `t1_wall_ns`, `t1_perf_ns` |
| `CAPTURE_START.json` | — | written when the session starts: `schema` (`FTDI-Live-Capture-Start-v1`), `symbol`, `lane`, `started_utc`, `software_version`, `endpoints`, `public_network`, `limits`, `max_events`, `max_seconds`, `previous` (as in the manifest) |
| `SESSION_MANIFEST.json` | — | below; written last, atomically |

Files are flushed and `fsync`ed every `limits.fsync_every` events and at sealing.

`SESSION_MANIFEST.json` (`schema: "FTDI-Live-Session-v1"`): `session_id` (directory name), `symbol`, `lane`
(the 4 scope fields), `software_version`, `started_utc`, `sealed_utc`, `sealed_by` (`capture`/`recovery`),
`first_seq`, `last_seq` (both `null` when empty), `event_count`, `previous` (`null` or `{session_id, last_seq,
manifest_sha256}`), `chain` (`FIRST` without previous, `CONTIGUOUS` when `first_seq == previous.last_seq + 1` or
the session is empty), `stop_reason`, `stop_detail` (human-readable cause, may be empty), `files` (`{name: {bytes, sha256, lines}}` for the four data files),
`counts` (`ws_messages`, `ws_duplicates`, `rest_pages`, `rest_records`, `rest_duplicates`, `text_variants`,
`before_anchor`, `connections`, `reconnects`, `clock_samples`, `gaps_repaired`), `endpoints` (`ws`, `rest`),
`public_network`, `limits`, `capture_start_sha256`, and for recovery `recovered_from`
(`{name.part: {bytes, sha256}}`; counts that cannot be derived from the files are `null`).

The return value of `capture_session` is the manifest plus `session_dir`.

## 5. Verification (read-only)

`verify_session(session_dir, *, previous=None) -> dict` with `status` `VERIFIED` or `INVALID`, `issue_counts`
(code → count, notices included, as in `audit-e2e-archive`), `notice_codes` and `metrics`. It never creates or changes a file. Codes:

| Code | Condition |
|---|---|
| `UNSEALED` | no manifest (status `INVALID`) |
| `MANIFEST_INVALID` | manifest unreadable, wrong schema or inconsistent fields |
| `FILE_MISSING` / `FILE_MISMATCH` | a data file absent, or bytes / SHA-256 / line count differ from the manifest |
| `EVENT_INVALID` | an events line is not a canonical Core event of §2 for this symbol |
| `EVENT_SEQUENCE_GAP` | `source_seq` not contiguous `first_seq..last_seq`, or count ≠ `event_count` |
| `EVENT_WITHOUT_RAW_EVIDENCE` | no `raw_ws` message or `raw_rest` aggTrades record with that `a` that is identical (§2) |
| `RAW_CONFLICT` | a raw record with `a` in `[first_seq, last_seq]` not identical to the event |
| `RAW_UNPARSEABLE` | a raw line is not valid JSON of the documented shape |
| `CHAIN_MISMATCH` | `chain` incoherent with the recorded `previous` (`FIRST` with a previous, `CONTIGUOUS` without, `first_seq ≠ previous.last_seq + 1`); with `previous`: it is not `VERIFIED`, or its manifest SHA-256 / id / last_seq differ from the recorded link |

Notices (counted, do not change the status): `LEFTOVER_PART_FILE` (a `.part` file kept after recovery),
`RAW_STOP_MESSAGE` (the last raw message or page is not a valid aggTrade record and the session stopped with
`FORMAT_ERROR` or was recovered: it caused the stop and is kept as evidence).

## 6. Recovery of an unsealed session

`seal_session(session_dir, *, apply=False) -> dict`: for a directory with `.part` files and no manifest (a killed
capture). Dry run (default) changes nothing and reports `would_seal` (`first_seq`, `last_seq`, `event_count`).
Apply writes the final files with the **complete lines** only: `events.jsonl` = the longest prefix of complete,
valid, contiguous events that are supported by raw evidence; the raw files = their complete lines. The `.part`
files are **kept** (never deleted) and listed in `recovered_from`. `stop_reason` = `RECOVERED`. A directory that
already has a manifest → `ValueError`.

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

It proves that every event in `events.jsonl` was delivered by the venue (raw evidence) and that the range
`first_seq..last_seq` is contiguous with no conflicting delivery. It does **not** prove that the venue's stream
before `first_seq` or after `last_seq` was captured, nor any latency (phase 2, Task 003 rules).
