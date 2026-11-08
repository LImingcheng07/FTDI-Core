# Task 004 — Live read-only ingestion into Core (design)

Status: **implemented (v0.4.0)**: phase 1 sealed sessions (`docs/LIVE_CAPTURE.md` §1-§8) and phase 2 chain
composition + latency evidence (§9); ADR-011. Instead of the run-chain audit of §6, a verified chain is composed
into one immutable file and replayed by the unchanged pipeline (simpler, reuses every verified check). The
real-network pilot (§9 below) still needs the owner's explicit go-ahead.

## 1. Goal and non-goals

Bring the public Binance spot `aggTrade` stream into the existing Core pipeline (journal → outbox → independent
consumer → archive-aware audit) without weakening any v0.3 invariant: immutable source evidence, per-lane
isolation, gaps fail closed, `COMPLETE` only from per-event evidence, no latency claim without a clock model
(ADR-008). Read-only market data only: no API keys, no private endpoints, no orders.

Non-goals: order books (L2/L3), other venues, 24×7 operation certification, trading signals.

## 2. Facts from the current code that shape the design

1. A lane's **source is an immutable file**: `lane_registry.json` pins its SHA-256 at registration;
   `replay`, `archive-producer` and `audit-e2e-archive` all re-check it. A growing file cannot be a source.
2. `seq` **is the Binance aggregate trade ID** (`source_seq`), and the lane is
   `[source, symbol, venue, channel]`. Two producer runs that contain the same event produce the same consumer
   identity; the consumer de-duplicates across runs (v0.3 restart test: 554,441 duplicates, 0 inserts).
3. A new producer run starts its lane at the first observed `seq` (`durable_stream`, `lanes.next_seq`).
4. `audit-e2e-archive` audits **one** run against a consumer; consumer effects without producer evidence in
   that run are `orphan_consumer_effects` → `INCONSISTENT`. A consumer fed by several runs cannot pass a
   single-run audit today.
5. S07-005 (frozen, outside Core) proves the venue protocol pieces: WS first, then REST `fromId` catch-up;
   compare the 8 trade fields (WS-only `E` excluded); identical duplicate → ignore, different content →
   `HALTED_CONFLICT`; REST empty/partial never authorises skipping; HTTP 429/418 stop with `Retry-After`. It
   needs the third-party `websocket-client`; Core is stdlib-only.
6. Task 003 contract: reads timestamped with `perf_counter_ns` in a reader that does not persist inline, queue
   depth at read, offset samples against the venue's server time.

## 3. Options for the source of a live lane

| | A. Sealed capture sessions (recommended) | B. Journal is the source | C. Growing source with rolling fingerprint |
|---|---|---|---|
| Idea | Capture writes raw frames to an append-only session file; the session is **sealed** (fsync, SHA-256, manifest) after N frames / T seconds / a stop; each sealed session is replayed by the unchanged pipeline | Live reader writes straight into the producer journal; provenance in a side table | Registry stores a hash chain over blocks; fingerprint checks become "prefix unchanged" |
| Source evidence | an immutable file per session, like a historical file | none outside the producer DB (journal self-consistent ≠ independent) | the growing file |
| Changes to replay / prune / audit | none for one session; a **run-chain audit** for several sessions (§6) | new audit mode without source anchoring | every source check changes |
| Risk | latency of sealing (seconds) before publication | weaker evidence; producer DB is a single trust domain | broad, subtle changes to verified code |

**Decision (proposed): A.** It keeps every verified v0.3 path unchanged, keeps an independent raw-frame
source, and turns the live problem into "produce correct sealed sessions" plus a run-chain audit.

## 4. Components (all stdlib)

| Module | Responsibility |
|---|---|
| `ftdi_core/live/wsclient.py` | RFC 6455 client over `ssl`: handshake with `Sec-WebSocket-Accept` check, text frames only, fragmentation, server ping → pong, close handshake, frame and message size limits, read timeout. Fixed host allow-list (`data-stream.binance.vision`) unless `--venue-url` points at 127.0.0.1 (tests). |
| `ftdi_core/live/rest.py` | read-only `GET /api/v3/aggTrades?fromId` and `GET /api/v3/time`; fixed host allow-list; min interval; response size cap; 429/418 → `BackoffRequired(Retry-After)`, never an automatic long sleep |
| `ftdi_core/live/reader.py` | reader thread: `recv()` → `read_wall_ns`, `read_perf_ns`, `queue_depth_at_read` → bounded queue. Queue full → **stop reading and fail closed** (`READER_OVERFLOW`), never drop |
| `ftdi_core/live/session.py` | writer: dequeues, validates and normalises (8 fields), resolves WS/REST duplicates and conflicts, appends canonical raw records to the session file with batched fsync, writes provenance; seals the session |
| `ftdi_core/live/clock.py` | Cristian offset samples (`/api/v3/time`) before, during (every K seconds) and after the session → the Task 003 offset model (`timeprov`) |

## 5. Session format (`FTDI-Live-Session-v1`)

```
<capture>/<lane alias>/session-<UTC start>-<first seq>/
    frames.jsonl           # one canonical Core event per line, ascending seq, no gaps inside a sealed session
    provenance.jsonl       # per line: seq, origin (ws|rest), connection id, read_wall_ns, read_perf_ns,
                           #           queue_depth_at_read, raw frame SHA-256
    clock_samples.jsonl    # offset samples (t0, server_ms, t1, rtt, offset, uncertainty)
    SESSION_MANIFEST.json  # schema, lane scope, first/last seq, count, sha256 of each file, previous session
                           # id and its last seq, stop reason, sealed_at, software version
```

Rules:
* A session is sealed only with **no unresolved gap inside it**. If a gap cannot be recovered from REST within
  the limits, the session is sealed at the last contiguous seq with stop reason `GAP_BLOCKED`, and the next
  session must start exactly at that seq + 1 (recovered via REST) or the chain records an explicit gap.
* Conflicting content for one seq (WS vs REST, or reconnect replay) → `HALTED_CONFLICT`; the session is sealed
  before the conflicting seq and capture stops.
* `frames.jsonl` uses exactly the Core event contract, so `replay --lane ALIAS=frames.jsonl` works unchanged.
* Lane scope: `source=binance_spot_ws_aggtrade` (not merged with the historical `binance_spot_aggtrades` lane;
  a later cross-check tool may compare the two by seq).

## 6. Run-chain audit (`audit-e2e-chain`)

Input: an ordered list of producer runs (one per sealed session, or several sessions per run) and one consumer.
Checks: every run passes the archive-aware per-run checks; session manifests chain (previous id, last seq + 1 =
next first seq, or an explicit recorded gap that makes the chain `INCOMPLETE_GAP` — never `COMPLETE`); the
union of producer evidence equals the consumer's effects for the lane (no orphan, no missing); every effect
matches its event. Implemented as a wrapper around `reconcile_archive_aware` with a consumer-side range filter,
so the single-run auditor is unchanged.

## 7. Failure matrix (excerpt; full matrix in the implementation PR)

| Failure | Behaviour |
|---|---|
| WS disconnect / server shutdown | reconnect with bounded jittered backoff, then REST catch-up from next expected seq before accepting WS frames past it |
| REST empty, partial, non-consecutive page | keep gap; `GAP_BLOCKED` after the wait budget; never skip |
| HTTP 429 / 418 | stop capture, record `Retry-After`, seal what is contiguous |
| Duplicate identical (WS/REST overlap) | ignore, count |
| Same seq, different content | `HALTED_CONFLICT`, seal before it, stop |
| Reader queue full | `READER_OVERFLOW`, stop reading, seal contiguous prefix |
| Oversized / non-JSON / wrong symbol / schema-invalid frame | `FORMAT_ERROR`, stop (evidence kept in provenance) |
| Process killed | unsealed session stays unsealed (never replayed); a recovery command seals its contiguous prefix after verification |
| Local clock step | `WALL_CLOCK_STEP` tags (Task 003); intervals unaffected (perf clock) |

## 8. Test plan (offline, localhost only)

A fake venue (`tests/fakevenue.py`): a stdlib WS server and REST server on 127.0.0.1 driven by a script of
events and faults (drop frames, duplicate, reorder, conflict, disconnect, 429, slow reader, oversized frame,
ping, fragmented message). Tests assert sealed-session content against the script's ground truth per seq, the
chain audit status, and that no gap is ever skipped. Plus WS client unit tests (handshake, masking, framing,
limits). Black-box tests from this contract by an independent agent, then an adversarial review, as for
Tasks 001–002.

## 9. Real-network pilot (requires the owner's explicit go-ahead)

The only step that leaves 127.0.0.1: `python -m ftdi_core live-capture --symbol BTCUSDT --max-seconds 600`
on the owner's machine (public read-only endpoints, no key), followed by replay, delivery, chain audit and the
Task 003 latency report. Until then, the network code paths run only against the fake venue.

## 10. Open points for the owner

* Pilot duration and schedule (the machine must stay on; the HDD vs SSD guidance applies).
* Whether a later cross-check against the next day's official historical ZIP is wanted (strong independent
  evidence that the live capture was complete for its range).
