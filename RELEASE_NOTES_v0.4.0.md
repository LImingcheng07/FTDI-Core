# FTDI-Core v0.4.0 — release notes

Date: 2026-11-08. Platform verified: native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1. Status: **local research
prototype; not production certified; not a trading program.** Standard library only (the WebSocket client is
built in), read-only public market data, no API key.

## New: live read-only ingestion through sealed capture sessions (Task 004, ADR-011)

```powershell
python -m ftdi_core live-capture --symbol BTCUSDT --capture-dir C:\ftdi\live --public-network --max-seconds 600
python -m ftdi_core verify-session --session C:\ftdi\live\BTCUSDT\session-<UTC>
python -m ftdi_core live-capture --symbol BTCUSDT --capture-dir C:\ftdi\live --public-network --previous-session <last session>
python -m ftdi_core compose-sessions --session <s1> --session <s2> --output C:\ftdi\live_chain1
python -m ftdi_core replay --lane LIVE=C:\ftdi\live_chain1\events.jsonl --outdir C:\ftdi\live_run1
python -m ftdi_core analyze-clock-evidence --session <session>
```

* **Capture** (`live-capture`): own RFC 6455 client (handshake proof, fragmentation, ping/pong, size and frame
  limits) and read-only REST (`aggTrades` by `fromId`, server time). A reader thread stamps each message right after
  the read (`perf_counter_ns`, wall clock, queue depth) into a bounded queue — overflow stops, nothing is dropped.
  Every raw WebSocket message and REST response is written before it is interpreted. Events are sequenced by the
  aggregate trade id: identical duplicates counted, conflicting content halts, gaps are buffered and repaired by
  REST, never skipped; reconnects with backoff and deferred REST catch-up; clock samples for the Task 003 model;
  all network calls bounded by the capture deadline; HTTP 429/418 stops.
* **Sealed session**: `events.jsonl` (Core events, contiguous), `raw_ws.jsonl`, `raw_rest.jsonl`, `clock.jsonl`,
  `CAPTURE_START.json`, `SESSION_MANIFEST.json`. Before sealing all raw evidence is scanned and events are cut
  before any contradicted sequence number.
* **Verification** (`verify-session`, read-only): every event re-derived from raw evidence, contiguity, no
  contradicting raw record, file hashes, start/manifest coherence, chain link to the previous session.
* **Recovery** (`seal-session`): a killed capture (also one killed while sealing) is sealed from its complete
  lines; the `.part` files are kept.
* **Chains** (`compose-sessions`): sessions continue at `previous.last_seq + 1`; a verified chain becomes one
  immutable `events.jsonl` for the unchanged `replay` → `lab-e2e` → `audit-e2e-archive` pipeline. Re-composing a
  longer chain into a new run gives duplicates only for known events and an archive-aware audit `COMPLETE`.
* **Latency evidence** (`analyze-clock-evidence --session`): ADR-008 rules on the first WebSocket delivery of each
  event with the nearest minimum-RTT clock sample; no number without a clock model, nothing certified.
* Network rule: only `ws://127.0.0.1:<port>` / `http://127.0.0.1:<port>` (tests use a simulated venue,
  `tests/fakevenue.py`) unless `--public-network`, which selects exactly `wss://data-stream.binance.vision` and
  `https://data-api.binance.vision`.

## Compatibility

No change to existing schemas, contracts or commands. A new Core lane source `binance_spot_ws_aggtrade` (separate
from the historical `binance_spot_aggtrades`). `timeprov.classify_events` additionally accepts a per-event
estimator (backward compatible).

## Verification

| Suite | Result |
|---|---|
| Core (`python -W error::ResourceWarning -m unittest discover -s tests`) | **408 / 408 OK** (1 skipped by design) |
| Frozen S07 compatibility | **45 / 45 OK** |
| Live pipeline on 127.0.0.1 (`scripts/acceptance_live_loopback.py`) | **LIVE ACCEPTANCE PASS** |
| Independent review (Codex) | black-box suite (26) from the contract; three review rounds: 12 + 7 + 6 findings, all fixed with regressions that fail on their pre-fix commits |

Gates and evidence: `docs/RELEASE_GATES_V04.md`.

## Not proven

Sustained real-network operation: the collector is tested against a simulated venue on 127.0.0.1; a real-network
pilot is the owner's decision. No latency claim is certified.
