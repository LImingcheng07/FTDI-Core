# FTDI-Core v1 event/data dictionary

## Scope

`lane = canonical_json([source,symbol,venue,channel])`, exact order and case. The historical converter emits `source=binance_spot_aggtrades`, `symbol=BTCUSDT`, `venue=BINANCE_SPOT`, `channel=aggTrades`. Never merge a spot aggTrade sequence with order book or different venue.

## JSONL market record (contract for the historical converter)

| Field | Type | Semantics |
|---|---|---|
| `source` | string | Protocol-specific source family identifier |
| `symbol` | string | E.g. BTCUSDT, case-preserving scope |
| `venue` | string | `BINANCE_SPOT` for this converter |
| `channel` | string | `aggTrades` |
| `source_seq` | nonnegative integer | Binance aggregate trade ID for this source, **not a book update ID** |
| `event_time_ms` | nonnegative integer | Native `T`, normalized to milliseconds, **not receive time** |
| `event_time_native` | integer | Original timestamp from archive CSV |
| `event_time_native_unit` | `ms` or `us` | Inferred from numeric magnitude under this *historical ZIP* format; must be validated on other sources |
| `price` | decimal string | Source text retained, finite and positive |
| `qty` | decimal string | Source text retained, finite and positive |
| `first_trade_id` | integer | First individual trade ID in agg trade |
| `last_trade_id` | integer | Last individual trade ID; >= first |
| `is_buyer_maker` | boolean | Seventh CSV field |
| `is_best_match` | boolean, optional | Eighth archive CSV boolean, **preserved in Core v0.1**; old S07-004.1 converter silently ignored it |

The original ZIP has eight CSV fields per row and no header. S07-004.1 conversion used the first seven fields, so its normalized JSONL checksum does **not** match Core v0.1. This is an explicit schema-version change. The original ZIP SHA-256 remains comparable.

## Canonical hashing

`canonical_json = json.dumps(event,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)`.
`payload_sha256 = SHA256(canonical_json UTF-8)`.
S07-004.1 effect id is `SHA256(canonical_json([FTDI-S07-004.1-envelope-v1,lane,seq,epoch,payload_sha256]))` with epoch `0`. This **legacy identity version is frozen** until an independently tested migration. Never regenerate old consumer IDs with a new version without an explicit migration ledger.

## Separated status axes

- `record_status`: `VALID`, `QUARANTINED_INVALID`, or `QUARANTINED_CONFLICT` (Core contract; S07 adapter internally uses `disposition`).
- `stream_integrity`: `UNVERIFIED_PREFIX`, `CONTIGUOUS`, `WAITING_FOR_MISSING`, `CONFLICT` (Core intended states; S07-004.1 maintains its own lane state `CONTIGUOUS`, `WAITING_FOR_MISSING`, `CONFLICT`).
- `feature_usable`: **false** unless record valid, stream contiguous **and** source-prefix authoritative verification exists. The prefix authority is not available for live S07-005 200-event observation. Thus this project does not assert all records are trading-feature safe.

## Source manifest

`FTDI-Core-SourceManifest-v1` keeps `archive_sha256`, companion-checksum indication, `jsonl_sha256`, `canonical_events_sha256`, normalization version, record count, source sequence span, and explicit count of sequence gaps. A manifest does not claim a trading venue certified the archive.

## Clock / latency evidence

Keep `exchange_event_ms`, `local_receive_wall_ns`, `local_receive_monotonic_ns`, `db_commit_wall_ns`, clock offset estimate/error bound in separate evidence. `TimestampEvidence.has_trusted_one_way_delay` deliberately remains false in v0.1. Negative `wall - E` is clock evidence, not a physically negative network latency.
