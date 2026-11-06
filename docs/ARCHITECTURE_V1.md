# FTDI-Core Architecture v1.0 -- Local Development Baseline

Status: **ACCEPTED FOR LOCAL DEVELOPMENT / NOT PRODUCTION CERTIFIED**
Implementation: FTDI-Core v0.1.0, 2026-11-06.

## Scope of this baseline

An audit-first, research-only event ingest and delivery system. Its first executable vertical slice is immutable historical Binance Spot BTCUSDT aggregate trades. It also supports independent synthetic lanes and local cross-process idempotent consumer experiments through the versioned S07-004.1 adapter. It does **not** place trades, require credentials, assert price truth, or claim live 24-hour reliability.

## Component architecture

```text
User-provided original ZIP + companion SHA-256 (immutable)
    |
    v
[Archive verifier + CSV normalizer]
    |      -- separate source ZIP SHA256 / JSONL SHA256 / canonical-event SHA256
    v
[Per-scope JSONL source, immutable]
    |  (source, symbol, venue, channel) -- one physical source per lane
    v
[Per-lane SQLite WAL journal + byte cursor, one atomic transaction]
    |
    v
[Schema+semantic validation, sequence state, bounded pending]
    |  -- no crossing unresolved gap, per-lane fail-closed backpressure
    v
[Durable ordered outbox, semantic content digest]
    |
    +---> [Local state/hash audit]                      IMPLEMENTED in Core CLI
    |
    +---> [At-least-once HTTP publisher to localhost]  S07-004.1 ADAPTER / LAB
                  |
                  v
          [Independent consumer effects DB]
          -- unique event_id AND unique (lane,seq,epoch)
          -- commit before returning ACK
                  |
                  v
          [Producer ACK transaction]
                  |
                  v
          [Receipt-gated gzip archive and audit]        S07-004.1 ADAPTER / LAB
```

The verified S07-005 live collector is **not** silently wired into this first core. It was observed to capture 200 Binance WS events on Windows, but its clock samples included 199/200 negative apparent exchange-local deltas. Integration requires independent clock, reconnect, restart, provenance, and backfill contract checks.

## Hard invariants (MUST)

1. **Immutable provenance**. Preserve the raw supplied archive, compare user-provided checksum when available, and store independent checksums for each normalized derivative. A matching SHA-256 authenticates bytes relative to the asserted checksum, not exchange origin.
2. **Partition by full identity**. Each lane is `(source, symbol, venue, channel)`, a separate immutable source file and SQLite DB. Never alias two distinct lanes onto a shared sequence counter.
3. **No advance-before-commit**. Source byte offset and journal rows advance atomically. A crash before commit replays; a crash after commit resumes without inserting a second copy of already acknowledged raw rows.
4. **Strict semantic event identity**. `sha256(canonical_json(event))` is checked before publishing. Unknown extra source fields are preserved if explicitly declared by the normalization revision; conversion never quietly reinterprets source event time.
5. **No unknown-gap passing**. Sequence IDs above a missing `next_expected_seq` remain in durable pending. Exhausted pending capacity stops advancing that lane's source acknowledgment. Lack of recovery source is never interpreted as successful repair.
6. **Safe independent lanes**. A permanently blocked ETH lane must not prevent a healthy BTC lane from progressing under a shared scheduler. Shared disk/host saturation is **not** fault-domain isolation.
7. **At-least-once delivery, idempotent consumer effects**. A producer may resend after losing HTTP ACK; consumer commits effect then sends ACK. It rejects two payloads for the same `(lane,seq,epoch)`. This is local consumer DB dedup, not global exactly-once for external actions.
8. **Archive only after independent evidence**. Do not prune a raw/outbox prefix without a committed consumer effect/receipt for each published event, archive checksum, durable manifest and crash-safe prune transaction. Retention remains behind the S07-004.1 adapter, not an automatic Core command.
9. **Separate clocks**. Venue event time is not monotonic receive time. Without bounded source-vs-local clock offset, raw `receive_wall - exchange_event_ms` is **not** trustworthy one-way latency.
10. **Fail visibly**. Invalid source, mismatch, conflict, tampering, unsupported epoch or capacity exhaustion result in explicit error or auditable blocked state; never fabricate observations or declare production certification.

## Contracts and physical boundaries

| Boundary | Contract | Implemented now | Missing before production |
|---|---|---|---|
| Archive -> normalized JSONL | File SHA256; explicit normalization revision; 8th source CSV flag preserved; canonical hash | Yes | Broader exchange formats and schema registry |
| Normalized event -> raw journal | Single-lane immutable file; cursor+raw rows in SQLite transaction | Yes | True WebSocket raw-frame precommit |
| Raw journal -> ordered outbox | Payload digest, unique `(lane,seq)`, durable pending, no gap crossing | Yes | Verified automatic epoch reanchor, cross-venue sequence contracts |
| Outbox -> local consumer | HTTP localhost; batch digest, consumer idempotency, atomic local ACK | S07 adapter, tests | Authenticated remote endpoints, retry service, DLQ and metrics |
| Confirmed records -> compressed archive | Independent receipts; gzip checksum; prune/manifest | S07 adapter, tests | Retention policy, backups, remote WORM/object store |
| Event timestamps -> latency | Explicit `TimestampEvidence`; one-way reliability currently false | Contract only | Calibrated synchronized clocks, end-to-end traces, P99 |

## Transactional boundaries

- T1: `(raw_event_batch, file_index, byte_offset)` commit together in **producer lane DB**.
- T2: `(processed status, pending/outbox, disposition, lane next_seq)` commit together in producer DB.
- T3: consumer independently commits `(event_id, lane,seq,epoch,payload_hash,effect)` in **consumer DB**; HTTP ACK occurs only afterward.
- T4: producer independently commits `(delivery receipt, outbox delivered=1)` after verifying ACK digest. Crash between T3 and T4 is a deliberate at-least-once retry window.
- T5: after independent receipt check, verified gzip file is created and hashed, then archive segment manifest and row deletion share a producer DB transaction. File remains outside SQLite transaction; orphan file is preferable to silent data loss.

## Live-only contract (not yet implemented in the Core runtime)

A live adapter must emit: raw WS bytes + read-only transport identity + receive monotonic clock + wall-clock sample + venue `E`/`T` timestamps + source identity; it must preserve first observed aggregate-ID anchor as `UNVERIFIED_PREFIX`, only mark observed contiguous intervals, request missing IDs through bounded REST pages, observe `Retry-After`/429 and prove repair availability, and never treat an empty repair page as a complete interval. It must have crash injection after raw-frame fsync and before publication. This is a **design requirement**, not a description of current Core-v0.1 code.

## Non-goals

- Predictive alpha, strategies, broker/trading execution, exchange credentials.
- Real-time per-message latency SLA or production-grade throughput guarantees.
- Market truth authentication, order-book snapshots, L2/L3 reconciliation.
- Cross-host exactly-once, arbitrary consumer side effects, disaster recovery.

## Architecture approval

The architecture is **frozen as the local-development interface baseline**, subject to recorded ADRs and gate-controlled changes. It is **not** a security review or production launch approval. Any incompatible schema/identity/transaction change requires a new version and migration proof.
