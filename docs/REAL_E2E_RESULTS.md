# FTDI-Core v0.2 real-data delivery evidence

**Executed 2026-11-06** in local Linux test container. Input derived from user-uploaded Binance BTCUSDT 2024-06-01 archive. **No live venue network connection is inferred**. The source was already normalized and replayed by v0.1; v0.2 reuses that verified, unchanged-protocol outbox to test the NEW independent consumer chain. The original v0.1 databases were copied; they were **not mutated**.

## Provenance

- Original ZIP SHA-256: `78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9`.
- Core v0.1 historical converter preserved the eighth original CSV field and stored separate source/JSONL hashes; no old seven-field hash is silently reused.
- Core v0.1 first historical run: 50,000 distinct contiguous source events, outbox=50,000, initial consumer ACK=0.
- Core v0.1 full-day historical run: 554,441 distinct contiguous source events, outbox=554,441, initial consumer ACK=0.

## Actual v0.2 delivery and read-only reconciliation

| Metric | First 50,000 | Full day |
|---|---:|---:|
| Source/producer outbox | 50,000 | 554,441 |
| Sent / consumer committed / producer ACKed | 50,000 | 554,441 |
| Consumer effects rowwise matched | 50,000 | 554,441 |
| Remote missing | 0 | 0 |
| Consumer-only orphan effects | 0 | 0 |
| Consumer/producer content conflicts | 0 | 0 |
| Invalid source/consumer records in audit | 0 | 0 |
| Remote committed but producer unacked | 0 | 0 |
| Consumer duplicate insertions | 0 | 0 |
| Batch POSTs | 196 | 1,083 |
| Batch size | 256 | 512 |
| Local delivery wall-clock phase | 4.0383s | 58.9752s |
| End-to-end audit | `COMPLETE` | `COMPLETE` |

The `lab-e2e` runner launched a **separate consumer OS process** bound to localhost and a **different SQLite database**, made actual HTTP requests, terminated the process, and reconciled effects against producer committed outbox rows. Counts are **not** the sole basis: the audit compares event IDs, `(lane,seq,epoch)`, canonical payload hashes, and local ACK ledger rows.

Time figures are **single local-run wall-clock for the publish phase only**. They exclude source data acquisition, original historical replay, independent audit, and remote network costs. Do **not** interpret them as live P50/P99 or trading performance.

## Crash and adversarial tests

The new 15-test E2E suite includes: real subprocess publisher abruptly exiting after consumer commit but before local ACK, consumer process exiting after DB commit before HTTP ACK, partial delivery and resumption, forged producer ACK, altered consumer payload, valid orphan effect, multi-lane independent delivery, blocked lane not declared complete, and refusing to certify archived prefixes with a resident-only audit.

## Boundaries

- No independent live Binance data were captured in this run; this was previously archived market data.
- Localhost HTTP lacks authenticated remote endpoint identity and secure inter-host delivery.
- Exactly-once only applies to inserting effect rows into this consumer's SQLite database. Other side effects are not covered.
- Cross-database snapshots are not atomically consistent during concurrent writers; use an exclusive maintenance window for audit acceptance.
- Archive-aware comparison, long-term dedup retention, actual Windows power-loss tests, and real synchronized live latency remain future gates.
