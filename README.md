> Windows readiness hotfix: the consumer now publishes its port file atomically,
> and the parent waits for a valid port rather than merely checking file existence.
> This does not modify the FTDI event contract, SQLite schema, or recovery logic.
> See `WINDOWS_READY_FIX.md` for application and test instructions.

# FTDI-Core v0.2 — Local development baseline + independent delivery gate

**Status:** local research/development prototype; **not** production certified and **not** a trading program. No API keys or trading operations. Python 3.10+ (stdlib only).

This release **keeps FTDI-Core v0.1's per-lane journal/transaction contracts and the frozen S07-004.1 adapter** and adds a simple *two-database end-to-end workflow* in the official Core command line. It does not silently integrate the still-uncertified S07-005 live collector.

## New in v0.2

- `python -m ftdi_core lab-e2e`: start an independent **localhost-only consumer process**, send pending outbox batches via HTTP, commit consumer effects **before** producer ACK, stop the process, and perform an independent row-by-row audit.
- `python -m ftdi_core audit-e2e`: read-only comparison of **every resident producer outbox event** against an independent consumer SQLite effects database. The consumer count alone is **not** an acceptance criterion.
- Explicit audit outcomes: `COMPLETE`, `PENDING_DELIVERY`, `RETRY_REQUIRED` (consumer committed, producer confirmation lost), `INCONSISTENT` (corruption or conflicting evidence).
- 14 new tests for cross-process crash windows, resume, multi-lane behavior, tampering, missing receipts, and data consistency. Original v0.1 tests and frozen S07 compatibility tests remain in the package.

## Windows quick start

Open PowerShell **inside** the extracted `FTDI-Core-v0.2` directory:

```powershell
python -W error::ResourceWarning -m unittest discover -s tests -v
Push-Location reference_s07_0041
python -W error::ResourceWarning -m unittest discover -s tests -v
Pop-Location
```

### Using an original verified Binance ZIP (historical data)

```powershell
python -m ftdi_core prepare --archive "C:\data\BTCUSDT-aggTrades-2024-06-01.zip" --checksum "C:\data\BTCUSDT-aggTrades-2024-06-01.zip.zip" --outdir data\btc_50k --limit 50000
python -m ftdi_core replay --lane "BTC=data\btc_50k\BTCUSDT-aggTrades-50000-core-v1.jsonl" --outdir runs\btc
python -m ftdi_core audit --outdir runs\btc
```

### Close the loop with a second process and a second database

```powershell
python -m ftdi_core lab-e2e --outdir runs\btc --consumer-db runs\consumer_effects.sqlite3 --batch 256
python -m ftdi_core audit-e2e --outdir runs\btc --consumer-db runs\consumer_effects.sqlite3
```

`lab-e2e` launches a consumer subprocess bound to `127.0.0.1` on an OS-assigned port. It **only** sends localhost HTTP, never sends data to Binance or any external server. Both producer and consumer SQLite files survive command exit; **rerun the same command with the same paths to resume** after interruption. Do not delete the consumer database between retries.

`--max-batches 1` limits one run to a single batch, returning exit code `2` until delivery completes. Codes: **0** = complete/passed, **2** = explicitly incomplete/blocked, **3** = fail-closed input/runtime error.

### Audit-only and offline failure evidence

Reports are stored under the producer `--outdir`:

- `INGEST_REPORT.json`: source checkpoint and lane counts
- `AUDIT_REPORT.json`: producer journal/outbox integrity
- `E2E_LAB_REPORT.json`: publisher and reconciler results
- `END_TO_END_AUDIT.json`: full delivery comparison, including mismatch counters and bounded issue samples

Consumer state is in the separate path supplied to `--consumer-db`. The `reference_s07_0041/` directory retains the independent failure-injection tests, consumer service and archive tools for explicitly scoped maintenance experiments. The new Core audit does not prune or archive data automatically.

## Evidence from uploaded historical BTCUSDT data

Uploaded original archive checksum: `78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9`.

Version v0.1 already prepared/replayed the archive while preserving the eighth CSV field, with full-day **554,441** source records and local audit pass. Version v0.2 takes the verified v0.1 SQLite outbox as an **immutable-contract-compatible source for new external delivery tests**, leaving v0.1 untouched.

See [v0.2 real-data evidence](docs/REAL_E2E_RESULTS.md). All times are local test host process times; **not** exchange-to-user network latency.

## Architecture, gates and limitations

- [Architecture v1.0 baseline](docs/ARCHITECTURE_V1.md) (inherited unchanged)
- [v0.2 architecture and transaction gates](docs/ARCHITECTURE_V1_1.md)
- [v0.2 test report](docs/TEST_REPORT_V02.md)
- [Event contract v1](docs/DATA_CONTRACT_V1.md)
- [Prior architectural decisions](docs/ADRS.md)

**Not proven:** externally hosted delivery authentication, arbitrary third-party side-effect exactly-once, full-host power failures, Windows-specific crash behavior, trusted live latency P99, 24-hour network uptime, L2/L3 book reconstruction, or archive-aware cross-database audit of already pruned prefixes.

To avoid a false pass, v0.2's `audit-e2e` explicitly rejects archived producer prefixes until an archive-aware reconciler is developed. It also assumes a **quiescent maintenance window** for strong comparisons because two independent SQLite snapshots are not an atomic distributed snapshot.

Source market data are **not included** in the release ZIP.
