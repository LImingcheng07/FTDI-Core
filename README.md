# FTDI-Core v0.4.3 — auditable local market-data delivery

**Status:** local research prototype; **not** production certified and **not** a trading program. No API keys,
no account or trading operations; delivery over localhost HTTP only; the only outbound connections are the
read-only public market-data hosts of `live-capture --public-network`. Python 3.10+, standard library only.
Verified on Windows 11 and Ubuntu 24.04.

**Chinese user guide (中文使用手册): [`docs/USER_GUIDE_zh.md`](docs/USER_GUIDE_zh.md)** · Release notes:
[`RELEASE_NOTES_v0.4.3.md`](RELEASE_NOTES_v0.4.3.md) (earlier: `RELEASE_NOTES_v0.4.2.md`, `RELEASE_NOTES_v0.4.1.md`, `RELEASE_NOTES_v0.4.0.md`, `RELEASE_NOTES_v0.3.1.md`, `RELEASE_NOTES_v0.3.0.md`) · Gates and evidence: [`docs/RELEASE_GATES_V04.md`](docs/RELEASE_GATES_V04.md), [`docs/RELEASE_GATES_V03.md`](docs/RELEASE_GATES_V03.md)

FTDI-Core turns a verified historical Binance aggTrades ZIP into an immutable event stream, persists it per lane
with transactional checkpoints, delivers it at-least-once to an **independent consumer process** that commits
before acknowledging, and proves per event — not by counts — that nothing was lost, duplicated or altered, even
after both sides have archived or compacted their history.

## Verify the package (PowerShell, inside the extracted `FTDI-Core-v0.4.3` folder)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Verify-FTDI.ps1   # 441 Core + 45 frozen S07 tests + CLI smoke
python scripts\Check-Release-Manifest.py                                          # every file vs release/PACKAGE_MANIFEST.v0.4.3.json
```

## Typical run

```powershell
python -m ftdi_core prepare --archive C:\data\BTCUSDT-aggTrades-2024-06-01.zip --checksum C:\data\BTCUSDT-aggTrades-2024-06-01.zip.CHECKSUM --outdir C:\ftdi\prepared --limit 0
python -m ftdi_core replay --lane "BTC=C:\ftdi\prepared\BTCUSDT-aggTrades-full-core-v1.jsonl" --outdir C:\ftdi\run --batch 1024 --max-pending 4096 --max-backlog 8192
python -m ftdi_core lab-e2e --outdir C:\ftdi\run --consumer-db C:\ftdi\consumer\effects.sqlite3 --batch 1024
python -m ftdi_core audit-e2e-archive --outdir C:\ftdi\run --consumer-db C:\ftdi\consumer\effects.sqlite3
```

Maintenance (every command is a **dry run unless `--apply`**; archive files are never deleted):

```powershell
python -m ftdi_core archive-producer --outdir C:\ftdi\run --lane BTC --consumer-db C:\ftdi\consumer\effects.sqlite3 --limit 500000 [--apply]
python -m ftdi_core plan-consumer-retention --consumer-db C:\ftdi\consumer\effects.sqlite3 --producer-root C:\ftdi\run
python -m ftdi_core consumer-retention migrate|compact|rehydrate --consumer-db ... [--apply]
python -m ftdi_core vacuum-db --db C:\ftdi\consumer\effects.sqlite3 [--apply]
```

One-command full-day acceptance on your own ZIP (read-only input, new work dir, ~4 GB, ~25 min on an SSD, evidence JSON):

```powershell
python scripts\acceptance_full_day.py --archive C:\data\BTCUSDT-aggTrades-2024-06-01.zip --checksum C:\data\BTCUSDT-aggTrades-2024-06-01.zip.CHECKSUM --workdir C:\ftdi_acceptance
```

Keep run data on an SSD (an HDD made delivery ~6× slower, `docs/PERF_WINDOWS_DELIVERY.md`).

## Commands

| Command | Purpose | Changes data? |
|---|---|---|
| `prepare` | verify ZIP SHA-256 against the checksum file, write canonical JSONL (all 8 CSV fields) | new output dir only |
| `replay` | resumable per-lane journal + gap-safe ordered outbox | producer run dir |
| `audit` | producer journal/outbox integrity | writes `AUDIT_REPORT.json` only |
| `lab-e2e` | start a localhost-only consumer process, deliver, ACK, audit (auto archive-aware) | producer + consumer |
| `audit-e2e` | v0.2 resident-only audit; refuses archived prefixes by design | writes `END_TO_END_AUDIT.json` only |
| `audit-e2e-archive` | read-only per-event audit across source, archives, outbox and all consumer tiers | no (optional report) |
| `archive-producer` | archive the acknowledged, independently receipted producer prefix | only with `--apply` |
| `plan-consumer-retention` | what consumer retention could safely compact | no |
| `consumer-retention migrate / compact / rehydrate` | consumer schema v2, compaction, exact restoration | only with `--apply` |
| `vacuum-db` | rebuild one DB file to reclaim space; logical content verified identical; original kept, never deleted | only with `--apply` |
| `live-capture` | read-only public aggTrades into a sealed session (loopback unless `--public-network`) | new session directory |
| `verify-session` / `seal-session` | verify a session from its raw evidence / recover a killed capture | no / only with `--apply` |
| `compose-sessions` | verified chain of sessions -> one immutable `events.jsonl` for replay | new directory |
| `analyze-clock-evidence` | timestamp provenance of a live capture; no latency without a clock model | no (optional report) |

Exit codes: **0** complete/done, **2** explicitly incomplete or inconsistent (see the JSON), **3** fail-closed
precondition error (`FTDI_CORE_FAIL_CLOSED: ...` on stderr).

## Evidence (owner's original full-day ZIP, SHA-256 `78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9`, 554,441 events, native Windows 11)

* Full pipeline incl. both retention tiers, restart replay (554,441 duplicates, 0 new effects), tamper
  detection, legacy-writer block, byte-identical rehydration: **ACCEPTANCE PASS**
  (`examples/V031_ACCEPTANCE_EVIDENCE.json`, v0.3.0: `examples/V030_ACCEPTANCE_EVIDENCE.json`; `docs/RELEASE_GATES_V03.md`).
* Archive-aware audit of 500,000 archived + 54,441 resident events: COMPLETE (`docs/REAL_ARCHIVE_AUDIT_RESULTS.md`).
* Consumer retention: 397 MiB → 61 MiB after VACUUM (on a copy) + 51 MiB verified archive (`docs/REAL_CONSUMER_RETENTION_RESULTS.md`).
* Real-network pilot of the live capture (owner-approved, 2 chained BTCUSDT sessions, 2,480 events, 196 recovered
  by REST): verified, composed, delivered and audited **COMPLETE**; no latency figure (clock evidence too weak; fixed
  in v0.4.1) — `docs/REAL_LIVE_PILOT_RESULTS.md`. Second pilot with v0.4.1 (5,560 events, 229 recovered by REST):
  audit **COMPLETE** without any notice, clock-sample round trips 43–81 ms, no clock-step artefact; still no trusted
  latency (best uncertainty 22.5 ms, limit 20 ms) — `docs/REAL_LIVE_PILOT2_RESULTS.md`. A third capture with a clock
  sample every 5 s (4,683 events, audit COMPLETE) showed a round-trip floor of ≈ 43 ms from this machine: the 20 ms
  limit cannot be met from here, whatever the sampling rate.
* **First trusted latency estimate**, on the owner's Linux cloud server (10,870 events, audit COMPLETE on Linux):
  `ESTIMATED`, 1,147 trusted reads, P50 18.5 ms / P95 19.7 ms / P99 20.0 ms, each ± up to 20 ms; an independent check
  with the NTP-disciplined host clock gives P50 19.5 ms — `docs/REAL_LIVE_REMOTE_RESULTS.md`. Not certified. A
  one-hour capture there in the evening (54,438 events, audit COMPLETE): P50 35.4 ms (independent check
  36.56 ms), stable over the hour; the difference is the network path each connection gets.
* **Path selection** (v0.4.3, `--path-probes`): probe connections opened next to the active one, the faster kept
  without a gap; on the real network two runs switched from ≈ 35 ms to ≈ 19 ms — `docs/REAL_LIVE_REMOTE_RESULTS.md`.

All timings are local process times, **not** exchange-to-user latency. Source market data are not included.

## Documentation

| | |
|---|---|
| Architecture | `docs/ARCHITECTURE_V1.md` → `V1_1.md` → [`V1_2.md`](docs/ARCHITECTURE_V1_2.md) |
| Contracts | `docs/DATA_CONTRACT_V1.md`, `docs/ARCHIVE_E2E_AUDIT.md`, `docs/CONSUMER_RETENTION_V2.md`, `docs/STORAGE_MAINTENANCE.md`, `docs/LIVE_CAPTURE.md`, `docs/TASK003_TIME_PROVENANCE_DESIGN.md` |
| Decisions | `docs/ADRS.md` (ADR-001 … ADR-011) |
| Tests | `docs/TEST_REPORT_V02.md`, `docs/TEST_REPORT_TASK001.md`, `docs/RELEASE_GATES_V03.md` |
| Development | `CLAUDE.md`, `docs/DEV_WORKFLOW.md`, `tasks/` |

## Not proven

Authenticated or cross-host delivery; exactly-once for side effects outside the consumer DB; sustained live
operation (three owner-approved real-network captures of 10–13 minutes; otherwise tested against a simulated
venue on 127.0.0.1), trusted latency or P99; 24-hour uptime; power-loss, backup/restore; signatures or WORM storage for
evidence; L2/L3 book reconstruction. `COMPLETE` covers the supplied local evidence for the observed range, not
venue completeness before the first sequence. Strong audits assume a quiescent maintenance window.
