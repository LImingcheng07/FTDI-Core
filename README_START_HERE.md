# FTDI + Claude Code / Windows development kit

This folder is the **FTDI-Core v0.4.1 source tree** (released 2026-11-08), with `CLAUDE.md`, work orders and Windows scripts. Python 3.10+; Core runtime/tests use the standard library. No market-data archive or API secrets are bundled. **End users: read `docs/USER_GUIDE_zh.md` (Chinese) and `README.md`.**

## Start in PowerShell (in this folder)

1. Check your system, run both suites, optionally create a local Git repository:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Setup-FTDI.ps1
```

2. Open Claude Code in this project directory:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Start-Claude.ps1
```

If you want the setup script to initialize an **offline/local** Git repository and you have Git installed:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Setup-FTDI.ps1 -InitializeGit
```

The scripts do not install Claude Code or Python; they do not push to GitHub, download packages, request API keys, or contact Binance. If Claude Code is not installed, follow its official installation instructions: https://code.claude.com/docs/en/overview and then run `claude` inside the repo. Login is handled by Claude Code itself.

## First message to Claude Code

```text
Read CLAUDE.md, README_START_HERE.md, docs/ARCHITECTURE_V1_2.md,
docs/DATA_CONTRACT_V1.md and docs/RELEASE_GATES_V03.md. Run both test suites
first, then propose the next scoped task with contract, acceptance tests and
risks. Ask before deleting data or compacting real consumer databases.
Respond in Chinese.
```

## Re-run tests later

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Verify-FTDI.ps1
```

Expected for v0.4.1: **424 Core (1 skipped by design) + 45 frozen compatibility = 469 tests**, verified on native Windows 11 / Python 3.11.9 (`docs/RELEASE_GATES_V04.md`). The v0.2 baseline was 65 + 45. Verify the package files with `python scripts\Check-Release-Manifest.py`.

## Known limits

- `FTDI-Core v0.3` is a local research/development prototype. It is not certified for production trading or real-time trading decisions.
- The 554,441-row historical BTCUSDT source ZIP is **not bundled**, so the first setup does not require market data.
- The E2E laboratory uses an independent SQLite consumer process and localhost HTTP. It cannot guarantee exactly-once side effects in unrelated services.
- Performance: keep run directories on an SSD (an HDD made full-day delivery ~6x slower; `docs/PERF_WINDOWS_DELIVERY.md`) and use `--batch 1024` for large historical deliveries.
- `audit-e2e` refuses already-archived prefixes by design; do not interpret that explicit refusal as a broken data-integrity check. Use the read-only `python -m ftdi_core audit-e2e-archive --outdir RUN --consumer-db CONSUMER.sqlite3 [--report REPORT.json]` for archived or compacted runs (`docs/ARCHIVE_E2E_AUDIT.md`).
- Retention (`archive-producer`, `consumer-retention`) is a dry run unless `--apply`; archive files are never deleted. A compacted consumer DB only shrinks after a VACUUM, which no command performs.
- For developer review, share `git diff`, test output, and report JSON instead of copying the entire data archive repeatedly.

See `docs/DEV_WORKFLOW.md` and `tasks/` for the work process.
