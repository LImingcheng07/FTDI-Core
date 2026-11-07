# FTDI + Claude Code / Windows development kit

This folder is the **actual FTDI-Core v0.2 source tree**, augmented with a `CLAUDE.md`, work orders, and Windows scripts. It is **not** a dummy project or a new v0.3 release. Python 3.10+; Core runtime/tests use the standard library. No market-data archive or API secrets are bundled.

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
Read CLAUDE.md, README_START_HERE.md, docs/ARCHITECTURE_V1_1.md,
docs/DATA_CONTRACT_V1.md, and tasks/001-archive-aware-e2e-audit.md.
Do not change code yet. First run the baseline test suites, inspect the
relevant archive and E2E implementations, then provide a scoped implementation
plan, risk assessment, and acceptance tests. Wait for my approval before
changing database schema, pruning, or deleting data. Respond in Chinese.
```

## Re-run tests later

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Verify-FTDI.ps1
```

Expected Windows-ready patched baseline: **65/65 Core + 45/45 compatibility = 110/110** (verified by the owner on native Windows, 2026-11-06). With Task 001 (branch `feat/archive-e2e-audit`): **154 Core + 45 compatibility**, verified on native Windows 11 / Python 3.11.9 (`docs/TEST_REPORT_TASK001.md`).

## Known limits

- `FTDI-Core v0.2` is a local research/development prototype. It is not certified for production trading or real-time trading decisions.
- The 554,441-row historical BTCUSDT source ZIP is **not bundled**, so the first setup does not require market data.
- The E2E laboratory uses an independent SQLite consumer process and localhost HTTP. It cannot guarantee exactly-once side effects in unrelated services.
- `audit-e2e` refuses already-archived prefixes by design; do not interpret that explicit refusal as a broken data-integrity check. Use the read-only `python -m ftdi_core audit-e2e-archive --outdir RUN --consumer-db CONSUMER.sqlite3 [--report REPORT.json]` for archived runs (`docs/ARCHIVE_E2E_AUDIT.md`).
- For developer review, share `git diff`, test output, and report JSON instead of copying the entire data archive repeatedly.

See `docs/DEV_WORKFLOW.md` and `tasks/` for the work process.
