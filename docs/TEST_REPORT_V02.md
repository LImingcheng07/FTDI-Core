# FTDI-Core v0.2 test report

Run the two test suites separately from the project root:

```powershell
python -W error::ResourceWarning -m unittest discover -s tests -q
Push-Location reference_s07_0041
python -W error::ResourceWarning -m unittest discover -s tests -q
Pop-Location
```

- Core tests: **62/62 passed** (47 inherited from v0.1 plus 15 new).
- Frozen S07-004.1 compatibility tests: **45/45 passed**.
- **Total: 107 tests passed** in the local container.
- All tests use local files, SQLite and loopback only; no live exchange API requests.
- Both destructive crash test windows execute in **separate subprocesses** and check resilient idempotent replay.
- Real historical first 50k and full-day 554,441 events passed `audit-e2e` with strict per-event matching.

### Release conditions

The project is allowed as a **local development and offline experiment** base. The source contract remains v1 and legacy S07-004.1 code is retained as a compatibility adapter. No claim of authenticated external delivery, arbitrary side-effect exactly-once, crash-proof disk power-loss guarantees or real-time latency certification is made. Unverified live operations stay outside the main Core data path.
