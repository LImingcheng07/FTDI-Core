# FTDI development workflow with Claude Code

**Baseline**: FTDI-Core v0.2. `CLAUDE.md` is authoritative project guidance; data contract and ADRs take precedence over convenience or speed.

1. **Initialize and establish a reproducible baseline:** execute `scripts/Setup-FTDI.ps1`; record Python, Claude Code, Git versions, the two suite counts and any Windows-specific problems.
2. **Use local Git:** `git init -b main` only if requested. Baseline commit after tests pass: `git add .` then `git commit -m "chore: import audited FTDI Core v0.2 baseline"`. Do not create a remote or push without permission.
3. **One task at a time:** create a short-lived branch, e.g. `git switch -c feat/archive-e2e-audit`; provide the task file to Claude Code; ask it to propose tests before implementation.
4. **Review gates:** behavior must fail closed on bad input; new tests must check failure modes; original 62 + 45 suites must stay green. Audit must cross-check producer/consumer identities and payload digests, not just counts.
5. **Data isolation:** use temporary directories and ignored `data/` and `runs/`. Never commit actual original market data, SQLite files, tokens, or machine-specific paths.
6. **Deliver evidence:** collect `git status --short`, `git diff --stat`, test commands/exit codes, crash/retry results, changed data contracts, test fixture hashes, known gaps and rollback plan. Ask for a second review on anything touching persistence, retention, or customer-visible semantics.
7. **Commit and tag only after acceptance:** do not auto-push, and do not mark a release production-ready because localhost or offline tests pass.

**Evidence hierarchy**: unmodified source archive + SHA > exact normalized record content/hash > durable journal/outbox > independent consumer effects + ACK receipts > an aggregate count. Each level answers a different question.

**When asked to change data contract**: write a new ADR and versioned migration plus dual-read or an explicit fail-closed upgrade strategy; do not quietly mutate v1 events.

**When the user wants a result reviewed externally**: provide the relevant diff, exact test output, and generated audit JSON; no need to re-upload the original BTCUSDT ZIP unless the source has changed.
