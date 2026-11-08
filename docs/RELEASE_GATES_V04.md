# FTDI-Core v0.4.0 — release gates and evidence

Release date 2026-11-08. Platform: native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1 (owner's machine). Status:
**released for local research use; not production certified.** All v0.3.x gates (`RELEASE_GATES_V03.md`) still
hold: the historical pipeline, retention and `vacuum-db` are unchanged.

| # | Gate | Evidence | Status |
|---|---|---|---|
| L1 | Capture writes only venue-delivered, contiguous, non-contradicted events; raw evidence complete (received messages never lost, also at the stop and on overflow) | `docs/LIVE_CAPTURE.md` §3; black-box `tests/test_live_capture.py` (26, Codex, from the contract); `tests/test_live_capture_internals.py` (9, incl. a real process kill); regressions `tests/test_live_capture_review.py` (24: L1–L12, M1–M7, N1–N6) | PASS |
| L2 | Gaps never skipped (REST repair or `GAP_BLOCKED`), conflicts halt and are cut before sealing, every network call bounded by the capture deadline, 429/418 stop | same suites; loopback acceptance below | PASS |
| L3 | `verify-session` re-derives every event from raw evidence, read-only, never raises on content; `seal-session` recovers killed captures (also mid-seal) and refuses what would not verify | same suites | PASS |
| L4 | Chain → pipeline: `compose-sessions` (fingerprinted, re-checked) → unchanged `replay` → `lab-e2e` → `audit-e2e-archive` COMPLETE; a longer chain re-composed into a new run gives duplicates only | `tests/test_live_chain.py` (4); loopback acceptance | PASS |
| L5 | Latency only under ADR-008 rules (nearest minimum-RTT sample, exclusion tags, minimum counts), never certified | `tests/test_live_chain.py`; loopback acceptance (ESTIMATED on loopback, P99 withheld for lack of samples) | PASS |
| L6 | Network rule: loopback only unless `--public-network`, which selects exactly the two public market-data hosts; refused before any socket otherwise; no key, no private endpoint, no order | endpoint tests (black-box and review: 18 unusual URL forms) | PASS |
| L7 | Independent review | Codex: black-box suite from the contract; adversarial review 12 findings (L1–L12); verification 7 findings (M1–M7); third pass 6 findings (N1: a transient, consistently forged change of an original during composition — fixed by composing from private verified snapshots; N2/N4 loopback port handling; N3 retained `.part` files in fingerprints; N5 recovery verified on a private copy before writing; N6 cleanup on a failing chain-manifest write). Every regression fails on its pre-fix commit (mutation-checked) | PASS |
| L8 | No regression of the historical pipeline | full-day acceptance `scripts/acceptance_full_day.py` from the release code: @@FULLDAY@@ | PASS |
| L9 | Suites and package | Core 408 (1 skipped by design), frozen S07 45, `-W error::ResourceWarning`, native Windows 11; extracted release ZIP verified (recorded in tag `v0.4.0`) | PASS |

## Live pipeline acceptance on 127.0.0.1 (`scripts/acceptance_live_loopback.py`)

Simulated venue with real-world faults: three chained sessions (2,500 events), a 40-event gap repaired by REST, a
dropped connection with catch-up (99 events by REST), periodic duplicate deliveries.

@@LIVE_TABLE@@

## Not proven

Sustained real-network operation of the v0.4 collector (a real-network pilot is the owner's decision);
venue-to-user latency (the loopback figures measure this machine only); everything listed as not proven in v0.3.
