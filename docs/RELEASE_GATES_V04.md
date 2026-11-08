# FTDI-Core v0.4.0 — release gates and evidence

Release date 2026-11-08. Platform: native Windows 11 Pro, Python 3.11.9, SQLite 3.45.1 (owner's machine). Status:
**released for local research use; not production certified.** All v0.3.x gates (`RELEASE_GATES_V03.md`) still
hold: the historical pipeline, retention and `vacuum-db` are unchanged.

| # | Gate | Evidence | Status |
|---|---|---|---|
| L1 | Capture writes only venue-delivered, contiguous, non-contradicted events; raw evidence complete (received messages never lost, also at the stop and on overflow) | `docs/LIVE_CAPTURE.md` §3; black-box `tests/test_live_capture.py` (26, Codex, from the contract); `tests/test_live_capture_internals.py` (9, incl. a real process kill); regressions `tests/test_live_capture_review.py` (25: L1–L12, M1–M7, N1–N6, O3) | PASS |
| L2 | Gaps never skipped (REST repair or `GAP_BLOCKED`), conflicts halt and are cut before sealing, every network call bounded by the capture deadline, 429/418 stop | same suites; loopback acceptance below | PASS |
| L3 | `verify-session` re-derives every event from raw evidence, read-only, never raises on content; `seal-session` recovers killed captures (also mid-seal) and refuses what would not verify | same suites | PASS |
| L4 | Chain → pipeline: `compose-sessions` (fingerprinted, re-checked) → unchanged `replay` → `lab-e2e` → `audit-e2e-archive` COMPLETE; a longer chain re-composed into a new run gives duplicates only | `tests/test_live_chain.py` (4); loopback acceptance | PASS |
| L5 | Latency only under ADR-008 rules (nearest minimum-RTT sample, exclusion tags, minimum counts), never certified | `tests/test_live_chain.py`; loopback acceptance (ESTIMATED on loopback, P99 withheld for lack of samples) | PASS |
| L6 | Network rule: loopback only unless `--public-network`, which selects exactly the two public market-data hosts; refused before any socket otherwise; no key, no private endpoint, no order | endpoint tests (black-box and review: 18 unusual URL forms) | PASS |
| L7 | Independent review | Codex: black-box suite from the contract; adversarial review 12 findings (L1–L12); verification 7 findings (M1–M7); third pass 6 findings (N1: a transient, consistently forged change of an original during composition — fixed by composing from private verified snapshots; N2/N4 loopback port handling; N3 retained `.part` files in fingerprints; N5 recovery verified on a private copy before writing; N6 cleanup on a failing chain-manifest write); fourth pass: N1–N6 confirmed fixed; remaining findings need a concurrent writer racing a maintenance command (junction or directory inserted mid-run, a session's evidence replaced coherently during the snapshot) and are outside the documented maintenance-window precondition and trust boundary (`docs/LIVE_CAPTURE.md` §8); output-inside-a-session is now refused, the recovery check directory is created with `mkdtemp`, cleanup covers every failure. Every regression fails on its pre-fix commit (mutation-checked) | PASS |
| L8 | No regression of the historical pipeline | full-day acceptance `scripts/acceptance_full_day.py` from the release code: @@FULLDAY@@ | PASS |
| L9 | Suites and package | Core 409 (1 skipped by design), frozen S07 45, `-W error::ResourceWarning`, native Windows 11; extracted release ZIP verified (recorded in tag `v0.4.0`) | PASS |

## Live pipeline acceptance on 127.0.0.1 (`scripts/acceptance_live_loopback.py`)

Simulated venue with real-world faults: three chained sessions (2,500 events), a 40-event gap repaired by REST, a
dropped connection with catch-up (99 events by REST), periodic duplicate deliveries.

| Session | Range | Events | Stop | Chain | WS messages | WS duplicates | REST records | Reconnects | Gaps repaired | Verify |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---|
| `session-20261108T135840Z` | 1..1000 | 1000 | MAX_EVENTS | FIRST | 980 | 19 | 40 | 0 | 1 | VERIFIED |
| `session-20261108T135843Z` | 1001..2000 | 1000 | MAX_EVENTS | CONTIGUOUS | 920 | 18 | 99 | 1 | 1 | VERIFIED |
| `session-20261108T135847Z` | 2001..2500 | 500 | MAX_EVENTS | CONTIGUOUS | 510 | 9 | 0 | 0 | 0 | VERIFIED |

| Check | Result |
|---|---|
| session 1: gap repaired by REST, VERIFIED | PASS |
| session 2: reconnect, catch-up, chain CONTIGUOUS, VERIFIED | PASS |
| events 1..2000 equal the venue ground truth exactly (gap and reconnect range included) | PASS |
| chain 1 composed 1..2000 | PASS |
| replay of chain 1 complete | PASS |
| lab-e2e (separate consumer process) passes | PASS |
| archive-aware audit COMPLETE | PASS |
| session 3 VERIFIED | PASS |
| re-composed chain: 2000 duplicates, 500 new effects | PASS |
| archive-aware audit of the longer chain COMPLETE | PASS |
| latency evidence follows ADR-008 (never certified) | PASS |

Latency evidence of session 1 (loopback, this machine only): status ESTIMATED, 536 trusted of 960 samples, excluded tags {'QUEUED_READ': 424}, P50 0.226 ms, P95 0.618 ms, P99: insufficient samples (need 1000 trusted); 40 events recovered by REST only (no read time). ftdi-core 0.4.0, Python 3.11.9, SQLite 3.45.1, 2026-11-08 21:58:40 → 2026-11-08 21:58:50.

Evidence: `examples/V040_LIVE_LOOPBACK_EVIDENCE.json` (commit `5b6b7f9`).

## Not proven

Sustained real-network operation of the v0.4 collector (a real-network pilot is the owner's decision);
venue-to-user latency (the loopback figures measure this machine only); everything listed as not proven in v0.3.
