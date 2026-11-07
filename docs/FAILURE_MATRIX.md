# Failure matrix, safety expectations and evidence

| Fault / boundary | Required safe behavior | Verification |
|---|---|---|
| Input ZIP checksum mismatch | Reject before creating official normalized output | Core new test |
| ZIP contains malformed/duplicate agg trade ID | Reject; never skip malformed data | Core new tests |
| Historical CSV has eighth field | Preserve `is_best_match` with explicit v1 normalization | Core new test + real 50k replay |
| Source file modified after replay started | Refuse registry reuse; no progress cursor mutation | Core integration test |
| Missing early ID fills lane pending | Stop that lane ACK; other lane progresses | Core integration + 45 compatibility tests |
| Duplicate same aggregate ID / same content | Dedup in lane/consumer store | Inherited 45 tests + Core integration |
| Same `(lane, seq, epoch)` / different content | Quarantine/conflict; consumer rejects | Core integration + inherited tests |
| Raw journal insert crash before COMMIT | Rollback and re-read from source byte cursor | Inherited S07-004.1 tests |
| Crash after journal COMMIT | Resume from committed offset, no extra raw rows | Inherited S07-004.1 tests |
| Consumer effect COMMIT then HTTP ACK lost | Re-send, one local consumer effect | Inherited tests + Core idempotency test |
| Producer HTTP ACK received, local ACK lost | Resend, one consumer effect | Inherited tests |
| Archive file present, DB prune not COMMIT | Archive orphan may exist; original DB rows retained | Inherited tests |
| Prune transaction committed, caller died | Archived segment retained and auditable | Inherited tests |
| Raw/outbox tampering | Checksum audit fails | Core audit tests |
| No external receipts | No destructive retention | Inherited tests, Core CLI disabled |
| Clock offset unknown | No one-way latency SLA claimed | Contract test |
| Live WS disconnect/network DNS failure | Must not synthesize market data or claim repair | S07-005 prototype only; **not** Core production gate |
| Windows SQLite lock | Explicit closing on failure; native Windows QA needed | Inherited regression + strict warning tests on Linux |
| Archived quarantine hidden by prune (local audit flips FAIL->PASS) | `audit-e2e-archive` re-reads archived dispositions: `ARCHIVED_QUARANTINE_PRESENT`, INCONSISTENT | Core archive tests |
| Archive segment missing / truncated / bit-flipped / garbage / extra gzip member | Fail closed with specific code, never COMPLETE | Core archive tests |
| Self-consistent archive forgery (archive rewritten, manifest SHA updated) | Source-line anchoring and raw->outbox binding detect it | Core archive tests |
| Archive copied/moved to another root | Only canonical in-root files read; stored path never opened; relocation requires explicit flag | Core archive tests |
| Evidence changes while the audit runs | Pre/post fingerprints + `data_version`: `CONCURRENT_MODIFICATION_DETECTED` | Core archive internals tests |
| Hostile archive sizes (decompression bomb, huge lines, deep JSON) | Bounded memory, explicit finding, no crash | Core archive tests |
| Quarantined or delivered record relabelled `DUPLICATE_IDENTICAL` | Duplicate claim proven against an identical published event; else `DISPOSITION_CLAIM_UNVERIFIED` | Review regression C1 |
| View/virtual table substituted for an evidence table | Rejected as unrecognized schema before any query (no hang) | Review regression C4 |
| Junction/symlink under `archives/` | Never enumerated or written through; report path vetted after resolution | Review regressions C3, C7 |
| Replay of a compacted consumer identity (producer restored or restarted) | Answered from the digest tier: identical -> duplicate, different -> 409, digest missing -> 503 and no ACK | Consumer retention tests + full-day restart replay |
| Old/frozen consumer writes into a compacted range | `effects_guard_compacted` trigger aborts the insert; no second effect | Consumer retention tests + full-day legacy check |
| Crash during consumer compaction | before COMMIT: rows kept, orphan segment reported, retry reuses identical bytes; after COMMIT: consistent | Subprocess crash tests |
| Consumer segment/digest/chain tampering | `audit-e2e-archive` CONSUMER_* findings, INCONSISTENT | Consumer retention tests |
| Disk full / torn sector / power loss / shared disk exhaustion | Fail closed, preserve committed log per OS/storage semantics | **NOT YET TESTED** |

## Interpretation

PASS on historical replay proves committed local source continuity and no unexplained gaps within the *supplied archive's interval*, **not** completeness before/after interval, or venue signing. A direct database effect is not a real external trading effect. Crash simulations are process aborts, not sudden host power loss.
