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
| Disk full / torn sector / power loss / shared disk exhaustion | Fail closed, preserve committed log per OS/storage semantics | **NOT YET TESTED** |

## Interpretation

PASS on historical replay proves committed local source continuity and no unexplained gaps within the *supplied archive's interval*, **not** completeness before/after interval, or venue signing. A direct database effect is not a real external trading effect. Crash simulations are process aborts, not sudden host power loss.
