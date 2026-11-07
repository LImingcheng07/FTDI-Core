# Archive-aware end-to-end audit (`audit-e2e-archive`) — interface contract

Status: **Task 001 implementation contract, local research use, NOT production certified.**
Module: `ftdi_core/archive_e2e.py`. Report schema: `FTDI-Core-E2E-Archive-Audit-v1`.
Trust decisions: ADR-006 in `docs/ADRS.md`.

## 1. Purpose and compatibility

`audit-e2e` (`ftdi_core.e2e.reconcile`) deliberately returns `INCONSISTENT` for any lane
whose history was pruned into receipt-gated archives. That behavior is **unchanged**.
`audit-e2e-archive` is a separate, explicit mode that verifies an archived prefix
plus the resident suffix of every registered lane against the independent consumer
`effects` database, record by record. A false `COMPLETE` is the failure this mode
exists to prevent.

Not changed: `e2e.py`, `audit.py`, `delivery.py`, `lab-e2e`, `audit-e2e`, the
S07-004.1 adapters, `reference_s07_0041/`, any database schema, any data contract.

## 2. Python API

```python
from ftdi_core.archive_e2e import reconcile_archive_aware, ArchiveLimits

result = reconcile_archive_aware(
    root,                      # run root containing lane_registry.json, lanes/, archives/
    consumer_db,               # independent consumer SQLite (effects table)
    *,
    report_path=None,          # None = write nothing; else write the JSON report atomically here
    allow_relocated_archives=False,
    limits=None,               # None = ArchiveLimits() defaults
) -> dict
```

`ArchiveLimits` (frozen dataclass; every field must be a positive `int`, else `ValueError`):

| Field | Default | Enforced on |
|---|---:|---|
| `max_compressed_bytes` | 32 MiB | size of one `.jsonl.gz` segment file (registered or unreferenced) |
| `max_decompressed_bytes` | 128 MiB | total inflated bytes of one segment |
| `max_line_bytes` | 1 MiB | one archived record line (excluding `\n`) |
| `max_records_per_segment` | 20000 | records in one segment, and manifest `record_count` |
| `max_source_line_bytes` | 1 MiB | one source JSONL line including its newline |
| `max_events_per_lane` | 100000000 | `next_file_index` / segment rows of one lane |
| `max_metadata_bytes` | 4 MiB | `lane_registry.json`, `source_manifest.json` |
| `max_unreferenced_overlap_files` | 16 | unregistered segment files overlapping the archived range |
| `max_archive_dir_entries` | 100000 | entries listed in one `archives/<alias>/` directory |

### Precondition failures (raise; CLI exit code 3; no report written)

- `FileNotFoundError`: run root, `lane_registry.json`, a lane DB `lanes/<alias>/state.sqlite3`,
  the consumer DB, or a registered source file does not exist. The audit never creates them.
- `ValueError`: malformed/oversized registry, invalid alias, consumer DB is (or is the same
  file as) a producer lane DB, invalid `report_path` (see §6), invalid limits.
- `sqlite3.Error`: a database file is not a readable SQLite database.

All other evidence problems are **findings in the report**, never uncaught exceptions
(gzip/zlib/EOF/Unicode/JSON/recursion errors included).

## 3. CLI

```text
python -m ftdi_core audit-e2e-archive --outdir RUN_ROOT --consumer-db CONSUMER.sqlite3
                                      [--report REPORT.json] [--allow-relocated-archives]
```

- JSON result printed to stdout. Without `--report` no file is written anywhere.
- Exit code: `0` = `COMPLETE`; `2` = `PENDING_DELIVERY`, `RETRY_REQUIRED`, `INCONSISTENT`;
  `3` = precondition failure (`FTDI_CORE_FAIL_CLOSED: ...` on stderr).

## 4. Status semantics

| Status | Condition |
|---|---|
| `COMPLETE` | zero fatal findings; snapshot consistency verified; every source line anchored; every published event (archived + resident) matches exactly one consumer effect; consumer has no other effects; all invariants in §7 hold |
| `RETRY_REQUIRED` | no fatal findings; some **resident** event committed by the consumer but not ACKed by the producer |
| `PENDING_DELIVERY` | no fatal findings; some **resident** undelivered event has no consumer effect yet |
| `INCONSISTENT` | any fatal finding (§5) |

Archived events can never be `PENDING`/`RETRY`: an archived event without an exactly
matching consumer effect is `INCONSISTENT`. An archived `QUARANTINED_*` disposition makes
the run `INCONSISTENT` (quarantine moved into an archive is still quarantine).

## 5. Findings

Every finding has a code. Codes not listed as NOTICE are FATAL (fail-closed default).
`issue_counts` gives exact counts per code; `issues` holds at most 50 samples
`{code, severity: FATAL|NOTICE, lane?, ...detail}`.

### NOTICE (reported, never block COMPLETE)
| Code | Meaning |
|---|---|
| `UNREFERENCED_ARCHIVE_FILE` | segment-named file (or `.tmp`) not in the manifest that starts at/after the archive cursor, or that overlaps the archived range with byte-identical records; never used as evidence |
| `UNRECOGNIZED_ARCHIVE_DIR_ENTRY` | other entry in `archives/<alias>/` |
| `ARCHIVE_PATH_RELOCATED_ALLOWED` | stored `archive_path` differs from the canonical in-root path and `allow_relocated_archives=True` |
| `SOURCE_MANIFEST_ABSENT` | no `source_manifest.json` describing this source file (e.g. synthetic sources) |

### FATAL
| Group | Codes |
|---|---|
| Producer structure | `PRODUCER_SCHEMA_UNRECOGNIZED`, `PRODUCER_META_INVALID`, `PRODUCER_LANE_STATE_UNRECOGNIZED` |
| Lane/source state (v0.2 local-audit parity) | `SOURCE_FINGERPRINT_MISMATCH`, `SOURCE_NOT_EXHAUSTED`, `RAW_UNPROCESSED`, `PENDING_GAP_UNRESOLVED`, `LANE_WAITING_FOR_MISSING`, `LANE_CONFLICT`, `RESIDENT_QUARANTINE_PRESENT` |
| Resident journal/outbox | `RESIDENT_JOURNAL_TAMPERED`, `RESIDENT_JOURNAL_RANGE_MISMATCH`, `RAW_ID_ORDER_VIOLATION`, `RESIDENT_DISPOSITION_MISSING`, `PRODUCER_DISPOSITION_UNRECOGNIZED`, `PRODUCER_DISPOSITION_MISMATCH`, `PRODUCER_DISPOSITION_ORPHAN`, `PRODUCER_OUTBOX_TAMPERED`, `PRODUCER_OUTBOX_ORPHAN`, `RESIDENT_RAW_OUTBOX_BINDING_MISMATCH`, `PRODUCER_ACK_LEDGER_INCONSISTENT`, `PRODUCER_RECEIPT_ORPHAN`, `PRODUCER_ACK_WITHOUT_CONSUMER_EFFECT` |
| Published sequence | `PRODUCER_DUPLICATE_IDENTITY`, `PRODUCER_SEQUENCE_OUT_OF_RANGE`, `PRODUCER_SEQUENCE_GAP` |
| Source anchoring/provenance | `SOURCE_ANCHOR_MISMATCH`, `SOURCE_LINE_TOO_LONG`, `SOURCE_OFFSET_MISMATCH`, `SOURCE_MANIFEST_UNRECOGNIZED`, `SOURCE_MANIFEST_MISMATCH` |
| Archive manifest | `ARCHIVE_CURSOR_INVALID`, `ARCHIVE_MANIFEST_MISSING`, `ARCHIVE_MANIFEST_UNRECOGNIZED`, `ARCHIVE_SEGMENT_CONTINUITY`, `ARCHIVE_CURSOR_MISMATCH` |
| Archive paths | `ARCHIVE_PATH_UNRECOGNIZED`, `ARCHIVE_PATH_RELOCATED`, `ARCHIVE_PATH_CONFLICT`, `ARCHIVE_SEGMENT_MISSING`, `ARCHIVE_UNREFERENCED_CONTENT_CONFLICT` |
| Archive bytes | `ARCHIVE_SEGMENT_TOO_LARGE`, `ARCHIVE_SHA256_MISMATCH`, `ARCHIVE_SEGMENT_UNREADABLE`, `ARCHIVE_DECOMPRESSED_TOO_LARGE`, `ARCHIVE_LINE_TOO_LONG`, `ARCHIVE_TOO_MANY_RECORDS` |
| Archive records | `ARCHIVE_RECORD_UNRECOGNIZED`, `ARCHIVE_INDEX_GAP`, `ARCHIVE_RAW_HASH_MISMATCH`, `ARCHIVE_OUTBOX_INVALID`, `ARCHIVE_RAW_OUTBOX_BINDING_MISMATCH`, `ARCHIVED_RECORD_NOT_ACKED`, `ARCHIVE_MANIFEST_COUNT_MISMATCH`, `ARCHIVED_QUARANTINE_PRESENT` |
| Disposition claims | `DISPOSITION_CLAIM_UNVERIFIED` (a `DUPLICATE_IDENTICAL` record, archived or resident, whose raw event is not valid, not in the lane, or not byte-identical to a published event with the same seq) |
| Consumer | `CONSUMER_SCHEMA_UNRECOGNIZED`, `ARCHIVED_ACK_WITHOUT_CONSUMER_EFFECT`, `CONSUMER_ARCHIVE_CONTENT_CONFLICT`, `CONSUMER_PRODUCER_CONTENT_CONFLICT`, `CONSUMER_EFFECT_TAMPERED`, `CONSUMER_DUPLICATE_IDENTITY`, `ORPHAN_CONSUMER_EFFECT` |
| Snapshot/self-check | `CONCURRENT_MODIFICATION_DETECTED`, `SNAPSHOT_UNVERIFIABLE`, `INTERNAL_INVARIANT_VIOLATION` |

Key definitions:
- **Missing segment**: manifest row exists, canonical file absent → `ARCHIVE_SEGMENT_MISSING`.
- **Missing manifest row / broken tiling**: segments must tile `[0, archived_records)` with
  `end = start + record_count` → `ARCHIVE_SEGMENT_CONTINUITY` / `ARCHIVE_CURSOR_MISMATCH`.
- **Missing manifest table**: legal when `meta.archived_records` is absent or `0`
  (never-pruned lane). `archived_records > 0` without `archive_segments` → `ARCHIVE_MANIFEST_MISSING`.
  Segment rows with cursor `0` → `ARCHIVE_CURSOR_MISMATCH`.
- **Record format** (S07-004.1 `retention.export_gzip`, implicit v1): one gzip member, every
  line exactly `canonical(item)` + `\n`, keys exactly `{id, file_index, raw_sha256, payload,
  received_ns, outcome, detail}` plus `outbox` iff `outcome == ORDERED_PUBLISHED`;
  `outbox` keys exactly `{lane, seq, raw_sha256, payload, acknowledged}` and
  `acknowledged is True`. Anything else → `ARCHIVE_RECORD_UNRECOGNIZED`
  (`acknowledged` not `True` → `ARCHIVED_RECORD_NOT_ACKED`).
- **Disposition claims**: every source line's disposition is proven from evidence: `ORDERED_PUBLISHED` by the raw-to-outbox binding and a valid published event; `DUPLICATE_IDENTICAL` by an identical published event with the same seq (resident outbox row, or consumer effect that the consumer pass ties to a published event); any `QUARANTINED_*` is fatal.
- **Source anchoring**: for every `file_index` in `[0, next_file_index)`,
  `SHA256(source line without trailing CR/LF)` must equal the archived or resident
  `raw_sha256` → else `SOURCE_ANCHOR_MISMATCH`.
- **Identity domains are separate**: `raw_journal.id` (producer row id, must strictly increase
  with `file_index` across archive+resident, else `RAW_ID_ORDER_VIOLATION`), `file_index`
  (source line position, contiguous), and market `seq`/`source_seq` (may interleave between
  archive and resident, e.g. archived `[1,2,3,5]` + resident `[4,6,7,8]` is valid; the union
  must be unique and contiguous, ending at `lanes.next_seq - 1`).
- **No cascading findings**: `PRODUCER_SEQUENCE_GAP` is reported only when every archived record was
  read, and the source manifest's sequence span is compared only for a verifiable, contiguous lane; holes
  caused by an unreadable segment are explained by that segment's own fatal finding (the lane is then
  `verifiable: false`, its consumer effects count as `unverifiable_lane_effects`, `first_seq` is null).

## 6. Read-only boundary

- Producer lane DBs and consumer DB: SQLite `mode=ro`, `PRAGMA query_only=ON`, one read
  transaction each. Evidence tables must be plain `CREATE TABLE` objects with exactly the
  expected columns (`pragma_table_xinfo`): views and virtual tables are rejected.
  When a DB is quiescent (no WAL frames, no `-journal`) `immutable=1` is added so SQLite
  touches **no** file. With WAL frames and an existing `-shm`, `mode=ro` is used: SQLite may use
  its `-shm` coordination file, which is not evidence, and `PRAGMA data_version` is compared
  before/after. WAL frames without `-shm` (reading would create it) or a `-journal` (reading
  would roll a hot journal back) are refused with `SNAPSHOT_UNVERIFIABLE`; nothing is created.
- Source, source manifest, registry and archive files: opened `rb` only.
- Never called: `retention.connect_db`, `retention.audit`, `retention.prune`,
  `ftdi_core.audit.audit` (`StreamDB` opens read-write).
- The only write is the report, written atomically (temp file + `os.replace`) and only when
  `report_path` is given. `report_path` must have an existing parent directory, must not be a
  directory, must not be inside `<root>/lanes`, `<root>/archives` or any lane archive/DB
  directory, compared both literally and after resolving links/junctions, and must not equal
  any evidence file (lane DBs/consumer DB and their `-wal`/`-shm`/`-journal`, registry,
  sources, source manifests), literally or resolved → else `ValueError`.
- Precondition: run in a maintenance window (no publisher, consumer, replay or prune running).

## 7. Snapshot consistency and COMPLETE invariants

Before opening anything and after closing everything the auditor fingerprints all evidence:
SQLite main/`-wal`/`-journal` files (type, size, mtime_ns, inode, SHA-256), registry bytes,
source and source-manifest files (stat), and the listings of `archives/` and
`archives/<alias>/` (names + stat). Any difference → `CONCURRENT_MODIFICATION_DETECTED`;
any fingerprint that cannot be taken → `SNAPSHOT_UNVERIFIABLE`. Source bytes are also
re-hashed while being anchored, and archive bytes are hashed from the exact in-memory buffer
that is decompressed (no hash-then-reread window). After all handles are closed, every
source, source manifest and archive file the audit relied on is re-hashed by **content** and
compared with the hash verified during the audit (a same-size rewrite with a restored mtime is
therefore still detected). Directory listings never enumerate a link/junction target.

`COMPLETE` additionally requires (`invariants` in the report, all `true`):
`no_fatal_issues`, `snapshot_consistent`, `consumer_schema_recognized`,
`all_lanes_verifiable`, `every_source_line_anchored`, `archived_records_fully_parsed`,
`sequences_contiguous`, `expected_equals_matched`, `consumer_effects_equals_matched`,
`identity_digests_equal` (order-independent multiset digest of `(event_id, payload_sha256)`
computed independently from producer evidence and from consumer rows). If the decision logic
would return `COMPLETE` but an invariant is false → `INTERNAL_INVARIANT_VIOLATION`.

## 8. Archive paths (relocation and conflicts)

Only the canonical in-root path `<root>/archives/<alias>/raw_{start:012d}_{end:012d}.jsonl.gz`
is ever opened. The stored absolute `archive_path` is compared as a string only; it is never
opened, stat'ed or resolved.
- Stored basename differs from the canonical name → `ARCHIVE_PATH_UNRECOGNIZED`.
- Stored path differs from canonical path → `ARCHIVE_PATH_RELOCATED` (FATAL) unless
  `allow_relocated_archives=True` (then NOTICE `ARCHIVE_PATH_RELOCATED_ALLOWED`).
- `ARCHIVE_PATH_CONFLICT` (FATAL): two manifest rows with the same stored path; canonical
  path is a directory/symlink/non-regular file; `archives/` or `archives/<alias>/` resolves
  outside the run root (symlink/junction); a `<registered name>.tmp` sibling exists.
- Unregistered segment-named files overlapping `[0, archived_records)` are parsed under the
  same limits and compared line-by-line with the registered records; any difference or
  unparseable file → `ARCHIVE_UNREFERENCED_CONTENT_CONFLICT`; identical → NOTICE.

## 9. Report (top-level keys)

`schema`, `mode` (`ARCHIVE_AWARE_READ_ONLY`), `status`, `passed`, `metrics`, `lanes`,
`issue_counts`, `fatal_issue_count`, `notice_count`, `issues`, `snapshot_consistency`,
`invariants`, `options`, `trust_model`, `limits_of_claim`, `producer_root`, `consumer_db`.
The report contains no wall-clock timestamps: auditing the same unchanged evidence twice
yields an identical report.

`metrics`: `producer_outbox` (resident outbox rows), `producer_acked`, `archived_records`,
`archived_records_parsed`, `archived_segments`, `archived_segments_parsed`,
`archived_events`, `archived_events_matched`, `archived_quarantined`,
`expected_events_total`, `consumer_effects`, `matched_effects`, `remote_missing`,
`remote_committed_unacked`, `orphan_consumer_effects`, `content_conflicts`,
`invalid_consumer_rows`, `invalid_producer_rows`, `producer_receipt_inconsistencies`,
`archived_missing_consumer_effects`, `duplicate_identities`, `unverifiable_lane_effects`,
`source_lines_anchored`.

`lanes[<alias>]`: `status` (lane-attributable status), `scope`, `verifiable`, `issue_counts`,
`source_lines_expected`, `source_lines_anchored`, `archived_records`,
`archived_records_parsed`, `archived_segments`, `archived_segments_parsed`,
`relocated_segments`, `unreferenced_archive_files`, `resident_raw_records`,
`resident_outbox`, `resident_outbox_delivered`, `published_events`, `first_seq`,
`next_seq`, `sequence_contiguous`, `dispositions` (`{archived: {outcome: n}, resident: {...}}`),
`provenance`, `prefix_note`.

`lanes[<alias>].provenance`: `source_path`, `source_sha256_computed`, `registry_sha256`,
`producer_meta_source_sha256`, `producer_meta_source_filename`, `source_manifest`
(`null`, or declared ZIP `archive_sha256_declared` with
`archive_sha256_verified_by_this_audit: false`, `normalization_version`,
`jsonl_sha256_declared`, `jsonl_sha256_matches_computed`, `records_declared`,
`source_seq_first_declared`, `source_seq_last_declared`, ...).

## 10. Limits of the claim

`COMPLETE` means: within the supplied source interval, the local evidence chain
(source bytes → archived/resident producer records → published events → independent consumer
effects) is complete and consistent, and nothing changed while it was read. It does **not**
mean: venue-certified completeness before the first observed sequence (`UNVERIFIED_PREFIX`),
protection against an actor able to rewrite source, registry, producer DB, archives and
consumer DB together (no signatures/WORM), cross-host exactly-once, or real exchange
availability/latency.
