# FTDI-Core v0.1 Test and Historical Data Report

Date: 2026-11-06. All executions were inside this Linux container. No Windows-native tests were executed in this release build.

## Automated tests

- New Core tests: **47/47 PASS** using `-W error::ResourceWarning`.
- Frozen S07-004.1 compatibility suite: **45/45 PASS** using `-W error::ResourceWarning`.
- Combined: **92/92 PASS**. These tests cover different codepaths and should not be interpreted as a statistical failure probability.

## Real user-provided archive

- ZIP SHA-256: `78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9`; matches separately provided companion checksum text.
- First 50,000 source ID gaps: `0`; source ACK/outbox: `50000/50000`; pending: `0`; audit: `True`.
- 50,000 normalized JSONL SHA-256: `e1a8b765cf0ad74f36774303fc4fa140f8452dfa8963fff38afa277ad09fc013`.
- Full day records: `554441`; source ID range `3023902824..3024457264`; gaps: `0`.
- Full-day Core source ACK/outbox: `554441/554441`; pending: `0`; audit: `True`.
- Full-day normalized JSONL SHA-256: `1f157174bc3555276e596074c13e34b6fb11b8fa8f557654a3c88ec2c56fdb0a`.
- Rerun on same full-day DB: outbox and raw file ACK **unchanged**, not re-ingested.

## Independent consumer/ack boundary

- This Core run did **not** start the independent HTTP consumer.
- Full-day producer outbox records not independently consumer-ACKed: **554441**.
- A committed outbox is not proof that a remote consumer or trading system applied any effect. The prior S07-004.1 cross-process consumer contract remains preserved in versioned adapter tests.

## Explicit normalization revision

The raw source CSV has eight fields. The old S07-004.1 converter used seven; Core v0.1 additionally preserves `is_best_match`, so the derivative JSONL hash **intentionally differs** from the old normalized source hash. Do not substitute one normalized source for another in an existing SQLite journal. Original source ZIP checksum is unchanged.

## Unverified release gates

No 24-hour live WS sessions, real reconnect under packet loss, trusted P99 network latency, true machine power-loss, cross-host external exactly-once, or actual Windows/NTFS file-handle testing. **This is a local-development architecture baseline only.**
