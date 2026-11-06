# FTDI-Core v0.1 release gates

## Pass requirements for local-development baseline

1. `python -W error::ResourceWarning -m unittest discover -s tests -q` returns 0.
2. From `reference_s07_0041/`, `python -W error::ResourceWarning -m unittest discover -s tests -q` returns 0 (frozen compatibility suite).
3. User-provided ZIP digest matches independent companion text; converter produces explicit 8th field and reports old-normalization hash drift.
4. Historical 50,000 records: source sequence gaps 0; per-lane raw checkpoint 50,000; ordered outbox 50,000; integrity audit PASS; outbox consumer-unacked metric is truthfully 50,000 unless external effect service runs.
5. Repeating replay against same source causes no new outbox rows, changing source or registry rejects reuse.
6. Multi-lane permanent-gap and independent-consumer duplicate cases pass tests.
7. ZIP from release tree can be extracted and run with imports/tests, without implicit network calls.

## Explicitly NOT certified by local-development baseline

- Live WS API reliability or 24-hour service recovery.
- Trusted one-way market latency quantiles.
- Remote authenticated delivery or arbitrary outside-DB exactly-once effects.
- True 2-host fault isolation / power-loss durability / no-loss trading system.
- L2/L3 book reconstruction or cross-symbol sequence semantics.
- Windows-native file handle/resource behavior (until run there).

## Version and acceptance policy

- Contract `FTDI-Core-Event-v1`, manifest `FTDI-Core-SourceManifest-v1`.
- S07 compatibility `FTDI-S07-004.1-envelope-v1` and DB schema unchanged.
- Source adapter `FTDI-CSV-AGGTRADES-v1`; preserving eighth flag causes a documented JSONL hash difference.
- Return codes: `0` complete/audit pass, `2` explicitly incomplete/blocked, `3` fail-closed input/contract error.
