# FTDI-Core v0.2 release gates

1. Core 62 tests and frozen 45-test compatibility suite pass, including `ResourceWarning` as error.
2. Plain ZIP extracted elsewhere passes the same suites without path dependencies or external network.
3. Uploaded historical archive SHA remains unchanged; Core v1 normalized event contract remains unchanged.
4. First 50k and full-day 554,441 archived historical events deliver to a second process/database and pass **rowwise** reconcilation.
5. Consumer commit with lost publisher ACK returns `RETRY_REQUIRED`; replay yields `COMPLETE` with no duplicate consumer effect.
6. Bad consumer payload, forged producer ACK, or orphan consumer effect cannot return `COMPLETE`.
7. Incomplete lanes, missing gaps, partial delivery, and unsupported archive prefixes do not pass falsely.
8. No market data or live credentials in distribution ZIP. Package manifest lists per-file SHA-256.

**Blocked production gates:** authenticated remote delivery, truthful synchronized timestamps, continuous live WS + REST behavior, archived-prefix audit, long-term effect-retention strategy, database backup/restore and disk-power-loss tests, Windows-native crash tests, L2/L3 market data semantics.
