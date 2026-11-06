# Task 003 (design only): trustworthy timestamp provenance

Do not blend this with Tasks 001/002.

Goal: define independent event timestamps, local receive monotonic timestamps, local persisted/ack times, and clock-offset uncertainty. Specify why websocket event time minus unsynchronized machine wall-clock is not one-way network latency. Define P50/P95/P99 measurement conditions and failure tags.

The previous 200-message Windows live pilot had 199 negative event-to-local clock differences: treat it as evidence of a clock/timestamp-definition issue, not faster-than-light delivery or a certified latency estimate.

Deliver a contract/ADR and local deterministic tests before any fresh live capture. No exchange credentials or order capability.
