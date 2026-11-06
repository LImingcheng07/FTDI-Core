# Task 002 (design only): consumer idempotency retention

Do not implement before Task 001 is reviewed and accepted.

Goal: design a bounded-storage idempotency ledger that can still recognize replays across the guaranteed retry window, archive history, and process restarts. Specify invariant-preserving retention watermarks, archive verification, tombstone/checkpoint semantics, failure scenarios and rollbacks.

No automatic deletion, VACUUM, compacting effects, or trust in time-based TTL alone. Produce an ADR, a failure matrix and deterministic tests/specification first; require explicit user approval before destructive code.
