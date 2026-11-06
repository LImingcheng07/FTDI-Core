# Provenance and source isolation

**Original Binance historical archive**: `BTCUSDT-aggTrades-2024-06-01.zip`, SHA-256 `78085fc50146fcd2a228bb95495246c539bacc5cd635268bc8a641ce90e3c9`.

**Historical record count**: 554,441 BTCUSDT spot aggregate trades for the sampled day in the original archive. The original CSV contains 8 columns; v0.1+ canonicalization retains the previously overlooked eighth field, so the canonical JSONL SHA cannot be compared naively with S07-002's 7-field Windows JSONL fingerprint.

**Do not** include the original archive, `.zip.zip` checksum document, normalized JSONL, or DB files in Git commits or development kit ZIP. The input stays in the user's own ignored `data/` location if used locally.

**Evidence limits**: verified historical data are not live network performance; synthetic fault timings are not venue recovery endpoint proof; outbox publishing is not external consumer acceptance; receipt from an independent SQLite consumer does not certify third-party business effects.
