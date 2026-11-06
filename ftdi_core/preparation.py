"""Verified, streaming conversion of an immutable Binance aggTrades CSV ZIP.

A checksum beside the ZIP is independent evidence. Without it, SHA256 only
fingerprints the local bytes and does not establish venue authenticity.
"""
from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
import zipfile

from .contracts import canonical, content_hash, validate_market_event

NORMALIZATION_VERSION = 'FTDI-CSV-AGGTRADES-v1'
MAX_MEMBER_BYTES = 500_000_000


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def read_checksum_file(path: Path, expected_filename: str) -> str:
    parts = Path(path).read_text(encoding='utf-8-sig').split()
    if not parts or not re.fullmatch('[0-9a-fA-F]{64}', parts[0]):
        raise ValueError('Invalid SHA-256 checksum text')
    if len(parts) > 1 and parts[1].lstrip('*') != expected_filename:
        raise ValueError('Checksum filename does not match archive')
    return parts[0].lower()


def parse_csv_row(row: list[str], symbol: str) -> dict:
    if len(row) not in (7, 8):
        raise ValueError(f'aggTrades CSV must have 7 or 8 columns, found {len(row)}')
    try:
        seq = int(row[0])
        price, qty = Decimal(row[1]), Decimal(row[2])
        first, last = int(row[3]), int(row[4])
        native_time = int(row[5])
        maker_str = row[6].strip().lower()
        if maker_str not in ('true', 'false'):
            raise ValueError('Bad is_buyer_maker flag')
        best_str = row[7].strip().lower() if len(row) == 8 else None
        if best_str is not None and best_str not in ('true', 'false'):
            raise ValueError('Bad is_best_match flag')
        if seq < 0 or first < 0 or last < first or native_time <= 0:
            raise ValueError('Invalid source ID, trade range, or timestamp')
        if not price.is_finite() or not qty.is_finite() or price <= 0 or qty <= 0:
            raise ValueError('Bad price or quantity')
    except (TypeError, InvalidOperation, ValueError) as exc:
        raise ValueError(f'Invalid Binance aggTrade row: {row[:1]}') from exc
    unit = 'us' if native_time > 10**14 else 'ms'
    e = {'source':'binance_spot_aggtrades','symbol':symbol,'venue':'BINANCE_SPOT','channel':'aggTrades',
         'source_seq':seq,'event_time_ms':native_time//1000 if unit == 'us' else native_time,
         'event_time_native':native_time,'event_time_native_unit':unit,
         'price':row[1],'qty':row[2], 'first_trade_id':first,'last_trade_id':last,
         'is_buyer_maker':maker_str == 'true'}
    if best_str is not None:
        e['is_best_match'] = (best_str == 'true')
    validate_market_event(e)
    return e


def prepare_archive(archive: Path, outdir: Path, *, checksum: Path | None = None,
                    expected_sha256: str | None = None, symbol='BTCUSDT', limit: int = 50000) -> dict:
    """Create new JSONL/manifest; never replace existing work product.

    limit=0 consumes entire input. No network calls, no in-memory list of rows.
    """
    archive, outdir = Path(archive).resolve(), Path(outdir).resolve()
    if limit < 0 or not re.fullmatch('[A-Z0-9]{2,24}', symbol):
        raise ValueError('Bad record limit or symbol')
    if not archive.is_file() or not zipfile.is_zipfile(archive):
        raise ValueError('Expected a valid ZIP file')
    digest = sha256_file(archive)
    supplied_checksum = read_checksum_file(checksum, archive.name) if checksum else None
    if supplied_checksum and supplied_checksum != digest:
        raise ValueError('ZIP does not match companion checksum')
    if expected_sha256 is not None and digest.lower() != expected_sha256.lower():
        raise ValueError('ZIP does not match expected SHA-256')
    outdir.mkdir(parents=True, exist_ok=True)
    output = outdir / f'{symbol}-aggTrades-{limit or "full"}-core-v1.jsonl'
    manifest_path = outdir / 'source_manifest.json'
    if output.exists() or manifest_path.exists():
        raise FileExistsError('Output/manifest already exists: refuse overwrite; use another outdir')
    with zipfile.ZipFile(archive) as z:
        candidates = [i for i in z.infolist() if i.filename.endswith('.csv') and not i.is_dir()]
        if len(candidates) != 1 or candidates[0].file_size > MAX_MEMBER_BYTES:
            raise ValueError('Expected one bounded CSV entry')
        info = candidates[0]
        if '/' in info.filename or '\\' in info.filename:
            raise ValueError('Nested/unexpected archive member path')
        member_match = re.fullmatch(r'([A-Z0-9]+)-aggTrades-[0-9]{4}-[0-9]{2}-[0-9]{2}\.csv', info.filename)
        if member_match is None or member_match.group(1) != symbol:
            raise ValueError('Archive CSV member filename does not match declared symbol/schema')
        count = 0
        first_id = last_id = previous_id = None
        gaps = 0
        h = hashlib.sha256()
        staged_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', newline='\n',
                                             dir=outdir, prefix='.staged-', suffix='.jsonl',
                                             delete=False) as tmp:
                staged_path = Path(tmp.name)
                with z.open(info) as raw, io.TextIOWrapper(raw, encoding='utf-8-sig', newline='') as f:
                    for row in csv.reader(f):
                        if not row:
                            raise ValueError('Blank CSV row: refusing silent skip')
                        if row[0].strip().lower() in ('a','agg_tradeid','agg_trade_id','aggregate tradeid'):
                            if count != 0:
                                raise ValueError('Repeated CSV header after data start')
                            continue
                        e = parse_csv_row(row, symbol)
                        seq = e['source_seq']
                        if previous_id is not None:
                            if seq <= previous_id:
                                raise ValueError(f'Non-increasing aggregate trade IDs at row {count+1}')
                            gaps += max(0, seq - previous_id - 1)
                        first_id = seq if first_id is None else first_id
                        previous_id, last_id = seq, seq
                        text = json.dumps(e,ensure_ascii=False,separators=(',',':'))+'\n'
                        tmp.write(text)
                        h.update((canonical(e)+'\n').encode('utf-8'))
                        count += 1
                        if limit and count >= limit:
                            break
                tmp.flush()
                os.fsync(tmp.fileno())
            if not count:
                raise ValueError('No data rows found')
            output_digest = sha256_file(staged_path)
            manifest = {'schema':'FTDI-Core-SourceManifest-v1','normalization_version':NORMALIZATION_VERSION,
                        'eighth_csv_boolean_preserved':True,
                        'source_kind':'HISTORICAL_ARCHIVE','archive_name':archive.name,'archive_sha256':digest,
                        'independent_checksum_provided':bool(supplied_checksum or expected_sha256),
                        'archive_member':info.filename,'records':count,'record_limit':limit,
                        'source_seq_first':first_id,'source_seq_last':last_id,
                        'source_seq_gaps':gaps,'contiguous_in_file':gaps==0,
                        'jsonl_filename':output.name,'jsonl_sha256':output_digest,
                        'canonical_events_sha256':h.hexdigest(),
                        'timestamp_note':'Event time is exchange-provided; no wall-clock offset measured.'}
            # Refuse replacement even if another writer races to produce target.
            if output.exists() or manifest_path.exists():
                raise FileExistsError('Output appeared while preparing; refuse replacement')
            os.rename(staged_path, output)
            staged_path = None
            manifest_path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
            return {'output':str(output),'manifest':str(manifest_path),**manifest}
        finally:
            if staged_path is not None:
                staged_path.unlink(missing_ok=True)
