"""FTDI-Core v0.3 full-day acceptance on a real Binance aggTrades ZIP (read-only input, new work dir).

    python scripts/acceptance_full_day.py --archive C:\\data\\BTCUSDT-aggTrades-2024-06-01.zip ^
        --checksum C:\\data\\BTCUSDT-aggTrades-2024-06-01.zip.zip --workdir C:\\ftdi_acceptance

Put --workdir on an SSD (on a 7200 rpm HDD delivery is ~6x slower; see docs/PERF_WINDOWS_DELIVERY.md).
The ZIP is only read; its SHA-256 is compared before and after. Everything else is created inside the new
work directory (about 3 GB for a full day). Nothing is sent anywhere except 127.0.0.1. Exit 0 = all checks
passed; evidence: <workdir>/acceptance_evidence.json.

Checks: prepare -> replay -> two-process lab-e2e -> producer archive (300k) -> consumer retention v2
compaction (500k) -> producer archive completed with digest receipts (500k) -> audit-e2e-archive COMPLETE ->
restarted producer resends everything (0 new effects) -> tamper and legacy-writer refusals -> rehydration
identical to the original consumer table.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import closing

PROJECT = Path(__file__).resolve().parents[1]
PY = sys.executable
summary = {'steps': [], 'checks': {}}


def sha_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def run(label, args, expect=0, module='ftdi_core'):
    started = time.perf_counter()
    done = subprocess.run([PY, '-m', module, *args], cwd=PROJECT, capture_output=True, text=True)
    step = {'step': label, 'rc': done.returncode, 'wall_s': round(time.perf_counter() - started, 2)}
    try:
        out = json.loads(done.stdout)
    except ValueError:
        out = None
    if isinstance(out, dict):
        for key in ('status', 'pass', 'complete', 'rows', 'audit_mode', 'archived_total', 'archived_new'):
            if key in out:
                step[key] = out[key]
        if 'issue_counts' in out:
            step['issue_counts'] = {k: v for k, v in out['issue_counts'].items() if k != 'SOURCE_MANIFEST_ABSENT'}
        if 'metrics' in out:
            step['metrics'] = {k: v for k, v in out['metrics'].items() if k in (
                'matched_effects', 'consumer_effects', 'consumer_compacted_effects', 'consumer_segments_parsed',
                'archived_events_matched', 'source_lines_anchored', 'archived_records_unsupported')}
        if 'delivery' in out:
            step['delivery'] = {k: out['delivery'][k] for k in ('events_sent', 'acknowledged', 'remote_duplicates',
                                                                'wall_seconds')}
    print(json.dumps(step, ensure_ascii=False), flush=True)
    summary['steps'].append(step)
    if done.returncode != expect:
        step['stderr'] = done.stderr[-1500:]
        fail(f'{label}: exit {done.returncode}, expected {expect}')
    return out


def hot_rows(db_path):
    digest, count = hashlib.sha256(), 0
    with closing(sqlite3.connect(Path(db_path).as_uri() + '?mode=ro', uri=True)) as db:
        for row in db.execute('SELECT event_id, lane, seq, epoch, payload_sha256, payload FROM effects '
                              'ORDER BY lane, seq, epoch'):
            digest.update(json.dumps(list(row), ensure_ascii=False).encode('utf-8') + b'\n')
            count += 1
    return digest.hexdigest(), count


def check(name, ok, detail=None):
    summary['checks'][name] = {'ok': bool(ok), 'detail': detail}
    print(f'[{"PASS" if ok else "FAIL"}] {name}' + (f': {detail}' if detail is not None else ''), flush=True)
    if not ok:
        fail(name)


def fail(reason):
    finish(False, reason)


def finish(ok, reason=None):
    summary['zip_sha256_after'] = sha_file(ARCHIVE)
    summary['zip_unchanged'] = summary['zip_sha256_after'] == summary['zip_sha256_before']
    summary['result'] = 'PASS' if ok and summary['zip_unchanged'] else 'FAIL'
    summary['failure'] = reason
    summary['finished'] = time.strftime('%Y-%m-%d %H:%M:%S')
    (WORK / 'acceptance_evidence.json').write_bytes(
        (json.dumps(summary, indent=2, ensure_ascii=False) + '\n').encode('utf-8'))
    print(f'ACCEPTANCE {summary["result"]}' + (f' ({reason})' if reason else ''), flush=True)
    raise SystemExit(0 if summary['result'] == 'PASS' else 1)


def prune(run_root, consumer, target, archived):
    """Archive the producer up to `target` events, starting from `archived` already archived."""
    while archived < target:
        limit = min(20000, target - archived)
        out = run(f'producer archive (+{limit})', ['--outdir', str(run_root), '--lane', 'BTC', '--mode', 'archive',
                                                   '--limit', str(limit), '--consumer-db', str(consumer)],
                  module='ftdi_core.adapters.s070041.retention')
        if not out.get('archived_new'):
            fail('producer archive made no progress (missing receipts or unacknowledged events)')
        archived = out['archived_total']
    return archived


parser = argparse.ArgumentParser(description='FTDI-Core v0.3 full-day acceptance (read-only input)')
parser.add_argument('--archive', type=Path, required=True)
parser.add_argument('--checksum', type=Path, required=True)
parser.add_argument('--workdir', type=Path, required=True, help='new directory (on an SSD)')
args = parser.parse_args()
ARCHIVE, WORK = args.archive.resolve(), args.workdir.resolve()
if WORK.exists():
    raise SystemExit(f'--workdir must not exist yet: {WORK}')
WORK.mkdir(parents=True)
summary.update(started=time.strftime('%Y-%m-%d %H:%M:%S'), zip_sha256_before=sha_file(ARCHIVE),
               python=sys.version.split()[0], sqlite=sqlite3.sqlite_version)
prepared, run_root, restarted = WORK / 'prepared', WORK / 'run', WORK / 'run_restart'
consumer = WORK / 'consumer' / 'effects.sqlite3'
consumer.parent.mkdir()

out = run('prepare (full day)', ['prepare', '--archive', str(ARCHIVE), '--checksum', str(args.checksum),
                                 '--outdir', str(prepared), '--limit', '0'])
jsonl, records = out['output'], out['records']
summary['source'] = {k: out[k] for k in ('records', 'archive_sha256', 'jsonl_sha256', 'canonical_events_sha256',
                                         'source_seq_first', 'source_seq_last', 'source_seq_gaps')}
replay_args = ['--batch', '1024', '--max-pending', '4096', '--max-backlog', '8192']
run('replay', ['replay', '--lane', f'BTC={jsonl}', '--outdir', str(run_root), *replay_args])
run('lab-e2e (separate consumer process, localhost)', ['lab-e2e', '--outdir', str(run_root), '--consumer-db',
                                                       str(consumer), '--batch', '1024'])
original_digest, original_rows = hot_rows(consumer)
check('all events delivered', original_rows == records, original_rows)

keep_hot = min(54_441, records // 10)
compact_n = records - keep_hot
first_archive = compact_n * 3 // 5
archived_so_far = prune(run_root, consumer, first_archive, 0)
old = run('v0.2 audit-e2e refuses the archived prefix', ['audit-e2e', '--outdir', str(run_root), '--consumer-db',
                                                         str(consumer)], expect=2)
check('v0.2 refusal unchanged', old['metrics']['archived_records_unsupported'] == first_archive)
run('consumer-retention migrate', ['consumer-retention', 'migrate', '--consumer-db', str(consumer), '--apply'])
plan = run('plan-consumer-retention', ['plan-consumer-retention', '--consumer-db', str(consumer),
                                       '--producer-root', str(run_root)])
watermark = plan['lanes'][0]['first_seq'] + compact_n - 1
run('consumer-retention compact (dry run)', ['consumer-retention', 'compact', '--consumer-db', str(consumer),
                                             '--lane', 'BTC', '--producer-root', str(run_root),
                                             '--watermark', str(watermark)])
run('consumer-retention compact --apply', ['consumer-retention', 'compact', '--consumer-db', str(consumer), '--lane',
                                           'BTC', '--producer-root', str(run_root), '--watermark', str(watermark),
                                           '--apply'])
archived_so_far = prune(run_root, consumer, compact_n, archived_so_far)   # rest receipted by consumer digests
check('producer archive stops at the target', archived_so_far == compact_n, archived_so_far)
audit = run('audit-e2e-archive (producer and consumer compacted)', ['audit-e2e-archive', '--outdir', str(run_root),
                                                                    '--consumer-db', str(consumer)])
check('archive-aware audit COMPLETE', audit['status'] == 'COMPLETE' and
      audit['metrics']['consumer_compacted_effects'] == compact_n, audit['metrics'])
compacted_digest, compacted_hot = hot_rows(consumer)

run('replay into a NEW producer run', ['replay', '--lane', f'BTC={jsonl}', '--outdir', str(restarted), *replay_args])
lab = run('restarted producer resends every event', ['lab-e2e', '--outdir', str(restarted), '--consumer-db',
                                                     str(consumer), '--batch', '1024'])
check('restart replay creates no new effect',
      lab['delivery']['remote_duplicates'] == records and hot_rows(consumer)[1] == compacted_hot,
      lab['delivery'])

tamper = WORK / 'tamper'
shutil.copytree(consumer.parent, tamper)
segment = sorted(Path(f'{tamper / consumer.name}.archive').rglob('*.gz'))[0]
data = bytearray(segment.read_bytes())
data[len(data) // 2] ^= 1
segment.write_bytes(bytes(data))
bad = run('tampered consumer segment', ['audit-e2e-archive', '--outdir', str(run_root), '--consumer-db',
                                        str(tamper / consumer.name)], expect=2)
check('tampering detected', 'CONSUMER_SEGMENT_SHA256_MISMATCH' in bad['issue_counts'])

sys.path.insert(0, str(PROJECT))
from ftdi_core.adapters.s070041.external_delivery import EffectStore, envelope  # noqa: E402
legacy = WORK / 'legacy'
shutil.copytree(consumer.parent, legacy)
with closing(sqlite3.connect((restarted / 'lanes' / 'BTC' / 'state.sqlite3').as_uri() + '?mode=ro', uri=True)) as db:
    db.row_factory = sqlite3.Row
    events = [envelope(row) for row in db.execute('SELECT * FROM outbox ORDER BY seq LIMIT 3')]
try:
    with EffectStore(legacy / consumer.name) as store:
        store.accept(events)
    check('legacy writer blocked by trigger', False, 'accepted')
except sqlite3.IntegrityError as exc:
    check('legacy writer blocked by trigger', True, str(exc))

rehydrated = WORK / 'rehydrate'
shutil.copytree(consumer.parent, rehydrated)
run('consumer-retention rehydrate --apply', ['consumer-retention', 'rehydrate', '--consumer-db',
                                             str(rehydrated / consumer.name), '--lane', 'BTC', '--producer-root',
                                             str(run_root), '--apply'])
check('rehydration identical to the original table', hot_rows(rehydrated / consumer.name) == (original_digest,
                                                                                               original_rows))
vacuum = WORK / 'vacuum.sqlite3'
with closing(sqlite3.connect(consumer.as_uri() + '?mode=ro', uri=True)) as src, closing(sqlite3.connect(vacuum)) as dst:
    src.backup(dst)
with closing(sqlite3.connect(vacuum)) as db:
    db.execute('VACUUM')
summary['storage_mib'] = {'consumer_compacted_after_vacuum_copy': round(vacuum.stat().st_size / 2**20, 1),
                          'consumer_archive': round(sum(p.stat().st_size for p in
                                                        Path(f'{consumer}.archive').rglob('*.gz')) / 2**20, 1)}
vacuum.unlink()
finish(True)
