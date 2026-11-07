#!/usr/bin/env python3
"""Verified receipt-gated archival and offline compaction for S07-004.1 lanes.

NEVER prunes unprocessed, pending, or externally unacknowledged outbox records.
Non-regulatory local experiment. Exclusive maintenance window is required.

Low-level tool: direct use (``python -m ftdi_core.adapters.s070041.retention``) applies immediately and does
NOT run the archive-aware audit first. Use ``python -m ftdi_core archive-producer`` (dry run by default,
mandatory audit-e2e-archive gate, record validation, collision prediction).
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from contextlib import closing
from .durable_stream import canonical, sha256_file


def connect_db(root, alias):
    reg=json.loads((root/'lane_registry.json').read_text(encoding='utf-8'))
    if alias not in reg:raise ValueError('Unknown registered lane')
    path=root/'lanes'/alias/'state.sqlite3'
    if not path.is_file():
        raise FileNotFoundError(f'producer state database missing: {path}')
    db=sqlite3.connect(str(path),timeout=30)
    try:
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA busy_timeout=30000')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE INDEX IF NOT EXISTS idx_retention_outbox_raw ON outbox(raw_id)')
        db.execute('CREATE INDEX IF NOT EXISTS idx_retention_delivery_raw ON delivery(raw_id)')
        db.execute('CREATE INDEX IF NOT EXISTS idx_retention_pending_raw ON pending(raw_id)')
        db.execute('''CREATE TABLE IF NOT EXISTS archive_segments (
              start_file_index INTEGER PRIMARY KEY,
              end_file_index INTEGER NOT NULL,
              record_count INTEGER NOT NULL,
              outbox_count INTEGER NOT NULL,
              archive_path TEXT NOT NULL,
              archive_sha256 TEXT NOT NULL
        )''')
        db.commit()
    except BaseException:
        db.close()   # Windows: a leaked handle would keep the lane DB locked
        raise
    return db,reg[alias]


def get_meta(db,key):
    r=db.execute('SELECT v FROM meta WHERE k=?',(key,)).fetchone()
    return r['v'] if r else None


def set_meta(db,k,v):
    db.execute('INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',(k,str(v)))


def collect_archivable(db,start,limit):
    """Find contiguous safe prefix; never cross a raw row missing an ACK."""
    rows=db.execute('''SELECT r.* FROM raw_journal r WHERE file_index>=?
                       ORDER BY file_index LIMIT ?''',(start,limit)).fetchall()
    result=[]
    for r in rows:
        if r['file_index']!=start+len(result):
            break
        if not r['processed']:
            break
        pend=db.execute('SELECT 1 FROM pending WHERE raw_id=?',(r['id'],)).fetchone()
        if pend:break
        dispo=db.execute('SELECT outcome,detail FROM disposition WHERE raw_id=?',(r['id'],)).fetchone()
        if dispo is None:break
        outboxes=db.execute('SELECT * FROM outbox WHERE raw_id=?',(r['id'],)).fetchall()
        deliveries=db.execute('SELECT * FROM delivery WHERE raw_id=?',(r['id'],)).fetchall()
        if len(outboxes)>1 or len(deliveries)>1:
            # Deletion is by raw_id: a second row would be removed without being archived or receipted.
            raise ValueError('Several outbox or delivery rows reference one raw record; refuse archival')
        outbox=outboxes[0] if outboxes else None
        delivery=deliveries[0] if deliveries else None
        if outbox is not None and (type(outbox['delivered']) is not int or outbox['delivered'] not in (0,1)):
            raise ValueError('Invalid outbox delivered flag; refuse archival')
        if outbox is not None and (not outbox['delivered'] or delivery is None):
            break
        if dispo['outcome']=='ORDERED_PUBLISHED' and outbox is None:
            break
        if delivery is not None and outbox is None:
            raise ValueError('Orphan delivered event; refuse archival')
        payload=r['payload']
        if hashlib.sha256(payload.encode('utf-8')).hexdigest()!=r['raw_sha256']:
            raise ValueError('Tampered raw journal; refuse archival')
        item={'id':r['id'],'file_index':r['file_index'],'raw_sha256':r['raw_sha256'],
              'payload':payload,'received_ns':r['received_ns'],
              'outcome':dispo['outcome'],'detail':dispo['detail']}
        if outbox is not None:
            oe=json.loads(outbox['payload'])
            if hashlib.sha256(canonical(oe).encode('utf-8')).hexdigest()!=outbox['raw_sha256']:
                raise ValueError('Tampered outbox payload; refuse archival')
            if (outbox['raw_sha256']!=delivery['raw_sha256'] or outbox['seq']!=delivery['seq'] or
                    outbox['lane']!=delivery['lane']):
                raise ValueError('External ACK evidence mismatch')
            item['outbox']={'lane':outbox['lane'],'seq':outbox['seq'],'raw_sha256':outbox['raw_sha256'],
                            'payload':outbox['payload'],'acknowledged':True}
        result.append(item)
    return result


def export_gzip(path,items):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+'.tmp')
    if tmp.exists():tmp.unlink()
    with open(tmp,'wb') as f:
        with gzip.GzipFile(filename='',mode='wb',fileobj=f,mtime=0,compresslevel=6) as z:
            for item in items:
                z.write((canonical(item)+'\n').encode('utf-8'))
        f.flush()
        os.fsync(f.fileno())
    checksum=sha256_file(tmp)
    if path.exists():
        if sha256_file(path)!=checksum:
            tmp.unlink()
            raise ValueError('Archive segment file collision with different content')
        tmp.unlink()
    else:
        os.replace(tmp,path)
    if sha256_file(path)!=checksum:
        raise ValueError('Archive was not durable or changed prior to prune')
    return checksum


EVIDENCE_TABLES=('raw_journal','outbox','delivery','disposition','pending')


def require_no_triggers(db):
    """Deletes must have no side effects: a trigger could remove rows that are not in the archive."""
    names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name")]
    if names:
        raise ValueError(f'Producer DB contains triggers ({", ".join(names)}); refuse archival')


def table_counts(db):
    return tuple(db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in EVIDENCE_TABLES)


def require_independent(root,consumer_db):
    """The receipt source must be an existing DB that is none of this run's producer lane DBs."""
    if consumer_db is None or not Path(consumer_db).is_file():
        raise ValueError('Verified independent --consumer-db is required before any prune')
    reg=json.loads((Path(root)/'lane_registry.json').read_text(encoding='utf-8'))
    for alias in reg:
        lane_db=Path(root)/'lanes'/alias/'state.sqlite3'
        if lane_db.is_file() and os.path.samefile(lane_db,consumer_db):
            raise ValueError('Consumer effects DB must be independent of every producer lane DB')


def check_receipts(consumer_db,items):
    """A local publisher ACK is insufficient evidence for deletion: every archived published event needs an
    independent consumer receipt. Read-only. Returns (hot_rows, digest_rows); raises on any missing receipt."""
    if consumer_db is None or not Path(consumer_db).is_file():
        raise ValueError('Verified independent --consumer-db is required before any prune')
    read_uri=Path(consumer_db).resolve().as_uri()+'?mode=ro'
    with closing(sqlite3.connect(read_uri,uri=True)) as receipts:
        return count_receipts(receipts,items)


def count_receipts(receipts,items):
    """Receipt rule on an open consumer connection (see check_receipts)."""
    from .external_delivery import event_identity
    hot=digest=0
    # Core divergence from reference_s07_0041 (ADR-007 phase 2): a consumer compacted with retention v2
    # keeps compacted identities as digest rows; an identical digest is an equally independent receipt.
    tiered=receipts.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='effect_digests'").fetchone()
    for item in items:
        if 'outbox' in item:
            o=item['outbox']
            eid=event_identity(o['lane'],o['seq'],0,o['raw_sha256'])
            if receipts.execute('SELECT 1 FROM effects WHERE event_id=?',(eid,)).fetchone():
                hot+=1
                continue
            row=receipts.execute('SELECT d.payload_sha256 FROM effect_digests d JOIN lane_ids l '
                                 'ON l.lane_id=d.lane_id WHERE l.lane=? AND d.epoch=0 AND d.seq=?',
                                 (o['lane'],o['seq'])).fetchone() if tiered else None
            if row is None or not isinstance(row[0],bytes) or row[0].hex()!=o['raw_sha256']:
                raise ValueError('Independent consumer receipt missing; refuse prune')
            digest+=1
    return hot,digest


def prune(root,alias,limit=5000,do_vacuum=False,consumer_db=None,crash_stage=None):
    if limit<1 or limit>20000:raise ValueError('limit must be 1..20000')
    db,reg=connect_db(root,alias)
    try:
        if not Path(reg['source']).is_file() or sha256_file(reg['source'])!=reg['sha256']:
            raise ValueError('Source immutable fingerprint not satisfied; refuse archival')
        start=int(get_meta(db,'archived_records') or 0)
        items=collect_archivable(db,start,limit)
        old_size=(root/'lanes'/alias/'state.sqlite3').stat().st_size
        if not items:
            return {'lane':alias,'archived_new':0,'archived_total':start,'blocked':True,'db_bytes':old_size}
        require_independent(root,consumer_db)
        check_receipts(consumer_db,items)
        end=start+len(items)   # half-open
        archive=root/'archives'/alias/f'raw_{start:012d}_{end:012d}.jsonl.gz'
        checksum=export_gzip(archive,items)
        out_count=sum('outbox' in item for item in items)
        if crash_stage=='after_archive_before_commit':
            os._exit(86)  # archive exists but no DB record has been deleted
        # Last safety check; sqlite BEGIN IMMEDIATE prevents another SQLite writer from
        # changing ACKs while we confirm and delete. Filesystem archive was verified above.
        db.execute('BEGIN IMMEDIATE')
        try:
            if int(get_meta(db,'archived_records') or 0)!=start:
                raise ValueError('Concurrent pruning cursor change; rollback')
            require_no_triggers(db)
            before=table_counts(db)
            for x in items:
                # Every row about to be deleted must equal what the committed segment holds.
                current=db.execute('SELECT * FROM raw_journal WHERE id=?',(x['id'],)).fetchone()
                if (current is None or current['file_index']!=x['file_index'] or not current['processed'] or
                        current['raw_sha256']!=x['raw_sha256'] or current['payload']!=x['payload'] or
                        current['received_ns']!=x['received_ns']):
                    raise ValueError('Rows changed since archive was prepared; rollback')
                dispo=db.execute('SELECT outcome,detail FROM disposition WHERE raw_id=?',(x['id'],)).fetchone()
                if dispo is None or dispo['outcome']!=x['outcome'] or dispo['detail']!=x['detail']:
                    raise ValueError('Rows changed since archive was prepared; rollback')
                if db.execute('SELECT 1 FROM pending WHERE raw_id=?',(x['id'],)).fetchone():
                    raise ValueError('Pending event not prunable')
                outs=db.execute('SELECT * FROM outbox WHERE raw_id=?',(x['id'],)).fetchall()
                dels=db.execute('SELECT * FROM delivery WHERE raw_id=?',(x['id'],)).fetchall()
                if 'outbox' in x:
                    o=x['outbox']
                    if len(outs)!=1 or outs[0]['delivered']!=1:
                        raise ValueError('Unacknowledged event not prunable')
                    if (len(dels)!=1 or outs[0]['payload']!=o['payload'] or
                            any((r['lane'],r['seq'],r['raw_sha256'])!=(o['lane'],o['seq'],o['raw_sha256'])
                                for r in (outs[0],dels[0]))):
                        raise ValueError('ACK evidence changed since archive was prepared; rollback')
                elif outs or dels:
                    raise ValueError('Rows changed since archive was prepared; rollback')
            for x in items:
                raw_id=x['id']
                counts=(db.execute('DELETE FROM delivery WHERE raw_id=?',(raw_id,)).rowcount,
                        db.execute('DELETE FROM outbox WHERE raw_id=?',(raw_id,)).rowcount,
                        db.execute('DELETE FROM disposition WHERE raw_id=?',(raw_id,)).rowcount,
                        db.execute('DELETE FROM raw_journal WHERE id=?',(raw_id,)).rowcount)
                if counts!=((1,1,1,1) if 'outbox' in x else (0,0,1,1)):
                    raise ValueError('Rows deleted differ from the archived evidence; rollback')
            removed=tuple(b-a for b,a in zip(before,table_counts(db)))
            if removed!=(len(items),out_count,out_count,len(items),0):
                raise ValueError('Rows deleted differ from the archived evidence; rollback')
            db.execute('''INSERT INTO archive_segments VALUES(?,?,?,?,?,?)''',
                       (start,end,len(items),out_count,str(archive.resolve()),checksum))
            set_meta(db,'archived_records',end)
            db.commit()
        except BaseException:
            db.rollback()
            raise
        if crash_stage=='after_commit':
            os._exit(86)  # all prune changes and archive manifest are atomic in SQLite
        if do_vacuum:
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            db.execute('VACUUM')
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        return {'lane':alias,'archived_new':len(items),'archived_total':end,'outbox_archived':out_count,
                'archive_file':str(archive),'archive_sha256':checksum,'archive_bytes':archive.stat().st_size,
                'db_bytes_before':old_size,
                'db_bytes_after':(root/'lanes'/alias/'state.sqlite3').stat().st_size}
    finally:
        db.close()


def audit(root,alias,consumer_db=None):
    db,reg=connect_db(root,alias)
    try:
        archive_count=0; archive_output=0; resident_output=0; missing_effects=0
        expected_start=0
        effects=sqlite3.connect(str(consumer_db)) if consumer_db else None
        try:
            for seg in db.execute('SELECT * FROM archive_segments ORDER BY start_file_index'):
                if seg['start_file_index']!=expected_start:
                    raise ValueError('Archive segment continuity lost')
                p=Path(seg['archive_path'])
                if not p.is_file() or sha256_file(p)!=seg['archive_sha256']:
                    raise ValueError(f'Archive missing or hash mismatch: {p}')
                got=0; outs=0
                with gzip.open(p,'rt',encoding='utf-8') as f:
                    for line in f:
                        x=json.loads(line)
                        if x['file_index']!=expected_start+got:
                            raise ValueError('Archive record index gap or reorder')
                        if hashlib.sha256(x['payload'].encode('utf-8')).hexdigest()!=x['raw_sha256']:
                            raise ValueError('Archive payload hash mismatch')
                        if x.get('outbox'):
                            o=x['outbox']
                            if not o.get('acknowledged'):
                                raise ValueError('Archive of non-ACKed event')
                            if hashlib.sha256(canonical(json.loads(o['payload'])).encode('utf-8')).hexdigest()!=o['raw_sha256']:
                                raise ValueError('Archived outbox corrupt')
                            outs+=1
                            if effects:
                                from .external_delivery import event_identity
                                eid=event_identity(o['lane'],o['seq'],0,o['raw_sha256'])
                                if not effects.execute('SELECT 1 FROM effects WHERE event_id=?',(eid,)).fetchone():
                                    missing_effects+=1
                        got+=1
                if got!=seg['record_count'] or outs!=seg['outbox_count']:
                    raise ValueError('Archive manifest record count mismatch')
                if seg['end_file_index']!=seg['start_file_index']+got:
                    raise ValueError('Archive segment boundary mismatch')
                expected_start+=got
                archive_count+=got
                archive_output+=outs
            if expected_start!=int(get_meta(db,'archived_records') or 0):
                raise ValueError('Archived cursor mismatch')
            for r in db.execute('SELECT lane,seq,raw_sha256,delivered FROM outbox'):
                if r['delivered']:
                    resident_output+=1
                    if effects:
                        from .external_delivery import event_identity
                        eid=event_identity(r['lane'],r['seq'],0,r['raw_sha256'])
                        if not effects.execute('SELECT 1 FROM effects WHERE event_id=?',(eid,)).fetchone():
                            missing_effects+=1
            remaining=db.execute('SELECT COUNT(*) FROM raw_journal').fetchone()[0]
            ack=int(get_meta(db,'next_file_index'))
            if archive_count+remaining!=ack:
                raise ValueError('Archived plus resident records do not equal file checkpoint')
            if missing_effects:
                raise ValueError(f'{missing_effects} external receipt IDs are missing')
            return {'lane':alias,'archive_records':archive_count,'resident_raw':remaining,
                    'checkpoint':ack,'archived_outbox_acked':archive_output,
                    'resident_outbox_acked':resident_output,'consumer_identities_missing':missing_effects,
                    'archive_continuity_and_hashes':True}
        finally:
            if effects:effects.close()
    finally:
        db.close()


def main(argv=None):
    p=argparse.ArgumentParser(description='Offline verified archive + ACK-gated prune; requires EXCLUSIVE maintenance window')
    p.add_argument('--outdir',type=Path,required=True)
    p.add_argument('--lane',required=True)
    p.add_argument('--mode',choices=['archive','audit'],required=True)
    p.add_argument('--limit',type=int,default=5000)
    p.add_argument('--vacuum',action='store_true')
    p.add_argument('--crash-stage',choices=['after_archive_before_commit','after_commit'])
    p.add_argument('--consumer-db',type=Path)
    a=p.parse_args(argv)
    result=prune(a.outdir,a.lane,a.limit,a.vacuum,a.consumer_db,a.crash_stage) if a.mode=='archive' else audit(a.outdir,a.lane,a.consumer_db)
    print(json.dumps(result,indent=2,ensure_ascii=False))
    report=a.outdir/('RETENTION_ARCHIVE_RESULT.json' if a.mode=='archive' else 'RETENTION_AUDIT_RESULT.json')
    report.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')

if __name__=='__main__':main()
