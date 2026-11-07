#!/usr/bin/env python3
"""Two-database HTTP idempotency laboratory (loopback only, no trading).

Producer outbox and consumer effect store are separate SQLite files/processes.
Remote commit before local acknowledgement is intentionally at-least-once delivery.
"""
from __future__ import annotations
import hashlib
import json
import sqlite3
from pathlib import Path
from .durable_stream import canonical, scope_of

VERSION = 'FTDI-S07-004.1-envelope-v1'


def event_identity(lane, seq, epoch, payload_sha256):
    return hashlib.sha256(canonical([VERSION,lane,seq,epoch,payload_sha256]).encode('utf-8')).hexdigest()


def envelope(row):
    payload=json.loads(row['payload'])
    lane,seq,digest=row['lane'],row['seq'],row['raw_sha256']
    if scope_of(payload)!=lane or payload['source_seq']!=seq:
        raise ValueError('Source DB outbox identity mismatch')
    if hashlib.sha256(canonical(payload).encode('utf-8')).hexdigest()!=digest:
        raise ValueError('Source DB outbox payload hash mismatch')
    epoch=0   # S07-004.1 does NOT yet implement verified book/stream epoch re-anchoring.
    return {'event_id':event_identity(lane,seq,epoch,digest),'lane':lane,'seq':seq,
            'epoch':epoch,'payload_sha256':digest,'payload':payload}


def validate(x):
    if not isinstance(x,dict):
        raise ValueError('expected event object')
    lane=x.get('lane'); seq=x.get('seq'); epoch=x.get('epoch'); digest=x.get('payload_sha256')
    payload=x.get('payload')
    if not isinstance(lane,str) or type(seq) is not int or seq<0 or type(epoch) is not int or epoch!=0:
        raise ValueError('bad lane, seq, or unsupported epoch')
    if not isinstance(payload,dict) or scope_of(payload)!=lane or payload.get('source_seq')!=seq:
        raise ValueError('scope / sequence mismatch')
    actual=hashlib.sha256(canonical(payload).encode('utf-8')).hexdigest()
    if digest!=actual or x.get('event_id')!=event_identity(lane,seq,epoch,actual):
        raise ValueError('payload/event identity hash mismatch')
    return (x['event_id'],lane,seq,epoch,digest,canonical(payload))


def batch_digest(events):
    h=hashlib.sha256()
    for e in events:
        h.update((e['event_id']+'\n').encode('ascii'))
    return h.hexdigest()


class Conflict(Exception):
    pass


class EffectStore:
    def __init__(self,path):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(str(self.path),timeout=30)
        try:
            self.db.row_factory=sqlite3.Row
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.execute('PRAGMA busy_timeout=30000')
            # Core divergence from reference_s07_0041 (ADR-009): fewer, larger checkpoints coalesce
            # the random index-page writes of event_id keys. Durability unchanged: every commit is
            # still synchronous=FULL in the WAL. Measured -37% publish time on a SATA HDD.
            self.db.execute('PRAGMA wal_autocheckpoint=16000')
            self.db.executescript('''
            CREATE TABLE IF NOT EXISTS effects(
                event_id TEXT PRIMARY KEY,
                lane TEXT NOT NULL, seq INTEGER NOT NULL, epoch INTEGER NOT NULL,
                payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL,
                UNIQUE(lane,seq,epoch)
            );
            ''')
            self.db.commit()
        except BaseException:
            self.db.close()
            raise

    def accept(self,events):
        if not isinstance(events,list) or not 1<=len(events)<=1024:
            raise ValueError('Expected nonempty batch of at most 1024')
        rows=[validate(e) for e in events]
        inserted=0; duplicated=0
        self.db.execute('BEGIN IMMEDIATE')
        try:
            for event_id,lane,seq,epoch,digest,payload in rows:
                old=self.db.execute('SELECT event_id,payload_sha256 FROM effects WHERE lane=? AND seq=? AND epoch=?',
                                    (lane,seq,epoch)).fetchone()
                if old is not None:
                    if old['event_id']!=event_id or old['payload_sha256']!=digest:
                        raise Conflict(f'Conflicting event for lane={lane}, seq={seq}')
                    duplicated+=1
                    continue
                self.db.execute('INSERT INTO effects VALUES(?,?,?,?,?,?)',
                                (event_id,lane,seq,epoch,digest,payload))
                inserted+=1
            self.db.commit()  # External effect is committed BEFORE HTTP ack is sent.
        except BaseException:
            self.db.rollback()
            raise
        return {'accepted':len(rows),'inserted':inserted,'duplicates':duplicated,
                'batch_digest':batch_digest(events)}

    def count(self):
        return self.db.execute('SELECT COUNT(*) FROM effects').fetchone()[0]

    def logical_sha256(self):
        h=hashlib.sha256()
        for r in self.db.execute('SELECT event_id FROM effects ORDER BY lane,seq,epoch'):
            h.update((r['event_id']+'\n').encode('ascii'))
        return h.hexdigest()

    def close(self):self.db.close()
    def __enter__(self):return self
    def __exit__(self,*args):self.close()
