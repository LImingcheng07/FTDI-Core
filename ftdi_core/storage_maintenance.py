"""Storage maintenance: ``vacuum-db`` rebuilds one FTDI SQLite file without changing a logical row (ADR-010).

Contract: ``docs/STORAGE_MAINTENANCE.md``. The original file is never deleted: it is kept next to the new one
as ``<db>.pre-vacuum-<UTC timestamp>``; the owner decides when to remove it. Dry run unless ``apply``.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import time

from .archive_e2e import _Snapshot, _Unopenable

PRODUCER_TABLES = frozenset({'meta', 'raw_journal', 'outbox', 'delivery', 'disposition', 'pending'})
MARGIN_BYTES = 16 * 2**20
DIGEST_DOMAIN = b'FTDI-logical-digest-v1\n'


def _quote(name):
    return '"' + str(name).replace('"', '""') + '"'


def _encode(values):
    out = []
    for value in values:
        if value is None:
            out.append(b'N')
        elif isinstance(value, int):
            out.append(b'I' + str(value).encode())
        elif isinstance(value, float):
            out.append(b'F' + value.hex().encode())
        elif isinstance(value, str):
            data = value.encode('utf-8')
            out.append(b'S' + str(len(data)).encode() + b':' + data)
        elif isinstance(value, bytes):
            out.append(b'B' + str(len(value)).encode() + b':' + value)
        else:  # pragma: no cover - sqlite3 returns only the types above
            raise ValueError(f'unexpected SQLite value type {type(value).__name__}')
    return b'|'.join(out) + b'\n'


def logical_digest(db):
    """SHA-256 over the schema and every row of every table; independent of page layout and rowids."""
    digest = hashlib.sha256(DIGEST_DOMAIN)
    schema = db.execute('SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name').fetchall()
    for row in schema:
        digest.update(_encode(tuple(row)))
    for name in sorted(row[1] for row in schema if row[0] == 'table'):
        columns = [info[1] for info in db.execute(f'PRAGMA table_info({_quote(name)})')]
        digest.update(_encode(('TABLE', name, len(columns))))
        order = ', '.join(_quote(c) for c in columns)
        for row in db.execute(f'SELECT * FROM {_quote(name)} ORDER BY {order}'):
            digest.update(_encode(tuple(row)))
    return digest.hexdigest()


def _kind(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    if PRODUCER_TABLES <= tables:
        return 'producer'
    if 'effects' in tables:
        return 'consumer'
    raise ValueError('not an FTDI producer lane DB or consumer DB; refused')


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _wal_frames(path):
    wal = Path(f'{path}-wal')
    return wal.is_file() and wal.stat().st_size > 0


def _inspect(path):
    """Read-only facts and digest of the database at ``path``."""
    try:
        snap = _Snapshot(path, 'database')
    except _Unopenable as exc:
        raise ValueError(f'refusing read-only open ({exc})') from None
    except sqlite3.DatabaseError as exc:
        raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
    try:
        db = snap.conn
        kind = _kind(db)
        facts = {name: db.execute(f'PRAGMA {name}').fetchone()[0]
                 for name in ('page_size', 'page_count', 'freelist_count', 'user_version', 'application_id')}
        facts['logical_digest'] = logical_digest(db)
    except sqlite3.DatabaseError as exc:
        raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
    finally:
        snap.close()
    if not snap.stable:
        raise ValueError('database changed while it was read; stop writers and retry')
    facts['kind'] = kind
    return facts


def _check_path(path):
    if not path.exists() and not path.is_symlink():
        raise FileNotFoundError(f'database not found: {path}')
    if path.is_symlink() or not path.is_file():
        raise ValueError(f'not a regular database file (links are refused): {path}')
    if Path(f'{path}-journal').exists():
        raise ValueError('rollback journal present: let the writing program recover it first; refused')


def _checkpoint(path):
    """Fold the WAL into the main file; an active reader or writer elsewhere blocks this -> refuse."""
    with closing(sqlite3.connect(str(path), timeout=5, isolation_level=None)) as writer:
        writer.execute('PRAGMA busy_timeout=5000')
        mode = writer.execute('PRAGMA journal_mode').fetchone()[0].lower()
        if mode == 'wal':
            busy = writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]
            if busy:
                raise ValueError('database in use: another connection blocks the checkpoint; refused')
    if _wal_frames(path):
        raise ValueError('database in use: WAL frames remain after the checkpoint; refused')
    return mode


def vacuum_db(path, *, apply=False):
    given = str(path)
    path = Path(path).absolute()
    _check_path(path)
    bytes_before = path.stat().st_size
    mode = None
    if _wal_frames(path) and not Path(f'{path}-shm').exists():
        if not apply:
            raise ValueError('WAL frames without -shm: the dry run never creates files; run with --apply (it first '
                             'recovers and folds the WAL) or open and close the database with its writing program')
        mode = _checkpoint(path)   # a normal open recovers the WAL; logical content unchanged
    facts = _inspect(path)
    used = (facts['page_count'] - facts['freelist_count']) * facts['page_size']
    needed = used + MARGIN_BYTES
    free = shutil.disk_usage(path.parent).free
    base = {'kind': facts['kind'], 'db': given}
    if not apply:
        wal = Path(f'{path}-wal')
        return {'status': 'DRY_RUN', **base, 'page_size': facts['page_size'], 'page_count': facts['page_count'],
                'freelist_count': facts['freelist_count'], 'file_bytes': path.stat().st_size,
                'wal_bytes': wal.stat().st_size if wal.is_file() else 0,
                'reclaimable_bytes_estimate': facts['freelist_count'] * facts['page_size'],
                'space_needed_bytes': needed, 'space_free_bytes': free,
                'logical_digest': facts['logical_digest'], 'apply_required': True, 'changed': False}
    if free < needed:
        raise ValueError(f'not enough free space in {path.parent}: need {needed} bytes, {free} free; refused')
    stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    kept = Path(f'{path}.pre-vacuum-{stamp}')
    temp = Path(f'{path}.vacuum-{stamp}.tmp')
    for candidate in (kept, temp, Path(f'{kept}-wal'), Path(f'{kept}-shm'), Path(f'{temp}-wal'),
                      Path(f'{temp}-shm'), Path(f'{temp}-journal')):
        if candidate.exists() or candidate.is_symlink():
            raise ValueError(f'name collision: {candidate.name} already exists; retry in a second; refused')

    # 1. Fold the WAL into the main file (done above already if the WAL had to be recovered first).
    if mode is None:
        mode = _checkpoint(path)
    # 2. Fingerprint and logical digest of the quiescent original.
    fingerprint = _sha256(path)
    before = _inspect(path)
    if before['logical_digest'] != facts['logical_digest']:
        raise ValueError('database changed between inspection and checkpoint; stop writers and retry')
    try:
        # 3. Rebuild into our own temporary file in the same directory. No WAL frames remain, so the file is read
        #    as immutable (no sidecar is created); any concurrent change is caught by the fingerprint in step 5.
        with closing(sqlite3.connect(f'{path.as_uri()}?mode=ro&immutable=1', uri=True, timeout=5)) as reader:
            reader.execute('VACUUM INTO ?', (str(temp),))
        # 4. Verify the rebuilt file.
        with closing(sqlite3.connect(str(temp), isolation_level=None)) as new:
            check = new.execute('PRAGMA integrity_check').fetchall()
            if check != [('ok',)]:
                raise ValueError(f'integrity_check of the rebuilt file failed: {check[:3]}')
            if mode == 'wal':
                new.execute('PRAGMA journal_mode=WAL')
        rebuilt = _inspect(temp)
        for key in ('logical_digest', 'user_version', 'application_id'):
            if rebuilt[key] != before[key]:
                raise ValueError(f'rebuilt file differs from the original ({key}); refused')
        for sidecar in (Path(f'{temp}-wal'), Path(f'{temp}-shm'), Path(f'{temp}-journal')):
            if sidecar.exists():
                raise ValueError(f'rebuilt file left a sidecar {sidecar.name}; refused')
        # 5. The original must not have changed while it was rebuilt.
        if _wal_frames(path) or _sha256(path) != fingerprint:
            raise ValueError('database changed during vacuum; stop writers and retry; refused')
        # 6. Swap, keeping the original; undo every completed rename on failure.
        _swap(path, temp, kept)
    except BaseException:
        if temp.exists():
            temp.unlink()   # only our own scratch file; the original is untouched
        raise
    # 7. Verify the result in place.
    after = _inspect(path)
    if after['logical_digest'] != before['logical_digest']:
        raise ValueError(f'post-swap verification failed; the original is kept at {kept}')
    return {'status': 'VACUUMED', **base, 'bytes_before': bytes_before, 'bytes_after': path.stat().st_size,
            'logical_digest': after['logical_digest'], 'kept_original': str(kept), 'changed': True,
            'note': 'the kept original is never deleted by FTDI; remove it yourself once you have verified the '
                    'result (e.g. with audit-e2e-archive)'}


def _swap(path, temp, kept):
    done = []
    try:
        os.rename(path, kept)
        done.append((kept, path))
        for suffix in ('-wal', '-shm'):
            sidecar = Path(f'{path}{suffix}')
            if sidecar.exists():
                os.rename(sidecar, f'{kept}{suffix}')
                done.append((Path(f'{kept}{suffix}'), sidecar))
        os.rename(temp, path)
        done.append((path, temp))
    except OSError as exc:
        for source, target in reversed(done):
            try:
                os.rename(source, target)
            except OSError as again:
                raise ValueError(f'swap failed ({exc}) and could not be undone ({again}): the original database is '
                                 f'at {kept}; rename it back to {path}') from exc
        raise ValueError(f'swap failed ({exc}); every rename was undone, the original is at {path}; '
                         'is another process using the database?') from exc
