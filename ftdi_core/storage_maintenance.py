"""Storage maintenance: ``vacuum-db`` reclaims space in one FTDI SQLite file in place (ADR-010).

Contract: ``docs/STORAGE_MAINTENANCE.md``. Dry run unless ``apply``. Apply holds an exclusive SQLite lock for the
whole operation (any other open connection -> refused), makes and *verifies* a byte copy of the database
(``<db>.pre-vacuum-<UTC timestamp>``) before the first write, then runs SQLite's own transactional ``VACUUM`` on
the original and proves the logical content unchanged. The copy is never deleted by FTDI.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time

from .archive_e2e import _Snapshot, _Unopenable

PRODUCER_TABLES = frozenset({'meta', 'raw_journal', 'outbox', 'delivery', 'disposition', 'pending'})
MARGIN_BYTES = 16 * 2**20
DIGEST_DOMAIN = b'FTDI-logical-digest-v2\n'
WAL_MAGIC = (b'\x37\x7f\x06\x82', b'\x37\x7f\x06\x83')
COPY_CHUNK = 1 << 20


def _quote(name):
    return '"' + str(name).replace('"', '""') + '"'


def _encode(tag, values):
    """One self-delimiting record: tag byte, then type-tagged values (strings/blobs length-prefixed)."""
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
    return tag + str(len(out)).encode() + b'(' + b'|'.join(out) + b')\n'


def logical_digest(db):
    """SHA-256 over the schema and every row of every table (main schema); independent of page layout and rowids.

    Records are framed by kind (M schema, T table header, R row, E table end with row count), so a row can never
    be mistaken for a table boundary. Rows are ordered by ``typeof(c), c COLLATE BINARY`` for every column, which is
    a total order except for distinct values SQLite compares as equal within one type (only REAL 0.0 / -0.0);
    such a tie can only make two equal databases look different (a refusal), never the reverse.
    """
    digest = hashlib.sha256(DIGEST_DOMAIN)
    schema = db.execute('SELECT type, name, tbl_name, sql FROM main.sqlite_master ORDER BY type, name').fetchall()
    for row in schema:
        digest.update(_encode(b'M', tuple(row)))
    for name in sorted(row[1] for row in schema if row[0] == 'table'):
        columns = [_quote(info[1]) for info in db.execute(f'PRAGMA main.table_info({_quote(name)})')]
        digest.update(_encode(b'T', (name, len(columns))))
        order = ', '.join(f'typeof({c}), {c} COLLATE BINARY' for c in columns)
        count = 0
        for row in db.execute(f'SELECT {", ".join(columns)} FROM main.{_quote(name)} ORDER BY {order}'):
            digest.update(_encode(b'R', tuple(row)))
            count += 1
        digest.update(_encode(b'E', (name, count)))
    return digest.hexdigest()


def _kind(db):
    tables = {row[0] for row in db.execute("SELECT name FROM main.sqlite_master WHERE type = 'table'")}
    if PRODUCER_TABLES <= tables:
        return 'producer'
    if 'effects' in tables:
        return 'consumer'
    raise ValueError('not an FTDI producer lane DB or consumer DB; refused')


def _facts(db):
    return {name: db.execute(f'PRAGMA {name}').fetchone()[0]
            for name in ('page_size', 'page_count', 'freelist_count', 'user_version', 'application_id',
                         'auto_vacuum')}


def _inspect(path):
    """Read-only facts and digest (dry run, backup verification)."""
    try:
        snap = _Snapshot(path, 'database')
    except _Unopenable as exc:
        raise ValueError(f'refusing read-only open ({exc})') from None
    except sqlite3.DatabaseError as exc:
        raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
    try:
        db = snap.conn
        kind = _kind(db)
        facts = _facts(db)
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
    """Refuse links, hard links and sidecars that are not plain SQLite sidecars of this file."""
    if not path.exists() and not path.is_symlink():
        raise FileNotFoundError(f'database not found: {path}')
    if path.is_symlink() or not path.is_file():
        raise ValueError(f'not a regular database file (links are refused): {path}')
    if os.path.normcase(os.path.realpath(path)) != os.path.normcase(str(path)):
        raise ValueError(f'database reached through a link or junction ({os.path.realpath(path)}); refused')
    if os.stat(path).st_nlink != 1:
        raise ValueError('database file has several hard links; refused')
    if Path(f'{path}-journal').exists() or Path(f'{path}-journal').is_symlink():
        raise ValueError('rollback journal present: let the writing program recover it first; refused')
    for suffix in ('-wal', '-shm'):
        sidecar = Path(f'{path}{suffix}')
        if not sidecar.exists() and not sidecar.is_symlink():
            continue
        if sidecar.is_symlink() or not sidecar.is_file():
            raise ValueError(f'{sidecar.name} is not a regular file; refused')
        if os.stat(sidecar).st_nlink != 1 or os.path.samefile(sidecar, path):
            raise ValueError(f'{sidecar.name} is hard-linked to another file; refused')
        if suffix == '-wal' and sidecar.stat().st_size:
            with open(sidecar, 'rb') as handle:
                header = handle.read(32)
            if len(header) < 32 or header[:4] not in WAL_MAGIC:
                raise ValueError(f'{sidecar.name} is not a SQLite WAL file; refused')


def _wal_frames(path):
    wal = Path(f'{path}-wal')
    return wal.is_file() and wal.stat().st_size > 0


def _space(path, facts):
    """(needed_in_directory, needed_in_temp_dir, same_device): copy + VACUUM WAL growth, and VACUUM's temp DB."""
    used = (facts['page_count'] - facts['freelist_count']) * facts['page_size']
    wal = Path(f'{path}-wal')
    copy = path.stat().st_size + (wal.stat().st_size if wal.is_file() else 0)
    temp_dir = tempfile.gettempdir()
    same = os.stat(path.parent).st_dev == os.stat(temp_dir).st_dev
    return copy + used + MARGIN_BYTES, used + MARGIN_BYTES, temp_dir, same


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(COPY_CHUNK), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_exclusive(source, target):
    """Byte copy into a file this call creates (fails if the name exists); returns the target's identity."""
    digest = hashlib.sha256()
    with open(source, 'rb') as src, open(target, 'xb') as dst:
        identity = os.fstat(dst.fileno())
        for chunk in iter(lambda: src.read(COPY_CHUNK), b''):
            dst.write(chunk)
            digest.update(chunk)
        dst.flush()
        os.fsync(dst.fileno())
    return (identity.st_dev, identity.st_ino), digest.hexdigest()


def _remove_own(path, identity):
    """Delete ``path`` only if it is still the file this run created."""
    try:
        info = os.stat(path)
    except FileNotFoundError:
        return
    if identity is not None and (info.st_dev, info.st_ino) == identity:
        os.unlink(path)


def vacuum_db(path, *, apply=False):
    given = str(path)
    path = Path(os.path.abspath(path))
    _check_path(path)
    if not apply:
        if _wal_frames(path) and not Path(f'{path}-shm').exists():
            raise ValueError('WAL frames without -shm: the dry run never creates files; run with --apply (it '
                             'recovers the WAL under its exclusive lock) or open and close the database with its '
                             'writing program')
        facts = _inspect(path)
        needed, needed_temp, temp_dir, same = _space(path, facts)
        wal = Path(f'{path}-wal')
        return {'status': 'DRY_RUN', 'kind': facts['kind'], 'db': given, 'page_size': facts['page_size'],
                'page_count': facts['page_count'], 'freelist_count': facts['freelist_count'],
                'file_bytes': path.stat().st_size, 'wal_bytes': wal.stat().st_size if wal.is_file() else 0,
                'reclaimable_bytes_estimate': facts['freelist_count'] * facts['page_size'],
                'space_needed_bytes': needed + (needed_temp if same else 0),
                'space_free_bytes': shutil.disk_usage(path.parent).free,
                'temp_space_needed_bytes': 0 if same else needed_temp, 'temp_dir': temp_dir,
                'logical_digest': facts['logical_digest'], 'apply_required': True, 'changed': False}
    return _apply(path, given)


def _apply(path, given):
    stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    kept = Path(f'{path}.pre-vacuum-{stamp}')
    for candidate in (kept, Path(f'{kept}-wal'), Path(f'{kept}-shm'), Path(f'{kept}-journal')):
        if candidate.exists() or candidate.is_symlink():
            raise ValueError(f'name collision: {candidate.name} already exists; retry in a second; refused')
    bytes_before = path.stat().st_size
    identity = None
    vacuumed = False
    try:
        db = sqlite3.connect(str(path), timeout=1, isolation_level=None)
    except sqlite3.DatabaseError as exc:
        raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
    try:
        try:
            # Exclusive for the whole operation: fails if any other connection (any process) has the DB open.
            db.execute('PRAGMA locking_mode=EXCLUSIVE')
            db.execute('BEGIN EXCLUSIVE')
        except sqlite3.OperationalError as exc:
            raise ValueError(f'database in use: another connection has it open ({exc}); refused') from None
        except sqlite3.DatabaseError as exc:
            raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
        try:
            kind = _kind(db)
            facts = _facts(db)
            before = logical_digest(db)
        except sqlite3.DatabaseError as exc:
            raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
        db.execute('COMMIT')   # the exclusive lock is kept (locking_mode=EXCLUSIVE)
        mode = db.execute('PRAGMA journal_mode').fetchone()[0].lower()
        if mode == 'wal' and db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]:
            raise ValueError('could not fold the WAL into the database file; refused')
        if _wal_frames(path):
            raise ValueError('WAL frames remain after the checkpoint; refused')
        needed, needed_temp, temp_dir, same = _space(path, facts)
        free = shutil.disk_usage(path.parent).free
        if free < needed + (needed_temp if same else 0) or (
                not same and shutil.disk_usage(temp_dir).free < needed_temp):
            raise ValueError(f'not enough free space: need {needed} bytes in {path.parent} and {needed_temp} for '
                             f'SQLite temporary files in {temp_dir}; refused')
        # Verified backup before the first change of the original.
        identity, copied = _copy_exclusive(path, kept)
        if not copied == _sha256(path) == _sha256(kept):   # re-read the copy from disk
            raise ValueError('backup copy differs from the database file; refused')
        backup = _inspect(kept)
        if backup['logical_digest'] != before:
            raise ValueError('backup copy is not logically identical to the database; refused')
        # In-place, transactional rebuild by SQLite itself.
        vacuumed = True
        db.execute('VACUUM')
        if mode == 'wal' and db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]:
            raise ValueError('VACUUM done but its WAL could not be folded back')
        check = db.execute('PRAGMA integrity_check').fetchall()
        if check != [('ok',)]:
            raise ValueError(f'integrity_check after VACUUM failed: {check[:3]}')
        after_facts = _facts(db)
        after = logical_digest(db)
        for key in ('user_version', 'application_id', 'page_size', 'auto_vacuum'):
            if after_facts[key] != facts[key]:
                raise ValueError(f'{key} changed by VACUUM')
        if after != before:
            raise ValueError('logical content changed by VACUUM')
    except BaseException as exc:
        if not vacuumed:
            db.close()
            _remove_own(kept, identity)   # only our own copy; the original was not changed
            raise
        db.close()
        raise ValueError(f'{exc} -- the verified backup of the original is kept at {kept}; the database at {path} '
                         'is a consistent SQLite file (VACUUM is transactional); audit it before further use') from exc
    db.close()
    return {'status': 'VACUUMED', 'kind': kind, 'db': given, 'bytes_before': bytes_before,
            'bytes_after': path.stat().st_size, 'logical_digest': before, 'kept_original': str(kept),
            'changed': True,
            'note': 'the kept original is a verified copy made before the rebuild; FTDI never deletes it. Remove '
                    'it yourself once you have verified the result (e.g. with audit-e2e-archive)'}
