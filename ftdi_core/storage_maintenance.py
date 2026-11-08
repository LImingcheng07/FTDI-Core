"""Storage maintenance: ``vacuum-db`` reclaims space in one FTDI SQLite file in place (ADR-010).

Contract: ``docs/STORAGE_MAINTENANCE.md``. Dry run unless ``apply``. Apply holds an exclusive SQLite lock for the
whole operation (refused if another connection holds a lock; nobody can open a transaction meanwhile), makes and
*verifies* a byte copy of the database
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
import struct
import tempfile
import time

from .archive_e2e import _Snapshot, _Unopenable

PRODUCER_TABLES = frozenset({'meta', 'raw_journal', 'outbox', 'delivery', 'disposition', 'pending'})
# Every schema object FTDI itself creates (durable_stream, retention, external_delivery, consumer_retention).
# Anything else (virtual/FTS tables, views, foreign tables, indexes or triggers, generated columns) is refused:
# VACUUM may renumber implicit rowids, and objects FTDI does not know could depend on them.
PRODUCER_OBJECTS = frozenset({
    ('table', 'meta'), ('table', 'raw_journal'), ('table', 'lanes'), ('table', 'pending'), ('table', 'outbox'),
    ('table', 'delivery'), ('table', 'disposition'), ('table', 'archive_segments'), ('table', 'sqlite_sequence'),
    ('index', 'idx_unprocessed'), ('index', 'idx_outbox_delivery'), ('index', 'idx_retention_outbox_raw'),
    ('index', 'idx_retention_delivery_raw'), ('index', 'idx_retention_pending_raw')})
CONSUMER_OBJECTS = frozenset({
    ('table', 'effects'), ('table', 'consumer_meta'), ('table', 'lane_ids'), ('table', 'retention_state'),
    ('table', 'effect_digests'), ('table', 'effect_segments'), ('trigger', 'effects_guard_compacted')})
MARGIN_BYTES = 16 * 2**20
DIGEST_DOMAIN = b'FTDI-logical-digest-v2\n'
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
        columns = [_quote(info[1]) for info in db.execute(f'PRAGMA main.table_xinfo({_quote(name)})')]
        digest.update(_encode(b'T', (name, len(columns))))
        order = ', '.join(f'typeof({c}), {c} COLLATE BINARY' for c in columns)
        count = 0
        for row in db.execute(f'SELECT {", ".join(columns)} FROM main.{_quote(name)} ORDER BY {order}'):
            digest.update(_encode(b'R', tuple(row)))
            count += 1
        digest.update(_encode(b'E', (name, count)))
    return digest.hexdigest()


def _kind(db):
    """'producer' or 'consumer' if every schema object is one FTDI creates, else refused."""
    rows = db.execute('SELECT type, name, tbl_name, sql FROM main.sqlite_master').fetchall()
    tables = {name for kind, name, _, _ in rows if kind == 'table'}
    if PRODUCER_TABLES <= tables:
        kind, allowed = 'producer', PRODUCER_OBJECTS
    elif 'effects' in tables:
        kind, allowed = 'consumer', CONSUMER_OBJECTS
    else:
        raise ValueError('not an FTDI producer lane DB or consumer DB; refused')
    known_tables = {name for typ, name in allowed if typ == 'table'}
    for typ, name, table, sql in rows:
        automatic = typ == 'index' and sql is None and name.startswith(f'sqlite_autoindex_{table}_')
        if not (automatic and table in known_tables) and ((typ, name) not in allowed or table not in known_tables):
            raise ValueError(f'schema object {typ} {name!r} is not an FTDI {kind} object; refused')
        if typ == 'table' and name != 'sqlite_sequence' and not str(sql).startswith('CREATE TABLE'):
            raise ValueError(f'table {name!r} is not a plain table; refused')
    for name in tables:
        if any(info[6] for info in db.execute(f'PRAGMA main.table_xinfo({_quote(name)})')):
            raise ValueError(f'table {name!r} has hidden or generated columns; refused')
    return kind


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


def _sidecar_problem(sidecar, suffix, page_size):
    """None if ``sidecar`` is structurally a SQLite WAL / shared-memory file for this database, else a reason.

    SQLite deletes or truncates these files; a foreign file at one of these names must never be touched."""
    size = sidecar.stat().st_size
    if size == 0:
        return None
    with open(sidecar, 'rb') as handle:
        header = handle.read(96)
    if suffix == '-wal':
        if len(header) < 32:
            return 'too short for a WAL header'
        magic, version, wal_page = struct.unpack('>3I', header[:12])
        if magic not in (0x377f0682, 0x377f0683) or version != 3007000:
            return 'no SQLite WAL magic/version'
        if page_size is not None and wal_page != page_size:
            return 'WAL page size differs from the database'
        words = struct.unpack(('>' if magic & 1 else '<') + '6I', header[:24])
        s1 = s2 = 0
        for i in range(0, 6, 2):
            s1 = (s1 + words[i] + s2) & 0xFFFFFFFF
            s2 = (s2 + words[i + 1] + s1) & 0xFFFFFFFF
        if (s1, s2) != struct.unpack('>2I', header[24:32]):
            return 'WAL header checksum mismatch'
        return None
    if size % 32768 or len(header) < 96 or header[:48] != header[48:96] \
            or struct.unpack('<I', header[:4])[0] != 3007000:
        return 'not a SQLite shared-memory file'
    return None


def _page_size(path):
    with open(path, 'rb') as handle:
        header = handle.read(100)
    if len(header) < 100 or not header.startswith(b'SQLite format 3\x00'):
        return None
    value = struct.unpack('>H', header[16:18])[0]
    return 65536 if value == 1 else value


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
    page_size = _page_size(path)
    for suffix in ('-wal', '-shm'):
        sidecar = Path(f'{path}{suffix}')
        if not sidecar.exists() and not sidecar.is_symlink():
            continue
        if sidecar.is_symlink() or not sidecar.is_file():
            raise ValueError(f'{sidecar.name} is not a regular file; refused')
        if os.stat(sidecar).st_nlink != 1 or os.path.samefile(sidecar, path):
            raise ValueError(f'{sidecar.name} is hard-linked to another file; refused')
        problem = _sidecar_problem(sidecar, suffix, page_size)
        if problem:
            raise ValueError(f'{sidecar.name} is not a SQLite {"WAL" if suffix == "-wal" else "shared-memory"} '
                             f'file of this database ({problem}); refused')


def _sidecar_identities(path):
    """(dev, ino, nlink) of existing sidecars, to detect a file swapped in after the checks."""
    result = {}
    for suffix in ('-wal', '-shm'):
        try:
            info = os.stat(f'{path}{suffix}')
        except FileNotFoundError:
            result[suffix] = None
            continue
        result[suffix] = (info.st_dev, info.st_ino, info.st_nlink)
    return result


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
    """Byte copy into a file this call creates (fails if the name exists); returns the target's identity.

    If anything fails after the file was created, this call removes it again (it knows the file is its own)
    or names it in the error."""
    digest = hashlib.sha256()
    with open(source, 'rb') as src:
        dst = open(target, 'xb')
        info = os.fstat(dst.fileno())
        identity = (info.st_dev, info.st_ino)
        try:
            with dst:
                for chunk in iter(lambda: src.read(COPY_CHUNK), b''):
                    dst.write(chunk)
                    digest.update(chunk)
                dst.flush()
                os.fsync(dst.fileno())
        except BaseException as exc:
            try:
                _remove_own(target, identity)
            except OSError as again:
                raise ValueError(f'copying failed ({exc}); the incomplete copy {target} could not be removed '
                                 f'({again}); it is NOT a valid backup') from exc
            raise
    return identity, digest.hexdigest()


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
    sidecars = _sidecar_identities(path)
    try:
        db = sqlite3.connect(str(path), timeout=1, isolation_level=None)
    except sqlite3.DatabaseError as exc:
        raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
    try:
        try:
            # Exclusive for the whole operation: refused if another connection is reading or writing, and nobody
            # else can open a read or write transaction until this run ends.
            db.execute('PRAGMA locking_mode=EXCLUSIVE')
            db.execute('BEGIN EXCLUSIVE')
        except sqlite3.OperationalError as exc:
            raise ValueError(f'database in use: another connection holds a lock ({exc}); refused') from None
        except sqlite3.DatabaseError as exc:
            raise ValueError(f'not a readable SQLite database ({exc}); refused') from None
        # The checks ran before the open; repeat them under the lock (no SQLite writer can interfere any more)
        # and require that no sidecar was swapped in meanwhile. A file created at a sidecar name after the
        # first check is refused before SQLite writes to it (the checkpoint below is the first write).
        _check_path(path)
        if any(old is not None and old != new for old, new in zip(sidecars.values(),
                                                                    _sidecar_identities(path).values())):
            raise ValueError('a sidecar file was replaced during the run; refused')
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
        try:
            identity, copied = _copy_exclusive(path, kept)
        except FileExistsError:
            raise ValueError(f'name collision: {kept.name} appeared during the run (not created by FTDI, left '
                             'untouched); refused') from None
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
        db.close()
        bytes_after = os.path.getsize(path)
    except BaseException as exc:
        if not vacuumed:
            db.close()
            _remove_own(kept, identity)   # only our own copy; the original was not changed
            if identity is not None and kept.exists() and isinstance(exc, Exception):
                raise ValueError(f'{exc} (the copy at {kept} could not be removed; it is NOT a valid backup)') from exc
            raise
        db.close()
        raise ValueError(f'{exc} -- the verified backup of the original is kept at {kept}; the database at {path} '
                         'is a consistent SQLite file (VACUUM is transactional); audit it before further use') from exc
    return {'status': 'VACUUMED', 'kind': kind, 'db': given, 'bytes_before': bytes_before,
            'bytes_after': bytes_after, 'logical_digest': before, 'kept_original': str(kept),
            'changed': True,
            'note': 'the kept original is a verified copy made before the rebuild; FTDI never deletes it. Remove '
                    'it yourself once you have verified the result (e.g. with audit-e2e-archive)'}
