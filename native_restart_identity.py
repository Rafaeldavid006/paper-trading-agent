"""Restart-portable local cohort identity. No enrollment, repair or I/O writes.

A completed, separately approved migration binds a random logical cohort to the
existing paths/inodes. Current device/inode pairs still fence every process use.
The markers are not secrets or MACs and do not authenticate whole-store restores,
coherent same-user rewrites, or contents predating the migration's new baseline.
"""
from dataclasses import dataclass
import hashlib
import json
import os
import re
from types import MappingProxyType

VERSION = 2
TABLE = 'native_restart_identity'
SQL = ('CREATE TABLE native_restart_identity(id INTEGER PRIMARY KEY CHECK(id=1),'
       'version INTEGER NOT NULL,status TEXT NOT NULL,cohort TEXT NOT NULL,'
       'inventory TEXT NOT NULL,preimage_digest TEXT NOT NULL,contract_digest TEXT NOT NULL)')
FORMAT = 'native-restart-identity-v2'
MAX_RECORD = 16384


class RestartIdentityRefused(ValueError):
    pass


def require(ok, code='restart_identity_invalid'):
    if not ok:
        raise RestartIdentityRefused(code)


def _hex(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def marker(cohort, role):
    require(_hex(cohort) and type(role) is str)
    from runtime_state_bundle import GENERATION
    return hashlib.sha256((FORMAT + ':' + GENERATION + ':' + cohort + ':' + role).encode()).hexdigest()


def stamp(cohort, role):
    raw = bytes.fromhex(marker(cohort, role))
    return tuple((int.from_bytes(raw[i:i+4], 'big') & 0x7fffffff) or 1 for i in (0, 4))


def file_marker(cohort, role, contract):
    require(_hex(contract))
    return dict(format=FORMAT, cohort=cohort, role=role, token=marker(cohort, role), contract_digest=contract)


def contract_digest(cohort, inventory, preimage):
    return hashlib.sha256(_json([VERSION, cohort, inventory, preimage]).encode()).hexdigest()


def table_present(db):
    rows = db.execute('SELECT type,sql FROM sqlite_master WHERE name=?', (TABLE,)).fetchall()
    if not rows:
        return False
    require(rows == [('table', SQL)], 'restart_identity_schema_invalid')
    require(not db.execute("SELECT 1 FROM sqlite_master WHERE (type='trigger' OR type='index') AND tbl_name=?",
                           (TABLE,)).fetchone(), 'restart_identity_schema_invalid')
    return True


def read_record(db, *, allow_prepared=False):
    if not table_present(db):
        return None
    rows = db.execute('SELECT * FROM ' + TABLE).fetchmany(2)
    require(len(rows) == 1, 'restart_identity_record_invalid')
    row = rows[0]
    require(len(row) == 7 and row[:2] == (1, VERSION), 'restart_identity_record_invalid')
    require(row[2] in (('prepared', 'completed') if allow_prepared else ('completed',)),
            'restart_identity_migration_incomplete')
    require(_hex(row[3]) and type(row[4]) is str and len(row[4]) <= MAX_RECORD
            and _hex(row[5]) and _hex(row[6]), 'restart_identity_record_invalid')
    from runtime_state_bundle import _load
    inventory = _load(row[4])
    require(row[4] == _json(inventory) and row[6] == contract_digest(row[3], inventory, row[5]),
            'restart_identity_record_invalid')
    return row, inventory


def read_file(path, identity, *, maximum=4096, public_read=False):
    from runtime_state_bundle import _private
    require(_private(path, public_read=public_read) == identity, 'restart_identity_file_changed')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require((info.st_dev, info.st_ino) == identity, 'restart_identity_file_changed')
        chunks, remaining = [], maximum + 1
        while remaining:
            chunk = os.read(fd, min(1024 * 1024, remaining))
            if not chunk: break
            chunks.append(chunk); remaining -= len(chunk)
        value = b''.join(chunks)
        require(len(value) <= maximum, 'restart_identity_record_invalid')
        require(_private(path, public_read=public_read) == identity, 'restart_identity_file_changed')
        return value
    finally:
        os.close(fd)


@dataclass(frozen=True)
class Identity:
    cohort: str
    references: object
    contract: str
    preimage: str
    metadata: object


def load(bundle, current, *, allow_prepared=False):
    """Read every marker. A return value is not a substitute for bundle.verify."""
    from runtime_state_bundle import _database, _load, DB_ROLES, GENERATION, _stamp
    with _database(bundle.paths['anchor'], current['anchor'], public_read=True) as db:
        record = read_record(db, allow_prepared=allow_prepared)
    if record is None:
        # No silent fallback after a missing root. These headers and the lock
        # were unowned/empty at migration, and the old v1-checked seal/new-role
        # headers separately reject remnants of any completed migration.
        for role in ('legacy', 'anchor', 'manifest'):
            with _database(bundle.paths[role], current[role], public_read=role=='anchor') as db:
                require((db.execute('PRAGMA application_id').fetchone()[0],
                         db.execute('PRAGMA user_version').fetchone()[0]) == (0, 0),
                        'restart_identity_root_missing')
        require(read_file(bundle.paths['legacy_lock'], current['legacy_lock'], maximum=512) == b'',
                'restart_identity_root_missing')
        return None
    row, inventory = record
    require(type(inventory) is dict and set(inventory) == {'generation','binding','files','manifest_digest','history','schemas'}
            and inventory['generation'] == GENERATION and inventory['binding'] == bundle.binding,
            'restart_identity_binding_mismatch')
    require(_hex(inventory['manifest_digest']) and type(inventory['history']) is list
            and len(inventory['history']) == 2 and all(_hex(v) for v in inventory['history'])
            and type(inventory['schemas']) is dict, 'restart_identity_inventory_invalid')
    files = inventory['files']
    require(type(files) is dict and set(files) == set(bundle.paths), 'restart_identity_inventory_invalid')
    references = {}
    for role, path in bundle.paths.items():
        item = files[role]
        require(type(item) is dict and set(item) == {'path','device','inode'} and item['path'] == path
                and type(item['device']) is int and item['device'] >= 0
                and type(item['inode']) is int and item['inode'] > 0,
                'restart_identity_inventory_invalid')
        require(current[role][1] == item['inode'], 'restart_identity_inode_changed')
        references[role] = (item['device'], item['inode'])
    require(len({pair[0] for pair in references.values()}) == 1
            and len({pair[0] for pair in current.values()}) == 1, 'restart_identity_device_set_invalid')
    cohort = row[3]
    roles = (*DB_ROLES, 'anchor', 'manifest')
    require(len({stamp(cohort, role) for role in roles}) == len(roles), 'restart_identity_stamp_invalid')
    for role in roles:
        expected = stamp(cohort, role)
        require(expected != _stamp(role), 'restart_identity_stamp_invalid')
        with _database(bundle.paths[role], current[role], public_read=role=='anchor') as db:
            actual = (db.execute('PRAGMA application_id').fetchone()[0],
                      db.execute('PRAGMA user_version').fetchone()[0])
            require(actual == expected, 'restart_identity_stamp_mismatch')
    seal = _load(read_file(bundle.paths['seal'], current['seal']))
    require(type(seal) is dict and seal.get('restart_identity') == file_marker(cohort, 'seal', row[6]),
            'restart_identity_seal_mismatch')
    require(read_file(bundle.paths['legacy_lock'], current['legacy_lock'], maximum=512)
            == _json(file_marker(cohort, 'legacy_lock', row[6])).encode(), 'restart_identity_lock_mismatch')
    require({role: bundle._identity(role) for role in bundle.paths} == current,
            'restart_identity_file_changed')
    return Identity(cohort, MappingProxyType(references), row[6], row[5], MappingProxyType(inventory))
