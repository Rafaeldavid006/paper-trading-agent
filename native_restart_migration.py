"""Disabled one-time identity-metadata migration. Never a recovery/startup action.

No credentials, network, trading calls, state resets, quota progression, or old
claim changes. A plan/approval digest is not itself user authorization. Applying
requires separate operator approval and a verified external quiescent window.
A prepared/crashed migration is never resumed or rolled back automatically.
"""
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
import fcntl
import hashlib
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import time

import runtime_state_bundle as b
import native_restart_identity as identity

DB_ROLES = (*b.DB_ROLES, 'anchor', 'manifest')
# Same outer seal/account order as the runtime; all acquisitions are bounded.
LOCK_ORDER = ('legacy','guard','runner','decision','lifecycle','source','anchor','manifest')


class MigrationRefused(ValueError):
    pass


def require(ok, code='restart_migration_refused'):
    if not ok:
        raise MigrationRefused(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _binding(paths, test_only):
    if not test_only:
        require(paths is None, 'restart_migration_fixed_paths_required')
        legacy = b.FIXED_PATHS['legacy']
    else:
        legacy = paths['legacy']
    with b._database(legacy, b._private(legacy)) as db:
        rows = db.execute('SELECT fingerprint FROM paper_readonly WHERE id=1').fetchmany(2)
        require(len(rows) == 1, 'restart_migration_binding_invalid')
    return b.RuntimeStateBundle(rows[0][0], paths=paths, test_only=test_only)


def _raw_snapshot(bundle, current):
    result = {}
    for role, path in bundle.paths.items():
        maximum = os.stat(path, follow_symlinks=False).st_size
        require(0 <= maximum <= 128 * 1024 * 1024, 'restart_migration_snapshot_too_large')
        raw = identity.read_file(path, current[role], maximum=maximum, public_read=role=='anchor')
        info = os.stat(path, follow_symlinks=False)
        result[role] = dict(sha256=_sha(raw), size=len(raw), mode=info.st_mode & 0o777,
                            inode=current[role][1])
    return result


def _rows(db, *, without_migration=False):
    schema = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()
    if without_migration:
        schema = [r for r in schema if r[1] != identity.TABLE]
    result = []
    for kind, name, table, sql in schema:
        require(type(name) is str, 'restart_migration_schema_invalid')
        if kind == 'table':
            safe_name = '"' + name.replace('"', '""') + '"'
            values = db.execute('SELECT * FROM ' + safe_name).fetchall()
            # Exact rows, multiplicity and SQLite values; independent of row order.
            result.append((name, sorted(values, key=repr)))
    return schema, result


def _row_digests(dbs, *, without_migration=False):
    # repr is private and used only as canonical hashing of SQLite scalar rows;
    # neither its text nor any row values are included in a public report.
    return {role: _sha(repr(_rows(db, without_migration=without_migration)).encode())
            for role, db in dbs.items()}


@contextmanager
def _locked(bundle):
    current = {role: bundle._identity(role) for role in bundle.paths}
    fds, dbs = {}, {}
    read_guards = ExitStack()
    try:
        for role in ('seal','legacy_lock'):
            fd = os.open(bundle.paths[role], os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
            fds[role] = fd
            info = os.fstat(fd)
            require((info.st_dev, info.st_ino) == current[role], 'restart_migration_file_changed')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Reject every sidecar before opening ANY database writable. A read-only
        # transaction then holds a SQLite shared lock until our reserved writer
        # lock is acquired, closing the cold-check -> writable-open recovery gap.
        for role in LOCK_ORDER:
            path = bundle.paths[role]
            require(not any(os.path.lexists(path+s) for s in b._SIDECARS),
                    'restart_migration_sidecar_present')
            db = read_guards.enter_context(b._database(path,current[role],public_read=role=='anchor'))
            db.execute('SELECT COUNT(*) FROM sqlite_master').fetchone()
        for role in LOCK_ORDER:
            path = bundle.paths[role]
            require(bundle._identity(role) == current[role], 'restart_migration_file_changed')
            require(not any(os.path.lexists(path+s) for s in b._SIDECARS),
                    'restart_migration_sidecar_present')
            db = sqlite3.connect(Path(path).as_uri()+'?mode=rw', uri=True, timeout=.05, isolation_level=None)
            dbs[role] = db
            db.execute('PRAGMA trusted_schema=OFF'); db.execute('PRAGMA temp_store=MEMORY')
            db.execute('PRAGMA synchronous=FULL')
            deadline=time.monotonic()+60
            db.set_progress_handler(lambda deadline=deadline:int(time.monotonic()>deadline),1000)
            db.execute('BEGIN IMMEDIATE')
            require(bundle._identity(role) == current[role], 'restart_migration_file_changed')
        # All writer reservations now belong to us. No write/recovery occurred
        # while the temporary read-only locks existed.
        read_guards.close()
        yield current, fds, dbs
        require({role: bundle._identity(role) for role in bundle.paths} == current,
                'restart_migration_file_changed')
    finally:
        # ExitStack closes every registered resource even when one close or the
        # readonly post-check raises. Do not strand writer/file locks on refusal.
        with ExitStack() as cleanup:
            for fd in fds.values(): cleanup.callback(os.close, fd)
            for db in dbs.values(): cleanup.callback(db.close)
            cleanup.callback(read_guards.close)


def _legacy_inventory(bundle, current, dbs):
    require(not identity.table_present(dbs['anchor']), 'restart_migration_already_started')
    for role in ('legacy','anchor','manifest'):
        require((dbs[role].execute('PRAGMA application_id').fetchone()[0],
                 dbs[role].execute('PRAGMA user_version').fetchone()[0]) == (0,0),
                'restart_migration_header_slots_in_use')
    require(identity.read_file(bundle.paths['legacy_lock'],current['legacy_lock'],maximum=512)==b'',
            'restart_migration_lock_not_empty')
    seal = bundle._read_seal(current['seal'])
    manifest = dbs['manifest']
    require(manifest.execute('PRAGMA quick_check(1)').fetchall()==[('ok',)], 'restart_migration_manifest_invalid')
    require(manifest.execute("SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
            == [('table','bundle_binding'),('table','bundle_files')], 'restart_migration_manifest_invalid')
    binding = b._singleton(manifest,'bundle_binding')
    records = manifest.execute('SELECT role,path,device,inode,schema_hash FROM bundle_files ORDER BY role').fetchall()
    roles = tuple(sorted((*b.DB_ROLES,'legacy_lock','anchor')))
    require(len(binding)==7 and binding[:5]==(1,b.VERSION,b.GENERATION,bundle.binding,1)
            and tuple(r[0] for r in records)==roles, 'restart_migration_manifest_invalid')
    references = {r[0]: tuple(r[2:4]) for r in records}
    require(type(seal['manifest_identity']) is list and len(seal['manifest_identity'])==2,
            'restart_migration_manifest_invalid')
    references.update(seal=tuple(binding[5:7]),manifest=tuple(seal['manifest_identity']))
    require(set(references)==set(bundle.paths)
            and all(len(p)==2 and all(type(v) is int and v>=0 for v in p) for p in references.values()),
            'restart_migration_reference_invalid')
    require(all(current[role][1]==pair[1] for role,pair in references.items())
            and len({p[0] for p in references.values()})==1 and len({p[0] for p in current.values()})==1,
            'restart_migration_not_device_only')
    anchor = bundle._anchor_row(dbs['anchor'],migration_reference=references['anchor'])
    digest = b._digest([binding,records])
    require(anchor[8:]==('completed',*references['seal'],*references['manifest'],digest)
            and seal['manifest_digest']==digest, 'restart_migration_manifest_invalid')
    # Existing full logical/schema/stamp checks, using original references only
    # for comparison. This is preflight, NOT RuntimeStateBundle.verify success.
    require(records==bundle._inspect(current,stamped=True,references=references),
            'restart_migration_inventory_invalid')
    from native_account_guard import SharedAccountGuard
    guard = SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,
                               enabled=False,state_database=bundle.paths['guard'],test_only=True)
    previous_factory = dbs['legacy'].row_factory
    dbs['legacy'].row_factory = sqlite3.Row
    guard._source = dbs['legacy']
    try:
        revision = str(guard._stable_execution_revision())
    finally:
        dbs['legacy'].row_factory = previous_factory
        guard._source = None
    require(dbs['guard'].execute('SELECT legacy_revision FROM account_guard_binding').fetchall()==[(revision,)],
            'restart_migration_legacy_revision_changed')
    return dict(generation=b.GENERATION,binding=bundle.binding,
        files={role:dict(path=bundle.paths[role],device=p[0],inode=p[1]) for role,p in references.items()},
        manifest_digest=digest,history=list(anchor[6:8]),schemas={r[0]:r[4] for r in records})


@dataclass(frozen=True)
class Plan:
    """Private local object. Contains no raw database rows or credentials."""
    digest: str
    inventory: str
    snapshot: str
    rows: str
    # Observational pins, never part of portable operator approval. A fresh
    # apply session must reacquire its own physical identities and all locks.
    current: tuple


def _make_plan(bundle,current,dbs):
    inventory = _legacy_inventory(bundle,current,dbs)
    snapshot = _raw_snapshot(bundle,current)
    rows = _row_digests(dbs)
    payload = [identity.VERSION,inventory,snapshot,rows]
    return Plan(b._digest(payload),identity._json(inventory),identity._json(snapshot),
                identity._json(rows),tuple(sorted(current.items())))


def prepare(*, paths=None, test_only=False):
    require(type(test_only) is bool, 'restart_migration_arguments_invalid')
    bundle = _binding(paths,test_only)
    with _locked(bundle) as (current,_,dbs):
        return _make_plan(bundle,current,dbs)


def _backup(bundle,current,plan,destination):
    target = Path(destination)
    require(target.is_absolute() and '..' not in target.parts and str(target)==str(destination),
            'restart_migration_backup_invalid')
    b._parents(str(target/'placeholder'))
    directory = os.open(str(target),os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        info = os.fstat(directory)
        require(info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode) in (0o700,0o2700)
                and not os.listdir(directory), 'restart_migration_backup_invalid')
        pinned = (info.st_dev,info.st_ino)
        def check_directory():
            b._parents(str(target/'placeholder'))
            now=target.stat(follow_symlinks=False)
            require((now.st_dev,now.st_ino)==pinned and now.st_uid==os.geteuid()
                    and stat.S_IMODE(now.st_mode) in (0o700,0o2700),
                    'restart_migration_backup_changed')
        def write(name,raw):
            check_directory()
            fd=os.open(name,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory)
            try:
                b.RuntimeStateBundle._write(fd,raw)
                os.lseek(fd,0,os.SEEK_SET)
                digest=hashlib.sha256()
                while True:
                    chunk=os.read(fd,1024*1024)
                    if not chunk:break
                    digest.update(chunk)
                stored=os.fstat(fd);named=os.stat(name,dir_fd=directory,follow_symlinks=False)
                require(digest.hexdigest()==_sha(raw) and stored.st_size==len(raw)
                        and stored.st_uid==os.geteuid() and stored.st_mode&0o777==0o600
                        and stored.st_nlink==1 and (stored.st_dev,stored.st_ino)==(named.st_dev,named.st_ino),
                        'restart_migration_backup_changed')
            finally:os.close(fd)
        check_directory()
        snapshot = b._load(plan.snapshot)
        for role,path in bundle.paths.items():
            raw = identity.read_file(path,current[role],maximum=snapshot[role]['size'],public_read=role=='anchor')
            require(_sha(raw)==snapshot[role]['sha256'], 'restart_migration_preimage_changed')
            write(role+'.backup',raw)
        write('manifest.json',b._json(dict(plan_digest=plan.digest,snapshot=snapshot)))
        require(set(os.listdir(directory))=={*(r+'.backup' for r in bundle.paths),'manifest.json'},
                'restart_migration_backup_changed')
        os.fsync(directory);check_directory()
        parent=os.open(str(target.parent),os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(parent)
        finally:os.close(parent)
        check_directory()
    finally:os.close(directory)


def _sync(bundle,role,current):
    fd=os.open(bundle.paths[role],os.O_RDONLY|os.O_NOFOLLOW)
    try:
        info=os.fstat(fd)
        require((info.st_dev,info.st_ino)==current[role],'restart_migration_file_changed')
        os.fsync(fd)
    finally: os.close(fd)


def apply(*, enabled=False, plan=None, approved_digest=None, quiescence_confirmed=False,
          backup_directory=None, paths=None, test_only=False, _checkpoint=None):
    """No call site/startup enables this. Permission must be established externally.

    Checkpoint injection is synthetic-test only. Unknown/crash outcomes consume
    the prepared record and cannot be retried by calling this function again.
    """
    require(type(enabled) is bool and type(test_only) is bool and type(quiescence_confirmed) is bool
            and (_checkpoint is None or test_only), 'restart_migration_arguments_invalid')
    if not enabled:
        return dict(reason='restart_migration_disabled',state_mutated=False,provider_requests=0,execution_ready=False)
    require(type(plan) is Plan and type(approved_digest) is str and approved_digest==plan.digest
            and quiescence_confirmed and type(backup_directory) is str, 'restart_migration_approval_required')
    checkpoint = _checkpoint or (lambda phase:None)
    bundle = _binding(paths,test_only)
    with _locked(bundle) as (current,fds,dbs):
        observed = _make_plan(bundle,current,dbs)
        require(observed.digest==approved_digest and observed.inventory==plan.inventory
                and observed.snapshot==plan.snapshot and observed.rows==plan.rows,
                'restart_migration_preimage_changed')
        _backup(bundle,current,plan,backup_directory)
        checkpoint('backup_synced')
        cohort = None
        for _ in range(3):
            candidate = secrets.token_hex(32)
            stamps = {r:identity.stamp(candidate,r) for r in DB_ROLES}
            if len(set(stamps.values()))==len(DB_ROLES) and all(stamps[r]!=b._stamp(r) for r in DB_ROLES):
                cohort=candidate;break
        require(cohort is not None,'restart_migration_nonce_invalid')
        inventory = b._load(plan.inventory)
        contract=identity.contract_digest(cohort,inventory,plan.digest)
        record=(1,identity.VERSION,'prepared',cohort,plan.inventory,plan.digest,contract)
        anchor=dbs['anchor']
        anchor.execute(identity.SQL)
        anchor.execute('INSERT INTO '+identity.TABLE+' VALUES(?,?,?,?,?,?,?)',record)
        anchor.commit();_sync(bundle,'anchor',current);anchor.execute('BEGIN IMMEDIATE')
        checkpoint('prepared_synced')
        # Guard first: standalone quota users cannot accept a partial v2 cohort.
        for role in ('guard',*(r for r in DB_ROLES if r!='guard')):
            db=dbs[role]
            app,version=identity.stamp(cohort,role)
            db.execute('PRAGMA application_id='+str(app));db.execute('PRAGMA user_version='+str(version))
            db.commit();_sync(bundle,role,current);db.execute('BEGIN IMMEDIATE')
            require((db.execute('PRAGMA application_id').fetchone()[0],db.execute('PRAGMA user_version').fetchone()[0])
                    ==(app,version),'restart_migration_stamp_failed')
            checkpoint('stamp_'+role+'_synced')
        lock_bytes=identity._json(identity.file_marker(cohort,'legacy_lock',contract)).encode()
        b.RuntimeStateBundle._write(fds['legacy_lock'],lock_bytes)
        checkpoint('lock_synced')
        seal=b._load(identity.read_file(bundle.paths['seal'],current['seal']))
        seal['restart_identity']=identity.file_marker(cohort,'seal',contract)
        b.RuntimeStateBundle._write(fds['seal'],b._json(seal))
        checkpoint('seal_synced')
        require(_row_digests(dbs,without_migration=True)==b._load(plan.rows),'restart_migration_business_rows_changed')
        require({role:bundle._identity(role) for role in bundle.paths}==current,'restart_migration_file_changed')
        # Retain every reserved writer lock through publication. Read-only
        # verification can coexist with these unchanged BEGIN IMMEDIATE locks.
        adopted=identity.load(bundle,current,allow_prepared=True)
        require(adopted is not None and adopted.contract==contract,'restart_migration_verify_failed')
        checkpoint('before_complete')
        changed=anchor.execute('UPDATE '+identity.TABLE+' SET status=\'completed\' WHERE id=1 AND version=? AND status=\'prepared\' AND contract_digest=?',
                               (identity.VERSION,contract)).rowcount
        require(changed==1,'restart_migration_compare_and_set_failed')
        anchor.commit();_sync(bundle,'anchor',current)
        checkpoint('complete_synced')
    require(bundle.verify(),'restart_migration_verify_failed')
    return dict(reason='restart_identity_migrated',state_mutated=True,provider_requests=0,
                business_rows_preserved=True,execution_ready=False,order_permission='none',
                historical_continuity_proven=False)
