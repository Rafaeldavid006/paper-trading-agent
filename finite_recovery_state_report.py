"""Read-only existing recovery state; no grants, providers, enrollment or reset."""
from contextlib import ExitStack, contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import finite_pilot_contract as contract
from finite_pilot_anchor import AnchorJournal, _bounded
import runtime_state_bundle as inventory

EXPECTED_SEAL = '4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6'
EXPECTED_RUNNER = '745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f'
EXPECTED_PROTECTED = '64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282'
EXPECTED_COHORT = '8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753'
EXPECTED_RECOVERY_DIGEST = '02f3bf75c43620512b56e4eaa2b7e3ff46a3dd9e1ffa4de42b11cd0162f42433'
MAX_SECONDS = 8


def _require(value):
    if not value:raise ValueError('report_refused')


@contextmanager
def _readonly_lock(path):
    identity=inventory._private(path)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd)
        _require((info.st_dev,info.st_ino)==identity)
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        yield
        _require(inventory._private(path)==identity)
    finally:os.close(fd)


def _stamp(paths):
    result={}
    for role,path in paths.items():
        inventory._private(path,public_read=role=='anchor')
        for suffix in ('-journal','-wal','-shm'):
            _require(not os.path.lexists(path+suffix))
        s=os.stat(path,follow_symlinks=False)
        result[role]=(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
    return result



def current_anchor_digest(db):
    """Versioned logical snapshot of every schema object and table row.

    Caller owns a pinned read transaction and verifies file stamps. No writes,
    row omission, account output, or old founding-file-hash substitution.
    Rowids are evidence where present; WITHOUT ROWID tables use stored columns.
    Explicit value tags preserve SQLite NULL/integer/real/text/blob distinctions.
    """
    objects=_bounded(db,"SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name",limit=256)
    _require(all(r[0] in ('table','index') and type(r[1]) is str
                 and (r[3] is None or type(r[3]) is str) for r in objects))
    _require(all(r[3] is not None and not r[3].lstrip().upper().startswith('CREATE VIRTUAL')
                 for r in objects if r[0]=='table'))
    table_flags=db.execute('PRAGMA main.table_list').fetchmany(257)
    _require(table_flags and len(table_flags)<=256
             and all(len(r)>=6 and r[0]=='main' and type(r[1]) is str
                     and r[4] in (0,1) for r in table_flags))
    flags={r[1]:r[4] for r in table_flags}
    _require(len(flags)==len(table_flags))
    remaining_rows=20000
    remaining_bytes=16*1024*1024
    h=hashlib.sha256()
    def typed(value):
        if value is None:return ['null']
        if type(value) is int:return ['integer',str(value)]
        if type(value) is float:
            _require(math.isfinite(value))
            return ['real',value.hex()]
        if type(value) is str:return ['text',value]
        if type(value) is bytes:return ['blob',value.hex()]
        raise ValueError('report_refused')
    def encoded(value):
        return json.dumps(value,ensure_ascii=True,separators=(',',':'),allow_nan=False).encode('ascii')
    def add(value):
        nonlocal remaining_bytes
        raw=encoded(value);remaining_bytes-=len(raw)
        _require(remaining_bytes>=0)
        h.update(str(len(raw)).encode('ascii')+b':'+raw)
    add('anchor-logical-v1')
    add([[typed(v) for v in r] for r in objects])
    for kind,name,_,sql in objects:
        if kind!='table':continue
        quoted='"'+name.replace('"','""')+'"'
        columns=db.execute('PRAGMA table_xinfo('+quoted+')').fetchall()
        _require(columns and len(columns)<=128)
        names={r[1].lower() for r in columns}
        _require(name in flags)
        without_rowid=bool(flags[name])
        aliases=[v for v in ('_rowid_','rowid','oid') if v not in names]
        _require(without_rowid or bool(aliases))
        selector='*' if without_rowid else aliases[0]+',*'
        rows=_bounded(db,'SELECT '+selector+' FROM '+quoted,limit=remaining_rows)
        remaining_rows-=len(rows)
        # Sort canonical encoded rows, not SQLite collation or query-plan order.
        ordered=sorted((encoded([typed(v) for v in row]) for row in rows))
        add([name,'without-rowid' if without_rowid else 'with-rowid',len(ordered)])
        for row in ordered:
            remaining_bytes-=len(row);_require(remaining_bytes>=0)
            h.update(str(len(row)).encode('ascii')+b':'+row)
    return h.hexdigest()


def report(*,paths=None,test_only=False,expected=None):
    _require(contract.APPROVED_PILOT is None and contract.APPROVED_RECOVERY is None
             and contract.APPROVED_INSPECTION is None)
    _require(type(test_only) is bool and (test_only or (paths is None and expected is None)))
    expected=expected or dict(seal=EXPECTED_SEAL,runner=EXPECTED_RUNNER,
                             protected=EXPECTED_PROTECTED,cohort=EXPECTED_COHORT)
    _require(set(expected)=={'seal','runner','protected','cohort'}
             and all(contract._hash(v) for v in expected.values()))
    started=time.monotonic()
    resolved=inventory.FIXED_PATHS if paths is None else paths
    seal_path=resolved['seal']
    with _readonly_lock(seal_path):
        fd=os.open(seal_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:raw=os.read(fd,4097)
        finally:os.close(fd)
        _require(len(raw)<=4096 and hashlib.sha256(raw).hexdigest()==expected['seal'])
        seal=inventory._load(raw)
        _require(seal['generation']==inventory.GENERATION and contract._hash(seal['fingerprint']))
        # Production constructor resolves actual anchor solely from existing EVENT_DB.
        bundle=inventory.RuntimeStateBundle(seal['fingerprint'],paths=paths,test_only=test_only)
        with _readonly_lock(bundle.paths['legacy_lock']):
            before=_stamp(bundle.paths)
            bundle.verify()
            journal=AnchorJournal(bundle);journal._verify()
            with ExitStack() as stack:
                dbs={role:stack.enter_context(inventory._database(bundle.paths[role],bundle._pinned[role],
                     public_read=role=='anchor')) for role in (*inventory.DB_ROLES,'anchor','manifest')}
                db=dbs['anchor'];journal._binding_row(db)
                binding=_bounded(db,'SELECT * FROM finite_pilot_binding',limit=1)[0]
                enrolled=_bounded(db,'SELECT * FROM finite_pilot_recovery',limit=1)
                rows=_bounded(db,'SELECT * FROM finite_pilot_recovery_journal ORDER BY seq',limit=2)
                state='not_enrolled' if not enrolled else 'enrollment_only' if not rows else 'unvalidated_journal'
                receipt_validated=False
                if rows:
                    _require(enrolled and all(r[1]==contract.INCIDENT_ID and r[4]==enrolled[0][1] for r in rows))
                    if len(rows)==1 and rows[0][0]==1 and rows[0][2]=='prepared':state='prepared'
                    elif len(rows)==2 and rows[0][0]==1 and rows[0][2]=='prepared' and rows[1][0]==2 and rows[1][2]=='applied':state='applied_recorded'
                    else:raise ValueError('report_refused')
                    doc=json.loads(enrolled[0][2])
                    archived=contract.RecoveryManifest(**doc)  # Validation only; never installed as authority.
                    old=json.loads(rows[0][5]);new=json.loads(rows[0][6])
                    _require(contract.digest(old)==archived.original_runner_sha256
                             and len(old)==6 and old[0]==1 and old[2:]==[1,0,'readonly_runtime_stage_failed','started']
                             and new==[1,rows[0][3]/contract.NS,0,0,None,'recovered_readonly']
                             and new[1]>=old[1])
                    for record in rows:
                        _require(record[1]==archived.incident_id and record[4]==contract.digest(doc)
                                 and record[5:8]==rows[0][5:8]
                                 and record[7]==archived.protected_state_sha256
                                 and contract._hash(record[8])
                                 and type(record[3]) is int
                                 and archived.starts_ns<=record[3]<archived.expires_ns)
                    _require(rows[0][9] is None)
                    if len(rows)==2:
                        _require(rows[1][3]>=rows[0][3] and contract._hash(rows[1][9]))
                        receipt_validated=True
                counts={k:len(_bounded(db,'SELECT * FROM '+table,limit=cap)) for k,table,cap in (
                    ('pilot_enrollment','finite_pilot_enrollment',1),('pilot_control','finite_pilot_control',1),
                    ('pilot_stages','finite_pilot_stages',200),('pilot_writes','finite_pilot_writes',8),
                    ('pilot_sources','finite_pilot_sources',120))}
                runner=inventory._singleton(dbs['runner'],'runner_control')
                digests=dict(runner=contract.digest(runner),protected=journal.protected_state_digest(),
                             cohort=journal.cohort_state_digest(),seal=journal._seal_digest())
                result=dict(mode='finite_recovery_readonly_report',status='observed',
                    recovery_state=state,recovery_enrollment_count=len(enrolled),recovery_journal_count=len(rows),
                    enrolled_recovery_digest=binding[12],enrollment_document_digest=(enrolled[0][1] if enrolled else None),
                    current_anchor_logical_sha256=current_anchor_digest(db),anchor_digest_format='anchor-logical-v1',
                    counts=counts,current_digests=digests,proof_matches={k:digests[k]==expected[k] for k in digests},
                    enrolled_matches_archived_manifest=(binding[12]==EXPECTED_RECOVERY_DIGEST),
                    runner_matches_prepared_old=(contract.digest(runner)==contract.digest(json.loads(rows[0][5])) if rows else None),
                    runner_matches_prepared_new=(contract.digest(runner)==contract.digest(json.loads(rows[0][6])) if rows else None),
                    runner_original_incident=(tuple(runner[2:])==(1,0,'readonly_runtime_stage_failed','started')),
                    broker_state_verified=False,provider_requests=0,state_writes=0,order_permission='none',
                    activation_authorized=False,applied_receipt_structurally_validated=receipt_validated)
                bundle.verify();journal._binding_row(db)
                _require(_stamp(bundle.paths)==before)
            _require(0<=time.monotonic()-started<MAX_SECONDS)
    _require(len(json.dumps(result))<4096)
    return result


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if args!=['--read-existing-state']:return 2
    try:result=report();code=0
    except Exception:
        result=dict(mode='finite_recovery_readonly_report',status='refused',provider_requests=0,
                    state_writes=0,order_permission='none',activation_authorized=False);code=1
    print(json.dumps(result,sort_keys=True),flush=True)
    return code

if __name__=='__main__':raise SystemExit(main())
