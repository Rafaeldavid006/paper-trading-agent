"""Bounded local receipt projection; no authority, broker IO or state writes.

Only hashes, validated counters and categorical state are emitted. Receipt
flatness is explicitly historical; report success never reactivates a grant.
Caller owns the existing seal/legacy locks and runtime bundle read snapshot.
"""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import fcntl, json, os, sqlite3, stat
from finite_pilot_contract import require, digest
from prepared_execution_state import file_hash
from prepared_successor_contract import AdmissionGrant, JOURNAL_NAME as ADMISSION
from prepared_execution_contract import ExecutionGrant, JOURNAL_NAME as EXECUTION
from prepared_successor_journal import Journal as AdmissionJournal
from prepared_execution_journal import Journal as ExecutionJournal
from finite_root_contract import from_document
from finite_root_journal import load

@contextmanager
def receipt(path):
    require(not any(os.path.lexists(str(path)+x) for x in ('-journal','-wal','-shm')),'receipt_sidecar')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);db=None
    try:
        fcntl.flock(fd,fcntl.LOCK_SH|fcntl.LOCK_NB)
        s=os.fstat(fd);identity=(s.st_dev,s.st_ino)
        require(stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==os.geteuid() and s.st_nlink==1,'receipt_private')
        before=file_hash(str(path),identity)
        db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.05)
        db.execute('PRAGMA query_only=ON');db.execute('PRAGMA trusted_schema=OFF');db.execute('BEGIN')
        yield db,before
        require(file_hash(str(path),identity)==before and not any(os.path.lexists(str(path)+x) for x in ('-journal','-wal','-shm')),'receipt_changed')
    finally:
        if db is not None:db.close()
        os.close(fd)

def _predecessor(anchor_db,grant):
    root=from_document(grant.predecessor_document)
    _,rows,counts,events=load(anchor_db,root)
    require(len(rows)==1 and root.children[0].pilot_id in rows,'receipt_predecessor')
    row=rows[root.children[0].pilot_id]
    require(row['prepared'] is not None and row['applied'] is None and row['ended'] is None and not row['halted'] and not row['dirty'] and row['smoke_entry'] is None and counts['ticks']==counts['writes']==counts['sources']==0 and all(e[3] in ('ROOT_IMPORTED','PREPARED','BROKER') for e in events),'receipt_predecessor')
    from finite_recovery_state_report import current_anchor_digest
    require(current_anchor_digest(anchor_db)==grant.anchor_sha256,'receipt_anchor_changed')
    return counts

def _admission(path,anchor_db):
    with receipt(path) as (db,sha):
        rows=db.execute('SELECT * FROM admission_events ORDER BY seq').fetchmany(16)
        require(bool(rows) and len(rows)<=14,'receipt_incomplete')
        payload=json.loads(rows[0][3]);grant=AdmissionGrant(**payload['grant'])
        prior=_predecessor(anchor_db,grant)
        check=AdmissionJournal.__new__(AdmissionJournal)
        check.db=db;check.grant=grant;check.sequence=len(rows);check.previous=rows[-1][-1]
        check.gets=sum(r[1]=='GET' for r in rows);check.applied=rows[-1][1]=='APPLIED';check.verify()
        result=dict(state='applied_readonly' if check.applied else 'consumed_incomplete',journal_sha256=sha,grant_sha256=digest(grant.document()),requests=check.gets,applied=check.applied,receipt_observed_ns=json.loads(rows[-1][3])['observed_ns'] if check.applied else None)
        return result,grant,prior

def _execution(path,anchor_db,admission):
    require(admission is not None and admission[0]['applied'],'receipt_admission_required')
    with receipt(path) as (db,sha):
        rows=db.execute('SELECT * FROM execution_events ORDER BY seq').fetchmany(256)
        require(bool(rows) and len(rows)<256,'receipt_incomplete')
        payload=json.loads(rows[0][3]);grant=ExecutionGrant(**payload['grant'])
        a,ag,prior=admission
        require(grant.admission_document==json.dumps(ag.document(),sort_keys=True,separators=(',',':')) and grant.admission_journal_sha256==a['journal_sha256'],'receipt_binding')
        require(prior['broker']+a['requests']+grant.max_broker<=from_document(ag.predecessor_document).max_broker,'receipt_budget')
        check=ExecutionJournal.__new__(ExecutionJournal)
        check.db=db;check.grant=grant;check.sequence=len(rows);check.previous=rows[-1][-1]
        check.pre=SimpleNamespace(prior_counts=prior,admission_gets=a['requests'])
        st=check.state();terminal=json.loads(rows[-1][3]) if st['terminal'] else None
        return dict(state=('terminal_flat_historical' if terminal['flat'] else 'manual_handoff') if terminal else 'consumed_incomplete',journal_sha256=sha,requests=st['requests'],writes=st['writes'],ticks=st['ticks'],dirty=st['dirty'],terminal=st['terminal'],receipt_observed_ns=terminal['observed_ns'] if terminal else None,inherited_requests=prior['broker']+a['requests']+st['requests'])

def project(bundle,anchor_db):
    parent=Path(bundle.paths['seal']).parent
    result=dict(prepared_admission=dict(state='absent'),prepared_execution=dict(state='absent'),prepared_receipts_are_historical=True,prepared_receipts_authorize_execution=False)
    admission=None
    for name,filename in (('prepared_admission',ADMISSION),('prepared_execution',EXECUTION)):
        path=parent/filename
        if not any(os.path.lexists(str(path)+x) for x in ('','-journal','-wal','-shm')):continue
        try:
            if name=='prepared_admission':
                admission=_admission(path,anchor_db);result[name]=admission[0]
            else:result[name]=_execution(path,anchor_db,admission)
        except Exception:
            # Even a partial/invalid or locked journal is consumed, never absent.
            result[name]=dict(state='consumed_unvalidated')
    return result
