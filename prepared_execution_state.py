"""Read-only predecessor/receipt binding; no archive editing or state adoption."""
from pathlib import Path
import fcntl,hashlib,json,os,sqlite3,stat
import prepared_execution_contract as c
import prepared_successor_contract as ac
from prepared_successor_journal import Journal as AdmissionJournal
import runtime_state_bundle as inventory
from finite_pilot_anchor import AnchorJournal
from finite_recovery_state_report import current_anchor_digest
from finite_root_contract import from_document
from finite_root_journal import load,clean_runner

def file_hash(path,identity=None):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        s=os.fstat(fd);c.require(stat.S_ISREG(s.st_mode) and s.st_size<=16*1024*1024 and s.st_nlink==1 and (identity is None or (s.st_dev,s.st_ino)==identity),'execution_file_changed')
        data=b''
        while len(data)<=16*1024*1024:
            part=os.read(fd,65536)
            if not part:break
            data+=part
        c.require(len(data)==s.st_size and (os.stat(path,follow_symlinks=False).st_dev,os.stat(path,follow_symlinks=False).st_ino)==(s.st_dev,s.st_ino),'execution_file_changed')
        return hashlib.sha256(data).hexdigest()
    finally:os.close(fd)

class Predecessor:
    def __init__(self,grant,*,paths=None,test_only=False,now_ns):
        c.approved(grant);self.grant=grant;self.admission=c.admission(grant);self.receipt_fd=None
        seal_path=(inventory.FIXED_PATHS if paths is None else paths)['seal'];identity=inventory._private(seal_path)
        c.require(file_hash(seal_path,identity)==self.admission.seal_sha256,'execution_seal_changed')
        fd=os.open(seal_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:seal=inventory._load(os.read(fd,4097))
        finally:os.close(fd)
        self.bundle=inventory.RuntimeStateBundle(seal['fingerprint'],paths=paths,test_only=test_only);self.bundle.verify()
        c.require(self.bundle._pinned['seal']==identity,'execution_seal_changed')
        self.anchor=AnchorJournal(self.bundle,now_ns);self.anchor._verify()
        self.receipt_path=str(Path(seal_path).parent/ac.JOURNAL_NAME)
        c.require(test_only or self.receipt_path==ac.JOURNAL_PATH,'execution_receipt_path')
        self.frozen=None
    def open_receipt(self):
        p=self.receipt_path
        c.require(not any(os.path.lexists(p+x) for x in ('-journal','-wal','-shm')),'execution_receipt_sidecar')
        self.receipt_fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        fcntl.flock(self.receipt_fd,fcntl.LOCK_SH|fcntl.LOCK_NB)
        s=os.fstat(self.receipt_fd);self.receipt_identity=(s.st_dev,s.st_ino)
        c.require(stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==os.geteuid() and s.st_nlink==1,'execution_receipt_private')
        c.require(file_hash(p,self.receipt_identity)==self.grant.admission_journal_sha256,'execution_receipt_changed')
        db=sqlite3.connect(Path(p).as_uri()+'?mode=ro',uri=True,timeout=.05)
        try:
            db.execute('PRAGMA query_only=ON');db.execute('PRAGMA trusted_schema=OFF');db.execute('BEGIN')
            rows=db.execute('SELECT * FROM admission_events ORDER BY seq').fetchmany(16)
            c.require(bool(rows) and len(rows)<=14,'execution_receipt_incomplete')
            # Historical validation only: do not reactivate admission or reinterpret its code pin.
            check=AdmissionJournal.__new__(AdmissionJournal)
            check.db=db;check.grant=self.admission;check.sequence=len(rows);check.previous=rows[-1][-1]
            check.gets=sum(r[1]=='GET' for r in rows);check.applied=True
            check.verify()
            c.require(rows[-1][1]=='APPLIED' and rows[-1][2]<=self.grant.starts_ns,'execution_receipt_incomplete')
            self.admission_gets=check.gets
        finally:db.close()
    def initial(self):
        b,j,a=self.bundle,self.anchor,self.admission
        j._verify();j._controls()
        with j._db() as db:
            c.require(current_anchor_digest(db)==a.anchor_sha256,'execution_anchor_changed')
            root=from_document(a.predecessor_document);_,rows,counts,events=load(db,root)
        c.require(len(rows)==1 and root.children[0].pilot_id in rows,'execution_exact_predecessor')
        r=rows[root.children[0].pilot_id]
        c.require(r['prepared'] is not None and r['applied'] is None and r['ended'] is None and not r['halted'] and not r['dirty'] and r['smoke_entry'] is None and counts['ticks']==counts['writes']==counts['sources']==0 and all(e[3] in ('ROOT_IMPORTED','PREPARED','BROKER') for e in events),'execution_exact_prepared')
        with inventory._database(b.paths['runner'],b._pinned['runner']) as db:runner=clean_runner(inventory._singleton(db,'runner_control'))
        c.require(tuple(r['prepared']['old'])==runner and tuple(r['prepared']['new'])!=runner and c.digest(runner)==a.runner_sha256,'execution_runner_not_old')
        with inventory._database(b.paths['lifecycle'],b._pinned['lifecycle']) as db:
            c.require(all(db.execute('SELECT COUNT(*) FROM '+t).fetchone()==(0,) for t in ('intents','cancels','native_dispatch')),'execution_native_history_present')
        c.require(j.protected_state_digest()==a.protected_sha256 and j.cohort_state_digest()==self.grant.current_cohort_sha256,'execution_current_state_changed')
        c.require(counts['broker']+self.admission_gets+self.grant.max_broker<=root.max_broker,'execution_inherited_budget_exceeded')
        self.prior_counts=counts;self.frozen={r:file_hash(p,b._pinned[r]) for r,p in b.paths.items() if r not in ('lifecycle','guard','legacy_lock')}
    def check(self):
        c.approved(self.grant);self.anchor._verify();self.anchor._controls()
        c.require(self.frozen is not None and all(file_hash(self.bundle.paths[r],self.bundle._pinned[r])==v for r,v in self.frozen.items()),'execution_predecessor_changed')
        c.require(file_hash(self.receipt_path,self.receipt_identity)==self.grant.admission_journal_sha256,'execution_receipt_changed')
    def close(self):
        if self.receipt_fd is not None:os.close(self.receipt_fd);self.receipt_fd=None
