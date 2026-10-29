"""Separate append-only attempt ledger. Presence consumes it even before SQL exists."""
from contextlib import contextmanager
from pathlib import Path
import fcntl,json,os,sqlite3,stat
import prepared_successor_contract as c
from finite_pilot_anchor import _json
SQL='CREATE TABLE admission_events(seq INTEGER PRIMARY KEY,kind TEXT NOT NULL,at_ns INTEGER NOT NULL,payload TEXT NOT NULL,previous TEXT NOT NULL,sha256 TEXT NOT NULL)'

class Journal:
    def __init__(self,path,grant,now_ns,seal,checkpoint=lambda _:None):
        c.approved(grant);self.grant,self.path,self.now,self.checkpoint=grant,str(path),now_ns,checkpoint
        self.fd=self.db=None;self.previous='0'*64;self.sequence=0;self.gets=0;self.applied=False
        parent=Path(path).parent
        c.require(parent==Path(seal).parent and parent.resolve()==parent,'successor_journal_directory')
        directory=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            c.require(not any(os.path.lexists(self.path+x) for x in ('','-journal','-wal','-shm')),'successor_attempt_consumed')
            self.fd=os.open(Path(path).name,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600,dir_fd=directory)
            fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            info=os.fstat(self.fd);self.identity=(info.st_dev,info.st_ino);os.fsync(directory)
            self.checkpoint('file_claimed')
            self.db=sqlite3.connect(Path(path).as_uri()+'?mode=rw',uri=True,timeout=0,isolation_level=None)
            self.db.execute('PRAGMA trusted_schema=OFF');self.db.execute('PRAGMA synchronous=FULL')
            c.require(self.db.execute('PRAGMA journal_mode').fetchone()==('delete',),'successor_journal_mode')
            self.last=c.window(grant,now_ns())
            with self.tx():
                self.db.execute(SQL)
                self._append('PREPARED',dict(grant=grant.document(),digest=c.digest(grant.document())))
            self.checkpoint('prepared_committed')
        except BaseException:
            self.close();raise
        finally:os.close(directory)
    def check(self):
        info=os.stat(self.path,follow_symlinks=False)
        c.require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode)==0o600 and info.st_uid==os.geteuid() and info.st_nlink==1 and (info.st_dev,info.st_ino)==self.identity,'successor_journal_replaced')
        self.last=c.window(self.grant,self.now(),getattr(self,'last',0))
    @contextmanager
    def tx(self):
        self.check();self.db.execute('BEGIN IMMEDIATE')
        try:yield;self.check();self.db.commit()
        except BaseException:self.db.rollback();raise
    def _append(self,kind,payload):
        raw=_json(payload);at=self.last;seq=self.sequence+1
        sha=c.digest((seq,kind,at,raw,self.previous))
        self.db.execute('INSERT INTO admission_events VALUES(?,?,?,?,?,?)',(seq,kind,at,raw,self.previous,sha))
        self.sequence=seq;self.previous=sha
    def verify(self):
        objects=self.db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchall()
        c.require(objects==[('table','admission_events','admission_events',SQL)],'successor_ledger_schema')
        rows=self.db.execute('SELECT * FROM admission_events ORDER BY seq').fetchmany(16)
        c.require(len(rows)==self.sequence and len(rows)<=14,'successor_ledger_changed')
        prior='0'*64;previous_time=0;gets=0;applied=False
        for index,(seq,kind,at,raw,parent,sha) in enumerate(rows,1):
            p=json.loads(raw)
            c.require(type(p) is dict and _json(p)==raw and seq==index and type(at) is int and self.grant.starts_ns<=at<self.grant.expires_ns and at>=previous_time and parent==prior and sha==c.digest((seq,kind,at,raw,parent)),'successor_ledger_changed')
            if seq==1:c.require(kind=='PREPARED' and p==dict(grant=self.grant.document(),digest=c.digest(self.grant.document())),'successor_prepared_changed')
            elif kind=='GET':
                gets+=1;c.require(not applied and type(p.get('request')) is int and p['request']==gets and set(p)=={'request','target_sha256'} and c._hash(p['target_sha256']),'successor_get_changed')
            elif kind=='APPLIED':
                c.require(not applied and 1<=gets<=12 and seq==len(rows) and set(p)=={'evidence_sha256','guard_sha256','observed_ns','requests','execution_permission'} and type(p['requests']) is int and p['requests']==gets and p['execution_permission']=='none' and c._hash(p['evidence_sha256']) and c._hash(p['guard_sha256']) and type(p['observed_ns']) is int and self.grant.starts_ns<=p['observed_ns']<=at and at-p['observed_ns']<=10*c.NS,'successor_applied_changed');applied=True
            else:c.require(False,'successor_event_invalid')
            prior=sha;previous_time=at
        c.require(prior==self.previous and gets==self.gets and applied==self.applied,'successor_ledger_changed')
        return rows
    def reserve(self,path):
        import hashlib
        self.check()
        with self.tx():
            self.verify();c.require(not self.applied and self.gets<self.grant.max_gets,'successor_get_cap')
            self._append('GET',dict(request=self.gets+1,target_sha256=hashlib.sha256(path.encode()).hexdigest()));self.gets+=1
            self.verify()
        self.checkpoint('request_reserved')
    def complete(self,evidence,guard_sha256):
        self.check()
        with self.tx():
            self.verify();c.require(not self.applied and evidence.request_count==self.gets,'successor_evidence_count')
            self._append('APPLIED',dict(evidence_sha256=evidence.observation.native_digest,guard_sha256=guard_sha256,observed_ns=evidence.observation.observed_ns,requests=self.gets,execution_permission='none'))
            self.applied=True;self.verify();self.checkpoint('applied_before_commit')
        self.checkpoint('applied_committed')
    def close(self):
        if self.db is not None:self.db.close();self.db=None
        if self.fd is not None:os.close(self.fd);self.fd=None
