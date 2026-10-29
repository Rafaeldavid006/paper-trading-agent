"""Fixed-name, append-only execution owner. Any existence consumes this attempt."""
from contextlib import contextmanager
from pathlib import Path
import fcntl,json,os,sqlite3,stat
import prepared_execution_contract as c
SQL='CREATE TABLE execution_events(seq INTEGER PRIMARY KEY,kind TEXT NOT NULL,at_ns INTEGER NOT NULL,payload TEXT NOT NULL,previous TEXT NOT NULL,sha256 TEXT NOT NULL)'
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
class Journal:
    def __init__(self,path,grant,predecessor,now_ns,checkpoint):
        self.grant,self.path,self.pre,self.now,self.checkpoint=grant,str(path),predecessor,now_ns,checkpoint
        self.fd=self.db=None;self.sequence=0;self.previous='0'*64;self.last=0
        parent=Path(path).parent
        c.require(parent==Path(predecessor.bundle.paths['seal']).parent and parent.resolve()==parent,'execution_journal_directory')
        directory=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            c.require(not any(os.path.lexists(self.path+x) for x in ('','-journal','-wal','-shm')),'execution_attempt_consumed')
            self.fd=os.open(Path(path).name,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600,dir_fd=directory)
            fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            s=os.fstat(self.fd);self.identity=(s.st_dev,s.st_ino);os.fsync(directory);checkpoint('execution_file_claimed')
            self.db=sqlite3.connect(Path(path).as_uri()+'?mode=rw',uri=True,timeout=0,isolation_level=None)
            self.db.execute('PRAGMA trusted_schema=OFF');self.db.execute('PRAGMA synchronous=FULL')
            c.require(self.db.execute('PRAGMA journal_mode').fetchone()==('delete',),'execution_journal_mode')
            with self.tx():
                self.db.execute(SQL)
                self._append('PREPARED',dict(grant=grant.document(),predecessor_counts=predecessor.prior_counts,admission_gets=predecessor.admission_gets))
            checkpoint('execution_prepared')
        except BaseException:self.close();raise
        finally:os.close(directory)
    def check(self):
        s=os.stat(self.path,follow_symlinks=False)
        c.require(stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==os.geteuid() and s.st_nlink==1 and (s.st_dev,s.st_ino)==self.identity,'execution_journal_replaced')
        self.pre.check();self.last=c.window(self.grant,self.now(),self.last)
    @contextmanager
    def tx(self):
        self.check();self.db.execute('BEGIN IMMEDIATE')
        try:yield;self.check();self.db.commit()
        except BaseException:self.db.rollback();raise
    def _append(self,kind,payload):
        raw=canonical(payload);seq=self.sequence+1;sha=c.digest((seq,kind,self.last,raw,self.previous))
        self.db.execute('INSERT INTO execution_events VALUES(?,?,?,?,?,?)',(seq,kind,self.last,raw,self.previous,sha));self.sequence=seq;self.previous=sha
    def state(self):
        objects=self.db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchall()
        c.require(objects==[('table','execution_events','execution_events',SQL)],'execution_schema_changed')
        rows=self.db.execute('SELECT * FROM execution_events ORDER BY seq').fetchmany(256)
        c.require(len(rows)==self.sequence and len(rows)<256,'execution_ledger_changed')
        st=dict(requests=0,writes=0,ticks=0,admission_gets=0,applied=False,dirty=False,entry=False,terminal=False,last_tick=0,stage_start=0)
        prior='0'*64;last=0;written=set()
        for index,(seq,kind,at,raw,parent,sha) in enumerate(rows,1):
            p=json.loads(raw)
            c.require(type(p) is dict and canonical(p)==raw and seq==index and type(at) is int and self.grant.starts_ns<=at<self.grant.expires_ns and at>=last and parent==prior and sha==c.digest((seq,kind,at,raw,parent)) and not st['terminal'],'execution_ledger_changed')
            if seq==1:
                c.require(kind=='PREPARED' and p==dict(grant=self.grant.document(),predecessor_counts=self.pre.prior_counts,admission_gets=self.pre.admission_gets),'execution_prepared_changed')
            elif kind=='REQUEST':
                c.require(set(p)=={'method','target_sha256'} and p['method'] in ('GET','POST','DELETE') and c._hash(p['target_sha256']),'execution_request_invalid')
                st['requests']+=1
                if not st['applied']:
                    st['admission_gets']+=1;c.require(p['method']=='GET' and st['admission_gets']<=12 and at<self.grant.admission_expires_ns,'execution_admission_readonly')
                else:c.require(st['dirty'] and at-st['stage_start']<=15*c.NS,'execution_request_inactive')
                if p['method']!='GET':
                    key=(p['method'],p['target_sha256']);c.require(st['entry'] and key not in written,'execution_write_replay');written.add(key);st['writes']+=1
            elif kind=='APPLIED':
                c.require(not st['applied'] and 1<=st['admission_gets']<=12 and at<self.grant.admission_expires_ns and set(p)=={'observed_ns','evidence_sha256','guard_sha256'} and all(c._hash(p[k]) for k in ('evidence_sha256','guard_sha256')) and type(p['observed_ns']) is int and self.grant.starts_ns<=p['observed_ns']<=at and at-p['observed_ns']<=10*c.NS,'execution_applied_invalid');st['applied']=True
            elif kind=='TICK_BEGIN':
                c.require(not p and st['applied'] and not st['dirty'] and (not st['last_tick'] or at-st['last_tick']>=self.grant.cadence_ns),'execution_tick_invalid')
                st['ticks']+=1;st['dirty']=True;st['last_tick']=st['stage_start']=at
            elif kind=='ENTRY':
                c.require(not p and st['applied'] and st['dirty'] and not st['entry'],'execution_entry_consumed');st['entry']=True
            elif kind=='TICK_END':
                c.require(not p and st['dirty'] and 0<=at-st['stage_start']<=15*c.NS,'execution_tick_end_invalid');st['dirty']=False
            elif kind=='TERMINAL':
                c.require(not st['dirty'] and st['applied'] and set(p)=={'flat','observed_ns','evidence_sha256'} and type(p['flat']) is bool and c._hash(p['evidence_sha256']) and type(p['observed_ns']) is int and self.grant.starts_ns<=p['observed_ns']<=at and at-p['observed_ns']<=10*c.NS,'execution_terminal_invalid');st['terminal']=True
            else:c.require(False,'execution_event_invalid')
            c.require(st['requests']<=self.grant.max_broker and st['writes']<=self.grant.max_writes and st['ticks']<=self.grant.max_ticks,'execution_budget_exhausted')
            prior=sha;last=at
        c.require(prior==self.previous,'execution_ledger_changed');return st
    def record(self,kind,payload):
        with self.tx():
            self.state();self._append(kind,payload);st=self.state();self.checkpoint('execution_'+kind.lower()+'_precommit')
        self.checkpoint('execution_'+kind.lower()+'_committed');return st
    def reserve(self,method,target):
        return self.record('REQUEST',dict(method=method,target_sha256=__import__('hashlib').sha256(target.encode()).hexdigest()))
    def active(self,*,write=False):
        self.check();st=self.state();c.require(st['applied'] and st['dirty'] and not st['terminal'] and self.last-st['stage_start']<=15*c.NS,'execution_inactive')
        if write:c.require(st['entry'],'execution_entry_required')
        return st
    def close(self):
        if self.db is not None:self.db.close();self.db=None
        if self.fd is not None:os.close(self.fd);self.fd=None
