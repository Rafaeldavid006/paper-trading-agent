"""Standalone one-process GET observer. No serve/startup integration.

Only observation journal and existing guard budget/audit/reconciliation may change.
Any existing journal consumes this proposal, even an incomplete creation. No resume.
"""
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
import fcntl,hashlib,json,os,sqlite3,stat,sys,threading,time
import finite_observation_contract as c
import runtime_state_bundle as inventory
from finite_pilot_anchor import AnchorJournal
from finite_recovery_state_report import current_anchor_digest,_session_report
from readonly_runtime_worker import serialized
from decimal_lifecycle import Lifecycle,SessionRiskPolicy
from native_account_guard import SharedAccountGuard
from native_paper_execution import NativeExecutionObserver,CoordinatedWire,ExecutionObservation
from finite_observation_transport import ObservationTransport

class ReadOnlyLifecycle(Lifecycle):
    def __init__(self,bundle):
        self.bundle=bundle;self.path=bundle.paths['lifecycle'];self.binding=bundle.binding
        self.allowlist=('NVDA',);self._session_policy=SessionRiskPolicy(4,Decimal('100'))
    @contextmanager
    def tx(self):
        with inventory._database(self.path,self.bundle._pinned['lifecycle']) as db:
            db.row_factory=sqlite3.Row
            yield db

class ObservationJournal:
    def __init__(self,path,grant,now_ns,seal):
        c.approved(grant);self.path=str(path);self.grant=grant;self.now_ns=now_ns
        parent=Path(path).parent
        c.require(parent==Path(seal).parent and parent.resolve()==parent,'journal_directory_invalid')
        directory=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        self.fd=None;self.db=None
        try:
            c.require(not any(os.path.lexists(str(path)+x) for x in ('','-journal','-wal','-shm')),'observation_attempt_consumed')
            self.fd=os.open(Path(path).name,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600,dir_fd=directory)
            fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            self.identity=(os.fstat(self.fd).st_dev,os.fstat(self.fd).st_ino)
            os.fsync(directory)
            self.db=sqlite3.connect(Path(path).as_uri()+'?mode=rw',uri=True,timeout=0,isolation_level=None)
            self.db.execute('PRAGMA trusted_schema=OFF')
            c.require(self.db.execute('PRAGMA journal_mode').fetchone()==('delete',),'journal_mode_invalid')
            self.db.execute('PRAGMA synchronous=FULL')
            self.last=c.window(grant,now_ns());self.seq=0;self.gets=0;self.cycle_gets=0;self.active=False;self.event_seq=0
            with self.tx():
                self.db.execute('CREATE TABLE observation_events(seq INTEGER PRIMARY KEY,kind TEXT NOT NULL,cycle INTEGER NOT NULL,at_ns INTEGER NOT NULL,payload TEXT NOT NULL)')
                self.event('RUN_PREPARED',dict(digest=c.digest(grant.document()),document=grant.document()))
        except BaseException:
            self.close();raise
        finally:os.close(directory)
    def event(self,kind,payload,*,cycle=None):
        self.db.execute('INSERT INTO observation_events VALUES(?,?,?,?,?)',(self.event_seq+1,kind,self.seq if cycle is None else cycle,self.last,json.dumps(payload,sort_keys=True,separators=(',',':'))))
        self.event_seq+=1
    def check(self):
        c.approved(self.grant)
        info=os.stat(self.path,follow_symlinks=False)
        c.require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and (info.st_dev,info.st_ino)==self.identity,'journal_replaced')
        self.last=c.window(self.grant,self.now_ns(),self.last)
    @contextmanager
    def tx(self):
        self.check();self.db.execute('BEGIN IMMEDIATE')
        try:yield;self.check();self.db.commit()
        except BaseException:self.db.rollback();raise
    def begin(self):
        c.require(not self.active and self.seq<self.grant.max_cycles and self.gets+self.grant.max_gets_per_cycle<=self.grant.max_gets,'observation_cap')
        with self.tx():
            self.event('CYCLE_PREPARED',dict(cycle=self.seq+1),cycle=self.seq+1)
        self.seq+=1;self.cycle_gets=0;self.active=True
    def reserve(self):
        c.require(self.active and self.cycle_gets<self.grant.max_gets_per_cycle and self.gets<self.grant.max_gets,'observation_get_cap')
        with self.tx():self.event('REQUEST',dict(request=self.cycle_gets+1,total=self.gets+1))
        self.gets+=1;self.cycle_gets+=1
    def complete(self,evidence):
        c.require(self.active and type(evidence) is ExecutionObservation,'invalid_evidence')
        with self.tx():self.event('CYCLE_COMPLETE',dict(evidence_digest=evidence.native_digest,requests=self.cycle_gets))
        self.active=False
    def finish(self):
        c.require(not self.active,'incomplete_observation')
        with self.tx():self.event('RUN_COMPLETE',dict(cycles=self.seq,requests=self.gets))
    def close(self):
        if self.db is not None:self.db.close();self.db=None
        if self.fd is not None:os.close(self.fd);self.fd=None

def resolve(grant,*,paths=None,test_only=False):
    c.approved(grant)
    c.require(type(test_only) is bool and (test_only or paths is None),'fixed_paths_required')
    seal_path=(inventory.FIXED_PATHS if paths is None else paths)['seal']
    identity=inventory._private(seal_path)
    fd=os.open(seal_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd)
        c.require((info.st_dev,info.st_ino)==identity and stat.S_ISREG(info.st_mode) and info.st_size<=4096,'seal_identity_invalid')
        raw=os.read(fd,4097)
    finally:os.close(fd)
    c.require(inventory._private(seal_path)==identity and len(raw)<=4096 and hashlib.sha256(raw).hexdigest()==grant.seal_sha256,'seal_changed')
    seal=inventory._load(raw)
    c.require(type(seal) is dict and c.hash_ok(seal.get('fingerprint')) and seal.get('generation')==inventory.GENERATION,'seal_binding_invalid')
    bundle=inventory.RuntimeStateBundle(seal['fingerprint'],paths=paths,test_only=test_only)
    bundle.verify();return bundle

def verify(bundle,grant,*,initial=False):
    c.approved(grant);bundle.verify();j=AnchorJournal(bundle);j._verify()
    fd=os.open(bundle.paths['seal'],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:raw=os.read(fd,4097)
    finally:os.close(fd)
    c.require(len(raw)<=4096 and hashlib.sha256(raw).hexdigest()==grant.seal_sha256,'seal_changed')
    with inventory._database(bundle.paths['runner'],bundle._pinned['runner']) as db:
        runner=inventory._singleton(db,'runner_control')
    c.require(c.digest(runner)==grant.runner_sha256,'runner_changed')
    with j._db() as db:
        c.require(current_anchor_digest(db)==grant.anchor_sha256,'anchor_changed')
        state=_session_report(db,runner)
        c.require(state['reusable_session_state']=='closed_clean' and state['reusable_session_manifest_digest']==grant.predecessor_sha256 and state['reusable_session_terminal_digest']==grant.terminal_sha256,'closed_predecessor_required')
    c.require(j.protected_state_digest()==grant.protected_sha256,'protected_changed')
    if initial:c.require(j.cohort_state_digest()==grant.cohort_sha256,'cohort_changed')

def credentials():
    key=os.environ.get('ALPACA_API_KEY');secret=os.environ.get('ALPACA_SECRET_KEY')
    c.require(bool(key) and bool(secret),'credentials_missing')
    return key,secret

def run(*,paths=None,test_only=False,now_ns=time.time_ns,monotonic_ns=time.monotonic_ns,sleep=time.sleep,transport_factory=None,emit=print):
    grant=c.approved(c.APPROVED_OBSERVATION)
    c.require(type(test_only) is bool and (test_only or (paths is None and transport_factory is None)),'production_override_forbidden')
    c.window(grant,now_ns());bundle=resolve(grant,paths=paths,test_only=test_only)
    journal=None;last_mono=monotonic_ns();due=last_mono
    # Claim before credentials/provider, while preserving the existing seal.
    with serialized(bundle):
        verify(bundle,grant,initial=True)
        path=str(Path(bundle.paths['seal']).parent/'finite-observation.sqlite3')
        c.require(test_only or path==c.JOURNAL,'fixed_journal_path_required')
        journal=ObservationJournal(path,grant,now_ns,bundle.paths['seal'])
    try:
        for _ in range(grant.max_cycles):
            while True:
                journal.check();mono=monotonic_ns()
                c.require(type(mono) is int and mono>=last_mono,'monotonic_regression');last_mono=mono
                if mono>=due:break
                sleep(min((due-mono)/c.NS,0.25))
            # A hung observation must kill the worker, never leave a provider thread alive.
            timer=threading.Timer(15,lambda:os._exit(124));timer.daemon=True;timer.start()
            try:
                with serialized(bundle):
                    verify(bundle,grant)
                    guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
                    with guard.hold(now_ns=now_ns(),read_only=True):
                        guard.admit_cycle(now_ns=now_ns())
                        journal.begin()
                        started=now_ns();key,secret=credentials()
                        transport=ObservationTransport(key,secret,grant=grant,now_ns=now_ns,started_ns=started,reserve=journal.reserve,test_only=test_only,transport_factory=transport_factory)
                        life=ReadOnlyLifecycle(bundle)
                        observation=NativeExecutionObserver(life,guard,CoordinatedWire(transport,guard,now_ns),now_ns=now_ns,monotonic_ns=monotonic_ns).collect()
                        c.require(type(observation) is ExecutionObservation and transport.request_count==journal.cycle_gets,'invalid_observation')
                        verify(bundle,grant);journal.complete(observation)
            finally:timer.cancel();timer.join()
            finished=monotonic_ns();c.require(type(finished) is int and finished>=last_mono,'monotonic_regression')
            last_mono=finished;due=finished+grant.cadence_ns
            emit(json.dumps(dict(mode='finite_observation',phase='cycle_complete',cycles=journal.seq,requests=journal.gets,orders_attempted=0)))
        journal.finish()
        return dict(phase='completed',cycles=journal.seq,requests=journal.gets,orders_attempted=0)
    finally:
        if journal is not None:journal.close()

def main():
    try:result=run()
    except Exception:print(json.dumps(dict(mode='finite_observation',phase='refused')));return 1
    print(json.dumps(dict(mode='finite_observation',**result)));return 0
if __name__=='__main__':
    import finite_observation_worker as canonical
    sys.exit(canonical.main())
