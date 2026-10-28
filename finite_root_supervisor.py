"""One original owner for two bounded children; cold restart never resumes root."""
import threading,time
import finite_root_contract as rc
from finite_pilot_contract import require
from finite_pilot_supervisor import FinitePilotSupervisor
from finite_pilot_anchor import resolve_approved_bundle
from finite_root_worker import AdmissionJournal,record
from finite_root_journal import RootJournal,load
from readonly_runtime_worker import serialized

def cold_start_allowed():
    root=rc.require_root(rc.APPROVED_ROOT);m=root.children[0]
    bundle=resolve_approved_bundle(m);j=AdmissionJournal(bundle)
    with serialized(bundle):
        j._verify(m)
        with j._db() as db:
            if db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(rc.ROOTS,)).fetchone():return False
            j._prestate(db,m)
        j._controls()
        require(j.cohort_state_digest()==root.current_cohort_sha256 and j.protected_state_digest()==root.protected_state_sha256,'root_prestate_changed')
    return True

class RootSupervisor:
    def __init__(self,external_stop=None):
        self.root=rc.require_root(rc.APPROVED_ROOT);self.external_stop=external_stop
        self._stop=threading.Event();self._done=threading.Event();self._thread=None;self._active=None;self._ok=False;self._uncertain=False;self._last_wall=None;self._last_mono=None
    def stopped(self):return self._stop.is_set() or (self.external_stop is not None and self.external_stop.is_set())
    @property
    def completed(self):return self._done.is_set() and self._ok
    @property
    def halted(self):return self._done.is_set() and not self._ok
    @property
    def reaped(self):return not self._uncertain and (self._thread is None or not self._thread.is_alive()) and (self._active is None or self._active.reaped)
    def _clock(self):
        rc.require_root(self.root);wall=time.time_ns();mono=time.monotonic()
        require(type(wall) is int and wall>0 and (self._last_wall is None or wall>=self._last_wall) and (self._last_mono is None or mono>=self._last_mono),'root_clock_regression')
        self._last_wall,self._last_mono=wall,mono
        require(wall<self.root.expires_ns and mono<self._deadline,'root_expired');return wall
    def _wait(self,until):
        while self._clock()<until:
            if self.stopped():return False
            self._stop.wait(.1)
        return not self.stopped()
    def _run(self):
        try:
            now=time.time_ns();self._deadline=time.monotonic()+max(0,(self.root.expires_ns-now)/10**9)
            if now>=self.root.expires_ns or self.stopped() or not cold_start_allowed():return
            for i,m in enumerate(self.root.children):
                if not self._wait(m.starts_ns):return
                runtime=FinitePilotSupervisor(enabled=True,root_index=i,emit=lambda _:None)
                self._active=runtime;runtime.start()
                while not (runtime.completed or runtime.halted):
                    self._clock()
                    if self.stopped():break
                    runtime.poll();self._stop.wait(.1)
                runtime.close()
                if not runtime.reaped:self._uncertain=True;return
                # Failed initialization may be PREPARED/uncertain. Never normalize
                # or advance it, and never revoke another owner's winning claim.
                if not runtime.initialized:return
                if self._clock()>=m.root_binding.close_expires_ns:return
                # Only durable allocation completion or normal absolute expiry
                # permits handoff. An unexpected supervisory failure revokes but
                # cannot be converted into a successful next-child admission.
                bundle=resolve_approved_bundle(m);j=RootJournal(bundle)
                with serialized(bundle):
                    j._verify(m)
                    with j._db() as db:
                        _,rows,_,_=load(db,self.root);row=rows[m.pilot_id]
                        normal=(not row['dirty'] and (runtime.halt_reason=='scope_expired' or (row['ticks']==m.max_ticks and (runtime.completed or runtime.halt_reason=='process_failed'))))
                record('halt',root_index=i)
                if not normal:return
                if self.stopped() or not self._wait(m.expires_ns):return
                record('end',root_index=i)
                self._active=None
            self._ok=True
        except Exception:self._ok=False
        finally:
            try:
                if self._active is not None:
                    self._active.close()
                    if not self._active.reaped:self._uncertain=True
            except Exception:self._uncertain=True;self._ok=False
            finally:self._done.set()
    def start(self):
        require(self._thread is None and not self.stopped(),'root_start_consumed')
        self._thread=threading.Thread(target=self._run,daemon=True);self._thread.start()
    def poll(self):return None
    def close(self):
        self._stop.set()
        # The owned thread handles child close; never contend on a slow Popen
        # lock here. A timed-out join remains explicitly unreaped.
        if self._thread is not None and self._thread.ident is not None:self._thread.join(timeout=6)
