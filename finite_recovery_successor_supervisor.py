"""Explicit fixed recovery-only child owner. No serve hook, scheduling or retry."""
from pathlib import Path
import json
import signal
import subprocess
import sys
import threading
import time
import finite_recovery_successor_contract as contract
from finite_pilot_contract import require, NS


def supervise(stop, *, test_only=False, popen=None, wall=None, monotonic=None):
    manifest = contract.APPROVED_SUCCESSOR
    if manifest is None:
        return {'reason':'disabled','child_reaped':True,'automatic_retry':False,'order_permission':'none'}
    contract.require_successor_manifest(manifest)
    require(type(test_only) is bool and (test_only or all(v is None for v in (popen,wall,monotonic))),
            'production_overrides_forbidden')
    require(isinstance(stop,threading.Event),'successor_stop_required')
    now = time.time_ns if wall is None else wall
    clock = time.monotonic if monotonic is None else monotonic
    launch = subprocess.Popen if popen is None else popen
    at = now()
    require(type(at) is int and manifest.starts_ns <= at < manifest.expires_ns,'successor_window_closed')
    child = None
    reaped = True
    reason = 'stopped'
    try:
        if stop.is_set():
            return {'reason':reason,'child_reaped':True,'automatic_retry':False,'order_permission':'none'}
        deadline = clock() + min(15.0,(manifest.expires_ns-at)/NS)
        child = launch([sys.executable,str(Path(__file__).resolve().with_name('finite_recovery_successor_worker.py')),'--recover'])
        reaped = False
        reason = 'deadline_expired'
        previous = at
        while not stop.is_set():
            code = child.poll()
            if code is not None:
                reaped = True
                reason = 'completed' if type(code) is int and code == 0 else 'process_failed'
                break
            current = now()
            if type(current) is not int or current < previous or current >= manifest.expires_ns or clock() >= deadline:
                break
            previous = current
            stop.wait(0.1)
        if stop.is_set():
            reason = 'stopped'
    except Exception:
        reason = 'supervision_failed'
    finally:
        if child is not None and not reaped:
            try:
                child.kill()
            except Exception:
                pass
            try:
                child.wait(timeout=1.0)
                reaped = True
            except Exception:
                reason = 'child_reap_uncertain'
    return {'reason':reason,'child_reaped':reaped,'automatic_retry':False,'order_permission':'none'}


class SuccessorSupervisor:
    """One future-window child with independent timer, never a retry scheduler."""
    def __init__(self, *, enabled=False, test_only=False, popen=None, wall=None,
                 monotonic=None, timer_factory=None, emit=print):
        require(type(enabled) is bool and type(test_only) is bool, 'successor_enable_invalid')
        require(test_only or all(v is None for v in (popen,wall,monotonic,timer_factory)),
                'production_overrides_forbidden')
        self.manifest=contract.APPROVED_SUCCESSOR
        self.enabled=enabled and self.manifest is not None
        if self.enabled:contract.require_successor_manifest(self.manifest)
        self._popen=subprocess.Popen if popen is None else popen
        self._wall=time.time_ns if wall is None else wall
        self._clock=time.monotonic if monotonic is None else monotonic
        self._timer_factory=threading.Timer if timer_factory is None else timer_factory
        self._emit=emit
        self._events=[]
        self._lock=threading.RLock()
        self._child=None;self._timer=None;self._started=False;self._launched=False
        self._closed=False;self._previous_wall=None;self._deadline=None
        self.completed=False;self.halted=False;self.reaped=True

    def _terminal(self, reason, *, success=False):
        self.completed=success;self.halted=not success
        if self._timer is not None:self._timer.cancel()
        self._events.append(json.dumps(dict(mode='finite_recovery_successor_supervisor',reason=reason,
            child_reaped=self.reaped,automatic_retry=False,order_permission='none')))

    def _reap(self):
        if self._child is not None and not self.reaped:
            try:self._child.kill()
            except Exception:pass
            try:self._child.wait(timeout=1.0);self.reaped=True
            except Exception:self.reaped=False

    def _expire(self):
        with self._lock:
            if self.completed or self.halted or self._closed:return
            self._reap()
            self._terminal('deadline_expired' if self.reaped else 'child_reap_uncertain')

    def start(self):
        with self._lock:
            if self._started or self._closed:return
            self._started=True
            if self.enabled:
                self._events.append(json.dumps(dict(mode='finite_recovery_successor_supervisor',
                    reason='waiting_approved_window',provider_requests=0,order_permission='none')))
        self.poll()

    def _flush(self):
        # Output can block; it must never own the watchdog's child-state lock.
        with self._lock:
            events,self._events=self._events,[]
        for event in events:
            try:self._emit(event)
            except Exception:pass

    def poll(self):
        self._poll_locked()
        self._flush()

    def _poll_locked(self):
        with self._lock:
            if not self.enabled or not self._started or self._closed or self.completed or self.halted:return
            try:
                contract.require_successor_manifest(self.manifest)
                now=self._wall()
                require(type(now) is int and (self._previous_wall is None or now>=self._previous_wall),
                        'successor_clock_regression')
                self._previous_wall=now
                if now>=self.manifest.expires_ns:
                    self._reap();self._terminal('window_expired');return
                if now<self.manifest.starts_ns:return
                if not self._launched:
                    self._launched=True
                    duration=min(15.0,(self.manifest.expires_ns-now)/NS)
                    self._deadline=self._clock()+duration
                    self._child=self._popen([sys.executable,str(Path(__file__).resolve().with_name(
                        'finite_recovery_successor_worker.py')),'--recover'],stdin=subprocess.DEVNULL,
                        stdout=None,stderr=subprocess.DEVNULL,shell=False,close_fds=True)
                    self.reaped=False
                    self._timer=self._timer_factory(max(0.0,self._deadline-self._clock()),self._expire)
                    self._timer.daemon=True
                    self._timer.start()
                code=self._child.poll()
                if code is not None:
                    self.reaped=True
                    self._terminal('completed' if type(code) is int and code==0 else 'process_failed',
                                   success=type(code) is int and code==0)
                elif self._clock()>=self._deadline:self._expire()
            except Exception:
                self._reap();self._terminal('supervision_failed' if self.reaped else 'child_reap_uncertain')

    def close(self):
        with self._lock:
            self._closed=True
            if self._timer is not None:self._timer.cancel()
            self._reap()


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if args != ['--run']:
        return 2
    stop=threading.Event()
    for sig in (signal.SIGTERM,signal.SIGINT):
        signal.signal(sig,lambda *_:stop.set())
    try:
        result=supervise(stop)
    except Exception:
        result={'reason':'refused','child_reaped':True,'automatic_retry':False,'order_permission':'none'}
    print(json.dumps(dict(mode='finite_recovery_successor_supervisor',**result),sort_keys=True))
    return 0 if result['reason'] in ('disabled','completed') else 1


if __name__=='__main__':
    raise SystemExit(main())
