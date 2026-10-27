"""Standalone absolute-window watchdog. Not imported by serve; grant None."""
from pathlib import Path
import signal,subprocess,sys,time,threading
import finite_observation_contract as c

def run(*,now_ns=time.time_ns,monotonic_ns=time.monotonic_ns,sleep=time.sleep,popen=subprocess.Popen,stopped=lambda:False,ownership=None):
    ownership={} if ownership is None else ownership
    ownership['reaped']=True
    grant=c.approved(c.APPROVED_OBSERVATION)
    last=now_ns();mono=monotonic_ns()
    c.require(type(last) is int and type(mono) is int and last<grant.expires_ns,'observation_window_closed')
    absolute=mono+max(0,grant.expires_ns-last)
    while last<grant.starts_ns:
        c.require(not stopped(),'supervisor_stopped')
        sleep(min(.25,(grant.starts_ns-last)/c.NS))
        new=now_ns();m=monotonic_ns()
        c.require(type(new) is int and type(m) is int and new>=last and m>=mono and m<absolute,'clock_or_expiry')
        last,mono=new,m
    c.window(grant,last);c.require(not stopped(),'supervisor_stopped')
    child=None
    try:
        ownership['reaped']=False
        child=popen([sys.executable,str(Path(__file__).with_name('finite_observation_worker.py'))])
        while True:
            c.require(not stopped(),'supervisor_stopped')
            new=now_ns();m=monotonic_ns()
            c.require(type(new) is int and type(m) is int and new>=last and m>=mono,'clock_regression')
            if new>=grant.expires_ns or m>=absolute:raise c.ObservationRefused('observation_expired')
            last,mono=new,m
            result=child.poll()
            if result is not None:return result
            sleep(.1)
    finally:
        if child is not None:
            try:running=child.poll() is None
            except BaseException:running=True
            if running:
                try:child.kill()
                except ProcessLookupError:pass
                except BaseException:
                    try:child.wait(timeout=5)
                    except BaseException:raise c.ObservationRefused('child_reaping_uncertain') from None
                    raise c.ObservationRefused('child_kill_uncertain') from None
            try:child.wait(timeout=5)
            except BaseException:raise c.ObservationRefused('child_reaping_uncertain') from None
            ownership['reaped']=True

class ObservationSupervisor:
    """One owned thread supervising one directly reaped worker; no nesting."""
    def __init__(self,external_stop=None):
        self._external_stop=external_stop
        c.approved(c.APPROVED_OBSERVATION)
        self._stop=threading.Event();self._done=threading.Event()
        self._ownership={'reaped':True};self._thread=None;self._code=None
    @property
    def completed(self):return self._done.is_set() and self._code==0
    @property
    def halted(self):return self._done.is_set() and self._code!=0
    @property
    def reaped(self):return (self._thread is None or not self._thread.is_alive()) and self._ownership['reaped'] is True
    def _run(self):
        try:self._code=run(stopped=lambda:self._stop.is_set() or (self._external_stop is not None and self._external_stop.is_set()),ownership=self._ownership)
        except BaseException:self._code=1
        finally:self._done.set()
    def start(self):
        c.require(self._thread is None and not self._stop.is_set(),'observation_start_consumed')
        self._thread=threading.Thread(target=self._run,daemon=True)
        self._thread.start()
    def poll(self):return None
    def close(self):
        self._stop.set()
        if self._thread is not None and self._thread.ident is not None:self._thread.join(timeout=6)

def main():
    stopping=[False]
    def stop(signum,frame):stopping[0]=True
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    try:return run(stopped=lambda:stopping[0])
    except Exception:return 1
if __name__=='__main__':sys.exit(main())
