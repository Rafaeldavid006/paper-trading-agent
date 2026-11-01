"""One optional capture child; healthy ECB/source/web survive its refusal."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import paper_asset_capture as capture

class CaptureSupervisor:
    def __init__(self):
        self.child=None;self.reaped=True;self.started=False;self.terminal=False
        self.reason='disabled' if capture.CAPTURE_ENABLED is not True else 'waiting'
        self.buffer=b'';self.output_bytes=0;self.frames=[];self.result=None
        self.wall=time.time_ns();self.mono=time.monotonic_ns();self.deadline=None
        self.reported=False;self.stopping=False
    def _status(self):
        return {'mode':'paper_asset_capture_supervisor','reason':self.reason,'child_reaped':self.reaped,'automatic_retry':False,'order_permission':'none'}
    def _report(self):
        if not self.reported:
            self.reported=True  # no retry of a failed diagnostic
            try:print(json.dumps(self._status(),sort_keys=True,separators=(',',':')),flush=True)
            except BaseException:pass  # capture-only logging cannot stop siblings
    def start(self): self.poll()
    def _spawn(self):
        self.started=True  # no automatic second launch, even on partial failure
        try:
            self.child=subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve().with_name('paper_asset_capture.py')),'--capture-three-assets','--parent-pid',str(os.getpid())],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,shell=False,close_fds=True,env={k:os.environ[k] for k in ('ALPACA_API_KEY','ALPACA_SECRET_KEY','EVENT_DB') if k in os.environ})
            self.reaped=False
            self.deadline=time.monotonic()+min(capture.MAX_SECONDS,max(0,(capture.END_NS-time.time_ns())/10**9))+1
            os.set_blocking(self.child.stdout.fileno(),False)
            self.reason='running'
        except BaseException:
            self.reason='spawn_uncertain';self.terminal=True;self._kill_reap();self._report()
    def _frame(self,raw):
        capture.require(len(raw)<=4096,'export')
        f=capture.parse(raw);capture.require(type(f) is dict,'export')
        if f.get('kind')=='status':
            capture.require(self.result is None and set(f)=={'mode','kind','reason','attempts','order_permission','automatic_retry'} and f==capture.status(f.get('reason'),f.get('attempts')),'export');self.result=f
        else:
            keys={'mode','kind','capture_id','seq','total','sha256','chunk_sha256','payload'}
            capture.require(self.result is None and set(f)==keys and f['mode']=='paper_asset_capture' and f['kind']=='chunk' and f['capture_id']==capture.CAPTURE_ID,'export')
            capture.require(type(f['seq']) is int and f['seq']==len(self.frames) and type(f['total']) is int and 1<=f['total']<=24 and type(f['payload']) is str and len(f['payload'])<=capture.CHUNK,'export')
            capture.require(all(type(f[k]) is str and re.fullmatch('[0-9a-f]{64}',f[k]) for k in ('sha256','chunk_sha256')) and capture.sha(f['payload'].encode())==f['chunk_sha256'],'export')
            capture.require(not self.frames or (f['total'],f['sha256'])==(self.frames[0]['total'],self.frames[0]['sha256']),'export');self.frames.append(f)
    def _read(self):
        if self.child is None or self.child.stdout is None or self.child.stdout.closed:return
        for _ in range(16):
            try:raw=os.read(self.child.stdout.fileno(),4096)
            except BlockingIOError:return
            if not raw:return
            self.output_bytes+=len(raw);capture.require(self.output_bytes<=65536,'export');self.buffer+=raw
            while b'\n' in self.buffer:
                line,self.buffer=self.buffer.split(b'\n',1);self._frame(line)
            capture.require(len(self.buffer)<=4096,'export')
    def _finish(self,code):
        self._read();capture.require(not self.buffer,'export')
        if code==0 and self.result is not None and self.result['reason']=='completed':
            capture.require(self.result['attempts']==3 and self.frames and len(self.frames)==self.frames[0]['total'],'export')
            encoded=''.join(x['payload'] for x in self.frames);capture.require(len(encoded)<=capture.MAX_EXPORT,'export')
            raw=base64.b64decode(encoded,validate=True)
            capture.require(base64.b64encode(raw).decode()==encoded and capture.sha(raw)==self.frames[0]['sha256'],'export')
            capture.validate_public_envelope(capture.parse(raw))
            # Only bounded, complete PUBLIC asset protocol is forwarded; no native metadata.
            for f in self.frames: print(capture.canonical(f).decode(),flush=True)
            self.reason='completed'
        else:self.reason=self.result['reason'] if self.result is not None and self.result['reason']!='completed' else 'refused'
        self.terminal=True
    def poll(self):
        if self.terminal:return
        if capture.CAPTURE_ENABLED is not True:self.reason='disabled';self.terminal=True;self._report();return
        if self.stopping:self.close();return
        if not self.started:
            now,mono=time.time_ns(),time.monotonic_ns()
            if now<self.wall or mono<self.mono or abs((now-self.wall)-(mono-self.mono))>5*10**9:
                self.reason='clock';self.terminal=True;self._report();return
            if now>=capture.END_NS:self.reason='window';self.terminal=True;self._report();return
            if now<capture.START_NS:return  # no native/marker/credential read
            self._spawn()
        if self.child is None or self.terminal:return
        try:
            self._read();code=self.child.poll()
            if code is not None:self.reaped=True;self._finish(code);self._kill_reap();self._report();return
            if time.monotonic()>=self.deadline:
                self.reason='timeout';self.terminal=True;self._kill_reap();self._report()
        except BaseException:
            self.reason='protocol_or_cleanup_uncertain';self.terminal=True;self._kill_reap();self._report()
    def _close_stdout(self):
        if self.child is not None and self.child.stdout is not None and not self.child.stdout.closed:self.child.stdout.close()
    def _kill_reap(self):
        try:
            if self.child is not None and not self.reaped:
                try:self.child.kill()  # exact owned Popen PID, never a shared group
                except ProcessLookupError:pass
                except BaseException:self.reason='cleanup_uncertain'
                try:self.child.wait(timeout=1);self.reaped=True
                except BaseException:self.reaped=False
        finally:
            try:self._close_stdout()
            except BaseException:self.reason='cleanup_uncertain'
        if not self.reaped:self.reason='cleanup_uncertain'
    def begin_stop(self):self.stopping=True
    def close(self):
        self.stopping=True;self.terminal=True
        if self.reason not in ('completed','disabled','refused'):self.reason='stopped'
        self._kill_reap();self._report()
        return self.reaped
