"""Independent review: synthetic/mocked operations only, never source HTTP."""
import email.message
import unittest
from unittest.mock import patch
import source_worker
from transport_contract import LIMIT, URL, decode, encode

class FakeResponse:
    code = 200
    def __init__(self, body=b'<html><title>synthetic</title></html>', headers=()):
        self.body = body
        self.headers = email.message.Message()
        for key, value in headers: self.headers[key] = value
        self.reads = []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def geturl(self): return URL
    def read(self, cap):
        self.reads.append(cap)
        return self.body[:cap]

class FakeOpener:
    def __init__(self, response): self.response = response; self.calls = []
    def open(self, request, timeout):
        self.calls.append((request, timeout))
        return self.response

class IndependentTransportTests(unittest.TestCase):
    def request(self, response):
        opener = FakeOpener(response)
        # Compiled gate is patched only while executing this in-memory fake.
        with patch.object(source_worker, 'LIVE_APPROVED', True):
            result = source_worker.request_once(_opener=opener)
        self.assertEqual(len(opener.calls), 1)
        return result
    def test_live_gate_refuses_even_injected_opener(self):
        opener = FakeOpener(FakeResponse())
        with self.assertRaisesRegex(ValueError, 'live_source_gate_closed'):
            source_worker.request_once(_opener=opener)
        self.assertEqual(opener.calls, [])
    def test_ipc_body_has_independent_bound(self):
        meta = {'url': URL, 'status': 200, 'headers': {}, 'body_complete': True, 'tls_verified': True}
        with self.assertRaises(ValueError): decode(encode(meta, b'x' * (LIMIT + 1)))
    def test_duplicate_content_length_refused(self):
        r = FakeResponse(headers=(('Content-Length', '1'), ('Content-Length', '1')))
        with self.assertRaisesRegex(ValueError, 'duplicate_response_header'): self.request(r)
        self.assertEqual(r.reads, [])
    def test_encoding_refused_before_read(self):
        r = FakeResponse(headers=(('Content-Encoding', 'gzip'),))
        with self.assertRaisesRegex(ValueError, 'content_encoding_refused'): self.request(r)
        self.assertEqual(r.reads, [])
    def test_mismatched_length_and_oversize_refused(self):
        for body, headers in ((b'x', (('Content-Length', '2'),)), (b'x' * (LIMIT + 1), ())):
            with self.subTest(size=len(body)):
                with self.assertRaises(ValueError): self.request(FakeResponse(body, headers))
    def test_denial_body_never_read(self):
        for status in (401, 403, 429, 302):
            with self.subTest(status=status):
                r = FakeResponse(); r.code = status
                meta, body = self.request(r)
                self.assertEqual(r.reads, [])
                self.assertEqual(body, b'')
                self.assertFalse(meta['body_complete'])
    def test_nonallowlisted_secret_header_not_emitted(self):
        r = FakeResponse(headers=(('Set-Cookie', 'secret-sentinel'), ('Authorization', 'another-secret')))
        meta, body = self.request(r)
        self.assertNotIn(b'secret', encode(meta, body))
    def test_frame_duplicate_keys_refused(self):
        import struct
        raw = b'{"url":"a","url":"b"}'
        with self.assertRaisesRegex(ValueError, 'duplicate_json_key'): decode(struct.pack('!I', len(raw)) + raw)

from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import tempfile
import threading
import time
from recurring import LeaseCollector, Lease, LIMIT

class Clock:
    def __init__(self): self.value = int(time.time())
    def __call__(self): return datetime.fromtimestamp(self.value, timezone.utc).isoformat(timespec='microseconds')

class IndependentRecurringTests(unittest.TestCase):
    def harness(self, quota=6, duration=120, cadence=10, transient=False):
        t = tempfile.TemporaryDirectory(); self.addCleanup(t.cleanup)
        root = Path(t.name)/'stable'
        clock = Clock()
        c = LeaseCollector(root, 'independent', create=True, offline_test_enabled=True, wall=clock)
        self.addCleanup(c.close)
        lease = Lease('lease_a', clock.value, clock.value+duration, cadence, quota, quota*LIMIT, 'synthetic_review', transient)
        c.add_lease(lease)
        return c, root, clock, lease
    def reopen(self, root, clock):
        c=LeaseCollector(root,'independent',offline_test_enabled=True,wall=clock)
        self.addCleanup(c.close); return c
    def test_all_uncertain_boundaries_globally_fence_before_next_spawn(self):
        for boundary in ('after_claim','after_spawn_intent','after_capture','before_store','after_store','after_complete'):
            with self.subTest(boundary=boundary):
                c,root,clock,lease=self.harness()
                self.assertEqual(c.tick()['action'],'confirmed')
                clock.value+=10
                def crash(point):
                    if point==boundary: raise KeyboardInterrupt('synthetic crash')
                with self.assertRaises(KeyboardInterrupt): c.tick(_fault=crash)
                c.close(); clock.value+=10
                c=self.reopen(root,clock)
                result=c.tick(_popen=lambda *a,**k:self.fail('spawn after uncertain outcome'))
                self.assertEqual(result['state'],'fenced')
                self.assertEqual(result['attempts'],2)
                rows=c.observations('2099-01-01T00:00:00+00:00')
                self.assertEqual(sum(len(row['observations']) for row in rows),1)
                with self.assertRaisesRegex(ValueError,'renewal_cannot_clear_gate'):
                    c.add_lease(replace(lease,lease_id='lease_b',start=lease.end,end=lease.end+120))
    def test_renewal_uses_same_databases_global_ids_and_preserved_quota(self):
        c,root,clock,a=self.harness(quota=1,duration=10)
        before={p.name:p.stat().st_ino for p in root.glob('*.sqlite')}
        self.assertEqual(c.tick()['attempt_id'],1)
        self.assertEqual(c.add_lease(a),'already_recorded')
        self.assertEqual(c.tick()['action'],'no_claim')
        b=replace(a,lease_id='lease_b',start=a.end,end=a.end+10)
        self.assertEqual(c.add_lease(b),'recorded')
        c.close();clock.value=b.start;c=self.reopen(root,clock)
        self.assertEqual(c.tick()['attempt_id'],2)
        self.assertEqual(c.status()['attempts'],2)
        self.assertEqual(before,{p.name:p.stat().st_ino for p in root.glob('*.sqlite')})
        self.assertEqual(c.db.execute("SELECT COUNT(*) FROM attempts WHERE lease_id='lease_a'").fetchone()[0],1)
        with self.assertRaises(ValueError): c.add_lease(replace(a,request_limit=2,byte_limit=2*LIMIT))
    def test_overlapping_lease_and_bool_limits_refused(self):
        c,root,clock,a=self.harness()
        with self.assertRaisesRegex(ValueError,'overlapping_lease'):
            c.add_lease(replace(a,lease_id='overlap',start=a.start+1))
        with self.assertRaises(ValueError): replace(a,request_limit=True)
    def test_source_429_pause_survives_restart_and_blocks_renewal(self):
        c,root,clock,a=self.harness()
        c.tick();clock.value+=10
        self.assertEqual(c.tick(mode='rate_limited')['state'],'paused_source')
        c.close();clock.value+=10;c=self.reopen(root,clock)
        self.assertEqual(c.tick(_popen=lambda *a,**k:self.fail('paused source spawned'))['state'],'paused_source')
        with self.assertRaises(ValueError): c.add_lease(replace(a,lease_id='lease_b',start=a.end,end=a.end+120))
        self.assertEqual(sum(len(x['observations']) for x in c.observations('2099-01-01T00:00:00+00:00')),1)
    def test_known_local_shutdown_consumes_slot_but_resume_lease(self):
        c,root,clock,a=self.harness()
        def stop(point):
            if point=='after_claim': c.stop_event.set()
        self.assertEqual(c.tick(_fault=stop)['action'],'cancelled')
        self.assertEqual(c.status()['attempts'],1)
        c.close();c=self.reopen(root,clock)
        self.assertEqual(c.tick(_popen=lambda *a,**k:self.fail('interrupted slot retried'))['action'],'no_claim')
        clock.value+=10
        self.assertEqual(c.tick()['action'],'confirmed')
        self.assertEqual(c.status()['attempts'],2)
    def test_reaped_timeout_next_slot_only_with_explicit_policy(self):
        for allowed in (False,True):
            with self.subTest(allowed=allowed):
                c,root,clock,a=self.harness(transient=allowed)
                self.assertEqual(c.tick(mode='hang',timeout=.08)['action'],'failed')
                self.assertEqual(c.tick()['action'],'no_claim')
                clock.value+=10
                result=c.tick()
                self.assertEqual(result['action'],'confirmed' if allowed else 'no_claim')
    def test_operator_revoke_survives_restart_and_new_lease(self):
        c,root,clock,a=self.harness();c.revoke();c.close();c=self.reopen(root,clock)
        self.assertEqual(c.tick(_popen=lambda *a,**k:self.fail('revoked'))['state'],'operator_revoked')
        with self.assertRaises(ValueError): c.add_lease(replace(a,lease_id='lease_b',start=a.end,end=a.end+120))
    def test_wall_jump_after_claim_never_spawns_outside_lease(self):
        c,root,clock,a=self.harness()
        def jump(point):
            if point=='after_claim': clock.value=a.end+1
        spawns=[]
        def forbidden(*args,**kwargs):
            spawns.append(True); raise OSError('must not launch after expiry')
        c.tick(_fault=jump,_popen=forbidden)
        self.assertEqual(spawns,[])
    def test_clock_regression_is_global_fence_not_source_pause(self):
        c,root,clock,a=self.harness()
        def jump(point):
            if point=='after_spawn_intent': clock.value-=20
        result=c.tick(_fault=jump)
        self.assertEqual(result['state'],'fenced')

    def test_storage_reserve_failure_persists_fence_after_capacity_returns(self):
        c,root,clock,a=self.harness()
        with patch.object(c.root,'capacity',side_effect=ValueError('storage_reserve')):
            try: c.tick()
            except ValueError: pass
        self.assertEqual(c.status()['state'],'fenced')
        c.close();c=self.reopen(root,clock)
        self.assertEqual(c.tick(_popen=lambda *a,**k:self.fail('resumed after storage failure'))['state'],'fenced')

    def test_real_sigterm_reaps_worker_then_same_lease_resumes_next_slot(self):
        script='''
import json, os, signal, subprocess, tempfile, threading, time
from datetime import datetime, timezone
from pathlib import Path
from recurring import LeaseCollector, Lease, LIMIT
stop=threading.Event(); seen=[]; value=[int(time.time())]
def wall(): return datetime.fromtimestamp(value[0],timezone.utc).isoformat(timespec="microseconds")
def handler(sig, frame): seen.append(sig); stop.set()
signal.signal(signal.SIGTERM,handler)
with tempfile.TemporaryDirectory() as t:
 root=Path(t)/"run"
 c=LeaseCollector(root,"signal_review",create=True,offline_test_enabled=True,stop=stop,wall=wall)
 c.add_lease(Lease("lease",value[0],value[0]+60,10,3,3*LIMIT,"synthetic_review"))
 def spawn(*args,**kwargs):
  p=subprocess.Popen(*args,**kwargs); os.kill(os.getpid(),signal.SIGTERM); return p
 first=c.tick(mode="hang",_popen=spawn)
 audit=json.loads(c.db.execute("SELECT payload FROM events WHERE phase='CANCELLED'").fetchone()[0])["transport"]
 c.close(); value[0]+=10
 c=LeaseCollector(root,"signal_review",offline_test_enabled=True,wall=wall)
 second=c.tick(); c.close()
 print(json.dumps({"first":first,"second":second,"audit":audit,"seen":seen,"handler_preserved":signal.getsignal(signal.SIGTERM) is handler}))
'''
        p=subprocess.run([os.sys.executable,'-c',script],cwd=Path(__file__).resolve().parents[1],env={},capture_output=True,timeout=5,check=True)
        out=json.loads(p.stdout)
        self.assertEqual(out['seen'],[signal.SIGTERM]);self.assertTrue(out['handler_preserved'])
        self.assertEqual(out['first']['action'],'cancelled');self.assertTrue(out['audit']['reaped'])
        self.assertEqual(out['second']['action'],'confirmed');self.assertEqual(out['second']['attempts'],2)
        with self.assertRaises(ProcessLookupError): os.kill(out['audit']['child_pid'],0)

    def test_late_slot_is_skipped_not_backfilled_after_restart(self):
        c,root,clock,a=self.harness()
        clock.value=a.start+7
        self.assertEqual(c.tick(_popen=lambda *a,**k:self.fail('late slot spawned'))['attempts'],0)
        c.close();c=self.reopen(root,clock)
        clock.value=a.start+10
        self.assertEqual(c.tick()['action'],'confirmed')
        slots=[r[0] for r in c.db.execute('SELECT slot FROM attempts')]
        self.assertEqual(slots,[a.start+10])
        self.assertEqual(c.db.execute('SELECT slot FROM missed').fetchone()[0],a.start)

    def test_per_attempt_postcommit_cutoff_and_version_retention(self):
        c,root,clock,a=self.harness()
        def advance(point):
            if point=='after_complete': clock.value+=1
        self.assertEqual(c.tick(_fault=advance)['action'],'confirmed')
        self.assertEqual(c.observations(datetime.fromtimestamp(a.start,timezone.utc).isoformat()),[])
        self.assertEqual(len(c.observations(clock())),1)
        clock.value=a.start+10;c.tick()
        clock.value=a.start+20;c.tick(mode='revised')
        rows=c.observations('2099-01-01T00:00:00+00:00')
        self.assertEqual(len(rows),2)
        self.assertEqual(sum(len(x['observations']) for x in rows),3)
        self.assertEqual(len({x['event']['event_id'] for x in rows}),1)
        self.assertTrue(all(not x['eligible_for_backtest'] and x['order_permission']=='none' for x in rows))

    @unittest.skipUnless(os.sys.platform=='linux','Linux parent-death contract')
    def test_sigkill_parent_worker_dies_and_lease_is_fenced_not_resumed(self):
        inner='''
import json,os,subprocess,sys,time
from pathlib import Path
from recurring import LeaseCollector,Lease,LIMIT
root=Path(sys.argv[1]); start=int(sys.argv[2])
c=LeaseCollector(root,"kill_review",create=True,offline_test_enabled=True)
c.add_lease(Lease("lease",start,start+60,1,3,3*LIMIT,"synthetic_review"))
def spawn(*a,**kw):
 p=subprocess.Popen(*a,**kw);time.sleep(.15);print(json.dumps({"worker_pid":p.pid}),flush=True);return p
c.tick(mode="hang",_popen=spawn)
'''
        outer='''
import ctypes,json,os,signal,subprocess,sys,tempfile,time
from pathlib import Path
from recurring import LeaseCollector
libc=ctypes.CDLL(None,use_errno=True)
if libc.prctl(36,1,0,0,0)!=0:raise OSError("subreaper")
worker=None
with tempfile.TemporaryDirectory() as t:
 root=Path(t)/"run";start=int(time.time())
 parent=subprocess.Popen([sys.executable,'-c',INNER,str(root),str(start)],env={},stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 try:
  worker=json.loads(parent.stdout.readline())["worker_pid"]
  os.kill(parent.pid,signal.SIGKILL);parent.wait(timeout=2)
  deadline=time.monotonic()+3;status=None
  while time.monotonic()<deadline:
   pid,code=os.waitpid(worker,os.WNOHANG)
   if pid==worker:status=code;break
   time.sleep(.01)
  if status is None:raise AssertionError("worker survived")
  c=LeaseCollector(root,"kill_review",offline_test_enabled=True)
  result=c.tick(_popen=lambda *a,**k:(_ for _ in ()).throw(AssertionError("replayed")))
  c.close();print(json.dumps({"worker_status":status,"parent_status":parent.returncode,"restart":result}))
 finally:
  if parent.poll() is None:parent.kill();parent.wait(timeout=2)
  parent.stdout.close();parent.stderr.close()
  if worker is not None:
   try:os.kill(worker,signal.SIGKILL)
   except ProcessLookupError:pass
   try:os.waitpid(worker,0)
   except ChildProcessError:pass
'''.replace('INNER',repr(inner))
        cp=subprocess.run([os.sys.executable,'-c',outer],cwd=Path(__file__).resolve().parents[1],env={},capture_output=True,timeout=8,check=True)
        result=json.loads(cp.stdout)
        self.assertEqual(result['parent_status'],-signal.SIGKILL)
        self.assertTrue(os.WIFSIGNALED(result['worker_status']))
        self.assertEqual(os.WTERMSIG(result['worker_status']),signal.SIGKILL)
        self.assertEqual(result['restart']['state'],'fenced')
        self.assertEqual(result['restart']['attempts'],1)

    def test_deadline_expired_before_spawn_obeys_transient_policy(self):
        for allowed in (False,True):
            with self.subTest(allowed=allowed):
                c,root,clock,a=self.harness(transient=allowed)
                baseline=time.monotonic()
                def delay(point):
                    if point=='after_claim':
                        clock.value+=11
                        c.mono=lambda:baseline+11
                first=c.tick(_fault=delay,_popen=lambda *a,**k:self.fail('deadline-expired spawn'))
                c.mono=time.monotonic
                self.assertEqual(first['state'],'waiting_next_slot' if allowed else 'paused_source')
                clock.value=a.start+20
                result=c.tick()
                self.assertEqual(result['action'],'confirmed' if allowed else 'no_claim')

    def test_admission_marker_survives_gate_or_success_marker_write_failure(self):
        for failure in ('gate','success_marker'):
            with self.subTest(failure=failure):
                c,root,clock,a=self.harness()
                if failure=='gate':
                    with patch.object(c.root,'capacity',side_effect=ValueError('storage_reserve')), patch.object(c,'_set_gate',side_effect=sqlite3.OperationalError('synthetic disk full')):
                        with self.assertRaises(sqlite3.OperationalError): c.tick()
                else:
                    original=c._event
                    def fail_ok(phase,*args,**kwargs):
                        if phase=='ADMISSION_OK': raise sqlite3.OperationalError('synthetic commit path failure')
                        return original(phase,*args,**kwargs)
                    with patch.object(c,'_event',side_effect=fail_ok):
                        with self.assertRaises(sqlite3.OperationalError): c.tick()
                self.assertEqual(c.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0],0)
                c.close();c=self.reopen(root,clock)
                self.assertEqual(c.status()['state'],'fenced')
                self.assertEqual(c.tick(_popen=lambda *a,**k:self.fail('spawn after unmatched admission'))['action'],'no_claim')
                with self.assertRaises(ValueError):
                    c.add_lease(replace(a,lease_id='renewal',start=a.end,end=a.end+120))
