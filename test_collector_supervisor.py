import contextlib
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parent
BASE = HERE
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(HERE))
import collector_supervisor as cs
import serve


class Lifecycle(unittest.TestCase):
    def terminal_race(self, payload):
        """Write only inside poll(), after the first read saw an empty pipe."""
        read_fd, write_fd = os.pipe()
        os.set_blocking(read_fd, False)
        supervisor = cs.CollectorSupervisor()
        class ExitRace:
            stdout = os.fdopen(read_fd, 'rb', buffering=0)
            returncode = 0
            def poll(self):
                os.write(write_fd, payload)
                os.close(write_fd)
                return 0
        supervisor.child = ExitRace()
        supervisor.started = True
        supervisor.reaped = False
        supervisor._heartbeat = time.monotonic()
        supervisor.poll()
        self.assertTrue(supervisor.reaped)
        self.assertTrue(supervisor.child.stdout.closed)
        self.assertFalse(supervisor.status()['active_collection'])
        return supervisor

    def test_receipt_written_between_read_and_exit_is_drained(self):
        frame = {'order_permission': 'none', 'state': 'enrollment_complete',
                 'reason': 'awaiting_collect_manifest', 'enrollment_receipt_sha256': 'a' * 64}
        s = self.terminal_race((json.dumps(frame) + '\n').encode())
        self.assertEqual(s.reason, 'enrollment_complete')
        self.assertEqual(s.status()['owner_status']['enrollment_receipt_sha256'], 'a' * 64)
        child = s.child
        s.poll(); s.start()
        self.assertIs(s.child, child)
        self.assertEqual(s.report_if_due()['owner_status']['enrollment_receipt_sha256'], 'a' * 64)
        s._reported_at -= 61
        self.assertEqual(s.report_if_due()['owner_status']['enrollment_receipt_sha256'], 'a' * 64)

    def test_truncated_terminal_frame_refuses_enrollment(self):
        s = self.terminal_race(b'{"order_permission":"none"')
        self.assertEqual(s.failure_reason, 'owner_status_incomplete')
        self.assertNotEqual(s.reason, 'enrollment_complete')

    def test_malformed_terminal_frame_refuses_enrollment(self):
        s = self.terminal_race(b'not-json\n')
        self.assertEqual(s.failure_reason, 'owner_status_uncertain')
        self.assertNotEqual(s.reason, 'enrollment_complete')

    def test_final_drain_stays_one_bounded_read(self):
        s = cs.CollectorSupervisor()
        s.child = Mock(); s.child.stdout.fileno.return_value = 123
        s.child.poll.return_value = 0
        s.reaped = False; s._heartbeat = time.monotonic()
        with patch.object(cs.os, 'read', side_effect=[BlockingIOError(), b'x' * (cs.FRAME_LIMIT + 1)]) as read:
            s.poll()
        self.assertEqual(read.call_count, 2)
        self.assertEqual(read.call_args_list[-1].args, (123, cs.FRAME_LIMIT + 1))
        self.assertTrue(s.reaped)
        self.assertEqual(s.failure_reason, 'owner_status_incomplete')
        self.assertNotEqual(s.reason, 'enrollment_complete')

    def spawn_fixture(self, body):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        root = Path(tmp.name); (root / 'production_collector').mkdir()
        (root / 'production_collector' / 'collector_owner.py').write_text(body)
        s = cs.CollectorSupervisor(); self.addCleanup(s.close)
        with patch.object(cs, '__file__', str(root / 'collector_supervisor.py')):
            s.start()
        return s

    def until(self, condition, timeout=5):
        end = time.monotonic() + timeout
        while not condition():
            if time.monotonic() > end:self.fail('timed out')
            time.sleep(.02)

    def test_real_owner_environment_pipe_and_reap(self):
        s = self.spawn_fixture('import os,json,time\nprint(json.dumps({"order_permission":"none","env":dict(os.environ)}),flush=True)\ntime.sleep(60)\n')
        self.until(lambda:(s.poll(), s.status()['owner_status'] is not None)[1])
        # Python may coerce locale in an empty environment; no inherited secrets.
        self.assertNotIn('env', s.status()['owner_status'])
        before=time.monotonic(); s.close()
        self.assertLess(time.monotonic()-before, 4.5); self.assertTrue(s.reaped)
        self.assertIsNotNone(s.child.returncode)
        pid=s.child.pid;s.start();self.assertEqual(s.child.pid,pid)

    def test_ignored_term_killed_and_reaped(self):
        s=self.spawn_fixture('import signal,json,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\nprint(json.dumps({"order_permission":"none"}),flush=True)\ntime.sleep(60)\n')
        self.until(lambda:(s.poll(),s._last is not None)[1])
        before=time.monotonic();s.close()
        self.assertTrue(s.reaped);self.assertEqual(s.child.returncode,-signal.SIGKILL)
        self.assertLess(time.monotonic()-before,4.5)

    def test_malformed_status_stops_without_retry(self):
        s=self.spawn_fixture('import time\nprint("not-json",flush=True)\ntime.sleep(60)\n')
        self.until(lambda:(s.poll(),s._stop_at is not None)[1])
        s.close();self.assertTrue(s.reaped);old=s.child;s.start();self.assertIs(old,s.child)

    def test_stale_heartbeat_stops(self):
        s=self.spawn_fixture('import time\ntime.sleep(60)\n')
        s._heartbeat=time.monotonic()-100;s.poll()
        self.assertIsNotNone(s._stop_at);s.close();self.assertTrue(s.reaped)

    def test_spawn_failure_is_not_claimed_reaped_or_retried(self):
        s=cs.CollectorSupervisor()
        with patch.object(cs.subprocess,'Popen',side_effect=OSError('secret')) as p:
            s.start();s.start();s.close()
        self.assertEqual(p.call_count,1);self.assertFalse(s.reaped)
        self.assertNotIn('secret',json.dumps(s.status()))

    def test_sanitization_and_report_cadence(self):
        s=cs.CollectorSupervisor()
        s._last=cs.sanitized_status({'order_permission':'none','state':'disabled','SECRET':'hidden'})
        self.assertNotIn('SECRET',s._last)
        self.assertIsNotNone(s.report_if_due());self.assertIsNone(s.report_if_due())
        s._last['heartbeat_utc']='2026-10-29T20:00:00+00:00'
        self.assertIsNone(s.report_if_due())
        s._last['attempts']=1;self.assertIsNotNone(s.report_if_due())
        with self.assertRaises(ValueError):cs.sanitized_status({'order_permission':'none','reason':'/private/path'})
        with self.assertRaises(ValueError):cs.sanitized_status({'order_permission':'none','attempts':float('nan')})

    def test_real_signal_through_actual_serve(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'production_collector').mkdir()
            (root/'production_collector'/'collector_owner.py').write_text(
                'import signal,time,json\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\n'
                'print(json.dumps({"order_permission":"none","state":"synthetic_wait"}),flush=True)\n'
                'time.sleep(60)\n')
            child=subprocess.Popen([sys.executable,str(HERE/'collector_signal_test_harness.py'),tmp],
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.until(lambda:(root/'owner.pid').exists())
                # Wait for real loop to consume the owner's ready frame before TERM.
                time.sleep(.7)
                start=time.monotonic();child.send_signal(signal.SIGTERM)
                out,err=child.communicate(timeout=8)
                self.assertEqual(child.returncode,0,err.decode())
                self.assertLess(time.monotonic()-start,5)
                result=json.loads((root/'outcome.json').read_text())
                self.assertEqual(result['web_returncode'],-signal.SIGTERM)
                self.assertTrue(result['finite_close_called'])
                final=json.loads(out.splitlines()[-1]);self.assertTrue(final['owner_reaped'])
                for name in ('web.pid','owner.pid'):
                    with self.assertRaises(ProcessLookupError):os.kill(int((root/name).read_text()),0)
            finally:
                if child.poll() is None:child.kill();child.wait()

    def test_actual_owner_default_disabled_no_bootstrap(self):
        s=cs.CollectorSupervisor();self.addCleanup(s.close)
        with patch.object(cs,'__file__',str(HERE/'collector_supervisor.py')):
            s.start()
        self.until(lambda:(s.poll(),s.reaped)[1])
        value=s.status()
        self.assertEqual(value['reason'],'owner_disabled')
        self.assertEqual(value['owner_status']['state'],'disabled')
        self.assertEqual(value['owner_status']['live_http_requests'],0)
        self.assertFalse(value['active_collection']);self.assertFalse(value['owner_status_current'])
        self.assertEqual(value['owner_exit_code'],0)

    def test_nonblocking_log_full_pipe_preserves_stdout_flags(self):
        r,w=os.pipe()
        try:
            os.set_blocking(w,False)
            while True:
                try:os.write(w,b'x'*4096)
                except BlockingIOError:break
            os.set_blocking(w,True)
            before=fcntl.fcntl(w,fcntl.F_GETFL)
            start=time.monotonic()
            self.assertFalse(cs.CollectorSupervisor.emit_report({'order_permission':'none'},fd=w))
            self.assertLess(time.monotonic()-start,.2)
            self.assertEqual(fcntl.fcntl(w,fcntl.F_GETFL),before)
            os.set_blocking(r,False)
            while True:
                try:os.read(r,65536)
                except BlockingIOError:break
            self.assertTrue(cs.CollectorSupervisor.emit_report({'order_permission':'none'},fd=w))
            self.assertEqual(json.loads(os.read(r,4096)),{'order_permission':'none'})
            self.assertEqual(fcntl.fcntl(w,fcntl.F_GETFL),before)
            self.assertFalse(cs.CollectorSupervisor.emit_report({'huge':'x'*4096},fd=w))
        finally:
            os.close(r);os.close(w)
        with tempfile.TemporaryFile() as file:
            self.assertFalse(cs.CollectorSupervisor.emit_report({'order_permission':'none'},fd=file.fileno()))
            self.assertEqual(file.tell(),0)

    def test_enrollment_terminal_receipt_visible_without_respawn(self):
        frame={'order_permission':'none','state':'enrollment_complete',
               'reason':'awaiting_collect_manifest','enrollment_receipt_sha256':'a'*64,
               'active_collection':False,'live_http_requests':0}
        s=self.spawn_fixture('import json\nprint('+repr(json.dumps(frame))+',flush=True)\n')
        self.until(lambda:(s.poll(),s.reaped)[1])
        report=s.status()
        self.assertEqual(report['reason'],'enrollment_complete')
        self.assertEqual(report['owner_status']['enrollment_receipt_sha256'],'a'*64)
        self.assertFalse(report['active_collection'])
        child=s.child;s.start();self.assertIs(s.child,child)
        for bad in ('a'*63, 'z'*64, '../private', 42):
            with self.assertRaises(ValueError):
                cs.sanitized_status({**frame,'enrollment_receipt_sha256':bad})

    def test_cached_observation_timestamps_not_refreshed_by_heartbeat(self):
        s=cs.CollectorSupervisor()
        frame={'order_permission':'none','state':'waiting_next_slot',
               'status_observed_at':'2026-10-29T19:00:00+00:00',
               'integrity_verified_at':'2026-10-29T18:59:59+00:00',
               'heartbeat_utc':'2026-10-29T19:00:00+00:00',
               'status_cached':True,'slot_grace_seconds':21600}
        s._last=cs.sanitized_status(frame)
        first=s.report_if_due();self.assertIsNotNone(first)
        s._last=cs.sanitized_status({**frame,'heartbeat_utc':'2026-10-29T19:00:30+00:00'})
        self.assertIsNone(s.report_if_due())
        s._reported_at-=61
        second=s.report_if_due();self.assertIsNotNone(second)
        for key in ('status_observed_at','integrity_verified_at'):
            self.assertEqual(second['owner_status'][key],frame[key])
        self.assertTrue(second['owner_status']['status_cached'])
        self.assertEqual(second['owner_status']['slot_grace_seconds'],21600)

    def test_fixed_spawn_contract(self):
        child=Mock();child.stdout.fileno.return_value=10
        with patch.object(cs.subprocess,'Popen',return_value=child) as p,patch.object(cs.os,'set_blocking'):
            cs.CollectorSupervisor().start()
        kw=p.call_args.kwargs
        self.assertEqual(kw['env'],{});self.assertTrue(kw['close_fds']);self.assertTrue(kw['start_new_session'])
        self.assertEqual(p.call_args.args[0][1],'-I');self.assertFalse(kw['shell'])


class ServeHook(unittest.TestCase):
    def exercise(self, *, gate=True, gate_stop=False, web_dead=False, collector_reaped=True):
        calls=[]; stop=threading.Event(); finite=Mock(reaped=True); finite.requested=False
        collector=Mock(reaped=collector_reaped)
        collector.status.return_value={'mode':'separate_collector_supervisor','order_permission':'none'}
        collector.report_if_due.return_value=None
        for name in ('start','poll','begin_stop','close'):
            getattr(collector,name).side_effect=lambda n=name:calls.append('collector_'+n)
        finite.close.side_effect=lambda:calls.append('finite_close')
        web=Mock();web.poll.return_value=1 if web_dead else None
        web.terminate.side_effect=lambda:calls.append('web_term')
        def storage(**kwargs):
            calls.append('gate')
            if gate_stop:stop.set()
            return gate
        waits=0
        def wait(_):
            nonlocal waits
            waits+=1
            return waits>2
        with contextlib.ExitStack() as stack:
            for n in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
                stack.enter_context(patch(n,side_effect=AssertionError('network forbidden')))
            stack.enter_context(patch.object(serve,'CollectorSupervisor',return_value=collector))
            stack.enter_context(patch.object(serve,'PreparedRecoverySupervisor',return_value=finite))
            stack.enter_context(patch.object(serve,'FinitePilotSupervisor',return_value=finite))
            stack.enter_context(patch.object(serve,'validate_environment',return_value=('/tmp/fake','8000')))
            stack.enter_context(patch.object(serve,'run_startup_gate',side_effect=storage))
            stack.enter_context(patch.object(serve,'_run_readonly_report',return_value=True))
            stack.enter_context(patch.object(serve,'_finite_requested',return_value=False))
            stack.enter_context(patch.object(serve.threading,'Event',return_value=stop))
            stack.enter_context(patch.object(stop,'wait',side_effect=wait))
            stack.enter_context(patch.object(serve.signal,'signal'))
            stack.enter_context(patch.object(serve.subprocess,'Popen',side_effect=lambda *a,**k:(calls.append('web_start'),web)[1]))
            stack.enter_context(patch.object(serve.successor_contract,'APPROVED_SUCCESSOR',None))
            stack.enter_context(patch.object(serve.observation_contract,'APPROVED_OBSERVATION',None))
            stack.enter_context(patch.object(serve.root_contract,'APPROVED_ROOT',None))
            stack.enter_context(patch.object(serve.finite_contract,'APPROVED_PILOT',None))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            result=serve.main()
        return result,calls,collector

    def test_old_storage_gate_precedes_any_owner_or_web(self):
        for options in ({'gate':False},{'gate_stop':True}):
            result,calls,c=self.exercise(**options)
            self.assertNotIn('collector_start',calls);self.assertNotIn('web_start',calls)
        result,calls,c=self.exercise()
        self.assertEqual(result,0);self.assertLess(calls.index('gate'),calls.index('collector_start'))
        self.assertEqual(c.start.call_count,1)

    def test_shutdown_signals_owner_web_before_finite_cleanup(self):
        result,calls,c=self.exercise()
        self.assertLess(calls.index('collector_begin_stop'),calls.index('finite_close'))
        self.assertLess(calls.index('web_term'),calls.index('finite_close'))
        self.assertLess(calls.index('finite_close'),calls.index('collector_close'))

    def test_collector_death_does_not_take_web_down(self):
        result,calls,c=self.exercise()
        self.assertEqual(result,0);self.assertEqual(c.poll.call_count,2)

    def test_unreaped_shutdown_or_web_death_is_failure(self):
        self.assertEqual(self.exercise(collector_reaped=False)[0],1)
        self.assertEqual(self.exercise(web_dead=True)[0],1)


if __name__=='__main__':unittest.main(verbosity=2)
