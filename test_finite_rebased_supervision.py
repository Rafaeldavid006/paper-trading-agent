"""Local finite child lifecycle regression. No real process/provider access."""
from contextlib import ExitStack
from dataclasses import replace
import json
import subprocess
import unittest
from unittest.mock import Mock, patch

with ExitStack() as blocked:
    for name in ('socket.socket','socket.create_connection','socket.getaddrinfo','subprocess.Popen'):
        blocked.enter_context(patch(name,side_effect=AssertionError('OFFLINE ONLY')))
    import finite_pilot_contract as contract
    import finite_pilot_worker as worker
    from finite_pilot_supervisor import FinitePilotSupervisor
    from native_account_guard import AccountGuardRefused
    from native_observation_collector import CollectionRefused
    import test_finite_startup_integration as startup_fixture


class Child:
    def __init__(self):
        self.code=None;self.kills=0;self.waits=[];self.stuck=False
    def poll(self):return self.code
    def kill(self):
        self.kills+=1
        if not self.stuck:self.code=-9
    def wait(self,timeout=None):
        self.waits.append(timeout)
        if self.stuck:raise subprocess.TimeoutExpired('FIXED',timeout)
        return self.code


class Timer:
    def __init__(self,interval,callback):
        self.interval=interval;self.callback=callback;self.daemon=False;self.cancelled=False
    def start(self):pass
    def cancel(self):self.cancelled=True


class FiniteLifecycleTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
            self.enterContext(patch(name,side_effect=AssertionError('OFFLINE ONLY')))
        self.pilot,self.recovery,self.proof=startup_fixture.StartupSelectionTests.grants(self)
        self.now=100.;self.monotonic=0.;self.children=[];self.timers=[];self.logs=[]

    def make(self,*,pilot=False,recovery=False,proof=False,enabled=True,popen=None):
        for name,grant in (('APPROVED_PILOT',self.pilot if pilot else None),
                           ('APPROVED_RECOVERY',self.recovery if recovery else None),
                           ('APPROVED_INSPECTION',self.proof if proof else None)):
            self.enterContext(patch.object(contract,name,grant))
        def spawn(*args,**kw):
            child=Child();self.children.append(child);return child
        def timer(*args):
            timer=Timer(*args);self.timers.append(timer);return timer
        self.spawn=Mock(side_effect=popen or spawn)
        supervisor=FinitePilotSupervisor(enabled=enabled,popen=self.spawn,
            monotonic=lambda:self.monotonic,wall_time_ns=lambda:int(self.now*contract.NS),
            timer_factory=timer,emit=lambda text,**kw:self.logs.append(json.loads(text)))
        self.addCleanup(supervisor.close)
        return supervisor

    def test_default_is_disabled_even_when_enabled_true(self):
        s=self.make();s.start();s.poll();s.close();self.spawn.assert_not_called()

    def test_recovery_only_completes_once_without_initialize_or_provider_loop(self):
        s=self.make(recovery=True);s.start()
        self.assertEqual(self.spawn.call_args.args[0][-1],'--recover')
        self.assertEqual(self.timers[0].interval,15.)
        self.children[-1].code=0;s.poll()
        self.assertTrue(s.completed);self.assertFalse(s.initialized);self.assertTrue(s.reaped)
        for _ in range(5):s.start();s.poll()
        self.assertEqual(self.spawn.call_count,1)

    def test_combined_recovery_precedes_pilot_and_serializes_every_child(self):
        s=self.make(pilot=True,recovery=True);s.start();s.start();s.poll()
        self.assertEqual(self.spawn.call_count,1)
        self.children[-1].code=0;s.poll()
        self.assertEqual(self.spawn.call_args.args[0][-1],'--initialize')
        self.children[-1].code=0;s.poll()
        self.assertEqual(self.spawn.call_args.args[0][-1],'--source')
        self.children[-1].code=0;s.poll()
        self.assertEqual(self.spawn.call_args.args[0][-1],'--tick')
        self.assertEqual([call.args[0][-1] for call in self.spawn.call_args_list],
                         ['--recover','--initialize','--source','--tick'])
        for call in self.spawn.call_args_list:
            self.assertFalse(call.kwargs['shell']);self.assertTrue(call.kwargs['close_fds'])
            self.assertEqual(call.kwargs['stderr'],subprocess.DEVNULL)

    def test_crash_halts_without_automatic_repeat(self):
        s=self.make(pilot=True);s.start();self.children[0].code=-9;s.poll()
        for _ in range(5):s.start();s.poll()
        self.assertTrue(s.halted);self.assertEqual(self.spawn.call_count,1)

    def test_timeout_is_independent_and_stale_callback_cannot_kill_next_child(self):
        s=self.make(pilot=True);s.start();old=self.timers[0]
        self.children[0].code=0;s.poll();self.assertEqual(len(self.children),2)
        old.callback();self.assertEqual(self.children[-1].kills,0)
        self.timers[-1].callback()
        self.assertTrue(s.halted);self.assertEqual(self.children[-1].kills,1)
        s.poll();s.start();self.assertEqual(self.spawn.call_count,2)

    def test_cancellation_kills_and_bounded_reap_never_restarts(self):
        s=self.make(recovery=True);s.start();child=self.children[0];child.stuck=True
        s.close();s.close();s.poll();s.start()
        self.assertEqual(child.kills,1);self.assertEqual(child.waits,[1.])
        self.assertFalse(s.reaped)
        self.assertEqual(self.spawn.call_count,1)
        self.assertIn('reap_pending',[r['reason'] for r in self.logs])

    def test_expired_pilot_has_no_child_and_changed_approval_kills_current(self):
        s=self.make(pilot=True);self.now=200.;s.start()
        self.assertTrue(s.halted);self.spawn.assert_not_called()
        self.now=100.;s=self.make(pilot=True);s.start()
        with patch.object(contract,'APPROVED_PILOT',replace(self.pilot)):
            s.poll()
        self.assertTrue(s.halted);self.assertEqual(self.children[-1].kills,1)
        self.assertEqual(self.spawn.call_count,1)

    def test_spawn_failure_never_retries_or_leaks_exception(self):
        def fail(*args,**kw):raise RuntimeError('PRIVATE KEY ACCOUNT CLIENT')
        s=self.make(recovery=True,popen=fail);s.start();s.poll();s.start()
        self.assertTrue(s.halted);self.assertEqual(self.spawn.call_count,1)
        self.assertNotIn('PRIVATE',json.dumps(self.logs))

    def test_terminal_report_never_claims_uncertain_child_is_reaped(self):
        import serve
        s=self.make(recovery=True);s.start();self.children[0].stuck=True;s.close()
        with patch('builtins.print') as emit:
            serve._finite_terminal(s)
        report=json.loads(emit.call_args.args[0])
        self.assertFalse(report['finite_child_reaped'])
        self.assertFalse(report['inspection_health_only'])
        self.assertFalse(report['automatic_retry'])

    def test_poll_fault_is_killed_not_replayed(self):
        s=self.make(pilot=True);s.start()
        self.children[0].poll=Mock(side_effect=RuntimeError('PRIVATE'))
        s.poll();s.close();s.start()
        self.assertTrue(s.halted);self.assertEqual(self.children[0].kills,1)
        self.assertEqual(self.spawn.call_count,1)


class SafeWorkerErrorTests(unittest.TestCase):
    def test_allowlisted_codes_and_requested_stage_only(self):
        cases=((contract.PilotRefused('recovery_incomplete'),'recovery_incomplete'),
               (AccountGuardRefused('legacy_reserved_absence_unproven'),'legacy_reserved_absence_unproven'),
               (CollectionRefused('native_schema_refused'),'native_schema_refused'),
               (TimeoutError('PRIVATE ACCOUNT'),'operation_timeout'),
               (contract.PilotRefused('PRIVATE KEY CLIENT'),'unclassified_failure'),
               (RuntimeError('credentials_missing'),'unclassified_failure'))
        for exc,code in cases:
            with self.subTest(code=code),patch.object(worker,'run_phase',side_effect=exc),patch('builtins.print') as emit:
                self.assertEqual(worker.main(['--recover']),1)
                result=json.loads(emit.call_args.args[0])
                self.assertEqual(result['error_stage'],'recover');self.assertEqual(result['error_code'],code)
                self.assertNotIn('PRIVATE',json.dumps(result));self.assertFalse(result['execution_ready'])

    def test_exception_subclass_and_nonstring_args_never_render(self):
        class Malicious(contract.PilotRefused):
            def __str__(self):raise AssertionError('MUST NOT FORMAT')
        for exc in (Malicious('credentials_missing'),contract.PilotRefused({'SECRET':'value'})):
            self.assertEqual(worker.failure('tick',exc)['error_code'],'unclassified_failure')

    def test_public_error_vocabulary_is_closed(self):
        for kwargs in (dict(error_stage='private',error_code='credentials_missing'),
                       dict(error_stage='tick',error_code='private'),dict(error_stage='tick')):
            with self.assertRaises(contract.PilotRefused):
                worker.public('manual_handoff','pilot_stage_refused',**kwargs)


if __name__=='__main__':unittest.main()
