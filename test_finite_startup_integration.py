"""Offline startup selection; no actual child, credential access or state reset."""
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

with ExitStack() as blocked:
    for name in ('socket.socket','socket.create_connection','socket.getaddrinfo','subprocess.Popen'):
        blocked.enter_context(patch(name, side_effect=AssertionError('OFFLINE ONLY')))
    import serve
    import readonly_runtime_worker as readonly
    from finite_pilot_contract import PilotRefused
    from test_experiment_supervision import Child, Stop


class StartupSelectionTests(unittest.TestCase):
    """Rebased startup: existing readonly web, no legacy/provider side effects."""
    def grants(self):
        c=serve.finite_contract
        common=dict(starts_ns=100*c.NS,expires_ns=200*c.NS,bundle_generation='a'*64,
            bundle_seal_sha256='b'*64,anchor_pre_enrollment_sha256='c'*64,
            pre_enrollment_cohort_sha256='d'*64,approval_reference='SYNTHETIC')
        pilot=c.PilotManifest(pilot_id='SYNTHETIC',symbols=('NVDA',),max_ticks=2,
            max_write_attempts=2,max_source_requests=4,approval_artifact_sha256='e'*64,**common)
        recovery=c.RecoveryManifest(incident_id=c.INCIDENT_ID,original_runner_sha256='e'*64,
            protected_state_sha256='f'*64,diagnostic_artifact_sha256='a'*64,
            diagnostic_result_sha256='b'*64,reviewed_fix_sha256='c'*64,
            review_authorization_sha256='d'*64,**common)
        proof=c.InspectionManifest(c.INSPECTION_GENERATION,100*c.NS,200*c.NS,'a'*64,'b'*64,'SYNTHETIC')
        return pilot,recovery,proof

    def exercise(self, *, pilot=None, recovery=None, inspection=None, completed=False,
                 halted=False, gate=True, stop_at_gate=False):
        with ExitStack() as s:
            for name in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
                s.enter_context(patch(name,side_effect=AssertionError('OFFLINE ONLY')))
            s.enter_context(patch.object(serve.finite_contract,'APPROVED_PILOT',pilot))
            s.enter_context(patch.object(serve.finite_contract,'APPROVED_RECOVERY',recovery))
            s.enter_context(patch.object(serve.finite_contract,'APPROVED_INSPECTION',inspection))
            s.enter_context(patch.object(serve,'validate_environment',return_value=('/tmp/fictional-state','8000')))
            s.enter_context(patch.dict(serve.os.environ,{'ENABLE_AUTO_RESEARCH':'true'}))
            stop=Stop(5);s.enter_context(patch.object(serve.threading,'Event',return_value=stop))
            s.enter_context(patch.object(serve.signal,'signal'))
            calls=[]
            def startup(**kw):
                self.assertEqual(kw,dict(inspection_only=True,stop=stop));calls.append('gate')
                if stop_at_gate:stop.set()
                return gate
            s.enter_context(patch.object(serve,'run_startup_gate',side_effect=startup))
            def spawn(*args,**kw):calls.append('web');return Child()
            processes=s.enter_context(patch.object(serve.subprocess,'Popen',side_effect=spawn))
            retired=[s.enter_context(patch.object(serve,name)) for name in
                ('ReadonlyRuntimeSupervisor','SchemaDiagnosticSupervisor','NativeProbeSupervisor',
                 'GetDiagnosticSupervisor','DiagnosticAuditSupervisor','record_experiment_failure')]
            finite=s.enter_context(patch.object(serve,'FinitePilotSupervisor'))
            finite.return_value.completed=completed;finite.return_value.halted=halted
            terminal=s.enter_context(patch.object(serve,'_finite_terminal'))
            s.enter_context(patch('builtins.print'))
            result=serve.main()
        for old in retired:old.assert_not_called()
        if gate and not stop_at_gate:
            processes.assert_called_once()
            self.assertEqual(calls,['gate','web'])
            self.assertEqual(processes.call_args.args[0][-1],'inspection_wsgi:app')
            self.assertFalse(processes.call_args.kwargs['shell'])
        else:
            processes.assert_not_called();finite.return_value.start.assert_not_called()
        return result,finite,terminal

    def test_absent_grants_launch_only_readonly_health(self):
        result,finite,terminal=self.exercise()
        self.assertEqual(result,0);finite.assert_called_once_with(enabled=False)
        finite.return_value.start.assert_not_called();finite.return_value.poll.assert_not_called()
        terminal.assert_not_called()

    def test_exact_pilot_recovery_proof_and_combined_start_once(self):
        pilot,recovery,proof=self.grants()
        for grants in (dict(pilot=pilot),dict(recovery=recovery),dict(inspection=proof),
                       dict(pilot=pilot,recovery=recovery)):
            with self.subTest(grants=tuple(grants)):
                result,finite,_=self.exercise(**grants)
                self.assertEqual(result,0);finite.assert_called_once_with(enabled=True)
                finite.return_value.start.assert_called_once()

    def test_terminal_one_shot_keeps_health_but_stops_polling(self):
        pilot,recovery,proof=self.grants()
        for grants in (dict(recovery=recovery),dict(inspection=proof),dict(pilot=pilot)):
            for completed,halted in ((True,False),(False,True)):
                with self.subTest(grants=tuple(grants),halted=halted):
                    result,finite,terminal=self.exercise(**grants,completed=completed,halted=halted)
                    self.assertEqual(result,0);finite.return_value.poll.assert_called_once()
                    terminal.assert_called_once_with(finite.return_value)
                    self.assertTrue(finite.return_value.close.called)

    def test_failed_or_cancelled_storage_gate_precedes_every_child(self):
        pilot,_,_=self.grants()
        self.assertEqual(self.exercise(pilot=pilot,gate=False)[0],1)
        self.assertEqual(self.exercise(pilot=pilot,stop_at_gate=True)[0],0)

    def test_forged_or_mixed_grants_refuse_before_storage_or_child(self):
        pilot,_,proof=self.grants()
        for grants in (dict(APPROVED_PILOT=True),dict(APPROVED_RECOVERY=object()),
                       dict(APPROVED_INSPECTION=proof,APPROVED_PILOT=pilot)):
            with self.subTest(grants=tuple(grants)), ExitStack() as s:
                for key in ('APPROVED_PILOT','APPROVED_RECOVERY','APPROVED_INSPECTION'):
                    s.enter_context(patch.object(serve.finite_contract,key,grants.get(key)))
                s.enter_context(patch.object(serve,'validate_environment',return_value=('/tmp/fictional','8000')))
                gate=s.enter_context(patch.object(serve,'run_startup_gate'))
                spawn=s.enter_context(patch.object(serve.subprocess,'Popen'))
                s.enter_context(patch('builtins.print'))
                self.assertEqual(serve.main(),1)
                gate.assert_not_called();spawn.assert_not_called()


class ReadonlyGateOrderingTests(unittest.TestCase):
    def test_pending_gate_blocks_all_phases_before_constructors_or_credentials(self):
        for phase in ('initialize','source','broker'):
            with self.subTest(phase=phase), ExitStack() as s:
                for name in ('socket.socket','socket.create_connection','socket.getaddrinfo','subprocess.Popen'):
                    s.enter_context(patch(name,side_effect=AssertionError('OFFLINE ONLY')))
                s.enter_context(patch.object(readonly,'load_owned_ledger',return_value=SimpleNamespace(account_fingerprint='a'*64)))
                bundle=s.enter_context(patch.object(readonly,'RuntimeStateBundle'))
                gate=s.enter_context(patch.object(readonly,'read_only_startup_gate',side_effect=PilotRefused('pending')))
                components=s.enter_context(patch.object(readonly,'components',side_effect=AssertionError('NO CONSTRUCTORS')))
                env=Mock();env.get.side_effect=AssertionError('NO CREDENTIALS')
                with self.assertRaises(PilotRefused):
                    readonly.run_phase(phase,environ=env)
                gate.assert_called_once_with(bundle.return_value)
                bundle.return_value.initialize_once.assert_not_called()
                components.assert_not_called();env.get.assert_not_called()

if __name__=='__main__':unittest.main()
