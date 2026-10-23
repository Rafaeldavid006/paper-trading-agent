"""Synthetic disposable states only. No live paths, publication or providers."""
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import subprocess
import threading
import unittest
from unittest.mock import Mock,patch

import native_restart_apply_once as apply_once
import native_restart_migration as migration
import test_native_restart_identity as fixtures


class ApplyInvocationTests(unittest.TestCase):
    def setUp(self):
        self.case=fixtures.RestartIdentityTests();self.case.setUp();self.addCleanup(self.case.doCleanups)
        self.case.drift();self.plan=self.case.prepare();self.root=self.case.fixture.root/'approved-identity-backup'
        self.kw=dict(test_only=True,paths=self.case.paths,backup_root=str(self.root),
                     approved_digest=self.plan.digest,expected_bytes=sum(v['size'] for v in json.loads(self.plan.snapshot).values()))
        self.enterContext(patch.object(apply_once,'ENABLE_APPROVED_MIGRATION',True))
        self.enterContext(patch.object(apply_once,'EXTERNAL_WRITER_QUIESCENCE_CONFIRMED',True))
        self.messages=[]

    def reserve(self):return apply_once._reserve_attempt(str(self.root),self.kw['approved_digest'],self.kw['expected_bytes'])

    def run_child(self,**extra):return apply_once.run_once(**self.kw,**extra)

    def startup(self,**extra):
        return apply_once.run_startup_gate(inspection_only=True,stop=extra.pop('stop',threading.Event()),
            emit_report=self.messages.append,**self.kw,**extra)

    def successful_spawn(self,args,**options):
        self.assertEqual(args[1],'-B');self.assertEqual(args[-1],'--apply-approved-once')
        self.assertFalse(options['shell']);self.assertTrue(options['close_fds'])
        result=self.run_child();self.assertEqual(result['reason'],'migration_completed')
        child=Mock();child.poll.return_value=0;return child

    def test_disabled_has_zero_io_and_no_child_or_approval_inference(self):
        with patch.object(apply_once,'ENABLE_APPROVED_MIGRATION',False), \
             patch.object(apply_once,'_approved',side_effect=AssertionError('no IO')), \
             patch.object(apply_once,'_reserve_attempt',side_effect=AssertionError('no write')):
            self.assertTrue(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        with patch.object(apply_once,'EXTERNAL_WRITER_QUIESCENCE_CONFIRMED',False), \
             patch.object(apply_once.preflight,'source_digest',side_effect=AssertionError('no source read')):
            self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        self.assertFalse(self.root.exists())

    def test_frozen_core_digest_matches_live_and_tamper_refuses_before_reservation(self):
        self.assertEqual(apply_once.preflight.source_digest(),apply_once.EXPECTED_CORE_CODE_DIGEST)
        with patch.object(apply_once.preflight,'source_digest',return_value='0'*64):
            self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        self.assertFalse(self.root.exists());self.assertEqual(self.messages[-1]['code'],'frozen_code_mismatch')

    def test_success_preserves_rows_and_backup_and_is_readonly_on_remounted_restart(self):
        rows=self.case.row_state();before=self.case.state();spawn=Mock(side_effect=self.successful_spawn)
        self.assertTrue(self.startup(popen=spawn));self.assertEqual(spawn.call_count,1)
        self.assertEqual(rows,self.case.row_state());self.assertTrue(self.case.fixture.make().verify())
        for role,path in self.case.paths.items():
            self.assertEqual((self.root/'snapshot'/(role+'.backup')).read_bytes(),before[role][0])
        complete=self.case.state();record=(self.root/'attempt.json').read_bytes()
        self.assertEqual(json.loads(record)['status'],'completed')
        with fixtures.device_shift(77):self.assertTrue(self.startup(popen=spawn))
        self.assertEqual(spawn.call_count,1);self.assertEqual(complete,self.case.state())
        self.assertEqual(record,(self.root/'attempt.json').read_bytes())
        self.assertEqual(self.run_child()['code'],'attempt_already_started')
        self.assertEqual(complete,self.case.state())

    def test_wrong_snapshot_or_size_consumes_attempt_without_touching_original_state(self):
        for mismatch in ('digest','size'):
            if self.root.exists():break  # Each refusal remains permanently consumed.
            with sqlite3.connect(self.case.paths['runner']) as db:db.execute("UPDATE runner_control SET attention='new-incident'")
            before=self.case.state();self.reserve();result=self.run_child()
            self.assertEqual(result['code'],'approved_snapshot_changed')
            self.assertEqual(before,self.case.state());self.assertFalse((self.root/'snapshot').exists())
            self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no retry'))))
        # Separate prepared-size mismatch remains an explicit fail-closed check.
        other=self.case.fixture.root/'other-size';kw=dict(self.kw,backup_root=str(other),expected_bytes=self.kw['expected_bytes']+1)
        changed=self.case.prepare();kw['approved_digest']=changed.digest
        apply_once._reserve_attempt(str(other),kw['approved_digest'],kw['expected_bytes'])
        before=self.case.state();result=apply_once.run_once(**kw)
        self.assertEqual(result['code'],'approved_snapshot_size_changed');self.assertEqual(before,self.case.state())

    def test_free_space_and_inode_shortages_never_resize_or_spawn(self):
        for bytes_free,inodes,code in ((0,100,'insufficient_free_space'),(apply_once.MIN_FREE_BYTES,0,'insufficient_free_inodes')):
            info=Mock(f_frsize=1,f_bavail=bytes_free,f_favail=inodes)
            with patch.object(os,'fstatvfs',return_value=info):
                self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
            self.assertEqual(self.messages[-1]['code'],code);self.assertFalse(self.root.exists())

    def test_preexisting_empty_partial_or_foreign_attempt_blocks_web_and_never_retries(self):
        self.root.mkdir(mode=0o700);before=self.case.state()
        self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        self.assertEqual(before,self.case.state());self.assertEqual(list(self.root.iterdir()),[])

    def test_completed_claim_alone_does_not_authorize_startup(self):
        self.reserve();(self.root/'attempt.json').write_bytes(apply_once.bundle._json(
            apply_once._record(self.kw['approved_digest'],self.kw['expected_bytes'],'completed')))
        before=self.case.state();self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        self.assertEqual(before,self.case.state())

    def test_spawn_failure_is_durably_consumed_and_redacted(self):
        before=self.case.state();spawn=Mock(side_effect=RuntimeError('private credentials raw text'))
        self.assertFalse(self.startup(popen=spawn));self.assertEqual(spawn.call_count,1)
        self.assertTrue(self.root.is_dir());self.assertEqual(json.loads((self.root/'attempt.json').read_bytes())['status'],'pending')
        self.assertFalse(self.startup(popen=spawn));self.assertEqual(spawn.call_count,1)
        self.assertEqual(before,self.case.state());self.assertNotIn('credentials',json.dumps(self.messages))

    def test_watchdog_reap_and_cancel_all_prevent_web_and_restart_retry(self):
        child=Mock();child.poll.return_value=None;child.wait.return_value=-9
        spawn=Mock(return_value=child);clock=Mock(side_effect=[0,31])
        self.assertFalse(self.startup(popen=spawn,monotonic=clock))
        child.kill.assert_called_once();child.wait.assert_called_once_with(timeout=1)
        self.assertEqual(self.messages[-1]['reason'],'apply_watchdog_expired')
        self.assertFalse(self.startup(popen=spawn));self.assertEqual(spawn.call_count,1)

    def test_uncertain_reap_refuses_startup_without_another_spawn(self):
        child=Mock();child.poll.return_value=None;child.wait.side_effect=subprocess.TimeoutExpired('fixed',1)
        spawn=Mock(return_value=child)
        self.assertFalse(self.startup(popen=spawn,monotonic=Mock(side_effect=[0,31])))
        self.assertEqual(self.messages[-1]['reason'],'apply_reap_uncertain')
        self.assertFalse(self.startup(popen=spawn));self.assertEqual(spawn.call_count,1)

    def test_cancel_before_or_after_spawn_never_starts_web(self):
        stop=threading.Event();stop.set();spawn=Mock()
        self.assertFalse(self.startup(stop=stop,popen=spawn));spawn.assert_not_called();self.assertFalse(self.root.exists())
        stop.clear();child=Mock();child.poll.return_value=None;child.wait.return_value=-9
        def spawn_cancel(*args,**kwargs):stop.set();return child
        self.assertFalse(self.startup(stop=stop,popen=spawn_cancel));child.kill.assert_called_once()
        self.assertEqual(self.messages[-1]['reason'],'apply_cancelled')

    def test_cancellation_during_reservation_or_completed_verification_stops_startup(self):
        stop=threading.Event();reserve=apply_once._reserve_attempt
        def reserve_cancel(*args):
            result=reserve(*args);stop.set();return result
        with patch.object(apply_once,'_reserve_attempt',side_effect=reserve_cancel):
            self.assertFalse(self.startup(stop=stop,popen=Mock(side_effect=AssertionError('no child'))))
        self.assertTrue(self.root.exists())
        stop.clear()
        def verified_cancel(*args,**kwargs):stop.set();return True
        with patch.object(apply_once,'_completed_attempt',side_effect=verified_cancel):
            self.assertFalse(self.startup(stop=stop,popen=Mock(side_effect=AssertionError('no child'))))

    def test_fifo_and_symlink_attempt_records_refuse_without_blocking_or_following(self):
        self.root.mkdir(mode=0o700);record=self.root/'attempt.json';before=self.case.state()
        os.mkfifo(record,mode=0o600)
        self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        self.assertEqual(self.messages[-1]['code'],'unsafe_attempt_record')
        self.assertEqual(self.run_child()['code'],'unsafe_attempt_record')
        record.unlink();record.symlink_to(self.case.paths['anchor'])
        self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        self.assertEqual(before,self.case.state())
        self.assertEqual(self.messages[-1]['metadata_outcome'],'unknown')

    def test_corrupt_or_substituted_completed_cohort_never_uses_claim_as_bypass(self):
        self.assertTrue(self.startup(popen=self.successful_spawn))
        with sqlite3.connect(self.case.paths['guard']) as db:db.execute('PRAGMA user_version=0')
        before=self.case.state();self.assertFalse(self.startup(popen=Mock(side_effect=AssertionError('no child'))))
        self.assertEqual(before,self.case.state())

    def test_real_child_crashes_never_repeat_even_after_metadata_completion(self):
        # Fixture for each real crash is separate; no cleanup/recovery/reuse.
        for phase in ('prepared_synced','stamp_guard_synced','seal_synced','complete_synced'):
            case=fixtures.RestartIdentityTests();case.setUp()
            try:
                case.drift();plan=case.prepare();root=case.fixture.root/'attempt'
                kw=dict(test_only=True,paths=case.paths,backup_root=str(root),approved_digest=plan.digest,
                        expected_bytes=sum(v['size'] for v in json.loads(plan.snapshot).values()))
                apply_once._reserve_attempt(str(root),plan.digest,kw['expected_bytes']);rows=case.row_state()
                def child():
                    def die(where):
                        if where==phase:os._exit(77)
                    apply_once.run_once(**kw,_checkpoint=die);os._exit(78)
                process=multiprocessing.get_context('fork').Process(target=child);process.start();process.join(12)
                if process.is_alive():process.kill();process.join();self.fail('crash fixture timeout')
                self.assertEqual(process.exitcode,77);self.assertEqual(rows,case.row_state())
                self.assertEqual(json.loads((root/'attempt.json').read_bytes())['status'],'started')
                before=case.state()
                self.assertFalse(apply_once.run_startup_gate(inspection_only=True,stop=threading.Event(),
                    popen=Mock(side_effect=AssertionError('no replay')),emit_report=self.messages.append,**kw))
                self.assertEqual(before,case.state())
            finally:case.doCleanups()

    def test_production_overrides_and_invalid_cli_refuse_without_apply(self):
        result=apply_once.run_once(paths=self.case.paths)
        self.assertEqual(result['code'],'fixed_invocation_required')
        with patch.object(apply_once,'run_once') as run,patch.object(apply_once,'emit'):
            self.assertEqual(apply_once.main(['--force']),1);run.assert_not_called()


class StartupOrderingTests(unittest.TestCase):
    def test_gate_runs_before_any_web_or_worker_spawn_and_failure_blocks_all(self):
        import serve
        stop=Mock();stop.is_set.return_value=False;stop.wait.side_effect=[True]
        events=[]
        def gate(**kwargs):events.append('gate');return False
        with patch.object(serve,'INSPECT_EXISTING_AUDIT_ONLY',True), \
             patch.object(serve,'validate_environment',return_value=('/tmp/events.db','8000')), \
             patch.object(serve.threading,'Event',return_value=stop),patch.object(serve.signal,'signal'), \
             patch.object(serve,'run_startup_gate',side_effect=gate), \
             patch.object(serve.subprocess,'Popen') as spawn, \
             patch.object(serve,'NativeProbeSupervisor') as probe,patch.object(serve,'ReadonlyRuntimeSupervisor') as runtime, \
             patch.object(serve,'SchemaDiagnosticSupervisor') as diagnostic,patch.object(serve,'DiagnosticAuditSupervisor') as audit:
            self.assertEqual(serve.main(),1)
        self.assertEqual(events,['gate']);spawn.assert_not_called()
        for worker in (probe,runtime,diagnostic,audit):worker.return_value.start.assert_not_called()


if __name__=='__main__':unittest.main()
