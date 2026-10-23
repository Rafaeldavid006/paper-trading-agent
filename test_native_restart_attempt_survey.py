"""Disposable fixtures only. Survey cannot activate/reuse a failed attempt."""
import ast
import contextlib
import io
import json
import os
from pathlib import Path
import stat
import threading
import unittest
from unittest.mock import Mock,patch

import native_restart_apply_once as launcher
import native_restart_attempt_survey as survey
import native_restart_migration as migration
import native_restart_preflight as preflight
import test_native_restart_identity as fixtures


class AttemptSurveyTests(unittest.TestCase):
    def setUp(self):
        self.case=fixtures.RestartIdentityTests();self.case.setUp();self.addCleanup(self.case.doCleanups)
        self.case.drift();self.root=self.case.fixture.root/'attempt'
        self.kw=dict(enabled=True,test_only=True,root=str(self.root),anchor=self.case.paths['anchor'])

    def inventory(self):
        result={}
        for p in self.case.fixture.root.rglob('*'):
            info=p.lstat();raw=p.read_bytes() if stat.S_ISREG(info.st_mode) else None
            result[str(p.relative_to(self.case.fixture.root))]=(info.st_mode,info.st_uid,info.st_gid,info.st_dev,info.st_ino,info.st_size,raw)
        return result

    def run_survey(self):
        before=self.inventory()
        with patch.object(migration,'apply',side_effect=AssertionError('no apply')), \
             patch.object(migration,'prepare',side_effect=AssertionError('survey is read-only only')), \
             patch.object(launcher,'run_once',side_effect=AssertionError('no launcher')):
            result=survey.run_once(**self.kw)
        self.assertEqual(before,self.inventory());self.assertFalse(result['trusted'])
        self.assertFalse(result['state_write_attempted']);self.assertEqual(result['provider_requests'],0)
        self.assertLess(len(json.dumps(result)),4096)
        self.assertNotIn(fixtures.FP,json.dumps(result));self.assertNotIn(str(self.root),json.dumps(result))
        return result

    def test_disabled_invalid_and_fixed_paths_have_no_io(self):
        with patch.object(survey,'_observe_directory',side_effect=AssertionError('no IO')):
            self.assertEqual(survey.run_once()['reason'],'attempt_survey_disabled')
            self.assertEqual(survey.run_once(enabled=True,root=str(self.root))['reason'],'attempt_survey_arguments_refused')
            self.assertEqual(survey.run_once(enabled='yes')['reason'],'attempt_survey_arguments_refused')

    def test_missing_attempt_reports_absence_without_creating_it(self):
        result=self.run_survey();self.assertFalse(result['attempt_directory']['present'])
        self.assertFalse(result['attempt_record']['present']);self.assertEqual(result['anchor']['migration_marker'],'absent')
        self.assertFalse(self.root.exists())

    def test_real_setgid_inheritance_reproduces_failure_and_survey_explains_it(self):
        parent=self.case.fixture.root/'sgid-parent';parent.mkdir(mode=0o700);parent.chmod(0o2700)
        self.root=parent/'attempt';self.kw['root']=str(self.root);plan=self.case.prepare()
        spawn=Mock(side_effect=AssertionError('no child'));messages=[];before=self.case.state()
        with patch.object(launcher,'ENABLE_APPROVED_MIGRATION',True), \
             patch.object(launcher,'EXTERNAL_WRITER_QUIESCENCE_CONFIRMED',True), \
             patch.object(migration,'apply',side_effect=AssertionError('no apply')):
            result=launcher.run_startup_gate(inspection_only=True,stop=threading.Event(),test_only=True,
                paths=self.case.paths,backup_root=str(self.root),approved_digest=plan.digest,
                expected_bytes=sum(v['size'] for v in json.loads(plan.snapshot).values()),popen=spawn,emit_report=messages.append)
        self.assertFalse(result);spawn.assert_not_called();self.assertEqual(before,self.case.state())
        self.assertEqual(messages[-1]['code'],'unsafe_attempt_directory');self.assertEqual(list(self.root.iterdir()),[])
        report=self.run_survey()
        for name in ('parent','attempt_directory'):
            self.assertEqual(report[name]['mode_octal'],'2700');self.assertTrue(report[name]['setgid'])
            self.assertTrue(report[name]['owner_matches_runtime']);self.assertTrue(report[name]['private_directory_access'])
        self.assertTrue(report['attempt_directory']['empty']);self.assertFalse(report['attempt_record']['present'])

    def test_pending_record_is_redacted_untrusted_and_not_reused(self):
        self.root.mkdir(mode=0o700);record=self.root/'attempt.json'
        record.write_bytes(launcher.bundle._json(launcher._record(survey.SNAPSHOT_DIGEST,1896794,'pending')));record.chmod(0o600)
        result=self.run_survey();self.assertEqual(result['attempt_record']['status'],'pending')
        self.assertTrue(result['attempt_record']['exact_fixed_record'])
        record.write_text('{"status":"SECRET TOKEN", "secret":"DO NOT LOG"}')
        result=self.run_survey();self.assertEqual(result['attempt_record']['status'],'unrecognized')
        self.assertFalse(result['attempt_record']['exact_fixed_record']);self.assertNotIn('SECRET',json.dumps(result))
        self.assertNotIn('DO NOT LOG',json.dumps(result))
        record.write_bytes(launcher.bundle._json(launcher._record(survey.SNAPSHOT_DIGEST,1896794,'unrecognized')))
        self.assertFalse(self.run_survey()['attempt_record']['exact_fixed_record'])

    def test_fifo_symlink_and_oversized_record_do_not_block_or_follow(self):
        self.root.mkdir(mode=0o700);record=self.root/'attempt.json';os.mkfifo(record,mode=0o600)
        result=self.run_survey();self.assertEqual(result['attempt_record']['type'],'other')
        record.unlink();record.symlink_to(self.case.paths['anchor']);result=self.run_survey()
        self.assertEqual(result['attempt_record']['type'],'symlink');record.unlink()
        record.write_bytes(b'x'*4097);record.chmod(0o600)
        self.assertEqual(self.run_survey()['attempt_record']['observation'],'invalid_or_unavailable')

    def test_symlink_root_is_observed_not_followed(self):
        self.root.symlink_to(self.case.backup,target_is_directory=True)
        result=self.run_survey();self.assertEqual(result['attempt_directory']['type'],'symlink')
        self.assertEqual(result['attempt_record']['observation'],'invalid_or_unavailable')

    def test_anchor_sidecar_is_not_opened_or_recovered(self):
        journal=Path(self.case.paths['anchor']+'-journal');journal.write_bytes(b'hot-or-unknown-journal');journal.chmod(0o600)
        with patch.object(survey.bundle,'_database',side_effect=AssertionError('no sqlite open')):
            result=self.run_survey()
        self.assertEqual(result['anchor']['observation'],'sidecar_present_not_opened')

    def test_prepared_and_completed_markers_are_observations_not_success(self):
        class Stop(Exception):pass
        def crash(phase):
            if phase=='prepared_synced':raise Stop()
        with self.assertRaises(Stop):self.case.apply(_checkpoint=crash)
        result=self.run_survey();self.assertEqual(result['anchor']['migration_marker'],'prepared')
        self.assertFalse(result['anchor']['preimage_matches_fixed']);self.assertFalse(result['execution_ready'])

    def test_completed_marker_remains_untrusted(self):
        self.case.apply();result=self.run_survey()
        self.assertEqual(result['anchor']['migration_marker'],'completed');self.assertFalse(result['trusted'])
        self.assertFalse(result['recovery_authorized'])

    def test_cli_output_is_bounded_and_wrapper_runs_existing_readers_only(self):
        result=self.run_survey();output=io.StringIO()
        with patch.object(survey,'run_once',return_value=result),patch.object(preflight,'main',return_value=0) as reader,contextlib.redirect_stdout(output):
            self.assertEqual(survey.main(['--inspect-failed-attempt-and-audit']),0)
        reader.assert_called_once_with(['--inspect-restart-preflight']);self.assertLess(len(output.getvalue()),4096)

    def test_watchdog_fixed_child_cancels_and_never_respawns(self):
        child=Mock();child.poll.return_value=None;child.wait.return_value=-9;spawn=Mock(return_value=child);timer=Mock();logs=[]
        supervisor=survey.FailedAttemptSurveySupervisor(enabled=True,popen=spawn,monotonic=lambda:0,
            timer_factory=lambda *a:timer,emit=lambda raw,**kw:logs.append(json.loads(raw)))
        supervisor.start();args,kw=spawn.call_args
        self.assertEqual(args[0][-1],'--inspect-failed-attempt-and-audit');self.assertEqual(args[0][1],'-B')
        self.assertFalse(kw['shell']);supervisor._expire();supervisor.start();supervisor.close()
        self.assertEqual(spawn.call_count,1);self.assertTrue(child.kill.called)
        self.assertTrue(all(x['provider_requests']==0 for x in logs))

    def test_real_startup_defaults_disable_old_apply_and_preserve_seven_core_hash(self):
        import serve
        self.assertIs(launcher.ENABLE_APPROVED_MIGRATION,False)
        self.assertIs(launcher.EXTERNAL_WRITER_QUIESCENCE_CONFIRMED,False)
        defaults={node.targets[0].id:ast.literal_eval(node.value) for node in ast.parse(Path(serve.__file__).read_text()).body if isinstance(node,ast.Assign) and isinstance(node.targets[0],ast.Name) and node.targets[0].id=='INSPECT_EXISTING_AUDIT_ONLY'}
        self.assertIs(defaults['INSPECT_EXISTING_AUDIT_ONLY'],True)
        self.assertIs(serve.DiagnosticAuditSupervisor,survey.FailedAttemptSurveySupervisor)
        self.assertEqual(preflight.source_digest(),survey.CORE_DIGEST)
        with patch.object(launcher,'_reserve_attempt',side_effect=AssertionError('no old apply reservation')):
            self.assertTrue(launcher.run_startup_gate(inspection_only=True,stop=threading.Event()))


if __name__=='__main__':unittest.main()
