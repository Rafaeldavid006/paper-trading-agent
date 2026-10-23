"""Actual SGID inheritance on disposable fixtures, never live permissions/state."""
import json
import multiprocessing
import os
from pathlib import Path
import stat
import threading
import unittest
from unittest.mock import Mock,patch

import native_restart_apply_once as launch
import native_restart_attempt_survey as old_survey
import native_restart_migration as migration
import test_native_restart_identity as fixtures


class SetgidRestartTests(unittest.TestCase):
    def setUp(self):
        self.case=fixtures.RestartIdentityTests();self.case.setUp();self.addCleanup(self.case.doCleanups)
        self.case.drift();self.plan=self.case.prepare()
        self.parent=self.case.fixture.root/'render-like-mount';self.parent.mkdir(mode=0o700);self.parent.chmod(0o2775)
        self.old=self.parent/'failed-attempt';self.old.mkdir(mode=0o700)
        self.new=self.parent/'fixed-attempt-2'
        self.kw=dict(test_only=True,paths=self.case.paths,backup_root=str(self.new),approved_digest=self.plan.digest,
                     expected_bytes=sum(v['size'] for v in json.loads(self.plan.snapshot).values()))
        self.enterContext(patch.object(launch,'ENABLE_APPROVED_MIGRATION',True))
        self.enterContext(patch.object(launch,'EXTERNAL_WRITER_QUIESCENCE_CONFIRMED',True))
        self.messages=[]

    def old_state(self):
        info=self.old.stat();return info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode,tuple(self.old.iterdir())

    def reserve(self):return launch._reserve_attempt(str(self.new),self.kw['approved_digest'],self.kw['expected_bytes'])
    def run_child(self,**extra):return launch.run_once(**self.kw,**extra)
    def startup(self,popen):return launch.run_startup_gate(inspection_only=True,stop=threading.Event(),popen=popen,
                                                          emit_report=self.messages.append,**self.kw)

    def test_exact_fixed_second_namespace_never_chooses_or_reuses_prior_root(self):
        self.assertEqual(launch.BACKUP_ROOT,old_survey.ROOT+'-attempt-2')
        self.assertEqual(launch.APPROVED_SNAPSHOT_DIGEST,old_survey.SNAPSHOT_DIGEST)
        self.assertNotEqual(launch.EXPECTED_CORE_CODE_DIGEST,old_survey.CORE_DIGEST)

    def test_full_actual_2775_to_2700_chain_preserves_old_root_rows_backups_and_restart(self):
        old=self.old_state();rows=self.case.row_state();files=self.case.state();child=Mock();child.poll.return_value=0
        def spawn(*args,**kwargs):
            self.assertEqual(self.run_child()['reason'],'migration_completed');return child
        spawn=Mock(side_effect=spawn)
        self.assertTrue(self.startup(spawn));self.assertEqual(spawn.call_count,1)
        for directory in (self.old,self.new,self.new/'snapshot'):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode),0o2700)
            self.assertEqual(directory.stat().st_uid,os.geteuid())
        self.assertEqual(stat.S_IMODE(self.parent.stat().st_mode),0o2775)
        self.assertEqual(old,self.old_state());self.assertEqual(rows,self.case.row_state())
        for role in self.case.paths:
            target=self.new/'snapshot'/(role+'.backup')
            self.assertEqual(target.read_bytes(),files[role][0]);self.assertEqual(stat.S_IMODE(target.stat().st_mode),0o600)
        current=self.case.state();record=(self.new/'attempt.json').read_bytes()
        with fixtures.device_shift(99):self.assertTrue(self.startup(spawn))
        self.assertEqual(spawn.call_count,1);self.assertEqual(current,self.case.state())
        self.assertEqual(record,(self.new/'attempt.json').read_bytes());self.assertEqual(old,self.old_state())

    def test_directory_policy_rejects_other_special_or_access_bits_and_keeps_owner_pin(self):
        target=self.parent/'mode-test';target.mkdir(mode=0o700)
        for mode in (0o700,0o2700):
            target.chmod(mode);fd,pin=launch._directory(str(target));os.close(fd)
        for mode in (0o1700,0o4700,0o3700,0o6700,0o2770,0o2707,0o755,0o600):
            target.chmod(mode)
            with self.assertRaises((launch.InvocationRefused,PermissionError)):
                launch._directory(str(target))
        target.chmod(0o2700);actual=os.fstat
        def wrong_owner(fd):
            info=actual(fd);values=list(info);values[4]=info.st_uid+1;return os.stat_result(values)
        with patch.object(os,'fstat',side_effect=wrong_owner):
            with self.assertRaisesRegex(launch.InvocationRefused,'unsafe_attempt_directory'):launch._directory(str(target))

    def test_snapshot_mismatch_consumes_only_new_fence_and_never_touches_old_root(self):
        old=self.old_state();self.reserve()
        import sqlite3
        with sqlite3.connect(self.case.paths['runner']) as db:db.execute("UPDATE runner_control SET attention='changed-current-state'")
        before=self.case.state();result=self.run_child()
        self.assertEqual(result['code'],'approved_snapshot_changed');self.assertEqual(before,self.case.state())
        self.assertEqual(old,self.old_state());self.assertFalse((self.new/'snapshot').exists())
        self.assertFalse(self.startup(Mock(side_effect=AssertionError('no repeat'))))

    def test_legacy_empty_root_never_becomes_eligible_under_new_mode_rule(self):
        old=self.old_state();kw=dict(self.kw,backup_root=str(self.old))
        self.assertFalse(launch.run_startup_gate(inspection_only=True,stop=threading.Event(),
            popen=Mock(side_effect=AssertionError('never reuse')),emit_report=self.messages.append,**kw))
        self.assertEqual(old,self.old_state())

    def test_backup_initial_check_rejects_unexpected_special_bits_before_metadata(self):
        before=self.case.state()
        for mode in (0o1700,0o4700,0o3700,0o2770):
            self.case.backup.chmod(mode)
            with self.assertRaisesRegex(migration.MigrationRefused,'restart_migration_backup_invalid'):
                self.case.apply(self.plan)
            self.assertEqual(before,self.case.state());self.assertEqual(list(self.case.backup.iterdir()),[])

    def test_backup_recheck_rejects_new_special_bit_before_cohort_change(self):
        self.case.backup.chmod(0o2700);before=self.case.state();write=migration.b.RuntimeStateBundle._write
        def changed(fd,raw):
            write(fd,raw);self.case.backup.chmod(0o3700)
        with patch.object(migration.b.RuntimeStateBundle,'_write',side_effect=changed):
            with self.assertRaisesRegex(migration.MigrationRefused,'restart_migration_backup_changed'):
                self.case.apply(self.plan)
        self.assertEqual(before,self.case.state())

    def test_real_crash_after_guard_stamp_is_consumed_and_preserves_old_root(self):
        old=self.old_state();rows=self.case.row_state();self.reserve()
        def child():
            def die(phase):
                if phase=='stamp_guard_synced':os._exit(73)
            self.run_child(_checkpoint=die);os._exit(74)
        process=multiprocessing.get_context('fork').Process(target=child);process.start();process.join(10)
        if process.is_alive():process.kill();process.join();self.fail('fixture exceeded bound')
        self.assertEqual(process.exitcode,73);self.assertEqual(rows,self.case.row_state());self.assertEqual(old,self.old_state())
        self.assertEqual(json.loads((self.new/'attempt.json').read_bytes())['status'],'started')
        self.assertFalse(self.startup(Mock(side_effect=AssertionError('no retry'))));self.assertEqual(old,self.old_state())


if __name__=='__main__':unittest.main()
