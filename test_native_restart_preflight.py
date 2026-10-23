"""Preflight uses only disposable files. No actual startup/provider processes."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import select
import sqlite3
import unittest
from unittest.mock import Mock,patch

import native_restart_migration as migration
import native_restart_preflight as preflight
import test_native_restart_identity as fixtures
from native_diagnostic_audit_supervisor import DiagnosticAuditSupervisor


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.case=fixtures.RestartIdentityTests();self.case.setUp();self.addCleanup(self.case.doCleanups)
        self.case.drift()

    def run_preflight(self):
        return preflight.run_once(enabled=True,paths=self.case.paths,test_only=True)

    def test_disabled_and_invalid_arguments_never_inspect(self):
        with patch.object(migration,'prepare',side_effect=AssertionError('no filesystem')):
            self.assertEqual(preflight.run_once()['reason'],'preflight_disabled')
            self.assertEqual(preflight.run_once(enabled=True,paths=self.case.paths)['code'],'invalid_arguments')
            self.assertEqual(preflight.run_once(enabled='true')['code'],'invalid_arguments')

    def test_success_is_bounded_redacted_nonmutating_and_not_execution_permission(self):
        before=self.case.state();directory=sorted(str(p) for p in self.case.fixture.root.rglob('*'))
        with patch.object(migration,'apply',side_effect=AssertionError('no apply')), \
             patch.object(migration,'_backup',side_effect=AssertionError('no backup')), \
             patch('native_paper_execution.NativePaperHTTP',side_effect=AssertionError('no provider')):
            result=self.run_preflight()
        self.assertTrue(result['snapshot_verified']);self.assertTrue(result['writer_exclusion_during_snapshot'])
        for key in ('global_quiescence_proven','continuing_quiescence_proven','execution_ready',
                    'historical_continuity_proven','state_write_attempted','backup_created','migration_applied'):
            self.assertIs(result[key],False)
        self.assertEqual((result['verified_file_count'],result['verified_database_count']),(10,8))
        self.assertEqual(result['approval_snapshot_digest'],self.case.prepare().digest)
        raw=json.dumps(result);self.assertLess(len(raw),4096)
        self.assertNotIn(fixtures.FP,raw)
        for path in self.case.paths.values():self.assertNotIn(path,raw)
        self.assertEqual(before,self.case.state())
        self.assertEqual(directory,sorted(str(p) for p in self.case.fixture.root.rglob('*')))
        self.assertEqual(list(self.case.backup.iterdir()),[])

    def test_writer_exclusion_is_actually_held_for_every_database_and_lock(self):
        original=migration._make_plan;observed=[]
        def inspect(bundle,current,dbs):
            for role in migration.DB_ROLES:
                other=sqlite3.connect(self.case.paths[role],timeout=0,isolation_level=None)
                try:
                    with self.assertRaises(sqlite3.OperationalError) as exc:other.execute('BEGIN IMMEDIATE')
                    self.assertEqual(exc.exception.sqlite_errorcode,sqlite3.SQLITE_BUSY)
                finally:other.close()
                self.assertTrue(dbs[role].in_transaction);observed.append(role)
            for role in ('seal','legacy_lock'):
                fd=os.open(self.case.paths[role],os.O_RDONLY)
                try:
                    with self.assertRaises(BlockingIOError):fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                finally:os.close(fd)
            return original(bundle,current,dbs)
        with patch.object(migration,'_make_plan',side_effect=inspect):result=self.run_preflight()
        self.assertTrue(result['snapshot_verified']);self.assertEqual(set(observed),set(migration.DB_ROLES))
        # The success must not imply a continuing maintenance window.
        for role in migration.DB_ROLES:
            db=sqlite3.connect(self.case.paths[role],timeout=0,isolation_level=None)
            try:db.execute('BEGIN IMMEDIATE');db.rollback()
            finally:db.close()

    def test_held_file_lock_reports_inability_without_changes(self):
        for role in ('seal','legacy_lock'):
            with self.subTest(role=role):
                fd=os.open(self.case.paths[role],os.O_RDONLY)
                try:
                    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);before=self.case.state()
                    result=self.run_preflight()
                    self.assertEqual(result['code'],'file_lock_busy')
                    self.assertFalse(result['writer_exclusion_during_snapshot'])
                    self.assertEqual(before,self.case.state())
                finally:os.close(fd)
        self.assertTrue(self.run_preflight()['snapshot_verified'])

    def test_any_existing_sqlite_writer_reports_inability_and_releases_our_locks(self):
        for role in migration.DB_ROLES:
            with self.subTest(role=role):
                db=sqlite3.connect(self.case.paths[role],timeout=0,isolation_level=None)
                try:
                    db.execute('BEGIN IMMEDIATE');before=self.case.state();result=self.run_preflight()
                    self.assertEqual(result['code'],'sqlite_writer_lock_busy')
                    self.assertFalse(result['snapshot_verified']);self.assertEqual(before,self.case.state())
                finally:db.rollback();db.close()
        self.assertTrue(self.run_preflight()['snapshot_verified'])

    def test_device_only_remount_preserves_approval_fingerprint_and_apply_checks_fresh_pins(self):
        old=self.case.prepare();report=self.run_preflight();rows=self.case.row_state()
        with fixtures.device_shift(901):
            new=self.case.prepare();self.assertNotEqual(old.current,new.current)
            self.assertEqual(old.digest,new.digest);self.assertEqual(old.snapshot,new.snapshot)
            self.assertEqual(report['approval_snapshot_digest'],self.run_preflight()['approval_snapshot_digest'])
            self.case.apply(old)
            self.assertTrue(self.case.fixture.make().verify())
        self.assertEqual(rows,self.case.row_state())

    def test_changed_bytes_change_fingerprint_and_refuse_prior_approval(self):
        old=self.case.prepare()
        with sqlite3.connect(self.case.paths['runner']) as db:db.execute("UPDATE runner_control SET attention='new-incident'")
        new=self.run_preflight();self.assertTrue(new['snapshot_verified'])
        self.assertNotEqual(old.digest,new['approval_snapshot_digest'])
        before=self.case.state()
        with self.assertRaises(migration.MigrationRefused):self.case.apply(old)
        self.assertEqual(before,self.case.state())

    def test_sidecar_or_unknown_exception_is_redacted_and_never_success(self):
        path=Path(self.case.paths['runner']+'-journal');path.write_bytes(b'private raw data');path.chmod(0o600)
        before=self.case.state();result=self.run_preflight()
        self.assertEqual(result['code'],'sidecar_present');self.assertFalse(result['snapshot_verified'])
        self.assertEqual(path.read_bytes(),b'private raw data');self.assertEqual(before,self.case.state())
        with patch.object(migration,'prepare',side_effect=RuntimeError('private account password path')):
            result=self.run_preflight()
        self.assertEqual(result['code'],'preflight_unavailable');self.assertNotIn('password',json.dumps(result))
        self.assertEqual(result['preflight_code_digest'],preflight.source_digest())
        self.assertNotIn('approval_snapshot_digest',result)

    def test_extended_sqlite_lock_errors_are_classified_without_raw_text(self):
        for code in (5,6,261,517,262):
            exc=sqlite3.OperationalError('secret raw database path');exc.sqlite_errorcode=code
            self.assertEqual(preflight.safe_error(exc),'sqlite_writer_lock_busy')

    def test_main_sequential_reports_then_existing_audit_and_rejects_other_cli(self):
        events=[]
        with patch.object(preflight,'run_once',side_effect=lambda **kw:events.append('preflight') or preflight.report('preflight_refused')), \
             patch.object(preflight,'emit',side_effect=lambda value:events.append(value['reason'])), \
             patch('native_diagnostic_audit_inspection.main',side_effect=lambda args:events.append('audit') or 1):
            self.assertEqual(preflight.main(['--inspect-restart-preflight']),1)
        self.assertEqual(events,['preflight_started','preflight','preflight_refused','audit'])
        with patch.object(preflight,'run_once') as run,patch('native_diagnostic_audit_inspection.main') as audit,patch.object(preflight,'emit'):
            self.assertEqual(preflight.main(['--apply']),1);run.assert_not_called();audit.assert_not_called()

    def test_process_kill_during_locked_snapshot_never_changes_or_repeats_state(self):
        import multiprocessing
        read,write=os.pipe();before=self.case.state()
        inventory={str(p.relative_to(self.case.fixture.root)):p.read_bytes()
                   for p in self.case.fixture.root.rglob('*') if p.is_file()}
        def child():
            os.close(read)
            def blocked(*args):
                os.write(write,b'locked')
                import time
                while True:time.sleep(1)
            with patch.object(migration,'_make_plan',side_effect=blocked):self.run_preflight()
            os._exit(78)
        process=multiprocessing.get_context('fork').Process(target=child)
        try:
            process.start();os.close(write);write=None
            ready,_,_=select.select([read],[],[],8);self.assertTrue(ready,'child did not obtain exclusion locks')
            self.assertEqual(os.read(read,16),b'locked');process.kill();process.join(8)
            self.assertFalse(process.is_alive());self.assertEqual(before,self.case.state())
            self.assertEqual(inventory,{str(p.relative_to(self.case.fixture.root)):p.read_bytes()
                for p in self.case.fixture.root.rglob('*') if p.is_file()})
            self.assertEqual(list(self.case.backup.iterdir()),[])
            self.assertTrue(self.run_preflight()['snapshot_verified'])
        finally:
            if process.is_alive():process.kill();process.join()
            os.close(read)
            if write is not None:os.close(write)


class PreflightSupervisorTests(unittest.TestCase):
    def test_fixed_startup_child_bounded_cancelled_and_never_repeated(self):
        child=Mock();child.poll.return_value=None;child.wait.return_value=-9
        spawn=Mock(return_value=child);timer=Mock();messages=[]
        supervisor=DiagnosticAuditSupervisor(enabled=True,popen=spawn,
            timer_factory=lambda seconds,callback:timer,monotonic=lambda:1,
            emit=lambda raw,**kw:messages.append(json.loads(raw)))
        supervisor.start();supervisor.start()
        self.assertEqual(spawn.call_count,1)
        args=spawn.call_args.args[0]
        self.assertEqual(Path(args[1]).name,'native_restart_preflight.py')
        self.assertEqual(args[2:],[ '--inspect-restart-preflight'])
        self.assertFalse(spawn.call_args.kwargs['shell'])
        supervisor._expire();supervisor.start();supervisor.close()
        child.kill.assert_called();self.assertEqual(spawn.call_count,1)
        self.assertEqual(messages[0]['reason'],'probe_watchdog_expired')
        self.assertNotIn('snapshot_verified',messages[0])


class PreflightStartupTests(unittest.TestCase):
    def test_real_inspection_startup_selects_only_web_and_fixed_readonly_wrapper(self):
        import serve
        import native_observation_supervisor
        commands=[]
        def spawn(args,**kwargs):
            commands.append((args,kwargs));child=Mock();child.poll.return_value=None;child.wait.return_value=-9
            return child
        stop=Mock();stop.wait.side_effect=[False,True];stop.is_set.return_value=False
        with patch.object(serve,'INSPECT_EXISTING_AUDIT_ONLY',True), \
             patch.object(serve,'validate_environment',return_value=('/tmp/events.db','8000')), \
             patch.dict(serve.os.environ,{'ENABLE_AUTO_RESEARCH':'true'}), \
             patch.object(serve.subprocess,'Popen',side_effect=spawn), \
             patch.object(serve.threading,'Event',return_value=stop), \
             patch.object(serve.signal,'signal'), \
             patch.object(native_observation_supervisor.threading,'Timer',return_value=Mock()), \
             patch.object(serve,'NativeProbeSupervisor') as probe, \
             patch.object(serve,'ReadonlyRuntimeSupervisor') as runtime, \
             patch.object(serve,'SchemaDiagnosticSupervisor') as diagnostic:
            self.assertEqual(serve.main(),0)
        self.assertEqual(len(commands),2)
        self.assertEqual(commands[0][0][-1],'inspection_wsgi:app')
        self.assertEqual(Path(commands[1][0][1]).name,'native_restart_preflight.py')
        self.assertEqual(commands[1][0][2:],[ '--inspect-restart-preflight'])
        self.assertFalse(commands[1][1]['shell'])
        for worker in (probe,runtime,diagnostic):
            worker.assert_called_once_with(enabled=False);worker.return_value.start.assert_not_called()


if __name__=='__main__':unittest.main()
