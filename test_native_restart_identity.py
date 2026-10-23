"""Disposable fixtures only. No network, credentials or real deployment access."""
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import shutil
import sqlite3
import unittest
from unittest.mock import Mock, patch

import native_restart_identity as identity
import native_restart_migration as migration
import runtime_state_bundle as bundle
from native_account_guard import SharedAccountGuard, AccountGuardRefused
import test_runtime_state_bundle as fixtures
import test_native_cohort_identity_evidence as identity_fixtures
from test_native_observation_worker import FP,NOW


@contextmanager
def device_shift(amount):
    original_stat,original_fstat=os.stat,os.fstat
    def changed(value):
        fields=list(value);fields[2]+=amount
        return os.stat_result(fields)
    with patch.object(os,'stat',side_effect=lambda *a,**k:changed(original_stat(*a,**k))), \
         patch.object(os,'fstat',side_effect=lambda *a,**k:changed(original_fstat(*a,**k))):
        yield


class RestartIdentityTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.BundleTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.fixture.enroll();self.paths=self.fixture.paths
        self.backup=self.fixture.root/'backup';self.backup.mkdir(mode=0o700)

    def state(self):
        return {role:(Path(path).read_bytes(),Path(path).stat().st_ino,Path(path).stat().st_mode)
                for role,path in self.paths.items()}

    def row_state(self):
        out={}
        for role in migration.DB_ROLES:
            with sqlite3.connect(self.paths[role]) as db:
                out[role]=migration._rows(db,without_migration=True)
        return out

    def drift(self):
        identity_fixtures.IdentityEvidenceTests.old_identity_delta(self,device=71)

    def prepare(self):
        return migration.prepare(paths=self.paths,test_only=True)

    def apply(self,plan=None,**kw):
        plan=self.prepare() if plan is None else plan
        return migration.apply(enabled=True,plan=plan,approved_digest=plan.digest,
            quiescence_confirmed=True,backup_directory=str(self.backup),paths=self.paths,test_only=True,**kw)

    def guard(self,verified=True):
        return SharedAccountGuard(self.paths['legacy'],self.paths['lifecycle'],FP,enabled=True,
            state_database=self.paths['guard'],test_only=True,
            runtime_bundle=self.fixture.make() if verified else None)

    def test_disabled_touches_nothing(self):
        with patch.object(migration,'_binding',side_effect=AssertionError('no IO')):
            self.assertEqual(migration.apply()['reason'],'restart_migration_disabled')

    def test_plan_is_nonmutating_and_requires_explicit_approval_and_quiescence(self):
        self.drift();before=self.state();plan=self.prepare();self.assertEqual(before,self.state())
        for options in ({},dict(approved_digest='bad',quiescence_confirmed=True),
                        dict(approved_digest=plan.digest,quiescence_confirmed=False)):
            with self.assertRaises(migration.MigrationRefused):
                migration.apply(enabled=True,plan=plan,backup_directory=str(self.backup),
                                paths=self.paths,test_only=True,**options)
        self.assertEqual(before,self.state())

    def test_device_only_migration_preserves_all_business_rows_claims_and_quota(self):
        self.drift()
        with sqlite3.connect(self.paths['runner']) as db:
            db.execute("UPDATE runner_control SET dirty=1,stopped=1,attention='readonly_runtime_stage_failed'")
        with sqlite3.connect(self.paths['lifecycle']) as db:db.execute('UPDATE control SET stopped=1,attention=1')
        with sqlite3.connect(self.paths['decision']) as db:db.execute('UPDATE demo_control SET stopped=1')
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute('UPDATE native_read_budget SET high_ns=?',(NOW,))
            db.execute('INSERT INTO native_read_attempts VALUES(?)',(NOW,))
            db.execute("INSERT INTO native_read_audit VALUES(?,'native_schema_refused',6,0,2,NULL)",(NOW,))
        import native_schema_diagnostic as old
        with sqlite3.connect(self.paths['anchor']) as db:
            db.execute(old._SQL)
            db.execute('INSERT INTO native_schema_diagnostic_claim VALUES(1,?,?,?,\'collection_refused\',\'none\',\'none\',4)',
                       (old.GENERATION,old.code_hash(),NOW))
        before=self.row_state();physical=self.state();result=self.apply()
        self.assertTrue(result['business_rows_preserved']);self.assertFalse(result['execution_ready'])
        self.assertEqual(before,self.row_state())
        self.assertEqual({r:v[1:] for r,v in physical.items()},{r:v[1:] for r,v in self.state().items()})
        for role in self.paths:self.assertEqual((self.backup/(role+'.backup')).read_bytes(),physical[role][0])
        self.assertTrue(self.fixture.make().verify())
        # Consumed old diagnostic returns before guard/transport; never reset.
        with patch('native_schema_diagnostic.RuntimeReadOnlyHTTP',side_effect=AssertionError('no transport')):
            result=old.run_once(enabled=True,paths=self.paths,test_only=True,environ={})
        self.assertEqual(result['reason'],'diagnostic_already_claimed')
        with self.assertRaises(migration.MigrationRefused):self.apply()

    def test_second_remount_needs_no_migration_but_same_instance_cannot_repin(self):
        self.drift();self.apply();before=self.state()
        pinned=self.fixture.make();self.assertTrue(pinned.verify())
        with device_shift(123):
            self.assertTrue(self.fixture.make().verify())
            with self.assertRaisesRegex(bundle.RuntimeStateRefused,'bundle_file_replaced'):pinned.verify()
            with self.guard().hold(now_ns=NOW,read_only=True) as guard:
                self.assertEqual(guard.reserve_request(NOW),1)
        # Only the deliberately exercised existing quota changed after migration.
        for role in self.paths:
            if role!='guard':self.assertEqual(before[role],self.state()[role])

    def test_standalone_guard_does_not_ignore_device_or_accept_migrated_headers(self):
        self.drift();self.apply()
        with self.assertRaisesRegex(AccountGuardRefused,'restart_identity_context_required'):
            with self.guard(False).hold(now_ns=NOW,read_only=True):pass

    def test_quota_cap_and_clock_regression_are_not_reset_by_remount(self):
        self.drift();self.apply()
        with self.guard().hold(now_ns=NOW,read_only=True) as guard:
            for n in range(12):self.assertEqual(guard.reserve_request(NOW),n+1)
            with self.assertRaisesRegex(AccountGuardRefused,'shared_request_budget_exhausted'):guard.reserve_request(NOW)
        with device_shift(99):
            with self.guard().hold(now_ns=NOW,read_only=True) as guard:
                with self.assertRaisesRegex(AccountGuardRefused,'shared_request_budget_exhausted'):guard.reserve_request(NOW)
                with self.assertRaisesRegex(AccountGuardRefused,'request_clock_regression'):guard.reserve_request(NOW-1)

    def test_changed_preimage_refuses_before_any_metadata_write(self):
        self.drift();plan=self.prepare()
        with sqlite3.connect(self.paths['runner']) as db:db.execute("UPDATE runner_control SET attention='new-stop'")
        before=self.state()
        with self.assertRaisesRegex(migration.MigrationRefused,'restart_migration_preimage_changed'):self.apply(plan)
        self.assertEqual(before,self.state());self.assertEqual(list(self.backup.iterdir()),[])

    def test_any_replaced_file_rejected_even_with_copied_valid_markers(self):
        self.drift();self.apply()
        for role,path in self.paths.items():
            with self.subTest(role=role):
                target=Path(path);saved=target.with_suffix('.saved')
                target.rename(saved);shutil.copyfile(saved,target);os.chmod(target,saved.stat().st_mode&0o777)
                try:
                    with self.assertRaises(bundle.RuntimeStateRefused):self.fixture.make().verify()
                finally:target.unlink();saved.rename(target)

    def test_each_missing_or_foreign_db_stamp_refuses_without_repair(self):
        self.drift();self.apply()
        for role in migration.DB_ROLES:
            with self.subTest(role=role):
                path=Path(self.paths[role]);raw=path.read_bytes()
                with sqlite3.connect(path) as db:db.execute('PRAGMA user_version=0')
                changed=self.state()
                with self.assertRaises(bundle.RuntimeStateRefused):self.fixture.make().verify()
                self.assertEqual(changed,self.state());path.write_bytes(raw)

    def test_missing_root_with_remaining_markers_cannot_fall_back(self):
        self.drift();self.apply()
        with sqlite3.connect(self.paths['anchor']) as db:db.execute('DROP TABLE '+identity.TABLE)
        before=self.state()
        with self.assertRaises(bundle.RuntimeStateRefused):self.fixture.make().verify()
        self.assertEqual(before,self.state())

    def test_wrong_context_account_or_paths_refuses_guard(self):
        self.drift();self.apply();wrong=self.fixture.make();wrong.binding='0'*64
        guard=SharedAccountGuard(self.paths['legacy'],self.paths['lifecycle'],FP,enabled=True,
            state_database=self.paths['guard'],test_only=True,runtime_bundle=wrong)
        with self.assertRaisesRegex(AccountGuardRefused,'restart_identity_context_invalid'):
            with guard.hold(now_ns=NOW,read_only=True):pass

class RestartIdentityFaultTests(unittest.TestCase):
    @contextmanager
    def fixture(self):
        case=RestartIdentityTests();case.setUp()
        try:yield case
        finally:case.doCleanups()

    def test_each_durability_boundary_crash_keeps_data_and_never_repeats(self):
        phases=['backup_synced','prepared_synced',
            *('stamp_'+r+'_synced' for r in ('guard',*(r for r in migration.DB_ROLES if r!='guard'))),
            'lock_synced','seal_synced','before_complete','complete_synced']
        for phase in phases:
            with self.subTest(phase=phase),self.fixture() as case:
                case.drift();plan=case.prepare();rows=case.row_state();before=case.state()
                def crash(where):
                    if where==phase:raise SystemExit('synthetic interruption')
                with self.assertRaises(SystemExit):case.apply(plan,_checkpoint=crash)
                self.assertEqual(rows,case.row_state())
                if phase=='backup_synced':
                    self.assertEqual(before,case.state());self.assertEqual(plan,case.prepare())
                else:
                    if phase=='complete_synced':self.assertTrue(case.fixture.make().verify())
                    else:
                        with self.assertRaises(bundle.RuntimeStateRefused):case.fixture.make().verify()
                    interrupted=case.state()
                    with self.assertRaisesRegex(migration.MigrationRefused,'restart_migration_already_started'):case.apply(plan)
                    self.assertEqual(interrupted,case.state())

    def test_real_process_exit_releases_locks_but_never_authorizes_partial_state(self):
        import multiprocessing
        for phase in ('prepared_synced','stamp_guard_synced','seal_synced','complete_synced'):
            with self.subTest(phase=phase),self.fixture() as case:
                case.drift();plan=case.prepare();rows=case.row_state()
                def child():
                    def die(where):
                        if where==phase:os._exit(77)
                    case.apply(plan,_checkpoint=die)
                    os._exit(78)
                process=multiprocessing.get_context('fork').Process(target=child)
                process.start();process.join(15)
                if process.is_alive():process.kill();process.join();self.fail('migration test timed out')
                self.assertEqual(process.exitcode,77);self.assertEqual(rows,case.row_state())
                if phase=='complete_synced':self.assertTrue(case.fixture.make().verify())
                else:
                    with self.assertRaises(bundle.RuntimeStateRefused):case.fixture.make().verify()
                with self.assertRaises(migration.MigrationRefused):case.apply(plan)

    def test_mutated_manifest_seal_lock_or_cohort_stamp_refuses(self):
        for target in ('manifest','seal','legacy_lock','cohort','contract','self_hashed_root','phase','paths'):
            with self.subTest(target=target),self.fixture() as case:
                case.drift();case.apply()
                if target=='manifest':
                    with sqlite3.connect(case.paths['manifest']) as db:db.execute('UPDATE bundle_files SET inode=inode+1 WHERE role=\'runner\'')
                elif target in ('seal','legacy_lock'):
                    path=Path(case.paths[target]);data=json.loads(path.read_bytes())
                    if target=='seal':data['restart_identity']['token']='0'*64
                    else:data['token']='0'*64
                    path.write_text(json.dumps(data))
                else:
                    with sqlite3.connect(case.paths['anchor']) as db:
                        if target=='cohort':db.execute('UPDATE '+identity.TABLE+' SET cohort=?',('0'*64,))
                        elif target=='contract':db.execute('UPDATE '+identity.TABLE+' SET contract_digest=?',('0'*64,))
                        elif target=='phase':db.execute('UPDATE '+identity.TABLE+" SET status='prepared'")
                        elif target=='self_hashed_root':
                            row=db.execute('SELECT cohort,inventory FROM '+identity.TABLE).fetchone()
                            digest=identity.contract_digest(row[0],json.loads(row[1]),'0'*64)
                            db.execute('UPDATE '+identity.TABLE+' SET preimage_digest=?,contract_digest=?',('0'*64,digest))
                        else:
                            raw=db.execute('SELECT inventory FROM '+identity.TABLE).fetchone()[0]
                            data=json.loads(raw);data['files']['runner']['path']=case.paths['guard']
                            db.execute('UPDATE '+identity.TABLE+' SET inventory=?',(identity._json(data),))
                before=case.state()
                with self.assertRaises(bundle.RuntimeStateRefused):case.fixture.make().verify()
                self.assertEqual(before,case.state())

    def test_missing_each_file_never_calls_an_initializer(self):
        with self.fixture() as case:
            case.drift();case.apply()
            for role,path in case.paths.items():
                with self.subTest(role=role):
                    p=Path(path);saved=p.with_suffix('.saved');p.rename(saved)
                    try:
                        with patch.object(bundle.RuntimeStateBundle,'provision',side_effect=AssertionError('no repair')):
                            with self.assertRaises(Exception):case.fixture.make().verify()
                        self.assertFalse(p.exists())
                    finally:saved.rename(p)

    def test_each_lone_nonzero_header_or_lock_marker_without_root_refuses(self):
        for role in ('legacy','anchor','manifest','legacy_lock'):
            with self.subTest(role=role),self.fixture() as case:
                if role=='legacy_lock':Path(case.paths[role]).write_bytes(b'incomplete-new-marker')
                else:
                    with sqlite3.connect(case.paths[role]) as db:db.execute('PRAGMA application_id=123')
                with self.assertRaises(bundle.RuntimeStateRefused):case.fixture.make().verify()

    def test_quota_file_substitution_after_hold_is_detected_before_reservation(self):
        with self.fixture() as case:
            case.drift();case.apply();guard=case.guard()
            with guard.hold(now_ns=NOW,read_only=True):
                path=Path(case.paths['guard']);old=path.with_suffix('.old');path.rename(old)
                shutil.copyfile(old,path);os.chmod(path,0o600)
                before=path.read_bytes()
                try:
                    with self.assertRaises(Exception):guard.reserve_request(NOW)
                    self.assertEqual(before,path.read_bytes())
                finally:path.unlink();old.rename(path)

    def test_header_claim_or_revision_preflight_mismatch_is_not_adopted(self):
        for target in ('legacy_header','anchor_header','manifest_header','revision','inode'):
            with self.subTest(target=target),self.fixture() as case:
                case.drift()
                if target.endswith('_header'):
                    with sqlite3.connect(case.paths[target.split('_')[0]]) as db:db.execute('PRAGMA user_version=44')
                elif target=='revision':
                    with sqlite3.connect(case.paths['guard']) as db:db.execute("UPDATE account_guard_binding SET legacy_revision='123'")
                else:
                    path=Path(case.paths['source']);saved=path.with_suffix('.old');path.rename(saved)
                    shutil.copyfile(saved,path);os.chmod(path,0o600)
                before=case.state()
                with self.assertRaises(Exception):case.prepare()
                self.assertEqual(before,case.state())

    def test_identity_failure_before_startup_phase_never_reads_credentials_or_constructs_transport(self):
        from readonly_runtime_worker import run_phase
        with self.fixture() as case:
            case.drift();case.apply()
            with sqlite3.connect(case.paths['guard']) as db:db.execute('PRAGMA user_version=0')
            env=Mock();env.get.side_effect=AssertionError('no credential access')
            wire=Mock(side_effect=AssertionError('no provider constructor'))
            with self.assertRaises(Exception):
                run_phase('broker',paths=case.paths,test_only=True,environ=env,broker_factory=wire)
            wire.assert_not_called();env.get.assert_not_called()


    def test_initialize_phase_after_remount_is_readonly_and_preserves_latches(self):
        from readonly_runtime_worker import run_phase
        with self.fixture() as case:
            case.drift();case.apply();before=case.state()
            wire=Mock(side_effect=AssertionError('no transport'))
            with device_shift(310),patch.object(bundle.RuntimeStateBundle,'provision',side_effect=AssertionError('no enrollment')):
                result=run_phase('initialize',paths=case.paths,test_only=True,environ={},now_ns=lambda:NOW,
                                 broker_factory=wire,source_factory=wire)
            self.assertEqual(result['reason'],'initialized_readonly');wire.assert_not_called()
            self.assertEqual(before,case.state())
            with sqlite3.connect(case.paths['runner']) as db:
                db.execute("UPDATE runner_control SET dirty=1,attention='readonly_runtime_stage_failed'")
            before=case.state()
            with device_shift(510),self.assertRaisesRegex(ValueError,'runtime_attention_latched'):
                run_phase('initialize',paths=case.paths,test_only=True,environ={},now_ns=lambda:NOW,
                          broker_factory=wire,source_factory=wire)
            self.assertEqual(before,case.state());wire.assert_not_called()

    def test_preflight_refuses_every_sidecar_without_changing_any_bytes(self):
        with self.fixture() as case:
            for role in migration.DB_ROLES:
                for suffix in bundle._SIDECARS:
                    with self.subTest(role=role,suffix=suffix):
                        sidecar=Path(case.paths[role]+suffix);sidecar.write_bytes(b'preserve unexamined sidecar')
                        before={str(p):p.read_bytes() for p in case.fixture.root.rglob('*') if p.is_file()}
                        try:
                            with self.assertRaises(Exception):case.prepare()
                            after={str(p):p.read_bytes() for p in case.fixture.root.rglob('*') if p.is_file()}
                            self.assertEqual(before,after)
                        finally:sidecar.unlink()

    def test_preflight_does_not_recover_a_real_hot_journal(self):
        import multiprocessing
        for role in migration.DB_ROLES:
            with self.subTest(role=role),self.fixture() as case:
                def crash_writer():
                    db=sqlite3.connect(case.paths[role]);db.execute('PRAGMA cache_size=1')
                    db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
                    db.execute('CREATE TABLE crash_only(x BLOB)')
                    db.executemany('INSERT INTO crash_only VALUES(zeroblob(4096))',[() for _ in range(100)])
                    os._exit(77)
                child=multiprocessing.get_context('fork').Process(target=crash_writer)
                child.start();child.join(10)
                if child.is_alive():child.kill();child.join();self.fail('hot journal fixture timed out')
                self.assertEqual(child.exitcode,77)
                self.assertTrue(Path(case.paths[role]+'-journal').exists())
                before={str(p):p.read_bytes() for p in case.fixture.root.rglob('*') if p.is_file()}
                with self.assertRaises(Exception):case.prepare()
                after={str(p):p.read_bytes() for p in case.fixture.root.rglob('*') if p.is_file()}
                self.assertEqual(before,after)


    def test_backup_corruption_or_destination_substitution_prevents_metadata_writes(self):
        for fault in ('corruption','directory_swap','nonempty'):
            with self.subTest(fault=fault),self.fixture() as case:
                case.drift();plan=case.prepare();before=case.state()
                if fault=='nonempty':(case.backup/'unrelated').write_bytes(b'keep')
                original=bundle.RuntimeStateBundle._write
                done=False
                def faulty(fd,raw):
                    nonlocal done
                    original(fd,raw)
                    if done:return
                    done=True
                    if fault=='corruption':os.lseek(fd,0,os.SEEK_SET);os.write(fd,b'!')
                    elif fault=='directory_swap':
                        case.backup.rename(case.backup.with_name('backup-moved'));case.backup.mkdir(mode=0o700)
                with patch.object(bundle.RuntimeStateBundle,'_write',side_effect=faulty):
                    with self.assertRaises(migration.MigrationRefused):case.apply(plan)
                self.assertEqual(before,case.state())


if __name__=='__main__':unittest.main()
