"""Disposable real migrated cohorts: finite proof/recovery/receipt continuity.

All sockets are denied by the shared fixture; no credentials are read. The
fixture validator uses real guard, collector, parser and observer with
deterministic provider-shaped responses, never mocked successful evidence.
"""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import sqlite3
import unittest
from unittest.mock import Mock, patch

import finite_pilot_anchor as anchor
import finite_pilot_contract as contract
import finite_pilot_inspection as inspection
import finite_pilot_worker as worker
import native_get_diagnostic as get_diagnostic
import native_restart_identity as restart_identity
import native_restart_migration as migration
import native_schema_diagnostic as old_diagnostic
import runtime_state_bundle as inventory
from native_account_guard import SharedAccountGuard, AccountGuardRefused
import test_finite_pilot_anchor as fixtures
import test_native_cohort_identity_evidence as identity_fixtures
from test_native_restart_identity import device_shift


class MigratedAnchorTests(unittest.TestCase):
    @contextmanager
    def cohort(self, *, proof=True, recovery=False):
        case = fixtures.AnchorTests()
        case.setUp()
        try:
            case.halt()
            case.paths = case.bundle.paths
            # Preserve genuinely shaped old receipts and historical audit rows.
            with sqlite3.connect(case.paths['anchor']) as db:
                db.execute(old_diagnostic._SQL)
                db.execute("INSERT INTO native_schema_diagnostic_claim VALUES(1,?,?,?,'collection_refused','none','none',4)",
                    (old_diagnostic.GENERATION, 'a' * 64, case.now - 100 * contract.NS))
                db.execute(get_diagnostic.SQL)
                db.execute("INSERT INTO native_get_diagnostic_claim VALUES(1,?,?,?,'recorded',?,?)",
                    (get_diagnostic.GENERATION, 'b' * 64, case.now - 90 * contract.NS,
                     '{"status":"observed_readonly"}', '{"historical_cause_proven":false}'))
                db.execute('CREATE TABLE native_probe_claim (id INTEGER PRIMARY KEY, status TEXT NOT NULL)')
                db.execute("INSERT INTO native_probe_claim VALUES(1,'claimed')")
            case.sql('guard', "INSERT INTO native_read_audit VALUES(?,'native_schema_refused',6,0,2,NULL)",
                     (case.now - 110 * contract.NS,))
            case.sql('guard', "INSERT INTO native_read_audit VALUES(?,'provider_read_failed',4,0,0,NULL)",
                     (case.now - 100 * contract.NS,))
            identity_fixtures.IdentityEvidenceTests.old_identity_delta(case, device=71)
            backup = case.b.root / 'migration-backup'
            backup.mkdir(mode=0o700)
            plan = migration.prepare(paths=case.paths, test_only=True)
            migration.apply(enabled=True, plan=plan, approved_digest=plan.digest,
                quiescence_confirmed=True, backup_directory=str(backup), paths=case.paths, test_only=True)
            case.bundle = case.b.make()
            case.bundle.verify()
            case.journal = anchor.AnchorJournal(case.bundle, lambda: case.now)
            case.pilot = replace(case.pilot,
                bundle_seal_sha256=hashlib.sha256(Path(case.paths['seal']).read_bytes()).hexdigest())
            contract.APPROVED_PILOT = case.pilot
            case.inspection = contract.InspectionManifest(contract.INSPECTION_GENERATION,
                case.now - contract.NS, case.now + 600 * contract.NS, inventory.GENERATION,
                'c' * 64, 'synthetic:migrated-proof')
            case.historical = self.historical(case)
            if proof:
                with patch.object(contract, 'APPROVED_INSPECTION', case.inspection), \
                     patch.object(contract, 'APPROVED_PILOT', None):
                    result = inspection.run_inspection(paths=case.paths, test_only=True, now_ns=lambda: case.now)
                case.proof = result['proof']
                case.pilot = replace(case.pilot, **{key: case.proof[key] for key in
                    ('anchor_pre_enrollment_sha256', 'pre_enrollment_cohort_sha256')})
                contract.APPROVED_PILOT = case.pilot
            if recovery:
                self.enroll_recovery(case)
            yield case
        finally:
            case.doCleanups()

    def enroll_recovery(self, case):
        case.recovery = contract.RecoveryManifest(contract.INCIDENT_ID,
            case.now - contract.NS, case.now + 600 * contract.NS, inventory.GENERATION,
            case.proof['bundle_seal_sha256'], case.proof['original_runner_sha256'],
            case.proof['protected_state_sha256'], 'd' * 64, 'e' * 64, 'f' * 64,
            '1' * 64, 'synthetic:migrated-recovery',
            anchor_pre_enrollment_sha256=case.proof['anchor_pre_enrollment_sha256'],
            pre_enrollment_cohort_sha256=case.proof['pre_enrollment_cohort_sha256'])
        contract.APPROVED_RECOVERY = case.recovery
        case.journal.enroll_recovery(case.recovery)
        case.guard = self.guard(case)

    def guard(self, case, *, bundle=None):
        return SharedAccountGuard(case.paths['legacy'], case.paths['lifecycle'], case.bundle.binding,
            enabled=True, state_database=case.paths['guard'], test_only=True,
            runtime_bundle=case.bundle if bundle is None else bundle)

    def historical(self, case):
        result = {table: case.sql('anchor', 'SELECT * FROM ' + table) for table in
            ('native_schema_diagnostic_claim', 'native_get_diagnostic_claim',
             'native_probe_claim', restart_identity.TABLE)}
        result['old_audits'] = case.sql('guard', 'SELECT rowid,* FROM native_read_audit ORDER BY rowid')[:2]
        result['seal'] = Path(case.paths['seal']).read_bytes()
        result['manifest'] = Path(case.paths['manifest']).read_bytes()
        result['lock'] = Path(case.paths['legacy_lock']).read_bytes()
        return result

    def state(self, case):
        return {role: (Path(path).read_bytes(), Path(path).stat().st_mode)
                for role, path in case.paths.items()}

    def fresh(self, case):
        bundle = case.b.make()
        bundle.verify()
        return bundle, anchor.AnchorJournal(bundle, lambda: case.now)

    def test_recovery_contract_records_adoption_not_historical_causal_proof(self):
        with self.cohort(recovery=True) as case:
            document = case.recovery.document()
            self.assertEqual(document['recovery_basis'], 'operator_approved_fresh_baseline_adoption')
            self.assertIs(document['historical_cause_unproven'], True)
            for fields in ({'historical_cause_unproven': False}, {'historical_cause_unproven': 1},
                           {'recovery_basis': 'storage_fixed_historical_schema'}):
                with self.assertRaisesRegex(contract.PilotRefused, 'recovery_basis_invalid'):
                    replace(case.recovery, **fields)

    def test_migrated_proof_uses_enrolled_references_and_survives_device_remount(self):
        with self.cohort() as case:
            reference = case.bundle._reference_identity('anchor')
            self.assertNotEqual(reference, case.bundle._pinned['anchor'])
            self.assertEqual(case.sql('anchor', 'SELECT anchor_device,anchor_inode FROM finite_pilot_binding'), [reference])
            self.assertEqual(case.sql('anchor', 'SELECT anchor_device,anchor_inode FROM finite_inspection_receipt'), [reference])
            before = self.state(case)
            for shift in (123, 456):
                with device_shift(shift):
                    bundle, journal = self.fresh(case)
                    self.assertEqual(journal.protected_state_digest(), case.proof['protected_state_sha256'])
                    self.assertEqual(journal.cohort_state_digest(), case.proof['pre_enrollment_cohort_sha256'])
                    self.assertEqual(journal.founding_proofs(), {key: case.proof[key] for key in
                        ('anchor_pre_enrollment_sha256', 'pre_enrollment_cohort_sha256')})
                    with patch.object(contract, 'APPROVED_INSPECTION', case.inspection), \
                         patch.object(contract, 'APPROVED_PILOT', None):
                        self.assertEqual(inspection.run_inspection(paths=case.paths, test_only=True,
                            now_ns=lambda: case.now)['phase'], 'inspection_consumed')
                    self.assertTrue(bundle.verify())
            self.assertEqual(self.state(case), before)
            self.assertEqual(self.historical(case), case.historical)
            self.assertEqual(case.wire.calls, [])

    def test_recovery_then_pilot_and_second_remount_preserve_all_receipts_and_counters(self):
        with self.cohort(recovery=True) as case:
            original = case.runner()
            with device_shift(123):
                case.bundle, case.journal = self.fresh(case)
                case.guard = self.guard(case)
                self.assertEqual(case.recover()['phase'], 'recovered_readonly')
                self.assertEqual(case.runner(), (1, case.now / contract.NS, 0, 0, None, 'recovered_readonly'))
                case.journal.enroll_pilot(case.pilot)
                case.journal.begin_stage(case.pilot, 'source')
                case.journal.reserve_source_request(case.pilot)
                case.journal.finish_stage(case.pilot, 'source')
                counters = case.controls()
                guard_rows = case.journal._guard_snapshot()
                digest = case.journal.cohort_state_digest()
                self.assertTrue(guard_rows['native_read_attempts'])
                self.assertTrue(guard_rows['account_guard_attempts'])
            with device_shift(456):
                bundle, journal = self.fresh(case)
                row = journal.startup_gate(case.pilot)
                self.assertEqual(row['sources'], 1)
                self.assertEqual(case.controls(), counters)
                self.assertEqual(journal._guard_snapshot(), guard_rows)
                self.assertEqual(journal.cohort_state_digest(), digest)
                with patch.object(worker, 'validate_recovery_evidence', Mock(side_effect=AssertionError('no replay'))) as validate:
                    self.assertEqual(journal.recover_incident(case.recovery, guard=self.guard(case, bundle=bundle),
                        validate=validate)['phase'], 'already_recovered_readonly')
                    validate.assert_not_called()
            self.assertNotEqual(original, case.runner())
            self.assertEqual(self.historical(case), case.historical)
            self.assertEqual(case.validator_calls, 1)

    def test_same_process_cannot_repin_physical_identity_after_remount(self):
        with self.cohort() as case, device_shift(99):
            with self.assertRaisesRegex(inventory.RuntimeStateRefused, 'bundle_file_replaced'):
                case.journal.protected_state_digest()
            with self.assertRaises(AccountGuardRefused):
                with self.guard(case).hold(now_ns=case.now, read_only=True):
                    self.fail('physical instance silently repinned')

    def test_incompatible_physical_identity_foundation_is_never_rewritten(self):
        with self.cohort() as case:
            case.sql('anchor', 'UPDATE finite_pilot_binding SET anchor_device=?,anchor_inode=?',
                     case.bundle._pinned['anchor'])
            before = self.state(case)
            with self.assertRaisesRegex(contract.PilotRefused, 'anchor_binding_changed'):
                case.journal.enroll_pilot(case.pilot)
            self.assertEqual(self.state(case), before)
            self.assertEqual(case.wire.calls, [])

    def test_missing_foreign_or_mixed_markers_refuse_before_proof_or_provider(self):
        for mutation in ('missing_root', 'missing_stamp', 'foreign_stamp', 'foreign_lock', 'foreign_seal'):
            with self.subTest(mutation=mutation), self.cohort(proof=False) as case:
                if mutation == 'missing_root':
                    case.sql('anchor', 'DROP TABLE ' + restart_identity.TABLE)
                elif mutation in ('missing_stamp', 'foreign_stamp'):
                    value = 0 if mutation == 'missing_stamp' else restart_identity.stamp('0' * 64, 'runner')[1]
                    case.sql('runner', 'PRAGMA user_version=' + str(value))
                else:
                    role = 'seal' if mutation == 'foreign_seal' else 'legacy_lock'
                    path = Path(case.paths[role]); value = json.loads(path.read_bytes())
                    marker = value['restart_identity'] if role == 'seal' else value
                    marker['cohort'] = '0' * 64
                    path.write_text(json.dumps(value))
                before = self.state(case)
                with patch.object(contract, 'APPROVED_INSPECTION', case.inspection), \
                     patch.object(contract, 'APPROVED_PILOT', None), self.assertRaises(ValueError):
                    inspection.run_inspection(paths=case.paths, test_only=True, now_ns=lambda: case.now)
                self.assertEqual(self.state(case), before)
                self.assertEqual(case.wire.calls, [])
                self.assertEqual(case.sql('anchor', "SELECT name FROM sqlite_master WHERE name LIKE 'finite_%'"), [])

    def test_replaced_or_symlinked_role_refuses_before_recovery_or_cas(self):
        for role in ('runner', 'source', 'anchor'):
            for kind in ('inode', 'symlink'):
                with self.subTest(role=role, kind=kind), self.cohort(recovery=True) as case:
                    original = case.runner()
                    path = Path(case.paths[role]); saved = path.with_suffix('.original')
                    path.rename(saved)
                    if kind == 'inode':
                        shutil.copyfile(saved, path); path.chmod(saved.stat().st_mode & 0o777)
                    else:
                        path.symlink_to(saved)
                    try:
                        with self.assertRaises(ValueError):
                            case.recover()
                        self.assertEqual(case.validator_calls, 0)
                        self.assertEqual(case.wire.calls, [])
                        self.assertEqual(case.runner(), original)
                    finally:
                        path.unlink(); saved.rename(path)

    def test_historical_receipt_mutation_refuses_before_prepared(self):
        for table, column in (('native_schema_diagnostic_claim', 'code_hash'),
                              ('native_get_diagnostic_claim', 'code_hash'),
                              ('native_probe_claim', 'status')):
            with self.subTest(table=table), self.cohort(recovery=True) as case:
                case.sql('anchor', 'UPDATE ' + table + ' SET ' + column + '=?', ('changed',))
                original = case.runner()
                with self.assertRaisesRegex(contract.PilotRefused, 'recovery_protected_state_changed'):
                    case.recover()
                self.assertEqual(case.runner(), original)
                self.assertEqual(case.sql('anchor', 'SELECT * FROM finite_pilot_recovery_journal'), [])
                self.assertEqual(case.validator_calls, 0)

    def test_audit_mutation_between_enrollment_and_prepared_never_adopts_new_baseline(self):
        with self.cohort(recovery=True) as case:
            original = case.runner()
            case.sql('guard', "UPDATE native_read_audit SET status='unapproved_replacement' WHERE rowid=1")
            changed = self.state(case)
            with self.assertRaisesRegex(contract.PilotRefused, 'recovery_initial_cohort_changed'):
                case.recover()
            self.assertEqual(case.runner(), original)
            self.assertEqual(case.validator_calls, 0)
            self.assertEqual(case.wire.calls, [])
            self.assertEqual(case.sql('anchor', 'SELECT * FROM finite_pilot_recovery_journal'), [])
            # Refusal does not undo or normalize the observed external mutation.
            self.assertEqual(self.state(case), changed)

    def test_each_process_crash_boundary_survives_remount_without_retry_or_evidence_loss(self):
        phases = ('prepared', 'validated', 'cas_before_commit', 'cas_committed', 'applied_before_commit', 'applied')
        for phase in phases:
            with self.subTest(phase=phase), self.cohort(recovery=True) as case:
                original = case.runner()
                def child():
                    def crash(name):
                        if name == phase:
                            os._exit(77)
                    case.journal._checkpoint = crash
                    case.recover()
                    os._exit(78)
                process = multiprocessing.get_context('fork').Process(target=child)
                process.start(); process.join(15)
                if process.is_alive():
                    process.kill(); process.join(); self.fail('synthetic recovery exceeded test bound')
                self.assertEqual(process.exitcode, 77)
                # Check production admission before test assertions open SQLite:
                # a killed uncommitted transaction can leave a rollback journal.
                # Runtime admission must refuse without changing that sidecar.
                raw_before = self.state(case)
                sidecars = {path + suffix: Path(path + suffix).read_bytes()
                    for path in case.paths.values() for suffix in inventory._SIDECARS
                    if Path(path + suffix).exists()}
                if sidecars:
                    with device_shift(456):
                        bundle = case.b.make()
                        journal = anchor.AnchorJournal(bundle, lambda: case.now)
                        with patch.object(worker, 'validate_recovery_evidence',
                                          Mock(side_effect=AssertionError('no replay'))) as validate, \
                             self.assertRaises((inventory.RuntimeStateRefused, contract.PilotRefused)) as refused:
                            journal.recover_incident(case.recovery,
                                guard=self.guard(case, bundle=bundle), validate=validate)
                        if isinstance(refused.exception, contract.PilotRefused):
                            self.assertEqual(str(refused.exception), 'recovery_incomplete')
                        validate.assert_not_called()
                    self.assertEqual(self.state(case), raw_before)
                    self.assertEqual({path: Path(path).read_bytes() for path in sidecars}, sidecars)
                # These fixture-only reads may let SQLite clean up its rollback
                # journal for row assertions; that is not a production action.
                if phase in ('prepared', 'validated', 'cas_before_commit'):
                    self.assertEqual(case.runner(), original)
                else:
                    self.assertEqual(case.runner(), (1, case.now / contract.NS, 0, 0, None, 'recovered_readonly'))
                self.assertEqual(self.historical(case), case.historical)
                before = self.state(case)
                with device_shift(456):
                    bundle, journal = self.fresh(case)
                    with patch.object(worker, 'validate_recovery_evidence', Mock(side_effect=AssertionError('no replay'))) as validate:
                        if phase == 'applied':
                            self.assertEqual(journal.recover_incident(case.recovery,
                                guard=self.guard(case, bundle=bundle), validate=validate)['phase'], 'already_recovered_readonly')
                        else:
                            with self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
                                journal.recover_incident(case.recovery, guard=self.guard(case, bundle=bundle), validate=validate)
                            with patch.object(contract, 'APPROVED_PILOT', None), \
                                 self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
                                anchor.read_only_startup_gate(bundle)
                        validate.assert_not_called()
                self.assertEqual(self.state(case), before)


if __name__ == '__main__':
    unittest.main()
