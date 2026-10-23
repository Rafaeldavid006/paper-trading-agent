"""Hash-only inspection tests. All project imports and execution deny network."""
from contextlib import ExitStack
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import Mock, patch

with ExitStack() as imports:
    for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo', 'subprocess.Popen'):
        imports.enter_context(patch(name, side_effect=AssertionError('OFFLINE ONLY')))
    import finite_pilot_contract as contract
    import finite_pilot_inspection as inspect
    from finite_pilot_anchor import AnchorJournal
    from runtime_state_bundle import GENERATION
    import test_runtime_state_bundle as fixture


class EnvTrap:
    def __getattribute__(self, name):
        raise AssertionError('NO ENVIRONMENT ACCESS')


class DisabledInspectionTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo', 'subprocess.Popen'):
            self.enterContext(patch(name, side_effect=AssertionError('OFFLINE ONLY')))

    def test_default_none_before_state_sql_environment_clock_or_io(self):
        self.assertIsNone(contract.APPROVED_INSPECTION)
        trap = Mock(side_effect=AssertionError('NO IO'))
        with patch.object(inspect, 'load_owned_ledger', trap), patch('sqlite3.connect', trap), \
             patch('builtins.open', trap), patch.object(inspect.os, 'environ', EnvTrap()), \
             patch.object(inspect.time, 'time_ns', trap), patch.object(inspect.time, 'monotonic_ns', trap):
            result = inspect.run_inspection(paths=EnvTrap(), now_ns=trap)
        self.assertEqual(result, inspect.public('disabled'))
        self.assertNotIn('proof', result)
        trap.assert_not_called()

    def test_boolean_json_or_equivalent_object_is_not_a_grant(self):
        for forged in (True, {'approved': True}, object()):
            with patch.object(contract, 'APPROVED_INSPECTION', forged), \
                 patch.object(inspect, 'load_owned_ledger') as load:
                with self.assertRaises(contract.PilotRefused):
                    inspect.run_inspection()
                load.assert_not_called()

    def test_fixed_cli_only_and_errors_do_not_reflect_private_details(self):
        for args in ([], ['--inspect'], ['--initialize-proof-state', '/tmp/approval.json'], ['--recover']):
            with patch('builtins.print') as out:
                self.assertEqual(inspect.main(args), 2)
                self.assertNotIn('proof', json.loads(out.call_args.args[0]))
        with patch.object(inspect, 'run_inspection', side_effect=RuntimeError('secret-key-account-id')), \
             patch('builtins.print') as out:
            self.assertEqual(inspect.main(['--initialize-proof-state']), 1)
        self.assertNotIn('secret', out.call_args.args[0])
        self.assertNotIn('proof', json.loads(out.call_args.args[0]))

    def test_public_schema_rejects_raw_fields_and_nonhashes(self):
        for proof in ({'account': 'private'}, {name: 'private' for name in inspect.PROOF_KEYS}):
            with self.assertRaises(contract.PilotRefused):
                inspect.public('inspection_complete', proof)
        with self.assertRaises(contract.PilotRefused):
            inspect.public('inspection_consumed', {name: 'a' * 64 for name in inspect.PROOF_KEYS})


class InspectionTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo', 'subprocess.Popen'):
            self.enterContext(patch(name, side_effect=AssertionError('OFFLINE ONLY')))
        self.b = fixture.BundleTests()
        self.b.setUp()
        self.addCleanup(self.b.doCleanups)
        self.b.enroll()
        self.bundle = self.b.bundle
        self.now = fixture.NOW
        with sqlite3.connect(self.bundle.paths['runner']) as db:
            db.execute("UPDATE runner_control SET heartbeat=?,dirty=1,attention='readonly_runtime_stage_failed',stage='started'",
                       (self.now / contract.NS - 25,))
            self.original = db.execute('SELECT * FROM runner_control').fetchone()
        self.manifest = contract.InspectionManifest(contract.INSPECTION_GENERATION,
            self.now - contract.NS, self.now + 60 * contract.NS, GENERATION, 'a' * 64,
            'SYNTHETIC-INSPECTION-NOT-A-GRANT')
        self.enterContext(patch.object(contract, 'APPROVED_INSPECTION', self.manifest))
        self.enterContext(patch.object(contract, 'APPROVED_PILOT', None))
        self.enterContext(patch.object(contract, 'APPROVED_RECOVERY', None))
        self.kw = dict(paths=self.bundle.paths, test_only=True, now_ns=lambda: self.now)
        self.before = {role: Path(path).read_bytes() for role, path in self.bundle.paths.items()}

    def run_inspection(self):
        return inspect.run_inspection(**self.kw)

    def runner(self):
        with sqlite3.connect(self.bundle.paths['runner']) as db:
            return db.execute('SELECT * FROM runner_control').fetchone()

    def receipt(self):
        with sqlite3.connect(self.bundle.paths['anchor']) as db:
            return db.execute('SELECT * FROM ' + inspect.TABLE).fetchone()

    def test_minimal_hashes_describe_final_completed_state_without_operational_mutation(self):
        with patch.object(inspect.os, 'environ', EnvTrap()):
            result = self.run_inspection()
        self.assertEqual(result['phase'], 'inspection_complete')
        self.assertFalse(result['publication_authorized'])
        self.assertEqual(set(result['proof']), inspect.PROOF_KEYS)
        self.assertEqual(self.receipt()[3], 'complete')
        self.assertEqual(self.runner(), self.original)
        journal = AnchorJournal(self.bundle, lambda: self.now)
        proof = result['proof']
        self.assertEqual(proof['original_runner_sha256'], contract.digest(self.original))
        self.assertEqual(proof['protected_state_sha256'], journal.protected_state_digest())
        self.assertEqual(proof['pre_enrollment_cohort_sha256'], journal.cohort_state_digest())
        self.assertEqual(proof['bundle_seal_sha256'], hashlib.sha256(Path(self.bundle.paths['seal']).read_bytes()).hexdigest())
        self.assertEqual({key: proof[key] for key in ('anchor_pre_enrollment_sha256', 'pre_enrollment_cohort_sha256')},
                         journal.founding_proofs())
        with sqlite3.connect(self.bundle.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM finite_pilot_enrollment').fetchone(), (0,))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM finite_pilot_recovery').fetchone(), (0,))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM finite_pilot_control').fetchone(), (0,))
        for role, raw in self.before.items():
            if role != 'anchor':
                self.assertEqual(Path(self.bundle.paths[role]).read_bytes(), raw, role)
        with sqlite3.connect(self.bundle.paths['anchor']) as db:
            self.assertEqual(db.execute('PRAGMA application_id').fetchone(), (0,))
        output = json.dumps(result)
        for private in (self.bundle.binding, str(self.bundle.paths['anchor']), 'readonly_runtime_stage_failed',
                        'SYNTHETIC-INSPECTION-NOT-A-GRANT', str(self.original[1])):
            self.assertNotIn(private, output)

    def test_completed_repeat_is_no_output_no_mutation(self):
        self.run_inspection()
        before = {role: Path(path).read_bytes() for role, path in self.bundle.paths.items()}
        self.assertEqual(self.run_inspection(), inspect.public('inspection_consumed'))
        self.assertEqual(before, {role: Path(path).read_bytes() for role, path in self.bundle.paths.items()})

    def test_crash_after_claim_is_consumed_without_output_or_retry(self):
        def crash(name):
            if name == 'claimed':
                raise KeyboardInterrupt('synthetic interruption')
        with patch.object(inspect, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.run_inspection()
        self.assertEqual(self.receipt()[3], 'claimed')
        with self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        self.assertEqual(self.runner(), self.original)

    def test_crash_after_completion_never_reemits_hashes(self):
        def crash(name):
            if name == 'completed':
                raise KeyboardInterrupt('synthetic interruption')
        with patch.object(inspect, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.run_inspection()
        self.assertEqual(self.receipt()[3], 'complete')
        self.assertEqual(self.run_inspection(), inspect.public('inspection_consumed'))

    def test_all_cohort_and_application_writer_locks_cover_final_hashes(self):
        held = []
        def contender(name):
            if name != 'before_return':
                return
            for role in ('legacy', 'runner', 'decision', 'lifecycle', 'source', 'guard', 'manifest', 'anchor'):
                with sqlite3.connect(self.bundle.paths[role], timeout=0) as db:
                    with self.assertRaises(sqlite3.OperationalError):
                        db.execute('BEGIN IMMEDIATE')
                held.append(role)
        with patch.object(inspect, '_checkpoint', side_effect=contender):
            self.run_inspection()
        self.assertEqual(len(held), 8)

    def test_partial_schema_is_not_repaired(self):
        with sqlite3.connect(self.bundle.paths['anchor']) as db:
            db.execute('CREATE TABLE finite_inspection_receipt(id INTEGER)')
        with self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        with sqlite3.connect(self.bundle.paths['anchor']) as db:
            self.assertEqual(db.execute('PRAGMA table_info(finite_inspection_receipt)').fetchall()[0][1], 'id')
        self.assertEqual(self.runner(), self.original)

    def test_missing_cohort_file_not_recreated(self):
        Path(self.bundle.paths['runner']).unlink()
        with self.assertRaises(Exception):
            self.run_inspection()
        self.assertFalse(Path(self.bundle.paths['runner']).exists())

    def test_other_runner_state_is_not_inspected_or_reset(self):
        with sqlite3.connect(self.bundle.paths['runner']) as db:
            db.execute('UPDATE runner_control SET stopped=1')
        with self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        with sqlite3.connect(self.bundle.paths['anchor']) as db:
            self.assertIsNone(db.execute('SELECT 1 FROM sqlite_master WHERE name=?', (inspect.TABLE,)).fetchone())
        self.assertEqual(self.runner()[3], 1)

    def test_legacy_stop_is_preserved_without_creating_proof_claim(self):
        with sqlite3.connect(self.bundle.paths['legacy']) as db:
            db.execute('UPDATE safety SET stopped=1')
        with self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        with sqlite3.connect(self.bundle.paths['anchor']) as db:
            self.assertIsNone(db.execute('SELECT 1 FROM sqlite_master WHERE name=?', (inspect.TABLE,)).fetchone())

    def test_uncommitted_foundation_crash_leaves_only_consumed_claim(self):
        def crash(name):
            if name == 'foundation_created':
                raise KeyboardInterrupt('synthetic interruption')
        with patch.object(inspect, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.run_inspection()
        self.assertEqual(self.receipt()[3], 'claimed')
        with self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        self.assertEqual(self.runner(), self.original)

    def test_expiry_before_io_and_after_complete_never_emits_proof(self):
        self.now = self.manifest.expires_ns
        with patch.object(inspect, 'load_owned_ledger') as load, self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        load.assert_not_called()
        self.now = self.manifest.starts_ns + contract.NS
        def delay(name):
            if name == 'completed':
                self.now += 9 * contract.NS
        with patch.object(inspect, '_checkpoint', side_effect=delay), self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        self.assertEqual(self.receipt()[3], 'complete')
        self.assertEqual(self.run_inspection(), inspect.public('inspection_consumed'))

    def test_wrong_generation_or_runtime_overrides_fail_before_files(self):
        with patch.object(contract, 'APPROVED_INSPECTION', replace(self.manifest, bundle_generation='b' * 64)), \
             patch.object(inspect, 'load_owned_ledger') as load, self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        load.assert_not_called()
        with self.assertRaises(contract.PilotRefused):
            inspect.run_inspection(paths=self.bundle.paths)

    def test_hash_only_proof_supplies_exact_recovery_enrollment_without_raw_row(self):
        proof = self.run_inspection()['proof']
        recovery = contract.RecoveryManifest(contract.INCIDENT_ID, self.now - contract.NS,
            self.now + 60 * contract.NS, GENERATION,
            proof['bundle_seal_sha256'], proof['original_runner_sha256'],
            proof['protected_state_sha256'], 'a' * 64, 'b' * 64, 'c' * 64, 'd' * 64,
            'SYNTHETIC-RECOVERY-NOT-A-GRANT',
            anchor_pre_enrollment_sha256=proof['anchor_pre_enrollment_sha256'],
            pre_enrollment_cohort_sha256=proof['pre_enrollment_cohort_sha256'])
        with patch.object(contract, 'APPROVED_INSPECTION', None), \
             patch.object(contract, 'APPROVED_RECOVERY', recovery):
            self.assertTrue(AnchorJournal(self.bundle, lambda: self.now).enroll_recovery(recovery))
        self.assertEqual(self.runner(), self.original)

    def test_clock_regression_after_claim_retains_one_shot_claim(self):
        def regress(name):
            if name == 'claimed':
                self.now -= 1
        with patch.object(inspect, '_checkpoint', side_effect=regress), self.assertRaises(contract.PilotRefused):
            self.run_inspection()
        self.assertEqual(self.receipt()[3], 'claimed')
        with self.assertRaises(contract.PilotRefused):
            self.run_inspection()

    def test_pilot_or_recovery_grant_excludes_inspection(self):
        for name in ('APPROVED_PILOT', 'APPROVED_RECOVERY'):
            with patch.object(contract, name, object()), patch.object(inspect, 'load_owned_ledger') as load, \
                 self.assertRaises(contract.PilotRefused):
                self.run_inspection()
            load.assert_not_called()


if __name__ == '__main__':
    unittest.main()
