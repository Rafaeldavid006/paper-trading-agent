"""Local synthetic cohorts only. Network/process entrypoints blocked BEFORE imports."""
from contextlib import ExitStack
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import unittest
from unittest.mock import Mock, patch

with ExitStack() as _blocked_imports:
    for _name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo', 'subprocess.Popen'):
        _blocked_imports.enter_context(patch(_name, side_effect=AssertionError('OFFLINE ONLY')))
    import finite_pilot_contract as contract
    import finite_pilot_anchor as anchor
    import finite_pilot_worker as worker
    from decimal_lifecycle import Lifecycle, SessionRiskPolicy
    from native_account_guard import SharedAccountGuard, _order_digest
    from native_paper_execution import NativeExecutionObserver, CoordinatedWire
    from alpaca_schema import parse_order
    from runtime_state_bundle import GENERATION, RuntimeStateRefused
    import test_runtime_state_bundle as fixture
    from test_native_paper_execution import Wire


def initialize_synthetic_foundation(bundle, now):
    """TEST ONLY: preserve clean/halted synthetic controls, initialize metadata.

    Actual fixed initialization tests separately exercise the incident gate.
    This helper cannot be selected by production worker or any CLI argument.
    """
    import finite_pilot_inspection as inspection
    import runtime_state_bundle as inventory
    from readonly_runtime_worker import serialized
    value = now() if callable(now) else now
    grant = contract.InspectionManifest(contract.INSPECTION_GENERATION, value - contract.NS,
        value + 600 * contract.NS, GENERATION, '9' * 64, 'synthetic:foundation')
    journal = anchor.AnchorJournal(bundle, lambda: value)
    with patch.object(contract, 'APPROVED_INSPECTION', grant), \
         patch.object(contract, 'APPROVED_PILOT', None), patch.object(contract, 'APPROVED_RECOVERY', None), \
         serialized(bundle):
        guard = SharedAccountGuard(bundle.paths['legacy'], bundle.paths['lifecycle'], bundle.binding,
            enabled=True, state_database=bundle.paths['guard'], test_only=True, runtime_bundle=bundle)
        with guard.hold(now_ns=value, read_only=True), ExitStack() as stack:
            handles = {role: stack.enter_context(inventory._database(bundle.paths[role], bundle._pinned[role],
                write=True, public_read=role == 'anchor'))
                for role in ('runner', 'decision', 'lifecycle', 'source', 'guard', 'manifest', 'anchor')}
            db = handles['anchor']
            db.execute(inspection.SQL)
            db.execute('INSERT INTO ' + inspection.TABLE + " VALUES(1,?,?,'claimed',?,NULL,?,?)",
                (contract.INSPECTION_GENERATION, contract.digest(grant.document()), value, *bundle._reference_identity('anchor')))
            db.commit()
            db.execute('BEGIN IMMEDIATE')
            journal.initialize_foundation(grant, anchor_db=db)
            db.execute('UPDATE ' + inspection.TABLE + " SET status='complete',completed_ns=? WHERE id=1", (value,))
            journal.finalize_foundation(grant, anchor_db=db)
            db.commit()
        return journal.founding_proofs()


class AnchorTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo', 'subprocess.Popen'):
            self.enterContext(patch(name, side_effect=AssertionError('OFFLINE ONLY')))
        self.b = fixture.BundleTests()
        self.b.setUp()
        self.addCleanup(self.b.doCleanups)
        self.b.enroll()
        self.bundle = self.b.bundle
        self.now = fixture.NOW
        self.journal = anchor.AnchorJournal(self.bundle, lambda: self.now)
        self.pilot = contract.PilotManifest('synthetic-only', self.now - contract.NS,
            self.now + 600 * contract.NS, GENERATION,
            hashlib.sha256(Path(self.bundle.paths['seal']).read_bytes()).hexdigest(),
            ('NVDA',), 3, 2, 3, 'a' * 64, 'synthetic:approval',
            anchor_pre_enrollment_sha256=hashlib.sha256(Path(self.bundle.paths['anchor']).read_bytes()).hexdigest(),
            pre_enrollment_cohort_sha256=self.journal.cohort_state_digest())
        self.enterContext(patch.object(contract, 'APPROVED_PILOT', self.pilot))
        self.enterContext(patch.object(contract, 'APPROVED_RECOVERY', None))
        self.wire = Wire(lambda: datetime.fromtimestamp(self.now / contract.NS, timezone.utc))
        self.validator_calls = 0
        self.validator = self._validate
        self.enterContext(patch.object(worker, 'validate_recovery_evidence', self.validator))

    def sql(self, role, query, args=()):
        with sqlite3.connect(self.bundle.paths[role]) as db:
            return db.execute(query, args).fetchall()

    def runner(self):
        return self.sql('runner', 'SELECT * FROM runner_control')[0]

    def controls(self):
        return self.sql('anchor', 'SELECT * FROM finite_pilot_control')[0]

    def enroll(self):
        if not self.sql('anchor', "SELECT 1 FROM sqlite_master WHERE name='finite_inspection_receipt'"):
            proof = initialize_synthetic_foundation(self.bundle, self.now)
            self.pilot = replace(self.pilot, **proof)
            contract.APPROVED_PILOT = self.pilot
        self.assertTrue(self.journal.enroll_pilot(self.pilot))

    def halt(self):
        self.sql('runner', "UPDATE runner_control SET heartbeat=?,dirty=1,attention='readonly_runtime_stage_failed',stage='started'",
                 (self.now / contract.NS - 25,))

    def prepare_recovery(self):
        self.halt()
        proof = initialize_synthetic_foundation(self.bundle, self.now)
        self.recovery = contract.RecoveryManifest(contract.INCIDENT_ID, self.now - contract.NS,
            self.now + 600 * contract.NS, GENERATION,
            self.pilot.bundle_seal_sha256, contract.digest(self.runner()), self.journal.protected_state_digest(),
            'b' * 64, 'c' * 64, 'd' * 64, 'e' * 64, 'synthetic:recovery',
            **proof)
        contract.APPROVED_RECOVERY = self.recovery
        self.journal.enroll_recovery(self.recovery)
        self.guard = SharedAccountGuard(self.bundle.paths['legacy'], self.bundle.paths['lifecycle'],
            self.bundle.binding, enabled=True, state_database=self.bundle.paths['guard'], test_only=True,
            runtime_bundle=self.bundle)

    def _validate(self, manifest, *, bundle, guard, now_ns):
        self.validator_calls += 1
        started = now_ns()
        guard.admit_cycle(now_ns=started)
        life = Lifecycle(bundle.paths['lifecycle'], bundle.binding, ('NVDA',),
                         session_risk_policy=SessionRiskPolicy(4, Decimal('100')))
        observer = NativeExecutionObserver(life, guard, CoordinatedWire(self.wire, guard, now_ns),
            now_ns=now_ns, monotonic_ns=lambda: 100000000000)
        observation = observer.collect()
        finished = now_ns()
        life._parse(observation.arguments(), finished)
        terminal = tuple(sorted((r['client_order_id'], _order_digest(parse_order(r), r)) for r in self.wire.legacy))
        receipts = tuple(sorted((r.client_order_id, r.observed_ns, r.evidence_digest)
                               for r in guard._last_reconciliation.absence_receipts))
        return contract.RecoveryEvidence(observation, started, finished, len(self.wire.calls), terminal, receipts)

    def recover(self):
        return self.journal.recover_incident(self.recovery, guard=self.guard, validate=self.validator)

    def test_production_constants_and_caller_objects_never_grant(self):
        with patch.object(contract, 'APPROVED_PILOT', None), patch.object(contract, 'APPROVED_RECOVERY', None), \
             patch('sqlite3.connect', side_effect=AssertionError('NO IO')):
            for operation in (lambda: self.journal.enroll_pilot(self.pilot),
                              lambda: self.journal.startup_gate(self.pilot)):
                with self.assertRaisesRegex(contract.PilotRefused, 'pilot_not_approved'):
                    operation()
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_not_approved'):
            self.journal.enroll_pilot(replace(self.pilot))
        with self.assertRaises(FrozenInstanceError):
            self.pilot.max_ticks = 9

    def test_contract_requires_exact_caps_universe_artifacts_and_window(self):
        for fields in ({'symbols': ('AAPL',)}, {'symbols': ['NVDA']}, {'max_ticks': True},
                       {'max_ticks': 11}, {'max_source_requests': 121}, {'max_write_attempts': 9},
                       {'approval_artifact_sha256': ''}, {'approval_reference': ''}, {'cadence_ns': 60 * contract.NS},
                       {'source_cadence_ns': 19 * contract.NS}, {'max_positions': 3}, {'max_shares': 2},
                       {'max_loss_usd': '101'}, {'max_entry_claims': 5}, {'starts_ns': True},
                       {'expires_ns': self.pilot.starts_ns}, {'expires_ns': self.now + 3601 * contract.NS}):
            with self.subTest(fields=fields), self.assertRaises(contract.PilotRefused):
                replace(self.pilot, **fields)

    def test_no_implicit_creation_or_schema_change_to_cohort(self):
        before = {r: Path(p).read_bytes() for r, p in self.bundle.paths.items()}
        with self.assertRaises(contract.PilotRefused):
            self.journal.startup_gate(self.pilot)
        self.assertEqual(before, {r: Path(p).read_bytes() for r, p in self.bundle.paths.items()})
        self.enroll()
        self.assertTrue(self.bundle.verify())
        for role in before:
            if role != 'anchor':
                self.assertEqual(before[role], Path(self.bundle.paths[role]).read_bytes(), role)
        self.assertEqual(Path(self.bundle.paths['anchor']).stat().st_mode & 0o777, 0o644)

    def test_pilot_enrollment_is_exact_idempotent_never_renewed(self):
        self.enroll()
        before = Path(self.bundle.paths['anchor']).read_bytes()
        self.assertTrue(self.journal.enroll_pilot(self.pilot))
        self.assertEqual(before, Path(self.bundle.paths['anchor']).read_bytes())
        other = replace(self.pilot, pilot_id='another')
        with patch.object(contract, 'APPROVED_PILOT', other), self.assertRaisesRegex(contract.PilotRefused, 'manifest_changed'):
            self.journal.enroll_pilot(other)

    def test_dirty_stage_blocks_restart_finish_exactly_once_and_cadence(self):
        self.enroll()
        self.journal.begin_stage(self.pilot, 'tick')
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_stage_incomplete'):
            anchor.AnchorJournal(self.bundle, lambda: self.now).startup_gate(self.pilot)
        self.journal.finish_stage(self.pilot, 'tick')
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_stage_not_active'):
            self.journal.finish_stage(self.pilot, 'tick')
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_cadence_wait'):
            self.journal.begin_stage(self.pilot, 'tick')
        self.now += 65 * contract.NS
        self.journal.begin_stage(self.pilot, 'tick')
        self.assertEqual(self.controls()[1], 2)

    def test_write_attempt_commits_before_wire_and_no_refund_or_replay(self):
        self.enroll()
        self.journal.begin_stage(self.pilot, 'tick')
        with self.assertRaises(TimeoutError):
            with self.journal.write_attempt(self.pilot, 'POST', 'cid-1'):
                self.assertEqual(self.sql('anchor', 'SELECT method,target FROM finite_pilot_writes'), [('POST', 'cid-1')])
                raise TimeoutError('synthetic only')
        self.assertEqual(self.controls()[2], 1)
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_write_replay'):
            with self.journal.write_attempt(self.pilot, 'POST', 'cid-1'):
                self.fail('replayed')
        with self.journal.write_attempt(self.pilot, 'DELETE', '/v2/orders/order-1'):
            pass
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_write_cap'):
            with self.journal.write_attempt(self.pilot, 'POST', 'cid-2'):
                self.fail('over budget')

    def test_source_total_cap_and_cadence(self):
        self.enroll()
        self.journal.begin_stage(self.pilot, 'source')
        for _ in range(3):
            self.journal.reserve_source_request(self.pilot)
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_source_cap'):
            self.journal.reserve_source_request(self.pilot)
        self.journal.finish_stage(self.pilot, 'source')
        self.now += 20 * contract.NS
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_budget_exhausted'):
            self.journal.begin_stage(self.pilot, 'source')
        self.assertEqual(self.controls()[3], 3)

    def test_tick_cap_survives_restart(self):
        self.enroll()
        for _ in range(3):
            self.journal.begin_stage(self.pilot, 'tick')
            self.journal.finish_stage(self.pilot, 'tick')
            self.now += 65 * contract.NS
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_budget_exhausted'):
            anchor.AnchorJournal(self.bundle, lambda: self.now).begin_stage(self.pilot, 'tick')

    def test_expiry_backward_clock_and_stop_refuse_without_clear(self):
        self.enroll()
        self.journal.begin_stage(self.pilot, 'tick')
        self.now = self.pilot.expires_ns
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_window_closed'):
            with self.journal.write_attempt(self.pilot, 'POST', 'no-wire'):
                self.fail('expired')
        self.now = self.pilot.starts_ns
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_clock_regression'):
            self.journal.check_stage(self.pilot, 'tick')
        self.now += contract.NS
        self.sql('runner', "UPDATE runner_control SET stopped=1,stage='stopped'")
        with self.assertRaisesRegex(contract.PilotRefused, 'runner_attention_or_stopped'):
            self.journal.finish_stage(self.pilot, 'tick')
        self.assertEqual(self.runner()[3], 1)
        self.assertEqual(self.controls()[4], 1)

    def test_missing_tables_and_extra_trigger_fail_closed(self):
        self.enroll()
        self.sql('anchor', 'DROP TABLE finite_pilot_writes')
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_journal_schema_invalid'):
            self.journal.startup_gate(self.pilot)
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_journal_schema_invalid'):
            self.journal.enroll_pilot(self.pilot)

    def test_unrelated_named_trigger_on_journal_rejected(self):
        self.enroll()
        self.sql('anchor', 'CREATE TRIGGER unrelated AFTER INSERT ON finite_pilot_sources BEGIN SELECT 1; END')
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_journal_schema_invalid'):
            self.journal.startup_gate(self.pilot)

    def test_replaced_anchor_and_missing_state_never_recreated(self):
        self.enroll()
        path = Path(self.bundle.paths['anchor'])
        replacement = path.with_suffix('.copy')
        shutil.copyfile(path, replacement)
        replacement.chmod(0o644)
        replacement.replace(path)
        with self.assertRaises(RuntimeStateRefused):
            self.journal.startup_gate(self.pilot)

    def test_legacy_gate_absence_only_without_grants_and_pilot_owns_cohort(self):
        with patch.object(contract, 'APPROVED_PILOT', None):
            self.assertTrue(anchor.read_only_startup_gate(self.bundle))
        with self.assertRaisesRegex(contract.PilotRefused, 'approved_journal_missing'):
            anchor.read_only_startup_gate(self.bundle)
        self.enroll()
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_owns_runtime'):
            anchor.read_only_startup_gate(self.bundle)

    def test_recovery_exact_success_preserves_protected_state(self):
        self.prepare_recovery()
        before = self.journal.protected_state_digest()
        old = self.runner()
        result = self.recover()
        self.assertEqual(result['phase'], 'recovered_readonly')
        self.assertEqual(self.runner(), (1, self.now / contract.NS, 0, 0, None, 'recovered_readonly'))
        self.assertEqual(self.journal.protected_state_digest(), before)
        self.assertEqual(self.sql('anchor', 'SELECT status FROM finite_pilot_recovery_journal ORDER BY seq'),
                         [('prepared',), ('applied',)])
        self.assertEqual(self.validator_calls, 1)
        self.assertTrue(self.bundle.verify())
        with patch.object(contract, 'APPROVED_PILOT', None):
            self.assertTrue(anchor.read_only_startup_gate(self.bundle))
        self.assertNotEqual(self.runner(), old)

    def test_applied_restart_is_noop_even_after_expiry_not_second_recovery(self):
        self.prepare_recovery()
        self.recover()
        self.now = self.recovery.expires_ns + contract.NS
        before = {r: Path(p).read_bytes() for r, p in self.bundle.paths.items()}
        self.assertTrue(self.journal.enroll_recovery(self.recovery))
        self.assertEqual(self.recover()['phase'], 'already_recovered_readonly')
        self.assertEqual(self.validator_calls, 1)
        self.assertEqual(before, {r: Path(p).read_bytes() for r, p in self.bundle.paths.items()})

    def test_prepared_crash_consumes_attempt_blocks_clean_runner_and_every_restart(self):
        self.prepare_recovery()
        def crash(name):
            if name == 'prepared':
                raise KeyboardInterrupt('synthetic crash')
        with patch.object(self.journal, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.recover()
        self.assertEqual(self.validator_calls, 0)
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
            self.recover()
        self.sql('runner', "UPDATE runner_control SET dirty=0,attention=NULL,stage='recovered_readonly'")
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
            anchor.read_only_startup_gate(self.bundle)
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
            self.journal.enroll_recovery(self.recovery)

    def test_crash_after_cas_commit_remains_blocking_no_reapply(self):
        self.prepare_recovery()
        def crash(name):
            if name == 'cas_committed':
                raise KeyboardInterrupt('synthetic crash')
        with patch.object(self.journal, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.recover()
        self.assertEqual(self.runner()[2:5], (0, 0, None))
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
            self.recover()
        self.assertEqual(self.validator_calls, 1)
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
            anchor.read_only_startup_gate(self.bundle)

    def test_crash_before_cas_commit_preserves_original_row(self):
        self.prepare_recovery()
        old = self.runner()
        def crash(name):
            if name == 'cas_before_commit':
                raise KeyboardInterrupt('synthetic crash')
        with patch.object(self.journal, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.recover()
        self.assertEqual(self.runner(), old)
        self.assertEqual(self.sql('anchor', 'SELECT status FROM finite_pilot_recovery_journal'), [('prepared',)])

    def test_crash_before_applied_commit_leaves_clean_but_blocking(self):
        self.prepare_recovery()
        def crash(name):
            if name == 'applied_before_commit':
                raise KeyboardInterrupt('synthetic crash')
        with patch.object(self.journal, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.recover()
        self.assertEqual(self.runner()[2:5], (0, 0, None))
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_incomplete'):
            self.recover()

    def test_changed_original_runner_prevents_preparation(self):
        self.prepare_recovery()
        self.sql('runner', 'UPDATE runner_control SET heartbeat=heartbeat+1')
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_runner_changed'):
            self.recover()
        self.assertEqual(self.sql('anchor', 'SELECT * FROM finite_pilot_recovery_journal'), [])
        self.assertEqual(self.validator_calls, 0)

    def test_expired_recovery_and_wrong_validator_no_attempt(self):
        self.prepare_recovery()
        with self.assertRaisesRegex(contract.PilotRefused, 'reviewed_recovery_validator_required'):
            self.journal.recover_incident(self.recovery, guard=self.guard, validate=lambda *a, **k: True)
        self.now = self.recovery.expires_ns
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_window_closed'):
            self.recover()
        self.assertEqual(self.sql('anchor', 'SELECT * FROM finite_pilot_recovery_journal'), [])

    def test_changed_protected_data_during_validator_leaves_prepared(self):
        self.prepare_recovery()
        def changed(name):
            if name == 'validated':
                self.sql('source', 'UPDATE primary_meta SET last_clock=?', (self.now,))
        with patch.object(self.journal, '_checkpoint', side_effect=changed), \
             self.assertRaisesRegex(contract.PilotRefused, 'recovery_protected_state_changed'):
            self.recover()
        self.assertEqual(self.runner()[2], 1)
        self.assertEqual(self.sql('anchor', 'SELECT status FROM finite_pilot_recovery_journal'), [('prepared',)])

    def test_stop_after_observation_wins_and_is_never_cleared(self):
        self.prepare_recovery()
        def stopped(name):
            if name == 'validated':
                self.sql('runner', "UPDATE runner_control SET stopped=1,stage='stopped'")
        with patch.object(self.journal, '_checkpoint', side_effect=stopped), \
             self.assertRaisesRegex(contract.PilotRefused, 'exact_incident_runner_required'):
            self.recover()
        self.assertEqual(self.runner()[3], 1)

    def test_wrong_evidence_type_consumes_without_clearing(self):
        self.prepare_recovery()
        bogus = Mock(return_value=True)
        with patch.object(worker, 'validate_recovery_evidence', bogus), \
             self.assertRaisesRegex(contract.PilotRefused, 'typed_recovery_evidence_required'):
            self.journal.recover_incident(self.recovery, guard=self.guard, validate=bogus)
        self.assertEqual(self.runner()[2], 1)
        self.assertEqual(self.sql('anchor', 'SELECT status FROM finite_pilot_recovery_journal'), [('prepared',)])

    def test_prior_guard_evidence_may_not_be_erased_by_validator(self):
        self.sql('guard', "INSERT INTO native_read_audit VALUES(?,'old_failure',0,0,0,NULL)", (self.now - 100,))
        self.prepare_recovery()
        def erased(name):
            if name == 'validated':
                self.sql('guard', "DELETE FROM native_read_audit WHERE status='old_failure'")
        with patch.object(self.journal, '_checkpoint', side_effect=erased), \
             self.assertRaisesRegex(contract.PilotRefused, 'recovery_guard_history_changed'):
            self.recover()
        self.assertEqual(self.runner()[2], 1)

    def test_drop_all_journal_tables_cannot_reenroll_spent_budget(self):
        self.enroll()
        self.journal.begin_stage(self.pilot, 'tick')
        self.journal.finish_stage(self.pilot, 'tick')
        for table in anchor.SCHEMA:
            self.sql('anchor', 'DROP TABLE ' + table)
        self.assertTrue(self.bundle.verify())  # Original selected anchor schema still intact.
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_journal_schema_invalid'):
            self.journal.enroll_pilot(self.pilot)
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_journal_schema_invalid'):
            self.journal.startup_gate(self.pilot)

    def test_normal_enrollment_never_creates_missing_foundation(self):
        self.sql('anchor', "INSERT INTO health VALUES('synthetic','t','t','ok')")
        before = Path(self.bundle.paths['anchor']).read_bytes()
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_journal_schema_invalid'):
            self.journal.enroll_pilot(self.pilot)
        self.assertEqual(before, Path(self.bundle.paths['anchor']).read_bytes())
        self.assertEqual(self.sql('anchor', "SELECT name FROM sqlite_master WHERE name LIKE 'finite_pilot_%'"), [])

    def test_exhausted_final_stage_finishes_clean_then_all_new_stages_refuse(self):
        self.enroll()
        for _ in range(self.pilot.max_ticks):
            self.journal.begin_stage(self.pilot, 'tick')
            result = self.journal.finish_stage(self.pilot, 'tick')
            self.now += 65 * contract.NS
        self.assertTrue(result['budget_exhausted'])
        self.assertEqual(self.controls()[4], 0)
        for stage in ('source', 'tick'):
            with self.assertRaisesRegex(contract.PilotRefused, 'pilot_budget_exhausted'):
                self.journal.begin_stage(self.pilot, stage)

    def test_final_get_gate_rechecks_time_after_control_reads(self):
        self.enroll()
        self.journal.begin_stage(self.pilot, 'source')
        old = self.journal._controls
        def late(**kwargs):
            result = old(**kwargs)
            self.now = self.pilot.expires_ns
            return result
        with patch.object(self.journal, '_controls', side_effect=late), \
             self.assertRaisesRegex(contract.PilotRefused, 'pilot_window_closed'):
            self.journal.check_stage(self.pilot, 'source')

    def test_fabricated_new_guard_receipt_is_not_permitted_progression(self):
        self.prepare_recovery()
        def injected(name):
            if name == 'validated':
                self.sql('guard', 'INSERT INTO legacy_absence_receipts VALUES(?,?,?)',
                         ('not-an-owned-reservation', self.now, 'f' * 64))
        with patch.object(self.journal, '_checkpoint', side_effect=injected), \
             self.assertRaisesRegex(contract.PilotRefused, 'recovery_receipt_invalid'):
            self.recover()
        self.assertEqual(self.runner()[2], 1)

    def test_final_runner_lock_conflict_consumes_attempt_without_cas(self):
        self.prepare_recovery()
        old = self.runner()
        with sqlite3.connect(self.bundle.paths['runner'], isolation_level=None) as blocker:
            blocker.execute('BEGIN IMMEDIATE')
            with self.assertRaises(sqlite3.OperationalError):
                self.recover()
            blocker.rollback()
        self.assertEqual(self.runner(), old)
        self.assertEqual(self.sql('anchor', 'SELECT status FROM finite_pilot_recovery_journal'), [('prepared',)])

    def test_recovery_enrollment_requires_exact_original_protected_digest(self):
        self.prepare_recovery()
        # This validates existing recovery authority cannot be substituted with
        # a different compiled object/digest to normalize the same old state.
        altered = replace(self.recovery, protected_state_sha256='f' * 64)
        with patch.object(contract, 'APPROVED_RECOVERY', altered), \
             self.assertRaisesRegex(contract.PilotRefused, 'anchor_manifest_changed'):
            self.journal.enroll_recovery(altered)

    def test_anchor_only_restore_cannot_refill_advanced_cohort_budget(self):
        original_anchor = Path(self.bundle.paths['anchor']).read_bytes()
        self.enroll()
        self.journal.begin_stage(self.pilot, 'source')
        self.journal.reserve_source_request(self.pilot)
        self.journal.finish_stage(self.pilot, 'source')
        self.sql('source', 'UPDATE primary_meta SET last_clock=?', (self.now,))
        Path(self.bundle.paths['anchor']).write_bytes(original_anchor)  # Same inode, no full-cohort restore.
        self.assertTrue(self.bundle.verify())
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_journal_schema_invalid'):
            self.journal.enroll_pilot(self.pilot)

    def test_deleting_every_pilot_row_preserves_once_enrollment_marker(self):
        self.enroll()
        self.journal.begin_stage(self.pilot, 'tick')
        self.journal.finish_stage(self.pilot, 'tick')
        for table in ('finite_pilot_enrollment', 'finite_pilot_control', 'finite_pilot_stages',
                      'finite_pilot_writes', 'finite_pilot_sources'):
            self.sql('anchor', 'DELETE FROM ' + table)
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_enrollment_rows_missing'):
            self.journal.enroll_pilot(self.pilot)
        with self.assertRaisesRegex(contract.PilotRefused, 'anchor_enrollment_rows_missing'):
            self.journal.startup_gate(self.pilot)

    def test_recovery_anchor_only_restore_cannot_repeat_consumed_get(self):
        original_anchor = Path(self.bundle.paths['anchor']).read_bytes()
        self.prepare_recovery()
        def crash(name):
            if name == 'validated':
                raise KeyboardInterrupt('synthetic crash after GET')
        with patch.object(self.journal, '_checkpoint', side_effect=crash), self.assertRaises(KeyboardInterrupt):
            self.recover()
        Path(self.bundle.paths['anchor']).write_bytes(original_anchor)
        with self.assertRaisesRegex(contract.PilotRefused, 'foundation_receipt_schema_invalid'):
            self.journal.enroll_recovery(self.recovery)
        self.assertEqual(self.validator_calls, 1)

    def test_recovery_then_pilot_uses_same_bound_founding_proof(self):
        self.prepare_recovery()
        self.recover()
        combined = replace(self.pilot,
            anchor_pre_enrollment_sha256=self.recovery.anchor_pre_enrollment_sha256,
            pre_enrollment_cohort_sha256=self.recovery.pre_enrollment_cohort_sha256)
        self.assertNotEqual(self.journal.cohort_state_digest(), combined.pre_enrollment_cohort_sha256)
        with patch.object(contract, 'APPROVED_PILOT', combined):
            self.assertTrue(self.journal.enroll_pilot(combined))
            self.assertEqual(self.journal.startup_gate(combined)['ticks'], 0)
        wrong = replace(combined, pre_enrollment_cohort_sha256='f' * 64)
        with patch.object(contract, 'APPROVED_PILOT', wrong), \
             self.assertRaisesRegex(contract.PilotRefused, 'anchor_founding_binding_changed'):
            self.journal.enroll_pilot(wrong)

    def test_post_recovery_pre_pilot_anchor_restore_cannot_refill(self):
        self.prepare_recovery()
        self.recover()
        before_pilot = Path(self.bundle.paths['anchor']).read_bytes()
        combined = replace(self.pilot,
            anchor_pre_enrollment_sha256=self.recovery.anchor_pre_enrollment_sha256,
            pre_enrollment_cohort_sha256=self.recovery.pre_enrollment_cohort_sha256)
        with patch.object(contract, 'APPROVED_PILOT', combined):
            self.journal.enroll_pilot(combined)
            self.journal.begin_stage(combined, 'source')
            self.journal.reserve_source_request(combined)
            self.journal.finish_stage(combined, 'source')
            self.sql('source', 'UPDATE primary_meta SET last_clock=?', (self.now,))
            Path(self.bundle.paths['anchor']).write_bytes(before_pilot)
            with self.assertRaisesRegex(contract.PilotRefused, 'pilot_initial_cohort_changed'):
                self.journal.enroll_pilot(combined)

    def test_prior_unrelated_app_progress_does_not_invalidate_reviewed_proof(self):
        self.prepare_recovery()
        proof = self.journal.protected_state_digest()
        self.sql('anchor', "INSERT INTO health VALUES('unrelated-app','new','new','ok')")
        self.assertEqual(self.journal.protected_state_digest(), proof)
        self.assertEqual(self.recover()['phase'], 'recovered_readonly')
        self.assertEqual(self.sql('anchor', 'SELECT * FROM health'), [('unrelated-app', 'new', 'new', 'ok')])

    def test_unrelated_app_progress_during_attempt_refuses_without_cas(self):
        self.prepare_recovery()
        def mutated(name):
            if name == 'validated':
                self.sql('anchor', "INSERT INTO health VALUES('unrelated-app','new','new','ok')")
        with patch.object(self.journal, '_checkpoint', side_effect=mutated), \
             self.assertRaisesRegex(contract.PilotRefused, 'recovery_application_state_changed'):
            self.recover()
        self.assertEqual(self.runner()[2], 1)
        self.assertEqual(self.sql('anchor', 'SELECT status FROM finite_pilot_recovery_journal'), [('prepared',)])

    def test_private_binding_lookup_is_approved_and_hash_bound_not_public(self):
        self.enroll()
        self.assertNotIn('account_fingerprint', self.pilot.document())
        resolved = anchor.resolve_approved_bundle(self.pilot, paths=self.bundle.paths, test_only=True)
        self.assertEqual(resolved.binding, self.bundle.binding)
        with patch.object(contract, 'APPROVED_PILOT', None), \
             patch('os.open', side_effect=AssertionError('NO FILE ACCESS')), \
             self.assertRaisesRegex(contract.PilotRefused, 'pilot_not_approved'):
            anchor.resolve_approved_bundle(self.pilot, paths=self.bundle.paths, test_only=True)
        wrong = replace(self.pilot, bundle_seal_sha256='f' * 64)
        with patch.object(contract, 'APPROVED_PILOT', wrong), \
             self.assertRaisesRegex(contract.PilotRefused, 'manifest_seal_mismatch'):
            anchor.resolve_approved_bundle(wrong, paths=self.bundle.paths, test_only=True)

    def test_restored_empty_foundation_refuses_advanced_pilot_cohort(self):
        proof = initialize_synthetic_foundation(self.bundle, self.now)
        self.pilot = replace(self.pilot, **proof)
        contract.APPROVED_PILOT = self.pilot
        empty = Path(self.bundle.paths['anchor']).read_bytes()
        self.journal.enroll_pilot(self.pilot)
        self.journal.begin_stage(self.pilot, 'source')
        self.journal.reserve_source_request(self.pilot)
        self.journal.finish_stage(self.pilot, 'source')
        self.sql('source', 'UPDATE primary_meta SET last_clock=?', (self.now,))
        Path(self.bundle.paths['anchor']).write_bytes(empty)
        with self.assertRaisesRegex(contract.PilotRefused, 'pilot_initial_cohort_changed'):
            self.journal.enroll_pilot(self.pilot)

    def test_original_diagnostic_claim_is_stable_critical_anchor_evidence(self):
        self.sql('anchor', 'CREATE TABLE native_schema_diagnostic_claim(id INTEGER PRIMARY KEY,status TEXT,digest TEXT)')
        self.sql('anchor', "INSERT INTO native_schema_diagnostic_claim VALUES(1,'completed',?)", ('a' * 64,))
        self.prepare_recovery()
        self.sql('anchor', "UPDATE native_schema_diagnostic_claim SET status='claimed'")
        with self.assertRaisesRegex(contract.PilotRefused, 'recovery_protected_state_changed'):
            self.recover()
        self.assertEqual(self.sql('anchor', 'SELECT * FROM finite_pilot_recovery_journal'), [])

    def test_missing_foundation_never_repaired_by_old_readonly_gate(self):
        initialize_synthetic_foundation(self.bundle, self.now)
        for table in anchor.SCHEMA:
            self.sql('anchor', 'DROP TABLE ' + table)
        before = Path(self.bundle.paths['anchor']).read_bytes()
        with patch.object(contract, 'APPROVED_PILOT', None), \
             self.assertRaisesRegex(contract.PilotRefused, 'foundation_incomplete'):
            anchor.read_only_startup_gate(self.bundle)
        self.assertEqual(before, Path(self.bundle.paths['anchor']).read_bytes())


if __name__ == '__main__':
    unittest.main()
