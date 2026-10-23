"""Old code-pinned operations are retired, never repinned to authorize this release."""
import hashlib
import threading
import unittest
from unittest.mock import Mock, patch
import finite_pilot_contract as contract
import native_diagnostic_audit_inspection as audit
import native_get_diagnostic as get
import native_restart_apply_once as migration
import native_schema_diagnostic as schema


class RetiredCodeBoundaries(unittest.TestCase):
    def test_old_audit_fails_code_pin_before_any_mutable_state(self):
        bomb=Mock(side_effect=AssertionError('NO STATE OR TRANSPORT'))
        with patch.object(audit,'RuntimeStateBundle',bomb),patch.object(audit,'_database',bomb),patch.object(audit,'_private',bomb):
            with self.assertRaisesRegex(audit.InspectionRefused,'audit_inspection_unavailable') as error:
                audit.run_once(enabled=True)
            self.assertEqual(error.exception.code,'code_pin_mismatch')
        bomb.assert_not_called()

    def test_consumed_get_retired_before_clock_and_cannot_repin_when_enabled(self):
        self.assertFalse(get.ENABLE_APPROVED_GET_DIAGNOSTIC)
        bomb=Mock(side_effect=AssertionError('NO STATE OR TRANSPORT'))
        with patch.object(get,'code_hash',bomb):
            self.assertEqual(get.run_once()['reason'],'diagnostic_disabled')
        bomb.assert_not_called()
        with patch.object(get,'load_owned_ledger',bomb),patch.object(get,'RuntimeStateBundle',bomb):
            with self.assertRaisesRegex(audit.InspectionRefused,'code_pin_mismatch'):
                get.run_once(test_only=True,enabled=True,window=(1,100),now_ns=lambda:2)
        bomb.assert_not_called()

    def test_old_migration_source_pin_cannot_be_reused_for_changed_core(self):
        self.assertFalse(migration.ENABLE_APPROVED_MIGRATION)
        self.assertFalse(migration.EXTERNAL_WRITER_QUIESCENCE_CONFIRMED)
        self.assertNotEqual(migration.preflight.source_digest(),migration.EXPECTED_CORE_CODE_DIGEST)
        bomb=Mock(side_effect=AssertionError('NO RESERVATION APPLY OR CHILD'))
        reports=[]
        with patch.object(migration,'ENABLE_APPROVED_MIGRATION',True), \
             patch.object(migration,'EXTERNAL_WRITER_QUIESCENCE_CONFIRMED',True), \
             patch.object(migration,'_reserve_attempt',bomb),patch.object(migration,'_completed_attempt',bomb):
            self.assertFalse(migration.run_startup_gate(inspection_only=True,
                stop=threading.Event(),popen=bomb,emit_report=reports.append))
        bomb.assert_not_called()
        self.assertEqual(reports[-1]['code'],'frozen_code_mismatch')

    def test_all_future_grants_absent_and_old_diagnostic_artifact_unchanged(self):
        self.assertIsNone(contract.APPROVED_INSPECTION)
        self.assertIsNone(contract.APPROVED_RECOVERY)
        self.assertIsNone(contract.APPROVED_PILOT)
        from pathlib import Path
        self.assertEqual(hashlib.sha256(Path(schema.__file__).read_bytes()).hexdigest(),
                         audit.FROZEN_DIAGNOSTIC_SHA)


if __name__=='__main__':unittest.main()
