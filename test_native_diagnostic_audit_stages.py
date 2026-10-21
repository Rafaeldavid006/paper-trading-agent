"""Every refusal is fixed stage/role/code, never arbitrary exception text."""
from pathlib import Path
import json
import sqlite3
import unittest
from unittest.mock import patch

import native_diagnostic_audit_inspection as inspect
import test_native_diagnostic_audit_inspection as fixtures


class StageTests(unittest.TestCase):
    setUp = fixtures.AuditInspectionTests.setUp
    consumed = fixtures.AuditInspectionTests.consumed
    state = fixtures.AuditInspectionTests.state
    def call(self): return inspect.run_once(enabled=True, paths=self.paths, test_only=True)
    def failure(self):
        with self.assertRaises(inspect.InspectionFailure) as cm: self.call()
        result = cm.exception
        self.assertIn(result.stage, inspect.STAGES)
        self.assertIn(result.role, inspect.ROLES)
        self.assertIn(result.code, inspect.ERROR_CODES)
        return result.stage, result.role, result.code
    def update(self, role, sql):
        with sqlite3.connect(self.paths[role]) as db: db.execute(sql)

    def test_success_new_fields(self):
        self.consumed(); before=self.state();result=self.call()
        self.assertEqual((result['inspection_stage'],result['inspection_role'],result['inspection_code']),('complete','none','none'))
        self.assertEqual(before,self.state())

    def test_distinct_claim_record_failure(self):
        self.consumed(); self.update('anchor','UPDATE native_schema_diagnostic_claim SET requests=5')
        before=self.state();self.assertEqual(self.failure(),('claim_record','anchor','claim_contract_mismatch'))
        self.assertEqual(before,self.state())

    def test_distinct_missing_claim(self):
        self.consumed(); self.update('anchor','DROP TABLE native_schema_diagnostic_claim')
        self.assertEqual(self.failure(),('claim_schema','anchor','claim_schema_missing'))

    def test_distinct_missing_audit(self):
        self.consumed(); self.update('guard','DELETE FROM native_read_audit WHERE rowid=(SELECT MAX(rowid) FROM native_read_audit)')
        self.assertEqual(self.failure(),('audit_rows','guard','audit_history_incomplete'))

    def test_distinct_current_prior_and_time_checks(self):
        self.consumed(); path=Path(self.paths['guard']); original=path.read_bytes()
        cases=(('UPDATE native_read_audit SET attempts=5 WHERE rowid=(SELECT MAX(rowid) FROM native_read_audit)',('audit_current','guard','audit_current_mismatch')),
               ("UPDATE native_read_audit SET status='different' WHERE rowid=(SELECT MIN(rowid) FROM native_read_audit)",('audit_prior','guard','audit_prior_mismatch')),
               ('UPDATE native_read_audit SET at_ns=at_ns+16000000000 WHERE rowid=(SELECT MAX(rowid) FROM native_read_audit)',('audit_time','guard','audit_window_mismatch')))
        for sql,expected in cases:
            with self.subTest(expected=expected):
                self.update('guard',sql);self.assertEqual(self.failure(),expected);path.write_bytes(original)

    def test_nested_missing_source_role_keeps_inner_failure(self):
        self.consumed();Path(self.paths['source']).unlink()
        self.assertEqual(self.failure(),('bundle_verify_before_lock','source','file_missing'))

    def test_nested_source_content_gate(self):
        self.consumed();self.update('source',"UPDATE primary_meta SET binding='private bad data'")
        self.assertEqual(self.failure(),('bundle_verify_before_lock','source','bundle_source_state_corrupt'))

    def test_final_verification_cannot_keep_success_label(self):
        self.consumed(); original=inspect.RuntimeStateBundle.verify; calls=[]
        def final_fail(bundle):
            calls.append(None)
            if len(calls)==3: raise inspect.bundle_module.RuntimeStateRefused('bundle_inventory_or_schema_changed')
            return original(bundle)
        with patch.object(inspect.RuntimeStateBundle,'verify',final_fail):
            self.assertEqual(self.failure(),('bundle_verify_final','bundle','bundle_inventory_or_schema_changed'))
        self.assertEqual(len(calls),3)

    def test_helpers_restored_even_when_first_gate_fails(self):
        self.consumed();self.update('source',"UPDATE primary_meta SET binding='bad'")
        before=(inspect.bundle_module._private,inspect.bundle_module._database,inspect.bundle_module._require,
                inspect.RuntimeStateBundle._read_seal,inspect.bundle_module.load_owned_ledger)
        self.failure()
        after=(inspect.bundle_module._private,inspect.bundle_module._database,inspect.bundle_module._require,
                inspect.RuntimeStateBundle._read_seal,inspect.bundle_module.load_owned_ledger)
        self.assertEqual(before,after)

    def test_main_returns_only_literal_failure_fields(self):
        for error,expected in ((RuntimeError('private account secret /var/data/file'),'unexpected_error'),
                               (ValueError('private account secret /var/data/file'),'value_error')):
            with patch.object(inspect,'run_once',side_effect=error),patch('builtins.print') as emit:
                self.assertEqual(inspect.main(['--inspect-existing-audit']),1)
            raw=emit.call_args.args[0]; value=json.loads(raw)
            self.assertEqual(value['inspection_code'],expected)
            for private in ('private','secret','/var/data'):self.assertNotIn(private,raw)

    def test_all_report_error_values_are_bounded_enums(self):
        for stage in inspect.STAGES:
            for role in inspect.ROLES:
                for code in inspect.ERROR_CODES:
                    result=inspect.report('audit_inspection_unavailable',inspection_stage=stage,
                                          inspection_role=role,inspection_code=code)
                    self.assertEqual(result['provider_requests'],0)
                    self.assertFalse(result['state_mutated'])
        for key in ('inspection_stage','inspection_role','inspection_code'):
            with self.assertRaises(inspect.InspectionRefused):inspect.report('audit_inspection_unavailable',**{key:'private arbitrary text'})

if __name__=='__main__':unittest.main()
