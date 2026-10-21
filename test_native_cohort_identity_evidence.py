"""Synthetic-only identity comparisons. All networking remains prohibited."""
import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

import native_cohort_identity_evidence as evidence
import native_diagnostic_audit_inspection as inspector
import runtime_state_bundle as bundle
import test_runtime_state_bundle as fixture_module


class IdentityEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture_module.BundleTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.enroll()
        self.paths = self.fixture.paths

    def state(self):
        return {str(p): (p.read_bytes(), p.stat().st_dev, p.stat().st_ino,
            p.stat().st_mode) for p in self.fixture.root.iterdir() if p.is_file()}

    def inspect(self):
        before = self.state()
        result = evidence.inspect_identity_evidence(enabled=True, paths=self.paths, test_only=True)
        self.assertEqual(before, self.state())
        self.assertIs(result['trusted'], False)
        self.assertIs(result['runtime_gate_passed'], False)
        self.assertEqual(result['provider_requests'], 0)
        self.assertFalse(result['state_mutated'])
        return result

    def old_identity_delta(self, *, device=0, inode=0):
        with sqlite3.connect(self.paths['manifest']) as db:
            db.execute('UPDATE bundle_binding SET seal_device=seal_device+?,seal_inode=seal_inode+?', (device, inode))
            db.execute('UPDATE bundle_files SET device=device+?,inode=inode+?', (device, inode))
            binding = db.execute('SELECT * FROM bundle_binding').fetchone()
            records = db.execute('SELECT role,path,device,inode,schema_hash FROM bundle_files ORDER BY role').fetchall()
        digest = bundle._digest([binding, records])
        sealpath = Path(self.paths['seal'])
        seal = json.loads(sealpath.read_bytes())
        seal['manifest_identity'][0] += device; seal['manifest_identity'][1] += inode
        seal['manifest_digest'] = digest
        sealpath.write_text(json.dumps(seal))
        with sqlite3.connect(self.paths['anchor']) as db:
            db.execute('UPDATE native_runtime_enrollment SET anchor_device=anchor_device+?,anchor_inode=anchor_inode+?,'
                'seal_device=seal_device+?,seal_inode=seal_inode+?,manifest_device=manifest_device+?,manifest_inode=manifest_inode+?,'
                'manifest_digest=?', (device, inode, device, inode, device, inode, digest))
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute('UPDATE account_guard_binding SET legacy_device=legacy_device+?,legacy_inode=legacy_inode+?,'
                'lifecycle_device=lifecycle_device+?,lifecycle_inode=lifecycle_inode+?', (device, inode, device, inode))

    def test_unchanged_all_references_observed_but_never_trusted(self):
        result = self.inspect()
        self.assertEqual(result['status'], 'untrusted_comparisons_observed')
        self.assertEqual(len(result['identity_references']), 15)
        for row in result['identity_references'].values(): self.assertEqual(set(row.values()), {True})
        self.assertTrue(all(result['checks'].values()))
        self.assertEqual(result['full_mutable_content_continuity'], 'not_pinned')
        self.assertIsNone(result['roles']['manifest']['schema_match'])
        self.assertIsNone(result['roles']['anchor']['generation_stamp_match'])

    def test_device_only_mismatch_for_all_edges_is_not_cleared(self):
        self.old_identity_delta(device=1)
        with self.assertRaisesRegex(bundle.RuntimeStateRefused, 'bundle_anchor_replaced'):
            self.fixture.make().verify()
        result = self.inspect()
        for row in result['identity_references'].values():
            self.assertEqual(row, dict(device_match=False, inode_match=True, stored_references_agree=True))
        self.assertTrue(all(result['checks'].values()))
        with self.assertRaisesRegex(bundle.RuntimeStateRefused, 'bundle_anchor_replaced'):
            self.fixture.make().verify()

    def test_inode_only_mismatch_for_all_edges_remains_untrusted(self):
        self.old_identity_delta(inode=1)
        result = self.inspect()
        for row in result['identity_references'].values():
            self.assertEqual(row, dict(device_match=True, inode_match=False, stored_references_agree=True))

    def test_disagreeing_anchor_reference_is_distinguished(self):
        with sqlite3.connect(self.paths['anchor']) as db:
            db.execute('UPDATE native_runtime_enrollment SET anchor_device=anchor_device+1')
        result = self.inspect()
        a = result['identity_references']['enrollment_anchor']
        m = result['identity_references']['manifest_anchor']
        self.assertFalse(a['device_match']); self.assertTrue(m['device_match'])
        self.assertFalse(a['stored_references_agree']); self.assertFalse(m['stored_references_agree'])

    def test_manifest_digest_and_history_mismatches_are_reported(self):
        with sqlite3.connect(self.paths['anchor']) as db:
            db.execute("UPDATE native_runtime_enrollment SET manifest_digest=?", ('0'*64,))
            db.execute("UPDATE events SET payload='changed'")
        result = self.inspect()
        self.assertFalse(result['checks']['manifest_digest_to_anchor'])
        self.assertTrue(result['checks']['manifest_digest_to_seal'])
        self.assertFalse(result['checks']['anchor_history_digest'])

    def test_missing_role_does_not_block_independent_comparisons(self):
        Path(self.paths['source']).unlink()
        result = self.inspect()
        self.assertFalse(result['roles']['source']['safe_access'])
        self.assertIsNone(result['identity_references']['manifest_source']['device_match'])
        self.assertTrue(result['identity_references']['manifest_guard']['device_match'])
        self.assertEqual(result['roles']['source']['code'], 'file_missing')

    def test_unsafe_role_never_opened(self):
        os.chmod(self.paths['source'], 0o666)
        result = self.inspect()
        self.assertFalse(result['roles']['source']['safe_access'])
        self.assertIsNone(result['roles']['source']['schema_match'])
        self.assertEqual(result['roles']['source']['code'], 'unsafe_bundle_file')

    def test_schema_and_stamp_mismatches_are_distinct(self):
        with sqlite3.connect(self.paths['source']) as db:
            db.execute('CREATE TABLE unrelated(id INTEGER)')
        with sqlite3.connect(self.paths['decision']) as db:
            db.execute('PRAGMA application_id=1')
        result = self.inspect()
        self.assertIsNone(result['roles']['source']['schema_match'])
        self.assertEqual(result['roles']['source']['code'], 'bundle_schema_incomplete')
        self.assertFalse(result['roles']['decision']['generation_stamp_match'])

    def test_stored_paths_are_compared_not_followed_or_output(self):
        poison = '/private/never-open-this-secret-account'
        with sqlite3.connect(self.paths['manifest']) as db:
            db.execute('UPDATE bundle_files SET path=? WHERE role=?', (poison, 'source'))
        result = self.inspect()
        self.assertFalse(result['checks']['manifest_paths'])
        raw = json.dumps(result)
        self.assertNotIn(poison, raw)
        for value in self.paths.values(): self.assertNotIn(value, raw)

    def test_disabled_never_reads_state(self):
        with patch.object(inspector, '_frozen_code', side_effect=AssertionError('forbidden')):
            self.assertEqual(evidence.inspect_identity_evidence()['status'], 'disabled')

    def test_exact_inspector_failure_adds_detail_without_audit_success(self):
        exc = inspector.InspectionFailure('bundle_verify_before_lock', 'anchor', 'bundle_anchor_replaced')
        with patch.object(inspector, 'run_once', side_effect=exc), patch.object(evidence,
                'inspect_identity_evidence', return_value=evidence.blank('untrusted_partial')) as read:
            output = io.StringIO()
            with contextlib.redirect_stdout(output): self.assertEqual(inspector.main(['--inspect-existing-audit']), 1)
        read.assert_called_once_with(enabled=True)
        result = json.loads(output.getvalue())
        self.assertEqual(result['reason'], 'audit_inspection_unavailable')
        self.assertIsNone(result['diagnostic_attempts'])
        self.assertFalse(result['execution_ready'])
        self.assertEqual(result['inspection_code'], 'bundle_anchor_replaced')

    def test_other_failure_never_starts_detail_reader(self):
        exc = inspector.InspectionFailure('audit_rows', 'guard', 'audit_history_incomplete')
        with patch.object(inspector, 'run_once', side_effect=exc), patch.object(evidence,
                'inspect_identity_evidence') as read, patch('builtins.print'):
            self.assertEqual(inspector.main(['--inspect-existing-audit']), 1)
        read.assert_not_called()


if __name__ == '__main__': unittest.main()
