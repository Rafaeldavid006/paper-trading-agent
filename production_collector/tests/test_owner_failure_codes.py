"""Closed owner diagnostics: synthetic exceptions only; no live operations."""
from contextlib import ExitStack
import json
from pathlib import Path
import runpy
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import collector_owner as owner
import enrollment
import production_release as policy
import recurring

sys.path.insert(0, str(Path(owner.__file__).resolve().parent.parent))
from collector_supervisor import sanitized_status


class OwnerFailureCodesTests(unittest.TestCase):
    def setUp(self):
        prior = owner.OWNER_STAGE
        self.addCleanup(setattr, owner, 'OWNER_STAGE', prior)

    def test_all_closed_codes_survive_supervisor_sanitizer(self):
        for stage in owner.OWNER_STAGES:
            owner.OWNER_STAGE = stage
            for code in owner.OWNER_REFUSAL_CODES:
                reason = owner.failure_reason(ValueError(code))
                self.assertLessEqual(len(reason), 128)
                result = sanitized_status({'reason': reason, 'state': 'fenced',
                    'order_permission': 'none', 'active_collection': False,
                    'live_http_requests': None})
                self.assertEqual(result['reason'], reason)
                self.assertIsNone(result['live_http_requests'])

    def test_unknown_and_unhashable_values_never_leak(self):
        owner.OWNER_STAGE = 'storage_open'
        for error in (ValueError('/private/SECRET_SENTINEL'), ValueError(['SECRET_SENTINEL']),
                      ValueError({'SECRET_SENTINEL': 1}), ValueError('storage_reserve', 'SECRET_SENTINEL'),
                      RuntimeError('storage_reserve'), RuntimeError('SECRET_SENTINEL')):
            reason = owner.failure_reason(error)
            self.assertEqual(reason, 'owner_refused.storage_open.unexpected_failure')
            self.assertNotIn('SECRET_SENTINEL', reason)

    def test_valueerror_subclass_and_nonexact_string_are_not_trusted(self):
        class DifferentError(ValueError):
            pass
        class DifferentString(str):
            pass
        owner.OWNER_STAGE = 'enroll'
        for error in (DifferentError('storage_reserve'), ValueError(DifferentString('storage_reserve'))):
            self.assertEqual(owner.failure_reason(error), 'owner_refused.enroll.unexpected_failure')

    def test_oserror_emits_only_safe_generic_code(self):
        owner.OWNER_STAGE = 'enrollment_verify'
        error = OSError(13, 'SECRET_SENTINEL', '/private/SECRET_SENTINEL')
        self.assertEqual(owner.failure_reason(error), 'owner_refused.enrollment_verify.os_error')

    def test_unknown_stage_is_fixed_and_bounded(self):
        for stage in (['SECRET_SENTINEL'], {'SECRET_SENTINEL': 1}, 'SECRET_SENTINEL' * 100, None):
            owner.OWNER_STAGE = stage
            self.assertEqual(owner.failure_reason(ValueError('storage_reserve')),
                             'owner_refused.unknown_stage.storage_reserve')

    def test_main_error_frame_preserves_unknown_request_count(self):
        writes = []
        with patch.object(sys, 'argv', ['collector_owner.py']), \
             patch('os.set_blocking'), \
             patch('os.write', side_effect=lambda fd, raw: writes.append(raw) or len(raw)), \
             self.assertRaises(SystemExit) as raised:
            runpy.run_path(owner.__file__, run_name='__main__')
        self.assertEqual(raised.exception.code, 70)
        self.assertEqual(len(writes), 1)
        frame = json.loads(writes[0])
        self.assertEqual(frame['reason'], 'owner_refused.release.fixed_owner_arguments')
        self.assertEqual(frame['state'], 'fenced')
        self.assertFalse(frame['active_collection'])
        self.assertIsNone(frame['live_http_requests'])
        sanitized_status(frame)

    def harness(self, phase='collect'):
        stack = ExitStack()
        self.addCleanup(stack.close)
        release = SimpleNamespace(phase=phase, root='/synthetic/unused', collector_id='synthetic')
        stack.enter_context(patch.object(policy, 'require_release', return_value=release))
        stack.enter_context(patch.object(policy, 'require_collection_release', return_value=release))
        stack.enter_context(patch.object(policy, 'REQUIRED_PYTHON', sys.version_info[:3]))
        stack.enter_context(patch.object(policy, 'approved_leases', return_value=()))
        stack.enter_context(patch.object(owner, 'emit_status'))
        verify = stack.enter_context(patch.object(enrollment, 'verify_enrollment'))
        factory = stack.enter_context(patch.object(recurring, 'LeaseCollector'))
        collector = factory.return_value
        collector.db.execute.return_value = []
        return stack, release, verify, factory, collector

    def test_enrollment_stage_identifies_ensure_and_receipt_failure(self):
        stack, release, *_ = self.harness('enroll')
        ensure = stack.enter_context(patch.object(enrollment, 'ensure_enrolled',
                                              side_effect=ValueError('storage_reserve')))
        with self.assertRaises(ValueError):
            owner.run(release, threading.Event())
        self.assertEqual(owner.OWNER_STAGE, 'enroll')
        ensure.side_effect = None
        stack.enter_context(patch.object(enrollment, 'enrollment_receipt_hash',
                                      side_effect=ValueError('enrolled_manifest_changed')))
        with self.assertRaises(ValueError):
            owner.run(release, threading.Event())
        self.assertEqual(owner.OWNER_STAGE, 'enrollment_verify')

    def test_collection_stages_preserve_original_failure_after_cleanup(self):
        _, release, verify, factory, collector = self.harness()
        error = ValueError('database_integrity')
        verify.side_effect = error
        with self.assertRaises(ValueError): owner.run(release, threading.Event())
        self.assertEqual(owner.OWNER_STAGE, 'enrollment_verify')
        verify.side_effect = None
        factory.side_effect = error
        with self.assertRaises(ValueError): owner.run(release, threading.Event())
        self.assertEqual(owner.OWNER_STAGE, 'storage_open')
        factory.side_effect = None
        collector.db.execute.side_effect = error
        with self.assertRaises(ValueError): owner.run(release, threading.Event())
        self.assertEqual(owner.OWNER_STAGE, 'renewals')
        collector.db.execute.side_effect = None
        collector.tick.side_effect = error
        with self.assertRaises(ValueError): owner.run(release, threading.Event())
        self.assertEqual(owner.OWNER_STAGE, 'tick')
        self.assertTrue(collector.close.called)

    def test_close_failure_is_shutdown_stage(self):
        _, release, _, _, collector = self.harness()
        collector.close.side_effect = ValueError('storage_anchor_closed')
        stop = threading.Event(); stop.set()
        with self.assertRaises(ValueError): owner.run(release, stop)
        self.assertEqual(owner.OWNER_STAGE, 'shutdown')


if __name__ == '__main__':
    unittest.main()
