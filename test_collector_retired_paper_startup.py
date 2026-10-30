"""Actual grant selection through service startup; no source or production IO."""
import contextlib
import io
import threading
import unittest
from unittest.mock import Mock, patch

import prepared_execution_activation as execution_activation
import prepared_recovery_supervisor as prepared
import prepared_successor_activation as admission_activation
import serve


class RetiredPaperStartup(unittest.TestCase):
    def test_actual_authority_selection_is_inert(self):
        self.assertIsNone(admission_activation.APPROVED_ADMISSION)
        self.assertIsNone(execution_activation.APPROVED_EXECUTION)
        self.assertIsNone(prepared.selected())
        supervisor = prepared.PreparedRecoverySupervisor()
        self.assertFalse(supervisor.requested)
        with patch('subprocess.Popen', side_effect=AssertionError('no paper child')):
            supervisor.start()
            supervisor.close()
        self.assertTrue(supervisor.completed)
        self.assertTrue(supervisor.reaped)

    def exercise_real_selection(self, gate):
        # Do not mock PreparedRecoverySupervisor, selected(), or any grant slot.
        stop = threading.Event()
        collector = Mock(reaped=True)
        collector.report_if_due.return_value = None
        collector.status.return_value = {'order_permission': 'none'}
        web = Mock()
        web.poll.return_value = None
        waits = iter((False, True))
        calls = []
        def storage(**kwargs):
            calls.append('storage')
            return gate
        with contextlib.ExitStack() as stack:
            for name in ('socket.socket', 'socket.getaddrinfo', 'socket.create_connection'):
                stack.enter_context(patch(name, side_effect=AssertionError('no network')))
            stack.enter_context(patch.object(serve, 'validate_environment', return_value=('/tmp/synthetic', '8000')))
            stack.enter_context(patch.object(serve, 'run_startup_gate', side_effect=storage))
            readonly = stack.enter_context(patch.object(serve, '_run_readonly_report', return_value=True))
            stack.enter_context(patch.object(serve, 'CollectorSupervisor', return_value=collector))
            spawn = stack.enter_context(patch.object(serve.subprocess, 'Popen', return_value=web))
            stack.enter_context(patch.object(serve.threading, 'Event', return_value=stop))
            stack.enter_context(patch.object(stop, 'wait', side_effect=lambda _: next(waits)))
            stack.enter_context(patch.object(serve.signal, 'signal'))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
            code = serve.main()
        return code, calls, readonly, spawn, collector

    def test_unmocked_prepared_selection_reaches_existing_storage_gate(self):
        code, calls, readonly, spawn, collector = self.exercise_real_selection(False)
        self.assertEqual(code, 1)
        self.assertEqual(calls, ['storage'])
        spawn.assert_not_called()
        collector.start.assert_not_called()

    def test_unmocked_prepared_selection_preserves_readonly_report_and_web(self):
        code, calls, readonly, spawn, collector = self.exercise_real_selection(True)
        self.assertEqual(code, 0)
        self.assertEqual(calls, ['storage'])
        readonly.assert_called_once()
        spawn.assert_called_once()
        self.assertIn('inspection_wsgi:app', spawn.call_args.args[0])
        collector.start.assert_called_once()
        collector.close.assert_called_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
