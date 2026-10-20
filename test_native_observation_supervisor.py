"""No sockets or real subprocesses in the one-shot read-only supervisor tests."""
import json
import unittest
from unittest.mock import Mock, patch

import native_observation_supervisor as supervisor
import serve


class NativeSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket', side_effect=AssertionError('no sockets')))
        self.child = Mock()
        self.child.poll.return_value = None
        self.popen = Mock(return_value=self.child)
        self.now = 100.0
        self.messages = []
        self.timer = Mock()
        self.timer_factory = Mock(return_value=self.timer)
        self.probe = supervisor.NativeProbeSupervisor(
            enabled=True, popen=self.popen, monotonic=lambda: self.now,
            emit=lambda msg, **kw: self.messages.append(json.loads(msg)),
            timer_factory=self.timer_factory)

    def test_disabled_default_never_starts(self):
        probe = supervisor.NativeProbeSupervisor(popen=self.popen)
        probe.start(); probe.poll(); probe.close()
        self.popen.assert_not_called()

    def test_command_is_fixed_and_start_never_repeats(self):
        self.probe.start(); self.probe.start()
        self.popen.assert_called_once()
        args = self.popen.call_args.args[0]
        self.assertEqual(args[0], supervisor.sys.executable)
        self.assertTrue(args[1].endswith('/native_observation_worker.py'))
        self.assertEqual(args[2:], ['--once'])
        self.assertEqual({}, self.popen.call_args.kwargs)

    def test_success_does_not_respawn_or_kill(self):
        self.probe.start()
        self.child.poll.return_value = 0
        self.probe.poll(); self.probe.poll(); self.probe.start(); self.probe.close()
        self.popen.assert_called_once()
        self.child.kill.assert_not_called()
        self.assertEqual(self.messages[0]['reason'], 'probe_finished')
        self.assertEqual(len(self.messages), 1)

    def test_failure_does_not_retry_and_does_not_emit_exit_details(self):
        self.probe.start()
        self.child.poll.return_value = 7
        self.probe.poll(); self.probe.poll()
        self.popen.assert_called_once()
        self.assertEqual(self.messages, [{'mode': 'native_readonly_probe_supervisor',
            'reason': 'probe_process_failed', 'order_permission': 'none'}])

    def test_watchdog_kills_at_fifteen_seconds_once(self):
        self.probe.start()
        self.now = 114.999
        self.probe.poll()
        self.child.kill.assert_not_called()
        self.now = 115
        self.probe.poll(); self.probe.poll(); self.probe.start()
        self.child.kill.assert_called_once()
        self.child.wait.assert_not_called()
        self.popen.assert_called_once()
        self.assertEqual(self.messages[0]['reason'], 'probe_watchdog_expired')

    def test_shutdown_kills_pending_probe(self):
        self.probe.start(); self.probe.close()
        self.child.kill.assert_called_once()
        self.child.wait.assert_called_once_with(timeout=1)
        self.assertEqual(self.messages, [])

    def test_watchdog_is_independent_of_main_loop(self):
        self.probe.start()
        self.timer_factory.assert_called_once_with(15.0, self.probe._expire)
        self.timer.start.assert_called_once()
        self.assertTrue(self.timer.daemon)
        self.timer_factory.call_args.args[1]()
        self.child.kill.assert_called_once()
        self.child.wait.assert_not_called()
        self.assertEqual(self.messages[0]['reason'], 'probe_watchdog_expired')

    def test_shutdown_reap_wait_is_bounded(self):
        self.probe.start()
        self.child.wait.side_effect = supervisor.subprocess.TimeoutExpired('probe', 1)
        self.probe.close()
        self.child.wait.assert_called_once_with(timeout=1)
        self.assertEqual(self.messages[0]['reason'], 'probe_reap_pending')

    def test_spawn_failure_is_redacted_and_not_retried(self):
        self.popen.side_effect = RuntimeError('secret raw detail')
        self.probe.start(); self.probe.start(); self.probe.poll(); self.probe.close()
        self.popen.assert_called_once()
        self.assertNotIn('secret', json.dumps(self.messages))
        self.assertEqual(self.messages[0]['reason'], 'probe_start_failed')

    def test_bool_enable_required(self):
        with self.assertRaises(ValueError):
            supervisor.NativeProbeSupervisor(enabled='true')

    def test_serve_enables_watches_and_closes_separate_probe(self):
        child = Mock()
        child.poll.return_value = None
        stop = Mock()
        stop.wait.side_effect = [False, True]
        with patch.object(serve, 'validate_environment', return_value=('/tmp/events.db','8000')), \
             patch.dict(serve.os.environ, {'ENABLE_AUTO_RESEARCH':'false'}), \
             patch.object(serve.subprocess, 'Popen', return_value=child) as popen, \
             patch.object(serve.threading, 'Event', return_value=stop), \
             patch.object(serve.signal, 'signal'), \
             patch.object(serve, 'NativeProbeSupervisor') as factory:
            self.assertEqual(serve.main(), 0)
        factory.assert_called_once_with(enabled=True)
        factory.return_value.start.assert_called_once()
        factory.return_value.poll.assert_called_once()
        factory.return_value.close.assert_called_once()
        self.assertEqual(popen.call_count, 2)
        self.assertEqual(popen.call_args.args[0][-1], 'paper_renewal_worker.py')


if __name__ == '__main__':
    unittest.main()
