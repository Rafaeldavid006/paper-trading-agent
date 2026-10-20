"""Fake processes/clocks/timers only; no real subprocess or provider access."""
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import readonly_runtime_supervisor as supervisor


class FakeChild:
    def __init__(self):
        self.code = None
        self.poll = Mock(side_effect=lambda: self.code)
        self.kill = Mock()
        self.wait = Mock()


class FakeTimer:
    def __init__(self, seconds, callback):
        self.seconds = seconds
        self.callback = callback
        self.daemon = False
        self.start = Mock()
        self.cancel = Mock()

    def fire(self):
        # Intentionally permits an already-canceled callback, exercising races.
        self.callback()


class ReadonlyRuntimeSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket',
                                side_effect=AssertionError('no network')))
        self.enterContext(patch.object(supervisor.subprocess, 'Popen',
                                      side_effect=AssertionError('no real child')))
        self.enterContext(patch.object(supervisor.threading, 'Timer',
                                      side_effect=AssertionError('no real timer')))
        self.now = 100.0
        self.children = []
        self.timers = []
        self.messages = []
        self.popen = Mock(side_effect=self.make_child)
        self.timer_factory = Mock(side_effect=self.make_timer)
        self.runtime = self.make_runtime(enabled=True)

    def make_child(self, *_args, **_kwargs):
        child = FakeChild()
        self.children.append(child)
        return child

    def make_timer(self, seconds, callback):
        timer = FakeTimer(seconds, callback)
        self.timers.append(timer)
        return timer

    def make_runtime(self, **kwargs):
        return supervisor.ReadonlyRuntimeSupervisor(
            popen=self.popen, timer_factory=self.timer_factory,
            monotonic=lambda: self.now,
            emit=lambda value, **_kw: self.messages.append(json.loads(value)),
            **kwargs)

    def initialize(self):
        self.runtime.start()
        self.children[-1].code = 0
        self.runtime.poll()
        self.assertTrue(self.runtime.initialized)
        self.assertEqual(self.runtime.active_stage, 'source')

    def finish_stage(self, *, code=0):
        self.children[-1].code = code
        self.runtime.poll()

    def reasons(self):
        return [message['reason'] for message in self.messages]

    def test_default_disabled_never_starts(self):
        runtime = self.make_runtime()
        runtime.start()
        runtime.poll()
        runtime.close()
        self.popen.assert_not_called()
        self.timer_factory.assert_not_called()
        self.assertFalse(runtime.halted)

    def test_enable_requires_exact_boolean(self):
        for value in ('true', 1, 0, None, [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.make_runtime(enabled=value)

    def test_import_and_construction_do_not_read_environment_or_keys(self):
        with patch('os.getenv', side_effect=AssertionError('no environment reads')), \
             patch('os._Environ.__getitem__',
                   side_effect=AssertionError('no key reads')):
            runtime = self.make_runtime(enabled=False)
            runtime.start()
            runtime.poll()
            runtime.close()
        self.popen.assert_not_called()

    def test_commands_are_fixed_and_environment_is_inherited(self):
        self.initialize()
        self.finish_stage()
        self.assertEqual(self.runtime.active_stage, 'broker')
        calls = self.popen.call_args_list
        self.assertEqual([call.args[0][2] for call in calls],
                         ['--initialize', '--source', '--broker'])
        expected_path = str(Path(supervisor.__file__).resolve()
                            .with_name('readonly_runtime_worker.py'))
        for call in calls:
            self.assertEqual(call.args[0][:2],
                             [supervisor.sys.executable, expected_path])
            self.assertEqual(call.kwargs, {
                'stdin': supervisor.subprocess.DEVNULL,
                'stdout': None,
                'stderr': supervisor.subprocess.DEVNULL,
                'shell': False,
                'close_fds': True,
            })
            self.assertNotIn('env', call.kwargs)
            self.assertNotIn('cwd', call.kwargs)

    def test_start_is_idempotent_poll_does_not_implicitly_start(self):
        self.runtime.poll()
        self.popen.assert_not_called()
        self.runtime.start()
        self.runtime.start()
        self.runtime.poll()
        self.popen.assert_called_once()
        self.assertEqual(self.runtime.active_stage, 'initialize')
        self.assertFalse(self.runtime.initialized)

    def test_initialization_must_finish_before_network_stages(self):
        self.runtime.start()
        for delta in (1, 5, 14.9):
            self.now = 100.0 + delta
            self.runtime.poll()
        self.assertEqual(self.popen.call_count, 1)
        self.children[0].code = 0
        self.runtime.poll()
        self.assertEqual(self.popen.call_count, 2)
        self.assertEqual(self.runtime.active_stage, 'source')

    def test_initialization_failure_never_starts_source_or_broker(self):
        self.runtime.start()
        self.finish_stage(code=2)
        for _ in range(5):
            self.now += 1000
            self.runtime.start()
            self.runtime.poll()
        self.assertTrue(self.runtime.halted)
        self.assertFalse(self.runtime.initialized)
        self.assertEqual(self.runtime.halt_reason, 'process_failed')
        self.popen.assert_called_once()

    def test_stages_are_serialized_even_when_other_stage_is_overdue(self):
        self.initialize()
        source = self.children[-1]
        self.now = 124.9
        self.runtime.poll()
        self.assertEqual(self.popen.call_count, 2)
        source.code = 0
        self.runtime.poll()
        self.assertEqual(self.runtime.active_stage, 'broker')
        self.assertEqual(self.popen.call_count, 3)
        self.now = 139.8
        self.runtime.poll()
        self.assertEqual(self.popen.call_count, 3)
        self.children[-1].code = 0
        self.runtime.poll()
        self.assertEqual(self.runtime.active_stage, 'source')
        self.assertEqual(self.popen.call_count, 4)

    def test_source_twenty_second_cadence_and_broker_sixty_five_minimum(self):
        self.initialize()
        self.finish_stage()  # Source -> initial broker, both at t=100.
        self.finish_stage()  # Broker completes, next source t=120, broker t=165.
        self.assertIsNone(self.runtime.active_stage)
        for now in (119.999, 139.999, 159.999):
            self.now = now
            self.runtime.poll()
            self.assertIsNone(self.runtime.active_stage)
            self.now += .001
            self.runtime.poll()
            self.assertEqual(self.runtime.active_stage, 'source')
            self.finish_stage()
        self.now = 164.999
        self.runtime.poll()
        self.assertIsNone(self.runtime.active_stage)
        self.now = 165
        self.runtime.poll()
        self.assertEqual(self.runtime.active_stage, 'broker')
        broker_calls = [call for call in self.popen.call_args_list
                        if call.args[0][-1] == '--broker']
        self.assertEqual(len(broker_calls), 2)

    def test_missed_ticks_are_not_replayed_and_oldest_due_stage_wins(self):
        self.initialize()
        self.finish_stage()
        self.finish_stage()
        self.now = 500.0
        self.runtime.poll()
        self.assertEqual(self.runtime.active_stage, 'source')
        self.finish_stage()
        self.assertEqual(self.runtime.active_stage, 'broker')
        self.finish_stage()
        self.assertIsNone(self.runtime.active_stage)
        self.runtime.poll()
        self.assertEqual(self.popen.call_count, 5)
        self.now = 519.999
        self.runtime.poll()
        self.assertEqual(self.popen.call_count, 5)
        self.now = 520
        self.runtime.poll()
        self.assertEqual(self.runtime.active_stage, 'source')

    def test_deadline_intervals_are_fixed_and_timers_daemonized(self):
        self.initialize()
        self.finish_stage()
        self.assertEqual([timer.seconds for timer in self.timers],
                         [15.0, 25.0, 15.0])
        for timer in self.timers:
            self.assertTrue(timer.daemon)
            timer.start.assert_called_once_with()

    def test_watchdog_is_independent_of_poll_and_never_waits(self):
        self.initialize()
        child = self.children[-1]
        self.timers[-1].fire()
        self.assertTrue(self.runtime.halted)
        self.assertEqual(self.runtime.halt_reason, 'watchdog_expired')
        child.kill.assert_called_once_with()
        child.wait.assert_not_called()
        self.assertEqual(self.popen.call_count, 2)
        self.now = 1000
        self.runtime.poll()
        self.runtime.start()
        self.assertEqual(self.popen.call_count, 2)
        self.assertEqual(self.reasons().count('watchdog_expired'), 1)

    def test_poll_deadline_fallback_for_each_stage(self):
        for stage, deadline in (('initialize', 15), ('source', 25), ('broker', 15)):
            with self.subTest(stage=stage):
                self.setUp_runtime()
                self.runtime.start()
                if stage != 'initialize':
                    self.finish_stage()
                if stage == 'broker':
                    self.finish_stage()
                child = self.children[-1]
                self.now += deadline - .001
                self.runtime.poll()
                child.kill.assert_not_called()
                self.now += .001
                self.runtime.poll()
                child.kill.assert_called_once()
                self.assertTrue(self.runtime.halted)
                self.assertEqual(self.runtime.halt_reason, 'watchdog_expired')

    def setUp_runtime(self):
        self.now = 100.0
        self.children = []
        self.timers = []
        self.messages = []
        self.popen.reset_mock()
        self.timer_factory.reset_mock()
        self.runtime = self.make_runtime(enabled=True)

    def test_canceled_timer_cannot_kill_successor(self):
        self.initialize()
        initialize_timer = self.timers[0]
        source_timer = self.timers[1]
        self.finish_stage()
        broker = self.children[-1]
        initialize_timer.fire()
        source_timer.fire()
        broker.kill.assert_not_called()
        self.assertFalse(self.runtime.halted)
        self.assertEqual(self.runtime.active_stage, 'broker')

    def test_timer_can_observe_success_without_starting_another_child(self):
        self.runtime.start()
        self.children[-1].code = 0
        self.timers[-1].fire()
        self.assertTrue(self.runtime.initialized)
        self.assertEqual(self.popen.call_count, 1)
        self.runtime.poll()
        self.assertEqual(self.runtime.active_stage, 'source')
        self.assertEqual(self.popen.call_count, 2)

    def test_broker_expected_closed_session_exit_zero_continues(self):
        self.initialize()
        self.finish_stage()
        self.finish_stage(code=0)
        self.assertFalse(self.runtime.halted)
        self.now = 120
        self.runtime.poll()
        self.assertEqual(self.runtime.active_stage, 'source')

    def test_source_and_broker_nonzero_or_signal_exits_latch_forever(self):
        for stage in ('source', 'broker'):
            for code in (1, 2, 127, -9, False, '0'):
                with self.subTest(stage=stage, code=code):
                    self.setUp_runtime()
                    self.initialize()
                    if stage == 'broker':
                        self.finish_stage()
                    count = self.popen.call_count
                    self.finish_stage(code=code)
                    self.assertTrue(self.runtime.halted)
                    self.assertEqual(self.runtime.halt_reason, 'process_failed')
                    for _ in range(3):
                        self.now += 1000
                        self.runtime.start()
                        self.runtime.poll()
                    self.assertEqual(self.popen.call_count, count)
                    self.assertEqual(self.reasons().count('process_failed'), 1)

    def test_spawn_failure_is_redacted_and_never_retried(self):
        self.popen.side_effect = RuntimeError('private credential path secret')
        self.runtime.start()
        self.runtime.start()
        self.runtime.poll()
        self.runtime.close()
        self.popen.assert_called_once()
        self.assertEqual(self.runtime.halt_reason, 'start_failed')
        self.assertNotIn('private', json.dumps(self.messages))
        self.assertNotIn('credential', json.dumps(self.messages))
        self.assertEqual(self.reasons(), ['start_failed'])

    def test_timer_creation_or_start_failure_kills_and_halts(self):
        for failure in ('create', 'start'):
            with self.subTest(failure=failure):
                self.setUp_runtime()
                if failure == 'create':
                    self.timer_factory.side_effect = RuntimeError('secret')
                else:
                    def broken_timer(seconds, callback):
                        timer = self.make_timer(seconds, callback)
                        timer.start.side_effect = RuntimeError('secret')
                        return timer
                    self.timer_factory.side_effect = broken_timer
                self.runtime.start()
                self.assertTrue(self.runtime.halted)
                self.assertEqual(self.runtime.halt_reason, 'watchdog_setup_failed')
                self.children[-1].kill.assert_called_once()
                self.runtime.poll()
                self.runtime.start()
                self.assertEqual(self.popen.call_count, 1)
                self.assertNotIn('secret', json.dumps(self.messages))
                self.timer_factory.side_effect = self.make_timer

    def test_spawn_time_counts_against_deadline(self):
        def slow_fake_spawn(*args, **kwargs):
            child = self.make_child(*args, **kwargs)
            self.now += 4
            return child
        self.popen.side_effect = slow_fake_spawn
        self.runtime.start()
        self.assertEqual(self.timers[0].seconds, 11.0)

    def test_poll_exception_is_uncertain_and_not_retried(self):
        self.initialize()
        child = self.children[-1]
        child.poll.side_effect = RuntimeError('sensitive details')
        self.runtime.poll()
        self.runtime.poll()
        self.assertTrue(self.runtime.halted)
        self.assertEqual(self.runtime.halt_reason, 'poll_uncertain')
        child.kill.assert_called_once()
        self.assertEqual(self.popen.call_count, 2)
        self.assertEqual(self.reasons().count('poll_uncertain'), 1)
        self.assertNotIn('sensitive', json.dumps(self.messages))

    def test_timer_poll_exception_kills_without_waiting(self):
        self.runtime.start()
        child = self.children[-1]
        child.poll.side_effect = RuntimeError('secret')
        self.timers[-1].fire()
        self.assertTrue(self.runtime.halted)
        self.assertEqual(self.runtime.halt_reason, 'poll_uncertain')
        child.kill.assert_called_once()
        child.wait.assert_not_called()

    def test_kill_exception_keeps_halt_and_serialization(self):
        self.runtime.start()
        child = self.children[-1]
        child.kill.side_effect = RuntimeError('private pid')
        self.timers[-1].fire()
        self.runtime.poll()
        self.runtime.start()
        self.assertTrue(self.runtime.halted)
        self.popen.assert_called_once()
        self.assertIn('kill_uncertain', self.reasons())
        self.assertNotIn('private', json.dumps(self.messages))

    def test_reap_after_timeout_never_allows_respawn(self):
        self.initialize()
        self.timers[-1].fire()
        self.children[-1].code = -9
        self.runtime.poll()
        self.assertIsNone(self.runtime.active_stage)
        self.now = 1000
        self.runtime.poll()
        self.runtime.start()
        self.assertTrue(self.runtime.halted)
        self.assertEqual(self.popen.call_count, 2)

    def test_shutdown_kills_and_bounded_reaps_once(self):
        self.runtime.start()
        child = self.children[-1]
        self.runtime.close()
        self.runtime.close()
        self.runtime.poll()
        self.runtime.start()
        child.kill.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=1.0)
        self.timers[-1].cancel.assert_called_once()
        self.popen.assert_called_once()
        self.assertEqual(self.runtime.halt_reason, 'shutdown_interrupted')

    def test_shutdown_timeout_or_wait_error_is_redacted(self):
        for error, reason in (
                (supervisor.subprocess.TimeoutExpired('secret', 1), 'reap_pending'),
                (RuntimeError('private path'), 'reap_uncertain')):
            with self.subTest(reason=reason):
                self.setUp_runtime()
                self.runtime.start()
                child = self.children[-1]
                child.wait.side_effect = error
                self.runtime.close()
                child.wait.assert_called_once_with(timeout=1.0)
                self.assertIn(reason, self.reasons())
                self.assertNotIn('secret', json.dumps(self.messages))
                self.assertNotIn('private', json.dumps(self.messages))

    def test_close_after_watchdog_does_not_duplicate_kill(self):
        self.runtime.start()
        child = self.children[-1]
        self.timers[-1].fire()
        self.runtime.close()
        child.kill.assert_called_once()
        child.wait.assert_called_once_with(timeout=1.0)
        self.assertEqual(self.runtime.halt_reason, 'watchdog_expired')

    def test_idle_shutdown_does_not_start_or_kill_anything(self):
        self.initialize()
        self.finish_stage()
        self.finish_stage()
        self.runtime.close()
        self.now = 1000
        self.runtime.start()
        self.runtime.poll()
        self.assertEqual(self.popen.call_count, 3)
        for child in self.children:
            child.kill.assert_not_called()
        self.assertFalse(self.runtime.halted)

    def test_close_observes_already_failed_child_without_retrying(self):
        self.runtime.start()
        self.children[-1].code = -9
        self.runtime.close()
        self.assertTrue(self.runtime.halted)
        self.assertEqual(self.runtime.halt_reason, 'process_failed')
        self.children[-1].kill.assert_not_called()
        self.children[-1].wait.assert_not_called()
        self.popen.assert_called_once()

    def test_close_reaps_and_clears_active_stage(self):
        self.runtime.start()
        self.runtime.close()
        self.assertIsNone(self.runtime.active_stage)

    def test_invalid_or_regressed_clock_halts(self):
        for value in (float('nan'), float('inf'), 'secret', True, 99.0):
            with self.subTest(value=value):
                self.setUp_runtime()
                self.runtime.start()
                self.now = value
                self.runtime.poll()
                self.assertTrue(self.runtime.halted)
                self.assertEqual(self.runtime.halt_reason, 'clock_failed')
                self.children[-1].kill.assert_called_once()
                self.assertNotIn('secret', json.dumps(self.messages))

    def test_logging_exception_never_clears_halt_or_prevents_kill(self):
        self.runtime._emit = Mock(side_effect=RuntimeError('broken sink'))
        self.runtime.start()
        self.timers[-1].fire()
        self.runtime.poll()
        self.assertTrue(self.runtime.halted)
        self.children[-1].kill.assert_called_once()

    def test_logs_contain_only_fixed_sanitized_fields(self):
        self.initialize()
        self.finish_stage()
        self.finish_stage(code=8)
        for message in self.messages:
            self.assertEqual(set(message),
                             {'mode', 'stage', 'reason', 'order_permission'})
            self.assertEqual(message['mode'], 'readonly_runtime_supervisor')
            self.assertEqual(message['order_permission'], 'none')
            self.assertIn(message['stage'], ('initialize', 'source', 'broker'))
            self.assertIn(message['reason'], ('stage_finished', 'process_failed'))
        self.assertNotIn('hash', json.dumps(self.messages))
        self.assertNotIn('exit_code', json.dumps(self.messages))


if __name__ == '__main__':
    unittest.main()
