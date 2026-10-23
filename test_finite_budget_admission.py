"""Real migrated recovery quota waits before any finite tick claim. Offline only."""
from datetime import timedelta
from pathlib import Path
import unittest

import test_migrated_pilot_end_to_end as migrated


class MigratedBudgetAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.case = migrated.MigratedPilotEndToEndTests()
        self.case.setUp(); self.addCleanup(self.case.doCleanups)
        self.assertEqual(self.case.recover()['reason'], 'incident_recovered')
        self.manifest, self.run = self.case.pilot()
        self.run('source')

    def test_immediate_recovery_tick_waits_without_io_claim_or_heartbeat_then_65s_admits(self):
        case = self.case
        before = {role: Path(path).read_bytes() for role, path in case.paths.items()}
        runner = case.rows('runner', 'runner_control')
        control = case.rows('anchor', 'finite_pilot_control')
        calls = len(case.h.wire.calls)
        result = self.run('tick')
        self.assertEqual((result['phase'], result['outcome']), ('tick', 'waiting_request_budget'))
        self.assertEqual((result['events'], result['exits']), (0, 0))
        self.assertEqual(len(case.h.wire.calls), calls)
        self.assertEqual(before, {role: Path(path).read_bytes() for role, path in case.paths.items()})
        self.assertEqual(case.rows('runner', 'runner_control'), runner)
        self.assertEqual(case.rows('anchor', 'finite_pilot_control'), control)
        for role, table in (('runner', 'runner_events'), ('lifecycle', 'intents'),
                            ('lifecycle', 'cancels'), ('lifecycle', 'native_dispatch')):
            self.assertEqual(case.rows(role, table), [])
        # The supervisor's existing next-tick cadence, not a new faster retry.
        case.h.now += timedelta(seconds=65)
        result = self.run('tick')
        self.assertEqual((result['phase'], result['outcome']), ('tick', 'waiting_regular_bar_window'), result)
        self.assertEqual(case.rows('anchor', 'finite_pilot_control')[0][1], 1)
        self.assertGreater(len(case.h.wire.calls), calls)
        self.assertTrue(all(method == 'GET' for method, _, _ in case.h.wire.calls))
        self.assertEqual(case.rows('runner', 'runner_control')[0][1], case.h.now.timestamp())
        self.assertEqual(case.history(), case.original)

    def test_budget_wait_does_not_extend_original_120_second_heartbeat(self):
        case = self.case
        runner = case.rows('runner', 'runner_control')
        calls = len(case.h.wire.calls)
        self.assertEqual(self.run('tick')['outcome'], 'waiting_request_budget')
        self.assertEqual(case.rows('runner', 'runner_control'), runner)
        case.h.now += timedelta(seconds=121)
        result = self.run('tick')
        self.assertEqual(result['phase'], 'manual_handoff', result)
        self.assertEqual(len(case.h.wire.calls), calls)
        after = case.rows('runner', 'runner_control')[0]
        self.assertEqual(after[1], runner[0][1])
        self.assertEqual(after[4], 'stale_heartbeat')
        self.assertEqual(case.history(), case.original)


if __name__ == '__main__': unittest.main()
