"""Offline admission regressions. Runs against candidate, never live storage."""
import sqlite3
import unittest
from unittest.mock import patch

import test_native_account_guard as fixtures
from native_account_guard import AccountGuardRefused, MAX_REQUESTS, WINDOW_NS
from native_observation_collector import SharedRequestBudget


class CycleAdmissionTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            self.enterContext(patch(name, side_effect=AssertionError('no network during admission tests')))

    def fixture(self):
        f = fixtures.GuardTests('test_disabled_zero_state_and_no_implicit_files')
        self.addCleanup(f.doCleanups)
        f.setUp()
        return f

    @staticmethod
    def budget_rows(f):
        with sqlite3.connect(f.state) as db:
            return (db.execute('SELECT high_ns FROM account_guard_binding').fetchall(),
                    db.execute('SELECT at_ns FROM account_guard_attempts').fetchall())

    def test_existing_hold_required_without_creating_state(self):
        f = self.fixture()
        for method in (f.guard.request_budget_remaining, f.guard.admit_cycle):
            with self.assertRaisesRegex(AccountGuardRefused, '^account_guard_not_held$'):
                method(now_ns=fixtures.NOW)
        self.assertFalse(f.state.exists())

    def test_empty_budget_admission_is_readonly_not_prepaid(self):
        f = self.fixture()
        with f.guard.hold(now_ns=fixtures.NOW):
            before = f.state.read_bytes()
            self.assertEqual(f.guard.request_budget_remaining(now_ns=fixtures.NOW), 12)
            self.assertTrue(f.guard.admit_cycle(now_ns=fixtures.NOW))
            self.assertTrue(f.guard.admit_cycle(now_ns=fixtures.NOW + 100))
            self.assertEqual(f.state.read_bytes(), before)
            self.assertEqual(self.budget_rows(f), ([(0,)], []))

    def test_one_attempt_waits_without_refund_and_survives_restart(self):
        f = self.fixture()
        with f.guard.hold(now_ns=fixtures.NOW):
            f.guard.reserve_request(fixtures.NOW)  # Failed I/O would retain this exact attempt.
            before = f.state.read_bytes()
            self.assertEqual(f.guard.request_budget_remaining(now_ns=fixtures.NOW), 11)
            with self.assertRaisesRegex(AccountGuardRefused, '^cycle_budget_wait$'):
                f.guard.admit_cycle(now_ns=fixtures.NOW)
            self.assertEqual(f.state.read_bytes(), before)
        g = f.make_guard()
        with g.hold(now_ns=fixtures.NOW + 1):
            self.assertEqual(g.request_budget_remaining(now_ns=fixtures.NOW + 1), 11)
            with self.assertRaisesRegex(AccountGuardRefused, '^cycle_budget_wait$'):
                g.admit_cycle(now_ns=fixtures.NOW + 1)
        self.assertEqual(self.budget_rows(f), ([(fixtures.NOW,)], [(fixtures.NOW,)]))

    def test_admission_does_not_change_per_request_actual_timestamps_or_cap(self):
        f = self.fixture()
        with f.guard.hold(now_ns=fixtures.NOW):
            self.assertTrue(f.guard.admit_cycle(now_ns=fixtures.NOW))
            for i in range(12):
                self.assertEqual(f.guard.reserve_request(fixtures.NOW + i), i + 1)
            with self.assertRaisesRegex(AccountGuardRefused, '^shared_request_budget_exhausted$'):
                f.guard.reserve_request(fixtures.NOW + 12)
            self.assertEqual(f.guard.request_budget_remaining(now_ns=fixtures.NOW + 12), 0)
        self.assertEqual(self.budget_rows(f), ([(fixtures.NOW + 11,)],
                         [(fixtures.NOW + i,) for i in range(12)]))

    def test_exact_window_boundary_read_does_not_prune_or_advance_high_water(self):
        f = self.fixture()
        with f.guard.hold(now_ns=fixtures.NOW):
            f.guard.reserve_request(fixtures.NOW)
            before = f.state.read_bytes()
            self.assertEqual(f.guard.request_budget_remaining(now_ns=fixtures.NOW + WINDOW_NS - 1), 11)
            self.assertTrue(f.guard.admit_cycle(now_ns=fixtures.NOW + WINDOW_NS))
            self.assertEqual(f.state.read_bytes(), before)
            self.assertEqual(self.budget_rows(f), ([(fixtures.NOW,)], [(fixtures.NOW,)]))
            f.guard.reserve_request(fixtures.NOW + WINDOW_NS)
            self.assertEqual(self.budget_rows(f),
                             ([(fixtures.NOW + WINDOW_NS,)], [(fixtures.NOW + WINDOW_NS,)]))

    def test_stranded_newer_collector_attempt_requires_clean_wait(self):
        f = self.fixture()
        with f.guard.hold(now_ns=fixtures.NOW):
            b = SharedRequestBudget(f.state)
            f.guard.reserve_request(fixtures.NOW)
            b.reserve(fixtures.NOW + 1_000_000_000)  # Collector reserves before wire can refuse.
            before = f.state.read_bytes()
            now = fixtures.NOW + WINDOW_NS
            self.assertEqual(f.guard.request_budget_remaining(now_ns=now), 11)
            with self.assertRaisesRegex(AccountGuardRefused, '^cycle_budget_wait$'):
                f.guard.admit_cycle(now_ns=now)
            self.assertEqual(f.state.read_bytes(), before)
            self.assertTrue(f.guard.admit_cycle(now_ns=now + 1_000_000_000))
            self.assertEqual(f.state.read_bytes(), before)

    def test_remaining_is_minimum_of_both_existing_budgets(self):
        f = self.fixture()
        with f.guard.hold(now_ns=fixtures.NOW):
            b = SharedRequestBudget(f.state)
            for i in range(2):
                f.guard.reserve_request(fixtures.NOW + i)
            for i in range(4):
                b.reserve(fixtures.NOW + i)
            self.assertEqual(f.guard.request_budget_remaining(now_ns=fixtures.NOW + 4), 8)
            for i in range(2, 6):
                f.guard.reserve_request(fixtures.NOW + i)
            self.assertEqual(f.guard.request_budget_remaining(now_ns=fixtures.NOW + 6), 6)

    def test_clock_regression_rejected_for_either_high_water_mark(self):
        for budget in ('wire', 'collector'):
            with self.subTest(budget=budget):
                f = self.fixture()
                with f.guard.hold(now_ns=fixtures.NOW):
                    if budget == 'wire':
                        f.guard.reserve_request(fixtures.NOW)
                    else:
                        SharedRequestBudget(f.state).reserve(fixtures.NOW)
                    before = f.state.read_bytes()
                    with self.assertRaisesRegex(AccountGuardRefused, '^request_clock_regression$'):
                        f.guard.admit_cycle(now_ns=fixtures.NOW - 1)
                    self.assertEqual(f.state.read_bytes(), before)

    def test_native_stop_rechecked_before_capacity_query(self):
        f = self.fixture()
        with f.guard.hold(now_ns=fixtures.NOW):
            f.lifecycle.stop()
            with self.assertRaisesRegex(AccountGuardRefused, '^native_stopped_or_attention$'):
                f.guard.admit_cycle(now_ns=fixtures.NOW)
            self.assertEqual(self.budget_rows(f), ([(0,)], []))

    def test_exclusive_hold_prevents_another_guard_stealing_admitted_capacity(self):
        f = self.fixture()
        other = f.make_guard()
        with f.guard.hold(now_ns=fixtures.NOW):
            self.assertTrue(f.guard.admit_cycle(now_ns=fixtures.NOW))
            with self.assertRaisesRegex(AccountGuardRefused, '^legacy_worker_busy$'):
                with other.hold(now_ns=fixtures.NOW):
                    other.reserve_request(fixtures.NOW)
            self.assertEqual(f.guard.request_budget_remaining(now_ns=fixtures.NOW), 12)

    def test_collector_corruption_is_not_wait_or_repaired(self):
        mutations = {
            'missing_row': 'DELETE FROM native_read_budget',
            'bad_version': 'UPDATE native_read_budget SET version=2',
            'negative_high': 'UPDATE native_read_budget SET high_ns=-1',
            'text_high': "UPDATE native_read_budget SET high_ns='invalid'",
            'zero_attempt': 'INSERT INTO native_read_attempts VALUES(0)',
            'future_attempt': 'INSERT INTO native_read_attempts VALUES(1)',
            'schema_type': 'DROP TABLE native_read_attempts; CREATE TABLE native_read_attempts(at_ns TEXT NOT NULL)',
            'missing_table': 'DROP TABLE native_read_audit',
        }
        for fault, sql in mutations.items():
            with self.subTest(fault=fault):
                f = self.fixture()
                with f.guard.hold(now_ns=fixtures.NOW):
                    SharedRequestBudget(f.state)
                    with sqlite3.connect(f.state) as db:
                        db.executescript(sql)
                    before = f.state.read_bytes()
                    with self.assertRaisesRegex(AccountGuardRefused, '^guard_state_corrupt$'):
                        f.guard.admit_cycle(now_ns=fixtures.NOW)
                    self.assertEqual(f.state.read_bytes(), before)

    def test_overfull_or_invalid_wire_and_collector_attempts_refuse(self):
        for table in ('account_guard_attempts', 'native_read_attempts'):
            for fault in ('overfull', 'zero', 'future', 'text'):
                with self.subTest(table=table, fault=fault):
                    f = self.fixture()
                    with f.guard.hold(now_ns=fixtures.NOW):
                        SharedRequestBudget(f.state)
                        with sqlite3.connect(f.state) as db:
                            db.execute('UPDATE account_guard_binding SET high_ns=?', (fixtures.NOW,))
                            db.execute('UPDATE native_read_budget SET high_ns=?', (fixtures.NOW,))
                            vals = [fixtures.NOW] * 13 if fault == 'overfull' else [
                                {'zero': 0, 'future': fixtures.NOW + 1, 'text': 'invalid'}[fault]]
                            db.executemany('INSERT INTO ' + table + ' VALUES(?)', [(v,) for v in vals])
                        before = f.state.read_bytes()
                        with self.assertRaisesRegex(AccountGuardRefused, '^guard_state_corrupt$'):
                            f.guard.admit_cycle(now_ns=fixtures.NOW)
                        self.assertEqual(f.state.read_bytes(), before)


if __name__ == '__main__':
    unittest.main(verbosity=2)
