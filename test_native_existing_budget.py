"""Migrated full-cohort observer/quota regressions. No network or live files."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path
import shutil
import sqlite3
import unittest
from unittest.mock import patch

from alpaca_schema import timestamp
from decimal_lifecycle import Lifecycle, SessionRiskPolicy
from native_account_guard import SharedAccountGuard, AccountGuardRefused
from native_existing_budget import ExistingStateRequestBudget
from native_observation_collector import CollectionRefused, SharedRequestBudget
from native_paper_execution import NativeExecutionObserver, CoordinatedWire
import native_existing_budget as budget_module
import native_restart_migration as migration
import test_runtime_state_bundle as fixtures
import test_native_cohort_identity_evidence as identity_fixtures
from test_native_restart_identity import device_shift
from test_native_observation_worker import FP
from test_native_paper_execution import Wire


class ExistingBudgetTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.BundleTests()
        self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.f.enroll(); self.paths = self.f.paths
        self.now = datetime(2026, 10, 20, 13, 36, 1, tzinfo=timezone.utc)
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute("INSERT INTO native_read_audit VALUES(?,'native_schema_refused',6,0,2,NULL)",
                       (self.ns() - 100 * 10**9,))
            db.execute("INSERT INTO native_read_audit VALUES(?,'provider_read_failed',4,0,0,NULL)",
                       (self.ns() - 90 * 10**9,))
        identity_fixtures.IdentityEvidenceTests.old_identity_delta(self, device=71)
        backup = self.f.root / 'backup'; backup.mkdir(mode=0o700)
        plan = migration.prepare(paths=self.paths, test_only=True)
        migration.apply(enabled=True, plan=plan, approved_digest=plan.digest,
                        quiescence_confirmed=True, backup_directory=str(backup),
                        paths=self.paths, test_only=True)
        self.bundle = self.f.make()
        self.life = Lifecycle(self.paths['lifecycle'], FP, ('NVDA',),
                              session_risk_policy=SessionRiskPolicy(4, Decimal('100')))
        self.wire = Wire(lambda: self.now)
        self.guard = self.make_guard()
        self.observer = self.make_observer()

    def ns(self): return timestamp(self.now.isoformat())

    def make_guard(self):
        return SharedAccountGuard(self.paths['legacy'], self.paths['lifecycle'], FP,
            enabled=True, state_database=self.paths['guard'], test_only=True,
            runtime_bundle=self.bundle)

    def make_observer(self):
        return NativeExecutionObserver(self.life, self.guard, CoordinatedWire(self.wire, self.guard, self.ns),
            now_ns=self.ns, monotonic_ns=lambda: 100 * 10**9)

    def rows(self):
        with sqlite3.connect(self.paths['guard']) as db:
            return {name: db.execute('SELECT rowid,* FROM ' + name).fetchall() for name in
                    ('native_read_audit', 'native_read_attempts', 'account_guard_attempts')}

    @contextmanager
    def no_initialization(self):
        original_open, original_connect = os.open, sqlite3.connect
        statements = []
        def open_existing(path, flags, *args, **kwargs):
            self.assertFalse(flags & os.O_CREAT, 'no create during existing-state observation')
            return original_open(path, flags, *args, **kwargs)
        def connect(*args, **kwargs):
            db = original_connect(*args, **kwargs)
            db.set_trace_callback(statements.append)
            return db
        with patch.object(os, 'open', side_effect=open_existing), \
             patch.object(os, 'fchmod', side_effect=AssertionError('no fchmod')), \
             patch.object(os, 'chmod', side_effect=AssertionError('no chmod')), \
             patch.object(sqlite3, 'connect', side_effect=connect), \
             patch.object(SharedRequestBudget, '__init__', side_effect=AssertionError('no historical budget')):
            yield
        self.assertFalse(any(s.lstrip().upper().startswith(('CREATE ', 'ALTER ', 'DROP '))
                             for s in statements), statements)

    def test_real_migrated_guard_collector_parser_and_observer_no_create_or_chmod(self):
        before = self.rows()['native_read_audit']
        modes = {r: Path(p).stat().st_mode for r, p in self.paths.items()}
        with self.no_initialization(), self.guard.hold(now_ns=self.ns(), read_only=True):
            result = self.observer.collect()
        self.assertEqual(result.arguments()['orders'], [])  # Proven legacy terminal-flat history excluded.
        self.assertEqual(len(self.wire.calls), 9)  # Includes authenticated legacy reservation 404.
        rows = self.rows()
        self.assertEqual(rows['native_read_audit'][:2], before)
        self.assertEqual(len(rows['native_read_audit']), 3)
        self.assertEqual(rows['native_read_audit'][-1][2:4], ('observed_readonly', 8))
        self.assertEqual(len(rows['native_read_attempts']), 8)
        self.assertEqual(len(rows['account_guard_attempts']), 9)
        self.assertEqual(modes, {r: Path(p).stat().st_mode for r, p in self.paths.items()})
        self.assertTrue(self.bundle.verify())

    def test_same_algorithm_limits_clock_and_durable_no_refund(self):
        with self.guard.hold(now_ns=self.ns(), read_only=True):
            budget = ExistingStateRequestBudget(self.paths['guard'], self.guard)
            for _ in range(12): budget.reserve(self.ns())
            with self.assertRaisesRegex(CollectionRefused, '^shared_request_budget_exhausted$'):
                budget.reserve(self.ns())
            with self.assertRaisesRegex(CollectionRefused, '^budget_clock_regression$'):
                budget.reserve(self.ns() - 1)
            self.assertEqual(self.guard.request_budget_remaining(now_ns=self.ns()), 0)
            with self.assertRaisesRegex(AccountGuardRefused, '^cycle_budget_wait$'):
                self.guard.admit_cycle(now_ns=self.ns())
        self.assertEqual(len(self.rows()['native_read_attempts']), 12)
        with device_shift(42):
            self.bundle = self.f.make(); self.guard = self.make_guard()
            with self.guard.hold(now_ns=self.ns(), read_only=True):
                self.assertEqual(self.guard.request_budget_remaining(now_ns=self.ns()), 0)
                with self.assertRaisesRegex(CollectionRefused, '^shared_request_budget_exhausted$'):
                    ExistingStateRequestBudget(self.paths['guard'], self.guard).reserve(self.ns())

    def test_historical_audit_row_cannot_be_updated_but_current_row_can(self):
        before = self.rows()['native_read_audit']
        with self.guard.hold(now_ns=self.ns(), read_only=True):
            budget = ExistingStateRequestBudget(self.paths['guard'], self.guard)
            values = dict(at_ns=self.ns(), status='observed_readonly', attempts=8,
                          owned_count=0, order_count=2, digest='a' * 64)
            with self.assertRaisesRegex(CollectionRefused, '^budget_state_corrupt$'):
                budget.audit(rowid=before[0][0], **values)
            rowid = budget.audit(**values)
            budget.audit(rowid=rowid, **dict(values, status='collection_deadline_exceeded', digest=None))
            another = ExistingStateRequestBudget(self.paths['guard'], self.guard)
            with self.assertRaisesRegex(CollectionRefused, '^budget_state_corrupt$'):
                another.audit(rowid=rowid, **values)
        rows = self.rows()['native_read_audit']
        self.assertEqual(rows[:2], before)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[-1][2], 'collection_deadline_exceeded')

    def test_collector_late_deadline_updates_only_its_current_audit(self):
        before = self.rows()['native_read_audit']
        original = ExistingStateRequestBudget.audit
        def delayed(budget, **values):
            rowid = original(budget, **values)
            if values['status'] == 'observed_readonly':
                self.now += timedelta(seconds=11)
            return rowid
        with self.guard.hold(now_ns=self.ns(), read_only=True), \
             patch.object(ExistingStateRequestBudget, 'audit', delayed), \
             self.assertRaisesRegex(CollectionRefused, '^collection_deadline_exceeded$'):
            self.observer.collect()
        rows = self.rows()['native_read_audit']
        self.assertEqual(rows[:2], before)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[-1][2], 'collection_deadline_exceeded')
        self.assertIsNone(rows[-1][-1])

    def test_requires_exact_bundle_and_owned_hold(self):
        with self.assertRaises(CollectionRefused):
            ExistingStateRequestBudget(self.paths['guard'], self.guard)
        with self.guard.hold(now_ns=self.ns(), read_only=True):
            with self.assertRaises(CollectionRefused):
                ExistingStateRequestBudget(self.paths['runner'], self.guard)
        standalone = SharedAccountGuard(self.paths['legacy'], self.paths['lifecycle'], FP,
            enabled=True, state_database=self.paths['guard'], test_only=True)
        with self.assertRaises(AccountGuardRefused):
            with standalone.hold(now_ns=self.ns()): pass
        self.assertEqual(self.wire.calls, [])

    def test_missing_or_replaced_each_cohort_file_refuses_before_any_get(self):
        for role, path in self.paths.items():
            for replacement in (False, True):
                with self.subTest(role=role, replacement=replacement):
                    target = Path(path); saved = target.with_suffix('.saved')
                    target.rename(saved)
                    if replacement:
                        shutil.copyfile(saved, target); os.chmod(target, saved.stat().st_mode & 0o777)
                    try:
                        with self.no_initialization(), self.assertRaises(Exception):
                            with self.guard.hold(now_ns=self.ns(), read_only=True): self.observer.collect()
                        self.assertEqual(target.exists(), replacement)
                        self.assertEqual(self.wire.calls, [])
                    finally:
                        if replacement: target.unlink()
                        saved.rename(target)

    def test_schema_or_missing_budget_state_refuses_before_any_get(self):
        for sql in ('DROP TABLE native_read_audit', 'DELETE FROM native_read_budget',
                    'UPDATE native_read_budget SET version=2', 'PRAGMA user_version=0'):
            with self.subTest(sql=sql):
                path = Path(self.paths['guard']); original = path.read_bytes()
                with sqlite3.connect(path) as db: db.execute(sql)
                changed = path.read_bytes()
                try:
                    with self.no_initialization(), self.assertRaises(Exception):
                        with self.guard.hold(now_ns=self.ns(), read_only=True): self.observer.collect()
                    self.assertEqual(self.wire.calls, [])
                    self.assertEqual(path.read_bytes(), changed)
                finally: path.write_bytes(original)

    def test_mutation_after_adapter_construction_refuses_before_next_request(self):
        with self.guard.hold(now_ns=self.ns(), read_only=True):
            budget = ExistingStateRequestBudget(self.paths['guard'], self.guard)
            with sqlite3.connect(self.paths['source']) as db: db.execute('PRAGMA user_version=0')
            with self.assertRaises(CollectionRefused): budget.reserve(self.ns())
        self.assertEqual(self.wire.calls, [])
        self.assertEqual(self.rows()['native_read_attempts'], [])

    def test_http_schema_and_transport_failures_consume_without_retry_and_preserve_old_audits(self):
        for kind in ('http', 'schema', 'transport'):
            with self.subTest(kind=kind):
                # Clear neither quota nor history: advance into next minute.
                self.now += timedelta(seconds=65)
                before = self.rows()['native_read_audit']; original = self.wire.call
                def fail(method, path, body=None):
                    if path == '/v2/account':
                        self.wire.calls.append((method, path, body))
                        if kind == 'transport': raise TimeoutError('secret must not escape')
                        return (500, None) if kind == 'http' else (200, {})
                    return original(method, path, body)
                self.wire.call = fail
                start = len(self.wire.calls)
                with self.guard.hold(now_ns=self.ns(), read_only=True), self.assertRaises(CollectionRefused):
                    self.observer.collect()
                self.wire.call = original
                rows = self.rows()
                self.assertEqual(len(self.wire.calls) - start, 2)
                self.assertEqual(rows['native_read_audit'][:-1], before)
                self.assertEqual(len(rows['native_read_attempts']), 1)
                self.assertEqual(len(rows['account_guard_attempts']), 2)
                self.assertNotIn('secret', str(rows))

    @contextmanager
    def uncertain_commit(self, sql_prefix):
        original = sqlite3.connect
        failures = []
        class Connection:
            def __init__(self, db): self.db = db; self.hit = False
            def __getattr__(self, key): return getattr(self.db, key)
            def execute(self, sql, *args):
                if sql.startswith(sql_prefix): self.hit = True
                return self.db.execute(sql, *args)
            def __enter__(self): self.db.__enter__(); return self
            def __exit__(self, *args):
                result = self.db.__exit__(*args)
                if args[0] is None and self.hit and not failures:
                    failures.append(True)
                    raise sqlite3.OperationalError('uncertain commit fixture')
                return result
        def connect(*args, **kwargs):
            db = original(*args, **kwargs)
            # Only the adapter has this URI with default isolation level.
            if str(args[0]) == Path(self.paths['guard']).as_uri() + '?mode=rw' and 'isolation_level' not in kwargs:
                return Connection(db)
            return db
        with patch.object(budget_module.sqlite3, 'connect', side_effect=connect): yield failures

    def test_uncertain_quota_commit_never_opens_corresponding_request_or_retries(self):
        with self.guard.hold(now_ns=self.ns(), read_only=True):
            with self.uncertain_commit('INSERT INTO native_read_attempts') as failures:
                with self.assertRaises(CollectionRefused): self.observer.collect()
        rows = self.rows()
        self.assertEqual(failures, [True])
        self.assertEqual(len(self.wire.calls), 1)  # Only prior legacy-404 lookup.
        self.assertEqual(len(rows['native_read_attempts']), 1)
        self.assertEqual(len(rows['account_guard_attempts']), 1)
        self.assertEqual(len(rows['native_read_audit']), 2)  # Poisoned adapter cannot retry a write.

    def test_uncertain_audit_commit_is_not_repeated_or_reported_as_success(self):
        before = self.rows()['native_read_audit']
        with self.guard.hold(now_ns=self.ns(), read_only=True):
            with self.uncertain_commit('INSERT INTO native_read_audit') as failures:
                with self.assertRaises(CollectionRefused): self.observer.collect()
        rows = self.rows()
        self.assertEqual(failures, [True])
        self.assertEqual(len(self.wire.calls), 9)
        self.assertEqual(rows['native_read_audit'][:2], before)
        self.assertEqual(len(rows['native_read_audit']), 3)
        self.assertEqual(len(rows['native_read_attempts']), 8)
        self.assertEqual(len(rows['account_guard_attempts']), 9)


if __name__ == '__main__': unittest.main(verbosity=2)
