"""Offline account coordination tests. Every socket entry point is blocked."""
import copy
from dataclasses import FrozenInstanceError
import fcntl
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from alpaca_schema import timestamp
from decimal import Decimal
from decimal_lifecycle import Lifecycle, SessionRiskPolicy
from native_account_guard import (SharedAccountGuard, AccountGuardRefused,
                                  FIXED_GUARD_PATH, WINDOW_NS)
from test_native_observation_worker import create_source, FP, F, NOW
import native_observation_worker as legacy


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / 'legacy.sqlite'
        self.lifecycle_path = self.root / 'native.sqlite'
        self.state = self.root / 'guard.sqlite'
        create_source(self.source)
        with sqlite3.connect(self.source) as db:
            db.execute('CREATE TABLE loss_days (day TEXT PRIMARY KEY, stopped INTEGER NOT NULL)')
        self.lock = str(self.source) + '.experiment.lock'
        Path(self.lock).touch(mode=0o600)
        self.lifecycle = Lifecycle(self.lifecycle_path, FP,
                                   session_risk_policy=SessionRiskPolicy(4, Decimal('100')))
        os.chmod(self.lifecycle_path, 0o600)
        self.guard = self.make_guard()
        self.original = legacy._client_id(legacy.ORIGINAL_ID, 'buy')
        self.orders = []
        for side in ('buy', 'sell'):
            raw = copy.deepcopy(F['orders'][0])
            raw.update(id='broker-' + side, client_order_id=legacy._client_id(legacy.RENEWAL_ID, side),
                       side=side, qty='1', filled_qty='1', filled_avg_price='200', limit_price='200',
                       status='filled', created_at='2026-10-19T17:14:16Z',
                       submitted_at='2026-10-19T17:14:16Z', updated_at='2026-10-19T17:15:00Z',
                       filled_at='2026-10-19T17:15:00Z', canceled_at=None)
            self.orders.append(raw)
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            p = patch(name, side_effect=AssertionError('No network permitted'))
            p.start(); self.addCleanup(p.stop)

    def make_guard(self, **kw):
        args = dict(enabled=True, state_database=self.state, test_only=True)
        args.update(kw)
        return SharedAccountGuard(self.source, self.lifecycle_path, FP, **args)

    def sql(self, sql, params=()):
        with sqlite3.connect(self.source) as db:
            db.execute(sql, params)

    def reconcile(self, **kw):
        args = dict(native_orders=self.orders, missing_ids=(self.original,), now_ns=NOW, observed_ns=NOW)
        args.update(kw)
        return self.guard.reconcile_legacy(**args)

    def test_disabled_zero_state_and_no_implicit_files(self):
        g = self.make_guard(enabled=False)
        with self.assertRaisesRegex(AccountGuardRefused, 'account_guard_disabled'):
            with g.hold(now_ns=NOW):
                self.fail('disabled')
        self.assertFalse(self.state.exists())

    def test_import_and_constructor_have_no_database_or_network_side_effect(self):
        with patch('sqlite3.connect', side_effect=AssertionError('No DB constructor')):
            SharedAccountGuard(self.source, self.lifecycle_path, FP)
        self.assertFalse(Path(FIXED_GUARD_PATH).exists())

    def test_requires_hold_and_fixed_path_without_test_opt_in(self):
        for method in (lambda: self.guard.check(), lambda: self.guard.reserve_request(NOW)):
            with self.assertRaisesRegex(AccountGuardRefused, 'account_guard_not_held'):
                method()
        with self.assertRaisesRegex(AccountGuardRefused, 'fixed_guard_path_required'):
            SharedAccountGuard(self.source, self.lifecycle_path, FP, state_database=self.state)

    def test_source_byte_for_byte_unchanged_and_immutable_absence_receipt(self):
        before = self.source.read_bytes()
        with self.guard.hold(now_ns=NOW):
            self.assertEqual(len(self.guard.legacy_client_ids), 3)
            self.assertEqual(self.guard.reserved_client_ids, (self.original,))
            self.assertEqual(len(self.guard.submitted_client_ids), 2)
            receipt = self.reconcile()
            self.assertTrue(receipt.legacy_flat)
            self.assertFalse(receipt.account_atomic)
            self.assertEqual(receipt.absent_client_order_ids, (self.original,))
            with self.assertRaises(FrozenInstanceError):
                receipt.observed_ns = 4
            with self.assertRaises(sqlite3.OperationalError):
                self.guard._source.execute('UPDATE safety SET stopped=1')
        self.assertEqual(self.source.read_bytes(), before)
        self.assertEqual(self.state.stat().st_mode & 0o777, 0o600)
        with sqlite3.connect(self.state) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM legacy_absence_receipts').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM legacy_native_terminals').fetchone()[0], 2)

    def test_shared_flock_and_nonmutating_source_writer_lock(self):
        descriptor = os.open(self.lock, os.O_RDWR)
        self.addCleanup(os.close, descriptor)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with self.assertRaisesRegex(AccountGuardRefused, 'legacy_worker_busy'):
            with self.guard.hold(now_ns=NOW): pass
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        with self.guard.hold(now_ns=NOW):
            with self.assertRaisesRegex(AccountGuardRefused, 'legacy_worker_busy'):
                with self.make_guard().hold(now_ns=NOW): pass
            with sqlite3.connect(self.source, timeout=0) as db:
                with self.assertRaisesRegex(sqlite3.OperationalError, 'locked'):
                    db.execute('UPDATE safety SET stopped=1')
        self.sql('UPDATE safety SET stopped=1')
        with self.assertRaisesRegex(AccountGuardRefused, 'legacy_stopped'):
            with self.make_guard().hold(now_ns=NOW): pass

    def test_source_existing_writer_refuses_without_state(self):
        db = sqlite3.connect(self.source)
        self.addCleanup(db.close)
        db.execute('BEGIN IMMEDIATE')
        with self.assertRaisesRegex(AccountGuardRefused, 'legacy_writer_busy'):
            with self.guard.hold(now_ns=NOW): pass
        self.assertFalse(self.state.exists())
        db.rollback()
        with self.guard.hold(now_ns=NOW):
            self.assertTrue(self.guard.check())

    def test_budget_durable_across_restart_includes_failed_attempts_and_boundary(self):
        with self.guard.hold(now_ns=NOW):
            for i in range(12):
                self.assertEqual(self.guard.reserve_request(NOW), i + 1)
            with self.assertRaisesRegex(AccountGuardRefused, 'shared_request_budget_exhausted'):
                self.guard.reserve_request(NOW)
        g = self.make_guard()
        with g.hold(now_ns=NOW + WINDOW_NS - 1):
            with self.assertRaisesRegex(AccountGuardRefused, 'shared_request_budget_exhausted'):
                g.reserve_request(NOW + WINDOW_NS - 1)
            self.assertEqual(g.reserve_request(NOW + WINDOW_NS), 1)
            with self.assertRaisesRegex(AccountGuardRefused, 'request_clock_regression'):
                g.reserve_request(NOW)

    def test_new_lifecycle_file_cannot_reset_account_budget(self):
        with self.guard.hold(now_ns=NOW): self.guard.reserve_request(NOW)
        alternate = self.root / 'other-native.sqlite'
        Lifecycle(alternate, FP, session_risk_policy=SessionRiskPolicy(4, Decimal('100')))
        os.chmod(alternate, 0o600)
        g = SharedAccountGuard(self.source, alternate, FP, enabled=True,
                               state_database=self.state, test_only=True)
        with self.assertRaisesRegex(AccountGuardRefused, 'guard_binding_mismatch'):
            with g.hold(now_ns=NOW): pass

    def test_native_stop_attention_and_legacy_revision_fail_closed(self):
        with self.guard.hold(now_ns=NOW):
            self.lifecycle.stop()
            with self.assertRaisesRegex(AccountGuardRefused, 'native_stopped_or_attention'):
                self.guard.reserve_request(NOW)
        with sqlite3.connect(self.lifecycle_path) as db: db.execute('UPDATE control SET stopped=0')
        self.sql("UPDATE paper_renewal SET reason='changed'")
        with self.assertRaisesRegex(AccountGuardRefused, 'guard_binding_mismatch'):
            with self.make_guard().hold(now_ns=NOW): pass

    def test_legacy_loss_latch_and_buy_claims_include_unsubmitted_reservation(self):
        self.sql("INSERT INTO loss_days VALUES('2026-10-20',1)")
        with self.guard.hold(now_ns=NOW):
            with self.assertRaisesRegex(AccountGuardRefused, 'legacy_loss_entry_gate'):
                self.guard.legacy_entry_gate(now_ns=NOW)
            count = self.guard.legacy_buy_claims(session_open_ns=timestamp('2026-10-18T00:00:00Z'),
                                                session_close_ns=timestamp('2026-10-20T00:00:00Z'))
            self.assertEqual(count, 2)
            self.assertEqual(self.guard.legacy_buy_claims(session_open_ns=NOW - 1,
                                                         session_close_ns=NOW + 1), 0)
            self.assertTrue(self.reconcile().legacy_flat)  # Entry loss isn't a fake non-flat ledger.

    def test_absence_requires_exact_original_and_no_native_conflict(self):
        with self.guard.hold(now_ns=NOW):
            for missing in ((), self.guard.legacy_client_ids, ('unrelated',)):
                with self.assertRaisesRegex(AccountGuardRefused, 'legacy_missing_id_mismatch'):
                    self.reconcile(missing_ids=missing)
            orders = copy.deepcopy(self.orders)
            orders[0]['client_order_id'] = self.original
            with self.assertRaisesRegex(AccountGuardRefused, 'legacy_absence_conflict'):
                self.reconcile(native_orders=orders)

    def test_submitted_native_payload_fill_status_id_and_timestamps_match(self):
        changes = [('id', 'wrong'), ('qty', '2'), ('limit_price', '201'),
                   ('filled_qty', '0'), ('status', 'new'), ('side', 'sell'),
                   ('submitted_at', '2026-10-19T17:14:15Z')]
        with self.guard.hold(now_ns=NOW):
            for field, value in changes:
                with self.subTest(field=field):
                    orders = copy.deepcopy(self.orders)
                    orders[0][field] = value
                    with self.assertRaises(AccountGuardRefused):
                        self.reconcile(native_orders=orders)
            with self.assertRaisesRegex(AccountGuardRefused, 'legacy_submitted_order_missing'):
                self.reconcile(native_orders=self.orders[:1])
            with self.assertRaisesRegex(AccountGuardRefused, 'invalid_legacy_order_evidence'):
                self.reconcile(observed_ns=NOW - 11_000_000_000)

    def test_provider_extras_are_neither_stored_nor_terminal_identity(self):
        with self.guard.hold(now_ns=NOW):
            self.reconcile()
            orders = copy.deepcopy(self.orders)
            orders[0]['account_id'] = 'private-extra-must-not-be-stored'
            orders[0]['unexpected_private_balance'] = 'private-extra-value'
            orders[0]['qty'] = '1.000000000'
            self.assertTrue(self.reconcile(native_orders=orders).legacy_flat)
        contents = self.state.read_bytes()
        self.assertNotIn(b'private-extra', contents)

    def test_terminal_archive_pins_all_validated_provider_timestamps(self):
        changes = {
            'created_at': '2026-10-19T17:14:15Z',
            'submitted_at': '2026-10-19T17:14:17Z',
            'updated_at': '2026-10-19T17:15:01Z',
            'filled_at': '2026-10-19T17:14:59Z',
            'canceled_at': '2026-10-19T17:14:59Z',
            'expired_at': '2026-10-19T17:14:59Z',
            'failed_at': '2026-10-19T17:14:59Z',
        }
        with self.guard.hold(now_ns=NOW): self.reconcile()
        self.guard = self.make_guard()
        with self.guard.hold(now_ns=NOW):
            for field, value in changes.items():
                with self.subTest(field=field):
                    orders = copy.deepcopy(self.orders)
                    orders[0][field] = value
                    # Every mutation remains valid to the frozen native parser;
                    # rejection must be the immutable archive, not syntax.
                    from alpaca_schema import parse_order
                    parse_order(orders[0])
                    with self.assertRaisesRegex(AccountGuardRefused, 'legacy_terminal_changed'):
                        self.reconcile(native_orders=orders)

    def test_terminal_archive_timestamp_equivalent_forms_normalize_to_ns(self):
        with self.guard.hold(now_ns=NOW): self.reconcile()
        orders = copy.deepcopy(self.orders)
        for order in orders:
            for field in ('created_at', 'submitted_at', 'updated_at', 'filled_at'):
                order[field] = order[field].replace('T17:', 'T13:').replace('Z', '.000000000-04:00')
        self.guard = self.make_guard()
        with self.guard.hold(now_ns=NOW):
            self.assertTrue(self.reconcile(native_orders=orders).legacy_flat)

    def test_native_terminal_archive_is_immutable_across_restarts(self):
        with self.guard.hold(now_ns=NOW): self.reconcile()
        self.guard = self.make_guard()
        with self.guard.hold(now_ns=NOW):
            self.assertTrue(self.reconcile().legacy_flat)
            orders = copy.deepcopy(self.orders)
            orders[0]['updated_at'] = '2026-10-19T17:15:01Z'
            with self.assertRaisesRegex(AccountGuardRefused, 'legacy_terminal_changed'):
                self.reconcile(native_orders=orders)

    def test_missing_loss_ledger_and_nonfinal_renewal_fail_closed(self):
        self.sql('DROP TABLE loss_days')
        with self.assertRaisesRegex(AccountGuardRefused, 'legacy_risk_state_missing'):
            with self.guard.hold(now_ns=NOW): pass
        self.sql('CREATE TABLE loss_days(day TEXT PRIMARY KEY,stopped INTEGER NOT NULL)')
        self.sql("UPDATE paper_renewal SET phase='manual_attention'")
        with self.assertRaisesRegex(AccountGuardRefused, 'legacy_not_terminal_flat'):
            with self.guard.hold(now_ns=NOW): pass

    def test_symlink_permissions_hardlink_and_foreign_empty_state_rejected(self):
        os.chmod(self.lifecycle_path, 0o644)
        with self.assertRaisesRegex(AccountGuardRefused, 'unsafe_database_file'):
            with self.guard.hold(now_ns=NOW): pass
        os.chmod(self.lifecycle_path, 0o600)
        self.state.symlink_to(self.lifecycle_path)
        with self.assertRaisesRegex(AccountGuardRefused, 'unsafe_database_file'):
            with self.guard.hold(now_ns=NOW): pass
        self.state.unlink()
        os.link(self.lifecycle_path, self.state)
        with self.assertRaisesRegex(AccountGuardRefused, 'unsafe_database_file'):
            with self.guard.hold(now_ns=NOW): pass
        self.state.unlink()
        self.state.touch(mode=0o600)
        with self.assertRaisesRegex(AccountGuardRefused, 'guard_state_corrupt'):
            with self.guard.hold(now_ns=NOW): pass

    def test_same_instance_reentry_or_thread_cannot_corrupt_holder(self):
        with self.guard.hold(now_ns=NOW):
            with self.assertRaisesRegex(AccountGuardRefused, 'account_guard_already_held'):
                with self.guard.hold(now_ns=NOW): pass
            errors = []
            def run():
                try:
                    with self.guard.hold(now_ns=NOW): pass
                except AccountGuardRefused as exc:
                    errors.append(str(exc))
            thread = threading.Thread(target=run); thread.start(); thread.join()
            self.assertEqual(errors, ['account_guard_already_held'])
            self.assertTrue(self.guard.check())
            self.guard.reserve_request(NOW)

    def test_read_only_reconciliation_preserves_stops_attention_and_denies_writes(self):
        with self.guard.hold(now_ns=NOW): self.guard.reserve_request(NOW)
        self.sql('UPDATE paper_readonly SET stopped=1,revision=revision+1')
        self.sql('UPDATE safety SET stopped=1')
        with sqlite3.connect(self.lifecycle_path) as db:
            db.execute('UPDATE control SET stopped=1,attention=1')
        self.guard = self.make_guard()
        before = self.source.read_bytes()
        with self.guard.hold(now_ns=NOW, read_only=True):
            self.assertTrue(self.guard.check())
            self.guard.reserve_request(NOW)
            self.assertTrue(self.reconcile().legacy_flat)
            with self.assertRaisesRegex(AccountGuardRefused, 'read_only_guard_write_forbidden'):
                self.guard.assert_write_allowed()
        self.assertEqual(self.source.read_bytes(), before)
        self.assertEqual(self.lifecycle.status()['attention'], 1)
        with self.assertRaisesRegex(AccountGuardRefused, 'legacy_stopped'):
            with self.guard.hold(now_ns=NOW): pass

    def test_benign_readonly_refresh_preserves_execution_revision_and_budget(self):
        with self.guard.hold(now_ns=NOW):
            revision = self.guard.legacy_revision
            self.guard.reserve_request(NOW)
        self.sql("UPDATE paper_readonly SET checked_at=?,revision=revision+1,reason='updated',masked_identity='masked',snapshot='{}',ok=1", (NOW / 1_000_000_000,))
        with self.make_guard().hold(now_ns=NOW) as guard:
            self.assertEqual(guard.legacy_revision, revision)
            self.assertEqual(guard.reserve_request(NOW), 2)

    def test_read_only_does_not_relax_non_stop_revision_change(self):
        with self.guard.hold(now_ns=NOW): pass
        self.sql("UPDATE paper_renewal SET reason='changed'")
        with self.assertRaisesRegex(AccountGuardRefused, 'guard_binding_mismatch'):
            with self.make_guard().hold(now_ns=NOW, read_only=True): pass

    def test_native_read_check_does_not_deadlock_inside_dispatch_transaction(self):
        with self.guard.hold(now_ns=NOW):
            with self.lifecycle.tx():
                self.assertTrue(self.guard.assert_write_allowed())
                self.guard.reserve_request(NOW)

    def test_collector_budget_cohabits_without_resetting_combined_budget(self):
        from native_observation_collector import SharedRequestBudget
        with self.guard.hold(now_ns=NOW):
            self.guard.reserve_request(NOW)
            budget = SharedRequestBudget(self.state)
            budget.reserve(NOW)
            self.assertEqual(self.guard.reserve_request(NOW), 2)
        with self.make_guard().hold(now_ns=NOW) as guard:
            self.assertEqual(guard.reserve_request(NOW), 3)

    def test_body_errors_preserved_and_all_locks_released(self):
        with self.assertRaisesRegex(ValueError, 'caller_body_error'):
            with self.guard.hold(now_ns=NOW):
                raise ValueError('caller_body_error')
        with self.make_guard().hold(now_ns=NOW) as guard:
            self.assertTrue(guard.check())

    def test_guard_replacement_during_hold_denied(self):
        with self.guard.hold(now_ns=NOW):
            replacement = self.root / 'replacement'
            replacement.write_bytes(self.state.read_bytes()); replacement.chmod(0o600)
            os.replace(replacement, self.state)
            with self.assertRaisesRegex(AccountGuardRefused, 'guard_database_replaced'):
                self.guard.reserve_request(NOW)


if __name__ == '__main__':
    unittest.main()
