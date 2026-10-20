"""Entirely local enrollment/inventory regressions; all network entrypoints fail."""
from decimal import Decimal
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from active_demo_pipeline import Config, DemoPipeline
from alpaca_schema import PAPER_ORIGIN
from decimal_lifecycle import Lifecycle, SessionRiskPolicy
from demo_tick_runner import RunnerLedger
from event_core import Store
from native_account_guard import SharedAccountGuard
from native_observation_collector import DATA_ORIGIN, SharedRequestBudget
from native_paper_execution import NativePaperDispatcher
from primary_earnings_source import PrimaryEarningsSource
from runtime_state_bundle import (FIXED_PATHS, GENERATION, NEW_ROLES,
                                  RuntimeStateBundle, RuntimeStateRefused)
from test_native_observation_worker import FP, NOW, create_source


class BundleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.paths = {role: str(self.root / Path(path).name) for role, path in FIXED_PATHS.items()}
        self.paths['legacy_lock'] = self.paths['legacy'] + '.experiment.lock'
        create_source(self.paths['legacy'])
        with sqlite3.connect(self.paths['legacy']) as db:
            db.execute('CREATE TABLE loss_days(day TEXT PRIMARY KEY,stopped INTEGER NOT NULL)')
        Path(self.paths['legacy_lock']).touch(mode=0o600)
        old = datetime(2026, 10, 19, 12, tzinfo=timezone.utc)
        store = Store(self.paths['anchor'], clock=lambda: old)
        self.history_id, _ = store.ingest(source='offline-fixture', source_id='historical-1',
                                         symbol='NVDA', published_at=old,
                                         payload={'fixture': 'historical anchor'},
                                         url='https://example.invalid/fixture')
        os.chmod(self.paths['anchor'], 0o644)  # Existing app file, never changed by bundle.
        self.old_bytes = Path(self.paths['legacy']).read_bytes()
        self.old_lock = Path(self.paths['legacy_lock']).read_bytes()
        self.callback = Mock(side_effect=self.initialize)
        self.bundle = self.make()
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            self.enterContext(patch(name, side_effect=AssertionError('No network permitted')))

    def make(self, **kwargs):
        return RuntimeStateBundle(kwargs.pop('fingerprint', FP), paths=self.paths,
                                  test_only=True, **kwargs)

    def initialize(self, paths):
        self.assertFalse(Path(paths['guard']).exists())
        for role in ('source', 'lifecycle', 'decision', 'runner'):
            self.assertEqual(Path(paths[role]).stat().st_mode & 0o777, 0o600)
        life = Lifecycle(paths['lifecycle'], FP, ('NVDA',),
                         session_risk_policy=SessionRiskPolicy(4, Decimal('100')))
        guard = SharedAccountGuard(paths['legacy'], paths['lifecycle'], FP, enabled=True,
                                   state_database=paths['guard'], test_only=True)
        with guard.hold(now_ns=NOW, read_only=True):
            pass
        SharedRequestBudget(paths['guard'])
        wire = Mock(paper_origin=PAPER_ORIGIN, data_origin=DATA_ORIGIN, feed='iex')
        wire.call.side_effect = AssertionError('No transport permitted')
        NativePaperDispatcher(life, guard, wire, enabled=False)
        with life.tx() as db:
            db.execute('CREATE TABLE native_strategy_sessions (close_ns INTEGER PRIMARY KEY,open_ns INTEGER NOT NULL)')
        PrimaryEarningsSource(paths['source'], transport=Mock(), now_ns=lambda: NOW)
        DemoPipeline(paths['decision'], Config(enabled=True, allowlist=('NVDA',)))
        RunnerLedger(paths['runner'])

    def enroll(self):
        self.assertTrue(self.bundle.initialize_once(initialize=self.callback))
        self.callback.assert_called_once()

    def execute(self, role, sql, params=()):
        with sqlite3.connect(self.paths[role]) as db:
            db.execute(sql, params)

    def test_constructor_has_no_file_or_sql_side_effect(self):
        before = set(self.root.iterdir())
        with patch('sqlite3.connect', side_effect=AssertionError('no SQL')):
            self.make()
        self.assertEqual(before, set(self.root.iterdir()))

    def test_initial_verify_never_creates_anything(self):
        before = {p: p.read_bytes() for p in self.root.iterdir()}
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_file_missing'):
            self.bundle.verify()
        self.assertEqual(before, {p: p.read_bytes() for p in self.root.iterdir()})

    def test_explicit_generation_and_anchor_cannot_be_bypassed(self):
        for reviewed, generation in ((False, GENERATION), (True, 'bad'), (True, GENERATION)):
            with self.assertRaises(RuntimeStateRefused):
                self.bundle.provision(reviewed=reviewed, generation=generation, initialize=self.callback)
        self.callback.assert_not_called()
        self.assertEqual(GENERATION, hashlib.sha256(b'native-readonly-runtime-20261020-g1').hexdigest())

    def test_first_enrollment_restart_never_reinitializes_or_changes_old_files(self):
        self.enroll()
        for role in (*NEW_ROLES, 'seal', 'manifest'):
            self.assertEqual(Path(self.paths[role]).stat().st_mode & 0o777, 0o600)
        self.assertEqual(Path(self.paths['anchor']).stat().st_mode & 0o777, 0o644)
        self.assertEqual(Path(self.paths['legacy']).read_bytes(), self.old_bytes)
        self.assertEqual(Path(self.paths['legacy_lock']).read_bytes(), self.old_lock)
        callback = Mock(side_effect=AssertionError('must not initialize twice'))
        self.assertTrue(self.make().initialize_once(initialize=callback))
        callback.assert_not_called()
        self.assertTrue(self.make().verify())

    def test_verification_is_readonly_for_entire_cohort(self):
        self.enroll()
        before = {p: p.read_bytes() for p in self.root.iterdir()}
        self.assertTrue(self.make().verify())
        self.assertEqual(before, {p: p.read_bytes() for p in self.root.iterdir()})

    def test_fixed_paths_and_complete_inventory_required(self):
        with self.assertRaisesRegex(RuntimeStateRefused, 'fixed_bundle_paths_required'):
            RuntimeStateBundle(FP, paths=self.paths)
        paths = dict(self.paths)
        paths.pop('runner')
        with self.assertRaisesRegex(RuntimeStateRefused, 'complete_inventory_required'):
            RuntimeStateBundle(FP, paths=paths, test_only=True)
        with self.assertRaises(TypeError):
            self.bundle.paths['runner'] = '/tmp/no'

    def test_production_anchor_uses_only_existing_event_db_configuration(self):
        with patch.dict(os.environ, {'EVENT_DB': '/var/data/already-configured.sqlite'}), \
             patch('runtime_state_bundle._private', return_value=(7, 8)) as check:
            production = RuntimeStateBundle(FP)
            self.assertEqual(production.paths['anchor'], '/var/data/already-configured.sqlite')
            check.assert_called_once_with('/var/data/already-configured.sqlite', public_read=True)
            for role in NEW_ROLES:
                self.assertEqual(production.paths[role], FIXED_PATHS[role])
            self.assertEqual(os.environ['EVENT_DB'], '/var/data/already-configured.sqlite')

    def test_production_anchor_has_no_default_or_external_path_override(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeStateRefused, 'existing_event_db_configuration_required'):
                RuntimeStateBundle(FP)
        for path in ('relative.db', '/tmp/events.sqlite', '/var/data', '/var/data/../events.db'):
            with self.subTest(path=path), patch.dict(os.environ, {'EVENT_DB': path}):
                with self.assertRaises(RuntimeStateRefused):
                    RuntimeStateBundle(FP)
        with patch.dict(os.environ, {'EVENT_DB': '/var/data/existing.sqlite'}), \
             patch('runtime_state_bundle._private', side_effect=FileNotFoundError):
            with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_anchor_unavailable'):
                RuntimeStateBundle(FP)

    def test_bad_account_refuses_before_claim_or_callback(self):
        wrong = RuntimeStateBundle('0' * 64, paths=self.paths, test_only=True)
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_account_mismatch'):
            wrong.initialize_once(initialize=self.callback)
        self.callback.assert_not_called()
        with sqlite3.connect(self.paths['anchor']) as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='native_runtime_enrollment'").fetchone())

    def test_missing_anchor_never_recreated(self):
        Path(self.paths['anchor']).unlink()
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_file_missing'):
            self.bundle.initialize_once(initialize=self.callback)
        self.callback.assert_not_called()
        self.assertFalse(Path(self.paths['anchor']).exists())

    def test_anchor_schema_required_and_existing_rows_untouched(self):
        self.execute('anchor', "INSERT INTO health VALUES('fixture','a','b','ok')")
        self.enroll()
        with sqlite3.connect(self.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT * FROM health').fetchall(), [('fixture', 'a', 'b', 'ok')])
        self.execute('anchor', 'DROP TABLE expectations')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_anchor_schema_invalid'):
            self.make().verify()

    def test_empty_recent_naive_and_invalid_history_cannot_enroll(self):
        for timestamp in ('2026-10-20T07:02:56+00:00', '2026-10-20T12:00:00Z',
                          '2026-10-19T12:00:00', 'bad', '2026-10-19T10:00:00-23:59'):
            with self.subTest(timestamp=timestamp):
                self.execute('anchor', 'UPDATE events SET received_at=?', (timestamp,))
                with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_existing_history_required'):
                    self.bundle.initialize_once(initialize=self.callback)
        self.execute('anchor', 'DELETE FROM events')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_existing_history_required'):
            self.bundle.initialize_once(initialize=self.callback)
        self.callback.assert_not_called()
        self.assertFalse(Path(self.paths['seal']).exists())

    def test_history_pinned_and_immutable_across_restart(self):
        self.enroll()
        self.execute('anchor', "UPDATE events SET payload='changed'")
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_anchor_history_changed'):
            self.make().verify()

    def test_history_deletion_refuses(self):
        self.enroll()
        self.execute('anchor', 'DELETE FROM events')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_anchor_history_changed'):
            self.make().initialize_once(initialize=self.callback)
        self.callback.assert_called_once()

    def test_history_marker_contains_only_hashes_not_original_content(self):
        self.enroll()
        with sqlite3.connect(self.paths['anchor']) as db:
            row = db.execute('SELECT * FROM native_runtime_enrollment').fetchone()
        self.assertNotIn(FP, row)
        self.assertNotIn('historical anchor', str(row))
        self.assertIn(self.history_id, row)

    def test_zero_byte_anchor_never_reinitialized(self):
        path = Path(self.paths['anchor'])
        path.write_bytes(b'')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_database_corrupt'):
            self.bundle.initialize_once(initialize=self.callback)
        self.assertEqual(path.read_bytes(), b'')
        self.callback.assert_not_called()

    def test_missing_risk_history_with_claims_refuses(self):
        self.enroll()
        self.execute('lifecycle', "INSERT INTO intents VALUES('x','x','NVDA','buy','1','10',?,NULL)", (NOW,))
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_risk_state_corrupt'):
            self.make().verify()

    def test_orphan_dispatch_refuses(self):
        self.enroll()
        self.execute('lifecycle', "INSERT INTO native_dispatch VALUES('submit','missing',?,'uncertain')", (NOW,))
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_dispatch_state_corrupt'):
            self.make().verify()

    def test_partial_existing_cohort_does_not_claim_anchor(self):
        Path(self.paths['runner']).touch(mode=0o600)
        with self.assertRaisesRegex(RuntimeStateRefused, 'partially_provisioned'):
            self.bundle.initialize_once(initialize=self.callback)
        self.callback.assert_not_called()
        with sqlite3.connect(self.paths['anchor']) as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='native_runtime_enrollment'").fetchone())

    def test_failed_provision_latches_anchor_and_never_retries(self):
        def fail(paths):
            raise RuntimeError('secret internal detail')
        with self.assertRaisesRegex(RuntimeStateRefused, '^bundle_provision_failed$'):
            self.bundle.initialize_once(initialize=fail)
        self.assertTrue(Path(self.paths['seal']).exists())
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_anchor_provision_incomplete'):
            self.make().initialize_once(initialize=self.callback)
        self.callback.assert_not_called()

    def test_pending_anchor_survives_all_cohort_file_loss(self):
        with self.assertRaises(RuntimeStateRefused):
            self.bundle.initialize_once(initialize=lambda _: (_ for _ in ()).throw(ValueError()))
        for role in (*NEW_ROLES, 'seal', 'manifest'):
            Path(self.paths[role]).unlink(missing_ok=True)
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_anchor_provision_incomplete'):
            self.make().initialize_once(initialize=self.callback)
        self.callback.assert_not_called()

    def test_completed_anchor_survives_all_cohort_file_loss(self):
        self.enroll()
        for role in (*NEW_ROLES, 'seal', 'manifest'):
            Path(self.paths[role]).unlink()
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_file_missing'):
            self.make().initialize_once(initialize=self.callback)
        self.callback.assert_called_once()
        self.assertFalse(Path(self.paths['seal']).exists())

    def test_missing_each_file_fails_without_recreating(self):
        self.enroll()
        for role in self.paths:
            with self.subTest(role=role):
                path = Path(self.paths[role])
                saved = path.with_suffix('.saved')
                path.rename(saved)
                try:
                    with self.assertRaises(RuntimeStateRefused):
                        self.make().verify()
                    self.assertFalse(path.exists())
                finally:
                    saved.rename(path)

    def test_replaced_database_refuses_even_with_identical_bytes(self):
        self.enroll()
        for role in (*NEW_ROLES, 'legacy', 'anchor', 'seal', 'manifest', 'legacy_lock'):
            with self.subTest(role=role):
                path = Path(self.paths[role])
                backup = path.with_suffix('.original')
                path.rename(backup)
                shutil.copyfile(backup, path)
                os.chmod(path, backup.stat().st_mode & 0o777)
                try:
                    with self.assertRaises(RuntimeStateRefused):
                        self.make().verify()
                finally:
                    path.unlink()
                    backup.rename(path)

    def test_schema_loss_refuses_before_repairing_constructor(self):
        self.enroll()
        self.execute('runner', 'DROP TABLE runner_events')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_schema_incomplete'):
            self.make().initialize_once(initialize=self.callback)
        self.callback.assert_called_once()
        with sqlite3.connect(self.paths['runner']) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='runner_events'").fetchone())

    def test_missing_control_row_refuses_without_seed(self):
        self.enroll()
        self.execute('source', 'DELETE FROM primary_meta')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_state_uninitialized'):
            self.make().verify()
        with sqlite3.connect(self.paths['source']) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM primary_meta').fetchone()[0], 0)

    def test_sqlite_corruption_refuses(self):
        self.enroll()
        Path(self.paths['decision']).write_bytes(b'not a SQLite database')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_database_corrupt'):
            self.make().verify()

    def test_truncated_same_inode_recreated_database_header_refuses(self):
        self.enroll()
        path = Path(self.paths['runner'])
        identity = path.stat().st_ino
        path.write_bytes(b'')
        RunnerLedger(path)
        self.assertEqual(path.stat().st_ino, identity)
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_generation_mismatch'):
            self.make().verify()

    def test_generation_header_reset_refuses(self):
        self.enroll()
        self.execute('guard', 'PRAGMA user_version=0')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_generation_mismatch'):
            self.make().verify()

    def test_account_binding_changed_refuses(self):
        self.enroll()
        self.execute('lifecycle', "UPDATE control SET binding=?", ('0' * 64,))
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_account_mismatch'):
            self.make().verify()

    def test_stop_dirty_attention_latches_are_preserved_not_repaired(self):
        self.enroll()
        self.execute('runner', "UPDATE runner_control SET dirty=1,stopped=1,attention='operator'")
        self.execute('lifecycle', 'UPDATE control SET stopped=1,attention=1')
        self.execute('decision', 'UPDATE demo_control SET stopped=1')
        self.assertTrue(self.make().verify())
        self.assertTrue(self.make().initialize_once(initialize=self.callback))
        with sqlite3.connect(self.paths['runner']) as db:
            self.assertEqual(db.execute('SELECT dirty,stopped,attention FROM runner_control').fetchone(), (1, 1, 'operator'))

    def test_invalid_control_boolean_refuses(self):
        self.enroll()
        self.execute('runner', 'UPDATE runner_control SET dirty=2')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_runner_state_corrupt'):
            self.make().verify()

    def test_enrollment_row_missing_or_pending_refuses(self):
        self.enroll()
        self.execute('anchor', 'DELETE FROM native_runtime_enrollment')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_state_uninitialized'):
            self.make().initialize_once(initialize=self.callback)
        self.callback.assert_called_once()

    def test_seal_and_manifest_content_changes_refuse(self):
        self.enroll()
        path = Path(self.paths['seal'])
        before = path.read_bytes()
        value = json.loads(before)
        value['generation'] = '0' * 64
        path.write_text(json.dumps(value))
        with self.assertRaises(RuntimeStateRefused):
            self.make().verify()
        path.write_bytes(before)
        self.execute('manifest', "UPDATE bundle_files SET schema_hash='changed' WHERE role='runner'")
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_manifest_corrupt'):
            self.make().verify()

    def test_unexpected_schema_expansion_refuses(self):
        self.enroll()
        self.execute('source', 'CREATE TABLE unexpected(value)')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_schema_incomplete'):
            self.make().verify()

    def test_private_mode_never_silently_repaired(self):
        self.enroll()
        path = Path(self.paths['source'])
        os.chmod(path, 0o644)
        with self.assertRaisesRegex(RuntimeStateRefused, 'unsafe_bundle_file'):
            self.make().verify()
        self.assertEqual(path.stat().st_mode & 0o777, 0o644)

    def test_anchor_world_writable_refuses_without_chmod(self):
        os.chmod(self.paths['anchor'], 0o666)
        with self.assertRaisesRegex(RuntimeStateRefused, 'unsafe_bundle_file'):
            self.bundle.initialize_once(initialize=self.callback)
        self.assertEqual(Path(self.paths['anchor']).stat().st_mode & 0o777, 0o666)
        self.callback.assert_not_called()

    def test_symlink_hardlink_and_sidecar_refuse(self):
        self.enroll()
        path = Path(self.paths['source'])
        backup = path.with_suffix('.original')
        path.rename(backup)
        path.symlink_to(backup)
        with self.assertRaises(RuntimeStateRefused):
            self.make().verify()
        path.unlink()
        backup.rename(path)
        link = self.root / 'hardlink'
        os.link(path, link)
        with self.assertRaisesRegex(RuntimeStateRefused, 'unsafe_bundle_file'):
            self.make().verify()
        link.unlink()
        side = Path(str(path) + '-journal')
        side.symlink_to(self.paths['legacy'])
        with self.assertRaisesRegex(RuntimeStateRefused, 'unsafe_bundle_file'):
            self.make().verify()

    def test_symlink_parent_refuses(self):
        self.enroll()
        linked = self.root / 'alias'
        linked.symlink_to(self.root, target_is_directory=True)
        paths = {role: str(linked / Path(path).name) for role, path in self.paths.items()}
        with self.assertRaises(RuntimeStateRefused):
            RuntimeStateBundle(FP, paths=paths, test_only=True).verify()

    def test_preexisting_guard_refuses_without_touching(self):
        path = Path(self.paths['guard'])
        path.write_bytes(b'existing guard')
        os.chmod(path, 0o600)
        with self.assertRaises(RuntimeStateRefused):
            self.bundle.initialize_once(initialize=self.callback)
        self.callback.assert_not_called()
        self.assertEqual(path.read_bytes(), b'existing guard')

    def test_incomplete_callback_is_latched_not_repaired(self):
        def incomplete(paths):
            self.initialize(paths)
            with sqlite3.connect(paths['lifecycle']) as db:
                db.execute('DROP TABLE native_dispatch')
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_schema_incomplete'):
            self.bundle.initialize_once(initialize=incomplete)
        with self.assertRaisesRegex(RuntimeStateRefused, 'bundle_anchor_provision_incomplete'):
            self.make().initialize_once(initialize=self.callback)
        self.callback.assert_not_called()

    def test_constructor_and_verifier_do_not_use_chmod(self):
        with patch('os.chmod', side_effect=AssertionError('no chmod')), \
             patch('os.fchmod', side_effect=AssertionError('no fchmod')):
            self.make()
            with self.assertRaises(RuntimeStateRefused):
                self.bundle.verify()
        self.enroll()
        with patch('os.chmod', side_effect=AssertionError('no chmod')), \
             patch('os.fchmod', side_effect=AssertionError('no fchmod')):
            self.assertTrue(self.make().verify())


if __name__ == '__main__':
    unittest.main()
