"""Offline private-ledger regressions; synthetic data and blocked sockets only."""
import copy
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import stat
import tempfile
import time
import unittest
from unittest.mock import patch

from alpaca_schema import PAPER_ORIGIN, SchemaError, parse_order, timestamp
from decimal_lifecycle import Lifecycle, LifecycleError, Proposal, SessionRiskPolicy
import lifecycle_storage as storage

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'alpaca-native.json').read_text())
FP = hashlib.sha256(FIXTURE['account']['id'].encode()).hexdigest()
BASE = timestamp('2026-10-19T13:31:01Z')
SECRET = 'UNVALIDATED-PROVIDER-EXTRA-NEVER-PERSIST'


class LifecyclePrivacyTests(unittest.TestCase):
    def setUp(self):
        self.sockets = patch.object(socket, 'socket', side_effect=AssertionError('network forbidden'))
        self.sockets.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.sockets.stop)
        self.path = Path(self.tmp.name) / 'ledger.sqlite'
        self.policy = SessionRiskPolicy(4, Decimal('100'))
        self.lifecycle = Lifecycle(self.path, FP, session_risk_policy=self.policy)

    def observation(self, seconds=0, *, orders=None, positions=None):
        f = copy.deepcopy(FIXTURE)
        stamp = f'2026-10-19T13:31:{seconds + 1:02d}Z'
        f['clock']['timestamp'] = stamp
        f['quote']['quote']['t'] = stamp
        return dict(account=f['account'], clock=f['clock'], positions=positions or [],
                    orders=orders or [], origin=PAPER_ORIGIN,
                    response_account_fingerprint=FP, observed_ns=BASE + seconds * 10**9,
                    collection_complete=True, quotes=f['quote'], quote_symbols=('AAPL',), feed='iex')

    def observe(self, seconds=0, **kwargs):
        raw = self.observation(seconds, **kwargs)
        self.lifecycle.observe(**raw, now_ns=BASE + seconds * 10**9)
        return raw

    def claim(self, cid='entry', now_ns=BASE):
        return self.lifecycle.claim(Proposal(cid, cid, 'AAPL', 'buy', Decimal('1'), Decimal('200.2')),
                                    now_ns=now_ns)

    def order(self, cid='entry', seconds=1, status='new', fill='0'):
        result = copy.deepcopy(FIXTURE['orders'][0])
        stamp = f'2026-10-19T13:31:{seconds + 1:02d}Z'
        result.update(id='provider-' + cid, client_order_id=cid, limit_price='200.2',
                      status=status, filled_qty=fill, filled_avg_price='200.2' if Decimal(fill) else None,
                      created_at='2026-10-19T13:31:01Z', submitted_at='2026-10-19T13:31:01Z',
                      updated_at=stamp, canceled_at=stamp if status == 'canceled' else None,
                      filled_at=stamp if status == 'filled' else None)
        return result

    def encoded(self):
        with self.lifecycle.tx() as db:
            return db.execute('SELECT snapshot FROM control').fetchone()[0]

    def assert_no_provider_leak(self):
        for path in Path(self.tmp.name).iterdir():
            if path.is_file() and not path.is_symlink():
                data = path.read_bytes()
                for sensitive in (SECRET, FIXTURE['account']['id'], FIXTURE['account']['account_number']):
                    self.assertNotIn(sensitive.encode(), data)
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_native_projection_exact_semantics_and_original_timestamps(self):
        raw = self.observe(1, orders=[self.order(status='canceled', fill='0.125000001')],
                           positions=copy.deepcopy(FIXTURE['positions']))
        expected = self.lifecycle._parse(raw, BASE + 10**9)
        persisted = self.lifecycle._load_snapshot(self.encoded(), BASE + 10**9)
        self.assertEqual(expected, persisted)
        self.assertEqual(persisted.positions[0].quantity, Decimal('0.125000001'))
        self.assertEqual(persisted.orders[0].submitted_ns, BASE)
        self.assertEqual(persisted.observed_ns, BASE + 10**9)
        self.assert_no_provider_leak()

    def test_account_order_quote_extras_never_enter_database_or_status(self):
        self.observe()
        self.claim()
        order = self.order(status='canceled', fill='0.125000001')
        raw = self.observation(1, orders=[order], positions=copy.deepcopy(FIXTURE['positions']))
        for item in (raw['account'], raw['clock'], raw['positions'][0], order,
                     raw['quotes'], raw['quotes']['quote']):
            item['provider_extra'] = {'secret': SECRET}
        raw['account']['account_number'] = SECRET
        order['account_id'] = FIXTURE['account']['id']
        self.lifecycle.observe(**raw, now_ns=BASE + 10**9)
        state = self.lifecycle.status()
        stored_order = json.loads(state['intents'][0]['response'])
        self.assertEqual(set(stored_order), storage.ORDER_FIELDS)
        self.assertEqual(parse_order(stored_order), parse_order(order))
        self.assertNotIn(SECRET, json.dumps(state))
        self.assert_no_provider_leak()
        # Journal sidecars created in the private transaction are also 0600.
        with self.lifecycle.tx() as db:
            db.execute('UPDATE control SET stopped=1')
            self.assert_no_provider_leak()

    def test_public_status_contains_no_identifiers_responses_or_account_amounts(self):
        self.observe()
        self.claim()
        self.lifecycle.record_response('entry', self.order(), now_ns=BASE + 10**9)
        public = self.lifecycle.public_status()
        self.assertEqual(public['intent_count'], 1)
        self.assertEqual(public['intents_without_response'], 0)
        self.assertNotIn('intents', public)
        for key in ('fingerprint', 'cash', 'equity', 'baseline_equity', '"response":', 'provider-entry', '"cid":'):
            self.assertNotIn(key, json.dumps(public))

    def test_native_input_cannot_spoof_stored_projection(self):
        self.observe()
        projected = json.loads(self.encoded())
        raw = self.observation(1)
        raw['account'] = projected['account']
        raw['account'].update(format='decimal_lifecycle_snapshot', version=1)
        with self.assertRaisesRegex(LifecycleError, 'invalid_identifier'):
            self.lifecycle.observe(**raw, now_ns=BASE + 10**9)
        self.assertTrue(self.lifecycle.public_status()['attention'])
        with self.assertRaises((SchemaError, TypeError, LifecycleError)):
            self.lifecycle._parse(projected, BASE)

    def test_ignored_provider_extras_do_not_create_conflicting_snapshot(self):
        raw = self.observe()
        raw['account']['anything'] = SECRET
        raw['quotes']['quote']['anything'] = SECRET
        self.lifecycle.observe(**raw, now_ns=BASE)
        self.assertFalse(self.lifecycle.public_status()['attention'])
        self.assert_no_provider_leak()

    def test_restart_does_not_refresh_observation_or_loss_budget(self):
        raw = self.observation()
        raw['account']['equity'] = '99900'
        self.lifecycle.observe(**raw, now_ns=BASE)
        before = self.lifecycle.status()
        self.lifecycle = Lifecycle(self.path, FP, session_risk_policy=self.policy)
        self.assertEqual(self.lifecycle.status(), before)
        self.assertEqual(before['observed'], BASE)
        self.assertEqual(before['risk_sessions'][0]['loss_latched'], 1)
        with self.assertRaisesRegex(SchemaError, 'stale_or_future'):
            self.claim(now_ns=BASE + 11 * 10**9)

    def test_future_quote_is_stored_with_original_time_and_cannot_claim_early(self):
        raw = self.observation()
        raw['quotes']['quote']['t'] = '2026-10-19T13:31:02Z'
        self.lifecycle.observe(**raw, now_ns=BASE)
        projected = json.loads(self.encoded())
        self.assertEqual(projected['quotes'][0]['timestamp_ns'], BASE + 10**9)
        with self.assertRaisesRegex(SchemaError, 'stale_or_future'):
            self.claim()
        self.assertFalse(self.claim(now_ns=BASE + 10**9).executable)

    def test_invalid_quote_is_not_persisted_and_never_authorizes_claim(self):
        raw = self.observation()
        raw['quotes']['quote']['bs'] = SECRET
        self.lifecycle.observe(**raw, now_ns=BASE)
        self.assertIsNone(json.loads(self.encoded())['quotes'])
        self.assert_no_provider_leak()
        with self.assertRaisesRegex(LifecycleError, 'quote_missing'):
            self.claim()

    def test_record_response_only_updates_validated_order_not_snapshot_or_risk(self):
        self.observe()
        self.claim()
        before = self.lifecycle.status()
        encoded = self.encoded()
        raw = self.order()
        raw['account_id'] = FIXTURE['account']['id']
        raw['raw_extra'] = SECRET
        parsed = self.lifecycle.record_response('entry', raw, now_ns=BASE + 10**9)
        after = self.lifecycle.status()
        self.assertEqual(parsed, parse_order(raw))
        self.assertEqual(self.encoded(), encoded)
        self.assertEqual(after['observed'], before['observed'])
        self.assertEqual(after['risk_sessions'], before['risk_sessions'])
        self.assert_no_provider_leak()
        with self.assertRaisesRegex(LifecycleError, 'unresolved_claim'):
            self.claim('other')

    def test_invalid_responses_latch_attention_and_preserve_prior(self):
        for mutation, code in (
            ({'client_order_id': 'other'}, 'intent_order_mismatch'),
            ({'qty': '2'}, 'intent_order_mismatch'),
            ({'limit_price': '201'}, 'intent_order_mismatch'),
            ({'submitted_at': '2026-10-19T13:30:59Z', 'created_at': '2026-10-19T13:30:59Z'}, 'order_predates_claim'),
            ({'updated_at': '2026-10-19T13:31:10Z'}, 'future_order'),
            ({'id': 'mutated'}, 'order_identity_mutated'),
        ):
            with self.subTest(mutation=mutation):
                path = Path(self.tmp.name) / ('case-' + str(len(list(Path(self.tmp.name).iterdir()))) + '.db')
                self.lifecycle = Lifecycle(path, FP, session_risk_policy=self.policy)
                self.observe()
                self.claim()
                good = self.order()
                self.lifecycle.record_response('entry', good, now_ns=BASE + 10**9)
                before = self.lifecycle.status()['intents'][0]['response']
                bad = dict(good, **mutation)
                with self.assertRaisesRegex(LifecycleError, code):
                    self.lifecycle.record_response('entry', bad, now_ns=BASE + 10**9)
                state = self.lifecycle.status()
                self.assertEqual(state['attention'], 1)
                self.assertEqual(state['intents'][0]['response'], before)

    def test_unowned_response_cannot_create_intent(self):
        self.observe()
        with self.assertRaisesRegex(LifecycleError, 'response_not_owned'):
            self.lifecycle.record_response('entry', self.order(), now_ns=BASE + 10**9)
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_record_response_reconciles_with_next_full_observation(self):
        self.observe()
        self.claim()
        self.lifecycle.record_response('entry', self.order(), now_ns=BASE + 10**9)
        terminal = self.order(seconds=2, status='canceled', fill='0.125000001')
        self.observe(2, orders=[terminal], positions=copy.deepcopy(FIXTURE['positions']))
        self.assertEqual(parse_order(json.loads(self.lifecycle.status()['intents'][0]['response'])), parse_order(terminal))
        self.assertFalse(self.lifecycle.status()['attention'])

    def test_legacy_raw_ledger_requires_explicit_review_without_rewriting(self):
        path = Path(self.tmp.name) / 'legacy.db'
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        os.close(fd)
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE control (id INTEGER PRIMARY KEY, snapshot TEXT)')
            db.execute('INSERT INTO control VALUES(1,?)', (json.dumps(self.observation()),))
        before = path.read_bytes()
        with self.assertRaisesRegex(LifecycleError, 'legacy_storage_requires_reviewed_migration'):
            Lifecycle(path, FP)
        self.assertEqual(path.read_bytes(), before)
        self.assertIn(FIXTURE['account']['id'].encode(), before)

    def test_strict_decoder_rejects_extra_duplicate_version_and_bad_values(self):
        self.observe()
        encoded = self.encoded()
        for mutate in (
            lambda x: x.update(account_id=SECRET),
            lambda x: x.update(version=True),
            lambda x: x.update(version=2),
            lambda x: x['account'].update(id=SECRET),
            lambda x: x['clock'].update(timestamp_ns=True),
            lambda x: x['account'].update(fingerprint='f' * 64),
            lambda x: x['quotes'][0].update(ask='100'),
        ):
            raw = json.loads(encoded)
            mutate(raw)
            with self.subTest(raw=raw), self.assertRaises((LifecycleError, SchemaError)):
                self.lifecycle._load_snapshot(json.dumps(raw), BASE)
        duplicate = encoded[:-1] + ',"version":1}'
        with self.assertRaises(LifecycleError):
            self.lifecycle._load_snapshot(duplicate, BASE)

    def test_raw_snapshot_cannot_be_reinterpreted_on_restart(self):
        with self.lifecycle.tx() as db:
            db.execute('UPDATE control SET snapshot=?,observed=?', (json.dumps(self.observation()), BASE))
        before = self.path.read_bytes()
        with self.assertRaisesRegex(LifecycleError, 'invalid_stored_projection'):
            Lifecycle(self.path, FP, session_risk_policy=self.policy)
        self.assertEqual(before, self.path.read_bytes())

    def test_missing_core_table_or_control_never_gets_recreated(self):
        for statement in ('DELETE FROM control', 'DROP TABLE intents',
                          'DELETE FROM session_risk_binding', 'DROP TABLE control'):
            with self.subTest(statement=statement):
                path = Path(self.tmp.name) / ('corrupt-' + str(len(list(Path(self.tmp.name).iterdir()))) + '.db')
                ledger = Lifecycle(path, FP, session_risk_policy=self.policy)
                with ledger.tx() as db:
                    db.execute(statement)
                before = path.read_bytes()
                with self.assertRaises(LifecycleError):
                    Lifecycle(path, FP, session_risk_policy=self.policy)
                self.assertEqual(before, path.read_bytes())

    def test_insecure_mode_refused_without_chmod_or_mutation(self):
        self.observe()
        before = self.path.read_bytes()
        self.path.chmod(0o644)
        with self.assertRaisesRegex(LifecycleError, 'private_storage_required'):
            Lifecycle(self.path, FP, session_risk_policy=self.policy)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o644)
        self.assertEqual(self.path.read_bytes(), before)

    def test_symlink_main_parent_and_sidecars_refused(self):
        link = Path(self.tmp.name) / 'link.db'
        link.symlink_to(self.path)
        with self.assertRaises(LifecycleError):
            Lifecycle(link, FP)
        parent = Path(self.tmp.name) / 'dirlink'
        parent.symlink_to(self.tmp.name, target_is_directory=True)
        with self.assertRaises(LifecycleError):
            Lifecycle(parent / 'new.db', FP)
        self.assertFalse((Path(self.tmp.name) / 'new.db').exists())
        for suffix in ('-journal', '-wal', '-shm'):
            sidecar = Path(str(self.path) + suffix)
            sidecar.symlink_to(Path(self.tmp.name) / 'missing-target')
            try:
                with self.assertRaises(LifecycleError):
                    self.lifecycle.status()
            finally:
                sidecar.unlink()
        self.assertFalse((Path(self.tmp.name) / 'missing-target').exists())

    def test_insecure_sidecar_and_hardlink_and_fifo_refused(self):
        for suffix in ('-journal', '-wal', '-shm'):
            sidecar = Path(str(self.path) + suffix)
            sidecar.write_text('unchanged')
            sidecar.chmod(0o644)
            try:
                with self.assertRaises(LifecycleError):
                    self.lifecycle.status()
                self.assertEqual(sidecar.read_text(), 'unchanged')
            finally:
                sidecar.unlink()
        linked = Path(self.tmp.name) / 'hardlink'
        os.link(self.path, linked)
        try:
            with self.assertRaises(LifecycleError):
                self.lifecycle.status()
        finally:
            linked.unlink()
        fifo = Path(self.tmp.name) / 'fifo'
        os.mkfifo(fifo, 0o600)
        with self.assertRaises(LifecycleError):
            Lifecycle(fifo, FP)

    def test_deleted_or_replaced_open_ledger_is_not_reinitialized(self):
        original = Path(self.tmp.name) / 'moved.sqlite'
        self.path.rename(original)
        with self.assertRaises(LifecycleError):
            self.lifecycle.status()
        self.assertFalse(self.path.exists())
        self.path.write_bytes(b'not a database')
        self.path.chmod(0o600)
        with self.assertRaisesRegex(LifecycleError, 'private_storage_changed'):
            self.lifecycle.status()
        self.assertEqual(self.path.read_bytes(), b'not a database')

    def test_lock_wait_is_bounded(self):
        with sqlite3.connect(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            with self.assertRaises(sqlite3.OperationalError):
                self.lifecycle.status()
            self.assertLess(time.monotonic() - started, 2.0)


if __name__ == '__main__':
    unittest.main()
