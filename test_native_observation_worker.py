"""Offline one-shot worker tests. No credentials, provider or socket access."""
import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from alpaca_schema import timestamp
from native_observation_collector import CollectionRefused, ORDER_PATH, PAPER_ORIGIN, DATA_ORIGIN
import native_observation_worker as worker

NOW = timestamp('2026-10-20T13:31:02Z')
F = json.loads((Path(__file__).parent / 'fixtures/alpaca-native.json').read_text())
FP = hashlib.sha256(F['account']['id'].encode()).hexdigest()
CREDS = {'ALPACA_API_KEY': 'fixture-key-not-real', 'ALPACA_SECRET_KEY': 'fixture-secret-not-real'}


def create_source(path):
    """Actual deployed table shapes, with synthetic already-approved bindings."""
    with sqlite3.connect(path) as c:
        c.executescript('''
            CREATE TABLE paper_readonly (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL DEFAULT 0,
                fingerprint TEXT, masked_identity TEXT, checked_at REAL, ok INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT 'not_checked', snapshot TEXT NOT NULL DEFAULT '{}',
                stopped INTEGER NOT NULL DEFAULT 0, identity_first_seen REAL);
            CREATE TABLE safety (id INTEGER PRIMARY KEY CHECK(id=1), stopped INTEGER NOT NULL);
            CREATE TABLE paper_experiment (id INTEGER PRIMARY KEY CHECK(id=1), experiment TEXT NOT NULL,
                approval TEXT NOT NULL, fingerprint TEXT NOT NULL, phase TEXT NOT NULL DEFAULT 'waiting',
                reason TEXT NOT NULL DEFAULT 'waiting_clock', next_open TEXT, session_end REAL, updated REAL NOT NULL,
                buy_id TEXT, sell_id TEXT, cancel_buy INTEGER NOT NULL DEFAULT 0,
                cancel_sell INTEGER NOT NULL DEFAULT 0, cancel_at REAL);
            CREATE TABLE paper_renewal (id INTEGER PRIMARY KEY CHECK(id=1), experiment TEXT NOT NULL,
                approval TEXT NOT NULL, fingerprint TEXT NOT NULL, phase TEXT NOT NULL DEFAULT 'waiting',
                reason TEXT NOT NULL DEFAULT 'waiting_clock', next_open TEXT, session_end REAL, updated REAL NOT NULL,
                buy_id TEXT, sell_id TEXT, cancel_buy INTEGER NOT NULL DEFAULT 0,
                cancel_sell INTEGER NOT NULL DEFAULT 0, cancel_at REAL);
            CREATE TABLE intents (client_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created REAL NOT NULL,
                state TEXT NOT NULL, broker_id TEXT, filled TEXT NOT NULL DEFAULT '0');
            CREATE TABLE renewal_intents (client_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created REAL NOT NULL,
                state TEXT NOT NULL, broker_id TEXT, filled TEXT NOT NULL DEFAULT '0');
        ''')
        c.execute('INSERT INTO paper_readonly(id,fingerprint,identity_first_seen) VALUES(1,?,?)',
                  (FP, worker.ORIGINAL_CUTOFF - 60))
        c.execute('INSERT INTO safety VALUES(1,0)')
        for table, generation, approval, phase, reason, cutoff in (
            ('paper_experiment', worker.ORIGINAL_ID, worker.ORIGINAL_APPROVAL, 'manual_attention', 'quote_moved', worker.ORIGINAL_CUTOFF),
            ('paper_renewal', worker.RENEWAL_ID, worker.RENEWAL_APPROVAL, 'completed', 'roundtrip_filled', worker.RENEWAL_CUTOFF)):
            buy = worker._client_id(generation, 'buy')
            sell = worker._client_id(generation, 'sell') if table == 'paper_renewal' else None
            c.execute('INSERT INTO ' + table + '(id,experiment,approval,fingerprint,phase,reason,updated,buy_id,sell_id) VALUES(1,?,?,?,?,?,?,?,?)',
                      (generation, approval, FP, phase, reason, cutoff + 120, buy, sell))
            for side in ('buy', 'sell') if sell else ('buy',):
                cid = worker._client_id(generation, side)
                payload = dict(client_order_id=cid, symbol='AAPL', side=side, qty='1', type='limit', limit_price='200',
                               time_in_force='day', extended_hours=False)
                intent_table = 'renewal_intents' if sell else 'intents'
                c.execute('INSERT INTO ' + intent_table + ' VALUES(?,?,?,?,?,?)',
                          (cid, json.dumps(payload, sort_keys=True), cutoff + 30,
                           'filled' if sell else 'reserved', 'broker-' + side if sell else None, '1' if sell else '0'))
    os.chmod(path, 0o600)


class FixtureWire:
    paper_origin = PAPER_ORIGIN
    data_origin = DATA_ORIGIN
    feed = 'iex'
    def __init__(self, *, opened=False, include_original=False):
        self.calls = []
        self.hook = None
        self.account = copy.deepcopy(F['account'])
        self.clock = dict(timestamp='2026-10-20T13:31:01Z', is_open=opened,
                          next_open='2026-10-21T13:30:00Z', next_close='2026-10-20T20:00:00Z')
        self.orders = []
        generations = [worker.RENEWAL_ID]
        if include_original:
            generations.insert(0, worker.ORIGINAL_ID)
        for generation in generations:
            for side in ('buy',) if generation == worker.ORIGINAL_ID else ('buy', 'sell'):
                order = copy.deepcopy(F['orders'][0])
                cid = worker._client_id(generation, side)
                order.update(id='broker-' + cid, client_order_id=cid, side=side, qty='1', filled_qty='1',
                             filled_avg_price='200', status='filled', canceled_at=None,
                             filled_at='2026-10-20T13:31:01Z', created_at='2026-10-20T13:30:00Z',
                             submitted_at='2026-10-20T13:30:00.000000001Z', updated_at='2026-10-20T13:31:01Z')
                self.orders.append(order)
    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if self.hook:
            self.hook(path)
        assert method == 'GET' and body is None
        if path == '/v2/account': return 200, copy.deepcopy(self.account)
        if path == '/v2/clock': return 200, copy.deepcopy(self.clock)
        if path == '/v2/positions': return 200, []
        if path.startswith('/v2/calendar?'):
            return 200, [{'date': '2026-10-20', 'open': '09:30', 'close': '16:00'}]
        if path == ORDER_PATH: return 200, copy.deepcopy(self.orders)
        if path.startswith('/v2/orders:by_client_order_id?client_order_id='):
            cid = path.split('=')[1]
            found = next((o for o in self.orders if o['client_order_id'] == cid), None)
            return (200, copy.deepcopy(found)) if found else (404, None)
        if path == '/v2/stocks/quotes/latest?symbols=AAPL&feed=iex':
            quote = copy.deepcopy(F['quote']['quote']); quote['t'] = '2026-10-20T13:31:01.123456789Z'
            return 200, {'quotes': {'AAPL': quote}}
        raise AssertionError('Unexpected synthetic request')


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.source = Path(self.tmp.name) / 'source.sqlite'
        self.state = Path(self.tmp.name) / 'probe.sqlite'
        create_source(self.source)
        self.wire = FixtureWire()
        self.output = []
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            p = patch(name, side_effect=AssertionError('Sockets forbidden'))
            p.start(); self.addCleanup(p.stop)
        self.factory_calls = []
    def factory(self, key, secret, *, enabled):
        self.factory_calls.append((key, secret, enabled))
        return self.wire
    def run_probe(self, **kw):
        args = dict(enabled=True, environ=CREDS, source_database=self.source, state_database=self.state,
                    transport_factory=self.factory, now_ns=lambda: NOW, monotonic_ns=lambda: 100_000_000_000,
                    emit=self.output.append)
        args.update(kw)
        return worker.run_once(**args)
    def sql(self, sql, params=()):
        with sqlite3.connect(self.source) as c:
            c.execute(sql, params)
    def claim(self):
        with sqlite3.connect(self.state) as c:
            c.row_factory = sqlite3.Row
            return dict(c.execute('SELECT * FROM native_probe_claim').fetchone())
    def test_import_has_no_db_or_transport_side_effect(self):
        spec = importlib.util.spec_from_file_location('probe_import_test', worker.__file__)
        module = importlib.util.module_from_spec(spec)
        with patch('sqlite3.connect', side_effect=AssertionError('Import DB access')), patch.object(worker.NativeReadOnlyHTTP, '__init__', side_effect=AssertionError('Import transport')):
            spec.loader.exec_module(module)
        self.assertFalse(self.state.exists())
    def test_disabled_and_missing_credentials_are_zero_calls_zero_state(self):
        self.assertEqual(self.run_probe(enabled=False)['status'], 'probe_disabled')
        self.assertEqual(self.run_probe(environ={})['status'], 'credentials_missing')
        self.assertEqual(self.factory_calls, [])
        self.assertEqual(self.wire.calls, [])
        self.assertFalse(self.state.exists())
    def test_malformed_credentials_zero_calls(self):
        self.assertEqual(self.run_probe(environ={'ALPACA_API_KEY': 'bad\nvalue', 'ALPACA_SECRET_KEY': 'fake'})['status'], 'invalid_injected_credentials')
        self.assertFalse(self.state.exists())
        self.assertFalse(self.factory_calls)
    def test_invalid_binding_and_cutoff_zero_calls(self):
        for sql, params in (
            ('UPDATE paper_readonly SET fingerprint=?', ('0' * 64,)),
            ('UPDATE paper_readonly SET identity_first_seen=?', (worker.ORIGINAL_CUTOFF + 1,)),
            ('UPDATE paper_renewal SET approval=?', ('unapproved-generation',)),
            ('UPDATE paper_experiment SET experiment=?', ('unapproved-original',)),
        ):
            with self.subTest(sql=sql):
                self.source.unlink(); create_source(self.source)
                self.sql(sql, params)
                self.assertEqual(self.run_probe()['status'], 'invalid_binding')
                self.assertFalse(self.state.exists())
        self.assertFalse(self.factory_calls)
    def test_missing_required_table_and_row_zero_calls(self):
        self.sql('DELETE FROM paper_renewal')
        self.assertEqual(self.run_probe()['status'], 'invalid_binding')
        self.source.unlink(); create_source(self.source)
        self.sql('DROP TABLE renewal_intents')
        self.assertIn(self.run_probe()['status'], ('source_unavailable', 'inconsistent_ledger'))
        self.assertFalse(self.factory_calls)
        self.assertFalse(self.state.exists())
    def test_active_renewal_is_not_observed(self):
        for phase in ('waiting', 'buy_reserved', 'buy_working', 'sell_reserved', 'sell_working'):
            self.sql('UPDATE paper_renewal SET phase=?', (phase,))
            self.assertEqual(self.run_probe()['status'], 'inconsistent_ledger')
        self.assertFalse(self.factory_calls)
        self.assertFalse(self.state.exists())
    def test_all_ids_include_original_reserved_and_hash_tracks_full_rows(self):
        first = worker.load_owned_ledger(self.source)
        self.assertEqual(len(first.client_ids), 3)
        self.assertIn(worker._client_id(worker.ORIGINAL_ID, 'buy'), first.client_ids)
        self.assertEqual(first.previous_orders, ())
        self.assertIs(type(first.revision), int)
        self.assertEqual(first, worker.load_owned_ledger(self.source))
        self.sql('UPDATE paper_readonly SET stopped=1')
        second = worker.load_owned_ledger(self.source)
        self.assertNotEqual(first.revision, second.revision)
        self.sql("UPDATE renewal_intents SET broker_id='different-broker' WHERE rowid=1")
        self.assertNotEqual(second.revision, worker.load_owned_ledger(self.source).revision)
        self.sql('UPDATE paper_renewal SET updated=updated+1')
        self.assertNotEqual(second.revision, worker.load_owned_ledger(self.source).revision)
    def test_orphan_missing_payload_id_and_completed_fill_inconsistency(self):
        bad_updates = (
            "DELETE FROM renewal_intents WHERE rowid=1",
            "UPDATE renewal_intents SET client_id='unowned' WHERE rowid=1",
            "UPDATE renewal_intents SET filled='0' WHERE rowid=1",
            "UPDATE intents SET state='filled',filled='1',broker_id='unexpected'",
            "UPDATE renewal_intents SET payload='{}' WHERE rowid=1",
            "UPDATE paper_experiment SET sell_id='unowned'",
        )
        for sql in bad_updates:
            with self.subTest(sql=sql):
                self.source.unlink(); create_source(self.source)
                self.sql(sql)
                self.assertEqual(self.run_probe()['status'], 'inconsistent_ledger')
                self.assertFalse(self.state.exists())
        self.assertFalse(self.factory_calls)
    def test_claim_durable_before_factory_and_factory_failure_never_replayed(self):
        def fail(key, secret, *, enabled):
            record = self.claim()
            self.assertEqual(record['status'], 'claimed')
            self.assertEqual(record['probe_id'], worker.RELEASE_PROBE_ID)
            self.assertEqual(record['claimed_ns'], NOW)
            raise RuntimeError('RAW-SECRET-DO-NOT-LEAK')
        result = self.run_probe(transport_factory=fail)
        self.assertEqual(result['status'], 'probe_failed')
        self.assertEqual(self.claim()['status'], 'probe_failed')
        self.assertEqual(self.run_probe()['status'], 'probe_already_claimed')
        self.assertFalse(self.factory_calls)
        self.assertNotIn('RAW-SECRET', ''.join(self.output))
    def test_killed_uncertain_attempt_stays_claimed_on_restart(self):
        def crash(*args, **kw):
            self.assertEqual(self.claim()['status'], 'claimed')
            raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt): self.run_probe(transport_factory=crash)
        self.assertEqual(self.claim()['status'], 'claimed')
        self.assertEqual(self.run_probe()['status'], 'probe_already_claimed')
        self.assertFalse(self.factory_calls)
    def test_concurrent_workers_make_only_one_transport(self):
        entered, release = threading.Event(), threading.Event()
        def hold(*args, **kw):
            self.assertEqual(self.claim()['status'], 'claimed')
            entered.set()
            self.assertTrue(release.wait(timeout=3))
            return self.wire
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.run_probe, transport_factory=hold)
            self.assertTrue(entered.wait(timeout=3))
            second = pool.submit(self.run_probe)
            try:
                self.assertEqual(second.result(timeout=3)['status'], 'probe_already_claimed')
            finally:
                release.set()
            self.assertEqual(first.result(timeout=3)['status'], 'closed_session')
        self.assertEqual(len(self.wire.calls), 2)
        self.assertFalse(self.factory_calls)
    def test_closed_session_truthfully_reports_identity_but_not_order_count(self):
        before = self.source.read_bytes()
        result = self.run_probe()
        self.assertEqual(result['status'], 'closed_session')
        self.assertEqual(result['mode'], 'native_readonly_probe')
        self.assertEqual(result['order_permission'], 'none')
        self.assertIs(result['executable'], False)
        self.assertIs(result['execution_ready'], False)
        self.assertIs(result['legacy_state_reconciled'], False)
        self.assertIs(result['identity_match'], True)
        self.assertEqual(result['requests'], 2)
        self.assertEqual(result['owned_count'], 3)
        self.assertNotIn('order_count', result)
        self.assertEqual(self.source.read_bytes(), before)
        self.assertEqual(self.claim()['status'], 'closed_session')
        self.assertEqual(self.claim()['identity_match'], 1)
        self.assertEqual(os.stat(self.state).st_mode & 0o777, 0o600)
    def test_unmatched_account_never_claims_identity(self):
        self.wire.account['id'] = 'different-account'
        result = self.run_probe()
        self.assertEqual(result['status'], 'native_schema_refused')
        self.assertNotIn('identity_match', result)
        self.assertEqual(result['requests'], 1)
    def test_spoofed_closed_exception_before_parse_does_not_prove_identity(self):
        self.wire.hook = lambda path: (_ for _ in ()).throw(CollectionRefused('closed_session'))
        result = self.run_probe()
        self.assertEqual(result['status'], 'closed_session')
        self.assertNotIn('identity_match', result)
        self.assertEqual(result['requests'], 1)
    def test_unknown_exception_code_and_text_never_leak(self):
        exc = CollectionRefused('collection_failed'); exc.code = 'RAW-PROVIDER-OR-CREDENTIAL-DETAIL'
        self.wire.hook = lambda path: (_ for _ in ()).throw(exc)
        result = self.run_probe()
        self.assertEqual(result['status'], 'probe_failed')
        self.assertNotIn(exc.code, ''.join(self.output))
    def test_original_absent_strict_owned_order_missing_not_filtered(self):
        self.wire = FixtureWire(opened=True)
        result = self.run_probe()
        self.assertEqual(result['status'], 'owned_order_missing')
        self.assertEqual(result['owned_count'], 3)
        original = worker._client_id(worker.ORIGINAL_ID, 'buy')
        self.assertTrue(any(path.endswith(original) for _, path, _ in self.wire.calls))
        self.assertTrue(all(method == 'GET' and body is None for method, _, body in self.wire.calls))
    def test_success_is_observation_not_execution_and_no_payload_persisted(self):
        self.wire = FixtureWire(opened=True, include_original=True)
        result = self.run_probe()
        self.assertEqual(result['status'], 'observed_readonly')
        self.assertIs(result['execution_ready'], False)
        self.assertIs(result['legacy_state_reconciled'], False)
        self.assertEqual(result['owned_count'], 3)
        self.assertEqual(result['order_count'], 3)
        self.assertEqual(result['requests'], 11)
        with sqlite3.connect(self.state) as c:
            dump = '\n'.join(c.iterdump())
        exposed = dump + ''.join(self.output)
        for private in (F['account']['id'], F['account']['cash'], FP, CREDS['ALPACA_API_KEY'],
                        CREDS['ALPACA_SECRET_KEY'], worker._client_id(worker.ORIGINAL_ID, 'buy')):
            self.assertNotIn(private, exposed)
        self.assertNotIn('native_json', dump)
    def test_source_changes_before_first_request_refuses_without_network(self):
        def change(*args, **kw):
            self.sql('UPDATE paper_readonly SET revision=revision+1')
            return self.wire
        result = self.run_probe(transport_factory=change)
        self.assertEqual(result['status'], 'owned_ledger_unavailable')
        self.assertFalse(self.wire.calls)
        self.assertNotIn('owned_count', result)
    def test_state_and_source_symlinks_rejected_without_transport(self):
        target = Path(self.tmp.name) / 'target.sqlite'
        target.write_text('not sqlite'); target.chmod(0o600)
        self.state.symlink_to(target)
        self.assertEqual(self.run_probe()['status'], 'state_unavailable')
        self.assertEqual(target.read_text(), 'not sqlite')
        self.state.unlink(); self.source.unlink(); self.source.symlink_to(target)
        self.assertEqual(self.run_probe()['status'], 'source_unavailable')
        self.assertFalse(self.factory_calls)
    def test_nonprivate_state_and_symlink_parent_rejected(self):
        self.state.touch(mode=0o644)
        self.state.chmod(0o644)
        self.assertEqual(self.run_probe()['status'], 'state_unavailable')
        self.assertFalse(self.factory_calls)
        link = Path(self.tmp.name) / 'linked-dir'; link.symlink_to(Path(self.tmp.name), target_is_directory=True)
        self.assertEqual(self.run_probe(source_database=link / 'source.sqlite')['status'], 'source_unavailable')
    def test_corrupt_claim_never_reinitialized(self):
        self.run_probe()
        with sqlite3.connect(self.state) as c:
            c.execute("UPDATE native_probe_claim SET probe_id='unknown-probe'")
        self.wire.calls.clear(); self.factory_calls.clear()
        self.assertEqual(self.run_probe()['status'], 'probe_state_corrupt')
        self.assertFalse(self.factory_calls)
        self.assertFalse(self.wire.calls)
    def test_existing_empty_claim_is_corrupt_not_fresh(self):
        self.run_probe()
        with sqlite3.connect(self.state) as c:
            c.execute('DELETE FROM native_probe_claim')
        self.factory_calls.clear(); self.wire.calls.clear()
        self.assertEqual(self.run_probe()['status'], 'probe_state_corrupt')
        self.assertFalse(self.factory_calls)
        self.assertFalse(self.wire.calls)
        with sqlite3.connect(self.state) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM native_probe_claim').fetchone()[0], 0)
    def test_missing_claim_with_prior_audit_is_corrupt(self):
        self.run_probe()
        with sqlite3.connect(self.state) as c:
            c.execute('DROP TABLE native_probe_claim')
        self.factory_calls.clear(); self.wire.calls.clear()
        self.assertEqual(self.run_probe()['status'], 'probe_state_corrupt')
        self.assertFalse(self.factory_calls)
        self.assertFalse(self.wire.calls)
    def test_cli_fixed_once_and_no_arbitrary_id_or_path(self):
        with patch.object(worker, 'run_once', return_value={'status': 'closed_session'}) as run:
            self.assertEqual(worker.main(['--once'], emit=self.output.append), 0)
            self.assertTrue(run.call_args.kwargs['enabled'])
            self.assertEqual(worker.main([], emit=self.output.append), 0)
            self.assertFalse(run.call_args.kwargs['enabled'])
            run.reset_mock()
            self.assertEqual(worker.main(['--once', '--id', 'PRIVATE-ARGUMENT'], emit=self.output.append), 2)
            run.assert_not_called()
            self.assertNotIn('PRIVATE-ARGUMENT', ''.join(self.output))
    def test_reviewed_collector_hash_matches_and_output_hash_is_reproducible(self):
        collector = Path(worker.__file__).with_name('native_observation_collector.py').read_bytes()
        self.assertEqual(hashlib.sha256(collector).hexdigest(), worker.REVIEWED_COLLECTOR_SHA256)
        expected = hashlib.sha256(Path(worker.__file__).read_bytes() + b'\0' + collector).hexdigest()
        self.assertEqual(self.run_probe()['code_sha256'], expected)


if __name__ == '__main__':
    unittest.main()
