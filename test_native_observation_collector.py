"""Fixture-only tests. Every socket creation/connect is denied in this suite."""
import copy
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from urllib import error

from alpaca_schema import parse_order, parse_snapshot, timestamp
from native_observation_collector import (
    CollectionRefused, NativeReadOnlyHTTP, NativeObservationCollector,
    NativeObservation, OwnedLedgerView, SharedRequestBudget, PAPER_ORIGIN,
    DATA_ORIGIN, ORDER_PATH, MAX_BYTES, NS, quote_path)

F = json.loads((Path(__file__).parent / 'fixtures' / 'alpaca-native.json').read_text())
NOW = timestamp('2026-10-19T13:31:02Z')
FP = hashlib.sha256(F['account']['id'].encode()).hexdigest()


class Clock:
    def __init__(self):
        self.wall = NOW
        self.mono = 100 * NS
    def now(self): return self.wall
    def monotonic(self): return self.mono
    def advance(self, ns):
        self.wall += ns; self.mono += ns


class FixtureWire:
    paper_origin = PAPER_ORIGIN
    data_origin = DATA_ORIGIN
    feed = 'iex'
    def __init__(self, fixture=None):
        self.f = copy.deepcopy(F if fixture is None else fixture)
        self.calls = []
        self.overrides = {}
        self.hook = None
    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if self.hook:
            self.hook(path)
        if path in self.overrides:
            result = self.overrides[path]
            if callable(result): return result()
            return copy.deepcopy(result)
        if path == '/v2/account': return 200, copy.deepcopy(self.f['account'])
        if path == '/v2/positions': return 200, copy.deepcopy(self.f['positions'])
        if path == '/v2/clock': return 200, copy.deepcopy(self.f['clock'])
        if path.startswith('/v2/calendar?'):
            return 200, [{'date': '2026-10-19', 'open': '09:30', 'close': '16:00'}]
        if path.startswith(ORDER_PATH):
            start = 0
            if '&before_order_id=' in path:
                cursor = path.split('&before_order_id=')[1]
                start = next(i for i, o in enumerate(self.f['orders']) if o['id'] == cursor) + 1
            return 200, copy.deepcopy(self.f['orders'][start:start + 100])
        if path.startswith('/v2/orders:by_client_order_id?client_order_id='):
            cid = path.split('=')[1]
            order = next((o for o in self.f['orders'] if o['client_order_id'] == cid), None)
            return (200, copy.deepcopy(order)) if order else (404, None)
        if path == quote_path(('AAPL',)):
            return 200, {'quotes': {'AAPL': copy.deepcopy(self.f['quote']['quote'])}}
        raise AssertionError('unexpected fixture route')


class NoSockets(unittest.TestCase):
    def setUp(self):
        super().setUp()
        for name in ('socket.socket', 'socket.create_connection'):
            p = patch(name, side_effect=AssertionError('NETWORK FORBIDDEN IN TESTS'))
            p.start(); self.addCleanup(p.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'shared.sqlite'


class CollectorTests(NoSockets):
    def setUp(self):
        super().setUp()
        self.clock = Clock()
        self.wire = FixtureWire()
        self.view = OwnedLedgerView(FP, 1, ('demo-entry',))
    def collector(self, **kw):
        options = dict(database=self.path, transport=self.wire, expected_fingerprint=FP,
                       ledger_view=lambda: self.view, enabled=True,
                       now_ns=self.clock.now, monotonic_ns=self.clock.monotonic)
        options.update(kw)
        return NativeObservationCollector(**options)
    def reject(self, code=None, **kw):
        with self.assertRaises(CollectionRefused) as cm:
            self.collector(**kw).collect()
        if code: self.assertEqual(cm.exception.code, code)
        return cm.exception
    def fill_orders(self, count):
        first = copy.deepcopy(F['orders'][0])
        self.wire.f['orders'] = [first]
        for i in range(1, count):
            o = copy.deepcopy(first); o.update(id='order-' + str(i), client_order_id='client-' + str(i))
            self.wire.f['orders'].append(o)
    def test_disabled_zero_calls_and_zero_database(self):
        self.reject('collector_disabled', enabled=False)
        self.assertEqual(self.wire.calls, [])
        self.assertFalse(self.path.exists())
    def test_valid_native_observation_exact_ns_and_private_seam(self):
        result = self.collector().collect()
        self.assertIs(type(result), NativeObservation)
        self.assertEqual(result.requests, 9)
        self.assertEqual(result.quotes[0].timestamp_ns, timestamp(F['quote']['quote']['t']))
        self.assertEqual(result.session_open_ns, timestamp('2026-10-19T13:30:00Z'))
        self.assertFalse(result.executable)
        self.assertFalse(hasattr(result, 'read'))
        args = result.native_arguments()
        self.assertNotIn('expected_fingerprint', args)
        self.assertEqual(args['account']['id'], F['account']['id'])
        parsed = parse_snapshot(**{k: v for k, v in args.items() if k not in ('quotes','quote_symbols','feed')}, expected_fingerprint=FP, now_ns=NOW)
        self.assertEqual(parsed, result.snapshot)
        args['account']['id'] = 'mutated'
        self.assertEqual(result.native_arguments()['account']['id'], F['account']['id'])
        self.assertTrue(all(m == 'GET' and b is None for m, _, b in self.wire.calls))
    def test_audit_and_repr_have_no_raw_identity_balances_or_credentials(self):
        result = self.collector().collect()
        with sqlite3.connect(self.path) as db:
            audit = str(db.execute('SELECT * FROM native_read_audit').fetchall())
        exposed = audit + repr(result) + json.dumps(result.summary())
        for secret in (F['account']['id'], F['account']['account_number'], F['account']['cash']):
            self.assertNotIn(secret, exposed)
    def test_decimal_quote_values_remain_exact_and_json_safe(self):
        self.wire.f['quote']['quote']['bp'] = Decimal('200.100000001')
        result = self.collector().collect()
        self.assertEqual(result.native_arguments()['quotes']['quotes']['AAPL']['bp'], '200.100000001')
    def test_immutable_evidence_and_fresh_copy(self):
        result = self.collector().collect()
        self.assertIs(type(result._native_json), bytes)
        args = result.native_arguments(); args['orders'].clear()
        self.assertEqual(len(result.native_arguments()['orders']), 1)
        from dataclasses import replace
        tampered = replace(result, _native_json=b'{}')
        with self.assertRaisesRegex(CollectionRefused, 'evidence_digest_mismatch'):
            tampered.native_arguments()
        self.assertNotIn('demo-entry', repr(self.view))
    def test_instance_nonblocking_guard(self):
        c = self.collector()
        c._lock.acquire()
        try:
            with self.assertRaisesRegex(CollectionRefused, 'collector_already_running'): c.collect()
        finally: c._lock.release()
        self.assertEqual(self.wire.calls, [])
    def test_quotes_expiring_during_collection_are_not_returned(self):
        def change(path):
            if len(self.wire.calls) == 9: self.clock.advance(9 * NS + 500_000_000)
        self.wire.hook = change
        self.reject('native_schema_refused')
    def test_owned_valid_progress_is_still_uncertain_snapshot(self):
        first = self.wire.f['orders'][0]
        first.update(status='pending_cancel', canceled_at=None)
        later = copy.deepcopy(first)
        later.update(filled_qty='0.2', updated_at='2026-10-19T13:31:02Z')
        self.wire.overrides['/v2/orders:by_client_order_id?client_order_id=demo-entry'] = (200, later)
        self.reject('order_changed_during_collection')
    def test_quote_and_clock_received_before_exact_future_ns(self):
        self.clock.wall = timestamp('2026-10-19T13:31:02.123456788Z')
        self.wire.f['clock']['timestamp'] = '2026-10-19T13:31:02.123456789Z'
        self.reject('native_schema_refused')
    def test_transport_call_exception_cannot_inject_audit_text(self):
        self.wire.overrides['/v2/account'] = lambda: (_ for _ in ()).throw(CollectionRefused('FIXTURE_SECRET'))
        self.reject('collection_failed')
        with sqlite3.connect(self.path) as db:
            self.assertNotIn('FIXTURE_SECRET', str(db.execute('SELECT * FROM native_read_audit').fetchall()))
    def test_invalid_binding_has_no_calls(self): self.reject('invalid_expected_binding', expected_fingerprint='wrong')
    def test_different_authenticated_account(self):
        self.wire.f['account']['id'] = 'another-account'
        self.reject('native_schema_refused')
    def test_transport_host_context_rejected_before_calls(self):
        self.wire.paper_origin = 'https://api.alpaca.markets'
        self.reject('invalid_transport_context'); self.assertEqual(self.wire.calls, [])
    def test_wrong_transport_feed(self):
        self.wire.feed = 'sip'; self.reject('invalid_transport_context')
    def test_wrong_payload_feed(self):
        self.wire.overrides[quote_path(('AAPL',))] = (200, {'feed': 'sip', 'quotes': {'AAPL': F['quote']['quote']}})
        self.reject('wrong_quote_feed')
    def test_wrong_row_feed(self):
        self.wire.f['quote']['quote']['feed'] = 'sip'; self.reject('wrong_quote_feed')
    def test_single_symbol_envelope_is_not_accepted_on_multi_route(self):
        q = copy.deepcopy(F['quote']); q['quote']['feed'] = 'sip'
        self.wire.overrides[quote_path(('AAPL',))] = (200, q)
        self.reject('invalid_provider_payload')
    def test_final_clock_exception_records_refusal_not_success(self):
        collector = self.collector()
        original = SharedRequestBudget.audit
        def poison_clock(budget, **kwargs):
            result = original(budget, **kwargs)
            if kwargs['status'] == 'observed_readonly':
                collector.now_ns = lambda: (_ for _ in ()).throw(RuntimeError('private clock detail'))
            return result
        with patch.object(SharedRequestBudget, 'audit', poison_clock):
            with self.assertRaisesRegex(CollectionRefused, 'collection_failed'):
                collector.collect()
        with sqlite3.connect(self.path) as db:
            self.assertNotIn(('observed_readonly',), db.execute('SELECT status FROM native_read_audit').fetchall())

    def test_post_audit_deadline_overwrites_same_row_with_refusal(self):
        original = SharedRequestBudget.audit
        def slow(budget, **kwargs):
            rowid = original(budget, **kwargs)
            if kwargs['status'] == 'observed_readonly': self.clock.advance(10 * NS + 1)
            return rowid
        with patch.object(SharedRequestBudget, 'audit', slow):
            self.reject('collection_deadline_exceeded')
        with sqlite3.connect(self.path) as db:
            rows = db.execute('SELECT status,digest FROM native_read_audit').fetchall()
        self.assertEqual(rows, [('collection_deadline_exceeded', None)])
    def test_post_audit_native_freshness_revalidated(self):
        original = SharedRequestBudget.audit
        def slow(budget, **kwargs):
            rowid = original(budget, **kwargs)
            if kwargs['status'] == 'observed_readonly': self.clock.advance(9 * NS + 1)
            return rowid
        with patch.object(SharedRequestBudget, 'audit', slow):
            self.reject('native_schema_refused')
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT status FROM native_read_audit').fetchall(), [('native_schema_refused',)])
    def test_partial_quotes_refused(self):
        self.wire.overrides[quote_path(('AAPL',))] = (200, {'quotes': {}})
        self.reject('native_schema_refused')
    def test_stale_clock(self):
        self.wire.f['clock']['timestamp'] = '2026-10-19T13:30:51.999999999Z'
        self.reject('native_schema_refused')
    def test_future_clock_one_nanosecond(self):
        self.wire.f['clock']['timestamp'] = '2026-10-19T13:31:02.000000001Z'
        self.reject('native_schema_refused')
    def test_exact_nanosecond_session_freshness(self):
        self.clock.wall += 123456789
        self.wire.f['clock']['timestamp'] = '2026-10-19T13:31:02.123456789Z'
        self.collector().collect()
    def test_stale_quote(self):
        self.wire.f['quote']['quote']['t'] = '2026-10-19T13:30:51.999999999Z'
        self.reject('native_schema_refused')
    def test_future_quote_one_nanosecond(self):
        self.wire.f['quote']['quote']['t'] = '2026-10-19T13:31:02.000000001Z'
        self.reject('native_schema_refused')
    def test_closed_session_no_invented_data(self):
        self.wire.f['clock']['is_open'] = False
        self.reject('closed_session'); self.assertEqual(len(self.wire.calls), 2)
    def test_calendar_close_must_match_native(self):
        self.wire.f['clock']['next_close'] = '2026-10-19T15:00:00-04:00'
        self.reject('session_validation_refused')
    def test_calendar_absent_refused(self):
        self.wire.overrides['/v2/calendar?start=2026-10-19&end=2026-10-19'] = (200, [])
        self.reject('session_validation_refused')
    def test_blocked_account(self):
        self.wire.f['account']['account_blocked'] = True; self.reject('native_schema_refused')
    def test_account_changes_during_collection(self):
        def change(path):
            if path == quote_path(('AAPL',)): self.wire.f['account']['cash'] = '1'
        self.wire.hook = change
        self.reject('account_changed_during_collection')
    def test_position_changes_during_collection(self):
        def change(path):
            if path == quote_path(('AAPL',)): self.wire.f['positions'][0]['qty_available'] = '0'
        self.wire.hook = change
        self.reject('positions_changed_during_collection')
    def test_response_account_conflict(self):
        self.wire.f['orders'][0]['account_id'] = 'another-account'
        self.reject('conflicting_response_account')
    def test_identity_changes_on_second_account(self):
        def change(path):
            if path == quote_path(('AAPL',)): self.wire.f['account']['id'] = 'another-account'
        self.wire.hook = change
        self.reject('native_schema_refused')
    def test_invalid_or_partial_page_envelope(self):
        self.wire.overrides[ORDER_PATH] = (200, {'orders': [], 'next_page_token': 'opaque'})
        self.reject('partial_or_invalid_order_page')
    def test_oversized_page(self):
        self.wire.overrides[ORDER_PATH] = (200, F['orders'] * 101)
        self.reject('partial_or_invalid_order_page')
    def test_duplicate_ids_refused(self):
        self.wire.f['orders'] *= 2; self.reject('duplicate_order_or_cursor')
    def test_unsupported_order(self):
        self.wire.f['orders'][0]['status'] = 'pending_replace'; self.reject('native_schema_refused')
    def test_desc_id_pagination_handles_equal_submitted_times(self):
        self.fill_orders(101)
        result = self.collector().collect()
        self.assertEqual((result.order_count, result.order_pages), (101, 2))
        self.assertIn(('GET', ORDER_PATH + '&before_order_id=order-99', None), self.wire.calls)
        self.assertFalse(any('until=' in p or 'after=' in p for _,p,_ in self.wire.calls))
    def test_provider_repeats_cursor_page(self):
        self.fill_orders(101)
        self.wire.overrides[ORDER_PATH + '&before_order_id=order-99'] = (200, self.wire.f['orders'][:100])
        self.reject('duplicate_order_or_cursor')
    def test_unsupported_cursor_fails_closed(self):
        self.fill_orders(101)
        self.wire.overrides[ORDER_PATH + '&before_order_id=order-99'] = (400, None)
        self.reject('provider_http_error')
    def test_499_aggregate_requires_five_pages_and_full_budget(self):
        self.fill_orders(499); self.view = OwnedLedgerView(FP, 1, ())
        r = self.collector().collect()
        self.assertEqual((r.order_count, r.order_pages, r.requests), (499, 5, 12))
    def test_500_aggregate_refused(self):
        self.fill_orders(500); self.view = OwnedLedgerView(FP, 1, ())
        self.reject('aggregate_orders_overflow')
    def test_ledger_id_missing_get_still_attempted(self):
        self.wire.f['orders'] = []
        self.reject('owned_order_missing')
        self.assertTrue(self.wire.calls[-1][1].endswith('client_order_id=demo-entry'))
    def test_owned_order_missing_from_scan_but_lookup_present(self):
        self.wire.overrides[ORDER_PATH] = (200, [])
        self.reject('owned_order_missing_from_scan')
    def test_lookup_wrong_id(self):
        o = copy.deepcopy(F['orders'][0]); o['client_order_id'] = 'another-id'
        self.wire.overrides['/v2/orders:by_client_order_id?client_order_id=demo-entry'] = (200, o)
        self.reject('owned_lookup_mismatch')
    def test_lookup_fill_regression(self):
        o = copy.deepcopy(F['orders'][0]); o['filled_qty'] = '0.1'
        self.wire.overrides['/v2/orders:by_client_order_id?client_order_id=demo-entry'] = (200, o)
        self.reject('native_schema_refused')
    def test_prior_durable_order_regression(self):
        prior = copy.deepcopy(F['orders'][0]); prior['filled_qty'] = '0.2'
        self.view = OwnedLedgerView(FP, 1, ('demo-entry',), (parse_order(prior),))
        self.reject('native_schema_refused')
    def test_ledger_revision_changes(self):
        def change(path):
            if path == quote_path(('AAPL',)): self.view = OwnedLedgerView(FP, 2, ('demo-entry',))
        self.wire.hook = change; self.reject('owned_ledger_changed_during_collection')
    def test_ledger_unavailable_and_injection_have_zero_calls(self):
        self.view = OwnedLedgerView(FP, 1, ('demo-entry&feed=sip',))
        self.reject('invalid_owned_ledger'); self.assertEqual(self.wire.calls, [])
    def test_too_many_owned_ids_does_not_silently_skip(self):
        self.view = OwnedLedgerView(FP, 1, tuple('cid-' + str(i) for i in range(5)))
        self.reject('owned_lookups_exceed_collection_budget'); self.assertEqual(self.wire.calls, [])
    def test_every_owned_id_independently_requested(self):
        self.fill_orders(4)
        self.view = OwnedLedgerView(FP, 1, tuple(o['client_order_id'] for o in self.wire.f['orders']))
        r = self.collector().collect(); self.assertEqual(r.requests, 12)
        self.assertEqual(sum('/orders:by_client_order_id?' in p for _,p,_ in self.wire.calls), 4)
    def test_deadline_after_slow_call_never_returns_success(self):
        self.wire.hook = lambda path: self.clock.advance(10 * NS + 1)
        self.reject('collection_deadline_exceeded'); self.assertEqual(len(self.wire.calls), 1)
    def test_monotonic_clock_regression(self):
        self.wire.hook = lambda path: setattr(self.clock, 'mono', self.clock.mono - 1)
        self.reject('local_clock_regression')
    def test_wall_clock_regression(self):
        self.wire.hook = lambda path: setattr(self.clock, 'wall', self.clock.wall - 1)
        self.reject('local_clock_regression')
    def test_transport_error_sanitized_and_reservation_not_refunded(self):
        self.wire.overrides['/v2/account'] = lambda: (_ for _ in ()).throw(RuntimeError('credential-secret'))
        error = self.reject('provider_read_failed'); self.assertNotIn('credential-secret', str(error))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_read_attempts').fetchone()[0], 1)
    def test_auth_and_rate_no_retries(self):
        for status, code in ((401,'authentication_or_entitlement_failed'), (403,'authentication_or_entitlement_failed'), (429,'provider_rate_limited')):
            self.wire.overrides['/v2/account'] = (status, {'secret': 'not-logged'})
            self.reject(code)
        self.assertEqual(len(self.wire.calls), 3)
    def test_independent_collectors_share_quota(self):
        self.collector().collect()
        self.reject('shared_request_budget_exhausted')
        self.assertEqual(len(self.wire.calls), 12)
    def test_corrupt_database_refuses_without_network(self):
        self.path.write_bytes(b'not a sqlite database')
        self.reject('budget_storage_unavailable'); self.assertEqual(self.wire.calls, [])



class BudgetTests(NoSockets):
    def test_concurrent_shared_quota_and_restart(self):
        budgets = [SharedRequestBudget(self.path) for _ in range(24)]
        def reserve(b):
            try: b.reserve(NOW); return True
            except CollectionRefused: return False
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(reserve, budgets))
        self.assertEqual(sum(results), 12)
        with self.assertRaises(CollectionRefused): SharedRequestBudget(self.path).reserve(NOW)
        SharedRequestBudget(self.path).reserve(NOW + 60 * NS)
    def test_regression_cannot_reset_shared_budget(self):
        b = SharedRequestBudget(self.path); b.reserve(NOW)
        with self.assertRaisesRegex(CollectionRefused, 'budget_clock_regression'): b.reserve(NOW - 1)
        with sqlite3.connect(self.path) as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM native_read_attempts').fetchone()[0], 1)
    def test_corrupt_high_water_and_missing_metadata_refused(self):
        b = SharedRequestBudget(self.path); b.reserve(NOW)
        with sqlite3.connect(self.path) as db: db.execute("UPDATE native_read_budget SET high_ns='corrupt'")
        with self.assertRaisesRegex(CollectionRefused, 'budget_state_corrupt'): b.reserve(NOW)
        with sqlite3.connect(self.path) as db: db.execute('DELETE FROM native_read_budget')
        with self.assertRaisesRegex(CollectionRefused, 'budget_state_corrupt'): SharedRequestBudget(self.path)
    def test_empty_existing_metadata_deletion_is_not_reinitialized(self):
        SharedRequestBudget(self.path)
        with sqlite3.connect(self.path) as db: db.execute('DELETE FROM native_read_budget')
        with self.assertRaisesRegex(CollectionRefused, 'budget_state_corrupt'): SharedRequestBudget(self.path)
    def test_dropped_attempt_table_is_not_reinitialized(self):
        b = SharedRequestBudget(self.path); b.reserve(NOW)
        with sqlite3.connect(self.path) as db: db.execute('DROP TABLE native_read_attempts')
        with self.assertRaisesRegex(CollectionRefused, 'budget_state_corrupt'): SharedRequestBudget(self.path)
    def test_unexpected_future_or_excess_records_refused(self):
        b = SharedRequestBudget(self.path); b.reserve(NOW)
        with sqlite3.connect(self.path) as db: db.execute('INSERT INTO native_read_attempts VALUES(?)', (NOW + 1,))
        with self.assertRaisesRegex(CollectionRefused, 'budget_state_corrupt'): b.reserve(NOW + 1)
    def test_symlink_refused(self):
        target = Path(self.temp.name) / 'target'; target.write_text('')
        self.path.symlink_to(target)
        with self.assertRaises(CollectionRefused): SharedRequestBudget(self.path)


class Response(io.BytesIO):
    def __init__(self, raw, url=PAPER_ORIGIN + '/v2/account', status=200, headers=None):
        super().__init__(raw); self.url = url; self.status = status; self.headers = headers or {}
    def geturl(self): return self.url


class Opener:
    def __init__(self, response=None, exception=None):
        self.response, self.exception, self.calls = response, exception, []
    def open(self, req, timeout):
        self.calls.append((req, timeout))
        if self.exception: raise self.exception
        return self.response


class TransportTests(NoSockets):
    def wire(self, **kw): return NativeReadOnlyHTTP('FIXTURE_KEY', 'FIXTURE_SECRET', enabled=True, **kw)
    def test_disabled_wire_zero_calls(self):
        w = NativeReadOnlyHTTP('FIXTURE_KEY','FIXTURE_SECRET')
        w._opener = Opener(Response(b'{}'))
        with self.assertRaisesRegex(CollectionRefused, 'transport_disabled'): w.call('GET','/v2/account')
        self.assertFalse(w._opener.calls)
    def test_post_delete_body_and_path_injection_denied(self):
        w = self.wire(); w._opener = Opener(Response(b'{}'))
        cases = [('POST','/v2/account',None),('DELETE','/v2/orders',None),('GET','/v2/account',{}),
                 ('GET','https://evil.example/v2/account',None),('GET','//evil.example/v2/account',None),
                 ('GET','/v2/account?extra=x',None),('GET',ORDER_PATH+'&before_order_id=x%26status=open',None),
                 ('GET','/v2/orders:by_client_order_id?client_order_id=x&x=y',None),
                 ('GET','/v2/stocks/quotes/latest?symbols=AAPL&feed=sip',None),
                 ('GET','/v2/../account',None)]
        for m,p,b in cases:
            with self.subTest(path=p), self.assertRaises(CollectionRefused): w.call(m,p,b)
        self.assertFalse(w._opener.calls)
    def test_wrong_host_rejected_at_construction(self):
        for options in ({'paper_origin':'https://api.alpaca.markets'}, {'data_origin':'https://evil.example'}):
            with self.assertRaisesRegex(CollectionRefused,'forbidden_origin'): self.wire(**options)
    def test_changed_response_url_refused(self):
        w=self.wire();w._opener=Opener(Response(b'{}',url='https://evil.example'))
        with self.assertRaisesRegex(CollectionRefused,'redirect_refused'): w.call('GET','/v2/account')
    def test_http_redirect_not_followed_or_body_read(self):
        exc=error.HTTPError(PAPER_ORIGIN+'/v2/account',302,'secret-provider-message',{},None)
        w=self.wire();w._opener=Opener(exception=exc)
        self.assertEqual(w.call('GET','/v2/account'),(302,None));self.assertEqual(len(w._opener.calls),1)
    def test_real_opener_has_proxy_disabled_and_redirect_denial(self):
        w=self.wire()
        from paper_adapter import NoRedirect
        self.assertTrue(any(type(h) is NoRedirect for h in w._opener.handlers))
        self.assertFalse(any(type(h).__name__=='ProxyHandler' and h.proxies for h in w._opener.handlers))
    def test_duplicate_json_nan_partial_and_oversize_refused(self):
        cases=[(b'{"a":1,"a":2}',{}),(b'{"a":NaN}',{}),(b'{"a":',{}),
               (b'{}',{'Content-Length':'50'}),(b'{}',{'Content-Encoding':'gzip'}),
               (b'x'*(MAX_BYTES+1),{}),(b'{}',{'Content-Length':str(MAX_BYTES+1)})]
        for raw,headers in cases:
            w=self.wire();w._opener=Opener(Response(raw,headers=headers))
            with self.subTest(size=len(raw)),self.assertRaises(CollectionRefused):w.call('GET','/v2/account')
    def test_timeout_bounded_and_json_decimal(self):
        w=self.wire();w.set_timeout(.01);w._opener=Opener(Response(b'{"bp":200.100000001}'))
        status,data=w.call('GET','/v2/account')
        self.assertEqual((status,data['bp']),(200,Decimal('200.100000001')))
        self.assertEqual(w._opener.calls[0][1],.01)
        for seconds in (0,3,float('nan'),True):
            with self.assertRaises(CollectionRefused):w.set_timeout(seconds)
    def test_exception_messages_not_exposed(self):
        w=self.wire();w._opener=Opener(exception=RuntimeError('FIXTURE_SECRET'))
        with self.assertRaises(CollectionRefused) as cm:w.call('GET','/v2/account')
        self.assertEqual(str(cm.exception),'provider_read_failed')


from paper_adapter import SafetyError
if __name__ == '__main__': unittest.main()
