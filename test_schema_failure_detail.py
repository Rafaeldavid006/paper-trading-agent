"""Offline detail propagation only; no provider, production state or credential use."""
import ast
import copy
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

import alpaca_schema
import native_observation_collector as collector
from test_native_observation_collector import Clock, FixtureWire, FP, NoSockets, OwnedLedgerView


class SchemaFailureDetailTests(NoSockets):
    def setUp(self):
        super().setUp()
        p = patch('socket.getaddrinfo', side_effect=AssertionError('DNS FORBIDDEN'))
        p.start(); self.addCleanup(p.stop)
        self.clock = Clock()
        self.wire = FixtureWire()
        self.view = OwnedLedgerView(FP, 1, ())

    def collect(self):
        return collector.NativeObservationCollector(database=self.path, transport=self.wire,
            expected_fingerprint=FP, ledger_view=lambda: self.view, symbols=('AAPL',), enabled=True,
            now_ns=self.clock.now, monotonic_ns=self.clock.monotonic).collect()

    def failure(self, schema_error=None):
        if schema_error is None:
            with self.assertRaises(collector.CollectionRefused) as cm:
                self.collect()
        else:
            with patch.object(collector, 'parse_account', side_effect=schema_error):
                with self.assertRaises(collector.CollectionRefused) as cm:
                    self.collect()
        return cm.exception

    def test_actual_stale_quote_preserves_detail_and_unchanged_durable_contract(self):
        self.wire.f['quote']['quote']['t'] = '2026-10-19T13:30:00Z'
        exc = self.failure()
        self.assertEqual((exc.code, exc.schema_code, str(exc), exc.args),
            ('native_schema_refused', 'stale_or_future', 'native_schema_refused', ('native_schema_refused',)))
        self.assertEqual(len(self.wire.calls), 6)
        self.assertEqual(exc.schema_stage, 'quotes')
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT status,attempts,owned_count,digest FROM native_read_audit').fetchall(),
                [('native_schema_refused', 6, 0, None)])
            self.assertEqual([r[1] for r in db.execute('PRAGMA table_info(native_read_audit)')],
                ['at_ns', 'status', 'attempts', 'owned_count', 'order_count', 'digest'])
            self.assertEqual(set(r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")),
                {'native_read_budget', 'native_read_attempts', 'native_read_audit'})

    def test_actual_account_mismatch_preserves_detail(self):
        self.wire.f['account']['id'] = 'different-test-account'
        exc = self.failure()
        self.assertEqual((exc.code, exc.schema_code), ('native_schema_refused', 'account_binding_mismatch'))
        self.assertEqual(len(self.wire.calls), 1)

    def test_every_allowlisted_code_is_preserved_at_catch(self):
        # Each disposable SQLite path owns its own synthetic quota window.
        for index, code in enumerate(sorted(collector.SCHEMA_FAILURE_CODES)):
            with self.subTest(code=code):
                self.path = Path(self.temp.name) / ('allowlisted-' + str(index) + '.sqlite')
                self.assertEqual(self.failure(alpaca_schema.SchemaError(code)).schema_code, code)

    def test_unknown_nonliteral_and_malformed_errors_are_not_rendered_or_exposed(self):
        class Dangerous:
            def __str__(self): raise AssertionError('do not stringify private data')
            def __repr__(self): raise AssertionError('do not repr private data')
            def __hash__(self): raise AssertionError('do not hash private data')
        class FakeString(str):
            def __hash__(self): raise AssertionError('do not hash a subclass')
        class DerivedSchema(alpaca_schema.SchemaError): pass
        errors = [alpaca_schema.SchemaError('PRIVATE_SECRET'), alpaca_schema.SchemaError(),
            alpaca_schema.SchemaError('stale_or_future','PRIVATE_SECRET'),
            alpaca_schema.SchemaError(['stale_or_future']), alpaca_schema.SchemaError(Dangerous()),
            alpaca_schema.SchemaError(FakeString('stale_or_future')), DerivedSchema('stale_or_future')]
        for index, error in enumerate(errors):
            with self.subTest(index=index):
                self.path = Path(self.temp.name) / ('malformed-' + str(index) + '.sqlite')
                exc = self.failure(error)
                self.assertEqual((exc.code, exc.schema_code), ('native_schema_refused', 'unknown_schema'))
                exposed = json.dumps({'code':exc.code, 'schema_code':exc.schema_code, 'args':exc.args}) + repr(exc)
                self.assertNotIn('PRIVATE_SECRET', exposed)
                with sqlite3.connect(self.path) as db:
                    self.assertNotIn('PRIVATE_SECRET', str(db.execute('SELECT * FROM native_read_audit').fetchall()))

    def test_non_schema_refusal_does_not_inherit_detail(self):
        self.wire.overrides['/v2/account'] = (503, None)
        exc = self.failure()
        self.assertEqual((exc.code, exc.schema_code), ('provider_http_error','none'))
        self.assertEqual(collector.CollectionRefused('provider_http_error', schema_code='stale_or_future').schema_code, 'none')

    def test_success_contract_unchanged(self):
        result = self.collect()
        self.assertEqual(result.summary()['mode'], 'readonly_native_observation')
        self.assertIs(result.summary()['executable'], False)
        self.assertNotIn('schema_code', result.summary())
        self.assertEqual(len(self.wire.calls), 8)
        self.assertTrue(all(method=='GET' and body is None for method, _, body in self.wire.calls))

    def test_nonquote_collector_stages_are_coarse_fixed_validation_phases(self):
        cases=(('parse_account','account'),('parse_clock','clock'),('parse_order','orders'),
               ('parse_positions','positions_consistency'),('parse_snapshot','snapshot'))
        for index,(parser,stage) in enumerate(cases):
            with self.subTest(parser=parser):
                self.path=Path(self.temp.name)/('stage-'+str(index)+'.sqlite')
                with patch.object(collector,parser,side_effect=alpaca_schema.SchemaError('invalid_object')):
                    exc=self.failure()
                self.assertEqual((exc.schema_code,exc.schema_stage),('invalid_object',stage))
        self.path=Path(self.temp.name)/'owned-lookup-stage.sqlite'
        self.view=OwnedLedgerView(FP,1,('demo-entry',))
        with patch.object(collector,'reconcile_order',side_effect=alpaca_schema.SchemaError('order_regression')):
            exc=self.failure()
        self.assertEqual((exc.schema_code,exc.schema_stage),('order_regression','owned_lookup'))

    def test_invalid_stage_is_not_stringified_or_hashed(self):
        class Unsafe(str):
            def __hash__(self):raise AssertionError('subclass hash forbidden')
        for stage in ('PRIVATE_SECRET',['quotes'],{'quotes':1},Unsafe('quotes'),None):
            result=collector.schema_failure_detail('stale_or_future',stage)
            self.assertEqual(result,dict(schema_code='stale_or_future',schema_stage='unknown_stage'))

    def test_worker_reports_safe_detail_without_recovering_or_granting_permission(self):
        import finite_pilot_worker as worker
        exc = collector.CollectionRefused('native_schema_refused', schema_code='empty_quote_size', schema_stage='quotes')
        result = worker.failure('initialize', exc)
        self.assertEqual((result['error_code'], result['schema_code'], result['schema_stage']),
            ('native_schema_refused','empty_quote_size','quotes'))
        self.assertEqual(result['phase'],'manual_handoff')
        self.assertFalse(result['execution_ready'])
        self.assertFalse(result['historical_cause_proven'])
        self.assertEqual(result['owned_exposure'],'not_proven_flat')

    def test_worker_sanitizes_tampered_schema_metadata_and_direct_error(self):
        import finite_pilot_worker as worker
        exc = collector.CollectionRefused('native_schema_refused')
        exc.schema_code=['PRIVATE_SECRET'];exc.schema_stage={'PRIVATE_SECRET':1}
        result=worker.failure('initialize',exc)
        self.assertEqual((result['schema_code'],result['schema_stage']),('unknown_schema','unknown_stage'))
        self.assertNotIn('PRIVATE_SECRET',json.dumps(result))
        result=worker.failure('initialize',alpaca_schema.SchemaError('stale_or_future'))
        self.assertEqual((result['schema_code'],result['schema_stage']),('stale_or_future','outside_collector'))
        self.assertNotIn('schema_code',worker.failure('initialize',TimeoutError('PRIVATE_SECRET')))

    def test_allowlist_covers_only_reviewed_parser_literal_codes(self):
        import execution_session_policy
        actual = set()
        tree = ast.parse(Path(alpaca_schema.__file__).read_text() + '\n' + Path(execution_session_policy.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id=='require' and len(node.args)>1 and isinstance(node.args[1], ast.Constant):
                    actual.add(node.args[1].value)
                if node.func.id=='SchemaError' and node.args and isinstance(node.args[0], ast.Constant):
                    actual.add(node.args[0].value)
        self.assertEqual(collector.SCHEMA_FAILURE_CODES, actual)

    def test_consumed_diagnostic_is_not_silently_reauthorized_for_modified_code(self):
        import native_schema_diagnostic as diagnostic
        with self.assertRaisesRegex(ValueError, 'diagnostic_refused'):
            diagnostic.code_hash()


if __name__=='__main__': unittest.main()
