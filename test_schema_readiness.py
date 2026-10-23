"""Socket-denied synthetic schema hardening tests; no live compatibility claim."""
import copy
from decimal import Decimal, Inexact, ROUND_CEILING, localcontext
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from alpaca_schema import (Order, SchemaError, number, parse_order, parse_latest_quotes,
                           reconcile_order, timestamp)
from native_observation_collector import (CollectionRefused, NativeObservationCollector,
                                         OwnedLedgerView)
from test_native_observation_collector import F, FP, NOW, Clock, FixtureWire


class SchemaReadinessTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            p = patch(target, side_effect=AssertionError('NETWORK FORBIDDEN'))
            p.start(); self.addCleanup(p.stop)

    def test_numeric_bound_independent_of_ambient_precision(self):
        with localcontext() as context:
            context.prec = 4
            for value in ('1000000000000000.1', '-1000000000000000.1'):
                with self.subTest(value=value), self.assertRaisesRegex(SchemaError, 'invalid_decimal_range'):
                    number(value, nonnegative=False)
            self.assertEqual(number('1000000000000000'), Decimal('1000000000000000'))

    def test_spread_bound_independent_of_ambient_precision(self):
        raw = copy.deepcopy(F['quote'])
        raw['quote'].update(bp=Decimal('200.19'), ap=Decimal('202.195'))
        with localcontext() as context:
            context.prec = 4
            with self.assertRaisesRegex(SchemaError, 'invalid_quote_spread'):
                parse_latest_quotes(raw, symbols=('AAPL',), feed='iex', now_ns=NOW)

    def test_exact_spread_boundary_survives_hostile_ambient_context(self):
        raw = copy.deepcopy(F['quote'])
        raw['quote'].update(bp=Decimal('200.19'), ap=Decimal('202.1919'))
        with localcontext() as context:
            context.prec = 1
            context.rounding = ROUND_CEILING
            context.Emax = 1
            context.Emin = -1
            context.traps[Inexact] = True
            q = parse_latest_quotes(raw, symbols=('AAPL',), feed='iex', now_ns=NOW)[0]
        self.assertEqual(q.ask, Decimal('202.1919'))

    def test_all_seven_native_timestamps_retained_exactly(self):
        raw = copy.deepcopy(F['orders'][0])
        raw.update(filled_at='2026-10-19T13:30:30.123456789Z',
                   expired_at='2026-10-19T13:30:40.123456789Z',
                   failed_at='2026-10-19T13:30:50.123456789Z')
        order = parse_order(raw)
        for field in ('created', 'submitted', 'updated', 'filled', 'canceled', 'expired', 'failed'):
            with self.subTest(field=field):
                self.assertEqual(getattr(order, field + '_ns'), timestamp(raw[field + '_at']))
        # This retains the previous parser policy; it does not add status/event semantics.

    def test_terminal_fill_timestamp_mutation_is_conflicting(self):
        raw = copy.deepcopy(F['orders'][0])
        raw.update(status='filled', filled_qty='1', canceled_at=None, filled_at='2026-10-19T13:31:00Z')
        previous = parse_order(raw)
        raw['filled_at'] = '2026-10-19T13:31:00.000000001Z'
        with self.assertRaisesRegex(SchemaError, 'conflicting_same_timestamp'):
            reconcile_order(previous, parse_order(raw))

    def test_collector_rejects_changed_terminal_event_in_lookup(self):
        wire, clock = FixtureWire(), Clock()
        changed = copy.deepcopy(wire.f['orders'][0])
        changed['canceled_at'] = '2026-10-19T13:31:00Z'
        wire.overrides['/v2/orders:by_client_order_id?client_order_id=demo-entry'] = (200, changed)
        with tempfile.TemporaryDirectory() as root:
            collector = NativeObservationCollector(database=Path(root) / 'native.sqlite', transport=wire,
                expected_fingerprint=FP, ledger_view=lambda: OwnedLedgerView(FP, 1, ('demo-entry',)),
                enabled=True, now_ns=clock.now, monotonic_ns=clock.monotonic)
            with self.assertRaisesRegex(CollectionRefused, 'native_schema_refused'):
                collector.collect()

    def test_created_timestamp_is_immutable_order_identity(self):
        raw = copy.deepcopy(F['orders'][0])
        previous = parse_order(raw)
        raw['created_at'] = '2026-10-19T13:29:59Z'
        with self.assertRaisesRegex(SchemaError, 'order_identity_mutated'):
            reconcile_order(previous, parse_order(raw))

    def test_optional_terminal_timestamp_removal_is_conflicting(self):
        raw = copy.deepcopy(F['orders'][0])
        previous = parse_order(raw)
        raw['canceled_at'] = None
        with self.assertRaisesRegex(SchemaError, 'conflicting_same_timestamp'):
            reconcile_order(previous, parse_order(raw))

    def test_optional_timestamp_none_is_preserved_not_invented(self):
        order = parse_order(F['orders'][0])
        self.assertIsNone(order.filled_ns)
        self.assertIsNone(order.expired_ns)
        self.assertIsNone(order.failed_ns)

    def test_equivalent_timestamp_text_is_same_canonical_order(self):
        raw = copy.deepcopy(F['orders'][0])
        previous = parse_order(raw)
        raw['canceled_at'] = '2026-10-19T09:31:01-04:00'
        self.assertEqual(reconcile_order(previous, parse_order(raw)), previous)

    def test_legitimate_working_to_terminal_transition_still_allowed(self):
        raw = copy.deepcopy(F['orders'][0])
        raw.update(status='pending_cancel', canceled_at=None, updated_at='2026-10-19T13:31:00Z')
        previous = parse_order(raw)
        raw.update(status='canceled', canceled_at='2026-10-19T13:31:01Z', updated_at='2026-10-19T13:31:01Z')
        current = parse_order(raw)
        self.assertEqual(reconcile_order(previous, current), current)

    def test_existing_positional_order_constructor_remains_available(self):
        native = parse_order(F['orders'][0])
        old = Order(native.id, native.client_order_id, native.symbol, native.side,
                    native.quantity, native.filled, native.average_price, native.status,
                    native.submitted_ns, native.updated_ns, native.limit_price)
        self.assertEqual(old.quantity, native.quantity)
        self.assertIsNone(old.created_ns)
        # Missing historical timestamps never get silently adopted from new evidence.
        with self.assertRaisesRegex(SchemaError, 'order_identity_mutated'):
            reconcile_order(old, native)

    def test_required_timestamps_and_sequence_not_relaxed(self):
        for key in ('created_at', 'submitted_at', 'updated_at'):
            raw = copy.deepcopy(F['orders'][0]); raw[key] = None
            with self.subTest(key=key), self.assertRaisesRegex(SchemaError, 'invalid_timestamp'):
                parse_order(raw)
        raw = copy.deepcopy(F['orders'][0])
        raw['canceled_at'] = '2026-10-19T13:31:01.000000001Z'
        with self.assertRaisesRegex(SchemaError, 'order_event_timestamp_order'):
            parse_order(raw)

if __name__ == '__main__': unittest.main()
