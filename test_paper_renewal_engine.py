"""Offline regression tests for the one separately approved Paper renewal.

An actual original-engine quote-moved/no-transmission fixture supplies the prior
evidence. Provider calls remain in-memory and production HTTP is forbidden.
"""
import copy
import fcntl
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from paper_adapter import SafetyError
from paper_experiment import EXPERIMENT_ID as ORIGINAL_ID, OneShotExperiment
from paper_readonly import ReadOnlyPaperConnection
from paper_renewal import EXPERIMENT_ID, FINAL, RenewalExperiment
from paper_renewal_gate import client_id
from test_paper_experiment import ExperimentPaper, MockQuotes


class RenewalEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.http_guard = patch('paper_adapter.PaperHTTP.call',
                                side_effect=AssertionError('Real HTTP is forbidden in tests'))
        self.http_guard.start()
        self.addCleanup(self.http_guard.stop)
        self.now = datetime(2026, 10, 18, 20, tzinfo=timezone.utc)
        self.database = self.tmp.name + '/paper.sqlite'
        self.transport = ExperimentPaper(lambda: self.now)
        self.quotes = MockQuotes(lambda: self.now)
        self.connection = ReadOnlyPaperConnection(self.database, self.transport, now=lambda: self.now)
        self.assertTrue(self.connection.check_connection()['check_ok'])
        self.now = datetime(2026, 10, 19, 15, tzinfo=timezone.utc)
        self.original = OneShotExperiment(self.database, self.transport, self.quotes, now=lambda: self.now)
        first, moved = self.quotes.latest_quote(), self.quotes.latest_quote()
        moved.ask = Decimal('201.00')
        with patch.object(self.quotes, 'latest_quote', side_effect=[first, moved]):
            result = self.original.tick()
        self.assertEqual(('manual_attention', 'quote_moved'), (result['phase'], result['reason']))
        self.assertEqual([], self.writes())
        self.old_bytes = self.original_bytes()
        self.now = datetime(2026, 10, 19, 17, 15, tzinfo=timezone.utc)
        self.experiment = self.make()
        self.transport.calls.clear()
        self.quotes.calls = 0

    def make(self):
        return RenewalExperiment(self.database, self.transport, self.quotes, now=lambda: self.now)

    def calls(self, method):
        return [call for call in self.transport.calls if call[0] == method]

    def writes(self):
        return [call for call in self.transport.calls if call[0] != 'GET']

    def original_bytes(self):
        with self.connection.db() as c:
            result = {}
            for table in ('paper_experiment', 'intents'):
                result[table] = [dict(row) for row in c.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
            result['payload_bytes'] = [row[0].hex() for row in c.execute('SELECT CAST(payload AS BLOB) FROM intents ORDER BY client_id')]
        return json.dumps(result, sort_keys=True, separators=(',', ':')).encode('utf-8')

    def assert_original_unchanged(self):
        self.assertEqual(self.old_bytes, self.original_bytes())

    def orders(self, side):
        cid = client_id(EXPERIMENT_ID, side)
        return [] if cid not in self.transport.orders else [self.transport.orders[cid]]

    def position(self, qty='1', symbol='AAPL', value=None):
        return dict(symbol=symbol, asset_class='us_equity', side='long', qty=qty,
                    market_value=value or str(Decimal(qty) * Decimal('200')))

    def buy(self):
        result = self.experiment.tick()
        self.assertEqual('buy_working', result['phase'], result)
        self.assertEqual(1, len(self.calls('POST')))
        self.assert_original_unchanged()
        return self.orders('buy')[0]

    def fill_buy(self, qty='1', status='filled'):
        order = self.orders('buy')[0]
        order.update(status=status, filled_qty=qty)
        self.transport.positions = [] if Decimal(qty) == 0 else [self.position(qty)]
        return order

    def finish_roundtrip(self):
        self.buy()
        self.fill_buy()
        result = self.make().tick()
        self.assertEqual('sell_working', result['phase'], result)
        self.orders('sell')[0].update(status='filled', filled_qty='1')
        self.transport.positions = []
        result = self.make().tick()
        self.assertEqual(('completed', 'roundtrip_filled'), (result['phase'], result['reason']))
        return result

    def assert_final_no_retry(self, phase):
        self.assertIn(phase, FINAL)
        before = copy.deepcopy(self.transport.calls)
        for _ in range(3):
            self.assertEqual(phase, self.make().tick()['phase'])
        self.assertEqual(before, self.transport.calls)
        self.assert_original_unchanged()

    def assert_no_entry(self, phase='blocked', reason=None):
        result = self.experiment.tick()
        self.assertEqual(phase, result['phase'], result)
        if reason is not None:
            self.assertEqual(reason, result['reason'])
        self.assertEqual([], self.writes())
        self.assert_original_unchanged()
        return result

    def test_exact_renewal_roundtrip_preserves_original_rows_and_payload_bytes(self):
        self.finish_roundtrip()
        buy, sell = self.orders('buy')[0], self.orders('sell')[0]
        for order in (buy, sell):
            self.assertEqual('AAPL', order['symbol'])
            self.assertEqual('1', order['qty'])
            self.assertEqual('limit', order['type'])
            self.assertEqual('day', order['time_in_force'])
            self.assertIs(False, order['extended_hours'])
        self.assertEqual('200.01', buy['limit_price'])
        self.assertEqual('199.99', sell['limit_price'])
        self.assertEqual(2, len(self.calls('POST')))
        self.assertEqual([], self.calls('DELETE'))
        self.assert_original_unchanged()
        with self.connection.db() as c:
            self.assertEqual(1, c.execute('SELECT COUNT(*) FROM intents').fetchone()[0])
            self.assertEqual(2, c.execute('SELECT COUNT(*) FROM renewal_intents').fetchone()[0])
            self.assertEqual(('reserved', None, '0'), tuple(c.execute('SELECT state,broker_id,filled FROM intents').fetchone()))
        self.assert_final_no_retry('completed')

    def test_new_generation_ids_are_fixed_and_distinct_from_original(self):
        buy = self.buy()
        self.assertEqual(client_id(EXPERIMENT_ID, 'buy'), buy['client_order_id'])
        self.assertNotEqual(client_id(ORIGINAL_ID, 'buy'), buy['client_order_id'])
        self.fill_buy()
        self.make().tick()
        sell = self.orders('sell')[0]
        self.assertEqual(client_id(EXPERIMENT_ID, 'sell'), sell['client_order_id'])
        self.assertNotEqual(client_id(ORIGINAL_ID, 'sell'), sell['client_order_id'])
        self.assertNotEqual(buy['client_order_id'], sell['client_order_id'])

    def test_pending_buy_restart_cannot_create_second_entry(self):
        self.buy()
        for _ in range(4):
            self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))
        self.assert_original_unchanged()

    def test_pending_sell_restart_cannot_create_third_order(self):
        self.buy()
        self.fill_buy()
        self.experiment.tick()
        for _ in range(4):
            self.assertEqual('sell_working', self.make().tick()['phase'])
        self.assertEqual(2, len(self.calls('POST')))
        self.assert_original_unchanged()

    def test_completed_renewal_never_enters_again_on_later_day(self):
        self.finish_roundtrip()
        self.now += timedelta(days=1)
        self.assert_final_no_retry('completed')
        self.assertEqual(2, len(self.calls('POST')))

    def test_post_timeout_reconciles_accepted_order_without_resubmission(self):
        self.transport.timeout = True
        self.assertEqual('buy_working', self.experiment.tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))
        self.transport.timeout = False
        for _ in range(3):
            self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))
        self.assert_original_unchanged()

    def test_post_timeout_with_unknown_order_never_creates_second_entry(self):
        self.transport.timeout = True
        self.experiment.tick()
        self.transport.timeout = False
        self.transport.hidden = True
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.now += timedelta(seconds=180)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assert_final_no_retry('manual_attention')
        self.assertEqual(1, len(self.calls('POST')))

    def test_process_crash_after_new_reservation_never_posts_on_restart(self):
        original_guard = self.experiment._guard
        count = 0
        def crash(c):
            nonlocal count
            count += 1
            if count == 2:
                raise KeyboardInterrupt('mock process crash')
            return original_guard(c)
        with patch.object(self.experiment, '_guard', side_effect=crash):
            with self.assertRaises(KeyboardInterrupt):
                self.experiment.tick()
        self.assertEqual([], self.writes())
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.now += timedelta(seconds=180)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assert_final_no_retry('manual_attention')
        self.assertEqual([], self.writes())

    def test_active_partial_entry_never_sells_before_entry_terminal(self):
        self.buy()
        self.fill_buy('0.4', 'partially_filled')
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual([], self.orders('sell'))
        self.assertEqual(1, len(self.calls('POST')))

    def test_canceled_partial_entry_sells_exact_confirmed_quantity(self):
        self.buy()
        self.fill_buy('0.4', 'canceled')
        self.assertEqual('sell_working', self.make().tick()['phase'])
        self.assertEqual('0.4', self.orders('sell')[0]['qty'])
        self.orders('sell')[0].update(status='filled', filled_qty='0.4')
        self.transport.positions = []
        self.assertEqual('completed', self.make().tick()['phase'])
        self.assertEqual(2, len(self.calls('POST')))
        self.assert_final_no_retry('completed')

    def test_nonfractionable_partial_exit_never_sells_rounded_quantity(self):
        self.buy()
        self.fill_buy('0.4', 'canceled')
        self.transport.asset['fractionable'] = False
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'fractional_exit_unavailable'), (result['phase'], result['reason']))
        self.assertEqual([], self.orders('sell'))
        self.assert_original_unchanged()

    def test_zero_fill_terminal_cannot_start_second_entry(self):
        self.buy()
        self.fill_buy('0', 'canceled')
        self.assertEqual('no_fill', self.make().tick()['phase'])
        self.assert_final_no_retry('no_fill')
        self.assertEqual(1, len(self.calls('POST')))

    def test_incomplete_terminal_exit_does_not_sell_remainder_again(self):
        self.buy()
        self.fill_buy()
        self.make().tick()
        self.orders('sell')[0].update(status='canceled', filled_qty='0.4')
        self.transport.positions = [self.position('0.6')]
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'remaining_holding'), (result['phase'], result['reason']))
        self.assert_final_no_retry('manual_attention')
        self.assertEqual(2, len(self.calls('POST')))

    def test_sale_never_exceeds_verified_holdings(self):
        self.buy()
        self.fill_buy()
        self.transport.positions = [self.position('0.5')]
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'holding_mismatch'), (result['phase'], result['reason']))
        self.assertEqual([], self.orders('sell'))

    def test_each_original_client_id_present_blocks_renewal(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                cid = client_id(ORIGINAL_ID, side)
                self.transport.orders[cid] = dict(client_order_id=cid, symbol='AAPL', side=side,
                                                   qty='1', filled_qty='1', status='filled')
                self.assert_no_entry()
                self.transport.orders.clear()
                # Reset only NEW test-run status so both ID checks exercise the gate.
                with self.connection.db() as c:
                    c.execute("UPDATE paper_renewal SET phase='waiting',reason='waiting_clock' WHERE id=1")

    def test_original_lookup_unknown_response_blocks_renewal(self):
        original_call = self.transport.call
        original_ids = {client_id(ORIGINAL_ID, side) for side in ('buy', 'sell')}
        def unknown(method, path, body=None):
            result = original_call(method, path, body)
            if path.startswith('/v2/orders:') and path.split('=')[1] in original_ids:
                return 500, None
            return result
        with patch.object(self.transport, 'call', side_effect=unknown):
            self.assert_no_entry()
        self.assert_final_no_retry('blocked')

    def test_original_lookup_timeout_blocks_without_leaking_provider_text(self):
        original_call = self.transport.call
        def timeout(method, path, body=None):
            result = original_call(method, path, body)
            if path.endswith(client_id(ORIGINAL_ID, 'buy')):
                raise TimeoutError('secret-provider-detail')
            return result
        with patch.object(self.transport, 'call', side_effect=timeout):
            result = self.assert_no_entry()
        self.assertNotIn('secret-provider-detail', json.dumps(result))

    def test_original_order_appearing_between_reservation_and_post_blocks(self):
        original_call = self.transport.call
        count = 0
        def race(method, path, body=None):
            nonlocal count
            result = original_call(method, path, body)
            if path.endswith(client_id(ORIGINAL_ID, 'buy')):
                count += 1
                if count >= 2:
                    return 200, {'status': 'filled'}
            return result
        with patch.object(self.transport, 'call', side_effect=race):
            result = self.experiment.tick()
        self.assertNotEqual('completed', result['phase'])
        self.assertEqual([], self.writes())
        self.assertEqual(2, count)
        self.assert_original_unchanged()

    def test_new_generation_buy_history_fence_prevents_duplicate(self):
        cid = client_id(EXPERIMENT_ID, 'buy')
        self.transport.orders[cid] = dict(client_order_id=cid, symbol='AAPL', side='buy',
                                           qty='1', filled_qty='1', status='filled')
        self.assert_no_entry(reason='prior_experiment_or_uncertain')
        self.assert_final_no_retry('blocked')

    def test_new_generation_sell_history_fence_prevents_duplicate(self):
        cid = client_id(EXPERIMENT_ID, 'sell')
        self.transport.orders[cid] = dict(client_order_id=cid, symbol='AAPL', side='sell',
                                           qty='1', filled_qty='1', status='filled')
        self.assert_no_entry(reason='prior_experiment_or_uncertain')

    def test_uncertain_new_generation_history_never_authorizes_buy(self):
        original_call = self.transport.call
        def unknown(method, path, body=None):
            result = original_call(method, path, body)
            return (503, None) if path.endswith(client_id(EXPERIMENT_ID, 'buy')) else result
        with patch.object(self.transport, 'call', side_effect=unknown):
            self.assert_no_entry(reason='prior_experiment_or_uncertain')

    def test_stop_before_entry_is_stopped_and_does_not_contact_provider(self):
        self.connection.stop()
        self.assert_no_entry(phase='stopped', reason='stopped')
        self.assertEqual([], self.transport.calls)

    def test_stop_after_buy_prevents_sell_and_cancel(self):
        self.buy()
        self.fill_buy()
        self.connection.stop()
        before = copy.deepcopy(self.transport.calls)
        result = self.experiment.tick()
        self.assertEqual(('stopped', 'stopped'), (result['phase'], result['reason']))
        self.assertEqual(before, self.transport.calls)
        self.assertEqual([], self.orders('sell'))
        self.assertEqual([], self.calls('DELETE'))
        self.assert_original_unchanged()

    def test_shared_adapter_stop_latch_prevents_entry(self):
        with self.connection.db() as c:
            c.execute('UPDATE safety SET stopped=1 WHERE id=1')
        self.assert_no_entry(phase='stopped', reason='stopped')
        self.assertEqual([], self.transport.calls)

    def test_shared_original_lock_blocks_renewal_tick(self):
        descriptor = os.open(self.database + '.experiment.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            status = self.experiment.status()
            self.assertEqual(status, self.experiment.tick())
            self.assertEqual([], self.transport.calls)
            self.assert_original_unchanged()
        finally:
            os.close(descriptor)
        self.buy()

    def test_still_same_point_one_percent_gate_rejects_larger_quote_move(self):
        first = self.quotes.latest_quote()
        moved = copy.deepcopy(first)
        moved.ask = first.ask + first.ask * Decimal('.001') + Decimal('.00001')
        with patch.object(self.quotes, 'latest_quote', side_effect=[first, moved]):
            self.assert_no_entry(phase='manual_attention', reason='quote_moved')
        self.assert_final_no_retry('manual_attention')

    def test_point_one_percent_quote_boundary_is_not_tightened_or_widened(self):
        first = self.quotes.latest_quote()
        boundary = copy.deepcopy(first)
        boundary.ask = first.ask + first.ask * Decimal('.001')
        with patch.object(self.quotes, 'latest_quote', side_effect=[first, boundary]):
            self.buy()
        self.assertEqual('200.01', self.orders('buy')[0]['limit_price'])

    def test_sell_uses_same_point_one_percent_quote_gate(self):
        self.buy()
        self.fill_buy()
        first = self.quotes.latest_quote()
        moved = copy.deepcopy(first)
        moved.bid = first.bid - first.bid * Decimal('.001') - Decimal('.00001')
        with patch.object(self.quotes, 'latest_quote', side_effect=[first, moved]):
            result = self.experiment.tick()
        self.assertEqual(('manual_attention', 'quote_moved'), (result['phase'], result['reason']))
        self.assertEqual([], self.orders('sell'))
        self.assertEqual(1, len(self.calls('POST')))
        self.assert_original_unchanged()

    def test_account_identity_mismatch_blocks_renewal(self):
        self.transport.account['id'] = 'other-account'
        self.assert_no_entry(reason='identity_changed')
        self.assert_final_no_retry('blocked')

    def test_existing_aapl_position_blocks_entry(self):
        self.transport.positions = [self.position()]
        self.assert_no_entry()

    def test_existing_unrelated_open_order_blocks_entry(self):
        self.transport.orders['external'] = dict(client_order_id='external', status='new', symbol='MSFT',
                                                  side='buy', qty='1', filled_qty='0')
        self.assert_no_entry()

    def test_renewal_has_same_absolute_one_thousand_dollar_cap(self):
        self.quotes.ask = Decimal('1000.01')
        self.quotes.bid = Decimal('1000.00')
        self.assert_no_entry(reason='buy_cap')

    def test_missing_quote_fails_closed_without_resetting_original(self):
        self.quotes.failure = RuntimeError('quote-unavailable-private')
        result = self.assert_no_entry()
        self.assertNotIn('quote-unavailable-private', json.dumps(result))

    def test_stale_final_quote_never_posts_reserved_buy(self):
        first = self.quotes.latest_quote()
        stale = copy.deepcopy(first)
        stale.timestamp -= timedelta(seconds=11)
        with patch.object(self.quotes, 'latest_quote', side_effect=[first, stale]):
            self.assert_no_entry(phase='manual_attention', reason='stale_quote')

    def test_closed_market_waits_without_quotes_or_orders(self):
        self.transport.market_open = False
        result = self.experiment.tick()
        self.assertEqual(('waiting', 'waiting_regular_session'), (result['phase'], result['reason']))
        self.assertEqual([], self.writes())
        self.assertEqual(0, self.quotes.calls)
        self.assert_original_unchanged()

    def test_cancel_uncertainty_never_retries_delete(self):
        self.buy()
        self.now += timedelta(seconds=60)
        self.transport.cancel_timeout = True
        self.assertEqual('buy_working', self.experiment.tick()['phase'])
        self.assertEqual(1, len(self.calls('DELETE')))
        self.transport.cancel_timeout = False
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.now += timedelta(seconds=120)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('DELETE')))
        self.assert_final_no_retry('manual_attention')

    def test_final_success_requires_flat_position(self):
        self.buy()
        self.fill_buy()
        self.experiment.tick()
        self.orders('sell')[0].update(status='filled', filled_qty='1')
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'holding_mismatch'), (result['phase'], result['reason']))
        self.assert_original_unchanged()

    def test_status_is_local_and_only_exposes_safe_summary(self):
        self.buy()
        self.transport.calls.clear()
        result = self.experiment.status()
        self.assertEqual({'phase', 'reason', 'next_open', 'updated'}, set(result))
        self.assertEqual([], self.transport.calls)
        for private in ('fixture-account', 'fingerprint', 'broker-', 'client_order_id', 'approval'):
            self.assertNotIn(private, json.dumps(result))

    def test_missing_original_ledger_cannot_construct_renewal(self):
        database = self.tmp.name + '/missing-original.sqlite'
        connection = ReadOnlyPaperConnection(database, self.transport, now=lambda: self.now)
        connection.check_connection()
        before = copy.deepcopy(self.transport.calls)
        with self.assertRaises((SafetyError, sqlite3.OperationalError)):
            RenewalExperiment(database, self.transport, self.quotes, now=lambda: self.now)
        self.assertEqual(before, self.transport.calls)
        self.assertEqual([], self.writes())

    def test_stop_after_reservation_before_post_is_durable_across_restart(self):
        original_guard = self.experiment._guard
        count = 0
        def stop_after_reservation(c):
            nonlocal count
            count += 1
            if count == 2:
                c.execute('UPDATE paper_readonly SET stopped=1 WHERE id=1')
            return original_guard(c)
        with patch.object(self.experiment, '_guard', side_effect=stop_after_reservation):
            self.assert_no_entry(phase='stopped', reason='stopped')
        self.assert_final_no_retry('stopped')
        self.assertEqual([], self.writes())

    def test_stopped_pending_renewal_remains_stopped_across_restart(self):
        self.buy()
        self.connection.stop()
        self.assertEqual('stopped', self.make().tick()['phase'])
        self.assert_final_no_retry('stopped')
        self.assertEqual(1, len(self.calls('POST')))
        self.assertEqual([], self.calls('DELETE'))

    def test_changed_original_evidence_blocks_without_rewriting_it(self):
        with self.connection.db() as c:
            c.execute("UPDATE paper_experiment SET reason='different-original-evidence' WHERE id=1")
        changed_bytes = self.original_bytes()
        result = self.experiment.tick()
        self.assertEqual('blocked', result['phase'])
        self.assertEqual([], self.writes())
        self.assertEqual(changed_bytes, self.original_bytes())

    def test_external_order_during_reservation_is_rechecked_before_post(self):
        original_guard = self.experiment._guard
        count = 0
        def external_order(c):
            nonlocal count
            count += 1
            if count == 2:
                self.transport.orders['external'] = dict(client_order_id='external', status='new',
                    symbol='MSFT', side='buy', qty='1', filled_qty='0')
            return original_guard(c)
        with patch.object(self.experiment, '_guard', side_effect=external_order):
            result = self.experiment.tick()
        self.assertNotEqual('completed', result['phase'])
        self.assertEqual([], self.writes())
        self.assert_original_unchanged()

    def test_rejected_submission_response_only_reconciles_and_never_reposts(self):
        original_call = self.transport.call
        def unavailable(method, path, body=None):
            result = original_call(method, path, body)
            return (503, None) if method == 'POST' else result
        with patch.object(self.transport, 'call', side_effect=unavailable):
            result = self.experiment.tick()
        self.assertEqual(('buy_working', 'submission_uncertain'), (result['phase'], result['reason']))
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))
        self.assert_original_unchanged()

    def test_symlink_database_cannot_construct_renewal(self):
        link = Path(self.tmp.name) / 'link.sqlite'
        link.symlink_to(self.database)
        with self.assertRaises(SafetyError):
            RenewalExperiment(str(link), self.transport, self.quotes, now=lambda: self.now)
        self.assertEqual([], self.transport.calls)


if __name__ == '__main__':
    unittest.main()
