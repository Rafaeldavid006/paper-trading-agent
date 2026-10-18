"""Offline tests for the fixed one-share Paper experiment.

Every provider and quote is injected; no real transport is instantiated, no
credentials are read, and these tests cannot submit any broker orders.
"""
import copy
import fcntl
import hashlib
import os
import sqlite3
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from paper_adapter import SafetyError
from paper_experiment import EXPERIMENT_ID, FINAL, OneShotExperiment
from paper_readonly import ReadOnlyPaperConnection
from test_paper_adapter import MockPaper


class ExperimentPaper(MockPaper):
    """Existing broker fixture plus the clock field needed by the experiment."""
    def __init__(self, now):
        super().__init__(now)
        self.clock_age = 0
        self.next_open = datetime(2026, 10, 20, 13, 30, tzinfo=timezone.utc)
        self.cancel_timeout = False
        self.asset['fractionable'] = True

    def call(self, method, path, body=None):
        result = super().call(method, path, body)
        if method == 'DELETE' and self.cancel_timeout:
            raise TimeoutError('private-cancel-detail')
        if path == '/v2/clock':
            code, clock = result
            clock.update(timestamp=(self.now() - timedelta(seconds=self.clock_age)).isoformat(),
                         next_open=self.next_open.isoformat(),
                         next_close=self.now().replace(hour=20, minute=0, second=0, microsecond=0).isoformat())
            return code, clock
        return result


class MockQuotes:
    def __init__(self, now):
        self.now = now
        self.bid = Decimal('199.99')
        self.ask = Decimal('200.01')
        self.age = 0
        self.failure = None
        self.calls = 0

    def latest_quote(self):
        self.calls += 1
        if self.failure:
            raise self.failure
        return SimpleNamespace(bid=self.bid, ask=self.ask,
                               timestamp=self.now() - timedelta(seconds=self.age))


class OneShotExperimentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = datetime(2026, 10, 18, 20, tzinfo=timezone.utc)
        self.database = self.tmp.name + '/paper.sqlite'
        self.transport = ExperimentPaper(lambda: self.now)
        self.quotes = MockQuotes(lambda: self.now)
        self.connection = ReadOnlyPaperConnection(self.database, self.transport, now=lambda: self.now)
        self.assertTrue(self.connection.check_connection()['check_ok'])
        self.now = datetime(2026, 10, 19, 15, tzinfo=timezone.utc)
        self.experiment = self.make()
        self.transport.calls.clear()
        # An accidental use of a production transport fails before network I/O.
        self.addCleanup(patch.stopall)
        patch('paper_adapter.PaperHTTP.call', side_effect=AssertionError('Real HTTP is forbidden in tests')).start()

    def make(self):
        return OneShotExperiment(self.database, self.transport, self.quotes, now=lambda: self.now)

    def calls(self, method):
        return [call for call in self.transport.calls if call[0] == method]

    def orders(self, side):
        return [order for order in self.transport.orders.values() if order['side'] == side]

    def position(self, qty='1', symbol='AAPL', value=None):
        return dict(symbol=symbol, asset_class='us_equity', side='long', qty=qty,
                    market_value=value or str(Decimal(qty) * Decimal('200')))

    def fill_buy(self, qty='1', status='filled'):
        order = self.orders('buy')[0]
        order.update(status=status, filled_qty=qty)
        self.transport.positions = [] if Decimal(qty) == 0 else [self.position(qty)]
        return order

    def buy(self):
        self.assertEqual('buy_working', self.experiment.tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))
        return self.orders('buy')[0]

    def assert_terminal_without_retry(self, phase):
        self.assertIn(phase, FINAL)
        before = copy.deepcopy(self.transport.calls)
        for _ in range(3):
            self.assertEqual(phase, self.make().tick()['phase'])
        self.assertEqual(before, self.transport.calls)

    def assert_blocked(self, reason=None):
        result = self.experiment.tick()
        self.assertEqual('blocked', result['phase'])
        if reason is not None:
            self.assertEqual(reason, result['reason'])
        self.assertEqual([], self.calls('POST'))
        self.assertEqual([], self.calls('DELETE'))
        self.assert_terminal_without_retry('blocked')
        return result

    def test_exact_one_share_buy_then_exact_filled_sell_and_terminal(self):
        buy = self.buy()
        self.assertEqual(dict(symbol='AAPL', side='buy', qty='1', type='limit', limit_price='200.01',
                              time_in_force='day', extended_hours=False),
                         {k: buy[k] for k in ('symbol', 'side', 'qty', 'type', 'limit_price',
                                             'time_in_force', 'extended_hours')})
        self.assertEqual([], self.orders('sell'))
        self.fill_buy()
        self.assertEqual('sell_working', self.make().tick()['phase'])
        sell = self.orders('sell')[0]
        self.assertEqual('1', sell['qty'])
        self.assertEqual('199.99', sell['limit_price'])
        self.assertEqual('AAPL', sell['symbol'])
        self.assertFalse(sell['extended_hours'])
        self.assertNotEqual(buy['client_order_id'], sell['client_order_id'])
        sell.update(status='filled', filled_qty='1')
        self.transport.positions = []
        result = self.make().tick()
        self.assertEqual(('completed', 'roundtrip_filled'), (result['phase'], result['reason']))
        self.assertEqual(2, len(self.calls('POST')))
        self.assert_terminal_without_retry('completed')

    def test_new_order_restart_never_posts_duplicate(self):
        self.buy()
        for _ in range(3):
            self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))
        self.assertEqual(1, len(self.transport.orders))

    def test_sell_restart_never_posts_duplicate(self):
        self.buy()
        self.fill_buy()
        self.assertEqual('sell_working', self.experiment.tick()['phase'])
        for _ in range(3):
            self.assertEqual('sell_working', self.make().tick()['phase'])
        self.assertEqual(2, len(self.calls('POST')))

    def test_submit_timeout_does_not_retry_even_if_order_was_accepted(self):
        self.transport.timeout = True
        result = self.experiment.tick()
        self.assertEqual('buy_working', result['phase'])
        self.assertEqual(1, len(self.transport.orders))
        self.assertEqual(1, len(self.calls('POST')))
        self.assertNotIn('fixture', json.dumps(result))
        self.transport.timeout = False
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))

    def test_submit_timeout_and_not_found_never_blindly_resubmits(self):
        self.transport.timeout = True
        self.experiment.tick()
        self.transport.hidden = True
        self.transport.timeout = False
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.now += timedelta(seconds=180)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assert_terminal_without_retry('manual_attention')
        self.assertEqual(1, len(self.calls('POST')))

    def test_crash_after_durable_reservation_never_posts_on_restart(self):
        original = self.experiment._guard
        count = 0
        def crash_after_reservation(c):
            nonlocal count
            count += 1
            if count == 2:
                raise KeyboardInterrupt('simulated process exit')
            return original(c)
        with patch.object(self.experiment, '_guard', side_effect=crash_after_reservation):
            with self.assertRaises(KeyboardInterrupt):
                self.experiment.tick()
        self.assertEqual([], self.calls('POST'))
        result = self.make().tick()
        self.assertEqual('buy_working', result['phase'])
        self.assertEqual('order_unresolved', result['reason'])
        self.now += timedelta(seconds=180)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assert_terminal_without_retry('manual_attention')
        self.assertEqual([], self.calls('POST'))

    def test_readonly_stop_prevents_all_provider_calls(self):
        self.connection.stop()
        result = self.experiment.tick()
        self.assertEqual(('stopped', 'stopped'), (result['phase'], result['reason']))
        self.assertEqual([], self.transport.calls)
        self.assert_terminal_without_retry('stopped')

    def test_adapter_stop_latch_prevents_writes(self):
        with self.experiment.db() as c:
            c.execute('UPDATE safety SET stopped=1 WHERE id=1')
        self.assertEqual('stopped', self.experiment.tick()['phase'])
        self.assertEqual([], self.transport.calls)

    def test_stop_after_buy_never_initiates_sell_or_cancel(self):
        self.buy()
        self.fill_buy()
        self.connection.stop()
        before = copy.deepcopy(self.transport.calls)
        self.assertEqual('stopped', self.make().tick()['phase'])
        self.assertEqual(before, self.transport.calls)
        self.assertEqual([], self.orders('sell'))
        self.assertEqual([], self.calls('DELETE'))

    def test_account_identity_change_blocks_before_order(self):
        self.transport.account['id'] = 'different-fixture-account'
        self.assert_blocked('identity_changed')

    def test_stored_fingerprint_change_blocks_before_provider_call(self):
        with self.experiment.db() as c:
            c.execute('UPDATE paper_readonly SET fingerprint=? WHERE id=1', ('0' * 64,))
        self.assert_blocked('identity_changed')
        self.assertEqual([], self.transport.calls)

    def test_approval_binding_change_blocks(self):
        with self.experiment.db() as c:
            c.execute("UPDATE paper_experiment SET approval='changed' WHERE id=1")
        self.assert_blocked('identity_changed')

    def test_missing_database_or_missing_fingerprint_rejected(self):
        with self.assertRaises(SafetyError):
            OneShotExperiment(self.tmp.name + '/missing.sqlite', self.transport, self.quotes)
        other = self.tmp.name + '/unverified.sqlite'
        ReadOnlyPaperConnection(other, self.transport, now=lambda: self.now)
        with self.assertRaises(SafetyError):
            OneShotExperiment(other, self.transport, self.quotes, now=lambda: self.now)
        self.assertEqual([], self.transport.calls)

    def test_database_symlink_rejected(self):
        link = Path(self.tmp.name) / 'linked.sqlite'
        link.symlink_to(self.database)
        with self.assertRaises(SafetyError):
            OneShotExperiment(str(link), self.transport, self.quotes)

    def test_existing_open_order_blocks(self):
        self.transport.orders['external'] = dict(client_order_id='external', status='new', symbol='MSFT',
                                                  side='buy', qty='1', filled_qty='0')
        self.assert_blocked('existing_orders')

    def test_existing_aapl_holding_blocks(self):
        self.transport.positions = [self.position()]
        self.assert_blocked('entry_blocked')

    def test_unavailable_quote_blocks_and_sanitizes_error(self):
        self.quotes.failure = RuntimeError('private-provider-secret')
        result = self.assert_blocked()
        self.assertNotIn('private-provider-secret', json.dumps(result))

    def test_missing_quote_blocks(self):
        with patch.object(self.quotes, 'latest_quote', return_value=None):
            self.assert_blocked()

    def test_stale_quote_blocks(self):
        self.quotes.age = 11
        self.assert_blocked('stale_quote')

    def test_future_quote_blocks(self):
        self.quotes.age = -1
        self.assert_blocked('stale_quote')

    def test_buy_price_above_absolute_cap_blocks(self):
        self.quotes.bid, self.quotes.ask = Decimal('1000.00'), Decimal('1000.01')
        self.assert_blocked('buy_cap')

    def test_buy_uses_cash_not_margin_buying_power(self):
        self.transport.account.update(cash='199.99', buying_power='1000000')
        self.assert_blocked('entry_blocked')

    def test_per_symbol_exposure_cap_blocks(self):
        self.transport.positions = [self.position(qty='30', symbol='MSFT', value='5000.01')]
        self.assert_blocked('exposure_limit')

    def test_total_exposure_cap_includes_proposed_entry(self):
        self.transport.account.update(equity='30000', last_equity='30000')
        self.transport.positions = [self.position(qty='20', symbol=s, value='5000')
                                    for s in ('MSFT', 'IBM', 'GOOG', 'AMZN')]
        self.assert_blocked('exposure_limit')

    def test_daily_loss_latch_blocks(self):
        self.transport.account['equity'] = '19000'
        self.assert_blocked('daily_loss_stop')
        with self.experiment.db() as c:
            self.assertEqual(1, c.execute('SELECT COUNT(*) FROM loss_days').fetchone()[0])

    def test_closed_clock_waits_without_quote_or_order_then_enters_once(self):
        self.transport.market_open = False
        result = self.experiment.tick()
        self.assertEqual(('waiting', 'waiting_regular_session'), (result['phase'], result['reason']))
        self.assertEqual(self.transport.next_open.isoformat(), result['next_open'])
        self.assertEqual(0, self.quotes.calls)
        self.assertEqual([], self.calls('POST'))
        self.transport.market_open = True
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))

    def test_broker_open_flag_cannot_override_regular_hours(self):
        self.now = datetime(2026, 10, 19, 20, 1, tzinfo=timezone.utc)
        result = self.experiment.tick()
        self.assertEqual('waiting_regular_session', result['reason'])
        self.assertEqual([], self.calls('POST'))
        self.assertEqual(0, self.quotes.calls)

    def test_weekend_never_enters_even_with_open_flag(self):
        self.now = datetime(2026, 10, 18, 15, tzinfo=timezone.utc)
        self.assertEqual('waiting_regular_session', self.experiment.tick()['reason'])
        self.assertEqual([], self.calls('POST'))

    def test_stale_clock_blocks(self):
        self.transport.clock_age = 11
        self.assert_blocked('stale_clock')

    def test_active_partial_fill_never_sells_before_entry_terminal(self):
        self.buy()
        self.fill_buy(qty='0.4', status='partially_filled')
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual([], self.orders('sell'))
        self.assertEqual(1, len(self.calls('POST')))

    def test_canceled_partial_fill_sells_exact_filled_quantity_only(self):
        self.buy()
        self.fill_buy(qty='0.4', status='canceled')
        result = self.make().tick()
        self.assertEqual('sell_working', result['phase'])
        self.assertEqual('0.4', self.orders('sell')[0]['qty'])
        self.orders('sell')[0].update(status='filled', filled_qty='0.4')
        self.transport.positions = []
        self.assertEqual('completed', self.make().tick()['phase'])
        self.assertEqual(2, len(self.calls('POST')))

    def test_partial_fill_nonfractionable_exit_requires_attention(self):
        self.buy()
        self.fill_buy(qty='0.4', status='canceled')
        self.transport.asset['fractionable'] = False
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'fractional_exit_unavailable'),
                         (result['phase'], result['reason']))
        self.assertEqual([], self.orders('sell'))

    def test_exit_never_exceeds_verified_holding(self):
        self.buy()
        self.fill_buy()
        self.transport.positions = [self.position('0.5')]
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'holding_mismatch'), (result['phase'], result['reason']))
        self.assertEqual([], self.orders('sell'))

    def test_canceled_zero_fill_is_terminal_no_fill(self):
        self.buy()
        self.fill_buy(qty='0', status='canceled')
        result = self.make().tick()
        self.assertEqual(('no_fill', 'entry_not_filled'), (result['phase'], result['reason']))
        self.assertEqual([], self.orders('sell'))
        self.assert_terminal_without_retry('no_fill')

    def test_partial_exit_terminal_never_reposts_remainder(self):
        self.buy()
        self.fill_buy()
        self.experiment.tick()
        self.orders('sell')[0].update(status='expired', filled_qty='0.4')
        self.transport.positions = [self.position('0.6')]
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'remaining_holding'), (result['phase'], result['reason']))
        self.assertEqual(2, len(self.calls('POST')))
        self.assert_terminal_without_retry('manual_attention')

    def test_invalid_full_fill_quantity_does_not_trigger_exit(self):
        self.buy()
        self.fill_buy(qty='0.5', status='filled')
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual([], self.orders('sell'))
        self.now += timedelta(seconds=180)
        self.assertEqual('manual_attention', self.make().tick()['phase'])

    def test_broker_payload_identity_mismatch_does_not_trigger_exit(self):
        self.buy()
        order = self.fill_buy()
        order['symbol'] = 'MSFT'
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.now += timedelta(seconds=180)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assertEqual([], self.orders('sell'))

    def test_cancel_at_timeout_once_then_wait_for_confirmed_terminal(self):
        buy = self.buy()
        self.now += timedelta(seconds=60)
        result = self.make().tick()
        self.assertEqual(('buy_working', 'cancel_requested'), (result['phase'], result['reason']))
        self.assertEqual([('DELETE', '/v2/orders/' + buy['id'], None)], self.calls('DELETE'))
        buy.update(status='canceled', filled_qty='0')
        self.assertEqual('no_fill', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('DELETE')))
        self.assertEqual(1, len(self.calls('POST')))

    def test_cancel_unconfirmed_never_retries_delete(self):
        self.buy()
        self.now += timedelta(seconds=60)
        self.experiment.tick()
        result = self.make().tick()
        self.assertEqual(('buy_working', 'cancel_unconfirmed'), (result['phase'], result['reason']))
        self.assertEqual(1, len(self.calls('DELETE')))
        self.now += timedelta(seconds=120)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assert_terminal_without_retry('manual_attention')

    def test_cancel_timeout_reconciles_without_retry_then_requires_attention(self):
        self.buy()
        self.now += timedelta(seconds=60)
        self.transport.cancel_timeout = True
        result = self.make().tick()
        self.assertEqual('buy_working', result['phase'])
        self.assertNotIn('private-cancel-detail', json.dumps(result))
        self.assertEqual(1, len(self.calls('DELETE')))
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.now += timedelta(seconds=120)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assert_terminal_without_retry('manual_attention')

    def test_cancel_http_failure_is_not_reported_as_requested(self):
        self.buy()
        self.now += timedelta(seconds=60)
        self.transport.cancel_status = 500
        result = self.make().tick()
        self.assertEqual('buy_working', result['phase'])
        self.assertNotEqual('cancel_requested', result['reason'])
        self.assertEqual(1, len(self.calls('DELETE')))
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.now += timedelta(seconds=120)
        self.assertEqual('manual_attention', self.make().tick()['phase'])
        self.assert_terminal_without_retry('manual_attention')

    def test_outside_session_reconciles_but_does_not_cancel(self):
        self.buy()
        self.now += timedelta(hours=6)
        self.transport.market_open = False
        result = self.make().tick()
        self.assertEqual('waiting_regular_session', result['reason'])
        self.assertEqual([], self.calls('DELETE'))
        self.assertEqual(1, len(self.calls('POST')))
        self.assertTrue(any(path.startswith('/v2/orders:') for _, path, _ in self.calls('GET')))

    def test_new_identity_observed_after_approval_cutoff_cannot_arm(self):
        other = self.tmp.name + '/new-account.sqlite'
        connection = ReadOnlyPaperConnection(other, self.transport, now=lambda: self.now)
        self.assertTrue(connection.check_connection()['check_ok'])
        before = copy.deepcopy(self.transport.calls)
        with self.assertRaises(SafetyError):
            OneShotExperiment(other, self.transport, self.quotes, now=lambda: self.now)
        self.assertEqual(before, self.transport.calls)
        self.assertEqual([], self.calls('POST'))

    def test_original_identity_observation_survives_later_readonly_recheck(self):
        with self.connection.db() as c:
            first = c.execute('SELECT identity_first_seen FROM paper_readonly').fetchone()[0]
        self.assertTrue(self.connection.check_connection()['check_ok'])
        with self.connection.db() as c:
            self.assertEqual(first, c.execute('SELECT identity_first_seen FROM paper_readonly').fetchone()[0])
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))

    def test_prior_buy_history_blocks_fresh_local_experiment(self):
        cid = 'pa-' + hashlib.sha256((EXPERIMENT_ID + ':buy').encode()).hexdigest()[:40]
        self.transport.orders[cid] = dict(client_order_id=cid, symbol='AAPL', side='buy',
                                          qty='1', filled_qty='1', status='filled')
        self.assert_blocked('prior_experiment_or_uncertain')

    def test_prior_sell_history_blocks_fresh_local_experiment(self):
        cid = 'pa-' + hashlib.sha256((EXPERIMENT_ID + ':sell').encode()).hexdigest()[:40]
        self.transport.orders[cid] = dict(client_order_id=cid, symbol='AAPL', side='sell',
                                          qty='1', filled_qty='1', status='filled')
        self.assert_blocked('prior_experiment_or_uncertain')

    def test_uncertain_history_lookup_cannot_authorize_first_buy(self):
        original = self.transport.call
        def failed_lookup(method, path, body=None):
            result = original(method, path, body)
            return (500, None) if path.startswith('/v2/orders:') else result
        with patch.object(self.transport, 'call', side_effect=failed_lookup):
            self.assert_blocked('prior_experiment_or_uncertain')

    def test_completed_requires_flat_aapl_position(self):
        self.buy()
        self.fill_buy()
        self.experiment.tick()
        self.orders('sell')[0].update(status='filled', filled_qty='1')
        # A stale/inconsistent position snapshot cannot be called a success.
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'holding_mismatch'), (result['phase'], result['reason']))
        self.assertEqual(2, len(self.calls('POST')))

    def test_completed_requires_no_open_orders(self):
        self.buy()
        self.fill_buy()
        self.experiment.tick()
        self.orders('sell')[0].update(status='filled', filled_qty='1')
        self.transport.positions = []
        self.transport.orders['external'] = dict(client_order_id='external', status='new', symbol='MSFT',
                                                  side='buy', qty='1', filled_qty='0')
        result = self.make().tick()
        self.assertEqual(('manual_attention', 'holding_mismatch'), (result['phase'], result['reason']))
        self.assertEqual(2, len(self.calls('POST')))

    def test_last_quote_movement_blocks_reserved_submission(self):
        original = self.quotes.latest_quote
        count = 0
        def moving_quote():
            nonlocal count
            count += 1
            quote = original()
            if count >= 2:
                quote.ask = Decimal('250')
            return quote
        with patch.object(self.quotes, 'latest_quote', side_effect=moving_quote):
            result = self.experiment.tick()
        self.assertEqual(('manual_attention', 'quote_moved'), (result['phase'], result['reason']))
        self.assertEqual([], self.calls('POST'))
        self.assert_terminal_without_retry('manual_attention')

    def test_stale_final_quote_blocks_reserved_submission(self):
        original = self.quotes.latest_quote
        count = 0
        def stale_final_quote():
            nonlocal count
            count += 1
            quote = original()
            if count >= 2:
                quote.timestamp -= timedelta(seconds=11)
            return quote
        with patch.object(self.quotes, 'latest_quote', side_effect=stale_final_quote):
            result = self.experiment.tick()
        self.assertEqual(('manual_attention', 'stale_quote'), (result['phase'], result['reason']))
        self.assertEqual([], self.calls('POST'))

    def test_stop_between_reservation_and_post_prevents_order(self):
        original = self.experiment._guard
        count = 0
        def stop_after_reservation(c):
            nonlocal count
            count += 1
            if count == 2:
                c.execute('UPDATE paper_readonly SET stopped=1 WHERE id=1')
            return original(c)
        with patch.object(self.experiment, '_guard', side_effect=stop_after_reservation):
            self.assertEqual('stopped', self.experiment.tick()['phase'])
        self.assertEqual([], self.calls('POST'))
        self.assert_terminal_without_retry('stopped')

    def test_interprocess_lock_denies_parallel_tick_without_provider_calls(self):
        descriptor = os.open(self.database + '.experiment.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            before = self.experiment.status()
            self.assertEqual(before, self.make().tick())
            self.assertEqual([], self.transport.calls)
        finally:
            os.close(descriptor)
        self.assertEqual('buy_working', self.make().tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))

    def legacy_readonly_database(self, name, checked_at):
        database = self.tmp.name + '/' + name + '.sqlite'
        with sqlite3.connect(database) as c:
            c.execute("""CREATE TABLE paper_readonly (
                id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL DEFAULT 0,
                fingerprint TEXT, masked_identity TEXT, checked_at REAL,
                ok INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT 'not_checked',
                snapshot TEXT NOT NULL DEFAULT '{}', stopped INTEGER NOT NULL DEFAULT 0)""")
            fingerprint = hashlib.sha256(self.transport.account['id'].encode()).hexdigest()
            c.execute('INSERT INTO paper_readonly(id,fingerprint,checked_at) VALUES(1,?,?)',
                      (fingerprint, checked_at))
        return database

    def test_legacy_readonly_migration_preserves_preapproval_observation_before_refresh(self):
        before_approval = datetime(2026, 10, 18, 20, tzinfo=timezone.utc).timestamp()
        database = self.legacy_readonly_database('legacy', before_approval)
        connection = ReadOnlyPaperConnection(database, self.transport, now=lambda: self.now)
        with connection.db() as c:
            row = c.execute('SELECT identity_first_seen,checked_at FROM paper_readonly').fetchone()
            self.assertEqual((before_approval, before_approval), tuple(row))
        self.assertTrue(connection.check_connection()['check_ok'])
        with connection.db() as c:
            row = c.execute('SELECT identity_first_seen,checked_at FROM paper_readonly').fetchone()
            self.assertEqual((before_approval, self.now.timestamp()), tuple(row))
        experiment = OneShotExperiment(database, self.transport, self.quotes, now=lambda: self.now)
        self.assertEqual('buy_working', experiment.tick()['phase'])
        self.assertEqual(1, len(self.calls('POST')))

    def test_invalid_or_missing_legacy_observation_never_arms_even_after_refresh(self):
        for index, bad in enumerate((None, 'not-a-timestamp', 0, -1, float('inf'),
                                     self.now.timestamp() + 86400)):
            with self.subTest(timestamp=bad):
                database = self.legacy_readonly_database('invalid-' + str(index), bad)
                connection = ReadOnlyPaperConnection(database, self.transport, now=lambda: self.now)
                with self.assertRaises(SafetyError):
                    OneShotExperiment(database, self.transport, self.quotes, now=lambda: self.now)
                self.assertTrue(connection.check_connection()['check_ok'])
                with self.assertRaises(SafetyError):
                    OneShotExperiment(database, self.transport, self.quotes, now=lambda: self.now)
        self.assertEqual([], self.calls('POST'))

    def test_quote_that_ages_during_slow_snapshots_is_rejected_before_post(self):
        initial_quote = self.quotes.latest_quote()
        original = self.transport.call
        asset_calls = 0
        def slow_snapshot(method, path, body=None):
            nonlocal asset_calls
            result = original(method, path, body)
            if path == '/v2/assets/AAPL':
                asset_calls += 1
                if asset_calls == 1:
                    self.now += timedelta(seconds=11)
            return result
        with patch.object(self.quotes, 'latest_quote', return_value=initial_quote), \
                patch.object(self.transport, 'call', side_effect=slow_snapshot):
            result = self.experiment.tick()
        self.assertEqual(('manual_attention', 'stale_quote'), (result['phase'], result['reason']))
        self.assertEqual([], self.calls('POST'))

    def test_clock_that_ages_during_final_snapshot_blocks_even_with_fresh_quote(self):
        original = self.transport.call
        asset_calls = 0
        def slow_final_snapshot(method, path, body=None):
            nonlocal asset_calls
            result = original(method, path, body)
            if path == '/v2/assets/AAPL':
                asset_calls += 1
                if asset_calls == 2:
                    self.now += timedelta(seconds=11)
            return result
        with patch.object(self.transport, 'call', side_effect=slow_final_snapshot):
            result = self.experiment.tick()
        self.assertEqual(('manual_attention', 'stale_clock'), (result['phase'], result['reason']))
        self.assertEqual([], self.calls('POST'))

    def test_session_closes_during_final_snapshot_prevents_post(self):
        self.now = datetime(2026, 10, 19, 19, 59, 59, tzinfo=timezone.utc)
        original = self.transport.call
        asset_calls = 0
        def close_during_snapshot(method, path, body=None):
            nonlocal asset_calls
            result = original(method, path, body)
            if path == '/v2/assets/AAPL':
                asset_calls += 1
                if asset_calls == 2:
                    self.now += timedelta(seconds=2)
            return result
        with patch.object(self.transport, 'call', side_effect=close_during_snapshot):
            result = self.experiment.tick()
        self.assertEqual(('manual_attention', 'stale_clock'), (result['phase'], result['reason']))
        self.assertEqual([], self.calls('POST'))

    def test_status_contains_only_safe_local_summary(self):
        self.buy()
        self.transport.calls.clear()
        result = self.experiment.status()
        self.assertEqual({'phase', 'reason', 'next_open', 'updated'}, set(result))
        self.assertEqual([], self.transport.calls)
        for secret in ('fixture-account', 'broker-', 'client_order_id', 'fingerprint', 'approval'):
            self.assertNotIn(secret, json.dumps(result))


if __name__ == '__main__':
    unittest.main()
