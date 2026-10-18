import copy
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from paper_adapter import PaperAdapter, PaperHTTP, NoRedirect, Intent, SafetyError, PAPER_HOST


class MockPaper:
    def __init__(self, now):
        self.now = now
        self.calls = []
        self.orders = {}
        self.positions = []
        self.account = dict(id='fixture-account', status='ACTIVE', currency='USD',
                            trading_blocked=False, account_blocked=False,
                            trade_suspended_by_user=False, multiplier='1', short_market_value='0',
                            cash='20000', equity='20000', last_equity='20000', long_market_value='0')
        self.asset = dict(**{'class': 'us_equity'}, status='active', tradable=True)
        self.market_open = True
        self.timeout = False
        self.hidden = False
        self.cancel_status = 204

    def call(self, method, path, body=None):
        self.calls.append((method, path, copy.deepcopy(body)))
        if method == 'POST':
            order = dict(body, id='broker-' + str(len(self.orders)), status='new', filled_qty='0')
            self.orders[body['client_order_id']] = order
            if self.timeout:
                raise TimeoutError('fixture')
            return 200, copy.deepcopy(order)
        if method == 'DELETE':
            return self.cancel_status, None
        if path == '/v2/account': return 200, copy.deepcopy(self.account)
        if path == '/v2/positions': return 200, copy.deepcopy(self.positions)
        if path == '/v2/clock': return 200, dict(timestamp=self.now().isoformat(), is_open=self.market_open)
        if path.startswith('/v2/assets/'): return 200, copy.deepcopy(self.asset)
        if path.startswith('/v2/orders:'):
            order = self.orders.get(path.split('=')[1])
            return (404, None) if self.hidden or order is None else (200, copy.deepcopy(order))
        if path.startswith('/v2/orders?'):
            return 200, [copy.deepcopy(o) for o in self.orders.values() if o['status'] not in ('filled', 'canceled', 'rejected', 'expired')]
        raise AssertionError(path)


class PaperTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = datetime(2026, 10, 19, 15, tzinfo=timezone.utc)
        self.transport = MockPaper(lambda: self.now)
        self.adapter = self.make()
        state = self.adapter.check_connection()
        self.adapter.enable(revision=state['revision'])
        self.transport.calls.clear()

    def tearDown(self): self.tmp.cleanup()

    def make(self, **kwargs):
        args = dict(allowlist=['AAPL', 'MSFT', 'IBM', 'GOOG', 'AMZN'], enabled=True,
                    expected_account_id='fixture-account', now=lambda: self.now)
        args.update(kwargs)
        return PaperAdapter(self.tmp.name + '/paper.sqlite', self.transport, **args)

    def intent(self, **kwargs):
        args = dict(key='test-1', symbol='AAPL', side='buy', qty=10, limit_price='100.00')
        args.update(kwargs)
        return Intent(**args)

    def blocked(self, **kwargs):
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent(**kwargs))
        self.assertFalse(any(c[0] == 'POST' for c in self.transport.calls))

    def test_disabled_default_never_calls_transport(self):
        a = PaperAdapter(self.tmp.name + '/off.sqlite', self.transport)
        with self.assertRaises(SafetyError): a.submit(self.intent())
        self.assertEqual([], self.transport.calls)

    def test_read_only_preflight_while_disabled(self):
        self.make(enabled=False).preflight()
        self.assertTrue(all(c[0] == 'GET' for c in self.transport.calls))

    def test_idempotent_restart_and_changed_payload(self):
        result = self.adapter.submit(self.intent())
        self.assertEqual('new', result['state'])
        self.assertEqual(result, self.make().submit(self.intent()))
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent(qty=11))
        self.assertEqual(1, sum(c[0] == 'POST' for c in self.transport.calls))

    def test_timeout_reconciles_without_duplicate(self):
        self.transport.timeout = True
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent())
        self.assertEqual('uncertain', self.adapter.records()[0]['state'])
        self.assertEqual('new', self.make().submit(self.intent())['state'])
        self.assertEqual(1, sum(c[0] == 'POST' for c in self.transport.calls))

    def test_not_found_never_resubmits_and_blocks_other_intent(self):
        self.transport.timeout = True
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent())
        self.transport.hidden = True
        self.assertEqual('uncertain', self.make().submit(self.intent())['state'])
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent(key='another'))
        self.assertEqual(1, sum(c[0] == 'POST' for c in self.transport.calls))

    def test_partial_fill_reserves_full_quantity_and_no_short(self):
        self.adapter.submit(self.intent())
        order = next(iter(self.transport.orders.values()))
        order.update(status='partially_filled', filled_qty='4')
        self.transport.positions = [dict(symbol='AAPL', asset_class='us_equity', side='long', qty='4', market_value='400')]
        self.assertEqual('4', self.make().reconcile()[0]['filled'])
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent(key='oversize', qty=41))
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent(key='short', side='sell', qty=5))
        self.assertEqual('new', self.adapter.submit(self.intent(key='sell', side='sell', qty=4))['state'])

    def test_pending_sell_prevents_oversell(self):
        self.transport.positions = [dict(symbol='AAPL', asset_class='us_equity', side='long', qty='10', market_value='1000')]
        self.adapter.submit(self.intent(side='sell', qty=7))
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent(key='second', side='sell', qty=4))

    def test_limits(self):
        self.blocked(qty=51)
        self.transport.positions = [dict(symbol=s, asset_class='us_equity', side='long', qty='50', market_value='5000') for s in ('MSFT', 'IBM', 'GOOG', 'AMZN')]
        self.blocked()

    def test_existing_pending_buy_counts(self):
        self.adapter.submit(self.intent(qty=40))
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent(key='second', qty=11))
        self.assertEqual(1, sum(c[0] == 'POST' for c in self.transport.calls))

    def test_cash_not_buying_power(self):
        self.transport.account.update(cash='999', buying_power='999999')
        self.blocked()

    def test_daily_loss_latched_even_after_recovery(self):
        self.transport.account['equity'] = '19000'
        self.blocked()
        self.transport.account['equity'] = '20000'
        self.blocked()
        self.now += timedelta(days=1)
        self.assertEqual('new', self.adapter.submit(self.intent())['state'])

    def test_loss_blocks_sell_too(self):
        self.transport.account['equity'] = '19000'
        self.blocked(side='sell')

    def test_allowlist_and_structured_only(self):
        self.blocked(symbol='XYZ')
        with self.assertRaises(SafetyError): self.adapter.submit('buy AAPL because AI says so')

    def test_numeric_inputs(self):
        for changes in ({'qty':True}, {'qty':1.5}, {'qty':0}, {'limit_price':'NaN'}, {'limit_price':'Infinity'}, {'limit_price':'-1'}, {'limit_price':'0.99'}, {'limit_price':'1.001'}):
            self.blocked(**changes)

    def test_account_asset_and_market_checks(self):
        for key, bad in [('id','other'), ('multiplier','0'), ('short_market_value','100'), ('trading_blocked',True), ('currency','EUR'), ('cash','NaN')]:
            original = self.transport.account[key]
            self.transport.account[key] = bad
            self.blocked()
            self.transport.account[key] = original
        self.transport.asset['class'] = 'crypto'
        self.blocked()
        self.transport.asset['class'] = 'us_equity'
        self.transport.market_open = False
        self.blocked()

    def test_frequency_persisted_across_restart(self):
        self.adapter.submit(self.intent())
        self.make().submit(self.intent(key='second'))
        with self.assertRaises(SafetyError): self.make().submit(self.intent(key='third'))
        self.now += timedelta(seconds=61)
        self.assertEqual('new', self.make().submit(self.intent(key='third'))['state'])

    def test_kill_persistent_and_cancel_no_liquidation(self):
        self.adapter.submit(self.intent())
        result = self.adapter.stop(cancel_open=True)
        self.assertEqual('requested', result[0]['outcome'])
        self.assertEqual('new', self.adapter.records()[0]['state'])
        with self.assertRaises(SafetyError): self.make().submit(self.intent(key='second'))
        self.assertEqual(1, sum(c[0] == 'POST' for c in self.transport.calls))
        next(iter(self.transport.orders.values()))['status'] = 'canceled'
        self.assertEqual('canceled', self.make().reconcile()[0]['state'])

    def test_cancel_failure_is_audited_and_does_not_clear_stop(self):
        self.adapter.submit(self.intent())
        self.transport.cancel_status = 500
        self.assertEqual('uncertain', self.adapter.stop(cancel_open=True)[0]['outcome'])
        with self.adapter.db() as c:
            self.assertEqual('uncertain', c.execute('SELECT outcome FROM cancellation').fetchone()[0])
            self.assertEqual(1, c.execute('SELECT stopped FROM safety').fetchone()[0])

    def test_unknown_open_order_fail_closed(self):
        self.transport.orders['external'] = dict(client_order_id='external',status='new')
        self.blocked()

    def test_fill_regression_rejected(self):
        self.adapter.submit(self.intent())
        order = next(iter(self.transport.orders.values()))
        order.update(status='partially_filled', filled_qty='5')
        self.adapter.reconcile()
        order['filled_qty'] = '3'
        with self.assertRaises(SafetyError): self.adapter.reconcile()

    def test_terminal_order_does_not_get_reposted(self):
        self.adapter.submit(self.intent())
        order = next(iter(self.transport.orders.values()))
        order.update(status='filled', filled_qty='10')
        self.assertEqual('filled', self.adapter.reconcile()[0]['state'])
        self.assertEqual('filled', self.make().submit(self.intent())['state'])
        self.assertEqual(1, sum(c[0] == 'POST' for c in self.transport.calls))

    def test_fill_between_positions_and_orders_is_conservatively_reserved(self):
        self.adapter.submit(self.intent(qty=40))
        original = self.transport.call
        def racing(method, path, body=None):
            result = original(method, path, body)
            if path == '/v2/positions':
                next(iter(self.transport.orders.values())).update(status='partially_filled', filled_qty='30')
            return result
        self.transport.call = racing
        with self.assertRaises(SafetyError):
            self.adapter.submit(self.intent(key='racing-buy', qty=20))
        self.assertEqual(1, sum(c[0] == 'POST' for c in self.transport.calls))

    def test_sell_fill_between_snapshot_calls_cannot_oversell(self):
        self.transport.positions = [dict(symbol='AAPL', asset_class='us_equity', side='long', qty='50', market_value='5000')]
        self.adapter.submit(self.intent(qty=40, side='sell'))
        original = self.transport.call
        def racing(method, path, body=None):
            result = original(method, path, body)
            if path == '/v2/positions':
                next(iter(self.transport.orders.values())).update(status='partially_filled', filled_qty='30')
            return result
        self.transport.call = racing
        with self.assertRaises(SafetyError):
            self.adapter.submit(self.intent(key='racing-sell', qty=40, side='sell'))

    def test_stop_serializes_against_inflight_post(self):
        import threading
        entered, release, stopping, stopped = [threading.Event() for _ in range(4)]
        errors = []
        original = self.transport.call
        def delayed(method, path, body=None):
            if method == 'POST':
                entered.set()
                if not release.wait(3): raise AssertionError('test synchronization timeout')
            return original(method, path, body)
        self.transport.call = delayed
        def submit():
            try: self.adapter.submit(self.intent())
            except Exception as exc: errors.append(exc)
        def stop():
            try:
                stopping.set()
                self.make().stop(cancel_open=True)
                stopped.set()
            except Exception as exc: errors.append(exc)
        writer = threading.Thread(target=submit)
        stopper = threading.Thread(target=stop)
        writer.start()
        self.assertTrue(entered.wait(3))
        stopper.start()
        self.assertTrue(stopping.wait(3))
        self.assertFalse(stopped.wait(.05))
        release.set()
        writer.join(3); stopper.join(3)
        self.assertFalse(writer.is_alive() or stopper.is_alive())
        self.assertEqual([], errors)
        self.assertTrue(stopped.is_set())
        with self.assertRaises(SafetyError): self.make().submit(self.intent(key='after-stop'))
        methods = [c[0] for c in self.transport.calls]
        self.assertLess(methods.index('POST'), methods.index('DELETE'))

    def test_unknown_submission_is_visible_in_stop_outcomes(self):
        self.transport.timeout = True
        with self.assertRaises(SafetyError): self.adapter.submit(self.intent())
        self.transport.hidden = True
        self.assertEqual('unresolved', self.make().stop(cancel_open=True)[0]['outcome'])

    def test_http_fixed_host_endpoints_and_redirect(self):
        for host in ('https://api.alpaca.markets', PAPER_HOST+'/', PAPER_HOST+'.evil', 'http://paper-api.alpaca.markets'):
            with self.assertRaises(SafetyError): PaperHTTP('fixture','fixture',host)
        transport = PaperHTTP('fixture', 'fixture')
        with patch.object(transport._opener, 'open') as opener:
            for path in ('https://api.alpaca.markets/v2/account','//evil','/v2/account?redirect=x','/v2/positions/close'):
                with self.assertRaises(SafetyError): transport.call('GET',path)
            opener.assert_not_called()
        with self.assertRaises(SafetyError):
            NoRedirect().redirect_request(None,None,302,'',{},'https://api.alpaca.markets')


if __name__ == '__main__': unittest.main()
