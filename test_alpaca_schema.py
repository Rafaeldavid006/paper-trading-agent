import copy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import unittest
from alpaca_schema import *

FIX=Path(__file__).parent/'fixtures'/'alpaca-native.json'
F=json.loads(FIX.read_text()) if FIX.exists() else {}

class NativeSchemaTests(unittest.TestCase):
    def setUp(self):
        self.f=copy.deepcopy(F)
        self.fp=hashlib.sha256(self.f['account']['id'].encode()).hexdigest()
        self.now=timestamp('2026-10-19T13:31:02Z')
    def snapshot(self,**kw):
        args=dict(origin=PAPER_ORIGIN,expected_fingerprint=self.fp,response_account_fingerprint=self.fp,observed_ns=self.now,now_ns=self.now,collection_complete=True)
        args.update(kw)
        return parse_snapshot(self.f['account'],self.f['clock'],self.f['positions'],self.f['orders'],**args)
    def intent(self):
        return (OwnedIntent(self.fp,'demo-entry','AAPL','buy',Decimal('1')),)
    def test_native_schema_and_fraction(self):
        s=self.snapshot(); self.assertEqual(s.orders[0].filled,Decimal('0.125000001'))
        self.assertEqual(owned_exit_quantity(s,intents=self.intent(),symbol='AAPL',now_ns=self.now),Decimal('0.125000001'))
    def test_no_paper_field_in_native_account(self):
        self.assertNotIn('paper',self.f['account']); self.snapshot()
    def test_account_binding(self):
        with self.assertRaises(SchemaError): self.snapshot(expected_fingerprint='bad')
    def test_response_binding(self):
        with self.assertRaises(SchemaError): self.snapshot(response_account_fingerprint='bad')
    def test_live_origin(self):
        with self.assertRaises(SchemaError): self.snapshot(origin='https://api.alpaca.markets')
    def test_missing_block_flags(self):
        del self.f['account']['trade_suspended_by_user']
        with self.assertRaises(SchemaError): self.snapshot()
    def test_blocked(self):
        self.f['account']['trading_blocked']=True
        with self.assertRaises(SchemaError): self.snapshot()
    def test_unknown_completeness(self):
        with self.assertRaises(SchemaError): self.snapshot(collection_complete=None)
    def test_truncated_page(self):
        self.f['orders']*=500
        with self.assertRaises(SchemaError): self.snapshot()
    def test_duplicate_order(self):
        self.f['orders']*=2
        with self.assertRaises(SchemaError): self.snapshot()
    def test_stale_snapshot(self):
        with self.assertRaises(SchemaError): self.snapshot(now_ns=self.now+11*10**9)
    def test_pending_cancel_not_terminal(self):
        self.f['orders'][0]['status']='pending_cancel'
        s=self.snapshot()
        with self.assertRaises(SchemaError): owned_exit_quantity(s,intents=self.intent(),symbol='AAPL',now_ns=self.now)
    def test_partial_working_not_exit(self):
        self.f['orders'][0]['status']='partially_filled'
        with self.assertRaises(SchemaError): owned_exit_quantity(self.snapshot(),intents=self.intent(),symbol='AAPL',now_ns=self.now)
    def test_unknown_status(self):
        self.f['orders'][0]['status']='pending_replace'
        with self.assertRaises(SchemaError): self.snapshot()
    def test_filled_status_requires_full(self):
        self.f['orders'][0]['status']='filled'
        with self.assertRaises(SchemaError): self.snapshot()
    def test_filled_requires_timestamp(self):
        self.f['orders'][0].update(status='filled',filled_qty='1')
        with self.assertRaises(SchemaError): self.snapshot()
    def test_new_with_fill_rejected(self):
        self.f['orders'][0]['status']='new'
        with self.assertRaises(SchemaError): self.snapshot()
    def test_bad_decimal_inputs(self):
        for v in (True,0.1,'NaN','Infinity','1e2','0.0000000001'):
            with self.subTest(v=v),self.assertRaises(SchemaError): number(v,quantity=True)
    def test_overfill(self):
        self.f['orders'][0]['filled_qty']='1.000000001'
        with self.assertRaises(SchemaError): self.snapshot()
    def test_missing_average(self):
        self.f['orders'][0]['filled_avg_price']=None
        with self.assertRaises(SchemaError): self.snapshot()
    def test_unknown_available(self):
        del self.f['positions'][0]['qty_available']
        with self.assertRaises(SchemaError): self.snapshot()
    def test_mixed_external_holdings(self):
        self.f['positions'][0]['qty']='1.125000001'
        with self.assertRaises(SchemaError): owned_exit_quantity(self.snapshot(),intents=self.intent(),symbol='AAPL',now_ns=self.now)
    def test_missing_owned_order(self):
        self.f['orders']=[]
        with self.assertRaises(SchemaError): owned_exit_quantity(self.snapshot(),intents=self.intent(),symbol='AAPL',now_ns=self.now)
    def test_intent_account(self):
        i=(OwnedIntent('bad','demo-entry','AAPL','buy',Decimal(1)),)
        with self.assertRaises(SchemaError): owned_exit_quantity(self.snapshot(),intents=i,symbol='AAPL',now_ns=self.now)
    def test_unrelated_position_untouched(self):
        p=copy.deepcopy(self.f['positions'][0]);p['symbol']='MSFT';self.f['positions'].append(p)
        self.assertEqual(owned_exit_quantity(self.snapshot(),intents=self.intent(),symbol='AAPL',now_ns=self.now),Decimal('0.125000001'))
    def test_partial_exit_subtracts_exact(self):
        o=copy.deepcopy(self.f['orders'][0]);o.update(id='exit-id',client_order_id='demo-exit',side='sell',qty='0.125000001',filled_qty='0.025000001');self.f['orders'].append(o)
        self.f['positions'][0].update(qty='0.1',qty_available='0.1')
        intents=self.intent()+(OwnedIntent(self.fp,'demo-exit','AAPL','sell',Decimal('0.125000001')),)
        self.assertEqual(owned_exit_quantity(self.snapshot(),intents=intents,symbol='AAPL',now_ns=self.now),Decimal('0.1'))
    def test_timestamp_ns_exact(self):
        self.assertEqual(timestamp('2026-10-19T13:31:00.123456789Z')-timestamp('2026-10-19T09:31:00-04:00'),123456789)
    def test_bad_timestamp(self):
        for v in ('2026-10-19T13:31:00','2026-99-19T13:31:00Z',None):
            with self.assertRaises(SchemaError): timestamp(v)
    def test_future_one_nanosecond(self):
        with self.assertRaises(SchemaError): fresh(self.now+1,self.now)
    def test_quote_native_single(self):
        q=parse_latest_quotes(self.f['quote'],symbols=('AAPL',),feed='iex',now_ns=self.now)[0]
        self.assertEqual(q.bid,Decimal('200.1')); self.assertEqual(q.timestamp_ns,timestamp('2026-10-19T13:31:01.123456789Z'))
    def test_quote_native_multi(self):
        raw={'quotes':{'AAPL':self.f['quote']['quote']}}
        self.assertEqual(len(parse_latest_quotes(raw,symbols=('AAPL',),feed='sip',now_ns=self.now)),1)
    def test_missing_quote_symbol(self):
        with self.assertRaises(SchemaError): parse_latest_quotes(self.f['quote'],symbols=('MSFT',),feed='iex',now_ns=self.now)
    def test_stale_quote(self):
        with self.assertRaises(SchemaError): parse_latest_quotes(self.f['quote'],symbols=('AAPL',),feed='iex',now_ns=self.now+20*10**9)
    def test_empty_bid(self):
        self.f['quote']['quote']['bs']=0
        with self.assertRaises(SchemaError): parse_latest_quotes(self.f['quote'],symbols=('AAPL',),feed='iex',now_ns=self.now)
    def test_crossed_quote(self):
        self.f['quote']['quote']['ap']=199
        with self.assertRaises(SchemaError): parse_latest_quotes(self.f['quote'],symbols=('AAPL',),feed='iex',now_ns=self.now)
    def test_terminal_immutable(self):
        a=parse_order(self.f['orders'][0]); self.f['orders'][0]['updated_at']='2026-10-19T13:31:02Z';b=parse_order(self.f['orders'][0])
        with self.assertRaises(SchemaError): reconcile_order(a,b)
    def test_order_idempotent(self):
        a=parse_order(self.f['orders'][0]); self.assertEqual(reconcile_order(a,a),a)
    def test_order_regression(self):
        self.f['orders'][0]['status']='pending_cancel';a=parse_order(self.f['orders'][0]);self.f['orders'][0]['filled_qty']='0.1';b=parse_order(self.f['orders'][0])
        with self.assertRaises(SchemaError): reconcile_order(a,b)
    def test_replacement_fails_closed(self):
        self.f['orders'][0]['replaced_by']='other-id'
        with self.assertRaises(SchemaError): self.snapshot()
    def test_buy_fill_beyond_limit(self):
        self.f['orders'][0]['filled_avg_price']='201'
        with self.assertRaises(SchemaError): self.snapshot()
    def test_sell_fill_below_limit(self):
        self.f['orders'][0].update(side='sell',filled_avg_price='199')
        with self.assertRaises(SchemaError): self.snapshot()
    def test_invalid_offset_range(self):
        for offset in ('+00:99','+24:00','-01:60','-99:00'):
            with self.subTest(offset=offset),self.assertRaises(SchemaError): timestamp('2026-10-19T13:31:00'+offset)
    def test_short_rejected(self):
        self.f['positions'][0]['side']='short'
        with self.assertRaises(SchemaError): self.snapshot()

if __name__=='__main__': unittest.main()
