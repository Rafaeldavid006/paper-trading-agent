"""Connected native PAPER route tests, with all sockets/DNS forbidden."""
import copy
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch,Mock
from urllib.parse import parse_qs,urlsplit

from active_demo_pipeline import DemoPipeline,Config,Event
from alpaca_schema import timestamp,PAPER_ORIGIN
from decimal_lifecycle import Lifecycle,SessionRiskPolicy,Proposal
from demo_tick_runner import DemoTickRunner,RunnerLedger,RunnerConfig
from native_account_guard import SharedAccountGuard
from native_observation_collector import ORDER_PATH,DATA_ORIGIN
from native_paper_execution import NativePaperDispatcher,NativePaperHTTP,DispatchRefused,bar_path
from native_paper_runner import NativePaperExecutionPort,NativeMarketSource,BoundedPaperRunner
from test_native_observation_worker import create_source,FP,F
import native_observation_worker as legacy


class Wire:
    paper_origin=PAPER_ORIGIN;data_origin=DATA_ORIGIN;feed='iex'
    def __init__(self,now):
        self.now=now;self.calls=[];self.positions=[];self.native=[];self.fail_post=False;self.fail_cancel=False
        self.bid='200.10';self.ask='200.20';self.fractionable=True;self.on_call=lambda *a:None
        self.account=copy.deepcopy(F['account']);self.legacy=[]
        for side in ('buy','sell'):
            o=copy.deepcopy(F['orders'][0]);o.update(id='broker-'+side,client_order_id=legacy._client_id(legacy.RENEWAL_ID,side),
                side=side,qty='1',filled_qty='1',filled_avg_price='200',limit_price='200',status='filled',
                created_at='2026-10-19T17:14:16Z',submitted_at='2026-10-19T17:14:16Z',updated_at='2026-10-19T17:15:00Z',
                filled_at='2026-10-19T17:15:00Z',canceled_at=None)
            self.legacy.append(o)
    def call(self,method,path,body=None):
        self.calls.append((method,path,copy.deepcopy(body)));self.on_call(method,path,body)
        at=self.now().isoformat()
        if method=='POST':
            if self.fail_post: raise TimeoutError('secret provider detail')
            o=copy.deepcopy(F['orders'][0]);o.update(id='native-'+body['client_order_id'],**body,status='new',filled_qty='0',
                filled_avg_price=None,created_at=at,submitted_at=at,updated_at=at,filled_at=None,canceled_at=None)
            self.native.append(o);return 201,copy.deepcopy(o)
        if method=='DELETE':
            if self.fail_cancel:raise TimeoutError('secret provider detail')
            o=next(o for o in self.native if '/v2/orders/'+o['id']==path)
            o.update(status='pending_cancel',updated_at=at);return 204,None
        if path=='/v2/account':return 200,copy.deepcopy(self.account)
        if path=='/v2/positions':return 200,copy.deepcopy(self.positions)
        if path=='/v2/clock':return 200,dict(timestamp=at,is_open=True,next_open='2026-10-21T13:30:00Z',next_close='2026-10-20T20:00:00Z')
        if path.startswith('/v2/calendar?'):return 200,[dict(date='2026-10-20',open='09:30',close='16:00')]
        if path==ORDER_PATH:return 200,sorted(copy.deepcopy(self.native+self.legacy),key=lambda o:o['submitted_at'],reverse=True)
        if path.startswith('/v2/orders:by_client_order_id?'):
            cid=path.split('=')[1];o=next((o for o in self.native+self.legacy if o['client_order_id']==cid),None)
            return (200,copy.deepcopy(o)) if o else (404,None)
        if path.startswith('/v2/assets/'):
            return 200,dict(symbol=path.rsplit('/',1)[1],status='active',tradable=True,fractionable=self.fractionable,**{'class':'us_equity'})
        if path.startswith('/v2/stocks/quotes/latest?'):
            symbols=parse_qs(urlsplit(path).query)['symbols'][0].split(',')
            return 200,{'quotes':{s:dict(t=at,bp=self.bid,ap=self.ask,bs=10,**{'as':10}) for s in symbols}}
        if path.startswith('/v2/stocks/bars?'):
            p=parse_qs(urlsplit(path).query);start=datetime.fromisoformat(p['start'][0]);end=datetime.fromisoformat(p['end'][0])+timedelta(microseconds=1)
            count=int((end-start).total_seconds()/60)
            return 200,dict(bars={s:[dict(t=(start+timedelta(minutes=i)).isoformat(),c=('100.5' if s!='SPY' else '100.1') if i==count-1 else '100') for i in range(count)] for s in p['symbols'][0].split(',')},next_page_token=None)
        raise AssertionError((method,path))


class NativeDispatchTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
            self.enterContext(patch(name,side_effect=AssertionError('no sockets')))
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.now=datetime(2026,10,20,13,36,1,tzinfo=timezone.utc)
        self.source=self.root/'legacy.sqlite';create_source(self.source)
        with sqlite3.connect(self.source) as db:db.execute('CREATE TABLE loss_days(day TEXT PRIMARY KEY,stopped INTEGER)')
        Path(str(self.source)+'.experiment.lock').touch(mode=0o600)
        self.lifecycle=Lifecycle(self.root/'native.sqlite',FP,session_risk_policy=SessionRiskPolicy(4,Decimal('100')))
        self.guard=SharedAccountGuard(self.source,self.lifecycle.path,FP,enabled=True,state_database=self.root/'guard.sqlite',test_only=True)
        self.wire=Wire(lambda:self.now)
        self.dispatcher=NativePaperDispatcher(self.lifecycle,self.guard,self.wire,enabled=True,now_ns=lambda:timestamp(self.now.isoformat()),monotonic_ns=lambda:100000000000)
        self.pipeline=DemoPipeline(self.root/'decision.sqlite',Config(enabled=True,allowlist=('AAPL',)))
        self.port=NativePaperExecutionPort(evaluator=self.pipeline,lifecycle=self.lifecycle,dispatcher=self.dispatcher)
        self.market=NativeMarketSource(self.port)
        self.event=Event('fixture-event','fixture-Q4','1','AAPL',self.now-timedelta(minutes=5,seconds=1),self.now-timedelta(minutes=5),
            'https://issuer.example/earnings','a'*64,verified=True)
        self.inner=DemoTickRunner(eventsource=self,marketsource=self.market,executionport=self.port,ledger=RunnerLedger(self.root/'runner.sqlite'),clock=lambda:self.now,config=RunnerConfig(enabled=True))
        self.runner=BoundedPaperRunner(self.inner,enabled=True)
    def events(self,now,limit):return [self.event]
    def event_eligible(self,event):return event==self.event
    def tick(self):return self.runner.tick()
    def advance(self):self.now+=timedelta(seconds=65)
    def row(self):return self.lifecycle.status()['intents'][-1]
    def fill(self,qty,status='filled',held=None):
        o=self.wire.native[-1];at=self.now.isoformat();o.update(status=status,filled_qty=qty,filled_avg_price=o['limit_price'] if Decimal(qty)>0 else None,
            updated_at=at,filled_at=at if status=='filled' else None,canceled_at=at if status=='canceled' else None)
        if held is None:held=qty if o['side']=='buy' else '0'
        self.wire.positions=[] if Decimal(held)==0 else [dict(F['positions'][0],qty=held,qty_available=held,market_value=str(Decimal(held)*Decimal(self.wire.bid)))]
    def methods(self,method):return [c for c in self.wire.calls if c[0]==method]
    def test_end_to_end_one_share_entry_exact_fractional_exit_then_flat(self):
        before=self.source.read_bytes()
        result=self.tick();self.assertEqual(result['phase'],'complete',result)
        self.assertEqual(len(self.methods('POST')),1);self.assertEqual(self.methods('POST')[0][2]['qty'],'1')
        self.advance();self.fill('0.125000001','canceled');self.wire.bid='205.00';self.wire.ask='205.10'
        result=self.tick();self.assertEqual(result['exits'],1,result)
        self.assertEqual(self.methods('POST')[-1][2]['qty'],'0.125000001')
        self.advance();self.fill('0.025000001','canceled',held='0.100000000')
        result=self.tick();self.assertEqual(result['exits'],1,result)
        self.assertEqual(Decimal(self.methods('POST')[-1][2]['qty']),Decimal('0.1'))
        self.advance();self.fill('0.100000000');self.assertEqual(self.tick()['phase'],'complete')
        self.assertEqual(len(self.methods('POST')),3);self.assertEqual(before,self.source.read_bytes())
        self.assertNotIn(F['account']['id'].encode(),Path(self.lifecycle.path).read_bytes())
    def test_default_dispatcher_and_runner_are_disabled(self):
        d=NativePaperDispatcher(self.lifecycle,self.guard,self.wire,now_ns=lambda:timestamp(self.now.isoformat()))
        with self.assertRaisesRegex(DispatchRefused,'disabled'):
            with d.tick_scope():pass
        self.assertEqual(BoundedPaperRunner(self.inner).tick()['phase'],'disabled');self.assertEqual(self.wire.calls,[])
    def test_no_scope_blocks_direct_dispatch(self):
        with self.assertRaises(Exception):self.dispatcher.submit('unknown')
        self.assertEqual(self.wire.calls,[])
    def test_uncertain_post_never_replayed_even_after_restart(self):
        self.wire.fail_post=True;result=self.tick();self.assertEqual(result['phase'],'manual_attention')
        self.assertEqual(len(self.methods('POST')),1)
        d=NativePaperDispatcher(self.lifecycle,self.guard,self.wire,enabled=True,now_ns=lambda:timestamp(self.now.isoformat()))
        with self.assertRaises(Exception):
            with d.tick_scope():d.submit(self.row()['cid'])
        self.assertEqual(len(self.methods('POST')),1)
        with sqlite3.connect(self.lifecycle.path) as db:self.assertEqual(db.execute('SELECT state FROM native_dispatch').fetchone()[0],'uncertain')
    def test_success_receipt_not_replayed(self):
        self.tick();self.advance()
        with self.dispatcher.tick_scope():
            self.dispatcher.observe()
            with self.assertRaises(Exception):self.dispatcher.submit(self.row()['cid'])
        self.assertEqual(len(self.methods('POST')),1)
    def test_cancel_request_is_not_terminal_and_no_second_delete(self):
        self.assertEqual(self.tick()['phase'],'complete');self.advance()
        result=self.tick();self.assertEqual(len(self.methods('DELETE')),1,result)
        self.assertEqual(self.wire.native[0]['status'],'pending_cancel')
        self.advance();self.assertEqual(self.tick()['phase'],'blocked');self.assertEqual(len(self.methods('DELETE')),1)
        self.assertEqual(len(self.methods('POST')),1)
    def test_cancel_timeout_latches_without_replay(self):
        self.tick();self.advance();self.wire.fail_cancel=True
        self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(len(self.methods('DELETE')),1)
        self.advance();self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(len(self.methods('DELETE')),1)
    def test_external_manual_holding_blocks_entry(self):
        self.wire.positions=[dict(F['positions'][0],qty='1',qty_available='1',market_value='200')]
        self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(self.methods('POST'),[])
    def test_reserved_legacy_order_exists_blocks_without_deleting_history(self):
        self.wire.legacy[0]['client_order_id']=legacy._client_id(legacy.ORIGINAL_ID,'buy')
        before=self.source.read_bytes();self.assertEqual(self.tick()['phase'],'manual_attention')
        self.assertEqual(self.methods('POST'),[]);self.assertEqual(before,self.source.read_bytes())
    def test_legacy_terminal_mutation_blocks(self):
        self.wire.legacy[0]['filled_qty']='0.5'
        self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(self.methods('POST'),[])
    def test_fractional_sell_requires_fractionable_asset(self):
        self.tick();self.advance();self.fill('0.5','canceled');self.wire.bid='205';self.wire.ask='205.1';self.wire.fractionable=False
        self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(len(self.methods('POST')),1)
    def test_loss_latch_blocks_entry_and_does_not_clear_on_recovery(self):
        self.wire.account['equity']='99900';self.assertEqual(self.tick()['phase'],'blocked');self.assertEqual(self.methods('POST'),[])
        self.advance();self.wire.account['equity']='100000';self.assertEqual(self.tick()['phase'],'blocked')
    def test_asset_delay_cannot_make_quote_fresh_again(self):
        def delayed(method,path,body):
            if path.startswith('/v2/assets/'):
                self.now+=timedelta(seconds=11)
        self.wire.on_call=delayed
        self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(self.methods('POST'),[])
    def test_request_budget_covers_entry_bars_asset_post(self):
        self.assertEqual(self.tick()['phase'],'complete')
        self.assertLessEqual(len(self.wire.calls),12)
        with sqlite3.connect(self.guard.state_database) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM account_guard_attempts').fetchone()[0],len(self.wire.calls))
    def test_missing_terminal_archive_never_silently_ignored(self):
        self.tick();self.advance();self.fill('1');self.assertEqual(self.tick()['phase'],'complete')
        self.advance();self.wire.native=[];self.wire.positions=[]
        self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(len(self.methods('POST')),1)
    def test_readonly_reconcile_does_not_clear_uncertainty_or_post(self):
        self.wire.fail_post=True;self.tick();self.advance()
        with self.assertRaises(Exception):self.dispatcher.reconcile_only()
        self.assertTrue(self.lifecycle.status()['attention']);self.assertEqual(len(self.methods('POST')),1)
    def test_advancing_realistic_acquisition_clock_is_not_future_observation(self):
        def advance(method,path,body):self.now+=timedelta(milliseconds=2)
        self.wire.on_call=advance
        result=self.tick();self.assertEqual(result['phase'],'complete',result)
        self.assertEqual(len(self.methods('POST')),1)
    def test_absence_prefetch_is_included_in_total_observer_deadline(self):
        def delay(method,path,body):
            if path.startswith('/v2/orders:by_client_order_id?'):
                self.now+=timedelta(seconds=11)
        self.wire.on_call=delay
        self.assertEqual(self.tick()['phase'],'manual_attention');self.assertEqual(self.methods('POST'),[])
    def test_existing_legacy_loss_gate_blocks_entry_without_consuming_claim(self):
        with sqlite3.connect(self.source) as db:db.execute("INSERT INTO loss_days VALUES('2026-10-20',1)")
        result=self.tick();self.assertEqual(result['phase'],'complete',result)
        self.assertEqual(self.lifecycle.status()['intents'],[]);self.assertEqual(self.methods('POST'),[])
    def test_legacy_readonly_refresh_does_not_invalidate_next_tick(self):
        self.tick();self.advance();self.fill('1')
        with sqlite3.connect(self.source) as db:db.execute("UPDATE paper_readonly SET checked_at=200,revision=revision+1,snapshot='{}',reason='checked'")
        self.assertEqual(self.tick()['phase'],'complete')
    def test_terminal_event_times_cannot_mutate_after_archive(self):
        self.tick();self.advance();self.fill('1');self.assertEqual(self.tick()['phase'],'complete')
        stored=self.row()['response'];self.advance()
        original=datetime.fromisoformat(self.wire.native[0]['created_at'])
        self.wire.native[0]['created_at']=(original-timedelta(seconds=1)).isoformat()
        self.assertEqual(self.tick()['phase'],'manual_attention')
        self.assertEqual(self.row()['response'],stored)
        self.assertEqual(len(self.methods('POST')),1)
    def test_equivalent_terminal_time_representation_is_not_mutation(self):
        self.tick();self.advance();self.fill('1');self.assertEqual(self.tick()['phase'],'complete')
        self.advance()
        for key in ('created_at','submitted_at','updated_at','filled_at'):
            self.wire.native[0][key]=self.wire.native[0][key].replace('+00:00','Z')
        self.assertEqual(self.tick()['phase'],'complete')
    def test_exact_native_nanosecond_quote_survives_strategy_bridge(self):
        original=self.wire.call
        def ns_quote(method,path,body=None):
            status,data=original(method,path,body)
            if path.startswith('/v2/stocks/quotes/latest?'):
                for quote in data['quotes'].values():
                    quote['t']=(self.now-timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')+'.999999999Z'
            return status,data
        self.wire.call=ns_quote
        result=self.tick();self.assertEqual(result['phase'],'complete',result)
        self.assertEqual(len(self.methods('POST')),1)


class PrimaryEvidenceConnectedTests(unittest.TestCase):
    """Actual primary-source contract from synthetic original bytes to PAPER wire."""
    def setUp(self):
        NativeDispatchTests.setUp(self)
        from primary_earnings_source import PrimaryEarningsSource,stamp_ns
        from test_primary_earnings_source import FakeHTTP,feed_bytes,item_xml
        self.lifecycle=Lifecycle(self.root/'nvda-native.sqlite',FP,allowlist=('NVDA',),session_risk_policy=SessionRiskPolicy(4,Decimal('100')))
        self.guard=SharedAccountGuard(self.source,self.lifecycle.path,FP,enabled=True,state_database=self.root/'nvda-guard.sqlite',test_only=True)
        self.dispatcher=NativePaperDispatcher(self.lifecycle,self.guard,self.wire,enabled=True,now_ns=lambda:timestamp(self.now.isoformat()),monotonic_ns=lambda:100000000000)
        self.pipeline=DemoPipeline(self.root/'nvda-decisions.sqlite',Config(enabled=True,allowlist=('NVDA',)))
        self.port=NativePaperExecutionPort(evaluator=self.pipeline,lifecycle=self.lifecycle,dispatcher=self.dispatcher)
        self.market=NativeMarketSource(self.port)
        self.primary_time=timestamp('2026-10-20T13:31:01Z')
        self.http=FakeHTTP();self.http.feed=feed_bytes(item_xml(date='Tue, 20 Oct 2026 13:31:00 GMT'))
        self.primary=PrimaryEarningsSource(self.root/'primary.sqlite',transport=self.http,now_ns=lambda:self.primary_time)
        self.assertEqual(self.primary.collect().event_count,1)
        self.primary_time=timestamp(self.now.isoformat())
        self.primary.collect()
        self.inner=DemoTickRunner(eventsource=self.primary,marketsource=self.market,executionport=self.port,ledger=RunnerLedger(self.root/'nvda-runner.sqlite'),clock=lambda:self.now,config=RunnerConfig(enabled=True))
        self.runner=BoundedPaperRunner(self.inner,enabled=True)
    tick=NativeDispatchTests.tick
    methods=NativeDispatchTests.methods
    # Run only contract-specific tests, not inherited AAPL fixture scenarios.
    def test_primary_bytes_to_native_one_share_submission(self):
        result=self.tick();self.assertEqual(result['phase'],'complete',result)
        self.assertEqual(len(self.methods('POST')),1)
        self.assertEqual(self.methods('POST')[0][2]['symbol'],'NVDA')
        self.assertEqual(self.methods('POST')[0][2]['qty'],'1')
    def test_corrected_original_source_prevents_submission(self):
        self.primary_time+=16*10**9;self.now+=timedelta(seconds=16)
        self.http.article+=b'changed'
        self.primary.collect()
        self.assertEqual(self.tick()['phase'],'complete');self.assertEqual(self.methods('POST'),[])


class HTTPBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket',side_effect=AssertionError('no sockets')))
        self.http=NativePaperHTTP('fixture-key','fixture-secret',enabled=True)
        self.http._opener=Mock();self.http._reader=Mock()
        self.payload=dict(client_order_id='nd-fixture',symbol='AAPL',side='buy',qty='1',type='limit',time_in_force='day',limit_price='200.00',extended_hours=False)
    def test_default_is_disabled(self):
        h=NativePaperHTTP('fixture-key','fixture-secret')
        with self.assertRaisesRegex(DispatchRefused,'disabled'):h.call('POST','/v2/orders',self.payload)
    def test_live_origin_rejected(self):
        with self.assertRaises(DispatchRefused):NativePaperHTTP('fixture-key','fixture-secret',origin='https://api.alpaca.markets')
    def test_other_routes_methods_and_fractional_buy_rejected_before_opener(self):
        cases=[('POST','/v2/account',self.payload),('PATCH','/v2/orders',self.payload),('DELETE','/v2/orders',None),('DELETE','/v2/orders/a/../../account',None),('POST','/v2/orders',dict(self.payload,qty='0.5')),('POST','/v2/orders',dict(self.payload,extended_hours=True)),('POST','/v2/orders',dict(self.payload,type='market')),('POST','/v2/orders',dict(self.payload,notional='200'))]
        for args in cases:
            with self.subTest(args=args),self.assertRaises(Exception):self.http.call(*args)
        self.http._opener.open.assert_not_called()
    def test_bars_no_paid_feed_or_pagination(self):
        start=datetime(2026,10,20,13,30,tzinfo=timezone.utc);path=bar_path('AAPL',start,start+timedelta(minutes=6))
        for bad in (path.replace('feed=iex','feed=sip'),path+'&page_token=x',path.replace('limit=120','limit=10000')):
            with self.assertRaises(Exception):self.http.call('GET',bad)
        self.http._opener.open.assert_not_called()

if __name__=='__main__':unittest.main()
