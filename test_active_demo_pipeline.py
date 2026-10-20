import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from active_demo_pipeline import *

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.now=datetime(2026,10,19,15,6,tzinfo=timezone.utc)
        self.path=self.tmp.name+'/ledger.sqlite'
        self.broker=FakeBroker(self.now)
        self.p=DemoPipeline(self.path,Config(enabled=True),self.broker)
        self.event=Event('event','episode','v1','NVDA',self.now-timedelta(minutes=5),self.now-timedelta(minutes=5), 'https://investor.nvidia.com/release','a'*64,True)
        self.quote=Quote('NVDA','100.49','100.50',self.now,self.now)
        self.bars=self.make_bars('NVDA','100.50')
        self.spys=self.make_bars('SPY','100.10')
    def tearDown(self): self.tmp.cleanup()
    def make_bars(self,symbol,last,count=6,anchor=None):
        anchor=anchor or self.event.published_at
        return [Bar(symbol,anchor+timedelta(minutes=i-1),anchor+timedelta(minutes=i),last if i==count-1 else '100',anchor+timedelta(minutes=i)) for i in range(count)]
    def decision(self,event=None,quote=None,bars=None,spy=None,now=None):
        return self.p.evaluate(event or self.event,quote or self.quote,bars or self.bars,spy or self.spys,now or self.now)
    def buy(self):
        d=self.decision(); return d,self.p.submit(d['id'],self.now)
    def test_default_disabled(self):
        p=DemoPipeline(self.tmp.name+'/off.sqlite',broker=self.broker)
        d=p.evaluate(self.event,self.quote,self.bars,self.spys,self.now)
        self.assertEqual(d['reason'],'disabled'); self.assertEqual(self.broker.calls,[])
    def test_transport_rejected(self):
        with self.assertRaises(SafetyError): DemoPipeline(self.tmp.name+'/x',broker=object())
    def test_happy_idempotent_restart(self):
        d,r=self.buy(); p=DemoPipeline(self.path,Config(enabled=True),self.broker)
        self.assertEqual(p.submit(d['id'],self.now)['key'],r['key'])
        self.assertEqual(len(self.broker.calls),1)
    def test_timeout_never_reposts(self):
        self.broker.timeout=True
        d,r=self.buy(); self.assertEqual(r['state'],'uncertain')
        self.broker.hidden=True
        self.assertEqual(self.p.submit(d['id'],self.now)['state'],'uncertain')
        self.assertEqual(len(self.broker.calls),1)
        self.broker.hidden=False
        self.assertEqual(self.p.reconcile()[0]['state'],'new')
    def test_stop_persistent(self):
        d=self.decision(); self.p.stop()
        p=DemoPipeline(self.path,Config(enabled=True),self.broker)
        with self.assertRaises(SafetyError): p.submit(d['id'],self.now)
        self.assertEqual(self.broker.calls,[])
    def test_event_conflict(self):
        self.decision()
        with self.assertRaises(SafetyError): self.decision(event=replace(self.event,source_hash='b'*64))
    def test_unverified(self): self.assertEqual(self.decision(event=replace(self.event,verified=False))['action'],'abstain')
    def test_corrected(self): self.assertEqual(self.decision(event=replace(self.event,corrected=True))['action'],'abstain')
    def test_late_event(self): self.assertEqual(self.decision(event=replace(self.event,observed_at=self.now))['reason'],'late_or_future_event')
    def test_future_bar(self):
        bars=list(self.bars); bars[-1]=replace(bars[-1],available_at=self.now+timedelta(seconds=1))
        self.assertEqual(self.decision(bars=bars)['reason'],'bar_not_available')
    def test_stale_quote(self): self.assertEqual(self.decision(quote=replace(self.quote,asof=self.now-timedelta(seconds=11)))['reason'],'invalid_or_stale_quote')
    def test_high_spread(self): self.assertEqual(self.decision(quote=replace(self.quote,bid='99'))['reason'],'wide_spread')
    def test_benchmark_weakness(self):
        self.assertEqual(self.decision(spy=self.make_bars('SPY','100.4'))['reason'],'signal_threshold')
    def test_fractional_anchor(self):
        e=replace(self.event,published_at=self.event.published_at+timedelta(seconds=1),observed_at=self.event.observed_at+timedelta(seconds=2))
        now=self.now+timedelta(minutes=1)
        d=self.decision(event=e,quote=replace(self.quote,asof=now,received_at=now),bars=self.make_bars('NVDA','100.5',7),spy=self.make_bars('SPY','100.1',7),now=now)
        self.assertEqual(d['action'],'enter')
    def test_submit_quote_freshness_rechecked(self):
        d=self.decision(); self.broker.now+=timedelta(seconds=11)
        with self.assertRaisesRegex(SafetyError,'stale_quote'): self.p.submit(d['id'],self.broker.now)
    def test_daily_loss_latched(self):
        d=self.decision(); self.broker.equity='99900'
        with self.assertRaisesRegex(SafetyError,'loss'): self.p.submit(d['id'],self.now)
        self.broker.equity='100100'
        with self.assertRaisesRegex(SafetyError,'loss'): self.p.submit(d['id'],self.now)
    def test_exit_after_loss_allowed(self):
        d,r=self.buy(); self.broker.fill(r['key']); self.broker.equity='99000'
        self.broker.now+=timedelta(minutes=60)
        q=replace(self.quote,asof=self.broker.now,received_at=self.broker.now)
        out=self.p.exit(d['id'],q,self.broker.now)
        self.assertEqual(out['side'],'sell'); self.assertEqual(out['qty'],1)
    def test_exit_not_due(self):
        d,r=self.buy(); self.broker.fill(r['key'])
        with self.assertRaisesRegex(SafetyError,'not_due'): self.p.exit(d['id'],self.quote,self.now)
    def test_global_stop_blocks_exit(self):
        d,r=self.buy(); self.broker.fill(r['key']); self.p.stop()
        self.broker.now+=timedelta(hours=1)
        with self.assertRaisesRegex(SafetyError,'global_stop'): self.p.exit(d['id'],replace(self.quote,asof=self.broker.now,received_at=self.broker.now),self.broker.now)
    def test_unmanaged_position(self):
        d=self.decision(); self.broker.positions['AAPL']=1
        with self.assertRaisesRegex(SafetyError,'unmanaged_position'): self.p.submit(d['id'],self.now)
    def test_second_episode_entry_blocked(self):
        d,r=self.buy(); self.broker.fill(r['key'],qty=0,state='canceled')
        d2=self.decision(event=replace(self.event,event_id='another',version='v2'))
        with self.assertRaisesRegex(SafetyError,'dedup'): self.p.submit(d2['id'],self.now)
    def test_filled_quantity_mismatch(self):
        d,r=self.buy(); self.broker.orders[r['key']]['state']='filled'
        with self.assertRaisesRegex(SafetyError,'terminal_fill'): self.p.reconcile()

    def test_expired_entry_cancel_no_fill(self):
        d,r=self.buy(); self.broker.now+=timedelta(seconds=60)
        self.p.expire_entries(self.broker.now)
        self.assertEqual(self.p.status(d['id'])['phase'],'no_fill')
        self.p.expire_entries(self.broker.now)
        self.assertEqual(self.broker.calls.count('cancel:'+r['key']),1)
    def test_expired_uncertain_not_reposted(self):
        self.broker.timeout=True; d,r=self.buy(); self.broker.hidden=True
        self.broker.now+=timedelta(seconds=60); self.p.expire_entries(self.broker.now)
        self.assertEqual(self.p.status(d['id'])['phase'],'manual_attention')
        self.assertEqual(len(self.broker.calls),1)
    def test_expiry_stop_no_cancel(self):
        d,r=self.buy(); self.p.stop(); self.broker.now+=timedelta(seconds=60)
        self.p.expire_entries(self.broker.now)
        self.assertEqual(len(self.broker.calls),1)
        self.assertEqual(self.p.status(d['id'])['phase'],'manual_attention')
    def test_cancel_timeout_attention(self):
        d,r=self.buy(); self.broker.timeout=True; self.broker.now+=timedelta(seconds=60)
        self.p.expire_entries(self.broker.now)
        self.assertEqual(self.p.status(d['id'])['phase'],'manual_attention')
    def test_unknown_quote_receipt(self):
        self.assertEqual(self.decision(quote=replace(self.quote,received_at=None))['reason'],'unknown_quote_availability')
    def test_corrected_episode_blocks_prior_decision(self):
        d=self.decision(); self.decision(event=replace(self.event,version='v2',corrected=True))
        with self.assertRaisesRegex(SafetyError,'corrected'): self.p.submit(d['id'],self.now)
    def test_completed_roundtrip(self):
        d,r=self.buy(); self.broker.fill(r['key']); self.broker.now+=timedelta(hours=1)
        out=self.p.exit(d['id'],replace(self.quote,asof=self.broker.now,received_at=self.broker.now),self.broker.now)
        self.broker.fill(out['key']); self.p.reconcile()
        self.assertEqual(self.p.status(d['id'])['phase'],'completed')
    def test_future_fill_latches_attention(self):
        d,r=self.buy(); self.broker.fill(r['key'])
        self.broker.orders[r['key']]['first_fill_at']=stamp(self.now)+1
        with self.assertRaises(SafetyError): self.p.reconcile()
        self.assertEqual(self.p.status(d['id'])['phase'],'manual_attention')
    def test_limit_violation_latches_attention(self):
        d,r=self.buy(); self.broker.fill(r['key'],price='101')
        with self.assertRaises(SafetyError): self.p.reconcile()
        self.assertEqual(self.p.status(d['id'])['phase'],'manual_attention')

if __name__=='__main__': unittest.main()
