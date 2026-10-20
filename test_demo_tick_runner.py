import tempfile
import unittest
from dataclasses import replace
from datetime import datetime,timedelta,timezone
from unittest.mock import patch
from active_demo_pipeline import Event,Quote,Bar,Config,DemoPipeline,FakeBroker,SafetyError
from demo_tick_runner import *

class Events:
    def __init__(self, events): self.batch=events; self.calls=[]
    def event_eligible(self,event): return True
    def events(self,now,limit): self.calls.append('events'); return self.batch
class Market:
    def __init__(self,test): self.t=test; self.calls=[]
    def quote(self,symbol,now): self.calls.append('quote'); return replace(self.t.quote,asof=now,received_at=now)
    def inputs(self,event,now):
        self.calls.append('inputs')
        return MarketInputs(self.quote(event.symbol,now),tuple(self.t.bars),tuple(self.t.spys))

class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.now=datetime(2026,10,19,15,6,tzinfo=timezone.utc)
        self.broker=FakeBroker(self.now)
        self.pipeline=DemoPipeline(self.tmp.name+'/p.db',Config(True),self.broker)
        self.event=Event('event','episode','v1','NVDA',self.now-timedelta(minutes=5),self.now-timedelta(minutes=5),'https://investor.nvidia.com/release','a'*64,True)
        self.quote=Quote('NVDA','100.49','100.50',self.now,self.now)
        def bars(symbol,end):
            return [Bar(symbol,self.event.published_at+timedelta(minutes=i-1),self.event.published_at+timedelta(minutes=i),end if i==5 else '100',self.event.published_at+timedelta(minutes=i)) for i in range(6)]
        self.bars=bars('NVDA','100.5'); self.spys=bars('SPY','100.1')
        self.events=Events([self.event]); self.market=Market(self)
        self.ledger=RunnerLedger(self.tmp.name+'/r.db')
        self.runner=self.make()
    def make(self,config=RunnerConfig(True)):
        return DemoTickRunner(eventsource=self.events,marketsource=self.market,executionport=PipelineExecutionPort(self.pipeline),ledger=self.ledger,clock=lambda:self.now,config=config)
    def advance(self,seconds): self.now+=timedelta(seconds=seconds); self.broker.now=self.now
    def test_disabled_has_no_effect(self):
        r=self.make(RunnerConfig()); self.assertEqual(r.tick()['phase'],'disabled')
        self.assertEqual(self.events.calls,[]); self.assertEqual(self.broker.calls,[]); self.assertIsNone(self.ledger.status()['heartbeat'])
    def test_entry_fake(self):
        self.assertEqual(self.runner.tick()['phase'],'complete'); self.assertEqual(len(self.broker.calls),1)
        self.assertEqual(self.ledger.status()['dirty'],0)
    def test_clean_restart_no_replay(self):
        self.runner.tick(); self.advance(1); self.make().tick(); self.assertEqual(len(self.broker.calls),1)
    def test_uncertain_no_replay(self):
        self.broker.timeout=True; self.runner.tick(); self.broker.hidden=True
        self.advance(60); self.make().tick(); self.assertEqual(len(self.broker.calls),1)
    def test_crash_restart_latches(self):
        with patch.object(self.events,'events',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt): self.runner.tick()
        self.assertEqual(self.make().tick()['phase'],'manual_attention'); self.assertEqual(self.broker.calls,[])
    def test_heartbeat_failure_no_send(self):
        with patch.object(self.ledger,'heartbeat',side_effect=OSError('private detail')):
            out=self.runner.tick()
        self.assertEqual(out['reason'],'tick_failed'); self.assertNotIn('private',str(out)); self.assertEqual(self.broker.calls,[])
        self.assertEqual(self.make().tick()['phase'],'manual_attention')
    def test_storage_attention_failure_dirty_survives(self):
        with patch.object(self.ledger,'heartbeat',side_effect=OSError),patch.object(self.ledger,'attention',side_effect=OSError): self.runner.tick()
        self.assertEqual(self.ledger.status()['dirty'],1); self.assertEqual(self.make().tick()['phase'],'manual_attention')
    def test_shutdown_durable(self):
        self.runner.shutdown(); self.assertEqual(self.make().tick()['phase'],'stopped'); self.assertEqual(self.broker.calls,[])
    def test_global_stop_preserved(self):
        self.pipeline.stop(); self.assertEqual(self.runner.tick()['phase'],'blocked'); self.assertEqual(self.broker.calls,[])
    def test_reconcile_expire_before_sources(self):
        calls=[]
        with patch.object(self.runner.port,'reconcile',side_effect=lambda n:calls.append('reconcile')),patch.object(self.runner.port,'expire_entries',side_effect=lambda n:calls.append('expire')),patch.object(self.events,'events',side_effect=lambda n,l:calls.append('events') or []): self.runner.tick()
        self.assertEqual(calls,['reconcile','expire','events'])
    def test_expiry_cancel_once(self):
        self.runner.tick(); self.advance(60); self.runner.tick(); self.advance(1); self.make().tick()
        self.assertEqual(sum(x.startswith('cancel:') for x in self.broker.calls),1)
    def test_due_exit_before_new_events_even_after_loss(self):
        self.runner.tick(); self.broker.fill(self.broker.calls[0]); self.advance(1)
        self.broker.equity='99000'; self.quote=replace(self.quote,bid='103',ask='103.01')
        self.events.calls=[]
        result=self.runner.tick(); self.assertEqual(result['exits'],1)
        self.assertEqual(self.events.calls,[]); self.assertEqual(self.pipeline.records()[-1]['side'],'sell')
    def test_non_due_exit_then_entry_scan(self):
        self.runner.tick(); self.broker.fill(self.broker.calls[0]); self.advance(1); self.events.calls=[]
        result=self.runner.tick(); self.assertEqual(result['exits'],0); self.assertEqual(self.events.calls,['events'])
    def test_stale_heartbeat(self):
        self.events.batch=[]; self.runner.tick(); self.advance(121)
        self.assertEqual(self.make().tick()['reason'],'stale_heartbeat')
    def test_clock_regression(self):
        self.events.batch=[]; self.runner.tick(); self.advance(-1)
        self.assertEqual(self.runner.tick()['reason'],'clock_regression')
    def test_stale_broker_clock(self):
        self.now+=timedelta(seconds=11); self.assertEqual(self.runner.tick()['phase'],'manual_attention'); self.assertEqual(self.broker.calls,[])
    def test_closed_session(self):
        self.now=self.broker.session_close; self.broker.now=self.now
        self.assertEqual(self.runner.tick()['phase'],'closed_session'); self.assertEqual(self.events.calls,[])
    def test_open_boundary_entry_window_fail_closed(self):
        self.broker.session_open=self.now
        self.assertEqual(self.runner.tick()['phase'],'manual_attention'); self.assertEqual(self.broker.calls,[])
    def test_new_day_does_not_reset_shutdown(self):
        self.runner.shutdown(); self.advance(86400)
        self.assertEqual(self.make().tick()['phase'],'stopped')
    def test_new_day_without_ticks_stale_not_reset(self):
        self.events.batch=[]; self.runner.tick(); self.advance(86400)
        self.assertEqual(self.make().tick()['reason'],'stale_heartbeat')
    def test_blocking_source_timeout(self):
        def slow(n,l): self.advance(121); return [self.event]
        with patch.object(self.events,'events',side_effect=slow): result=self.runner.tick()
        self.assertEqual(result['phase'],'manual_attention'); self.assertEqual(self.broker.calls,[])
    def test_duplicate_identity_changed_event_fails(self):
        self.events.batch=[replace(self.event,verified=False)]; self.runner.tick()
        self.events.batch=[self.event]; self.advance(1)
        self.assertEqual(self.runner.tick()['phase'],'manual_attention'); self.assertEqual(self.broker.calls,[])
    def test_execution_port_substitution_rejected(self):
        with self.assertRaises(SafetyError): DemoTickRunner(eventsource=None,marketsource=None,executionport=object(),ledger=self.ledger,clock=lambda:self.now)
    def test_trusted_gate_before_evaluation(self):
        with patch.object(self.events,'event_eligible',return_value=False): self.runner.tick()
        self.assertEqual(self.market.calls,[]); self.assertEqual(self.broker.calls,[])
    def test_correction_between_evaluate_and_submit(self):
        with patch.object(self.events,'event_eligible',side_effect=[True,True,False]): self.runner.tick()
        self.assertEqual(self.broker.calls,[])
    def test_correction_during_market_fetch(self):
        with patch.object(self.events,'event_eligible',side_effect=[True,False]): self.runner.tick()
        self.assertEqual(self.pipeline.records(),[]); self.assertEqual(self.broker.calls,[])
    def test_config_validation(self):
        with self.assertRaises(SafetyError): self.make(RunnerConfig(True,0))

if __name__=='__main__': unittest.main()
