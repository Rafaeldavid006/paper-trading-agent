"""Disabled coordinator regressions with synthetic transports and no sockets."""
from datetime import timedelta
import sqlite3
import unittest
from unittest.mock import patch
from alpaca_schema import timestamp
import test_native_paper_execution as fixtures

class BoundedCoordinatorTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
            self.enterContext(patch(name,side_effect=AssertionError('NETWORK FORBIDDEN')))
        self.h=fixtures.NativeDispatchTests();self.h.setUp();self.addCleanup(self.h.doCleanups)
    def clean(self):
        state=self.h.inner.ledger.status()
        self.assertEqual((state['dirty'],state['attention']),(0,None))
    def test_committed_runner_stop_during_asset_read_blocks_post(self):
        h=self.h
        h.wire.on_call=lambda method,path,body: h.inner.shutdown() if path.startswith('/v2/assets/') else None
        self.assertEqual(h.tick()['phase'],'manual_attention')
        self.assertEqual(h.methods('POST'),[])
        with h.lifecycle.tx() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_dispatch').fetchone()[0],0)
    def test_committed_pipeline_stop_during_asset_read_blocks_post(self):
        h=self.h
        h.wire.on_call=lambda method,path,body: h.pipeline.stop() if path.startswith('/v2/assets/') else None
        self.assertEqual(h.tick()['phase'],'manual_attention')
        self.assertEqual(h.methods('POST'),[])
        with h.lifecycle.tx() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_dispatch').fetchone()[0],0)
    def test_control_locks_cover_actual_post_and_release_after_completion(self):
        h=self.h;locked=[]
        def contender(method,path,body):
            if method!='POST':return
            for path in (h.inner.ledger.path,h.pipeline.database):
                with sqlite3.connect(path,timeout=0) as db:
                    with self.assertRaises(sqlite3.OperationalError):db.execute('BEGIN IMMEDIATE')
                    locked.append(True)
        h.wire.on_call=contender
        self.assertEqual(h.tick()['phase'],'complete');self.assertEqual(locked,[True,True])
        h.inner.shutdown();h.pipeline.stop()
    def test_same_runner_new_wrapper_does_not_inherit_first_wrapper_enable_flag(self):
        from native_paper_runner import BoundedPaperRunner
        h=self.h;h.runner.enabled=False
        later=BoundedPaperRunner(h.inner,enabled=True)
        self.assertEqual(later.tick()['phase'],'complete')
        self.assertEqual(len(h.methods('POST')),1)
    def test_inner_disabled_flag_cannot_be_bypassed_by_coordinator(self):
        from demo_tick_runner import RunnerConfig
        h=self.h;h.inner.config=RunnerConfig(enabled=False)
        before=h.inner.ledger.status()
        self.assertEqual(h.tick()['phase'],'disabled')
        self.assertEqual(h.inner._run_started_tick(reconciled=True)['phase'],'disabled')
        self.assertEqual(h.wire.calls,[]);self.assertEqual(h.inner.ledger.status(),before)
    def test_caller_decimal_context_cannot_change_cycle_math(self):
        from decimal import localcontext,ROUND_UP,Inexact
        with localcontext() as c:
            c.prec=4;c.rounding=ROUND_UP;c.traps[Inexact]=True
            self.assertEqual(self.h.tick()['phase'],'complete')
        self.assertEqual(self.h.methods('POST')[0][2]['qty'],'1')
    def test_partial_budget_waits_without_io_claim_or_runner_mutation(self):
        h=self.h;before=h.inner.ledger.status()
        with h.guard.hold(now_ns=timestamp(h.now.isoformat()),read_only=True):
            h.guard.reserve_request(now_ns=timestamp(h.now.isoformat()))
        self.assertEqual(h.tick()['phase'],'waiting_request_budget')
        self.assertEqual(h.wire.calls,[]);self.assertEqual(h.lifecycle.status()['intents'],[])
        self.assertEqual(h.inner.ledger.status(),before)
        h.advance();self.assertEqual(h.tick()['phase'],'complete')
        # Capacity returning after the original one-minute event window never
        # reopens that event or fabricates a new entry.
        self.assertEqual(len(h.methods('POST')),0)
    def test_closed_preflight_skips_strategy_execution_and_remains_clean(self):
        h=self.h;call=h.wire.call
        def closed(method,path,body=None):
            status,raw=call(method,path,body)
            if path=='/v2/clock':raw['is_open']=False
            return status,raw
        h.wire.call=closed
        with patch.object(h.inner,'_run_started_tick',side_effect=AssertionError('no strategy tick')):
            self.assertEqual(h.tick()['phase'],'closed_session')
        self.assertEqual(len(h.wire.calls),3);self.clean()
        self.assertEqual(h.lifecycle.status()['intents'],[])
    def test_opening_bar_wait_does_not_consume_events_or_claim_orders(self):
        h=self.h;h.now=h.now.replace(minute=31)
        with patch.object(h,'events',side_effect=AssertionError('no event acquisition')):
            self.assertEqual(h.tick()['phase'],'waiting_regular_bar_window')
        self.clean();self.assertEqual(h.methods('POST'),[])
        self.assertFalse(any('/bars?' in p for _,p,_ in h.wire.calls))
        with h.inner.ledger.db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM runner_events').fetchone()[0],0)
    def test_preflight_is_not_repeated_and_entry_retains_full_cap(self):
        h=self.h;self.assertEqual(h.tick()['phase'],'complete')
        self.assertEqual([x[0] for x in h.wire.calls],['GET']*11+['POST'])
        self.assertEqual(len([x for x in h.wire.calls if x[1]=='/v2/calendar?start=2026-10-20&end=2026-10-20']),1)
    def test_preflight_schema_failure_stays_dirty_and_never_replays(self):
        h=self.h;h.wire.legacy[0]['submitted_at']=None
        self.assertEqual(h.tick()['phase'],'manual_attention')
        state=h.inner.ledger.status();self.assertEqual((state['dirty'],state['attention']),(1,'tick_failed'))
        calls=len(h.wire.calls);h.advance()
        self.assertEqual(h.tick()['phase'],'manual_attention')
        self.assertEqual(len(h.wire.calls),calls);self.assertEqual(h.methods('POST'),[])
    def test_exit_has_priority_over_entry_budget_and_new_event_inputs(self):
        h=self.h;self.assertEqual(h.tick()['phase'],'complete');h.advance()
        h.fill('0.125000001','canceled');h.wire.bid='205';h.wire.ask='205.1'
        with patch.object(h.market,'inputs',side_effect=AssertionError('no new-entry bars')):
            result=h.tick()
        self.assertEqual(result['exits'],1,result)
        self.assertEqual(h.methods('POST')[-1][2]['qty'],'0.125000001')
        self.assertLessEqual(len(h.wire.calls)-12,12)
    def test_budget_capacity_gates_before_order_claim(self):
        h=self.h
        # Simulate a larger complete history consuming one extra observation GET
        # via the real coordinated budget; no broker compatibility is asserted.
        original=h.port.reconcile
        def more_history(now):
            result=original(now)
            h.dispatcher.wire.call('GET','/v2/account')
            return result
        with patch.object(h.port,'reconcile',side_effect=more_history):
            result=h.tick()
        self.assertEqual(result['phase'],'waiting_request_budget');self.clean()
        self.assertEqual(h.methods('POST'),[]);self.assertEqual(h.lifecycle.status()['intents'],[])
        with h.inner.ledger.db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM runner_events').fetchone()[0],0)

if __name__=='__main__':unittest.main()
