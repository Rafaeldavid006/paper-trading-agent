"""Offline, explicitly driven ticks. No scheduler, credentials, transport, or activation wiring."""
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
import sqlite3
from active_demo_pipeline import DemoPipeline, FakeBroker, SafetyError, TERMINAL, stamp, money, encode

@dataclass(frozen=True)
class RunnerConfig:
    enabled: bool = False
    max_tick_gap_seconds: int = 120
    max_events: int = 100

@dataclass(frozen=True)
class MarketInputs:
    quote: object
    stock_bars: tuple
    benchmark_bars: tuple

class RunnerLedger:
    """Crash marker and permanent attention/stop latch. No reset method."""
    def __init__(self, path):
        if str(path) == ':memory:':
            raise SafetyError('durable_runner_ledger_required')
        self.path = str(path)
        with self.db() as c:
            c.executescript('''CREATE TABLE IF NOT EXISTS runner_control
              (id INTEGER PRIMARY KEY CHECK(id=1), heartbeat REAL, dirty INTEGER NOT NULL,
               stopped INTEGER NOT NULL, attention TEXT, stage TEXT);
              INSERT OR IGNORE INTO runner_control VALUES(1,NULL,0,0,NULL,'created');
              CREATE TABLE IF NOT EXISTS runner_events
              (event_id TEXT, version TEXT, symbol TEXT, payload TEXT, PRIMARY KEY(event_id,version,symbol));''')
    @contextmanager
    def db(self):
        c=sqlite3.connect(self.path, timeout=5)
        try:
            with c: yield c
        finally: c.close()
    def status(self):
        with self.db() as c:
            c.row_factory = sqlite3.Row
            return dict(c.execute('SELECT * FROM runner_control').fetchone())
    def begin(self, now, max_gap):
        t=stamp(now)
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            h,dirty,stopped,attention = c.execute('SELECT heartbeat,dirty,stopped,attention FROM runner_control').fetchone()
            if stopped or attention:
                return False
            reason = 'interrupted_or_concurrent_tick' if dirty else ('clock_regression' if h is not None and t<h else ('stale_heartbeat' if h is not None and t-h>max_gap else None))
            if reason:
                c.execute('UPDATE runner_control SET attention=? WHERE id=1',(reason,)); return False
            c.execute("UPDATE runner_control SET dirty=1,heartbeat=?,stage='started' WHERE id=1",(t,))
        return True
    def heartbeat(self, now, stage):
        t=stamp(now)
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            h,stopped,attention=c.execute('SELECT heartbeat,stopped,attention FROM runner_control').fetchone()
            if stopped or attention or (h is not None and t<h):
                raise SafetyError('runner_not_operational')
            c.execute('UPDATE runner_control SET heartbeat=?,stage=? WHERE id=1',(t,stage))
    def claim(self, event):
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            key=(event.event_id,event.version,event.symbol)
            payload=encode(asdict(event))
            prior=c.execute('SELECT payload FROM runner_events WHERE event_id=? AND version=? AND symbol=?',key).fetchone()
            if prior and prior[0]!=payload: raise SafetyError('event_identity_conflict')
            return c.execute('INSERT OR IGNORE INTO runner_events VALUES(?,?,?,?)',key+(payload,)).rowcount == 1
    def finish(self, now):
        self.heartbeat(now,'complete')
        with self.db() as c:
            c.execute('UPDATE runner_control SET dirty=0 WHERE id=1 AND stopped=0 AND attention IS NULL')
    def attention(self, reason):
        with self.db() as c:
            c.execute('UPDATE runner_control SET attention=COALESCE(attention,?) WHERE id=1',(reason,))
    def shutdown(self):
        with self.db() as c:
            c.execute("UPDATE runner_control SET stopped=1,stage='stopped' WHERE id=1")

class PipelineExecutionPort:
    """Exact existing FakeBroker boundary retained; not the pending paper bridge."""
    def __init__(self, pipeline):
        if type(pipeline) is not DemoPipeline or type(pipeline.broker) is not FakeBroker:
            raise SafetyError('offline_pipeline_required')
        self.pipeline=pipeline
    def reconcile(self, now): return self.pipeline.reconcile()
    def expire_entries(self, now): return self.pipeline.expire_entries(now)
    def candidates(self):
        rows=self.pipeline.records()
        exited={r['decision_id'] for r in rows if r['side']=='sell'}
        return [r for r in rows if r['side']=='buy' and r['state'] in TERMINAL and r['filled']>0 and r['decision_id'] not in exited]
    def due(self, row, quote, now):
        bid,_=self.pipeline._quote(quote,row['symbol'],now)
        delta=bid/money(row['average_price'])-1
        return stamp(now)-row['first_fill_at']>=3600 or delta<=Decimal('-.01') or delta>=Decimal('.02') or stamp(self.pipeline.broker.session_close)-stamp(now)<=600
    def exit(self, row, quote, now): return self.pipeline.exit(row['decision_id'],quote,now)
    def blocked(self):
        with self.pipeline.db() as c:
            attention=bool(c.execute('SELECT 1 FROM demo_attention').fetchone())
            stopped=bool(c.execute('SELECT stopped FROM demo_control').fetchone()[0])
        return attention or stopped or any(r['state'] not in TERMINAL for r in self.pipeline.records())
    def session(self, now):
        b=self.pipeline.broker
        if not 0<=stamp(now)-stamp(b.now)<=10: raise SafetyError('stale_broker_clock')
        if stamp(b.session_open)>=stamp(b.session_close): raise SafetyError('invalid_session')
        return b.market_open is True and stamp(b.session_open)<=stamp(now)<stamp(b.session_close)
    def evaluate(self, event, inputs, now):
        return self.pipeline.evaluate(event,inputs.quote,inputs.stock_bars,inputs.benchmark_bars,now)
    def submit(self, decision_id, now): return self.pipeline.submit(decision_id,now)

class DemoTickRunner:
    """Sources: events(now,limit), event_eligible(event); quote(symbol,now); inputs(event,now). Clock callable.

    Injected sources are trusted code, not a network sandbox. Fixtures only here.
    Ledger owns lifecycle, while pipeline owns immutable decisions/order reservations.
    """
    def __init__(self, *, eventsource, marketsource, executionport, ledger, clock, config=RunnerConfig()):
        # Explicit separate Decimal fixture adapter; the original FakeBroker
        # constructor fence and integer execution port stay unchanged.
        from native_decimal_port import NativeDecimalExecutionPort
        from native_paper_runner import NativePaperExecutionPort
        if type(executionport) not in (PipelineExecutionPort, NativeDecimalExecutionPort, NativePaperExecutionPort):
            raise SafetyError('offline_execution_port_required')
        self.mode = getattr(executionport, 'mode', 'offline_fake_only')
        if type(config.enabled) is not bool or type(config.max_tick_gap_seconds) is not int or not 1<=config.max_tick_gap_seconds<=3600 or type(config.max_events) is not int or not 1<=config.max_events<=100:
            raise SafetyError('invalid_runner_configuration')
        self.events,self.market,self.port,self.ledger,self.clock,self.config=eventsource,marketsource,executionport,ledger,clock,config
        self.local_failed=False
    def pulse(self, stage):
        now=self.clock()
        prior=self.ledger.status()['heartbeat']
        if prior is not None and stamp(now)-prior>self.config.max_tick_gap_seconds:
            raise SafetyError('stage_timeout')
        self.ledger.heartbeat(now,stage)
        return now
    def tick(self):
        if not self.config.enabled: return {'phase':'disabled','mode':self.mode}
        if self.local_failed: return {'phase':'manual_attention','reason':'local_failure','mode':self.mode}
        try:
            if not self.ledger.begin(self.clock(),self.config.max_tick_gap_seconds):
                state=self.ledger.status()
                return {'phase':'stopped' if state['stopped'] else 'manual_attention','reason':state['attention'],'mode':self.mode}
            self.port.reconcile(self.pulse('reconcile'))
            self.port.expire_entries(self.pulse('expire'))
            now=self.pulse('session')
            if not self.port.session(now):
                self.ledger.finish(self.clock()); return {'phase':'closed_session','mode':self.mode}
            # Risk-reducing exits always precede acquisition/evaluation of new events.
            exits=0
            for row in self.port.candidates():
                quote=self.market.quote(row['symbol'],self.pulse('exit_quote'))
                now=self.pulse('exit_check')
                if self.port.due(row,quote,now):
                    self.port.exit(row,quote,self.pulse('exit')); exits+=1
                    if self.port.blocked(): break
            if self.port.blocked():
                self.ledger.finish(self.clock()); return {'phase':'blocked','exits':exits,'mode':self.mode}
            count=0
            batch=self.events.events(self.pulse('events'),self.config.max_events)
            for event in batch:
                if count>=self.config.max_events: raise SafetyError('event_batch_overflow')
                count+=1
                now=self.pulse('event_claim')
                ready=getattr(self.port,'event_ready',None)
                if ready is not None and ready(event,now) is not True: continue
                if not self.ledger.claim(event): continue
                if self.events.event_eligible(event) is not True: continue
                inputs=self.market.inputs(event,self.pulse('market_inputs'))
                if self.events.event_eligible(event) is not True: continue
                decision=self.port.evaluate(event,inputs,self.pulse('evaluate'))
                if decision['action']=='enter' and self.events.event_eligible(event) is True:
                    self.port.submit(decision['id'],self.pulse('submit'))
                    if self.port.blocked(): break
            self.ledger.finish(self.clock())
            return {'phase':'complete','events':count,'exits':exits,'mode':self.mode}
        except Exception:
            self.local_failed=True
            try: self.ledger.attention('tick_failed')
            except Exception: pass  # Durable dirty marker survives when storage is unavailable.
            return {'phase':'manual_attention','reason':'tick_failed','mode':self.mode}
    def shutdown(self):
        self.local_failed=True
        self.ledger.shutdown()
