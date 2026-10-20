"""Disabled explicit PAPER ticks. No startup wiring, scheduler or secret loading."""
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from active_demo_pipeline import DemoPipeline, Quote, SafetyError
from alpaca_schema import parse_latest_quotes, fresh
from decimal_lifecycle import Lifecycle
from demo_tick_runner import DemoTickRunner, MarketInputs
from native_decimal_port import NativeDecimalExecutionPort, nanoseconds
from native_paper_execution import (NativePaperDispatcher, NativePaperHTTP, require,
    _datetime_ceil, bar_path)
from native_account_guard import AccountGuardRefused
from shadow_market_inputs import parse_bars


@dataclass(frozen=True)
class NativeStrategyQuote(Quote):
    native_timestamp_ns: int = 0


class NativePaperExecutionPort(NativeDecimalExecutionPort):
    """Reuse strategy orchestration; add explicit, independently disabled dispatch."""
    mode='disabled_by_default_native_paper'
    def __init__(self, *, evaluator, lifecycle, dispatcher):
        require(type(evaluator) is DemoPipeline and evaluator.broker is None,'decision_only_pipeline_required')
        require(type(lifecycle) is Lifecycle and type(dispatcher) is NativePaperDispatcher
                and dispatcher.lifecycle is lifecycle,'bound_dispatcher_required')
        require(set(evaluator.config.allowlist)==set(lifecycle.allowlist),'universe_mismatch')
        self.pipeline,self.lifecycle,self.dispatcher=evaluator,lifecycle,dispatcher
        self.snapshot=self.raw=None;self.receipts=[]

    def reconcile(self, now):
        observation=self.dispatcher.observe()
        self.raw=observation.arguments()
        self.session_open_ns=observation.session_open_ns
        self.snapshot=self.lifecycle._parse(self.raw,self.dispatcher.now_ns())
        require(self.session_open_ns<self.snapshot.clock.next_close_ns,'invalid_session_window')
        with self.lifecycle.tx() as db:
            db.execute('CREATE TABLE IF NOT EXISTS native_strategy_sessions (close_ns INTEGER PRIMARY KEY,open_ns INTEGER NOT NULL)')
            prior=db.execute('SELECT open_ns FROM native_strategy_sessions WHERE close_ns=?',(self.snapshot.clock.next_close_ns,)).fetchone()
            require(prior is None or prior['open_ns']==self.session_open_ns,'session_open_changed')
            db.execute('INSERT OR IGNORE INTO native_strategy_sessions VALUES(?,?)',(self.snapshot.clock.next_close_ns,self.session_open_ns))
        return self.lifecycle.public_status()

    def _match_quote(self, strategy_quote, symbol, now):
        require(type(strategy_quote) is NativeStrategyQuote,'exact_native_quote_required')
        quotes=parse_latest_quotes(self.raw['quotes'],symbols=tuple(self.raw['quote_symbols']),feed=self.raw['feed'],now_ns=nanoseconds(now))
        native=next((q for q in quotes if q.symbol==symbol),None)
        require(native is not None and (native.bid,native.ask,native.timestamp_ns)==(
            Decimal(strategy_quote.bid),Decimal(strategy_quote.ask),strategy_quote.native_timestamp_ns)
            and strategy_quote.asof==_datetime_ceil(native.timestamp_ns),'strategy_native_quote_mismatch')

    def submit(self, decision_id, now):
        receipt=super().submit(decision_id,now)
        if receipt.state=='uncertain':
            self.dispatcher.submit(receipt.client_order_id)
        return receipt

    def _entry_strategy_gates(self, row, payload, now):
        super()._entry_strategy_gates(row,payload,now)
        exposure=sum((max(p.market_value,p.quantity*1000) for p in self.snapshot.positions),Decimal(0))
        if exposure+Decimal(payload['limit_price'])>2000:
            raise SafetyError('strategy_exposure_cap')
        try:
            self.dispatcher.guard.legacy_entry_gate(now_ns=nanoseconds(now))
        except AccountGuardRefused as exc:
            if str(exc)=='legacy_loss_entry_gate':
                raise SafetyError('session_loss_entry_gate') from None
            raise
        legacy=self.dispatcher.guard.legacy_buy_claims(
            session_open_ns=self.session_open_ns,session_close_ns=self.snapshot.clock.next_close_ns)
        risk=next((r for r in self.lifecycle.status()['risk_sessions']
                   if r['session_close_ns']==self.snapshot.clock.next_close_ns),None)
        if risk is None or risk['entry_claims']+legacy>=4:
            raise SafetyError('session_entry_quota')

    def exit(self, row, quote, now):
        receipt=super().exit(row,quote,now)
        self.dispatcher.submit(receipt.client_order_id)
        return receipt

    def expire_entries(self, now):
        previous=len(self.receipts)
        super().expire_entries(now)
        for receipt in self.receipts[previous:]:
            self.dispatcher.cancel(receipt.client_order_id)


class NativeMarketSource:
    """Same native quote + one bounded IEX batch for stock and benchmark bars."""
    def __init__(self, port):
        require(type(port) is NativePaperExecutionPort,'native_port_required')
        self.port=port
    def quote(self,symbol,now):
        raw=self.port.raw
        require(raw is not None,'native_observation_required')
        quotes=parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=nanoseconds(now))
        q=next((q for q in quotes if q.symbol==symbol),None)
        require(q is not None,'quote_missing')
        fresh(raw['observed_ns'],nanoseconds(now))
        return NativeStrategyQuote(symbol,str(q.bid),str(q.ask),_datetime_ceil(q.timestamp_ns),
            _datetime_ceil(raw['observed_ns']),q.timestamp_ns)
    def inputs(self,event,now):
        anchor=event.published_at.replace(second=0,microsecond=0)
        end=anchor+timedelta(minutes=5+(event.published_at!=anchor))
        start=anchor-timedelta(minutes=1)
        status,data=self.port.dispatcher.wire.call('GET',bar_path(event.symbol,start,end))
        require(status==200 and type(data) is dict and type(data.get('bars')) is dict
                and set(data['bars'])=={event.symbol,'SPY'} and data.get('next_page_token') is None
                and data.get('feed','iex')=='iex','complete_iex_bars_required')
        received=_datetime_ceil(self.port.dispatcher.now_ns())
        sets=[]
        for symbol in (event.symbol,'SPY'):
            sets.append(parse_bars({'symbol':symbol,'bars':data['bars'][symbol],'next_page_token':None},symbol,
                start=start,end=end,received_at=received,now=received))
        return MarketInputs(self.quote(event.symbol,received),*sets)


class BoundedPaperRunner:
    """One caller-driven tick under the shared account fence. No automatic loop."""
    def __init__(self, runner, *, enabled=False):
        require(type(enabled) is bool and type(runner) is DemoTickRunner and type(runner.port) is NativePaperExecutionPort,'native_runner_required')
        self.runner,self.enabled=runner,enabled
        if type(runner.port.dispatcher.wire.transport) is NativePaperHTTP:
            from primary_earnings_source import PrimaryEarningsSource
            require(type(runner.events) is PrimaryEarningsSource and type(runner.market) is NativeMarketSource,
                    'reviewed_primary_and_market_sources_required')
    def tick(self):
        if not self.enabled:
            return {'phase':'disabled','mode':'native_paper','order_permission':'none'}
        try:
            with self.runner.port.dispatcher.tick_scope():
                return self.runner.tick()
        except Exception:
            return {'phase':'manual_attention','reason':'account_fence_or_tick_refused',
                    'mode':'native_paper','order_permission':'none'}
