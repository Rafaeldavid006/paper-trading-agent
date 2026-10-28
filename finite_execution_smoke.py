"""Explicit quote-based PAPER mechanics trial, never an earnings/research signal."""
from contextlib import contextmanager,ExitStack
from decimal import Decimal,localcontext
from pathlib import Path
import sqlite3
import finite_root_contract as rc
from finite_pilot_contract import require,NS
from decimal_lifecycle import Lifecycle,SessionRiskPolicy,Proposal
from demo_tick_runner import RunnerLedger
from native_account_guard import SharedAccountGuard,AccountGuardRefused
from native_paper_execution import NativePaperDispatcher,_datetime_ceil
from alpaca_schema import parse_latest_quotes,TERMINAL,OwnedIntent,owned_exit_quantity
from finite_pilot_transport import FinitePilotTransport,_read

@contextmanager
def _writer(path):
    db=sqlite3.connect(Path(path).as_uri()+'?mode=rw',uri=True,timeout=.05)
    try:
        with db:
            db.execute('PRAGMA busy_timeout=50');db.execute('BEGIN IMMEDIATE');yield db
    finally:db.close()

class SmokeCoordinator:
    def __init__(self,bundle,journal,manifest,life,guard,ledger,dispatch,now_ns):
        rc.smoke_scope(manifest)
        require(type(life) is Lifecycle and type(dispatch) is NativePaperDispatcher and dispatch.lifecycle is life,'smoke_dispatcher_required')
        self.bundle,self.journal,self.manifest=bundle,journal,manifest
        self.life,self.guard,self.ledger,self.dispatcher,self.now_ns=life,guard,ledger,dispatch,now_ns
        self.decision_path=bundle.paths['decision'];self.enabled=True;self.local_failed=False
        require(dispatch.pre_write_context is None,'smoke_existing_write_context')
        dispatch.pre_write_context=self.write_controls
    def pulse(self,stage):
        now=self.now_ns();state=self.ledger.status()
        require(state['heartbeat'] is not None and 0<=now/NS-state['heartbeat']<=120,'smoke_stage_timeout')
        self.ledger.heartbeat(_datetime_ceil(now),stage);return now
    @contextmanager
    def write_controls(self):
        rc.smoke_scope(self.manifest);require(self.enabled and not self.local_failed,'smoke_disabled')
        with _writer(self.ledger.path) as runner,_writer(self.decision_path) as decision:
            row=runner.execute('SELECT dirty,stopped,attention,stage FROM runner_control').fetchone()
            require(row is not None and row[:3]==(1,0,None) and row[3] in ('submit','exit','expire'),'runner_write_control_refused')
            require(decision.execute('SELECT stopped FROM demo_control').fetchall()==[(0,)] and decision.execute('SELECT 1 FROM demo_attention LIMIT 1').fetchone() is None,'pipeline_write_control_refused')
            yield
    def final_write_check(self,method,path,body):
        scope=rc.smoke_scope(self.manifest)
        require(self.manifest.root_binding.index==0,'smoke_second_child_readonly')
        raw,snapshot,now=self.dispatcher._gate()
        quotes=parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=now)
        require(len(quotes)==1 and quotes[0].symbol=='NVDA','smoke_quote_required');quote=quotes[0]
        require((quote.ask-quote.bid)/((quote.ask+quote.bid)/2)<=Decimal('.002'),'smoke_quote_spread')
        require(len(snapshot.positions)<=1 and all(p.symbol=='NVDA' and 0<p.quantity<=1 for p in snapshot.positions),'smoke_position_envelope')
        require(self.journal.smoke_entry_claimed(self.manifest),'smoke_entry_not_claimed')
        if method=='POST':
            side=body['side'];require(side in ('buy','sell') and body['client_order_id']==rc.smoke_cid(side),'smoke_exact_identity')
            quantity,price=Decimal(body['qty']),Decimal(body['limit_price'])
            require(0<quantity<=1,'smoke_quantity_cap')
            if side=='buy':
                require(quantity==1 and not snapshot.positions and price<=Decimal(scope.max_notional_usd),'smoke_entry_envelope')
                require(price==quote.ask,'smoke_buy_limit_not_current_ask')
            else:require(price==quote.bid,'smoke_exit_limit_not_current_bid')
        else:
            require(method=='DELETE' and body is None,'smoke_write_route')
            order=next((o for o in snapshot.orders if '/v2/orders/'+o.id==path),None)
            require(order is not None and order.client_order_id in (rc.smoke_cid('buy'),rc.smoke_cid('sell')),'smoke_owned_cancel')
            with _read(self.life.path) as db:
                row=db.execute('SELECT claimed FROM intents WHERE cid=?',(order.client_order_id,)).fetchone()
            require(row is not None and now-row[0]>=scope.cancel_after_ns and now-order.submitted_ns>=scope.cancel_after_ns,'smoke_cancel_too_early')
    def act(self):
        raw,snapshot,now=self.dispatcher._gate();scope=rc.smoke_scope(self.manifest)
        if self.manifest.root_binding.index==1:return 0
        with _read(self.life.path) as db:
            db.row_factory=sqlite3.Row
            rows=[dict(r) for r in db.execute('SELECT * FROM intents')]
            cancels={r[0] for r in db.execute('SELECT cid FROM cancels')}
        allowed={rc.smoke_cid('buy'),rc.smoke_cid('sell')}
        # Predecessor history remains legitimate but cannot become smoke-owned.
        own=[r for r in rows if r['cid'] in allowed]
        bycid={o.client_order_id:o for o in snapshot.orders}
        for row in own:
            order=bycid.get(row['cid']);require(order is not None,'smoke_owned_order_missing')
            if order.status not in TERMINAL:
                if order.status!='pending_cancel' and row['cid'] not in cancels and now-row['claimed']>=scope.cancel_after_ns and now-order.submitted_ns>=scope.cancel_after_ns:
                    if self.guard.request_budget_remaining(now_ns=now)<1:return 0
                    self.pulse('expire');self.life.claim_cancel(row['cid'],now_ns=self.now_ns());self.dispatcher.cancel(row['cid'])
                return 0  # A DELETE acknowledgement is never terminal evidence.
        quote=parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=now)[0]
        require(quote.symbol=='NVDA','smoke_quote_required')
        buy=next((r for r in own if r['side']=='buy'),None);sell=next((r for r in own if r['side']=='sell'),None)
        if buy is not None:
            if sell is not None:return 0 # Never replace an exit, including partial/cancelled.
            buy_order=bycid[buy['cid']]
            if buy_order.filled==0:
                net={}
                for order in snapshot.orders:
                    require(order.status in TERMINAL,'smoke_nonterminal_history')
                    net[order.symbol]=net.get(order.symbol,Decimal(0))+order.filled*(1 if order.side=='buy' else -1)
                require(not snapshot.positions and all(q==0 for q in net.values()),'smoke_zero_fill_flatness_unproven')
                return 0
            intents=tuple(OwnedIntent(self.life.binding,r['cid'],r['symbol'],r['side'],Decimal(r['quantity'])) for r in rows)
            quantity=owned_exit_quantity(snapshot,intents=intents,symbol='NVDA',now_ns=now)
            if quantity<=0:return 0
            require(quantity<=1,'smoke_quantity_cap')
            if self.guard.request_budget_remaining(now_ns=now)<2:return 0
            self.pulse('exit');cid=rc.smoke_cid('sell')
            self.life.claim(Proposal('execution-smoke-exit-'+cid,cid,'NVDA','sell',quantity,quote.bid),now_ns=self.now_ns())
            self.dispatcher.submit(cid);return 1
        require(not self.journal.smoke_entry_claimed(self.manifest),'smoke_consumed_entry_without_intent')
        if self.guard.request_budget_remaining(now_ns=now)<2:return 0
        require(not snapshot.positions and all(o.status in TERMINAL for o in snapshot.orders),'smoke_initial_flat_required')
        require(quote.ask<=Decimal(scope.max_notional_usd) and (quote.ask-quote.bid)/((quote.ask+quote.bid)/2)<=Decimal('.002'),'smoke_entry_price_or_spread')
        # Existing native dispatcher independently rechecks session, risk, asset,
        # quote and ownership before its final wire; do not manufacture events.
        self.pulse('submit');self.journal.claim_smoke_entry(self.manifest);cid=rc.smoke_cid('buy')
        self.life.claim(Proposal('execution-smoke-entry-'+cid,cid,'NVDA','buy',Decimal(1),quote.ask),now_ns=self.now_ns())
        self.dispatcher.submit(cid);return 0

def run_tick(bundle,journal,manifest,*,environ,now_ns,monotonic_ns,test_only,broker_factory):
    from finite_pilot_worker import _credentials,_pristine,public,failure
    rc.smoke_scope(manifest)
    policy=rc.smoke_execution_policy(manifest);policy_kw={} if policy is None else dict(execution_policy=policy)
    life=Lifecycle(bundle.paths['lifecycle'],bundle.binding,allowlist=('NVDA',),session_risk_policy=SessionRiskPolicy(4,Decimal('100')),**policy_kw)
    guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
    ledger=RunnerLedger(bundle.paths['runner']);key,secret=_credentials(environ)
    wire=FinitePilotTransport(key,secret,manifest=manifest,journal=journal,test_only=test_only,transport_factory=broker_factory)
    dispatch=NativePaperDispatcher(life,guard,wire,enabled=True,now_ns=now_ns,monotonic_ns=monotonic_ns,**policy_kw)
    coordinator=SmokeCoordinator(bundle,journal,manifest,life,guard,ledger,dispatch,now_ns);wire.bind(coordinator)
    begun=False
    try:
        with localcontext() as ctx,dispatch.tick_scope():
            ctx.prec=60
            try:guard.admit_cycle(now_ns=now_ns())
            except AccountGuardRefused as exc:
                if str(exc)!='cycle_budget_wait':raise
                result=public('tick','bounded_tick_finished',outcome='waiting_request_budget',events=0,exits=0)
                result.update(mode='finite_paper_execution_smoke',research_signal=False,execution_child=manifest.root_binding.index)
                if manifest.root_binding.index==1:result['order_permission']='none'
                return result
            journal.begin_stage(manifest,'tick');begun=True
            require(ledger.begin(_datetime_ceil(now_ns()),120),'smoke_runner_begin_refused')
            coordinator.pulse('reconcile');dispatch.observe()
            exits=coordinator.act()
            ledger.finish(_datetime_ceil(now_ns()));_pristine(bundle)
            row=journal.finish_stage(manifest,'tick')
            result=failure('tick',code='finite_scope_exhausted') if journal._exhausted(row,manifest) else public('tick','bounded_tick_finished',outcome='complete',events=0,exits=exits)
            result.update(mode='finite_paper_execution_smoke',research_signal=False,execution_child=manifest.root_binding.index)
            if manifest.root_binding.index==1:result['order_permission']='none'
            return result
    except Exception:
        if begun:
            coordinator.local_failed=True
            try:ledger.attention('smoke_tick_failed')
            except Exception:pass
        raise
