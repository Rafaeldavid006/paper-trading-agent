"""Explicit one-process, no-restart finite PAPER successor. Default disabled.

Original runner and failed anchor remain unchanged. This process owns its separate
journal; native ownership, loss latches, shared budget and attention never reset.
"""
from contextlib import contextmanager,ExitStack
from decimal import Decimal,localcontext
from pathlib import Path
import json,os,sys,threading,time
import prepared_execution_contract as c
import prepared_execution_activation as activation
from prepared_execution_state import Predecessor
from prepared_execution_journal import Journal
from prepared_execution_transport import Transport
from readonly_runtime_worker import serialized
from finite_observation_worker import ReadOnlyLifecycle
from finite_pilot_contract import RecoveryEvidence
from finite_pilot_transport import _read
import runtime_state_bundle as inventory
from native_account_guard import SharedAccountGuard
from native_paper_execution import NativeExecutionObserver,CoordinatedWire,ExecutionObservation,NativePaperDispatcher
from decimal_lifecycle import Lifecycle,SessionRiskPolicy,Proposal
from alpaca_schema import parse_latest_quotes,fresh,TERMINAL,OwnedIntent,owned_exit_quantity

def _checkpoint(name):
    """No-op crash seam; never an activation callback."""

@contextmanager
def watchdog(test_only):
    timer=None
    if not test_only:
        timer=threading.Timer(15,lambda:os._exit(124));timer.daemon=True;timer.start()
    try:yield
    finally:
        if timer is not None:timer.cancel();timer.join()

class Coordinator:
    def __init__(self,grant,journal,life,dispatch,now_ns):
        c.approved(grant);c.require(type(life) is Lifecycle and type(dispatch) is NativePaperDispatcher and dispatch.lifecycle is life and dispatch.pre_write_context is None,'execution_coordinator_invalid')
        self.g,self.j,self.life,self.dispatch,self.now=grant,journal,life,dispatch,now_ns
        dispatch.pre_write_context=self.write_controls
    @contextmanager
    def write_controls(self):
        self.j.active(write=True);b=self.j.pre.bundle
        with ExitStack() as stack:
            # No UPDATE: acquire stop-control exclusion while preserving old bytes.
            for role in ('runner','decision','source'):
                stack.enter_context(inventory._database(b.paths[role],b._pinned[role],write=True))
            self.j.check();yield
    def final_write_check(self,method,path,body):
        raw,snapshot,now=self.dispatch._gate();quotes=parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=now)
        c.require(len(quotes)==1 and quotes[0].symbol=='NVDA','execution_quote_required');q=quotes[0]
        c.require((q.ask-q.bid)/((q.ask+q.bid)/2)<=Decimal('.002') and len(snapshot.positions)<=1 and all(p.symbol=='NVDA' and 0<p.quantity<=1 for p in snapshot.positions),'execution_position_or_spread')
        if method=='POST':
            side=body['side'];qty,price=Decimal(body['qty']),Decimal(body['limit_price'])
            c.require(body['client_order_id']==c.cid(self.g,side) and 0<qty<=1,'execution_owned_quantity')
            if side=='buy':c.require(qty==1 and not snapshot.positions and price==q.ask and price<=Decimal(self.g.max_notional_usd),'execution_entry_envelope')
            else:c.require(price==q.bid,'execution_exit_price')
        else:
            order=next((o for o in snapshot.orders if '/v2/orders/'+o.id==path),None)
            c.require(order is not None and order.client_order_id in (c.cid(self.g,'buy'),c.cid(self.g,'sell')),'execution_cancel_owned')
            with _read(self.life.path) as db:row=db.execute('SELECT claimed FROM intents WHERE cid=?',(order.client_order_id,)).fetchone()
            c.require(row is not None and now-row[0]>=self.g.cancel_after_ns and now-order.submitted_ns>=self.g.cancel_after_ns,'execution_cancel_early')
    def act(self):
        self.j.active();raw,snapshot,now=self.dispatch._gate()
        state=self.life.status();rows=state['intents'];allowed={c.cid(self.g,'buy'),c.cid(self.g,'sell')}
        c.require(all(r['cid'] in allowed for r in rows),'execution_foreign_native_history')
        with _read(self.life.path) as db:cancels={r[0] for r in db.execute('SELECT cid FROM cancels')}
        bycid={o.client_order_id:o for o in snapshot.orders}
        for row in rows:
            o=bycid.get(row['cid']);c.require(o is not None,'execution_owned_missing')
            if o.status not in TERMINAL:
                if o.status!='pending_cancel' and row['cid'] not in cancels and now-row['claimed']>=self.g.cancel_after_ns and now-o.submitted_ns>=self.g.cancel_after_ns:
                    if self.dispatch.guard.request_budget_remaining(now_ns=now)<1:return
                    self.life.claim_cancel(row['cid'],now_ns=self.now());self.dispatch.cancel(row['cid'])
                return
        q=parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=now)[0]
        buy=next((r for r in rows if r['side']=='buy'),None);sell=next((r for r in rows if r['side']=='sell'),None)
        if buy is not None:
            if sell is not None or bycid[buy['cid']].filled==0:return
            intents=tuple(OwnedIntent(self.life.binding,r['cid'],r['symbol'],r['side'],Decimal(r['quantity'])) for r in rows)
            qty=owned_exit_quantity(snapshot,intents=intents,symbol='NVDA',now_ns=now)
            c.require(0<qty<=1,'execution_owned_quantity')
            if self.dispatch.guard.request_budget_remaining(now_ns=now)<2:return
            cid=c.cid(self.g,'sell');self.life.claim(Proposal('execution-exit-'+cid,cid,'NVDA','sell',qty,q.bid),now_ns=self.now());_checkpoint('execution_exit_claimed');self.dispatch.submit(cid);return
        c.require(not self.j.state()['entry'] and not snapshot.positions and not snapshot.orders,'execution_initial_flat_required')
        c.require(q.symbol=='NVDA' and q.ask<=Decimal(self.g.max_notional_usd) and (q.ask-q.bid)/((q.ask+q.bid)/2)<=Decimal('.002'),'execution_entry_quote')
        if self.dispatch.guard.request_budget_remaining(now_ns=now)<2:return
        self.j.record('ENTRY',{});cid=c.cid(self.g,'buy');self.life.claim(Proposal('execution-entry-'+cid,cid,'NVDA','buy',Decimal(1),q.ask),now_ns=self.now());_checkpoint('execution_entry_claimed');self.dispatch.submit(cid)
    def terminal_flat(self):
        raw,snapshot,now=self.dispatch._gate();rows=self.life.status()['intents']
        if not rows or not self.j.state()['entry'] or snapshot.positions:return False
        if {o.client_order_id for o in snapshot.orders}!={r['cid'] for r in rows}:return False
        if not all(o.status in TERMINAL for o in snapshot.orders):return False
        nets={}
        for o in snapshot.orders:nets[o.symbol]=nets.get(o.symbol,Decimal(0))+o.filled*(1 if o.side=='buy' else -1)
        return all(q==0 for q in nets.values())

def fresh_admission(grant,pre,guard,wire,j,now_ns,monotonic_ns):
    started=c.window(grant,now_ns(),admitting=True);mono=monotonic_ns();c.require(type(mono) is int and mono>0,'execution_monotonic_invalid')
    before=pre.anchor._guard_snapshot();guard.admit_cycle(now_ns=started)
    pages=max(1,(len(guard.submitted_client_ids)+99)//100)
    c.require(7+pages+len(guard.reserved_client_ids)<=12,'execution_admission_lookup_budget')
    life=ReadOnlyLifecycle(pre.bundle)
    observation=NativeExecutionObserver(life,guard,CoordinatedWire(wire,guard,now_ns),now_ns=now_ns,monotonic_ns=monotonic_ns).collect()
    c.require(type(observation) is ExecutionObservation,'execution_typed_observation')
    finished=c.window(grant,now_ns(),started,admitting=True);raw=observation.arguments();snapshot=life._parse(raw,finished)
    c.require(not snapshot.positions and not snapshot.orders and snapshot.clock.is_open and observation.session_open_ns<=finished<snapshot.clock.next_close_ns,'execution_fresh_flat_regular_required')
    parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=finished);fresh(observation.observed_ns,finished)
    c.require(started<=observation.observed_ns<=finished and finished-started<=10*c.NS and 0<=monotonic_ns()-mono<=10*c.NS,'execution_admission_stale')
    receipts=tuple(sorted((r.client_order_id,r.observed_ns,r.evidence_digest) for r in guard._last_reconciliation.absence_receipts))
    evidence=RecoveryEvidence(observation,started,finished,wire.request_count,wire.legacy_terminal_digests(guard.submitted_client_ids),receipts)
    after=pre.anchor._guard_snapshot();pre.anchor._guard_delta(before,after,evidence,started,guard);pre.check();guard.check();_checkpoint('execution_evidence_collected')
    j.record('APPLIED',dict(observed_ns=observation.observed_ns,evidence_sha256=observation.native_digest,guard_sha256=c.digest(after)))

def run(*,paths=None,test_only=False,now_ns=time.time_ns,monotonic_ns=time.monotonic_ns,sleep=time.sleep,transport_factory=None,environ=None):
    grant=c.approved(activation.APPROVED_EXECUTION)
    c.require(type(test_only) is bool and (test_only or (paths is None and now_ns is time.time_ns and monotonic_ns is time.monotonic_ns and sleep is time.sleep and transport_factory is None and environ is None)),'execution_production_override')
    c.window(grant,now_ns(),admitting=True)
    pre=Predecessor(grant,paths=paths,test_only=test_only,now_ns=now_ns);j=None
    try:
        with serialized(pre.bundle):
            pre.open_receipt();pre.initial()
            path=str(Path(pre.bundle.paths['seal']).parent/c.JOURNAL_NAME);c.require(test_only or path==c.JOURNAL_PATH,'execution_fixed_path')
            j=Journal(path,grant,pre,now_ns,_checkpoint)
            environment=os.environ if environ is None else environ;key,secret=environment.get('ALPACA_API_KEY'),environment.get('ALPACA_SECRET_KEY');c.require(bool(key) and bool(secret),'execution_credentials_missing')
            b=pre.bundle;guard=SharedAccountGuard(b.paths['legacy'],b.paths['lifecycle'],b.binding,enabled=True,state_database=b.paths['guard'],test_only=test_only,runtime_bundle=b)
            wire=Transport(key,secret,grant=grant,journal=j,test_only=test_only,transport_factory=transport_factory)
            with watchdog(test_only),guard.hold(now_ns=now_ns(),read_only=True):fresh_admission(grant,pre,guard,wire,j,now_ns,monotonic_ns)
            # Constructors and lifecycle writes are strictly after fresh admission.
            life=Lifecycle(b.paths['lifecycle'],b.binding,allowlist=('NVDA',),session_risk_policy=SessionRiskPolicy(4,Decimal('100')))
            dispatch=NativePaperDispatcher(life,guard,wire,enabled=True,now_ns=now_ns,monotonic_ns=monotonic_ns)
            co=Coordinator(grant,j,life,dispatch,now_ns);wire.bind(co)
            next_tick=c.window(grant,now_ns(),j.last)+grant.cadence_ns
            for index in range(grant.max_ticks):
                current=c.window(grant,now_ns(),j.last)
                remaining=max(0,(next_tick-current)/c.NS)
                c.require(next_tick<grant.expires_ns and remaining<=grant.cadence_ns/c.NS,'execution_sleep_outside_scope')
                if remaining>0:sleep(remaining)
                c.window(grant,now_ns(),current)
                with watchdog(test_only),localcontext() as ctx,dispatch.tick_scope():
                    ctx.prec=60;guard.admit_cycle(now_ns=now_ns());j.record('TICK_BEGIN',{})
                    dispatch.observe();_checkpoint('execution_tick_observed')
                    flat=co.terminal_flat()
                    if not flat:co.act()
                    j.record('TICK_END',{})
                    next_tick=j.state()['last_tick']+grant.cadence_ns
                    if flat or index==grant.max_ticks-1:
                        o=dispatch.observation
                        j.record('TERMINAL',dict(flat=flat,observed_ns=o.observed_ns,evidence_sha256=o.native_digest))
                        st=j.state();pre.check();_checkpoint('execution_before_return')
                        return dict(mode='prepared_successor_execution',phase='terminal_flat' if flat else 'manual_handoff',execution_permission='none',automatic_retry=False,owned_exposure='flat_at_final_observation' if flat else 'not_proven_flat',observed_ns=o.observed_ns,counts={k:st[k] for k in ('requests','writes','ticks')},inherited_requests=pre.prior_counts['broker']+pre.admission_gets+st['requests'],predecessor_unchanged=True,research_signal=False)
    finally:
        if j is not None:j.close()
        pre.close()

def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if activation.APPROVED_EXECUTION is None:
        print(json.dumps(dict(mode='prepared_successor_execution',phase='disabled',execution_permission='none',automatic_retry=False)),flush=True);return 0 if args==['--execute-once'] else 2
    try:
        c.require(args==['--execute-once'],'execution_arguments');result=run();print(json.dumps(result,sort_keys=True),flush=True);return 0 if result['phase']=='terminal_flat' else 1
    except Exception as exc:
        from native_observation_collector import CollectionRefused,schema_failure_detail
        result=dict(mode='prepared_successor_execution',phase='manual_handoff',execution_permission='none',automatic_retry=False,owned_exposure='not_proven_flat')
        if type(exc) is CollectionRefused and exc.code=='native_schema_refused':result.update(error_code='native_schema_refused',**schema_failure_detail(exc.schema_code,exc.schema_stage))
        print(json.dumps(result,sort_keys=True),flush=True);return 1
if __name__=='__main__':
    import prepared_execution_worker as canonical
    raise SystemExit(canonical.main())
