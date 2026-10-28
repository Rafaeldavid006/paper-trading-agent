"""Explicit v2 admission/end: terminal-flat only, no historical state normalization."""
from contextlib import ExitStack
from decimal import Decimal
import hashlib,json,os,sys,time
import finite_pilot_contract as c
import finite_root_contract as rc
import runtime_state_bundle as inventory
from finite_pilot_contract import require,digest,NS
from finite_pilot_anchor import _bounded,_json,resolve_approved_bundle
from finite_root_journal import RootJournal,append,load,clean_runner
from finite_recovery_state_report import current_anchor_digest,_anchor_digest
from readonly_runtime_worker import serialized
from native_account_guard import SharedAccountGuard
from native_paper_execution import NativePaperHTTP,NativeExecutionObserver,CoordinatedWire,ExecutionObservation
from finite_observation_worker import ReadOnlyLifecycle
from finite_pilot_transport import _FinalOpener
from native_observation_collector import PAPER_ORIGIN,DATA_ORIGIN,ORDER_PATH,_route
from alpaca_schema import parse_order,reconcile_order,TERMINAL,parse_latest_quotes,fresh


def _execution_policy():
    root=rc.require_root(rc.APPROVED_ROOT)
    return None if root.smoke is None else rc.smoke_execution_policy(root.children[0])
def _stored_order(raw):
    import lifecycle_storage as storage
    order=storage.decode_order(raw)
    require(_execution_policy() is not None or not order.extended_hours,'root_stored_execution_policy_mismatch')
    return order


def history(bundle,rows=None):
    with inventory._database(bundle.paths['lifecycle'],bundle._pinned['lifecycle']) as db:
        h=_anchor_digest(db)
        db.row_factory=__import__('sqlite3').Row
        intents=[dict(x) for x in db.execute('SELECT * FROM intents')]
        dispatch=[dict(x) for x in db.execute('SELECT * FROM native_dispatch')]
        cancels=[dict(x) for x in db.execute('SELECT * FROM cancels')]
    require(len(intents)<=8 and len(dispatch)<=8 and len(cancels)<=8,'root_history_bound')
    ids={i['cid']:i for i in intents};require(len(ids)==len(intents),'root_duplicate_cid')
    require({(r['kind'],r['cid']) for r in dispatch}=={('submit',i) for i in ids}|{('cancel',r['cid']) for r in cancels},'root_dispatch_history_incomplete')
    writes=[item for row in (rows or {}).values() for item in row['write_rows']]
    for r in dispatch:
        require(r['cid'] in ids and r['kind'] in ('submit','cancel'),'root_dispatch_foreign')
        i=ids[r['cid']];require(i['response'] is not None,'root_submission_uncertain')
        raw=json.loads(i['response']);order=_stored_order(raw)
        require(order.client_order_id==i['cid'] and (order.symbol,order.side,order.quantity,order.limit_price)==(i['symbol'],i['side'],Decimal(i['quantity']),Decimal(i['price'])),'root_owned_identity_changed')
        method,target=('POST',r['cid']) if r['kind']=='submit' else ('DELETE','/v2/orders/'+raw['id'])
        require(r['state']==('observed' if r['kind']=='submit' else 'uncertain'),'root_dispatch_outcome_uncertain')
        require(sum(m==method and t==target and at>=r['attempted_ns'] for at,m,t,seq in writes)==1,'root_dispatch_stage_unproven')
    require(len(writes)==len(dispatch),'root_write_history_unmatched')
    return h,intents

class RootReadOnlyTransport:
    paper_origin=PAPER_ORIGIN;data_origin=DATA_ORIGIN;feed='iex'
    def __init__(self,key,secret,*,manifest,journal,now_ns,started_ns,test_only=False,transport_factory=None):
        rc.require_child(manifest);require(type(test_only) is bool and (test_only or transport_factory is None),'root_factory_override')
        self.m,self.j,self.now,self.started=manifest,journal,now_ns,started_ns;self.request_count=0;self.pending=None;self.opened=False;self.scan=[]
        policy=_execution_policy()
        self.wire=(transport_factory or NativePaperHTTP)(key,secret,enabled=True,**({} if policy is None else dict(execution_policy=policy)));self.native=type(self.wire) is NativePaperHTTP
        require(test_only or self.native,'root_reviewed_transport_required')
        if self.native:
            self.wire._opener=_FinalOpener(self.wire._opener,self._open);self.wire._reader._opener=_FinalOpener(self.wire._reader._opener,self._open)
    def window(self):
        rc.require_child(self.m);at=self.now();require(type(at) is int and self.m.starts_ns<=at<self.m.root_binding.expires_ns and 0<=at-self.started<=10*NS,'root_admission_expired')
    def at_wire(self,invoke):
        self.window();self.j._controls();require(self.pending is not None and not self.opened,'root_single_open_required')
        self.j.reserve_broker_request(self.m,'GET',self.pending,admission=True);self.window();self.j._controls();self.opened=True;return invoke()
    def _open(self,opener,req,*args,**kwargs):
        require(self.pending is not None and req.get_method()=='GET' and req.data is None and req.full_url==_route(self.pending)+self.pending,'root_get_only')
        return self.at_wire(lambda:opener.open(req,*args,**kwargs))
    def call(self,method,path,body=None):
        self.window();require(method=='GET' and body is None and self.pending is None and self.request_count<12,'root_get_cap')
        _route(path);require(not path.startswith('/v2/stocks/') or path=='/v2/stocks/quotes/latest?symbols=NVDA&feed=iex','root_quote_scope')
        self.request_count+=1;self.pending=path;self.opened=False
        try:
            result=self.wire.call('GET',path) if self.native else self.at_wire(lambda:self.wire.call('GET',path))
            if path.startswith(ORDER_PATH) and result[0]==200:
                require(type(result[1]) is list and len(self.scan)+len(result[1])<500,'root_history_bound');self.scan.extend(result[1])
            return result
        finally:self.pending=None
    def legacy_terminal_digests(self,ids):
        from native_account_guard import _order_digest
        rows=[(r['client_order_id'],_order_digest(parse_order(r),r)) for r in self.scan if r.get('client_order_id') in ids]
        require(len(rows)==len(ids) and {r[0] for r in rows}==set(ids),'root_legacy_history_incomplete');return tuple(sorted(rows))


def validate_evidence(m,*,bundle,guard,journal,now_ns,environ,test_only=False,broker_factory=None):
    rc.require_child(m);journal._controls();guard.check();started=now_ns();guard.admit_cycle(now_ns=started)
    with journal._db() as db:_,rows,_,_=load(db,rc.APPROVED_ROOT)
    _,intents=history(bundle,rows)
    pending=sum(_stored_order(json.loads(i['response'])).status not in TERMINAL for i in intents)
    pages=max(1,(len(intents)+len(guard.submitted_client_ids)+99)//100)
    require(7+pages+len(guard.reserved_client_ids)+pending<=12,'root_history_lookup_budget')
    key,secret=environ.get('ALPACA_API_KEY'),environ.get('ALPACA_SECRET_KEY');require(bool(key) and bool(secret),'credentials_missing')
    wire=RootReadOnlyTransport(key,secret,manifest=m,journal=journal,now_ns=now_ns,started_ns=started,test_only=test_only,transport_factory=broker_factory)
    policy=_execution_policy();life=ReadOnlyLifecycle(bundle)
    if policy is not None:life.execution_policy=policy
    observation=NativeExecutionObserver(life,guard,CoordinatedWire(wire,guard,now_ns),now_ns=now_ns,**({} if policy is None else dict(execution_policy=policy))).collect()
    require(type(observation) is ExecutionObservation,'root_typed_observation_required')
    finished=now_ns();raw=observation.arguments();snapshot=life._parse(raw,finished)
    require(not snapshot.positions and all(o.status in TERMINAL for o in snapshot.orders),'root_flat_terminal_required')
    bycid={o.client_order_id:o for o in snapshot.orders};require(set(bycid)=={i['cid'] for i in intents},'root_foreign_or_missing_order')
    nets={}
    for i in intents:
        old=_stored_order(json.loads(i['response']));new=bycid[i['cid']];reconcile_order(old,new)
        require(next(o['id'] for o in raw['orders'] if o['client_order_id']==i['cid'])==json.loads(i['response'])['id'],'root_broker_id_changed')
        nets[new.symbol]=nets.get(new.symbol,Decimal(0))+(new.filled if new.side=='buy' else -new.filled)
    require(all(v==0 for v in nets.values()),'root_owned_residual_exposure')
    parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=finished);fresh(observation.observed_ns,finished)
    require(0<=finished-started<=10*NS,'root_fresh_regular_observation')
    if policy is None:
        require(snapshot.clock.is_open and observation.session_open_ns<=finished<snapshot.clock.next_close_ns,'root_fresh_regular_observation')
    else:
        from execution_session_policy import validate_execution
        validate_execution(snapshot,finished,policy)
    guard.check();journal._controls();bundle.verify()
    receipts=tuple(sorted((r.client_order_id,r.observed_ns,r.evidence_digest) for r in guard._last_reconciliation.absence_receipts))
    return c.RecoveryEvidence(observation,started,finished,wire.request_count,wire.legacy_terminal_digests(guard.submitted_client_ids),receipts)

class AdmissionJournal(RootJournal):
    def _time(self,m,previous=0):
        at=self._now(m,previous);require(at<m.root_binding.expires_ns,'root_admission_expired');return at
    def _prestate(self,db,m):
        root=rc.require_root(rc.APPROVED_ROOT);self._binding_row(db,m)
        present=db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(rc.ROOTS,)).fetchone()
        if not present:
            require(m.root_binding.index==0 and current_anchor_digest(db)==root.current_anchor_sha256,'root_initial_anchor_changed')
            from finite_pilot_sessions import _load as v1load
            old=rc.validate_reference(root);_,rows,_,_=v1load(db,old);r=rows[old.pilot_id]
            require(r['closed'] and r['clean'] and r['terminal_digest']==root.predecessor_terminal_sha256,'root_v1_bridge_required')
            return dict(cohort=root.current_cohort_sha256,runner=root.current_runner_sha256,protected=root.protected_state_sha256,predecessor=root.predecessor_terminal_sha256),{}
        _,rows,total,_=load(db,root)
        require(m.root_binding.index==1 and m.pilot_id not in rows,'root_attempt_consumed')
        prior=rows[root.children[0].pilot_id]['ended'];require(prior is not None,'root_predecessor_not_ended')
        require(total['ticks']<root.max_ticks and (root.smoke is not None or total['writes']<root.max_writes) and (root.smoke is not None or total['sources']<root.max_sources) and root.max_broker-total['broker']>=12,'root_remaining_budget_required')
        return dict(prior,predecessor=digest(prior)),rows
    def admit(self,m,*,guard,validate=validate_evidence,environ=None,test_only=False,broker_factory=None):
        import finite_root_worker as canonical
        rc.require_child(m);require(validate is canonical.validate_evidence,'root_canonical_validator_required')
        require(type(guard) is SharedAccountGuard and guard.binding==self.bundle.binding and guard.state_database==self.bundle.paths['guard'] and guard.legacy_database==self.bundle.paths['legacy'] and guard.lifecycle_database==self.bundle.paths['lifecycle'],'root_guard_mismatch')
        require(type(test_only) is bool and (test_only or (environ is None and broker_factory is None)),'root_test_override')
        root=rc.APPROVED_ROOT;require(root.max_broker>=12,'root_remaining_budget_required')
        with serialized(self.bundle):
            self._verify(m)
            with self._db() as db:self._prestate(db,m)
            with guard.hold(now_ns=self._time(m),read_only=True):
                self._controls();guard.admit_cycle(now_ns=self._time(m))
                with self._db() as db:state,rows=self._prestate(db,m);anchor=current_anchor_digest(db)
                with inventory._database(self.bundle.paths['runner'],self.bundle._pinned['runner']) as db:old=clean_runner(inventory._singleton(db,'runner_control'))
                hist,_=history(self.bundle,rows);require(not rows or hist==state['history'],'root_history_changed')
                require(self.cohort_state_digest()==state['cohort'] and self.protected_state_digest()==state['protected'] and digest(old)==state['runner'],'root_prestate_changed')
                before=self._guard_snapshot();application=self.application_state_digest();at=self._time(m);new=(old[0],at/NS,*old[2:]);require(new[1]>=old[1],'root_clock_regression')
                p=dict(old=list(old),new=list(new),cohort=state['cohort'],protected=state['protected'],guard=digest(before),history=hist,predecessor=state['predecessor'],anchor=anchor)
                with self._db(write=True) as db:
                    self._prestate(db,m);require(current_anchor_digest(db)==anchor,'root_anchor_changed');self._time(m,at)
                    if m.root_binding.index==0:
                        for sql in rc.SCHEMA.values():db.execute(sql)
                        db.execute('INSERT INTO '+rc.ROOTS+' VALUES(?,?,?)',(root.root_id,digest(root.document()),_json(root.document())))
                    db.execute('INSERT INTO '+rc.SESSIONS+' VALUES(?,?,?,?)',(m.pilot_id,root.root_id,m.root_binding.index,_json(m.document())))
                    if m.root_binding.index==0:append(db,m.pilot_id,'ROOT_IMPORTED',at,dict(predecessor=root.predecessor_terminal_sha256,anchor=root.current_anchor_sha256))
                    append(db,m.pilot_id,'PREPARED',at,p);_,_,_,prefix=load(db,root)
                self._checkpoint('root_prepared')
                evidence=validate(m,bundle=self.bundle,guard=guard,journal=self,now_ns=self.now_ns,environ=os.environ if environ is None else environ,test_only=test_only,broker_factory=broker_factory)
                require(type(evidence) is c.RecoveryEvidence and type(evidence.observation) is ExecutionObservation,'root_typed_evidence_required')
                self._checkpoint('root_validated')
                with ExitStack() as stack:
                    dbs={role:stack.enter_context(inventory._database(self.bundle.paths[role],self.bundle._pinned[role],write=True,public_read=role=='anchor')) for role in ('runner','decision','lifecycle','source','guard','anchor')}
                    db=dbs['anchor'];self._verify(m);self._controls();guard.check()
                    require(self.protected_state_digest()==p['protected'] and self.application_state_digest()==application and history(self.bundle,load(db,root)[1])[0]==hist,'root_poststate_changed')
                    require(inventory._singleton(dbs['runner'],'runner_control')==old,'root_runner_changed')
                    _,rows,_,events=load(db,root);r=rows[m.pilot_id]
                    require(r['prepared']==p and events[:len(prefix)]==prefix and len(events)-len(prefix)==evidence.request_count and all(e[1]==m.pilot_id and e[3]=='BROKER' and json.loads(e[5])['admission'] is True for e in events[len(prefix):]) and r['admission_gets']==evidence.request_count,'root_admission_history_changed')
                    now=self._time(m,at);require(at<=evidence.started_ns<=evidence.observation.observed_ns<=evidence.finished_ns<=now and now-evidence.started_ns<=10*NS,'root_evidence_stale')
                    after=self._guard_snapshot();self._guard_delta(before,after,evidence,at,guard)
                    pinned=current_anchor_digest(db)
                    self._checkpoint('root_before_cas')
                    result=dbs['runner'].execute('UPDATE runner_control SET heartbeat=? WHERE id=? AND heartbeat=? AND dirty=? AND stopped=? AND attention IS ? AND stage=?',(new[1],*old))
                    require(result.rowcount==1,'root_cas_failed');self._time(m,now);self._checkpoint('root_cas_before_commit');dbs['runner'].commit();self._checkpoint('root_cas_committed')
                    require(inventory._singleton(dbs['runner'],'runner_control')==new and current_anchor_digest(db)==pinned,'root_cas_poststate_changed')
                    final=self._time(m,now);require(final-evidence.started_ns<=10*NS,'root_evidence_stale')
                    append(db,m.pilot_id,'APPLIED',final,dict(prepared_digest=digest(p),evidence=evidence.observation.native_digest,guard=digest(after),history=hist,observed_ns=evidence.observation.observed_ns));load(db,root)
                    self._checkpoint('root_applied_before_commit');db.commit();self._checkpoint('root_applied')
        return dict(phase='admitted')


def record(action,*,root_index,paths=None,test_only=False,now_ns=time.time_ns):
    m=rc.child(root_index);require(action in ('halt','end'),'root_terminal_action');root=rc.APPROVED_ROOT
    bundle=resolve_approved_bundle(m,paths=paths,test_only=test_only);j=RootJournal(bundle,now_ns);j._verify(m)
    with serialized(bundle),ExitStack() as stack:
        dbs={role:stack.enter_context(inventory._database(bundle.paths[role],bundle._pinned[role],write=True,public_read=role=='anchor')) for role in ('runner','decision','lifecycle','source','guard','anchor')}
        db=dbs['anchor'];j._binding_row(db,m);_,rows,_,_=load(db,root);r=rows[m.pilot_id]
        at=now_ns();require(type(at) is int and max(m.starts_ns,r['last_ns'])<=at<m.root_binding.close_expires_ns and at<root.expires_ns,'root_terminal_window')
        if action=='halt':
            require(not r['halted'] and r['ended'] is None,'root_terminal_consumed');append(db,m.pilot_id,'HALT',at,{})
        else:
            require(at>=m.expires_ns and r['halted'] and r['applied'] is not None and not r['dirty'] and r['ended'] is None,'root_structured_end_required');j._controls()
            hist,_=history(bundle,rows);runner=clean_runner(inventory._singleton(dbs['runner'],'runner_control'))
            payload=dict(cohort=j.cohort_state_digest(),runner=digest(runner),protected=j.protected_state_digest(),history=hist,local_only=True)
            at2=now_ns();require(type(at2) is int and at<=at2<m.root_binding.close_expires_ns and at2<root.expires_ns,'root_terminal_window')
            append(db,m.pilot_id,'LOCAL_ENDED_STRUCTURED',at2,payload)
        load(db,root);db.commit()
    return dict(phase='halted' if action=='halt' else 'local_ended_structured',broker_flatness_verified=False,provider_requests=0)

def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    try:
        require(len(args)==2 and args[0] in ('--halt','--end') and args[1] in ('--root-child=0','--root-child=1'),'root_arguments')
        result=record(args[0][2:],root_index=int(args[1][-1]));print(json.dumps(dict(mode='finite_root_terminal',**result)),flush=True);return 0
    except Exception:print(json.dumps(dict(mode='finite_root_terminal',phase='refused',broker_flatness_verified=False)),flush=True);return 1
if __name__=='__main__':
    import finite_root_worker as canonical
    raise SystemExit(canonical.main())
