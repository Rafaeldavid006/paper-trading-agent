"""Pure consumer of owner-held v2 windows. No source writes, polling or orders.

Transitions are PROPOSED checkpoint values: caller must durably CAS old→new with
its downstream evidence before using next cursor. Hashes are consistency, not
provider authentication. Missing retained prefixes never silently reseed.
"""
import base64
from hashlib import sha256
from pathlib import Path
import re
import sys

HERE=Path(__file__).resolve().parent
for p in (HERE/'bridge',HERE/'bridge'/'vendor'/'stream_contract'):
    if str(p) not in sys.path:sys.path.insert(0,str(p))
from retained_stream import canonical, copy, decode, Refused, require, Capture, _project_body, MAX_BODY
from live_source_protocol import control
from pipeline import run as research_run

ZERO='0'*64
MAX_PAGE=4*1024*1024
MAX_STATE=32*1024*1024
STATE_SCHEMA='resident-research-consumer-state-v2'
SNAPSHOT_KEYS={'schema','policy','policy_sha256','predecessor','head_seq','head_sha256','anchor_seq',
 'anchor_sha256','purged_records','requested_after_seq','from_seq','from_sha256','next_seq','page_sha256',
 'rows','has_more','retention_gap','provider_coverage_complete','consumer_cursor_committed','order_permission'}
EVENTS={'owner_boot','attempt_reserved','subscription_accepted','control','source_terminal','transport_gap','data','pulse','owner_stopped'}
SOURCES=('iex','news')


def digest(value):return sha256(canonical(value)).hexdigest()
def h(value):require(type(value) is str and re.fullmatch('[0-9a-f]{64}',value),'hash_shape');return value
def integer(value):require(type(value) is int and 0<=value<2**63,'integer_shape');return value


def initial_state(*,expected_policy,expected_predecessor,evidence_mode):
    """Explicit known genesis only. Caller supplies expected source binding.
    Never learn a new policy or a retention anchor from an arbitrary incoming page.
    """
    p,pre=copy([expected_policy,expected_predecessor])
    require(type(p) is dict and p.get('version')==2 and p.get('predecessor')==pre,'policy_predecessor')
    require(type(p.get('symbols')) is list and p['symbols'] and p.get('news')=='*','policy_scope')
    require(evidence_mode in ('fixture','imported_capture'),'evidence_mode')
    return {'schema':STATE_SCHEMA,'policy':p,'policy_sha256':digest(p),'predecessor':pre,
        'evidence_mode':evidence_mode,'cursor_seq':0,'cursor_sha256':ZERO,'high_ns':0,
        'observed_head_seq':0,'observed_head_sha256':ZERO,'pending_heads':[],'caught_up':False,
        'phases':{s:'idle' for s in SOURCES},'last_control':{s:None for s in SOURCES},
        'events':[],'quotes':[],'gaps':[],'control_receipts':[],
        'processed_pages':0,'processed_bytes':0,'consumer_cursor_committed':False,'order_permission':'none'}


def checkpoint_bytes(state):
    """Serialization only; does not persist, install or advance a source cursor."""
    raw=canonical(state);require(len(raw)<=MAX_STATE,'consumer_projection_budget_exhausted')
    return raw,sha256(raw).hexdigest()


def restore_checkpoint(raw,*,expected_sha256,expected_policy_sha256,evidence_mode):
    require(type(raw) is bytes and len(raw)<=MAX_STATE,'checkpoint_bound')
    require(sha256(raw).hexdigest()==h(expected_sha256),'checkpoint_hash_mismatch')
    state=decode(raw)
    _validate_state(state)
    require(state['policy_sha256']==h(expected_policy_sha256) and state['evidence_mode']==evidence_mode,'checkpoint_binding')
    return state


def _validate_state(s):
    require(type(s) is dict and s.get('schema')==STATE_SCHEMA,'checkpoint_schema')
    require(s.get('evidence_mode') in ('fixture','imported_capture') and digest(s.get('policy'))==s.get('policy_sha256'),'checkpoint_policy')
    require(s['policy'].get('predecessor')==s.get('predecessor'),'checkpoint_predecessor')
    for k in ('cursor_seq','high_ns','observed_head_seq','processed_pages','processed_bytes'):integer(s[k])
    h(s['cursor_sha256']);h(s['observed_head_sha256'])
    require(s['cursor_seq']<=s['observed_head_seq'] and type(s['caught_up']) is bool,'checkpoint_cursor')
    require(type(s.get('pending_heads')) is list and all(type(x) is dict and set(x)=={'seq','sha256'}
        and type(x['seq']) is int and s['cursor_seq']<x['seq']<=s['observed_head_seq']
        and type(x['sha256']) is str and re.fullmatch('[0-9a-f]{64}',x['sha256']) for x in s['pending_heads']),'checkpoint_pending_heads')
    require(len({x['seq'] for x in s['pending_heads']})==len(s['pending_heads']),'checkpoint_duplicate_heads')
    require(set(s['phases'])==set(SOURCES) and set(s['last_control'])==set(SOURCES),'checkpoint_sources')
    require(all(x in ('idle','await_connected','await_authenticated','await_subscription','subscribed','gap','terminal','stopped','quarantined') for x in s['phases'].values()),'checkpoint_phase')
    require(all(type(s[k]) is list for k in ('events','quotes','gaps','control_receipts')),'checkpoint_evidence')
    require(s.get('consumer_cursor_committed') is False and s.get('order_permission')=='none','checkpoint_authority')


def _provider_frame(raw,source,ref,state):
    """Classify raw control/business content; summary.kind is never consulted."""
    parsed=decode(raw,prices=True)
    if type(parsed) is list and len(parsed)==1 and type(parsed[0]) is dict and parsed[0].get('T') in ('success','subscription'):
        stage=state['phases'][source]
        expected={'await_connected':'connected','await_authenticated':'authenticated','await_subscription':'subscription'}.get(stage)
        require(expected is not None,'unexpected_provider_control')
        control(raw,expected,source,state['policy'])
        state['phases'][source]={'connected':'await_authenticated','authenticated':'await_subscription','subscription':'subscribed'}[expected]
        state['last_control'][source]=expected
        state['control_receipts'].append({**ref,'source':source,'phase':expected})
        return {},[]
    require(state['phases'][source]=='subscribed','business_frame_before_subscription')
    return _project_body(raw,source,state['policy'],ref['received_at_ns'],
        {k:ref[k] for k in ('sequence','record_sha256','received_at_ns')},ref['body_sha256'],state['evidence_mode'])


def apply_snapshot(snapshot, prior_state):
    """Validate an entire bounded page then propose one atomic local transition.
    On any integrity/cursor/budget error the input state remains unchanged.
    Provider-invalid bodies are quarantined but valid chain progress is retained.
    """
    page,state=copy([snapshot,prior_state])
    _validate_state(state)
    require(len(canonical(page))<=MAX_PAGE+128*1024,'snapshot_envelope_bound')
    before=digest(state)
    require(type(page) is dict and set(page)==SNAPSHOT_KEYS and page['schema']=='resident-source-window-v2','snapshot_schema')
    require(page['policy']==state['policy'] and page['policy_sha256']==state['policy_sha256'] and
        page['predecessor']==state['predecessor'],'snapshot_binding')
    for k in ('head_seq','anchor_seq','purged_records','requested_after_seq','from_seq','next_seq'):integer(page[k])
    for k in ('head_sha256','anchor_sha256','from_sha256','page_sha256'):h(page[k])
    for k in ('has_more','retention_gap','provider_coverage_complete','consumer_cursor_committed'):require(type(page[k]) is bool,'snapshot_boolean')
    require(page['provider_coverage_complete'] is False and page['consumer_cursor_committed'] is False and page['order_permission']=='none','snapshot_authority')
    require(page['requested_after_seq']==state['cursor_seq'],'consumer_cursor_mismatch')
    require(page['anchor_seq']==page['purged_records']<=page['head_seq'],'retention_anchor_shape')
    require(page['retention_gap']==(state['cursor_seq']<page['anchor_seq']),'retention_gap_flag_mismatch')
    require(not page['retention_gap'],'retention_gap_requires_explicit_recovery')
    require(page['from_seq']==state['cursor_seq'] and page['from_sha256']==state['cursor_sha256'],'consumer_prefix_hash_mismatch')
    if page['anchor_seq']==state['cursor_seq']:require(page['anchor_sha256']==state['cursor_sha256'],'retention_anchor_mismatch')
    require(page['head_seq']>=state['observed_head_seq'],'source_head_regressed')
    if page['head_seq']==state['observed_head_seq']:require(page['head_sha256']==state['observed_head_sha256'],'source_head_changed')
    pending={x['seq']:x['sha256'] for x in state['pending_heads']}
    if page['head_seq'] in pending:require(pending[page['head_seq']]==page['head_sha256'],'advertised_head_changed')
    if page['head_seq']>state['cursor_seq']:pending[page['head_seq']]=page['head_sha256']
    require(type(page['rows']) is list and len(page['rows'])<=64,'snapshot_row_bound')
    seq,head,high=state['cursor_seq'],state['cursor_sha256'],state['high_ns']
    additions,quote_additions,gap_additions={},[],[]
    raw_bytes=0
    for row in page['rows']:
        require(type(row) is dict and set(row)=={'seq','prev','record','hash'},'row_shape')
        require(type(row['seq']) is int and row['seq']==seq+1 and row['prev']==head,'row_sequence')
        encoded=canonical({k:row[k] for k in ('seq','prev','record')});raw_bytes+=len(encoded)
        require(len(encoded)<=2*1024*1024 and raw_bytes<=MAX_PAGE and digest({k:row[k] for k in ('seq','prev','record')})==row['hash'],'row_hash_or_bound')
        r=row['record'];require(type(r) is dict and r.get('event') in EVENTS,'record_kind')
        at=integer(r.get('at_ns'));require(high<=at,'record_clock')
        source=r.get('source');require(source is None or source in SOURCES,'record_source')
        ref={'sequence':row['seq'],'record_sha256':row['hash'],'received_at_ns':at}
        event=r['event']
        if event=='owner_boot':
            for name in SOURCES:
                if state['phases'][name]!='terminal':state['phases'][name]='idle'
                state['last_control'][name]=None
            if seq or r.get('previous_owner_active'):
                gap_additions.append({**ref,'source':'all','gap_reason':'owner_restart_unfilled','continuity_invalid':False})
        elif event=='attempt_reserved':
            require(source in SOURCES and state['phases'][source]!='terminal','attempt_after_terminal')
            state['phases'][source]='await_connected';state['last_control'][source]=None
        elif event=='data':
            require(source in SOURCES,'data_source')
            try:raw=base64.b64decode(r['body_base64'],validate=True)
            except (ValueError,KeyError,TypeError) as error:raise Refused('body_base64') from error
            require(len(raw)<=MAX_BODY and sha256(raw).hexdigest()==r.get('body_sha256'),'body_integrity')
            body_ref={**ref,'body_sha256':r['body_sha256']}
            # Work on a temporary copy of protocol tracking too: malformed
            # mixed/partial frames cannot certify an authentication transition.
            protocol_copy={**state,'phases':dict(state['phases']),'last_control':dict(state['last_control']),
                'control_receipts':list(state['control_receipts'])}
            try:events,quotes=_provider_frame(raw,source,body_ref,protocol_copy)
            except (ValueError,TypeError,KeyError,OverflowError):
                state['phases'][source]='quarantined'
                gap_additions.append({**body_ref,'source':source,'gap_reason':'invalid_provider_frame','continuity_invalid':False})
            else:
                for k in ('phases','last_control','control_receipts'):state[k]=protocol_copy[k]
                for version,article in events.items():additions.setdefault(version,article)
                quote_additions.extend(quotes)
        elif event=='control':
            require(source in SOURCES and r.get('phase')==state['last_control'][source],'unbacked_control_record')
        elif event=='subscription_accepted':
            require(source in SOURCES and state['phases'][source]=='subscribed','unbacked_subscription_record')
        elif event in ('transport_gap','source_terminal'):
            require(source in SOURCES,'gap_source')
            state['phases'][source]='terminal' if event=='source_terminal' else 'gap'
            gap_additions.append({**ref,'source':source,'gap_reason':event,'continuity_invalid':event=='source_terminal'})
        elif event=='owner_stopped':
            for name in SOURCES:
                if state['phases'][name]!='terminal':state['phases'][name]='stopped'
            gap_additions.append({**ref,'source':'all','gap_reason':'owner_stopped','continuity_invalid':False})
        seq,head,high=row['seq'],row['hash'],at
        if seq in pending:
            require(head==pending[seq],'previously_advertised_head_mismatch')
            del pending[seq]
    require(seq==page['next_seq'] and head==page['page_sha256'] and seq<=page['head_seq'],'page_boundary_mismatch')
    require(page['has_more']==(seq<page['head_seq']),'page_has_more_mismatch')
    require(bool(page['rows']) or not page['has_more'],'empty_nonterminal_page')
    if not page['has_more']:require(head==page['head_sha256'],'caught_up_head_mismatch')
    known={e['version'] for e in state['events']}
    new_events=[e for v,e in additions.items() if v not in known]
    state['events'].extend(new_events);state['quotes'].extend(quote_additions);state['gaps'].extend(gap_additions)
    state.update(cursor_seq=seq,cursor_sha256=head,high_ns=high,observed_head_seq=page['head_seq'],
        observed_head_sha256=page['head_sha256'],caught_up=not page['has_more'],
        processed_pages=state['processed_pages']+1,processed_bytes=state['processed_bytes']+raw_bytes,
        pending_heads=[{'seq':seq,'sha256':head} for seq,head in sorted(pending.items())])
    _validate_state(state);encoded,checkpoint_sha=checkpoint_bytes(state)
    return {'schema':'resident-research-proposed-transition-v2','prior_checkpoint_sha256':before,
        'next_checkpoint_sha256':checkpoint_sha,'next_state':state,
        'delta':{'events':new_events,'quotes':quote_additions,'gaps':gap_additions},
        'consumer_cursor_committed':False,'requires_atomic_consumer_checkpoint_and_evidence_commit':True,
        'order_permission':'none'}


def research_capture(state):
    state=copy(state)
    _validate_state(state)
    receipt={'schema':'resident-source-research-projection-v2','policy':copy(state['policy']),
        'policy_sha256':state['policy_sha256'],'predecessor':copy(state['predecessor']),
        'consumer_seq':state['cursor_seq'],'consumer_head_sha256':state['cursor_sha256'],
        'observed_source_head_seq':state['observed_head_seq'],'observed_source_head_sha256':state['observed_head_sha256'],
        'consumer_caught_up':state['caught_up'],'last_processed_at_ns':state['high_ns'],'evidence_mode':state['evidence_mode'],
        'synthetic':state['evidence_mode']=='fixture','provider_coverage_complete':False,
        'rest_cursor_advanced':False,'consumer_cursor_committed':False,'order_permission':'none',
        'source_phases':copy(state['phases']),'checkpoint_sha256':digest(state)}
    events=copy(state['events'])
    for event in events:event['observed_gaps']=copy(state['gaps'])
    return Capture(receipt,tuple(events),tuple(copy(state['quotes'])),tuple(copy(state['gaps'])),tuple(copy(state['control_receipts'])))


def research_batch(state,*,research_registry,decision_ns,market_session='unknown',positions=()):
    """No hypothesis evaluation on a partial page prefix; v2 schema stays v2.
    Source catchup only means this known owner head, never full market coverage.
    """
    state,research_registry=copy([state,research_registry])
    _validate_state(state)
    require(type(decision_ns) is int and state['high_ns']<=decision_ns<2**63,'decision_before_consumed_frontier')
    require(not positions,'owned_exit_monitor_is_separate_from_entry_research')
    capture=research_capture(state)
    if not state['caught_up']:
        return {'schema':'resident-source-research-batch-v2','capture_receipt':capture.receipt,
            'observations':[],'decision_at_ns':decision_ns,'blocked_reason':'consumer_page_incomplete',
            'consumer_cursor_committed':False,'order_permission':'none'}
    if any(state['phases'][s]!='subscribed' for s in SOURCES):
        return {'schema':'resident-source-research-batch-v2','capture_receipt':capture.receipt,
            'observations':[],'decision_at_ns':decision_ns,'blocked_reason':'source_not_subscribed_in_retained_protocol',
            'consumer_cursor_committed':False,'order_permission':'none'}
    result=research_run(capture,bindings=research_registry['bindings'],graph=research_registry['graph'],
        identities=research_registry['identities'],revisions=research_registry.get('revisions',()),
        positions=positions,decision_ns=decision_ns,market_session=market_session)
    result['schema']='resident-source-research-batch-v2'
    result['consumer_cursor_committed']=False
    result['requires_atomic_consumer_checkpoint_and_evidence_commit']=True
    return result
