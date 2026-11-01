"""Frozen experimental long-continuation/exit observations. NO broker or risk authority.
Numerical settings are testable hypotheses, not claimed edge or price forecasts.
"""
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, Context, localcontext, ROUND_HALF_EVEN
from functools import wraps
from hashlib import sha256
import re
from retained_stream import NS, Refused, require, copy, canonical, SYMBOL

D=Decimal

@dataclass(frozen=True)
class Policy:
    version: str='long-continuation-observation-v2-fixed-decimal'
    arithmetic_precision: int=50
    max_quote_age_ns: int=5*NS
    max_receipt_age_ns: int=5*NS
    max_delivery_lag_ns: int=2*NS
    max_event_age_ns: int=15*60*NS
    max_news_delivery_lag_ns: int=120*NS
    min_observations: int=3
    min_window_ns: int=10*NS
    max_window_ns: int=90*NS
    max_spread_bps: int=25
    min_momentum_bps: int=15
    max_chase_bps: int=150
    max_pullback_bps: int=30
    entry_slippage_bps: int=5
    stop_bps: int=80
    trail_bps: int=60
    max_holding_ns: int=20*60*NS
    decision_ttl_ns: int=2*NS

    def validate(self):
        require(self.version=='long-continuation-observation-v2-fixed-decimal','policy_version')
        for k,v in asdict(self).items():
            if k!='version': require(type(v) is int and 0<v<2**63,'policy_'+k)
        require(self.arithmetic_precision==50,'fixed_arithmetic_required')
        require(2<=self.min_observations<=10000 and self.min_window_ns<=self.max_window_ns and
                self.min_momentum_bps<self.max_chase_bps and all(getattr(self,k)<=10000 for k in
                ('max_spread_bps','max_chase_bps','max_pullback_bps','entry_slippage_bps','stop_bps','trail_bps')),'policy_range')
        return self

    @property
    def digest(self):
        self.validate(); return sha256(canonical(asdict(self))).hexdigest()


def decimal(value, field):
    require(type(value) in (str,int),'decimal_'+field)
    try: result=D(value)
    except InvalidOperation as e: raise Refused('decimal_'+field) from e
    require(result.is_finite() and D('1e-9')<=result<=D('1e12'),'decimal_'+field)
    return result


def _quote(q):
    require(type(q) is dict and q.get('feed')=='iex' and type(q.get('symbol')) is str and SYMBOL.fullmatch(q['symbol']),'quote_shape')
    bid,ask=decimal(q.get('bid'),'bid'),decimal(q.get('ask'),'ask')
    require(bid<=ask,'crossed_quote')
    for k in ('bid_size','ask_size','provider_at_ns','received_at_ns'):
        require(type(q.get(k)) is int and q[k]>0,'quote_'+k)
    require(q['provider_at_ns']<=q['received_at_ns'],'future_quote')
    require(type(q.get('synthetic')) is bool and type(q.get('evidence')) is dict,'quote_provenance')
    return bid,ask


def _envelope(*, action, symbol, at, policy, reasons, market=None, evidence=None, metrics=None, plan=None, synthetic=False):
    result={'schema':'broad-paper-observation-v1','policy_sha256':policy.digest,'policy_version':policy.version,
        'generated_at_ns':at,'expires_at_ns':at+policy.decision_ttl_ns,'symbol':symbol,'action':action,
        'direction':'long','reasons':reasons,'market':market,'evidence':evidence or {},
        'metrics':metrics or {},'plan':plan,'synthetic':synthetic,'order_permission':'none',
        'requires_independent_risk_admission':True,'source_coverage':'IEX subscribed symbols only; not consolidated NBBO',
        'strategy_edge_established':False}
    result['decision_id']=sha256(canonical(result)).hexdigest()
    return result


def _quote_state(quotes,symbol,at,p):
    observations=[];usable=[]
    for q in quotes:
        if q.get('symbol')!=symbol: continue
        require(type(q.get('received_at_ns')) is int and type(q.get('provider_at_ns')) is int,'quote_time')
        if q['received_at_ns']>at or q['provider_at_ns']>at: continue
        observations.append(q)
        try: bid,ask=_quote(q)
        except Refused: continue
        if q['received_at_ns']-q['provider_at_ns']>p.max_delivery_lag_ns: continue
        usable.append((q,bid,ask))
    # Latest market state includes unusable quote withdrawals/zero sizes. Never
    # fall back to an older still-age-fresh executable state after withdrawal.
    key=lambda q:(q['provider_at_ns'],q['received_at_ns'])
    # Stable last-wins preserves retained sequence/item order when provider and
    # receipt timestamps are equal (max() would incorrectly pick first).
    latest=sorted(observations,key=key)[-1] if observations else None
    return latest,sorted(usable,key=lambda v:key(v[0]))


def _fixed_arithmetic(function):
    @wraps(function)
    def wrapped(*args,**kwargs):
        # Explicit context also resets inherited rounding/traps; neither caller
        # precision nor unrelated Decimal use can change a frozen experiment.
        with localcontext(Context(prec=50,rounding=ROUND_HALF_EVEN)):
            return function(*args,**kwargs)
    return wrapped

@_fixed_arithmetic

def observe_entry(*, mapping, candidate, symbol, quotes, decision_ns, market_session, policy=Policy()):
    """Return research entry candidate or abstention. Session/graph are caller attestations.
    No quantity, client_order_id, account capability or executable order is emitted.
    """
    policy.validate(); require(type(decision_ns) is int and 0<decision_ns<2**63,'decision_time')
    mapping,candidate,quotes=copy([mapping,candidate,list(quotes)])
    require(type(symbol) is str and SYMBOL.fullmatch(symbol),'symbol')
    require(candidate in mapping.get('candidates',[]) and symbol in candidate.get('symbols',[]),'candidate_membership')
    provenance=mapping.get('stream_provenance',{})
    observed=provenance.get('first_received_at_ns')
    require(type(observed) is int and observed<=decision_ns,'event_time')
    synthetic=bool(candidate.get('synthetic') or mapping.get('synthetic_used'))
    evidence={'event_id':mapping['event']['id'],'article_version':mapping['event']['version'],
        'mapping_id':mapping['mapping_id'],'bridge_id':mapping['bridge_id'],
        'entity_id':candidate['entity_id'],'relationship_path':candidate['path'],
        'stream':provenance,'candidate_kind':candidate['kind'],'synthetic':synthetic,
        'evidence_tier':'provider_news_retained_unverified','primary_verified':False,
        'market_session_attestation':market_session}
    reasons=[]
    if market_session!='regular': reasons.append('regular_session_not_attested')
    if decision_ns-observed>policy.max_event_age_ns: reasons.append('event_too_old')
    provider_updated=provenance.get('provider_updated_at_ns')
    require(type(provider_updated) is int and 0<provider_updated<=observed,'news_provider_time')
    if decision_ns-provider_updated>policy.max_event_age_ns: reasons.append('news_provider_version_stale')
    if observed-provider_updated>policy.max_news_delivery_lag_ns: reasons.append('news_delivery_lag_exceeded')
    if mapping.get('path_budget_exhausted'): reasons.append('mapping_work_budget_incomplete')
    if any(g.get('continuity_invalid') or g['received_at_ns']>=max(observed,decision_ns-policy.max_window_ns)
           for g in provenance.get('observed_gaps',[])):
        reasons.append('observed_stream_gap_in_decision_window')
    latest,values=_quote_state(quotes,symbol,decision_ns,policy)
    market=latest;metrics={};plan=None
    usable_latest=None
    if latest is None: reasons.append('no_usable_two_sided_quote')
    else:
        synthetic=synthetic or latest['synthetic']
        try: usable_latest=_quote(latest)
        except Refused: reasons.append('latest_quote_unusable')
    if usable_latest is not None:
        q=latest;bid,ask=usable_latest
        if q['received_at_ns']-q['provider_at_ns']>policy.max_delivery_lag_ns:
            reasons.append('latest_quote_delivery_lag_exceeded')
        if decision_ns-q['provider_at_ns']>policy.max_quote_age_ns or decision_ns-q['received_at_ns']>policy.max_receipt_age_ns:
            reasons.append('quote_stale')
        spread=(ask-bid)/((ask+bid)/2)*10000
        if spread>policy.max_spread_bps: reasons.append('spread_too_wide')
        start=max(observed,decision_ns-policy.max_window_ns)
        window=[v for v in values if v[0]['provider_at_ns']>=start and v[0]['received_at_ns']>=observed]
        # Count distinct provider timestamps, not retransmissions of one quote.
        distinct={v[0]['provider_at_ns']:v for v in window}; window=[distinct[k] for k in sorted(distinct)]
        if len(window)<policy.min_observations: reasons.append('insufficient_distinct_observations')
        if not window or window[-1][0]['provider_at_ns']-window[0][0]['provider_at_ns']<policy.min_window_ns:
            reasons.append('insufficient_observation_duration')
        if window:
            mids=[(v[1]+v[2])/2 for v in window]; momentum=(mids[-1]/mids[0]-1)*10000
            pullback=(1-mids[-1]/max(mids))*10000
            metrics={'momentum_bps':str(momentum),'pullback_bps':str(pullback),'spread_bps':str(spread),
                'observations':len(window),'window_start_ns':window[0][0]['provider_at_ns'],
                'window_end_ns':window[-1][0]['provider_at_ns']}
            if momentum<policy.min_momentum_bps: reasons.append('continuation_not_observed')
            if momentum>policy.max_chase_bps: reasons.append('move_too_extended_for_experiment')
            if pullback>policy.max_pullback_bps: reasons.append('pullback_exceeds_experiment')
            synthetic=synthetic or any(v[0]['synthetic'] for v in window)
        if not reasons:
            limit=ask*(1+D(policy.entry_slippage_bps)/10000)
            plan={'entry_limit_reference':str(limit),'entry_price_basis':'current_iex_ask_plus_bounded_slippage',
                'stop_bps':policy.stop_bps,'trail_bps':policy.trail_bps,'max_holding_ns':policy.max_holding_ns,
                'fill_assumption':'none; risk owner must reconcile actual PAPER fills',
                'notional':None,'quantity':None}
    evidence['synthetic']=synthetic
    return _envelope(action='abstain' if reasons else 'entry_candidate',symbol=symbol,at=decision_ns,
        policy=policy,reasons=reasons or ['frozen_continuation_conditions_observed'],market=market,
        evidence=evidence,metrics=metrics,plan=plan,synthetic=synthetic)


@_fixed_arithmetic
def observe_exit(*, position, quotes, decision_ns, source_gaps=(), policy=Policy()):
    """Long position projection only; durable fill/peak/risk ownership is external.
    Missing prices or history produce explicit urgent risk-review, not a guessed fill.
    """
    policy.validate(); position,quotes,source_gaps=copy([position,list(quotes),list(source_gaps)])
    require(type(decision_ns) is int and 0<decision_ns<2**63,'decision_time')
    require(position.get('side')=='long' and type(position.get('symbol')) is str and SYMBOL.fullmatch(position['symbol']),'position_side')
    require(position.get('confirmed_paper_fill') is True and type(position.get('fill_ref')) is str and position['fill_ref'],'confirmed_fill_required')
    entry=decimal(position.get('average_entry_price'),'entry_price')
    opened=position.get('opened_at_ns');require(type(opened) is int and 0<opened<=decision_ns,'position_time')
    # Durable risk owner supplies reconciled high-watermark and its knowledge time.
    peak=decimal(position.get('peak_bid'),'peak_bid')
    peak_at=position.get('peak_observed_at_ns');require(type(peak_at) is int and opened<=peak_at<=decision_ns,'peak_time')
    symbol=position['symbol'];latest,values=_quote_state(quotes,symbol,decision_ns,policy)
    values=[v for v in values if v[0]['provider_at_ns']>=opened]
    if latest is not None and latest['provider_at_ns']<opened: latest=None
    synthetic=position.get('synthetic');require(type(synthetic) is bool,'position_provenance')
    evidence={'fill_ref':position['fill_ref'],'synthetic':synthetic,'position_is_caller_attested':True}
    market=None; reasons=[];metrics={};action='hold_observation';plan=None
    valid=None
    if latest is None: reasons=['no_usable_exit_quote']
    else:
        market=latest;synthetic=synthetic or latest['synthetic']
        try: valid=_quote(latest)
        except Refused: reasons=['latest_exit_quote_unusable']
        if (decision_ns-latest['provider_at_ns']>policy.max_quote_age_ns or
            decision_ns-latest['received_at_ns']>policy.max_receipt_age_ns or
            latest['received_at_ns']-latest['provider_at_ns']>policy.max_delivery_lag_ns):
            reasons.append('exit_quote_stale_or_delayed')
    if any(g['received_at_ns']<=decision_ns and (g.get('continuity_invalid') or
           latest is None or g['received_at_ns']>=latest['received_at_ns']) for g in source_gaps):
        reasons.append('exit_source_continuity_unavailable')
    if reasons: action='risk_review_required'
    elif valid is not None:
        bid,ask=valid
        synthetic=synthetic or any(v[0]['synthetic'] for v in values)
        peak=max([peak]+[v[1] for v in values]);stop=entry*(1-D(policy.stop_bps)/10000)
        trailing=peak*(1-D(policy.trail_bps)/10000)
        if bid<=stop: reasons.append('hard_stop_observed')
        if bid<=trailing: reasons.append('trailing_drawdown_observed')
        if decision_ns-opened>=policy.max_holding_ns: reasons.append('holding_deadline_reached')
        if reasons: action='exit_candidate'
        metrics={'peak_bid':str(peak),'hard_stop_reference':str(stop),'trailing_reference':str(trailing)}
        plan={'exit_bid_reference':str(bid),'quantity':None,'fill_assumption':'none',
              'requires_reconciled_reduce_only_quantity':True} if reasons else None
    evidence['synthetic']=synthetic
    return _envelope(action=action,symbol=symbol,at=decision_ns,policy=policy,reasons=reasons or ['no_exit_condition_observed'],
        market=market,evidence=evidence,metrics=metrics,plan=plan,synthetic=synthetic)
