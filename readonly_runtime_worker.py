"""Fixed, supervised read-only runtime stages. No write-mode or order permission.

Only the reviewed initializer may enroll a fresh cohort through the pre-existing
application-database anchor. All recurring stages verify the sealed inventory
BEFORE constructing auto-creating classes. Stages are serialized by a seal-file
flock and a durable dirty marker; a killed stage is never silently replayed.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

from active_demo_pipeline import Config, DemoPipeline, encode
from alpaca_schema import PAPER_ORIGIN, timestamp, parse_latest_quotes
from decimal_lifecycle import Lifecycle, SessionRiskPolicy
from demo_tick_runner import RunnerLedger
from native_account_guard import SharedAccountGuard, AccountGuardRefused
from native_observation_collector import DATA_ORIGIN, SharedRequestBudget, CollectionRefused
from native_observation_worker import load_owned_ledger, ProbeRefused
from native_paper_execution import NativePaperDispatcher, NativePaperHTTP, DispatchRefused, bar_path, require, _datetime_ceil
from native_paper_runner import NativePaperExecutionPort, NativeMarketSource
from primary_earnings_source import PrimaryEarningsSource, NvidiaReadOnlyHTTP
from runtime_state_bundle import RuntimeStateBundle, RuntimeStateRefused, FIXED_PATHS
from shadow_market_inputs import parse_bars

PHASES = frozenset(('initialize','source','broker'))
PUBLIC_REFUSALS=frozenset((
    'existing_event_db_configuration_required','existing_event_db_persistent_path_required',
    'bundle_existing_history_required','bundle_file_missing','bundle_file_replaced',
    'bundle_anchor_provision_incomplete','bundle_anchor_incomplete_or_changed',
    'bundle_anchor_history_changed','bundle_already_or_partially_provisioned',
    'bundle_inventory_or_schema_changed','bundle_provision_incomplete',
    'credentials_missing','runtime_attention_latched','native_attention_latched',
    'source_interrupted','source_stage_refused','runtime_begin_refused',
    'legacy_worker_busy','legacy_writer_busy','legacy_not_terminal_flat',
    'legacy_reserved_absence_unproven','shared_request_budget_exhausted',
    'terminal_archive_missing','legacy_submitted_order_missing',
    'legacy_order_mismatch','legacy_terminal_changed','asset_probe_refused',
    'market_probe_incomplete','source_changed_during_inputs',
    'source_changed_during_evaluation',
))


class RuntimeReadOnlyHTTP:
    """Read capability only; rejects every write before any underlying transport."""
    paper_origin=PAPER_ORIGIN
    data_origin=DATA_ORIGIN
    feed='iex'
    def __init__(self,key,secret,*,enabled=False):
        require(type(enabled) is bool,'invalid_enable')
        self.enabled=enabled
        self.__reader=NativePaperHTTP(key,secret,enabled=enabled)
    def call(self,method,path,body=None):
        require(self.enabled is True and method=='GET' and body is None,'runtime_get_only')
        return self.__reader.call('GET',path)


class NoNetwork:
    paper_origin=PAPER_ORIGIN
    data_origin=DATA_ORIGIN
    feed='iex'
    def call(self,*args,**kwargs):
        raise ValueError('network_unavailable_in_initialization')


def components(paths,binding,*,broker=None,source_transport=None,now_ns=time.time_ns,test_only=False,bundle=None):
    life=Lifecycle(paths['lifecycle'],binding,allowlist=('NVDA',),session_risk_policy=SessionRiskPolicy(4,Decimal('100')))
    guard=SharedAccountGuard(paths['legacy'],paths['lifecycle'],binding,enabled=True,
        state_database=paths['guard'],test_only=test_only,runtime_bundle=bundle)
    dispatch=NativePaperDispatcher(life,guard,broker or NoNetwork(),enabled=False,now_ns=now_ns)
    pipeline=DemoPipeline(paths['decision'],Config(enabled=True,allowlist=('NVDA',)))
    runner=RunnerLedger(paths['runner'])
    source=PrimaryEarningsSource(paths['source'],transport=source_transport or NvidiaReadOnlyHTTP(),now_ns=now_ns)
    port=NativePaperExecutionPort(evaluator=pipeline,lifecycle=life,dispatcher=dispatch)
    return life,guard,dispatch,pipeline,runner,source,port


def initialize_components(paths,binding,*,now_ns=time.time_ns,test_only=False):
    life,guard,dispatch,pipeline,runner,source,port=components(paths,binding,now_ns=now_ns,test_only=test_only)
    with life.tx() as db:
        db.execute('CREATE TABLE IF NOT EXISTS native_strategy_sessions (close_ns INTEGER PRIMARY KEY,open_ns INTEGER NOT NULL)')
    with guard.hold(now_ns=now_ns(),read_only=True):
        SharedRequestBudget(paths['guard'])  # Initialize schema only, no request.
    require(not life.status()['intents'],'fresh_cohort_required')


@contextmanager
def serialized(bundle):
    bundle.verify()
    path=Path(bundle.paths['seal'])
    fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        bundle.verify()
        yield
        bundle.verify()
    finally:
        os.close(fd)


def public(phase,reason,**values):
    allowed={'source_events','source_requests','market_inputs_valid','legacy_reconciled',
             'preview_enter','preview_abstain','account_requests','source_healthy'}
    require(set(values)<=allowed and all(type(v) in (bool,int) for v in values.values()),'unsafe_summary')
    return dict(mode='native_readonly_runtime',phase=phase,reason=reason,
                order_permission='none',execution_ready=False,**values)


def _pristine_read_mode(life,runner):
    state=runner.status()
    require(not state['dirty'] and not state['stopped'] and state['attention'] is None,'runtime_attention_latched')
    control=life.public_status()
    require(not control['stopped'] and not control['attention'],'native_attention_latched')


def _bars_probe(port,now):
    """Validate actual IEX entitlement/shape without inventing an earnings event."""
    end=now.replace(second=0,microsecond=0);start=end-timedelta(minutes=6)
    status,raw=port.dispatcher.wire.call('GET',bar_path('NVDA',start,end))
    require(status==200 and type(raw) is dict and type(raw.get('bars')) is dict
            and set(raw['bars'])=={'NVDA','SPY'} and raw.get('next_page_token') is None
            and raw.get('feed','iex')=='iex','market_probe_incomplete')
    received=_datetime_ceil(port.dispatcher.now_ns())
    for symbol in ('NVDA','SPY'):
        parse_bars({'symbol':symbol,'bars':raw['bars'][symbol],'next_page_token':None},symbol,
                   start=start,end=end,received_at=received,now=received)


def broker_observation(life,guard,dispatch,pipeline,source,port,now_ns,bundle):
    with guard.hold(now_ns=now_ns(),read_only=True):
        try:
            observation=dispatch.observer.collect()
        except CollectionRefused as exc:
            if exc.code=='closed_session':
                return public('broker','closed_session',market_inputs_valid=False,legacy_reconciled=False)
            raise
        bundle.verify()
        raw=observation.arguments()
        life.observe(**raw,now_ns=now_ns())
        port.raw=raw;port.snapshot=life._parse(raw,now_ns());port.session_open_ns=observation.session_open_ns
        with life.tx() as db:
            prior=db.execute('SELECT open_ns FROM native_strategy_sessions WHERE close_ns=?',(port.snapshot.clock.next_close_ns,)).fetchone()
            require(prior is None or prior['open_ns']==port.session_open_ns,'session_open_changed')
            db.execute('INSERT OR IGNORE INTO native_strategy_sessions VALUES(?,?)',(port.snapshot.clock.next_close_ns,port.session_open_ns))
        now=_datetime_ceil(now_ns());events=source.events(now,100)
        ready=[]
        for event in events:
            anchor=event.published_at.replace(second=0,microsecond=0)
            endpoint=anchor+timedelta(minutes=5+(anchor!=event.published_at))
            did=hashlib.sha256(encode([pipeline.config.strategy_version,event.event_id,event.version,event.symbol]).encode()).hexdigest()
            with pipeline.db() as db:
                already=bool(db.execute('SELECT 1 FROM demo_decisions WHERE id=?',(did,)).fetchone())
            if not already and endpoint<=now<=endpoint+timedelta(seconds=60) and source.event_eligible(event):
                ready.append(event)
        require(len(ready)<=1,'ambiguous_ready_events')
        enter=abstain=0
        if ready:
            event=ready[0];market=NativeMarketSource(port)
            inputs=market.inputs(event,now)
            require(source.event_eligible(event),'source_changed_during_inputs')
            # Observe a signal only. Never call port.submit/exit/expire/claim.
            result=port.evaluate(event,inputs,_datetime_ceil(now_ns()))
            require(source.event_eligible(event),'source_changed_during_evaluation')
            enter=int(result['action']=='enter');abstain=1-enter
        else:
            if now.replace(second=0,microsecond=0)-timedelta(minutes=6)<_datetime_ceil(observation.session_open_ns):
                return public('broker','waiting_regular_bar_window',
                              market_inputs_valid=False,legacy_reconciled=True)
            _bars_probe(port,now)
        status,asset=dispatch.wire.call('GET','/v2/assets/NVDA')
        require(status==200 and type(asset) is dict and asset.get('symbol')=='NVDA'
                and asset.get('class')=='us_equity' and asset.get('status')=='active'
                and asset.get('tradable') is True,'asset_probe_refused')
        parse_latest_quotes(raw['quotes'],symbols=('NVDA',),feed='iex',now_ns=now_ns())
        with sqlite3.connect(Path(bundle.paths['source']).as_uri()+'?mode=ro',uri=True) as db:
            healthy=bool(source._healthy(db,now_ns()))
        return public('broker','observed_readonly',market_inputs_valid=True,legacy_reconciled=True,
                      preview_enter=enter,preview_abstain=abstain,source_healthy=healthy)


def run_phase(phase,*,paths=None,test_only=False,environ=None,now_ns=time.time_ns,
              broker_factory=RuntimeReadOnlyHTTP,source_factory=NvidiaReadOnlyHTTP):
    """Trusted offline injection only; production CLI accepts no paths/options."""
    require(phase in PHASES and type(test_only) is bool,'invalid_arguments')
    require(paths is None or test_only,'fixed_paths_required')
    resolved=FIXED_PATHS if paths is None else paths
    binding=load_owned_ledger(resolved['legacy']).account_fingerprint
    bundle=RuntimeStateBundle(binding,paths=paths,test_only=test_only)
    if phase=='initialize':
        bundle.initialize_once(initialize=lambda p:initialize_components(p,binding,now_ns=now_ns,test_only=test_only))
        with serialized(bundle):
            life,guard,dispatch,pipeline,runner,source,port=components(bundle.paths,binding,now_ns=now_ns,test_only=test_only,bundle=bundle)
            _pristine_read_mode(life,runner)
            with sqlite3.connect(Path(bundle.paths['source']).as_uri()+'?mode=ro',uri=True) as db:
                require(db.execute('SELECT busy FROM primary_meta WHERE id=1').fetchone()==(0,),'source_interrupted')
        return public(phase,'initialized_readonly')
    with serialized(bundle):
        # Credentials are touched only here, by existing server process identity,
        # after private state/ownership checks. They are never logged or persisted.
        env=os.environ if environ is None else environ
        broker=None
        if phase=='broker':
            key,secret=env.get('ALPACA_API_KEY'),env.get('ALPACA_SECRET_KEY')
            require(bool(key) and bool(secret),'credentials_missing')
            broker=broker_factory(key,secret,enabled=True)
        source_transport=source_factory(enabled=phase=='source')
        life,guard,dispatch,pipeline,runner,source,port=components(bundle.paths,binding,broker=broker,
            source_transport=source_transport,now_ns=now_ns,test_only=test_only,bundle=bundle)
        bundle.verify();_pristine_read_mode(life,runner)
        require(runner.begin(_datetime_ceil(now_ns()),120),'runtime_begin_refused')
        try:
            if phase=='source':
                result=source.collect()
                require(result.reason in ('collected_primary_evidence','no_earnings_in_feed'),'source_stage_refused')
                report=public(phase,result.reason,source_events=result.event_count,source_requests=result.request_count)
            else:
                report=broker_observation(life,guard,dispatch,pipeline,source,port,now_ns,bundle)
            bundle.verify()
            runner.finish(_datetime_ceil(now_ns()))
            return report
        except BaseException:
            # Even SystemExit/KeyboardInterrupt leave a permanent attention/dirty
            # record. SIGKILL cannot run this clause; the committed dirty bit stays.
            try:runner.attention('readonly_runtime_stage_failed')
            except Exception:pass
            raise


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    phase=args[0][2:] if len(args)==1 and args[0].startswith('--') else ''
    if phase not in PHASES:
        print(json.dumps(public('invalid','invalid_arguments')),flush=True);return 2
    try:
        report=run_phase(phase)
        print(json.dumps(report,sort_keys=True),flush=True);return 0
    except Exception as exc:
        reason='runtime_stage_refused'
        if type(exc) in (CollectionRefused,ProbeRefused):
            reason=exc.code  # Constructor itself maps to frozen safe codes.
        elif type(exc) in (RuntimeStateRefused,DispatchRefused,AccountGuardRefused) and str(exc) in PUBLIC_REFUSALS:
            reason=str(exc)
        print(json.dumps(public(phase,reason)),flush=True);return 1


if __name__=='__main__':raise SystemExit(main())
