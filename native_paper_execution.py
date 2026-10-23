"""Disabled native PAPER dispatch. No environment, startup, scheduler or key loading.

Only explicitly driven, account-fenced ticks may use this path. Tests inject a
wire and prohibit sockets. A durable Decimal claim is not permission to send;
all dispatch gates below remain mandatory. Uncertain writes are never replayed.
"""
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import json
import re
import time
from urllib import request, error
from urllib.parse import parse_qs, urlencode, urlsplit
from datetime import datetime, timedelta, timezone

from alpaca_schema import (PAPER_ORIGIN, TERMINAL, OwnedIntent, fresh, label,
    number, parse_order, parse_latest_quotes, reconcile_order, owned_exit_quantity, timestamp)
from decimal_lifecycle import Lifecycle, LifecycleError
from native_observation_collector import (NativeObservationCollector, NativeReadOnlyHTTP,
    OwnedLedgerView, DATA_ORIGIN, _route, MAX_BYTES)
from paper_adapter import NoRedirect
from paper_quotes import _unique_object, _invalid_constant

NS = 10**9

class DispatchRefused(ValueError):
    pass

def require(ok, code):
    if not ok:
        raise DispatchRefused(code)


def decimal_text(value):
    return format(value, 'f')


def order_timestamps(raw):
    """Canonical provider times, including fields absent from the parsed Order."""
    return tuple(None if raw.get(key) is None else timestamp(raw[key])
                 for key in ('created_at','submitted_at','updated_at','filled_at',
                             'canceled_at','expired_at','failed_at'))


class NativePaperHTTP:
    """Exact PAPER writes and existing GET routes only. No retries/redirects/proxy.

    Existing credentials must be explicitly injected by a separately reviewed
    caller. Default disabled; constructing this object sends nothing.
    """
    paper_origin = PAPER_ORIGIN
    data_origin = DATA_ORIGIN
    feed = 'iex'

    def __init__(self, key, secret, *, enabled=False, origin=PAPER_ORIGIN):
        require(type(enabled) is bool and origin == PAPER_ORIGIN, 'paper_only_disabled_boundary')
        require(all(type(v) is str and 0 < len(v) <= 512 and all(33 <= ord(c) <= 126 for c in v)
                    for v in (key, secret)), 'invalid_credentials')
        self.enabled = enabled
        self.__headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret,
                          'Accept': 'application/json', 'Content-Type': 'application/json',
                          'Accept-Encoding': 'identity'}
        self._reader = NativeReadOnlyHTTP(key, secret, enabled=enabled)
        self._opener = request.build_opener(request.ProxyHandler({}), NoRedirect())

    @staticmethod
    def validate_payload(body):
        require(type(body) is dict and set(body) == {'client_order_id','symbol','side','qty',
                'type','time_in_force','limit_price','extended_hours'}, 'invalid_payload')
        require(type(body['client_order_id']) is str and re.fullmatch(r'[A-Za-z0-9_.-]{1,128}',body['client_order_id']), 'invalid_client_id')
        require(type(body['symbol']) is str and re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,11}',body['symbol']), 'invalid_symbol')
        require(body['side'] in ('buy','sell') and body['type']=='limit' and body['time_in_force']=='day'
                and body['extended_hours'] is False, 'regular_day_limit_only')
        qty, price = number(body['qty'], quantity=True), number(body['limit_price'])
        require(0 < qty <= 1 and (body['side']!='buy' or qty == 1), 'one_share_entry_only')
        require(1 <= price <= 1000 and price % Decimal('.01') == 0, 'price_bound')

    def call(self, method, path, body=None):
        require(self.enabled is True, 'execution_transport_disabled')
        require(type(method) is str and type(path) is str and len(path)<=1024,'invalid_route')
        bar_route=method=='GET' and isinstance(path,str) and path.startswith('/v2/stocks/bars?')
        if bar_route:
            validate_bar_route(path)
        if method == 'GET' and not bar_route and not re.fullmatch(r'/v2/assets/[A-Z][A-Z0-9.\-]{0,11}', path or ''):
            require(body is None, 'forbidden_body')
            _route(path)
            return self._reader.call(method, path)
        if method == 'GET':
            require(body is None, 'forbidden_body')
        elif method == 'POST':
            require(path == '/v2/orders', 'forbidden_route')
            self.validate_payload(body)
        elif method == 'DELETE':
            require(body is None and type(path) is str and re.fullmatch(r'/v2/orders/[A-Za-z0-9_-]{1,128}',path), 'forbidden_cancel_route')
        else:
            raise DispatchRefused('forbidden_method')
        url = (DATA_ORIGIN if bar_route else PAPER_ORIGIN) + path
        payload = None if body is None else json.dumps(body, sort_keys=True, separators=(',',':'), allow_nan=False).encode()
        try:
            with self._opener.open(request.Request(url, data=payload, headers=self.__headers, method=method), timeout=2) as response:
                require(response.geturl() == url and response.status in range(200,300), 'response_origin_or_status')
                require(response.headers.get('Content-Encoding','identity').lower()=='identity', 'unsupported_encoding')
                raw=response.read(MAX_BYTES+1)
                require(len(raw)<=MAX_BYTES, 'response_too_large')
                return response.status, (json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant) if raw else None)
        except error.HTTPError as exc:
            return exc.code, None  # No raw request, response or secret leaks.
        except Exception:
            raise DispatchRefused('paper_transport_uncertain') from None


class CoordinatedWire:
    """Every attempted new-path GET/POST/DELETE shares the account guard budget."""
    paper_origin=PAPER_ORIGIN
    data_origin=DATA_ORIGIN
    feed='iex'
    def __init__(self, transport, guard, now_ns):
        require(getattr(transport,'paper_origin',None)==PAPER_ORIGIN and getattr(transport,'data_origin',None)==DATA_ORIGIN
                and getattr(transport,'feed',None)=='iex', 'transport_binding')
        self.transport,self.guard,self.now_ns=transport,guard,now_ns
    def call(self, method, path, body=None):
        self.guard.check()
        if method in ('POST','DELETE'):
            self.guard.assert_write_allowed()
        self.guard.reserve_request(now_ns=self.now_ns())
        return self.transport.call(method,path,body)


@dataclass(frozen=True)
class ExecutionObservation:
    """Scoped lifecycle view AFTER complete-book legacy/native reconciliation."""
    observed_ns: int
    session_open_ns: int
    legacy_revision: object = field(repr=False)
    native_digest: str
    _json: bytes = field(repr=False)
    def arguments(self):
        result=json.loads(self._json)
        result['quote_symbols']=tuple(result['quote_symbols'])
        return result


class NativeExecutionObserver:
    """Reuse the frozen collector, retaining full-scan terminal reconciliation.

    All known terminal native/legacy orders must still appear in the full scan
    and equal their immutable recorded state. Only working/uncertain native IDs
    need independent lookup. History >=500 refuses, never truncates/archives away.
    """
    def __init__(self, lifecycle, guard, wire, *, now_ns=time.time_ns, monotonic_ns=time.monotonic_ns):
        self.lifecycle,self.guard,self.wire=lifecycle,guard,wire
        self.now_ns,self.monotonic_ns=now_ns,monotonic_ns

    def collect(self):
        started=self.now_ns(); mono_started=self.monotonic_ns()
        self.guard.check()
        budget_factory=None
        if self.guard._runtime_bundle is not None:
            from native_existing_budget import ExistingStateRequestBudget
            # Validate complete existing storage before ANY request, including
            # legacy reserved-ID absence checks preceding the normal collector.
            budget=ExistingStateRequestBudget(self.guard.state_database,self.guard)
            def budget_factory(database):
                require(str(database)==budget.database,'collector_budget_binding')
                budget._check()
                return budget
        before=self.lifecycle.status()
        rows=before['intents']; pending=[]; previous=[]
        with self.lifecycle.tx() as db:
            dispatched={r['cid']:r['state'] for r in db.execute('SELECT cid,state FROM native_dispatch WHERE kind=\'submit\'')}
        for row in rows:
            old=None if row['response'] is None else parse_order(json.loads(row['response']))
            if old is not None and old.status not in TERMINAL:
                pending.append(row['cid']); previous.append(old)
            elif old is None and row['cid'] in dispatched:
                pending.append(row['cid'])  # A missing uncertain order refuses, never authorizes replay.
        missing=[]
        for cid in self.guard.reserved_client_ids:
            status,raw=self.wire.call('GET','/v2/orders:by_client_order_id?client_order_id='+cid)
            require(status==404, 'legacy_reserved_absence_unproven')
            missing.append(cid)
        fresh(started,self.now_ns())
        revision=int.from_bytes(hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).digest(),'big')
        def view():
            self.guard.check()
            require(self.lifecycle.status()['intents']==rows, 'native_ledger_changed')
            return OwnedLedgerView(self.lifecycle.binding, revision, tuple(pending), tuple(previous))
        collector=NativeObservationCollector(database=self.guard.state_database, transport=self.wire,
            expected_fingerprint=self.lifecycle.binding, ledger_view=view, symbols=self.lifecycle.allowlist,
            enabled=True,now_ns=self.now_ns,monotonic_ns=self.monotonic_ns,budget_factory=budget_factory)
        result=collector.collect(); raw=result.native_arguments()
        by_id={o.client_order_id:o for o in result.snapshot.orders}
        raw_by_id={o['client_order_id']:o for o in raw['orders']}
        for row in rows:
            if row['response'] is not None:
                require(row['cid'] in by_id, 'terminal_archive_missing')
                stored=json.loads(row['response']); previous=parse_order(stored)
                reconcile_order(previous,by_id[row['cid']])
                if previous.status in TERMINAL:
                    require(order_timestamps(stored)==order_timestamps(raw_by_id[row['cid']]),
                            'terminal_timestamps_changed')
            elif row['cid'] in by_id:
                require(row['cid'] in dispatched, 'unattempted_claim_provider_collision')
        accepted=self.now_ns(); fresh(started,accepted)
        require(0<=self.monotonic_ns()-mono_started<=10*NS,'observation_deadline_exceeded')
        legacy=self.guard.reconcile_legacy(raw['orders'],tuple(missing),now_ns=accepted,observed_ns=result.observed_ns)
        excluded=set(legacy.submitted_client_order_ids)
        require(not (excluded & {r['cid'] for r in rows}), 'legacy_native_id_collision')
        # The complete original book was reconciled above. The lifecycle's owned
        # namespace omits only proven terminal-flat historical legacy orders.
        raw['orders']=[o for o in raw['orders'] if o['client_order_id'] not in excluded]
        self.guard.check(); fresh(result.observed_ns,self.now_ns()); fresh(started,self.now_ns())
        require(0<=self.monotonic_ns()-mono_started<=10*NS,'observation_deadline_exceeded')
        return ExecutionObservation(result.observed_ns,result.session_open_ns,self.guard.legacy_revision,
            result.digest,json.dumps(raw,sort_keys=True,separators=(',',':')).encode())


class NativePaperDispatcher:
    """Durable one-attempt writes from Decimal claims; callable default disabled."""
    def __init__(self, lifecycle, guard, transport, *, enabled=False, now_ns=time.time_ns,
                 monotonic_ns=time.monotonic_ns, pre_write_context=None):
        require(type(enabled) is bool and type(lifecycle) is Lifecycle, 'invalid_dispatcher')
        require(pre_write_context is None or callable(pre_write_context), 'invalid_write_context')
        self.pre_write_context=pre_write_context
        require(lifecycle.session_policy is not None and lifecycle.session_policy.max_entries<=4
                and lifecycle.session_policy.max_loss<=Decimal('100'), 'bounded_risk_policy_required')
        self.lifecycle,self.guard,self.enabled,self.now_ns=lifecycle,guard,enabled,now_ns
        self.wire=CoordinatedWire(transport,guard,now_ns)
        self.observer=NativeExecutionObserver(lifecycle,guard,self.wire,now_ns=now_ns,monotonic_ns=monotonic_ns)
        self.observation=None
        with lifecycle.tx() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS native_dispatch(kind TEXT NOT NULL,cid TEXT NOT NULL,
                attempted_ns INTEGER NOT NULL,state TEXT NOT NULL,PRIMARY KEY(kind,cid))''')

    @contextmanager
    def tick_scope(self):
        require(self.enabled is True, 'dispatcher_disabled')
        with self.guard.hold(now_ns=self.now_ns()):
            self.observation=None
            try: yield self
            finally: self.observation=None

    def observe(self):
        require(self.enabled is True, 'dispatcher_disabled')
        self.guard.check()
        observation=self.observer.collect()
        self.lifecycle.observe(**observation.arguments(),now_ns=self.now_ns())
        self.observation=observation
        return observation

    def reconcile_only(self):
        """GET evidence after uncertainty; never clear attention, stop or retry."""
        require(self.enabled is True, 'dispatcher_disabled')
        with self.guard.hold(now_ns=self.now_ns(), read_only=True):
            observation=self.observer.collect()
            self.lifecycle.observe(**observation.arguments(),now_ns=self.now_ns())
            return self.lifecycle.public_status()

    def _attention(self):
        with self.lifecycle.tx() as db:
            db.execute('UPDATE control SET attention=1 WHERE id=1')

    def _gate(self):
        require(self.enabled is True, 'dispatcher_disabled')
        self.guard.check()
        require(type(self.observation) is ExecutionObservation, 'fresh_tick_observation_required')
        require(self.observation.legacy_revision==self.guard.legacy_revision, 'legacy_revision_changed')
        now=self.now_ns(); raw=self.observation.arguments()
        snapshot=self.lifecycle._parse(raw,now)
        parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=now)
        require(snapshot.clock.is_open and self.observation.session_open_ns<=now<snapshot.clock.next_close_ns,'regular_session_required')
        return raw,snapshot,now

    def _reserve(self, kind, cid, now):
        with self.lifecycle.tx() as db:
            control=db.execute('SELECT stopped,attention FROM control').fetchone()
            require(not control['stopped'] and not control['attention'],'stopped_or_attention')
            require(db.execute('SELECT 1 FROM native_dispatch WHERE kind=? AND cid=?',(kind,cid)).fetchone() is None,'dispatch_already_attempted')
            db.execute('INSERT INTO native_dispatch VALUES(?,?,?,?)',(kind,cid,now,'uncertain'))

    def submit(self,cid):
        raw,snapshot,now=self._gate()
        with self.lifecycle.tx() as db:
            rows=[dict(r) for r in db.execute('SELECT * FROM intents ORDER BY rowid')]
            row=next((r for r in rows if r['cid']==cid),None)
            require(row is not None and row['response'] is None,'owned_unsent_claim_required')
            require(db.execute('SELECT 1 FROM native_dispatch WHERE kind=\'submit\' AND cid=?',(cid,)).fetchone() is None,'dispatch_already_attempted')
        require(now-row['claimed']<=10*NS and now>=row['claimed'],'expired_claim')
        known={r['cid']:r for r in rows if r['cid']!=cid}
        by_id={o.client_order_id:o for o in snapshot.orders}
        require(cid not in by_id and set(known)<=set(by_id),'unresolved_or_collision')
        require(set(by_id)<=set(known),'external_order_history')
        require(all(o.status in TERMINAL for o in snapshot.orders),'working_order_reserved')
        for key,r in known.items():
            o=by_id[key]
            require((o.symbol,o.side,o.quantity,o.limit_price)==(r['symbol'],r['side'],Decimal(r['quantity']),Decimal(r['price'])),'native_intent_mismatch')
        qty,price=Decimal(row['quantity']),Decimal(row['price'])
        quotes=parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=now)
        quote=next((q for q in quotes if q.symbol==row['symbol']),None)
        require(quote is not None and (quote.ask-quote.bid)/((quote.ask+quote.bid)/2)<=Decimal('.002'),'quote_spread')
        require(abs(price-(quote.ask if row['side']=='buy' else quote.bid))<=price*Decimal('.001'),'quote_moved')
        owned={}
        for o in snapshot.orders:
            owned[o.symbol]=owned.get(o.symbol,Decimal(0))+o.filled*(1 if o.side=='buy' else -1)
        owned={s:q for s,q in owned.items() if q}
        require(owned=={p.symbol:p.quantity for p in snapshot.positions},'unmanaged_position')
        if row['side']=='buy':
            require(qty==1 and row['symbol'] not in owned and len(owned)<2,'one_share_position_cap')
            require(now-self.observation.session_open_ns>=300*NS and snapshot.clock.next_close_ns-now>=4500*NS,'entry_session_window')
            exposure=sum((max(p.market_value,p.quantity*1000) for p in snapshot.positions),Decimal(0))
            require(price<=snapshot.account.cash and exposure+price<=2000,'entry_cash_or_exposure')
            self.guard.legacy_entry_gate(now_ns=now)
            legacy_count=self.guard.legacy_buy_claims(session_open_ns=self.observation.session_open_ns,session_close_ns=snapshot.clock.next_close_ns)
            state=self.lifecycle.status(); risk=next((r for r in state['risk_sessions'] if r['session_close_ns']==snapshot.clock.next_close_ns),None)
            require(risk is not None and not risk['loss_latched'] and risk['entry_claims']+legacy_count<=4,'shared_entry_risk_gate')
            require(sum(r['side']=='buy' and r['symbol']==row['symbol'] and r['claimed']>=self.observation.session_open_ns for r in rows)==1,'symbol_session_cap')
        else:
            intents=tuple(OwnedIntent(self.lifecycle.binding,r['cid'],r['symbol'],r['side'],Decimal(r['quantity'])) for r in known.values())
            require(qty==owned_exit_quantity(snapshot,intents=intents,symbol=row['symbol'],now_ns=now),'exact_reduction_required')
        payload=dict(client_order_id=cid,symbol=row['symbol'],side=row['side'],qty=decimal_text(qty),type='limit',
            time_in_force='day',limit_price=decimal_text(price),extended_hours=False)
        NativePaperHTTP.validate_payload(payload)
        status,asset=self.wire.call('GET','/v2/assets/'+row['symbol'])
        require(status==200 and type(asset) is dict and asset.get('symbol')==row['symbol'] and asset.get('class')=='us_equity'
                and asset.get('status')=='active' and asset.get('tradable') is True,'invalid_asset')
        require(qty==qty.to_integral_value() or asset.get('fractionable') is True,'fractional_exit_unavailable')
        self._gate()  # All clocks, quotes and controls are revalidated after asset I/O.
        # Coordinator control locks span the final uncertainty marker and wire
        # call. A committed runner/pipeline stop cannot be missed before sending.
        with self.pre_write_context() if self.pre_write_context else nullcontext():
            self._reserve('submit',cid,self.now_ns())
            try:
                # Uncertainty committed first; hold native control lock across POST so
                # a committed stop cannot be missed between the final check and send.
                with self.lifecycle.tx() as db:
                    c=db.execute('SELECT stopped,attention FROM control').fetchone()
                    require(not c['stopped'] and not c['attention'],'stopped_or_attention')
                    self._gate()
                    status,order=self.wire.call('POST','/v2/orders',payload)
                    require(status in (200,201),'submission_uncertain')
                    require(type(order) is dict and ('account_id' not in order or order['account_id']==raw['account']['id']),
                            'response_account_mismatch')
                self.lifecycle.record_response(cid,order,now_ns=self.now_ns())
                with self.lifecycle.tx() as db:
                    db.execute("UPDATE native_dispatch SET state='observed' WHERE kind='submit' AND cid=?",(cid,))
                return {'status':'submission_observed','order_permission':'paper_only','executable':False}
            except Exception:
                self._attention()
                raise DispatchRefused('submission_uncertain_no_replay') from None
    def cancel(self,cid):
        raw,snapshot,now=self._gate()
        order=next((o for o in snapshot.orders if o.client_order_id==cid),None)
        require(order is not None and order.status not in TERMINAL and order.status!='pending_cancel','cancel_not_needed_or_pending')
        with self.lifecycle.tx() as db:
            row=db.execute('SELECT * FROM intents WHERE cid=?',(cid,)).fetchone()
            require(row is not None and db.execute('SELECT 1 FROM cancels WHERE cid=?',(cid,)).fetchone() is not None,'owned_cancel_claim_required')
            require((order.symbol,order.side,order.quantity,order.limit_price)==(row['symbol'],row['side'],Decimal(row['quantity']),Decimal(row['price'])),'cancel_identity')
        with self.pre_write_context() if self.pre_write_context else nullcontext():
            self._reserve('cancel',cid,now)
            try:
                with self.lifecycle.tx() as db:
                    c=db.execute('SELECT stopped,attention FROM control').fetchone()
                    require(not c['stopped'] and not c['attention'],'stopped_or_attention')
                    self._gate()
                    status,_=self.wire.call('DELETE','/v2/orders/'+order.id)
                    require(status in (200,202,204),'cancel_uncertain')
                # DELETE success is never terminal evidence; later full GET scan and
                # owned-ID lookup must reconcile remaining quantity before any exit.
                return {'status':'cancel_requested','terminal':False,'executable':False}
            except Exception:
                self._attention()
                raise DispatchRefused('cancel_uncertain_no_replay') from None

def _datetime_ceil(ns):
    return datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(microseconds=(ns+999)//1000)


def bar_path(symbol, start, end):
    from native_observation_collector import UNIVERSE
    require(symbol in UNIVERSE and symbol!='SPY' and start.utcoffset() is not None
            and end.utcoffset() is not None and timedelta(0)<end-start<=timedelta(minutes=60)
            and not start.second and not start.microsecond and not end.second and not end.microsecond,'invalid_bar_window')
    return '/v2/stocks/bars?'+urlencode(dict(symbols=symbol+',SPY',timeframe='1Min',
        start=start.isoformat(),end=(end-timedelta(microseconds=1)).isoformat(),
        limit='120',adjustment='raw',feed='iex',sort='asc',asof='-'))


def validate_bar_route(path):
    try:
        parts=urlsplit(path)
        require(not parts.scheme and not parts.netloc and not parts.fragment and parts.path=='/v2/stocks/bars','invalid_bar_route')
        p=parse_qs(parts.query,strict_parsing=True)
        require(set(p)=={'symbols','timeframe','start','end','limit','adjustment','feed','sort','asof'}
                and all(len(v)==1 for v in p.values()),'invalid_bar_route')
        require(all(p[k]==[v] for k,v in dict(timeframe='1Min',limit='120',adjustment='raw',feed='iex',sort='asc',asof='-').items()),'invalid_bar_route')
        symbols=p['symbols'][0].split(',')
        require(len(symbols)==2 and symbols[1]=='SPY','invalid_bar_route')
        start=datetime.fromisoformat(p['start'][0]); end=datetime.fromisoformat(p['end'][0])+timedelta(microseconds=1)
        require(path==bar_path(symbols[0],start,end),'noncanonical_bar_route')
    except DispatchRefused:
        raise
    except Exception:
        raise DispatchRefused('invalid_bar_route') from None
