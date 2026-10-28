"""Pure conservative Alpaca US equity response decoders. No I/O or execution."""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Context, Decimal, localcontext
import hashlib
import re

PAPER_ORIGIN = 'https://paper-api.alpaca.markets'
TERMINAL = frozenset({'filled', 'canceled', 'expired', 'rejected'})
WORKING = frozenset({'new', 'accepted', 'pending_new', 'partially_filled', 'pending_cancel'})

class SchemaError(ValueError):
    pass

def require(ok, reason):
    if not ok: raise SchemaError(reason)

def obj(value):
    require(type(value) is dict, 'invalid_object')
    return value

def label(value):
    require(isinstance(value,str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,128}',value), 'invalid_identifier')
    return value

def number(value, *, nonnegative=True, quantity=False):
    require(isinstance(value,str) and len(value)<=50 and re.fullmatch(r'-?\d+(?:\.\d+)?',value), 'invalid_decimal_string')
    n=Decimal(value)
    require(len(n.as_tuple().digits)<=24, 'decimal_precision')
    # copy_abs is exact; abs(Decimal) rounds under the caller's ambient context.
    require(n.copy_abs()<=Decimal('1e15') and (not nonnegative or n>=0), 'invalid_decimal_range')
    require(not quantity or n.as_tuple().exponent>=-9, 'quantity_precision')
    return n

def timestamp(value):
    """RFC3339 -> exact integer Unix nanoseconds, without float rounding."""
    require(isinstance(value,str), 'invalid_timestamp')
    m=re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',value)
    require(m is not None, 'invalid_timestamp')
    try:
        d=datetime.fromisoformat(m[1]+m[3].replace('Z','+00:00')).astimezone(timezone.utc)
        delta=d-datetime(1970,1,1,tzinfo=timezone.utc)
        return (delta.days*86400+delta.seconds)*10**9+int((m[2] or '').ljust(9,'0'))
    except (ValueError,OverflowError): raise SchemaError('invalid_timestamp') from None

def fresh(at, now, seconds=10):
    require(type(at) is int and type(now) is int and 0<=now-at<=seconds*10**9, 'stale_or_future')

@dataclass(frozen=True)
class Account:
    fingerprint: str
    cash: Decimal
    equity: Decimal
    last_equity: Decimal

@dataclass(frozen=True)
class Clock:
    timestamp_ns: int
    is_open: bool
    next_open_ns: int
    next_close_ns: int

@dataclass(frozen=True)
class Position:
    symbol: str
    quantity: Decimal
    available: Decimal
    market_value: Decimal

@dataclass(frozen=True)
class Order:
    id: str
    client_order_id: str
    symbol: str
    side: str
    quantity: Decimal
    filled: Decimal
    average_price: Decimal | None
    status: str
    submitted_ns: int
    updated_ns: int
    limit_price: Decimal
    # Appended defaults preserve the existing positional constructor. Native
    # parsing always supplies actual values, never invented event timestamps.
    created_ns: int | None = None
    filled_ns: int | None = None
    canceled_ns: int | None = None
    expired_ns: int | None = None
    failed_ns: int | None = None
    extended_hours: bool = False

@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: Decimal
    ask: Decimal
    timestamp_ns: int
    feed: str

@dataclass(frozen=True)
class Snapshot:
    account: Account
    clock: Clock
    positions: tuple
    orders: tuple
    observed_ns: int
    execution_session: object = None


def parse_account(raw, *, expected_fingerprint, origin):
    r=obj(raw)
    require(origin==PAPER_ORIGIN, 'paper_origin_required')
    fingerprint=hashlib.sha256(label(r.get('id')).encode()).hexdigest()
    require(fingerprint==expected_fingerprint, 'account_binding_mismatch')
    require(r.get('status')=='ACTIVE' and r.get('currency')=='USD', 'unsupported_account')
    require(all(r.get(k) is False for k in ('trading_blocked','account_blocked','trade_suspended_by_user')), 'account_blocked_or_unknown')
    return Account(fingerprint,number(r.get('cash')),number(r.get('equity')),number(r.get('last_equity')))

def parse_clock(raw, *, now_ns):
    r=obj(raw)
    at=timestamp(r.get('timestamp')); fresh(at,now_ns)
    require(type(r.get('is_open')) is bool, 'invalid_market_open')
    opening,closing=timestamp(r.get('next_open')),timestamp(r.get('next_close'))
    require(opening>at and closing>at, 'invalid_next_session')
    return Clock(at,r['is_open'],opening,closing)

def parse_positions(raw):
    require(type(raw) is list and len(raw)<=500, 'invalid_positions')
    result=[]
    for r in raw:
        obj(r)
        require(r.get('asset_class')=='us_equity' and r.get('side')=='long', 'unsupported_position')
        q=number(r.get('qty'),quantity=True); available=number(r.get('qty_available'),quantity=True)
        require(q>0 and available<=q, 'invalid_position_quantity')
        result.append(Position(label(r.get('symbol')),q,available,number(r.get('market_value'))))
    require(len({p.symbol for p in result})==len(result), 'duplicate_position')
    return tuple(result)

def parse_order(raw, *, execution_policy=None):
    if execution_policy is not None:
        from execution_session_policy import require_policy
        require_policy(execution_policy)
    return _parse_order(raw,allow_extended=execution_policy is not None)

def _parse_order(raw, *, allow_extended=False):
    """Internal structural decoder; private storage uses only after strict tag validation."""
    r=obj(raw)
    require(r.get('asset_class')=='us_equity' and r.get('side') in ('buy','sell'), 'unsupported_order_asset')
    require(r.get('type')=='limit' and r.get('time_in_force')=='day' and (r.get('extended_hours') is False or (allow_extended is True and r.get('extended_hours') is True)), 'unsupported_order_policy')
    require(r.get('order_class') in ('simple','') and not r.get('legs') and not r.get('replaced_by') and not r.get('replaces') and r.get('notional') is None, 'unsupported_order_structure')
    q,f=number(r.get('qty'),quantity=True),number(r.get('filled_qty'),quantity=True)
    require(q>0 and f<=q, 'invalid_fill_quantity')
    s=r.get('status'); require(s in TERMINAL|WORKING, 'unknown_or_unsupported_order_status')
    require(s!='filled' or f==q, 'filled_status_inconsistent')
    require(s not in ('canceled','expired') or f<q, 'terminal_status_inconsistent')
    require(s!='partially_filled' or 0<f<q, 'partial_status_inconsistent')
    require(s not in ('new','accepted','pending_new','rejected') or f==0, 'zero_fill_status_inconsistent')
    require(s in TERMINAL or f<q, 'working_full_fill_inconsistent')
    avg=None if r.get('filled_avg_price') is None else number(r['filled_avg_price'])
    require(f==0 or (avg is not None and avg>0), 'missing_fill_price')
    created,submitted,updated=(timestamp(r.get(k)) for k in ('created_at','submitted_at','updated_at'))
    require(created<=submitted<=updated, 'order_timestamp_order')
    if s=='filled': require(r.get('filled_at') is not None, 'missing_filled_timestamp')
    events=tuple(None if r.get(field) is None else timestamp(r[field])
                 for field in ('filled_at','canceled_at','expired_at','failed_at'))
    for event in events:
        if event is not None: require(submitted<=event<=updated, 'order_event_timestamp_order')
    price=number(r.get('limit_price')); require(price>0,'invalid_limit_price')
    require(f==0 or (avg<=price if r['side']=='buy' else avg>=price), 'fill_beyond_limit')
    return Order(label(r.get('id')),label(r.get('client_order_id')),label(r.get('symbol')),r['side'],q,f,avg,s,submitted,updated,price,created,*events,r['extended_hours'])

def reconcile_order(previous, current):
    """No stale update, mutation or terminal resurrection is silently accepted."""
    identity=lambda x:(x.id,x.client_order_id,x.symbol,x.side,x.quantity,x.created_ns,x.submitted_ns,x.limit_price,x.extended_hours)
    require(identity(previous)==identity(current), 'order_identity_mutated')
    require(previous.status!='pending_cancel' or current.status in TERMINAL|{'pending_cancel'}, 'cancel_state_regression')
    require(current.updated_ns>=previous.updated_ns and current.filled>=previous.filled, 'order_regression')
    require(current.updated_ns!=previous.updated_ns or current==previous, 'conflicting_same_timestamp')
    require(previous.status not in TERMINAL or current==previous, 'terminal_order_changed')
    return current

def parse_snapshot(account, clock, positions, orders, *, origin, expected_fingerprint, response_account_fingerprint, observed_ns, now_ns, collection_complete=False, execution_policy=None, execution_session=None):
    """Completeness and response identity are trusted caller attestations, NOT provider fields.
    Caller must bind every response to the same authenticated transport and verify every
    known durable client ID independently. A short filtered page alone is not evidence.
    """
    fresh(observed_ns,now_ns)
    require(collection_complete is True, 'collection_completeness_unknown')
    a=parse_account(account,expected_fingerprint=expected_fingerprint,origin=origin)
    require(response_account_fingerprint==a.fingerprint, 'response_context_mismatch')
    require(type(orders) is list and len(orders)<500, 'orders_truncated_or_invalid')
    os=tuple(parse_order(r,execution_policy=execution_policy) for r in orders)
    require(len({o.id for o in os})==len(os) and len({o.client_order_id for o in os})==len(os), 'duplicate_order')
    require(all(o.updated_ns<=observed_ns for o in os),'future_order')
    parsed_clock=parse_clock(clock,now_ns=now_ns)
    context=None
    if execution_policy is not None:
        from execution_session_policy import validate_context
        context=validate_context(execution_session,parsed_clock,now_ns,execution_policy)
    else:require(execution_session is None,'unexpected_execution_context')
    return Snapshot(a,parsed_clock,parse_positions(positions),os,observed_ns,context)

def parse_latest_quotes(raw, *, symbols, feed, now_ns):
    r=obj(raw); require(feed in ('iex','sip'), 'unknown_feed')
    require(type(symbols) is tuple and symbols and len(set(symbols))==len(symbols), 'invalid_symbols')
    # Support the real single-symbol and multi-symbol response envelopes.
    quotes={r.get('symbol'):r['quote']} if 'quote' in r else obj(r.get('quotes'))
    require(set(quotes)==set(symbols), 'quote_universe_mismatch')
    result=[]
    for symbol in symbols:
        q=obj(quotes[symbol]); at=timestamp(q.get('t')); fresh(at,now_ns)
        def price(v):
            require(type(v) in (int,float,str,Decimal), 'invalid_quote_number')
            return number(str(v))
        bid,ask=price(q.get('bp')),price(q.get('ap'))
        # Bounded inputs have <=24 significant digits. A fresh explicit context
        # makes this exact even if caller precision, rounding or exponents differ.
        with localcontext(Context(prec=60)):
            require(0<bid<=ask<=bid*Decimal('1.01'),'invalid_quote_spread')
        require(price(q.get('bs'))>0 and price(q.get('as'))>0,'empty_quote_size')
        result.append(Quote(label(symbol),bid,ask,at,feed))
    return tuple(result)

@dataclass(frozen=True)
class OwnedIntent:
    account_fingerprint: str
    client_order_id: str
    symbol: str
    side: str
    quantity: Decimal


def owned_exit_quantity(snapshot, *, intents, symbol, now_ns):
    """Return a quantity, NEVER an order or permission. All entry/exit orders must be
    reconciled terminal; pending_cancel reserves capacity and therefore blocks exit.
    Exact symbol-position equality excludes mixed manual/strategy ownership.
    """
    fresh(snapshot.observed_ns,now_ns)
    require(type(intents) is tuple and intents, 'missing_owned_intents')
    require(all(type(i) is OwnedIntent and type(i.quantity) is Decimal and i.quantity.is_finite() and 0<i.quantity<=Decimal('1e15') and i.quantity.as_tuple().exponent>=-9 and i.side in ('buy','sell') for i in intents), 'invalid_owned_intent')
    registry={i.client_order_id:i for i in intents}
    require(len(registry)==len(intents), 'duplicate_intent')
    require(all(i.account_fingerprint==snapshot.account.fingerprint for i in intents),'intent_account_mismatch')
    by_client={o.client_order_id:o for o in snapshot.orders}
    require(set(registry)<=set(by_client), 'owned_order_missing')
    net=Decimal(0)
    for cid,i in registry.items():
        o=by_client[cid]
        require((o.symbol,o.side,o.quantity)==(i.symbol,i.side,i.quantity), 'intent_order_mismatch')
        if o.symbol==symbol:
            require(o.status in TERMINAL, 'owned_order_still_working')
            net+=o.filled*(1 if o.side=='buy' else -1)
    require(not any(o.symbol==symbol and o.client_order_id not in registry for o in snapshot.orders),'external_symbol_order')
    p=next((p for p in snapshot.positions if p.symbol==symbol),None)
    require(net>0 and p is not None and p.quantity==net and p.available>=net, 'ownership_or_available_mismatch')
    return net
