"""Private Decimal lifecycle persistence, distinct from native provider input.

Native account identity is checked by alpaca_schema before this projection is
created. A stored projection is never accepted as native observation evidence.
No raw account identifier, unvalidated provider fields, or refreshed timestamp
is written. This is trusted local state, not tamper-proof provider evidence.
"""
from contextlib import contextmanager
from decimal import Decimal, localcontext
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from alpaca_schema import (Account, Clock, Position, Quote, Snapshot, SchemaError,
                           fresh, label, number, parse_latest_quotes, parse_order, _parse_order,
                           timestamp)

VERSION = 1
MAX_JSON_BYTES = 2 * 1024 * 1024
ORDER_FIELDS = frozenset((
    'id', 'client_order_id', 'symbol', 'asset_class', 'side', 'type',
    'order_class', 'time_in_force', 'extended_hours', 'qty', 'filled_qty',
    'filled_avg_price', 'limit_price', 'status', 'created_at', 'submitted_at',
    'updated_at', 'filled_at', 'canceled_at', 'expired_at', 'failed_at',
    'replaced_by', 'replaces', 'notional', 'legs',
))


class StorageError(ValueError):
    pass


def require(ok, code='invalid_stored_projection'):
    if not ok:
        raise StorageError(code)


def _fields(value, expected):
    require(type(value) is dict and set(value) == set(expected))
    return value


def _ns(value):
    # Native RFC3339 supports years 0001 through 9999. No float conversions.
    require(type(value) is int and -62135596800000000000 <= value <= 253402300799999999999)
    return value


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def loads(value):
    require(type(value) is str and len(value.encode('utf-8')) <= MAX_JSON_BYTES)
    try:
        return json.loads(value, object_pairs_hook=_pairs,
                          parse_constant=lambda _: require(False))
    except (ValueError, TypeError, RecursionError):
        raise StorageError('invalid_stored_projection') from None


def dumps(value):
    result = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    require(len(result.encode('utf-8')) <= MAX_JSON_BYTES, 'projection_too_large')
    return result


def project_order(raw, *, execution_policy=None):
    parse_order(raw,execution_policy=execution_policy)
    return _project_order(raw,allow_extended=execution_policy is not None)

def _project_order(raw, *, allow_extended=False):
    """Keep validated native-shaped response fields for private reconciliation.

    Preserve actual created/submitted/event timestamps, including their precision.
    Do not invent provider IDs or timestamps when materializing stored orders.
    Falsey optional structure is normalized, not copied as unvalidated content.
    """
    order = _parse_order(raw,allow_extended=allow_extended)
    result = {key: raw.get(key) for key in ORDER_FIELDS}
    result.update(asset_class='us_equity', type='limit', time_in_force='day',
                  order_class='simple', extended_hours=order.extended_hours,
                  replaced_by=None, replaces=None, notional=None, legs=None,
                  qty=format(order.quantity, 'f'), filled_qty=format(order.filled, 'f'),
                  limit_price=format(order.limit_price, 'f'),
                  filled_avg_price=None if order.average_price is None else format(order.average_price, 'f'))
    if order.extended_hours:result['execution_policy']='iex_premarket'
    return result


def decode_order(value):
    tagged=type(value) is dict and 'execution_policy' in value
    _fields(value, ORDER_FIELDS|{'execution_policy'} if tagged else ORDER_FIELDS)
    if tagged:require(value['execution_policy']=='iex_premarket' and value['extended_hours'] is True)
    # A stored tag describes actual historical policy; it grants no fresh action.
    parsed = _parse_order(value,allow_extended=tagged)
    require(_project_order(value,allow_extended=tagged) == value)
    return parsed


def _quote_projection(raw, symbols, feed):
    """Validate quote shape without claiming that persistence makes it fresh.

    Legacy observe accepts absent/bad quotes; only claims require valid fresh
    quotes. Preserve that boundary without persisting any invalid provider data.
    Freshness is checked from ORIGINAL quote timestamps when a claim is made.
    """
    try:
        require(type(raw) is dict and type(symbols) in (tuple, list) and symbols
                and len(symbols) <= 500 and len(set(symbols)) == len(symbols))
        envelope = {raw.get('symbol'): raw['quote']} if 'quote' in raw else raw.get('quotes')
        require(type(envelope) is dict and set(envelope) == set(symbols))
        result = []
        for symbol in symbols:
            quote = envelope[symbol]
            require(type(quote) is dict)
            at = timestamp(quote.get('t'))
            # This validates structure at the quote's OWN timestamp. It is not a
            # new observation clock and does not assert freshness versus now.
            q = parse_latest_quotes({'symbol': symbol, 'quote': quote},
                                    symbols=(symbol,), feed=feed, now_ns=at)[0]
            result.append(dict(symbol=q.symbol, bid=format(q.bid, 'f'), ask=format(q.ask, 'f'),
                               timestamp_ns=q.timestamp_ns, feed=q.feed))
        return result
    except (StorageError, SchemaError, TypeError, ValueError, KeyError):
        return None


def project_snapshot(snapshot, raw, *, execution_policy=None):
    a, c = snapshot.account, snapshot.clock
    result = {
        'format': 'decimal_lifecycle_snapshot', 'version': VERSION,
        'account': dict(fingerprint=a.fingerprint, cash=format(a.cash, 'f'),
                        equity=format(a.equity, 'f'), last_equity=format(a.last_equity, 'f')),
        'clock': dict(timestamp_ns=c.timestamp_ns, is_open=c.is_open,
                      next_open_ns=c.next_open_ns, next_close_ns=c.next_close_ns),
        'positions': [dict(symbol=p.symbol, quantity=format(p.quantity, 'f'),
                           available=format(p.available, 'f'), market_value=format(p.market_value, 'f'))
                      for p in snapshot.positions],
        'orders': [project_order(o,execution_policy=execution_policy) for o in raw['orders']],
        'observed_ns': snapshot.observed_ns,
        'quotes': _quote_projection(raw['quotes'], raw['quote_symbols'], raw['feed']),
    }
    if snapshot.execution_session is not None:result['execution_session']=snapshot.execution_session.document()
    return result


def decode_snapshot(value, *, binding, now_ns=None):
    """Decode ONLY the private versioned projection, never native provider input.

    Reopening storage validates structure but does not refresh the observation or
    run session accounting. Execution paths additionally check original freshness.
    """
    context_field=type(value) is dict and 'execution_session' in value
    _fields(value, ('format', 'version', 'account', 'clock', 'positions', 'orders', 'observed_ns', 'quotes')+(('execution_session',) if context_field else ()))
    require(value['format'] == 'decimal_lifecycle_snapshot' and type(value['version']) is int
            and value['version'] == VERSION, 'unsupported_storage_version')
    a = _fields(value['account'], ('fingerprint', 'cash', 'equity', 'last_equity'))
    require(a['fingerprint'] == binding, 'stored_account_binding_mismatch')
    account = Account(binding, number(a['cash']), number(a['equity']), number(a['last_equity']))
    c = _fields(value['clock'], ('timestamp_ns', 'is_open', 'next_open_ns', 'next_close_ns'))
    at, opening, closing = (_ns(c[k]) for k in ('timestamp_ns', 'next_open_ns', 'next_close_ns'))
    require(type(c['is_open']) is bool and opening > at and closing > at)
    clock = Clock(at, c['is_open'], opening, closing)
    positions = value['positions']
    require(type(positions) is list and len(positions) <= 500)
    ps = []
    for p in positions:
        _fields(p, ('symbol', 'quantity', 'available', 'market_value'))
        q, available = number(p['quantity'], quantity=True), number(p['available'], quantity=True)
        require(q > 0 and available <= q)
        ps.append(Position(label(p['symbol']), q, available, number(p['market_value'])))
    require(len({p.symbol for p in ps}) == len(ps))
    require(type(value['orders']) is list and len(value['orders']) < 500)
    orders = tuple(decode_order(o) for o in value['orders'])
    require(len({o.id for o in orders}) == len(orders)
            and len({o.client_order_id for o in orders}) == len(orders))
    observed = _ns(value['observed_ns'])
    require(all(o.updated_ns <= observed for o in orders))
    # Validate all projected quote data on every read, but require freshness only
    # for claims. Cancellation has never depended on a usable quote.
    decode_quotes(value['quotes'])
    if now_ns is not None:
        fresh(observed, now_ns)
        fresh(clock.timestamp_ns, now_ns)
    context=None
    if context_field:
        from execution_session_policy import stored_context
        context=stored_context(value['execution_session'],clock)
    require(context is not None or not any(o.extended_hours for o in orders),'stored_execution_context_missing')
    return Snapshot(account, clock, tuple(ps), orders, observed,context)


def decode_quotes(value, *, now_ns=None):
    if value is None:
        return ()
    require(type(value) is list and 0 < len(value) <= 500)
    result = []
    for q in value:
        _fields(q, ('symbol', 'bid', 'ask', 'timestamp_ns', 'feed'))
        bid, ask = number(q['bid']), number(q['ask'])
        with localcontext() as context:
            context.prec = 60
            require(0 < bid <= ask <= bid * Decimal('1.01'))
        require(q['feed'] in ('iex', 'sip'))
        at = _ns(q['timestamp_ns'])
        if now_ns is not None:
            fresh(at, now_ns)
        result.append(Quote(label(q['symbol']), bid, ask, at, q['feed']))
    require(len({q.symbol for q in result}) == len(result)
            and len({q.feed for q in result}) == 1)
    return tuple(result)


def private_file(path, *, create=False):
    """Check owner-only regular SQLite files without silently changing modes.

    Reject symlinks in every path component and sidecar, hard links, foreign
    ownership, or anything other than exact mode 0600. Keep existing data intact.
    """
    p = Path(path)
    require(p.is_absolute() and '..' not in p.parts, 'private_storage_required')
    try:
        for parent in reversed(p.parents):
            require(not parent.is_symlink() and parent.is_dir(), 'private_storage_required')
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
        if create:
            flags |= os.O_CREAT
        fd = os.open(p, flags, 0o600)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                    and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1,
                    'private_storage_required')
            current = p.stat(follow_symlinks=False)
            require((current.st_dev, current.st_ino) == (info.st_dev, info.st_ino),
                    'private_storage_changed')
            for suffix in ('-journal', '-wal', '-shm'):
                sidecar = Path(str(p) + suffix)
                try:
                    side = sidecar.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                require(stat.S_ISREG(side.st_mode) and side.st_uid == info.st_uid
                        and stat.S_IMODE(side.st_mode) == 0o600 and side.st_nlink == 1,
                        'private_storage_required')
            return info.st_dev, info.st_ino
        finally:
            os.close(fd)
    except OSError:
        raise StorageError('private_storage_unavailable') from None


@contextmanager
def transaction(path, *, identity):
    """Bounded durable transaction; never create/recreate a missing ledger."""
    require(private_file(path) == identity, 'private_storage_changed')
    db = None
    try:
        db = sqlite3.connect(Path(path).as_uri() + '?mode=rw', uri=True,
                             timeout=1.0, isolation_level=None)
        require(private_file(path) == identity, 'private_storage_changed')
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA trusted_schema=OFF')
        db.execute('PRAGMA temp_store=MEMORY')
        db.execute('PRAGMA synchronous=FULL')
        # SQL CPU work is bounded too; it cannot silently hold an unbounded lock.
        deadline = time.monotonic() + 5.0
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.execute('BEGIN IMMEDIATE')
        require(private_file(path) == identity, 'private_storage_changed')
        try:
            yield db
            require(private_file(path) == identity, 'private_storage_changed')
            db.commit()
        except BaseException:
            db.rollback()
            raise
    finally:
        if db is not None:
            db.close()
        require(private_file(path) == identity, 'private_storage_changed')
