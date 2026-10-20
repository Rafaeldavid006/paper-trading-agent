"""Disabled-by-default, GET-only native observations. No execution/startup wiring.

Injected transports are a trusted server boundary, not proof of source authenticity.
Collection completeness means bounded unfiltered pagination plus every independent
ledger-owned lookup. It is not an atomic account snapshot or ownership discovery.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import threading
from urllib import request, error
from zoneinfo import ZoneInfo

from alpaca_schema import (PAPER_ORIGIN, Order, SchemaError, fresh, timestamp,
                           parse_account, parse_clock, parse_order, parse_positions, parse_snapshot,
                           parse_latest_quotes, reconcile_order)
from paper_adapter import NoRedirect, SafetyError
from paper_quotes import _unique_object, _invalid_constant
from shadow_market_inputs import parse_session

DATA_ORIGIN = 'https://data.alpaca.markets'
UNIVERSE = ('AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'MU', 'SPY')
NS = 1_000_000_000
PAGE_SIZE = 100
MAX_PAGES = 5
MAX_BYTES = 524288
MAX_GETS = 12
WINDOW_NS = 60 * NS
MAX_COLLECTION_NS = 10 * NS
_ID = r'[A-Za-z0-9_.-]{1,128}'
ORDER_PATH = '/v2/orders?status=all&limit=100&direction=desc&nested=false'


_CODES = frozenset({
    'account_changed_during_collection',
    'aggregate_orders_overflow',
    'authentication_or_entitlement_failed',
    'budget_clock_regression',
    'budget_state_corrupt',
    'budget_storage_unavailable',
    'closed_session',
    'collection_deadline_exceeded',
    'collection_failed',
    'collector_already_running',
    'collector_disabled',
    'conflicting_response_account',
    'duplicate_order_or_cursor',
    'durable_database_required',
    'evidence_digest_mismatch',
    'forbidden_method',
    'forbidden_origin',
    'forbidden_route',
    'future_order',
    'invalid_database_file',
    'invalid_enable',
    'invalid_expected_binding',
    'invalid_injected_credentials',
    'invalid_local_clock',
    'invalid_owned_ledger',
    'invalid_prior_owned_orders',
    'invalid_provider_payload',
    'invalid_provider_status',
    'invalid_symbols',
    'invalid_timeout',
    'invalid_transport_context',
    'local_clock_regression',
    'native_schema_refused',
    'observed_readonly',
    'order_changed_during_collection',
    'order_page_sort_mismatch',
    'order_pagination_incomplete',
    'owned_ledger_changed_during_collection',
    'owned_ledger_unavailable',
    'owned_lookup_mismatch',
    'owned_lookups_exceed_collection_budget',
    'owned_order_missing',
    'owned_order_missing_from_scan',
    'partial_or_invalid_order_page',
    'partial_response',
    'positions_changed_during_collection',
    'provider_http_error',
    'provider_rate_limited',
    'provider_read_failed',
    'redirect_refused',
    'response_too_large',
    'session_ended_during_collection',
    'session_mismatch',
    'session_validation_refused',
    'shared_request_budget_exhausted',
    'transport_disabled',
    'unsupported_encoding',
    'wrong_quote_feed',
})

class CollectionRefused(SafetyError):
    """Only fixed local codes, never a provider message, URL or credential."""
    def __init__(self, code):
        self.code = code if code in _CODES else 'collection_failed'
        super().__init__(self.code)


def require(condition, code):
    if not condition:
        raise CollectionRefused(code)


def _reject_json(value):
    raise CollectionRefused('invalid_provider_payload')


def _ns(value):
    require(type(value) is int and 0 < value <= 2**63 - 1, 'invalid_local_clock')
    return value


def _datetime_ceil(ns):
    # Session helper works in microseconds. All freshness and future comparisons
    # are ALSO performed in exact integer nanoseconds before using this helper.
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=(ns + 999) // 1000)


def _symbols(symbols):
    require(type(symbols) is tuple and symbols and len(set(symbols)) == len(symbols)
            and all(s in UNIVERSE for s in symbols), 'invalid_symbols')
    return symbols


def quote_path(symbols):
    return '/v2/stocks/quotes/latest?symbols=' + ','.join(_symbols(symbols)) + '&feed=iex'


def _route(path):
    """Return the only permitted origin for an exact, canonical relative path."""
    require(type(path) is str and len(path) < 512, 'forbidden_route')
    if path in ('/v2/account', '/v2/positions', '/v2/clock', ORDER_PATH):
        return PAPER_ORIGIN
    if re.fullmatch(re.escape(ORDER_PATH) + r'&before_order_id=' + _ID, path):
        return PAPER_ORIGIN
    if re.fullmatch(r'/v2/orders:by_client_order_id\?client_order_id=' + _ID, path):
        return PAPER_ORIGIN
    m = re.fullmatch(r'/v2/calendar\?start=(\d{4}-\d{2}-\d{2})&end=(\d{4}-\d{2}-\d{2})', path)
    if m:
        try:
            require(m[1] == m[2] and datetime.strptime(m[1], '%Y-%m-%d').date().isoformat() == m[1], 'forbidden_route')
        except ValueError:
            raise CollectionRefused('forbidden_route') from None
        return PAPER_ORIGIN
    m = re.fullmatch(r'/v2/stocks/quotes/latest\?symbols=([A-Z,]+)&feed=iex', path)
    if m:
        _symbols(tuple(m[1].split(',')))
        return DATA_ORIGIN
    raise CollectionRefused('forbidden_route')


class NativeReadOnlyHTTP:
    """Injected credentials, immutable host/feed policy, GET only, no retries.

    Collection owns the shared quota. Do not call this lower-level wire outside
    the collector. Request timeouts are bounded inactivity timeouts, not a DNS or
    whole-call watchdog. A deployment must provide an external worker deadline.
    """
    paper_origin = PAPER_ORIGIN
    data_origin = DATA_ORIGIN
    feed = 'iex'

    def __init__(self, key, secret, *, enabled=False, paper_origin=PAPER_ORIGIN,
                 data_origin=DATA_ORIGIN):
        require(type(enabled) is bool, 'invalid_enable')
        require(paper_origin == PAPER_ORIGIN and data_origin == DATA_ORIGIN, 'forbidden_origin')
        require(all(type(x) is str and 0 < len(x) <= 512 and all(33 <= ord(c) <= 126 for c in x)
                    for x in (key, secret)), 'invalid_injected_credentials')
        self.enabled = enabled
        self.__headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret,
                          'Accept': 'application/json', 'Accept-Encoding': 'identity'}
        self._opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
        self._timeout = 2.0

    def set_timeout(self, seconds):
        require(type(seconds) in (int, float) and 0 < seconds <= 2, 'invalid_timeout')
        self._timeout = seconds

    def call(self, method, path, body=None):
        require(self.enabled is True, 'transport_disabled')
        require(method == 'GET' and body is None, 'forbidden_method')
        origin = _route(path)
        url = origin + path
        try:
            with self._opener.open(request.Request(url, headers=self.__headers, method='GET'),
                                   timeout=self._timeout) as response:
                require(response.geturl() == url, 'redirect_refused')
                require(type(response.status) is int and response.status == 200, 'provider_http_error')
                require(response.headers.get('Content-Encoding', 'identity') in ('identity', ''), 'unsupported_encoding')
                content_length = response.headers.get('Content-Length')
                if content_length is not None:
                    require(re.fullmatch(r'\d{1,9}', content_length) and int(content_length) <= MAX_BYTES, 'response_too_large')
                raw = response.read(MAX_BYTES + 1)
                require(len(raw) <= MAX_BYTES, 'response_too_large')
                if content_length is not None:
                    require(len(raw) == int(content_length), 'partial_response')
                data = json.loads(raw, parse_float=Decimal, parse_constant=_invalid_constant,
                                  object_pairs_hook=_unique_object)
                return 200, data
        except error.HTTPError as exc:
            code = exc.code
            try:
                exc.close()
            except Exception:
                pass
            # No response body, headers, exception message or credential escapes.
            return code, None
        except CollectionRefused:
            raise
        except Exception:
            raise CollectionRefused('provider_read_failed') from None


class SharedRequestBudget:
    """All participating callers in this SAME durable SQLite DB share 12/60s.

    BEGIN IMMEDIATE reserves before the attempted GET. Failures consume a slot.
    A durable wall-clock high-water mark rejects regressions across restarts.
    Unrelated clients/DBs are not covered. Corrupt state is never auto-reset.
    """
    def __init__(self, database):
        p = Path(database)
        require(p.is_absolute() and str(p) != ':memory:' and p.parent.is_dir(), 'durable_database_required')
        self.database = str(p)
        try:
            fd = os.open(self.database, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                require(stat.S_ISREG(os.fstat(fd).st_mode), 'invalid_database_file')
                os.fchmod(fd, 0o600)
            finally:
                os.close(fd)
            with self.db() as db:
                db.execute('BEGIN IMMEDIATE')
                names = {'native_read_budget', 'native_read_attempts', 'native_read_audit'}
                present = {r[0] for r in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('native_read_budget','native_read_attempts','native_read_audit')")}
                require(not present or present == names, 'budget_state_corrupt')
                new_budget = not present
                db.execute('CREATE TABLE IF NOT EXISTS native_read_budget (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, high_ns INTEGER NOT NULL)')
                db.execute('CREATE TABLE IF NOT EXISTS native_read_attempts (at_ns INTEGER NOT NULL)')
                db.execute('CREATE TABLE IF NOT EXISTS native_read_audit (at_ns INTEGER NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL, owned_count INTEGER NOT NULL, order_count INTEGER NOT NULL, digest TEXT)')
                # Only a genuinely new budget may initialize its high-water mark.
                if not db.execute('SELECT 1 FROM native_read_budget').fetchone():
                    require(new_budget and db.execute('SELECT COUNT(*) FROM native_read_attempts').fetchone()[0] == 0, 'budget_state_corrupt')
                    db.execute('INSERT INTO native_read_budget VALUES(1,1,0)')
        except CollectionRefused:
            raise
        except Exception:
            raise CollectionRefused('budget_storage_unavailable') from None

    @contextmanager
    def db(self):
        db = None
        try:
            require(not Path(self.database).is_symlink(), 'invalid_database_file')
            db = sqlite3.connect(self.database, timeout=0.05)
            with db:
                yield db
        except CollectionRefused:
            raise
        except Exception:
            raise CollectionRefused('budget_storage_unavailable') from None
        finally:
            if db is not None:
                db.close()

    def reserve(self, now_ns):
        now_ns = _ns(now_ns)
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            meta = db.execute('SELECT id,version,high_ns FROM native_read_budget').fetchall()
            require(len(meta) == 1 and meta[0][:2] == (1, 1) and type(meta[0][2]) is int
                    and 0 <= meta[0][2] <= 2**63 - 1, 'budget_state_corrupt')
            previous = meta[0][2]
            require(now_ns >= previous, 'budget_clock_regression')
            times = [r[0] for r in db.execute('SELECT at_ns FROM native_read_attempts')]
            require(len(times) <= MAX_GETS and all(type(t) is int and 0 < t <= previous for t in times), 'budget_state_corrupt')
            db.execute('DELETE FROM native_read_attempts WHERE at_ns <= ?', (now_ns - WINDOW_NS,))
            count = db.execute('SELECT COUNT(*) FROM native_read_attempts').fetchone()[0]
            require(count < MAX_GETS, 'shared_request_budget_exhausted')
            db.execute('UPDATE native_read_budget SET high_ns=? WHERE id=1', (now_ns,))
            db.execute('INSERT INTO native_read_attempts VALUES(?)', (now_ns,))

    def audit(self, *, at_ns, status, attempts, owned_count, order_count, digest=None, rowid=None):
        # Caller passes only local fixed status codes and integer aggregates.
        with self.db() as db:
            values = (at_ns, status, attempts, owned_count, order_count, digest)
            if rowid is None:
                return db.execute('INSERT INTO native_read_audit VALUES(?,?,?,?,?,?)', values).lastrowid
            changed = db.execute(
                'UPDATE native_read_audit SET at_ns=?,status=?,attempts=?,owned_count=?,order_count=?,digest=? WHERE rowid=?',
                values + (rowid,)).rowcount
            require(changed == 1, 'budget_state_corrupt')
            return rowid


@dataclass(frozen=True)
class OwnedLedgerView:
    """Caller attestation from an independent durable ledger, never scan inference.

    revision must change whenever durable IDs or their prior order state changes.
    The loader must read the complete registry for this account, not an active-only
    filter. previous_orders allows already-known native order state reconciliation.
    """
    account_fingerprint: str = field(repr=False)
    revision: int
    client_ids: tuple = field(repr=False)
    previous_orders: tuple = field(default=(), repr=False)


@dataclass(frozen=True)
class NativeObservation:
    """Private in-memory read evidence. NOT a fixture source or execution port."""
    observed_ns: int
    session_open_ns: int
    requests: int
    order_pages: int
    owned_count: int
    order_count: int
    digest: str
    snapshot: object = field(repr=False)
    quotes: tuple = field(repr=False)
    _native_json: bytes = field(repr=False)
    mode: str = field(default='readonly_native_observation', init=False)
    executable: bool = field(default=False, init=False)
    coverage: str = field(default='bounded_all_status_scan_plus_supplied_durable_owned_ids', init=False)

    def native_arguments(self):
        """Private in-process parser/observation seam; do not log or persist this.

        Exact existing Lifecycle.observe kwargs (excluding its separately injected
        now_ns). It contains private raw account ID and balances in memory only.
        No read()/submit()/claim() or NativeFixtureObservations inheritance.
        """
        require(hashlib.sha256(self._native_json).hexdigest() == self.digest, 'evidence_digest_mismatch')
        result = json.loads(self._native_json)
        result['quote_symbols'] = tuple(result['quote_symbols'])
        return result

    def summary(self):
        return {k: getattr(self, k) for k in ('mode', 'executable', 'observed_ns',
                'requests', 'order_pages', 'owned_count', 'order_count', 'digest', 'coverage')}


class NativeObservationCollector:
    """Explicit server enable and a stable independent ledger view are mandatory.

    A successful observation never spans >10s measured by monotonic time. This
    does NOT forcibly interrupt an uncooperative injected call/DNS; production
    needs a separate process watchdog. No env reads, startup hooks or scheduling.
    """
    def __init__(self, *, database, transport, expected_fingerprint, ledger_view,
                 symbols=('AAPL',), enabled=False, now_ns=time.time_ns,
                 monotonic_ns=time.monotonic_ns):
        require(type(enabled) is bool, 'invalid_enable')
        self.enabled = enabled
        self.database = str(database)
        self.transport = transport
        self.expected = expected_fingerprint
        self.ledger_view = ledger_view
        self.symbols = symbols
        self.now_ns = now_ns
        self.monotonic_ns = monotonic_ns
        self._running = False
        self._lock = threading.Lock()

    def _check_time(self):
        wall, mono = _ns(self.now_ns()), _ns(self.monotonic_ns())
        require(wall >= self._last_wall and mono >= self._last_mono, 'local_clock_regression')
        require(mono - self._start_mono <= MAX_COLLECTION_NS and wall - self._start_wall <= MAX_COLLECTION_NS,
                'collection_deadline_exceeded')
        self._last_wall, self._last_mono = wall, mono
        return wall

    def _view(self):
        try:
            view = self.ledger_view()
        except Exception:
            raise CollectionRefused('owned_ledger_unavailable') from None
        require(type(view) is OwnedLedgerView and view.account_fingerprint == self.expected
                and type(view.revision) is int and view.revision >= 0
                and type(view.client_ids) is tuple and len(set(view.client_ids)) == len(view.client_ids)
                and all(type(cid) is str and re.fullmatch(_ID, cid) for cid in view.client_ids)
                and type(view.previous_orders) is tuple
                and all(type(o) is Order for o in view.previous_orders), 'invalid_owned_ledger')
        require(len(view.client_ids) <= 4, 'owned_lookups_exceed_collection_budget')
        require(len({o.client_order_id for o in view.previous_orders}) == len(view.previous_orders)
                and all(o.client_order_id in view.client_ids for o in view.previous_orders), 'invalid_prior_owned_orders')
        return view

    def _get(self, path):
        _route(path)
        wall = self._check_time()
        self._budget.reserve(wall)
        self._requests += 1
        self._check_time()
        remaining = (MAX_COLLECTION_NS - (self._last_mono - self._start_mono)) / NS
        require(remaining > 0, 'collection_deadline_exceeded')
        if type(self.transport) is NativeReadOnlyHTTP:
            self.transport.set_timeout(min(2.0, remaining))
        try:
            status, raw = self.transport.call('GET', path)
        except CollectionRefused:
            raise
        except Exception:
            raise CollectionRefused('provider_read_failed') from None
        received_ns = self._check_time()
        require(type(status) is int, 'invalid_provider_status')
        if status in (401, 403):
            raise CollectionRefused('authentication_or_entitlement_failed')
        if status == 429:
            raise CollectionRefused('provider_rate_limited')
        if status == 404 and path.startswith('/v2/orders:by_client_order_id?'):
            raise CollectionRefused('owned_order_missing')
        require(status == 200, 'provider_http_error')
        # Freeze trusted injected mutable objects at receipt, as real JSON does.
        try:
            return copy.deepcopy(raw), received_ns
        except Exception:
            raise CollectionRefused('invalid_provider_payload') from None

    def _context(self, value, identity):
        rows = value if type(value) is list else [value]
        for row in rows:
            require(type(row) is dict, 'invalid_provider_payload')
            require('account_id' not in row or row['account_id'] == identity, 'conflicting_response_account')

    def collect(self):
        require(self.enabled is True, 'collector_disabled')
        require(self._lock.acquire(blocking=False), 'collector_already_running')
        # One collector instance is single-use-at-a-time; independent instances
        # share quota through SQLite. The production caller serializes an instance.
        self._running = True
        self._requests = self._pages = self._owned_count = self._order_count = 0
        self._budget = None
        self._audit_rowid = None
        status = 'collection_failed'
        digest = None
        try:
            require(type(self.expected) is str and re.fullmatch(r'[0-9a-f]{64}', self.expected), 'invalid_expected_binding')
            _symbols(self.symbols)
            require(getattr(self.transport, 'paper_origin', None) == PAPER_ORIGIN
                    and getattr(self.transport, 'data_origin', None) == DATA_ORIGIN
                    and getattr(self.transport, 'feed', None) == 'iex', 'invalid_transport_context')
            self._start_wall = self._last_wall = _ns(self.now_ns())
            self._start_mono = self._last_mono = _ns(self.monotonic_ns())
            self._budget = SharedRequestBudget(self.database)
            view = self._view()
            self._owned_count = len(view.client_ids)
            account, _ = self._get('/v2/account')
            a = parse_account(account, expected_fingerprint=self.expected, origin=PAPER_ORIGIN)
            identity = account['id']  # private raw ID remains only in memory
            self._context(account, identity)
            clock, clock_received = self._get('/v2/clock')
            self._context(clock, identity)
            c = parse_clock(clock, now_ns=clock_received)
            require(c.is_open, 'closed_session')
            day = _datetime_ceil(clock_received).astimezone(ZoneInfo('America/New_York')).date().isoformat()
            calendar, calendar_received = self._get('/v2/calendar?start=' + day + '&end=' + day)
            self._context(calendar, identity)
            session = parse_session(clock, calendar, received_at=_datetime_ceil(calendar_received),
                                    now=_datetime_ceil(calendar_received))
            session_open = timestamp(session.open.isoformat())
            require(session_open <= c.timestamp_ns <= calendar_received < c.next_close_ns, 'session_mismatch')
            positions, _ = self._get('/v2/positions')
            self._context(positions, identity)
            orders, parsed, ids, clients = [], [], set(), set()
            path = ORDER_PATH
            for _ in range(MAX_PAGES):
                page, received = self._get(path)
                self._pages += 1
                require(type(page) is list and len(page) <= PAGE_SIZE, 'partial_or_invalid_order_page')
                self._context(page, identity)
                for raw in page:
                    order = parse_order(raw)
                    require(order.updated_ns <= received, 'future_order')
                    require(order.id not in ids and order.client_order_id not in clients, 'duplicate_order_or_cursor')
                    require(not parsed or order.submitted_ns <= parsed[-1].submitted_ns, 'order_page_sort_mismatch')
                    ids.add(order.id); clients.add(order.client_order_id)
                    orders.append(raw); parsed.append(order)
                    require(len(orders) < 500, 'aggregate_orders_overflow')
                self._order_count = len(orders)
                if len(page) < PAGE_SIZE:
                    break
                path = ORDER_PATH + '&before_order_id=' + parsed[-1].id
            else:
                raise CollectionRefused('order_pagination_incomplete')
            by_client = {o.client_order_id: o for o in parsed}
            prior = {o.client_order_id: o for o in view.previous_orders}
            for cid in view.client_ids:
                # Independently query every durable ID, even absent from the scan.
                raw, received = self._get('/v2/orders:by_client_order_id?client_order_id=' + cid)
                self._context(raw, identity)
                current = parse_order(raw)
                require(current.client_order_id == cid and current.updated_ns <= received, 'owned_lookup_mismatch')
                require(cid in by_client, 'owned_order_missing_from_scan')
                if cid in prior:
                    reconcile_order(prior[cid], current)
                reconcile_order(by_client[cid], current)
                require(by_client[cid] == current, 'order_changed_during_collection')
            quotes_raw, quote_received = self._get(quote_path(self.symbols))
            require(type(quotes_raw) is dict and quotes_raw.get('feed', 'iex') == 'iex', 'wrong_quote_feed')
            require(type(quotes_raw.get('quotes')) is dict and 'quote' not in quotes_raw, 'invalid_provider_payload')
            for row in quotes_raw.get('quotes', {}).values() if type(quotes_raw.get('quotes')) is dict else ():
                require(type(row) is dict and row.get('feed', 'iex') == 'iex', 'wrong_quote_feed')
            self._context(quotes_raw, identity)
            quotes = parse_latest_quotes(quotes_raw, symbols=self.symbols, feed='iex', now_ns=quote_received)
            positions_after, _ = self._get('/v2/positions')
            self._context(positions_after, identity)
            require(parse_positions(positions) == parse_positions(positions_after), 'positions_changed_during_collection')
            account_after, _ = self._get('/v2/account')
            self._context(account_after, identity)
            a_after = parse_account(account_after, expected_fingerprint=self.expected, origin=PAPER_ORIGIN)
            require(a == a_after and account['id'] == account_after['id'], 'account_changed_during_collection')
            require(self._view() == view, 'owned_ledger_changed_during_collection')
            observed = self._check_time()
            fresh(c.timestamp_ns, observed)
            require(observed < c.next_close_ns, 'session_ended_during_collection')
            for q in quotes:
                fresh(q.timestamp_ns, observed)
            native = dict(account=account, clock=clock, positions=positions, orders=orders,
                          origin=PAPER_ORIGIN,
                          response_account_fingerprint=a.fingerprint, observed_ns=observed,
                          collection_complete=True, quotes=quotes_raw, quote_symbols=self.symbols, feed='iex')
            snapshot = parse_snapshot(account, clock, positions, orders, origin=PAPER_ORIGIN,
                                      expected_fingerprint=self.expected, response_account_fingerprint=a.fingerprint,
                                      observed_ns=observed, now_ns=observed, collection_complete=True)
            native_json = json.dumps(native, sort_keys=True, separators=(',', ':'),
                                     default=lambda v: str(v) if type(v) is Decimal else _reject_json(v)).encode()
            digest = hashlib.sha256(native_json).hexdigest()
            self._check_time()
            status = 'observed_readonly'
            result = NativeObservation(observed, session_open, self._requests, self._pages,
                                       self._owned_count, self._order_count, digest, snapshot, quotes, native_json)
            # Audit failure refuses success; raw provider evidence never goes to DB.
            self._audit_rowid = self._budget.audit(at_ns=observed, status=status, attempts=self._requests,
                               owned_count=self._owned_count, order_count=self._order_count, digest=digest)
            accepted_ns = self._check_time()
            fresh(observed, accepted_ns)
            fresh(c.timestamp_ns, accepted_ns)
            for q in quotes:
                fresh(q.timestamp_ns, accepted_ns)
            require(accepted_ns < c.next_close_ns, 'session_ended_during_collection')
            return result
        except SchemaError:
            status = 'native_schema_refused'
            raise CollectionRefused(status) from None
        except CollectionRefused as exc:
            status = exc.code
            raise
        except SafetyError:
            status = 'session_validation_refused'
            raise CollectionRefused(status) from None
        except Exception:
            status = 'collection_failed'
            raise CollectionRefused(status) from None
        finally:
            if status != 'observed_readonly' and self._budget is not None:
                try:
                    self._budget.audit(at_ns=self._last_wall, status=status, attempts=self._requests,
                                       owned_count=self._owned_count, order_count=self._order_count,
                                       rowid=self._audit_rowid)
                except Exception:
                    pass
            self._running = False
            self._lock.release()
