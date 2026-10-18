"""Isolated, opt-in Alpaca paper execution. Never imported by the web service.

No environment configuration, strategy, or AI-text parser. Monetary arithmetic
uses Decimal; only whole-share, regular-session, day limit orders are supported.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import sqlite3
from urllib import request, error
from zoneinfo import ZoneInfo

PAPER_HOST = 'https://paper-api.alpaca.markets'
TERMINAL = {'filled', 'canceled', 'expired', 'rejected'}
ACTIVE = {'new', 'accepted', 'pending_new', 'partially_filled', 'pending_cancel', 'accepted_for_bidding'}


class SafetyError(RuntimeError):
    pass


def money(value):
    if isinstance(value, bool):
        raise SafetyError('Invalid numeric value')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise SafetyError('Invalid numeric value') from None
    if not result.is_finite() or result < 0:
        raise SafetyError('Negative or nonfinite numeric value')
    return result


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SafetyError('Redirect refused')


class PaperHTTP:
    """Real HTTP transport; constructing it makes no requests. No proxy/redirects.

    Callers must obtain separately authorized paper credentials. Never log them.
    Only the exact fixed paper host and supported v2 endpoints are accepted.
    """
    def __init__(self, key, secret, base_url=PAPER_HOST):
        if base_url != PAPER_HOST:
            raise SafetyError('Only the exact paper host is allowed')
        if not key or not secret:
            raise SafetyError('Paper credentials required')
        self._headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret,
                         'Content-Type': 'application/json'}
        self._opener = request.build_opener(request.ProxyHandler({}), NoRedirect())

    def call(self, method, path, body=None):
        allowed = (method == 'GET' and (
            path in ('/v2/account', '/v2/positions', '/v2/clock', '/v2/orders?status=open&limit=500&nested=false')
            or re.fullmatch(r'/v2/assets/[A-Z][A-Z0-9.\-]{0,14}', path)
            or re.fullmatch(r'/v2/orders:by_client_order_id\?client_order_id=pa-[a-f0-9]{40}', path)))
        allowed = bool(allowed)
        allowed |= method == 'POST' and path == '/v2/orders'
        allowed |= method == 'DELETE' and bool(re.fullmatch(r'/v2/orders/[A-Za-z0-9\-]+', path))
        if not allowed:
            raise SafetyError('Unsupported endpoint')
        req = request.Request(PAPER_HOST + path, data=None if body is None else json.dumps(body).encode(),
                              headers=self._headers, method=method)
        try:
            with self._opener.open(req, timeout=10) as response:
                if response.geturl() != PAPER_HOST + path:
                    raise SafetyError('Response URL mismatch')
                data = response.read(2_000_001)
                if len(data) > 2_000_000:
                    raise SafetyError('Oversized response')
                return response.status, json.loads(data) if data else None
        except error.HTTPError as exc:
            # Do not expose credential-bearing requests or raw response bodies.
            return exc.code, None


@dataclass(frozen=True)
class Intent:
    key: str
    symbol: str
    side: str
    qty: int
    limit_price: str


class PaperAdapter:
    def __init__(self, database, transport, *, allowlist=(), enabled=False,
                 expected_account_id=None, now=None):
        if str(database) == ':memory:':
            raise SafetyError('Durable database required')
        self.database, self.transport = str(database), transport
        self.enabled = enabled is True
        self.allowlist = frozenset(allowlist)
        self.expected_account_id = expected_account_id
        self.now = now or (lambda: datetime.now(timezone.utc))
        with self.db() as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS safety (id INTEGER PRIMARY KEY CHECK(id=1), stopped INTEGER NOT NULL);
            INSERT OR IGNORE INTO safety VALUES(1,0);
            CREATE TABLE IF NOT EXISTS intents (client_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                created REAL NOT NULL, state TEXT NOT NULL, broker_id TEXT, filled TEXT NOT NULL DEFAULT '0');
            CREATE TABLE IF NOT EXISTS cancellation (id INTEGER PRIMARY KEY, broker_id TEXT NOT NULL,
                created REAL NOT NULL, outcome TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS loss_days (day TEXT PRIMARY KEY, stopped INTEGER NOT NULL);
            ''')

    @contextmanager
    def db(self):
        c = sqlite3.connect(self.database, timeout=15)
        c.row_factory = sqlite3.Row
        try:
            with c:
                yield c
        finally:
            c.close()

    def _get(self, path):
        status, data = self.transport.call('GET', path)
        if status != 200:
            raise SafetyError('Read failed; execution blocked')
        return data

    def preflight(self):
        """Read-only broker checks, available while execution is disabled."""
        account = self._get('/v2/account')
        if not self.expected_account_id or account.get('id') != self.expected_account_id:
            raise SafetyError('Account identity not verified')
        if account.get('status') != 'ACTIVE' or account.get('currency') != 'USD':
            raise SafetyError('Account is not active USD')
        if any(account.get(k) is not False for k in ('trading_blocked', 'account_blocked', 'trade_suspended_by_user')):
            raise SafetyError('Account block state missing or blocked')
        if money(account['multiplier']) != 1 or money(account['short_market_value']) != 0:
            raise SafetyError('Leverage or short exposure is forbidden')
        for key in ('cash', 'equity', 'last_equity', 'long_market_value'):
            money(account[key])
        positions = self._get('/v2/positions')
        orders = self._get('/v2/orders?status=open&limit=500&nested=false')
        if not isinstance(positions, list) or not isinstance(orders, list) or len(orders) >= 500:
            raise SafetyError('Incomplete broker snapshot')
        for p in positions:
            if p.get('asset_class') != 'us_equity' or p.get('side') != 'long':
                raise SafetyError('Non-equity or short position')
            money(p['qty']); money(p['market_value'])
        clock = self._get('/v2/clock')
        stamp = datetime.fromisoformat(clock['timestamp'].replace('Z', '+00:00'))
        age = (self.now() - stamp).total_seconds()
        if stamp.tzinfo is None or not 0 <= age <= 10:
            raise SafetyError('Stale broker clock')
        return account, positions, orders, clock

    def _record(self, c, row, order):
        payload = json.loads(row['payload'])
        for key in ('client_order_id', 'symbol', 'side', 'type', 'time_in_force'):
            if order.get(key) != payload[key]:
                raise SafetyError('Broker order identity mismatch')
        if money(order['qty']) != money(payload['qty']) or money(order['limit_price']) != money(payload['limit_price']):
            raise SafetyError('Broker order size mismatch')
        filled = money(order['filled_qty'])
        if filled < money(row['filled']) or filled > money(payload['qty']):
            raise SafetyError('Invalid fill progression')
        state = order.get('status')
        if state not in ACTIVE | TERMINAL or not re.fullmatch(r'[A-Za-z0-9\-]+', order.get('id', '')):
            raise SafetyError('Unknown broker order state')
        if state == 'filled' and filled != money(payload['qty']):
            raise SafetyError('Inconsistent filled status')
        c.execute('UPDATE intents SET state=?,broker_id=?,filled=? WHERE client_id=?',
                  (state, order['id'], str(filled), row['client_id']))

    def reconcile(self):
        """Look up every nonterminal durable intent. A 404 is NOT permission to retry."""
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            for row in c.execute('SELECT * FROM intents').fetchall():
                if row['state'] in TERMINAL:
                    continue
                status, order = self.transport.call('GET', '/v2/orders:by_client_order_id?client_order_id=' + row['client_id'])
                if status == 200:
                    self._record(c, row, order)
                else:
                    c.execute("UPDATE intents SET state='uncertain' WHERE client_id=?", (row['client_id'],))
        return self.records()

    def records(self):
        with self.db() as c:
            return [dict(r) for r in c.execute('SELECT * FROM intents ORDER BY created,client_id')]

    def submit(self, intent):
        if not self.enabled:
            raise SafetyError('Paper execution is disabled')
        if type(intent) is not Intent or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,120}', intent.key):
            raise SafetyError('Explicit structured intent required')
        if intent.symbol not in self.allowlist or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', intent.symbol):
            raise SafetyError('Symbol not allowed')
        if intent.side not in ('buy', 'sell') or type(intent.qty) is not int or intent.qty <= 0:
            raise SafetyError('Whole shares and buy/sell only')
        price = money(intent.limit_price)
        if price < 1 or price != price.quantize(Decimal('.01')):
            raise SafetyError('Price must be at least $1 with cent precision')
        cid = 'pa-' + hashlib.sha256(intent.key.encode()).hexdigest()[:40]
        payload = dict(client_order_id=cid, symbol=intent.symbol, side=intent.side,
                       qty=str(intent.qty), type='limit', limit_price=str(price),
                       time_in_force='day', extended_hours=False)
        encoded = json.dumps(payload, sort_keys=True)
        self.reconcile()
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            prior = c.execute('SELECT * FROM intents WHERE client_id=?', (cid,)).fetchone()
            if prior:
                if prior['payload'] != encoded:
                    raise SafetyError('Idempotency key reused with different intent')
                return dict(prior)  # Never POST twice, even after rejection or 404.
            if c.execute('SELECT stopped FROM safety').fetchone()[0]:
                raise SafetyError('Persistent kill switch is on')
            rows = c.execute('SELECT * FROM intents').fetchall()
            if any(r['state'] in ('uncertain', 'reserved') for r in rows):
                raise SafetyError('Uncertain order must be reconciled')
            timestamp = self.now().timestamp()
            if any(r['created'] > timestamp for r in rows) or sum(r['created'] > timestamp - 60 for r in rows) >= 2:
                raise SafetyError('Order frequency limited to two per minute')
            a, positions, orders, clock = self.preflight()
            if clock.get('is_open') is not True:
                raise SafetyError('Regular market session is closed')
            asset = self._get('/v2/assets/' + intent.symbol)
            if asset.get('class') != 'us_equity' or asset.get('status') != 'active' or asset.get('tradable') is not True:
                raise SafetyError('Asset is not tradable US equity')
            day = datetime.fromisoformat(clock['timestamp'].replace('Z', '+00:00')).astimezone(ZoneInfo('America/New_York')).date().isoformat()
            loss = money(a['last_equity']) - money(a['equity'])
            if loss >= 1000:
                c.execute('INSERT OR REPLACE INTO loss_days VALUES(?,1)', (day,))
                c.commit()
                raise SafetyError('Daily loss limit reached')
            if c.execute('SELECT 1 FROM loss_days WHERE day=?', (day,)).fetchone():
                raise SafetyError('Daily loss stop is latched')
            exposure = {}
            held = {}
            for p in positions:
                exposure[p['symbol']] = exposure.get(p['symbol'], Decimal(0)) + money(p['market_value'])
                held[p['symbol']] = held.get(p['symbol'], Decimal(0)) + money(p['qty'])
            pending_buy = Decimal(0)
            pending_sell = {}
            seen = set()
            for order in orders:
                oid = order.get('client_order_id')
                local = next((r for r in rows if r['client_id'] == oid), None)
                if not local or oid in seen or order.get('status') not in ACTIVE:
                    raise SafetyError('Unmanaged or inconsistent open order')
                self._record(c, local, order)
                seen.add(oid)
                # Full original quantity deliberately double-counts partial fills.
                # Positions and orders are not an atomic broker snapshot.
                remaining = money(order['qty'])
                if order['side'] == 'buy':
                    cost = remaining * money(order['limit_price'])
                    exposure[order['symbol']] = exposure.get(order['symbol'], Decimal(0)) + cost
                    pending_buy += cost
                else:
                    pending_sell[order['symbol']] = pending_sell.get(order['symbol'], Decimal(0)) + remaining
            if any(r['state'] in ACTIVE and r['client_id'] not in seen for r in rows):
                raise SafetyError('Order snapshot mismatch; reconcile again')
            cost = intent.qty * price
            if intent.side == 'buy':
                exposure[intent.symbol] = exposure.get(intent.symbol, Decimal(0)) + cost
                if pending_buy + cost > money(a['cash']):
                    raise SafetyError('Insufficient unleveraged cash')
            elif intent.qty + pending_sell.get(intent.symbol, Decimal(0)) > held.get(intent.symbol, Decimal(0)):
                raise SafetyError('Short selling is forbidden')
            if any(v > 5000 for v in exposure.values()) or sum(exposure.values()) > 20000:
                raise SafetyError('Position or gross exposure cap exceeded')
            c.execute('INSERT INTO intents(client_id,payload,created,state) VALUES(?,?,?,?)',
                      (cid, encoded, timestamp, 'reserved'))
        # Reservation is durable before any possible transmission. A crash or
        # timeout leaves it unresolved, globally blocking another new intent.
        try:
            with self.db() as c:
                # Serialize stop against the last gate AND transmission. The
                # durable reservation above survives failure of this transaction.
                c.execute('BEGIN IMMEDIATE')
                if c.execute('SELECT stopped FROM safety').fetchone()[0]:
                    raise SafetyError('Kill switch activated before transmission')
                status, order = self.transport.call('POST', '/v2/orders', payload)
                row = c.execute('SELECT * FROM intents WHERE client_id=?', (cid,)).fetchone()
                if status == 200 or status == 201:
                    self._record(c, row, order)
                else:
                    c.execute("UPDATE intents SET state='uncertain' WHERE client_id=?", (cid,))
        except Exception:
            with self.db() as c:
                c.execute("UPDATE intents SET state='uncertain' WHERE client_id=?", (cid,))
            raise SafetyError('Submission outcome uncertain; reconcile, never retry POST') from None
        return next(r for r in self.records() if r['client_id'] == cid)

    def stop(self, *, cancel_open=False):
        """Persist stop first. Cancellation is explicit, never liquidation."""
        with self.db() as c:
            c.execute('UPDATE safety SET stopped=1')
        if not cancel_open:
            return []
        if not self.enabled:
            raise SafetyError('Cancellation transport is disabled; stop remains on')
        outcomes = []
        for row in self.reconcile():
            if row['state'] in TERMINAL:
                continue
            if not row['broker_id']:
                outcomes.append({'client_id': row['client_id'], 'outcome': 'unresolved'})
                continue
            with self.db() as c:
                cursor = c.execute('INSERT INTO cancellation(broker_id,created,outcome) VALUES(?,?,?)',
                                   (row['broker_id'], self.now().timestamp(), 'uncertain'))
                audit_id = cursor.lastrowid
            try:
                status, _ = self.transport.call('DELETE', '/v2/orders/' + row['broker_id'])
                outcome = 'requested' if status == 204 else 'uncertain'
            except Exception:
                outcome = 'uncertain'
            with self.db() as c:
                c.execute('UPDATE cancellation SET outcome=? WHERE id=?', (outcome, audit_id))
            outcomes.append({'broker_id': row['broker_id'], 'outcome': outcome})
        # Requested is not canceled: next reconciliation checks partial fills / terminal state.
        return outcomes
