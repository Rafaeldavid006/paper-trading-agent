"""OFFLINE research simulator. No production broker or application wiring.

Input attestations must come from a future trusted ingestion boundary. They are
not proof of authenticity. Parameters are experimental and not activation approval.
"""
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from contextlib import contextmanager
import hashlib
import json
import sqlite3
import re
import math
from urllib.parse import urlsplit
from paper_adapter import Intent, SafetyError, money

VERSION = 'direct-earnings-continuation-v1'
UNIVERSE = ('AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'MU')
TERMINAL = {'filled', 'canceled', 'expired', 'rejected'}

@dataclass(frozen=True)
class Config:
    enabled: bool = False  # Enables fake simulation ONLY, never a broker transport.
    allowlist: tuple = UNIVERSE
    strategy_version: str = VERSION

@dataclass(frozen=True)
class Event:
    event_id: str
    episode_id: str
    version: str
    symbol: str
    published_at: datetime
    observed_at: datetime
    source_url: str
    source_hash: str
    verified: bool = False
    corrected: bool = False
    timestamp_precision: str = 'second'
    event_type: str = 'earnings'

@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: str
    ask: str
    asof: datetime
    received_at: datetime | None = None

@dataclass(frozen=True)
class Bar:
    symbol: str
    start: datetime
    end: datetime
    close: str
    available_at: datetime


def stamp(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise SafetyError('Timezone-aware timestamp required')
    return value.timestamp()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=lambda x: x.isoformat() if isinstance(x, datetime) else str(x))


class FakeBroker:
    """In-memory test double, deliberately no network methods or credentials."""
    def __init__(self, now):
        self.now = now
        self.session_open = now.replace(hour=13, minute=30, second=0, microsecond=0)
        self.session_close = now.replace(hour=20, minute=0, second=0, microsecond=0)
        self.market_open = True
        self.cash = '100000'
        self.equity = self.day_start_equity = '100000'
        self.positions = {}
        self.orders = {}
        self.calls = []
        self.timeout = False
        self.hidden = False

    def submit(self, intent):
        self.calls.append(intent.key)
        if intent.key in self.orders:
            raise SafetyError('Duplicate fake POST')
        self.orders[intent.key] = dict(asdict(intent), state='new', filled=0, average_price=None, first_fill_at=None)
        if self.timeout:
            raise TimeoutError('Synthetic uncertain submission')
        return dict(self.orders[intent.key])

    def get(self, key):
        return None if self.hidden else self.orders.get(key)

    def cancel(self, key):
        self.calls.append('cancel:'+key)
        if self.orders[key]['state'] not in TERMINAL:
            self.orders[key]['state'] = 'canceled'
        if self.timeout:
            raise TimeoutError('Synthetic uncertain cancellation')

    def fill(self, key, qty=None, price=None, state='filled'):
        order = self.orders[key]
        qty = order['qty'] if qty is None else qty
        delta = qty - order['filled']
        price = price or order['limit_price']
        if delta < 0 or qty > order['qty']:
            raise SafetyError('Invalid synthetic fill')
        order.update(filled=qty, average_price=price, state=state)
        if qty and not order['first_fill_at']:
            order['first_fill_at'] = stamp(self.now)
        signed = delta if order['side'] == 'buy' else -delta
        self.positions[order['symbol']] = self.positions.get(order['symbol'], 0) + signed
        self.cash = str(money(self.cash) - Decimal(signed) * money(price))


class DemoPipeline:
    def __init__(self, database, config=Config(), broker=None):
        if str(database) == ':memory:':
            raise SafetyError('Durable simulation ledger required')
        if broker is not None and type(broker) is not FakeBroker:
            raise SafetyError('Only the built-in offline FakeBroker is supported')
        if config.strategy_version != VERSION or not set(config.allowlist).issubset(UNIVERSE):
            raise SafetyError('Unsupported experimental configuration')
        self.database, self.config, self.broker = str(database), config, broker
        self.binding = hashlib.sha256(encode(asdict(config)).encode()).hexdigest()
        with self.db() as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS demo_control(id INTEGER PRIMARY KEY CHECK(id=1), stopped INTEGER NOT NULL);
            INSERT OR IGNORE INTO demo_control VALUES(1,0);
            CREATE TABLE IF NOT EXISTS demo_decisions(id TEXT PRIMARY KEY, episode TEXT, symbol TEXT, created REAL, expires REAL, binding TEXT, action TEXT, reason TEXT, payload TEXT);
            CREATE TABLE IF NOT EXISTS demo_intents(key TEXT PRIMARY KEY, decision_id TEXT, side TEXT, symbol TEXT, qty INTEGER, limit_price TEXT, created REAL, state TEXT, filled INTEGER DEFAULT 0, average_price TEXT, first_fill_at REAL);
            CREATE UNIQUE INDEX IF NOT EXISTS demo_one_side ON demo_intents(decision_id,side);
            CREATE TABLE IF NOT EXISTS demo_loss(day TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS demo_cancels(key TEXT PRIMARY KEY, created REAL, outcome TEXT);
            CREATE TABLE IF NOT EXISTS demo_attention(key TEXT PRIMARY KEY, reason TEXT);
            ''')

    @contextmanager
    def db(self):
        c = sqlite3.connect(self.database, timeout=10)
        c.row_factory = sqlite3.Row
        try:
            with c:
                yield c
        finally:
            c.close()

    def stop(self):
        # Explicit global stop blocks all fake writes, including exits; not liquidation.
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            c.execute('UPDATE demo_control SET stopped=1')

    def records(self):
        with self.db() as c:
            return [dict(r) for r in c.execute('SELECT * FROM demo_intents ORDER BY created,key')]

    def _quote(self, quote, symbol, now):
        if quote.received_at is None or not stamp(quote.asof) <= stamp(quote.received_at) <= stamp(now):
            raise SafetyError('unknown_quote_availability')
        age = stamp(now) - stamp(quote.asof)
        bid, ask = money(quote.bid), money(quote.ask)
        if quote.symbol != symbol or not 0 <= age <= 10 or bid <= 0 or ask < bid:
            raise SafetyError('invalid_or_stale_quote')
        if (ask-bid)/((ask+bid)/2) > Decimal('.002'):
            raise SafetyError('wide_spread')
        return bid, ask

    def _bars(self, bars, symbol, anchor, endpoint, now):
        count = int((endpoint-anchor).total_seconds()//60)+1
        if len(bars) != count:
            raise SafetyError('incomplete_bars')
        for i, b in enumerate(bars):
            expected_end = anchor + timedelta(minutes=i)
            if b.symbol != symbol or b.end != expected_end or b.start != expected_end-timedelta(minutes=1):
                raise SafetyError('bar_interval_mismatch')
            if not stamp(b.end) <= stamp(b.available_at) <= stamp(now) or money(b.close) <= 0:
                raise SafetyError('bar_not_available')
        return money(bars[-1].close)/money(bars[0].close)-1

    def evaluate(self, event, quote, stock_bars, benchmark_bars, now):
        """Persist one immutable decision per event-version/symbol, including abstention."""
        t = stamp(now)
        identity = [self.config.strategy_version, event.event_id, event.version, event.symbol]
        did = hashlib.sha256(encode(identity).encode()).hexdigest()
        payload = dict(event=asdict(event), quote=asdict(quote), stock_bars=[asdict(b) for b in stock_bars], benchmark_bars=[asdict(b) for b in benchmark_bars])
        action, reason = 'abstain', 'disabled'
        limit = None
        try:
            if not self.config.enabled:
                raise SafetyError('disabled')
            if not event.event_id or not event.episode_id or not event.version or event.symbol not in self.config.allowlist:
                raise SafetyError('invalid_event_identity')
            if event.verified is not True or event.corrected is not False or event.event_type != 'earnings' or event.timestamp_precision != 'second':
                raise SafetyError('unverified_or_ineligible_event')
            u = urlsplit(event.source_url)
            if u.scheme != 'https' or not u.hostname or u.username or u.password or not re.fullmatch('[a-f0-9]{64}', event.source_hash):
                raise SafetyError('invalid_provenance')
            published, observed = stamp(event.published_at), stamp(event.observed_at)
            if not published <= observed <= t or observed-published > 120:
                raise SafetyError('late_or_future_event')
            anchor = event.published_at.replace(second=0, microsecond=0)
            end = anchor+timedelta(minutes=5)
            if event.published_at != anchor:
                end += timedelta(minutes=1)
            if not stamp(end) <= t <= stamp(end)+60:
                raise SafetyError('decision_window')
            stock = self._bars(stock_bars, event.symbol, anchor, end, now)
            benchmark = self._bars(benchmark_bars, 'SPY', anchor, end, now)
            if not Decimal('.003') <= stock <= Decimal('.02') or stock-benchmark < Decimal('.002'):
                raise SafetyError('signal_threshold')
            _, ask = self._quote(quote, event.symbol, now)
            limit = ask.quantize(Decimal('.01'), rounding=ROUND_CEILING)
            if not Decimal('1') <= limit <= 1000:
                raise SafetyError('price_limit')
            action, reason = 'enter', 'eligible_experimental_signal'
        except (SafetyError, ValueError, TypeError):
            # Stable public reason, never persist exception/provider text.
            import sys
            exc = sys.exc_info()[1]
            reason = str(exc) if isinstance(exc, SafetyError) else 'malformed_input'
        payload['limit_price'] = None if limit is None else str(limit)
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            prior = c.execute('SELECT * FROM demo_decisions WHERE id=?', (did,)).fetchone()
            if prior:
                # Full immutable input prevents changed facts under same event version.
                if json.loads(prior['payload']) != json.loads(encode(payload)):
                    raise SafetyError('event_version_conflict')
                return dict(prior)
            c.execute('INSERT INTO demo_decisions VALUES(?,?,?,?,?,?,?,?,?)', (did,event.episode_id,event.symbol,t,t+60,self.binding,action,reason,encode(payload)))
            return dict(c.execute('SELECT * FROM demo_decisions WHERE id=?',(did,)).fetchone())

    def _operational(self, c, now):
        if self.config.enabled is not True or type(self.broker) is not FakeBroker:
            raise SafetyError('simulation_disabled')
        if c.execute('SELECT stopped FROM demo_control').fetchone()[0]:
            raise SafetyError('global_stop')
        if not 0 <= stamp(now)-stamp(self.broker.now) <= 10 or self.broker.market_open is not True:
            raise SafetyError('stale_or_closed_broker')
        if not stamp(self.broker.session_open) <= stamp(now) < stamp(self.broker.session_close):
            raise SafetyError('outside_regular_session')
        if c.execute('SELECT 1 FROM demo_attention').fetchone():
            raise SafetyError('manual_attention_latched')
        rows = c.execute('SELECT * FROM demo_intents').fetchall()
        if any(r['state'] not in TERMINAL for r in rows):
            raise SafetyError('unresolved_intent')
        if any(o['state'] not in TERMINAL for o in self.broker.orders.values()):
            raise SafetyError('unmanaged_order')

    def _bad_fill(self, c, row, reason):
        c.execute('INSERT OR REPLACE INTO demo_attention VALUES(?,?)',(row['key'],reason))
        c.commit()
        raise SafetyError(reason)

    def reconcile(self):
        if type(self.broker) is not FakeBroker:
            return self.records()
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            for row in c.execute('SELECT * FROM demo_intents').fetchall():
                order = self.broker.get(row['key'])
                if order is None:
                    continue  # Missing is uncertain, never permission to resubmit.
                if not isinstance(order,dict) or not {'key','side','symbol','qty','limit_price','state','filled','average_price','first_fill_at'}.issubset(order):
                    self._bad_fill(c,row,'malformed_broker_order')
                if any(order[k] != row[k] for k in ('key','side','symbol','qty','limit_price')) or order['state'] not in TERMINAL | {'new','partially_filled'}:
                    self._bad_fill(c,row,'broker_intent_mismatch')
                filled = order['filled']
                if type(filled) is not int or not row['filled'] <= filled <= row['qty']:
                    self._bad_fill(c,row,'invalid_fill')
                if row['state'] in TERMINAL and order['state'] != row['state']:
                    self._bad_fill(c,row,'terminal_state_regression')
                if order['state']=='filled' and filled != row['qty']:
                    self._bad_fill(c,row,'invalid_terminal_fill')
                try:
                    avg = money(order['average_price']) if filled else Decimal(0)
                except SafetyError:
                    self._bad_fill(c,row,'invalid_fill_price')
                if filled and (avg <= 0 or not isinstance(order['first_fill_at'],(int,float))):
                    self._bad_fill(c,row,'missing_fill_evidence')
                if filled:
                    ft = order['first_fill_at']
                    if isinstance(ft,bool) or not math.isfinite(ft) or not row['created'] <= ft <= stamp(self.broker.now):
                        self._bad_fill(c,row,'invalid_fill_time')
                    avg, limit = money(order['average_price']), money(row['limit_price'])
                    if (row['side']=='buy' and avg>limit) or (row['side']=='sell' and avg<limit):
                        self._bad_fill(c,row,'fill_violates_limit')
                    if row['first_fill_at'] is not None and ft != row['first_fill_at']:
                        self._bad_fill(c,row,'first_fill_changed')
                    if row['state'] in TERMINAL and (filled != row['filled'] or avg != money(row['average_price'])):
                        self._bad_fill(c,row,'terminal_fill_changed')
                c.execute('UPDATE demo_intents SET state=?,filled=?,average_price=?,first_fill_at=? WHERE key=?', (order['state'],filled,order['average_price'],order['first_fill_at'],row['key']))
                if row['side']=='sell' and ((order['state'] in TERMINAL and filled < row['qty']) or (order['state'] not in TERMINAL and stamp(self.broker.now)-row['created']>=60)):
                    c.execute('INSERT OR REPLACE INTO demo_attention VALUES(?,?)',(row['key'],'exit_incomplete'))
        return self.records()

    def expire_entries(self, now):
        """Fake-only one-shot cancellation at 60s. Never retry uncertain cancel."""
        self.reconcile()
        with self.db() as c:
            candidates = [dict(r) for r in c.execute("SELECT * FROM demo_intents WHERE side='buy' AND state NOT IN ('filled','canceled','expired','rejected')")]
        for row in candidates:
            if stamp(now)-row['created'] < 60:
                continue
            with self.db() as c:
                c.execute('BEGIN IMMEDIATE')
                if not self.config.enabled or type(self.broker) is not FakeBroker or c.execute('SELECT stopped FROM demo_control').fetchone()[0]:
                    c.execute('INSERT OR REPLACE INTO demo_attention VALUES(?,?)',(row['key'],'entry_expired_stop_or_disabled'))
                    continue
                if row['state'] in ('reserved','uncertain'):
                    c.execute('INSERT OR REPLACE INTO demo_attention VALUES(?,?)',(row['key'],'entry_expired_uncertain'))
                    continue
                if c.execute('SELECT 1 FROM demo_cancels WHERE key=?',(row['key'],)).fetchone():
                    continue
                c.execute('INSERT INTO demo_cancels VALUES(?,?,?)',(row['key'],stamp(now),'reserved'))
            try:
                with self.db() as c:
                    c.execute('BEGIN IMMEDIATE')
                    if c.execute('SELECT stopped FROM demo_control').fetchone()[0]:
                        raise SafetyError('global_stop')
                    self.broker.cancel(row['key'])
                    c.execute("UPDATE demo_cancels SET outcome='requested' WHERE key=?",(row['key'],))
            except Exception:
                with self.db() as c:
                    c.execute("UPDATE demo_cancels SET outcome='uncertain' WHERE key=?",(row['key'],))
                    c.execute('INSERT OR REPLACE INTO demo_attention VALUES(?,?)',(row['key'],'cancel_uncertain'))
        return self.reconcile()

    def status(self, decision_id):
        with self.db() as c:
            rows = [dict(r) for r in c.execute('SELECT * FROM demo_intents WHERE decision_id=?',(decision_id,))]
            d = c.execute('SELECT * FROM demo_decisions WHERE id=?',(decision_id,)).fetchone()
            if not d:
                raise SafetyError('unknown_decision')
            attention=[dict(r) for r in c.execute('SELECT * FROM demo_attention') if r['key'] in {x['key'] for x in rows}]
        if attention or any(r['state'] in ('reserved','uncertain') for r in rows):
            phase='manual_attention'
        elif not rows:
            phase='abstain' if d['action']=='abstain' else 'decision_only'
        elif any(r['side']=='sell' and r['state']=='filled' for r in rows):
            phase='completed'
        elif rows[0]['state'] in TERMINAL and rows[0]['filled']==0:
            phase='no_fill'
        else:
            phase='pending'
        return dict(phase=phase, intents=rows, attention=attention, mode='offline_fake_only')

    def submit(self, decision_id, now):
        return self._submit(decision_id, now)

    def exit(self, decision_id, quote, now, reason='automatic_exit'):
        return self._submit(decision_id, now, quote=quote)

    def _submit(self, decision_id, now, quote=None):
        side = 'buy' if quote is None else 'sell'
        key = 'sim-'+hashlib.sha256((decision_id+side).encode()).hexdigest()[:40]
        self.reconcile()
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            prior = c.execute('SELECT * FROM demo_intents WHERE key=?',(key,)).fetchone()
            if prior:
                return dict(prior)
            self._operational(c,now)
            d = c.execute('SELECT * FROM demo_decisions WHERE id=?',(decision_id,)).fetchone()
            if not d or d['binding'] != self.binding or d['action'] != 'enter':
                raise SafetyError('decision_not_eligible')
            if side == 'buy':
                related = c.execute('SELECT payload FROM demo_decisions WHERE episode=? AND symbol=?',(d['episode'],d['symbol'])).fetchall()
                if any(json.loads(r['payload'])['event']['corrected'] for r in related):
                    raise SafetyError('episode_corrected')
            rows = c.execute('SELECT * FROM demo_intents').fetchall()
            qty = 1
            if side == 'buy':
                if not d['created'] <= stamp(now) <= d['expires']:
                    raise SafetyError('expired_decision')
                # Recheck original quote at submission; never submit a stale frozen quote.
                q = json.loads(d['payload'])['quote']
                self._quote(Quote(q['symbol'],q['bid'],q['ask'],datetime.fromisoformat(q['asof']),datetime.fromisoformat(q['received_at'])),d['symbol'],now)
                if stamp(now)-stamp(self.broker.session_open)<300 or stamp(self.broker.session_close)-stamp(now)<4500:
                    raise SafetyError('entry_session_window')
                day = now.astimezone(timezone.utc).date().isoformat()
                if money(self.broker.day_start_equity)-money(self.broker.equity)>=100:
                    c.execute('INSERT OR IGNORE INTO demo_loss VALUES(?)',(day,)); c.commit()
                if c.execute('SELECT 1 FROM demo_loss WHERE day=?',(day,)).fetchone():
                    raise SafetyError('daily_loss_latched')
                buys=[r for r in rows if r['side']=='buy']
                episodes={r[0] for r in c.execute("SELECT d.episode FROM demo_decisions d JOIN demo_intents i ON d.id=i.decision_id WHERE i.side='buy'")}
                daily=[r for r in buys if datetime.fromtimestamp(r['created'],timezone.utc).date().isoformat()==day]
                if d['episode'] in episodes or any(r['symbol']==d['symbol'] for r in daily) or len(daily)>=4:
                    raise SafetyError('entry_dedup_or_daily_cap')
                # Only simulator-owned positions are accepted; external holdings block.
                owned={}
                for r in rows:
                    owned[r['symbol']]=owned.get(r['symbol'],0)+(r['filled'] if r['side']=='buy' else -r['filled'])
                if {k:v for k,v in owned.items() if v} != {k:v for k,v in self.broker.positions.items() if v}:
                    raise SafetyError('unmanaged_position')
                if sum(v>0 for v in owned.values())>=2 or owned.get(d['symbol'],0):
                    raise SafetyError('position_cap')
                price = json.loads(d['payload'])['limit_price']
                # Conservative $1000 reservation per owned share, not stale market values.
                if sum(owned.values())*1000+money(price)>2000 or money(price)>money(self.broker.cash):
                    raise SafetyError('exposure_or_cash_cap')
            else:
                entry=c.execute("SELECT * FROM demo_intents WHERE decision_id=? AND side='buy'",(decision_id,)).fetchone()
                if not entry or entry['state'] not in TERMINAL or entry['filled']<=0:
                    raise SafetyError('entry_not_terminal_with_fill')
                qty=entry['filled']
                if self.broker.positions.get(d['symbol'],0)!=qty:
                    raise SafetyError('exit_position_mismatch')
                bid,_=self._quote(quote,d['symbol'],now)
                change=bid/money(entry['average_price'])-1
                if not (stamp(now)-entry['first_fill_at']>=3600 or change<=Decimal('-.01') or change>=Decimal('.02') or stamp(self.broker.session_close)-stamp(now)<=600):
                    raise SafetyError('exit_not_due')
                price=str(bid.quantize(Decimal('.01'),rounding=ROUND_FLOOR))
                if money(price)<1:
                    raise SafetyError('exit_price_invalid')
            intent=Intent(key,d['symbol'],side,qty,price)
            c.execute('INSERT INTO demo_intents(key,decision_id,side,symbol,qty,limit_price,created,state) VALUES(?,?,?,?,?,?,?,?)',(key,decision_id,side,d['symbol'],qty,price,stamp(now),'reserved'))
        # Durable reservation precedes fake send. Serialize global stop with send.
        try:
            with self.db() as c:
                c.execute('BEGIN IMMEDIATE')
                if c.execute('SELECT stopped FROM demo_control').fetchone()[0]:
                    raise SafetyError('global_stop')
                self.broker.submit(intent)
        except Exception:
            with self.db() as c:
                c.execute("UPDATE demo_intents SET state='uncertain' WHERE key=?",(key,))
            return next(r for r in self.records() if r['key']==key)
        self.reconcile()
        return next(r for r in self.records() if r['key']==key)
