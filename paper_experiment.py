"""Single approved virtual AAPL roundtrip. Not a strategy or generic order API.

Durable state and stop share the private read-only account DB. Existing account
fingerprint is bound once; a missing fingerprint never becomes trading consent.
"""
import fcntl
import hashlib
import json
import math
import re
import os
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from zoneinfo import ZoneInfo
from paper_adapter import PaperAdapter, SafetyError, money, TERMINAL, ACTIVE

EXPERIMENT_ID = 'approved-paper-aapl-roundtrip-20261018'
APPROVAL_CUTOFF = datetime(2026,10,18,20,8,5,tzinfo=timezone.utc).timestamp()
APPROVAL = 'owner-approved-one-share-paper-roundtrip-20261018'
FINAL = {'completed', 'no_fill', 'blocked', 'manual_attention', 'stopped'}


class OneShotExperiment(PaperAdapter):
    def __init__(self, database, transport, quotes, *, now=None):
        # Read-only connection must have securely created this DB already.
        if not os.path.isfile(database) or os.path.islink(database):
            raise SafetyError('Existing private account state required')
        super().__init__(database, transport, allowlist=('AAPL',), now=now)
        self.quotes = quotes
        with self.db() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS paper_experiment (
                id INTEGER PRIMARY KEY CHECK(id=1), experiment TEXT NOT NULL,
                approval TEXT NOT NULL, fingerprint TEXT NOT NULL,
                phase TEXT NOT NULL DEFAULT 'waiting', reason TEXT NOT NULL DEFAULT 'waiting_clock',
                next_open TEXT, session_end REAL, updated REAL NOT NULL,
                buy_id TEXT, sell_id TEXT, cancel_buy INTEGER NOT NULL DEFAULT 0,
                cancel_sell INTEGER NOT NULL DEFAULT 0, cancel_at REAL)''')
            previous = c.execute('SELECT * FROM paper_readonly WHERE id=1').fetchone()
            if (not previous or not re.fullmatch(r'[a-f0-9]{64}',previous['fingerprint'] or '')
                    or not isinstance(previous['identity_first_seen'],(int,float))
                    or not math.isfinite(previous['identity_first_seen'])
                    or not 0<previous['identity_first_seen']<=APPROVAL_CUTOFF):
                raise SafetyError('Previously owner-matched account fingerprint required')
            c.execute('''INSERT OR IGNORE INTO paper_experiment
                (id,experiment,approval,fingerprint,updated) VALUES(1,?,?,?,?)''',
                (EXPERIMENT_ID, APPROVAL, previous['fingerprint'], self.now().timestamp()))

    def status(self):
        with self.db() as c:
            row = c.execute('SELECT * FROM paper_experiment WHERE id=1').fetchone()
            return {k: row[k] for k in ('phase','reason','next_open','updated')}

    def _state(self, c, phase, reason):
        c.execute('UPDATE paper_experiment SET phase=?,reason=?,updated=? WHERE id=1',
                  (phase, reason, self.now().timestamp()))

    def _guard(self, c):
        row = c.execute('SELECT * FROM paper_experiment WHERE id=1').fetchone()
        ro = c.execute('SELECT * FROM paper_readonly WHERE id=1').fetchone()
        if ro['stopped'] or c.execute('SELECT stopped FROM safety WHERE id=1').fetchone()[0]:
            raise SafetyError('stopped')
        if row['experiment'] != EXPERIMENT_ID or row['approval'] != APPROVAL or row['fingerprint'] != ro['fingerprint']:
            raise SafetyError('identity_changed')
        account = self._get('/v2/account')
        identity = account.get('id')
        if not isinstance(identity,str) or hashlib.sha256(identity.encode()).hexdigest() != row['fingerprint']:
            raise SafetyError('identity_changed')
        self.expected_account_id = identity  # Private in-memory only, never returned/logged.
        return row

    def _clock(self, c):
        clock = self._get('/v2/clock')
        timestamp = datetime.fromisoformat(clock['timestamp'].replace('Z','+00:00'))
        if timestamp.tzinfo is None or not 0 <= (self.now()-timestamp).total_seconds() <= 10:
            raise SafetyError('stale_clock')
        next_open = datetime.fromisoformat(clock['next_open'].replace('Z','+00:00'))
        if next_open.tzinfo is None:
            raise SafetyError('invalid_clock')
        c.execute('UPDATE paper_experiment SET next_open=? WHERE id=1', (next_open.astimezone(timezone.utc).isoformat(),))
        self._clock_at=timestamp
        self._close_at=datetime.fromisoformat(clock['next_close'].replace('Z','+00:00'))
        if self._close_at.tzinfo is None:raise SafetyError('invalid_clock')
        local = timestamp.astimezone(ZoneInfo('America/New_York'))
        regular = local.weekday()<5 and (9,30) <= (local.hour,local.minute) < (16,0)
        return clock.get('is_open') is True and regular and self.now()<self._close_at

    def _validate_snapshot(self, c, side, qty, price):
        account, positions, orders, clock = self.preflight()
        if clock.get('is_open') is not True or not self._clock(c):
            raise SafetyError('market_closed')
        day = self.now().astimezone(ZoneInfo('America/New_York')).date().isoformat()
        if money(account['last_equity'])-money(account['equity']) >= 1000:
            c.execute('INSERT OR REPLACE INTO loss_days VALUES(?,1)',(day,))
        if c.execute('SELECT 1 FROM loss_days WHERE day=?',(day,)).fetchone():
            raise SafetyError('daily_loss_stop')
        if len(positions)>500:raise SafetyError('exposure_limit')
        if orders:
            raise SafetyError('existing_orders')
        asset = self._get('/v2/assets/AAPL')
        if asset.get('class')!='us_equity' or asset.get('status')!='active' or asset.get('tradable') is not True:
            raise SafetyError('invalid_asset')
        if qty != qty.to_integral_value() and asset.get('fractionable') is not True:
            raise SafetyError('fractional_exit_unavailable')
        held = sum(money(p['qty']) for p in positions if p['symbol']=='AAPL')
        values = {}
        for p in positions:
            values[p['symbol']] = values.get(p['symbol'],Decimal(0))+money(p['market_value'])
        if side=='buy':
            if held or qty != 1 or price*qty>1000 or price*qty>money(account['cash']):
                raise SafetyError('entry_blocked')
            values['AAPL']=price*qty
        elif held != qty:
            raise SafetyError('holding_mismatch')
        if any(v>5000 for v in values.values()) or sum(values.values())>20000:
            raise SafetyError('exposure_limit')

    def _price(self, side):
        quote = self.quotes.latest_quote()
        if not 0 <= (self.now()-quote.timestamp).total_seconds() <= 10:
            raise SafetyError('stale_quote')
        reference = quote.ask if side=='buy' else quote.bid
        price = reference.quantize(Decimal('.01'), rounding=ROUND_CEILING if side=='buy' else ROUND_FLOOR)
        if price<1 or abs(price-reference)>reference*Decimal('.001'):
            raise SafetyError('price_bound')
        if side=='buy' and price>1000:
            raise SafetyError('buy_cap')
        return price

    def _reserve(self, c, side, qty):
        if not 0<qty<=1 or qty.as_tuple().exponent < -9:
            raise SafetyError('invalid_quantity')
        price = self._price(side)
        self._validate_snapshot(c,side,qty,price)
        cid = 'pa-'+hashlib.sha256((EXPERIMENT_ID+':'+side).encode()).hexdigest()[:40]
        payload = dict(client_order_id=cid,symbol='AAPL',side=side,qty=str(qty),type='limit',
                       limit_price=str(price),time_in_force='day',extended_hours=False)
        c.execute('INSERT INTO intents(client_id,payload,created,state) VALUES(?,?,?,?)',
                  (cid,json.dumps(payload,sort_keys=True),self.now().timestamp(),'reserved'))
        c.execute('UPDATE paper_experiment SET '+side+'_id=? WHERE id=1',(cid,))
        self._state(c,side+'_reserved','submission_pending')
        c.commit()  # A crash here leaves an unresolved reservation, NEVER a retry.
        c.execute('BEGIN IMMEDIATE')
        self._guard(c)
        if not self._clock(c):
            raise SafetyError('market_closed')
        self._validate_snapshot(c,side,qty,price)
        # Last external read before POST is a fresh quote; no slow snapshot follows.
        reference = self.quotes.latest_quote()
        if not 0 <= (self.now()-reference.timestamp).total_seconds() <= 10:
            raise SafetyError('stale_quote')
        if abs(price-(reference.ask if side=='buy' else reference.bid)) > price*Decimal('.001'):
            raise SafetyError('quote_moved')
        if not 0 <= (self.now()-self._clock_at).total_seconds() <= 10 or self.now()>=self._close_at:
            raise SafetyError('stale_clock')
        status, order = self.transport.call('POST','/v2/orders',payload)
        row = c.execute('SELECT * FROM intents WHERE client_id=?',(cid,)).fetchone()
        if status not in (200,201):
            self._state(c,side+'_working','submission_uncertain')
            return
        self._record(c,row,order)
        self._state(c,side+'_working','reconciling')

    def _cancel(self,c,side,row):
        flag = c.execute('SELECT cancel_'+side+' FROM paper_experiment WHERE id=1').fetchone()[0]
        if flag:
            requested=c.execute('SELECT cancel_at FROM paper_experiment WHERE id=1').fetchone()[0]
            self._state(c,'manual_attention' if self.now().timestamp()-requested>=120 else side+'_working','cancel_unconfirmed')
            return
        c.execute('UPDATE paper_experiment SET cancel_'+side+'=1,cancel_at=? WHERE id=1',(self.now().timestamp(),))
        c.commit()  # Intent durable before DELETE. Never blind retry after timeout.
        c.execute('BEGIN IMMEDIATE')
        self._guard(c)
        if not self._clock(c):
            raise SafetyError('market_closed')
        # Only broker ID validated against this experiment's immutable payload.
        status,_=self.transport.call('DELETE','/v2/orders/'+row['broker_id'])
        self._state(c,side+'_working','cancel_requested' if status in (200,202,204) else 'cancel_unconfirmed')

    def tick(self):
        # Nonblocking interprocess singleton. Stop uses the same SQLite write lock.
        descriptor=os.open(self.database+'.experiment.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:
            try: fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError: return self.status()
            with self.db() as c:
                c.execute('BEGIN IMMEDIATE')
                state=c.execute('SELECT * FROM paper_experiment WHERE id=1').fetchone()
                if state['phase'] in FINAL: return self.status()
                try:
                    self._guard(c)
                    # Reconcile reserved/working orders even outside the session.
                    for side in ('buy','sell'):
                        cid=state[side+'_id']
                        if not cid: continue
                        row=c.execute('SELECT * FROM intents WHERE client_id=?',(cid,)).fetchone()
                        if row['state'] not in TERMINAL:
                            status,order=self.transport.call('GET','/v2/orders:by_client_order_id?client_order_id='+cid)
                            if status!=200:
                                raise SafetyError('order_unresolved')
                            self._record(c,row,order)
                    opened=self._clock(c)
                    buy=c.execute('SELECT * FROM intents WHERE client_id=?',(state['buy_id'],)).fetchone()
                    sell=c.execute('SELECT * FROM intents WHERE client_id=?',(state['sell_id'],)).fetchone()
                    if sell and sell['state'] in TERMINAL:
                        if money(sell['filled'])==money(buy['filled']):
                            positions=self._get('/v2/positions')
                            orders=self._get('/v2/orders?status=open&limit=500&nested=false')
                            if not isinstance(positions,list) or not isinstance(orders,list) or orders or any(p.get('symbol')=='AAPL' for p in positions):
                                raise SafetyError('holding_mismatch')
                            self._state(c,'completed','roundtrip_filled')
                        else:self._state(c,'manual_attention','remaining_holding')
                    elif buy and buy['state'] in TERMINAL and money(buy['filled'])==0:
                        self._state(c,'no_fill','entry_not_filled')
                    elif not opened:
                        self._state(c,state['phase'],'waiting_regular_session')
                    elif sell or (buy and buy['state'] not in TERMINAL):
                        row=sell or buy; side='sell' if sell else 'buy'
                        if self.now().timestamp()-row['created']>=60:self._cancel(c,side,row)
                        else:self._state(c,side+'_working','reconciling')
                    elif buy:
                        self._reserve(c,'sell',money(buy['filled']))
                    else:
                        # One eligible regular session only. Never chase into later days.
                        if state['session_end'] is None:
                            c.execute('UPDATE paper_experiment SET session_end=? WHERE id=1',(self.now().timestamp()+900,))
                        elif self.now().timestamp()>state['session_end']:
                            raise SafetyError('entry_window_expired')
                        for side in ('buy','sell'):
                            cid='pa-'+hashlib.sha256((EXPERIMENT_ID+':'+side).encode()).hexdigest()[:40]
                            status,_=self.transport.call('GET','/v2/orders:by_client_order_id?client_order_id='+cid)
                            if status!=404:raise SafetyError('prior_experiment_or_uncertain')
                        self._reserve(c,'buy',Decimal(1))
                except Exception as exc:
                    # Never persist exception/provider text. Match only local fixed codes.
                    reason=str(exc) if type(exc) is SafetyError and str(exc) in {
                        'stopped','identity_changed','stale_clock','invalid_clock','market_closed',
                        'daily_loss_stop','existing_orders','invalid_asset','fractional_exit_unavailable',
                        'entry_blocked','holding_mismatch','exposure_limit','stale_quote','price_bound',
                        'buy_cap','invalid_quantity','quote_moved','order_unresolved','entry_window_expired','prior_experiment_or_uncertain'} else 'check_or_transport_failed'
                    has_order=c.execute('SELECT buy_id FROM paper_experiment WHERE id=1').fetchone()[0]
                    pending=c.execute("SELECT * FROM intents WHERE state NOT IN ('filled','canceled','expired','rejected') ORDER BY created DESC LIMIT 1").fetchone()
                    reconcile_pending=pending and reason in ('check_or_transport_failed','order_unresolved') and self.now().timestamp()-pending['created']<180
                    phase=('sell_working' if json.loads(pending['payload'])['side']=='sell' else 'buy_working') if reconcile_pending else ('stopped' if reason=='stopped' else ('manual_attention' if has_order else 'blocked'))
                    self._state(c,phase,reason)
            return self.status()
        finally:
            os.close(descriptor)
