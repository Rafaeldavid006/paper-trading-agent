"""Offline Decimal lifecycle. Receipts are NOT executable orders or authority.
No network, transport, credentials or integration with production.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, localcontext
import json
import re
from pathlib import Path
from alpaca_schema import (SchemaError, parse_snapshot, parse_order, reconcile_order,
                          owned_exit_quantity, OwnedIntent, fresh, label, number, TERMINAL)
import lifecycle_storage as storage

class LifecycleError(ValueError): pass

def check(ok, code):
    if not ok: raise LifecycleError(code)

@dataclass(frozen=True)
class SessionRiskPolicy:
    """Entry-only limits; callers must share this SQLite DB to share their budget."""
    max_entries: int
    max_loss: Decimal

    def __post_init__(self):
        check(type(self.max_entries) is int and 0 < self.max_entries <= 2**63-1,
              'invalid_session_risk_policy')
        check(type(self.max_loss) is Decimal and self.max_loss.is_finite() and self.max_loss > 0,
              'invalid_session_risk_policy')
        try:
            number(format(self.max_loss, 'f'))
        except SchemaError:
            raise LifecycleError('invalid_session_risk_policy') from None

    def as_dict(self):
        loss = format(self.max_loss, 'f')
        if '.' in loss: loss = loss.rstrip('0').rstrip('.')
        return {'max_entries': self.max_entries, 'max_loss': loss}

@dataclass(frozen=True)
class Proposal:
    logical_key: str
    client_order_id: str
    symbol: str
    side: str
    quantity: Decimal
    limit_price: Decimal

@dataclass(frozen=True)
class Receipt:
    logical_key: str
    client_order_id: str
    state: str = 'uncertain'
    executable: bool = False

class Lifecycle:
    def __init__(self, path, account_fingerprint, allowlist=('AAPL',), *, session_risk_policy=None):
        check(str(path) != ':memory:' and bool(str(path)), 'durable_path_required')
        check(isinstance(account_fingerprint,str) and re.fullmatch('[0-9a-f]{64}',account_fingerprint), 'invalid_binding')
        check(type(allowlist) is tuple and allowlist and len(set(allowlist))==len(allowlist), 'invalid_universe')
        check(session_risk_policy is None or type(session_risk_policy) is SessionRiskPolicy,
              'invalid_session_risk_policy')
        for s in allowlist: label(s)
        self.path=str(Path(path).absolute()); self.binding=account_fingerprint; self.allowlist=allowlist
        try:
            self._file_identity=storage.private_file(self.path, create=True)
        except storage.StorageError as exc:
            raise LifecycleError(str(exc)) from None
        self._session_policy=session_risk_policy
        with self.tx() as db:
            names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'control' in names:
                check('lifecycle_storage_binding' in names,'legacy_storage_requires_reviewed_migration')
                check({'control','intents','cancels','loss_days','session_risk_binding',
                       'risk_sessions','lifecycle_storage_binding'}<=names,'incomplete_lifecycle_storage')
                self._check_storage_binding(db)
                check(db.execute('SELECT COUNT(*) FROM control').fetchone()[0]==1 and
                      db.execute('SELECT 1 FROM control WHERE id=1').fetchone() is not None,
                      'invalid_stored_projection')
                check(db.execute('SELECT COUNT(*) FROM session_risk_binding').fetchone()[0]==1,
                      'invalid_stored_projection')
            else:
                check(not {'intents','cancels','loss_days','session_risk_binding',
                           'risk_sessions','lifecycle_storage_binding'} & names,
                      'incomplete_lifecycle_storage')
                db.execute('CREATE TABLE lifecycle_storage_binding (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL)')
                db.execute('INSERT INTO lifecycle_storage_binding VALUES(1,?)',(storage.VERSION,))
            db.execute('CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY CHECK(id=1), binding TEXT, universe TEXT, stopped INTEGER, attention INTEGER, snapshot TEXT, observed INTEGER)')
            db.execute('CREATE TABLE IF NOT EXISTS intents (logical_key TEXT UNIQUE NOT NULL, cid TEXT PRIMARY KEY, symbol TEXT, side TEXT, quantity TEXT, price TEXT, claimed INTEGER, response TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS cancels (cid TEXT PRIMARY KEY, claimed INTEGER NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS loss_days (day INTEGER PRIMARY KEY)')
            created=db.execute('INSERT OR IGNORE INTO control VALUES(1,?,?,0,0,NULL,NULL)',(self.binding,json.dumps(sorted(allowlist)))).rowcount
            c=db.execute('SELECT binding,universe FROM control WHERE id=1').fetchone()
            check(tuple(c)==(self.binding,json.dumps(sorted(allowlist))), 'ledger_binding_mismatch')
            db.execute('CREATE TABLE IF NOT EXISTS session_risk_binding (id INTEGER PRIMARY KEY CHECK(id=1), policy TEXT, last_clock_ns INTEGER)')
            db.execute('CREATE TABLE IF NOT EXISTS risk_sessions (session_close_ns INTEGER PRIMARY KEY, baseline_equity TEXT NOT NULL, loss_latched INTEGER NOT NULL CHECK(loss_latched IN (0,1)), entry_claims INTEGER NOT NULL CHECK(entry_claims>=0), first_observed_ns INTEGER NOT NULL, last_observed_ns INTEGER NOT NULL)')
            # Legacy ledgers cannot gain a fresh budget by enabling this policy.
            db.execute('INSERT OR IGNORE INTO session_risk_binding VALUES(1,?,NULL)',
                       (self._encoded_policy() if created else None,))
            self._check_policy_binding(db)
            self._check_stored_state(db)

    def _check_storage_binding(self, db):
        rows=db.execute('SELECT id,version FROM lifecycle_storage_binding').fetchall()
        check(len(rows)==1 and tuple(rows[0])==(1,storage.VERSION),'invalid_storage_binding')

    def _check_stored_state(self, db):
        self._check_storage_binding(db)
        row=db.execute('SELECT * FROM control WHERE id=1').fetchone()
        check(row is not None,'invalid_stored_projection')
        check((row['binding'],row['universe'])==(self.binding,json.dumps(sorted(self.allowlist))),
              'ledger_binding_mismatch')
        check(type(row['stopped']) is int and row['stopped'] in (0,1) and
              type(row['attention']) is int and row['attention'] in (0,1),
              'invalid_stored_projection')
        if row['snapshot'] is not None:
            snapshot=storage.decode_snapshot(storage.loads(row['snapshot']),binding=self.binding)
            check(snapshot.observed_ns==row['observed'],'invalid_stored_projection')
        else:
            check(row['observed'] is None,'invalid_stored_projection')
        for row in db.execute('SELECT response FROM intents WHERE response IS NOT NULL'):
            storage.decode_order(storage.loads(row['response']))

    @property
    def session_policy(self):
        return self._session_policy

    def _encoded_policy(self):
        return None if self.session_policy is None else json.dumps(self.session_policy.as_dict(),sort_keys=True,separators=(',',':'))

    def _check_policy_binding(self, db):
        row=db.execute('SELECT policy FROM session_risk_binding WHERE id=1').fetchone()
        check(row is not None and row['policy']==self._encoded_policy(),
              'session_risk_policy_mismatch')

    def _observe_session_risk(self, db, snapshot):
        """An open native clock's next close is a conservative session key, NOT
        proof of trusted calendar/session provenance. This DB does not coordinate
        the legacy one-shot app or any caller using a different lifecycle DB.
        """
        if self.session_policy is None: return
        s=snapshot
        previous_clock=db.execute('SELECT last_clock_ns FROM session_risk_binding WHERE id=1').fetchone()[0]
        check(previous_clock is None or s.clock.timestamp_ns>=previous_clock,
              'session_clock_regression')
        previous=db.execute('SELECT * FROM risk_sessions ORDER BY session_close_ns DESC LIMIT 1').fetchone()
        if s.clock.is_open:
            session=s.clock.next_close_ns
            check(previous is None or session>=previous['session_close_ns'],
                  'session_regression')
            if previous is None or session>previous['session_close_ns']:
                check(previous is None or s.clock.timestamp_ns>=previous['session_close_ns'],
                      'premature_session_change')
                baseline=s.account.last_equity
                with localcontext() as ctx:
                    ctx.prec=60
                    loss=int(baseline-s.account.equity>=self.session_policy.max_loss)
                db.execute('INSERT INTO risk_sessions VALUES(?,?,?,0,?,?)',
                           (session,str(baseline),loss,s.observed_ns,s.observed_ns))
            else:
                baseline=Decimal(previous['baseline_equity'])
                check(s.account.last_equity==baseline,'session_baseline_changed')
                with localcontext() as ctx:
                    ctx.prec=60
                    loss=int(baseline-s.account.equity>=self.session_policy.max_loss)
                db.execute('UPDATE risk_sessions SET loss_latched=MAX(loss_latched,?),last_observed_ns=? WHERE session_close_ns=?',
                           (loss,s.observed_ns,session))
        elif previous is not None:
            # A closed clock cannot establish a new baseline. It may only make
            # the last known session more conservative until the next open one.
            with localcontext() as ctx:
                ctx.prec=60
                loss=int(Decimal(previous['baseline_equity'])-s.account.equity>=self.session_policy.max_loss)
            db.execute('UPDATE risk_sessions SET loss_latched=MAX(loss_latched,?),last_observed_ns=? WHERE session_close_ns=?',
                       (loss,s.observed_ns,previous['session_close_ns']))
        # Closed observations never create, reset, or move a session budget.
        db.execute('UPDATE session_risk_binding SET last_clock_ns=? WHERE id=1',
                   (s.clock.timestamp_ns,))

    @contextmanager
    def tx(self):
        try:
            with storage.transaction(self.path, identity=self._file_identity) as db:
                yield db
        except storage.StorageError as exc:
            raise LifecycleError(str(exc)) from None

    def stop(self):
        with self.tx() as db: db.execute('UPDATE control SET stopped=1 WHERE id=1')

    def status(self):
        with self.tx() as db:
            self._check_policy_binding(db)
            self._check_stored_state(db)
            c=db.execute('SELECT stopped,attention,observed FROM control').fetchone()
            return dict(c) | {
                'intents':[dict(r) for r in db.execute('SELECT logical_key,cid,symbol,side,quantity,price,claimed,response FROM intents ORDER BY claimed,cid')],
                'risk_policy':None if self.session_policy is None else self.session_policy.as_dict(),
                'risk_sessions':[dict(r) for r in db.execute('SELECT * FROM risk_sessions ORDER BY session_close_ns')],
            }

    def public_status(self):
        """Allowlisted operational summary, not account or reconciliation data.

        status() is a PRIVATE compatibility API. Never publish it verbatim.
        """
        state=self.status()
        return {
            'stopped':bool(state['stopped']), 'attention':bool(state['attention']),
            'observed':state['observed'], 'intent_count':len(state['intents']),
            'intents_without_response':sum(r['response'] is None for r in state['intents']),
            'risk_policy':state['risk_policy'],
            'risk_sessions':[{k:r[k] for k in ('session_close_ns','loss_latched','entry_claims',
                                                'first_observed_ns','last_observed_ns')}
                             for r in state['risk_sessions']],
        }

    def _parse(self, raw, now_ns):
        """Native input only: a private projection cannot prove account identity."""
        return parse_snapshot(**{k:v for k,v in raw.items() if k not in ('quotes','quote_symbols','feed')},expected_fingerprint=self.binding,now_ns=now_ns)

    def _load_snapshot(self, encoded, now_ns):
        """Private stored-state decoder; never reconstruct a native account ID."""
        try:
            return storage.decode_snapshot(storage.loads(encoded),binding=self.binding,now_ns=now_ns)
        except storage.StorageError as exc:
            raise LifecycleError(str(exc)) from None

    def _stored_quotes(self, encoded, now_ns):
        """Private quotes with their original timestamps and claim-time freshness."""
        try:
            raw=storage.loads(encoded)
            storage.decode_snapshot(raw,binding=self.binding,now_ns=now_ns)
            return storage.decode_quotes(raw['quotes'],now_ns=now_ns)
        except storage.StorageError as exc:
            raise LifecycleError(str(exc)) from None

    def record_response(self, client_order_id, raw_order, *, now_ns):
        """Persist one independently received order response, not an observation.

        The caller must authenticate/bind the transport. This cannot establish
        account state, refresh a snapshot, advance a risk session or authorize a
        retry. Reconciliation still requires a subsequent complete observation.
        Invalid evidence latches attention without replacing a good response.
        """
        error=None
        parsed=None
        with self.tx() as db:
            try:
                self._check_policy_binding(db)
                self._check_stored_state(db)
                label(client_order_id)
                check(type(now_ns) is int,'invalid_response_clock')
                row=db.execute('SELECT * FROM intents WHERE cid=?',(client_order_id,)).fetchone()
                check(row is not None,'response_not_owned')
                projected=storage.project_order(raw_order)
                parsed=storage.decode_order(projected)
                check(parsed.client_order_id==client_order_id,'intent_order_mismatch')
                check((parsed.symbol,parsed.side,parsed.quantity,parsed.limit_price)==
                      (row['symbol'],row['side'],Decimal(row['quantity']),Decimal(row['price'])),
                      'intent_order_mismatch')
                check(parsed.submitted_ns>=row['claimed'],'order_predates_claim')
                check(parsed.updated_ns<=now_ns,'future_order')
                if row['response'] is not None:
                    reconcile_order(storage.decode_order(storage.loads(row['response'])),parsed)
                db.execute('UPDATE intents SET response=? WHERE cid=?',
                           (storage.dumps(projected),client_order_id))
            except (SchemaError,LifecycleError,storage.StorageError,TypeError,ValueError,KeyError) as exc:
                db.execute('UPDATE control SET attention=1')
                error=str(exc) if isinstance(exc,(SchemaError,LifecycleError,storage.StorageError)) else 'invalid_order_response'
        if error: raise LifecycleError(error)
        return parsed

    def observe(self, account, clock, positions, orders, *, origin, response_account_fingerprint, observed_ns, now_ns, collection_complete=False, quotes=None, quote_symbols=(), feed='iex'):
        raw=dict(account=account,clock=clock,positions=positions,orders=orders,origin=origin,
                 response_account_fingerprint=response_account_fingerprint,observed_ns=observed_ns,collection_complete=collection_complete,quotes=quotes,quote_symbols=quote_symbols,feed=feed)
        error=None
        with self.tx() as db:
            try:
                self._check_policy_binding(db)
                self._check_stored_state(db)
                s=self._parse(raw,now_ns)
                previous=db.execute('SELECT observed,snapshot FROM control').fetchone()
                check(previous['observed'] is None or observed_ns>=previous['observed'],'snapshot_regression')
                projection=storage.project_snapshot(s,raw)
                encoded=storage.dumps(projection)
                check(previous['observed']!=observed_ns or previous['snapshot']==encoded,'conflicting_snapshot')
                registered={r['cid']:r for r in db.execute('SELECT * FROM intents')}
                by_raw={o['client_order_id']:o for o in projection['orders']}
                for o in s.orders:
                    if o.client_order_id not in registered: continue
                    r=registered[o.client_order_id]
                    check((o.symbol,o.side,o.quantity,o.limit_price)==(r['symbol'],r['side'],Decimal(r['quantity']),Decimal(r['price'])),'intent_order_mismatch')
                    check(o.submitted_ns>=r['claimed'],'order_predates_claim')
                    if r['response'] is not None: reconcile_order(storage.decode_order(storage.loads(r['response'])),o)
                self._observe_session_risk(db,s)
                # Validate the entire observation before storing any responses.
                for cid in registered.keys() & by_raw.keys():
                    db.execute('UPDATE intents SET response=? WHERE cid=?',(storage.dumps(by_raw[cid]),cid))
                db.execute('UPDATE control SET snapshot=?,observed=?',(encoded,observed_ns))
                # UTC day is conservative daily latch; caller must not interpret as exchange-day accounting.
                if self.session_policy is None and s.account.last_equity-s.account.equity>=Decimal('1000'):
                    db.execute('INSERT OR IGNORE INTO loss_days VALUES(?)',(observed_ns//86400000000000,))
            except (SchemaError,LifecycleError,storage.StorageError,TypeError,ValueError,KeyError) as exc:
                db.execute('UPDATE control SET attention=1')
                error=str(exc) if isinstance(exc,(SchemaError,LifecycleError,storage.StorageError)) else 'invalid_observation'
        if error: raise LifecycleError(error)

    def claim(self, proposal, *, now_ns):
        check(type(proposal) is Proposal,'invalid_proposal')
        p=proposal
        for x in (p.logical_key,p.client_order_id,p.symbol): label(x)
        check(p.side in ('buy','sell') and p.symbol in self.allowlist,'unsupported_proposal')
        for value in (p.quantity,p.limit_price):
            check(type(value) is Decimal and value.is_finite() and value>0,'invalid_decimal')
        number(format(p.quantity,'f'),quantity=True); number(format(p.limit_price,'f'))
        tick=Decimal('0.01') if p.limit_price>=1 else Decimal('0.0001')
        check(p.limit_price % tick == 0,'unsupported_price_tick')
        with self.tx() as db, localcontext() as ctx:
            ctx.prec=60
            self._check_policy_binding(db)
            c=db.execute('SELECT * FROM control').fetchone()
            check(not c['stopped'] and not c['attention'],'stopped_or_attention')
            check(c['snapshot'] is not None,'snapshot_missing')
            self._check_stored_state(db)
            raw=storage.loads(c['snapshot'])
            s=storage.decode_snapshot(raw,binding=self.binding,now_ns=now_ns)
            check(s.clock.is_open and now_ns < s.clock.next_close_ns,'market_closed')
            qs=storage.decode_quotes(raw['quotes'],now_ns=now_ns)
            quote=next((q for q in qs if q.symbol==p.symbol),None)
            check(quote is not None,'quote_missing')
            check(quote.bid<=p.limit_price<=quote.ask*Decimal('1.001') if p.side=='buy' else quote.bid*Decimal('0.999')<=p.limit_price<=quote.ask,'limit_not_near_quote')
            check(not any(o.client_order_id==p.client_order_id for o in s.orders),'provider_client_id_collision')
            check(db.execute('SELECT 1 FROM intents WHERE cid=? OR logical_key=?',(p.client_order_id,p.logical_key)).fetchone() is None,'already_claimed')
            rows=list(db.execute('SELECT * FROM intents'))
            orders={o.client_order_id:o for o in s.orders}
            check(all(r['cid'] in orders for r in rows),'unresolved_claim')
            # No other working order may coexist; pending_cancel reserves until terminal.
            check(all(o.status in TERMINAL for o in s.orders),'working_order_reserved')
            if p.side=='buy':
                if self.session_policy is None:
                    check(not db.execute('SELECT 1 FROM loss_days WHERE day=?',(s.observed_ns//86400000000000,)).fetchone(),'daily_entry_gate')
                else:
                    risk=db.execute('SELECT * FROM risk_sessions ORDER BY session_close_ns DESC LIMIT 1').fetchone()
                    check(risk is not None and risk['session_close_ns']==s.clock.next_close_ns,
                          'session_risk_missing_or_mismatch')
                    check(not risk['loss_latched'],'session_loss_entry_gate')
                    check(risk['entry_claims']<self.session_policy.max_entries,'session_entry_quota')
                check(not any(pos.symbol==p.symbol for pos in s.positions),'position_already_exists')
                check(not any(o.symbol==p.symbol and o.client_order_id not in {r['cid'] for r in rows} for o in s.orders),'external_symbol_order')
                cost=p.quantity*p.limit_price
                check(cost<=Decimal('5000') and cost<=s.account.cash,'entry_size_or_cash')
                check(sum((pos.market_value for pos in s.positions),Decimal(0))+cost<=Decimal('20000'),'gross_limit')
            else:
                intents=tuple(OwnedIntent(self.binding,r['cid'],r['symbol'],r['side'],Decimal(r['quantity'])) for r in rows)
                remaining=owned_exit_quantity(s,intents=intents,symbol=p.symbol,now_ns=now_ns)
                check(p.quantity==remaining,'exact_owned_remainder_required')
            db.execute('INSERT INTO intents VALUES(?,?,?,?,?,?,?,NULL)',(p.logical_key,p.client_order_id,p.symbol,p.side,str(p.quantity),str(p.limit_price),now_ns))
            if p.side=='buy' and self.session_policy is not None:
                # Reservation and budget use commit together, including claims
                # that remain uncertain or are later rejected by the provider.
                changed=db.execute('UPDATE risk_sessions SET entry_claims=entry_claims+1 WHERE session_close_ns=? AND loss_latched=0 AND entry_claims<?',
                                   (s.clock.next_close_ns,self.session_policy.max_entries)).rowcount
                check(changed==1,'session_entry_quota')
        return Receipt(p.logical_key,p.client_order_id)

    def claim_cancel(self, client_order_id, *, now_ns):
        """Durable non-executable cancellation intention; never presume cancellation."""
        label(client_order_id)
        with self.tx() as db:
            self._check_policy_binding(db)
            c=db.execute('SELECT * FROM control').fetchone()
            check(not c['stopped'] and not c['attention'],'stopped_or_attention')
            check(c['snapshot'] is not None,'snapshot_missing')
            self._check_stored_state(db)
            raw=storage.loads(c['snapshot'])
            s=storage.decode_snapshot(raw,binding=self.binding,now_ns=now_ns)
            row=db.execute('SELECT * FROM intents WHERE cid=?',(client_order_id,)).fetchone()
            check(row is not None,'cancel_not_owned')
            order=next((o for o in s.orders if o.client_order_id==client_order_id),None)
            check(order is not None,'cancel_order_unresolved')
            check(order.status not in TERMINAL and order.status!='pending_cancel','cancel_not_needed_or_pending')
            check(db.execute('SELECT 1 FROM cancels WHERE cid=?',(client_order_id,)).fetchone() is None,'cancel_already_claimed')
            db.execute('INSERT INTO cancels VALUES(?,?)',(client_order_id,now_ns))
        return Receipt('cancel:'+client_order_id,client_order_id)
