"""Native-shaped fixture path through the EXISTING runner and Decimal ledger.

No broker transport, credentials, scheduler or executable order is exposed. An exact
fixture source supplies observations; claim receipts remain non-executable. This
adapter intentionally does not relax the original integer FakeBroker boundary.
"""
import copy
import hashlib
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_FLOOR

from active_demo_pipeline import DemoPipeline, SafetyError
from alpaca_schema import (TERMINAL, OwnedIntent, owned_exit_quantity,
                           parse_latest_quotes, fresh, timestamp)
from decimal_lifecycle import Lifecycle, Proposal, LifecycleError, Receipt


def nanoseconds(now):
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise SafetyError('aware_clock_required')
    return timestamp(now.astimezone(timezone.utc).isoformat())


class NativeFixtureObservations:
    """Handcrafted native-schema observations, never an authenticated provider.

    Tests replace the full observation between ticks. No clock is rewritten to
    make old data fresh and no order acknowledgement is invented after a claim.
    """
    def __init__(self, observation):
        self.replace(observation)

    def replace(self, observation):
        self._observation = copy.deepcopy(observation)

    def read(self):
        return copy.deepcopy(self._observation)


class NativeDecimalExecutionPort:
    mode = 'offline_native_decimal_claims'

    def __init__(self, *, evaluator, lifecycle, source):
        if type(evaluator) is not DemoPipeline or evaluator.broker is not None:
            raise SafetyError('decision_only_pipeline_required')
        if type(lifecycle) is not Lifecycle or type(source) is not NativeFixtureObservations:
            raise SafetyError('exact_native_fixture_boundary_required')
        if set(evaluator.config.allowlist) != set(lifecycle.allowlist):
            raise SafetyError('universe_mismatch')
        if getattr(lifecycle, 'session_policy', None) is None:
            raise SafetyError('session_risk_policy_required')
        if lifecycle.session_policy.max_entries > 4 or lifecycle.session_policy.max_loss > Decimal('100'):
            raise SafetyError('strategy_risk_policy_too_permissive')
        self.pipeline, self.lifecycle, self.source = evaluator, lifecycle, source
        self.snapshot = None
        self.raw = None
        self.receipts = []

    def reconcile(self, now):
        raw = self.source.read()
        if 'now_ns' in raw:
            raise SafetyError('caller_current_time_forbidden')
        self.session_open_ns = raw.pop('session_open_ns', None)
        if type(self.session_open_ns) is not int:
            raise SafetyError('explicit_session_open_required')
        self.lifecycle.observe(**raw, now_ns=nanoseconds(now))
        self.raw = raw
        self.snapshot = self.lifecycle._parse(raw, nanoseconds(now))
        if not self.session_open_ns < self.snapshot.clock.next_close_ns:
            raise SafetyError('invalid_session_window')
        if self.snapshot.clock.is_open and self.session_open_ns > nanoseconds(now):
            raise SafetyError('future_session_open')
        with self.lifecycle.tx() as db:
            db.execute('CREATE TABLE IF NOT EXISTS native_strategy_sessions (close_ns INTEGER PRIMARY KEY, open_ns INTEGER NOT NULL)')
            prior = db.execute('SELECT open_ns FROM native_strategy_sessions WHERE close_ns=?', (self.snapshot.clock.next_close_ns,)).fetchone()
            if prior is not None and prior['open_ns'] != self.session_open_ns:
                raise SafetyError('session_open_changed')
            db.execute('INSERT OR IGNORE INTO native_strategy_sessions VALUES(?,?)', (self.snapshot.clock.next_close_ns, self.session_open_ns))
        return self.lifecycle.status()

    def _control(self):
        if self.pipeline.config.enabled is not True:
            raise SafetyError('simulation_disabled')
        state = self.lifecycle.status()
        with self.pipeline.db() as db:
            if db.execute('SELECT stopped FROM demo_control').fetchone()[0]:
                raise SafetyError('global_stop')
        if state['stopped'] or state['attention']:
            raise SafetyError('stopped_or_attention')
        # SQLite insertion order represents the serialized claim sequence even
        # when two claims share a nanosecond timestamp.
        with self.lifecycle.tx() as db:
            state['intents'] = [dict(r) for r in db.execute('SELECT * FROM intents ORDER BY rowid')]
        return state

    def expire_entries(self, now):
        state = self._control()
        by_id = {o.client_order_id: o for o in self.snapshot.orders}
        with self.lifecycle.tx() as db:
            already = {r['cid'] for r in db.execute('SELECT cid FROM cancels')}
        for row in state['intents']:
            order = by_id.get(row['cid'])
            if (row['side'] == 'buy' and order is not None and order.status not in TERMINAL
                    and order.status != 'pending_cancel' and row['cid'] not in already
                    and nanoseconds(now) - row['claimed'] >= 60 * 10**9):
                self.receipts.append(self.lifecycle.claim_cancel(row['cid'], now_ns=nanoseconds(now)))

    def _owned(self):
        state = self._control()
        return tuple(OwnedIntent(self.lifecycle.binding, r['cid'], r['symbol'], r['side'],
                                 Decimal(r['quantity'])) for r in state['intents'])

    def candidates(self):
        state = self._control()
        by_id = {o.client_order_id: o for o in self.snapshot.orders}
        # Any uncertain or working claim reserves the account, including cancels.
        if any(r['cid'] not in by_id for r in state['intents']):
            return []
        if any(o.status not in TERMINAL for o in self.snapshot.orders):
            return []
        rows = []
        for symbol in self.lifecycle.allowlist:
            owned = [by_id[r['cid']] for r in state['intents'] if r['symbol'] == symbol]
            net = sum((o.filled * (1 if o.side == 'buy' else -1) for o in owned), Decimal(0))
            if net <= 0:
                if any(p.symbol == symbol for p in self.snapshot.positions) and owned:
                    raise SafetyError('flat_position_mismatch')
                continue
            qty = owned_exit_quantity(self.snapshot, intents=self._owned(), symbol=symbol,
                                      now_ns=self.snapshot.observed_ns)
            # Exclude completed historical round trips from current entry basis
            # and holding clock. All current inventory belongs to one flat-to-flat
            # cycle because the lifecycle prohibits overlapping symbol entries.
            cycle, cycle_net = [], Decimal(0)
            for order in owned:
                cycle.append(order)
                cycle_net += order.filled * (1 if order.side == 'buy' else -1)
                if cycle_net == 0:
                    cycle = []
            buys = [o for o in cycle if o.side == 'buy' and o.filled > 0]
            total = sum((o.filled for o in buys), Decimal(0))
            average = sum((o.filled * o.average_price for o in buys), Decimal(0)) / total
            # Submitted time is conservative: native order schema lacks first-fill
            # timestamp for terminal partial fills, so holding timeout may be early.
            rows.append(dict(symbol=symbol, quantity=qty, average_price=average,
                             first_fill_at=min(o.submitted_ns for o in buys),
                             order_ids=tuple(sorted(o.client_order_id for o in owned))))
        return rows

    def due(self, row, quote, now):
        self._match_quote(quote, row['symbol'], now)
        bid, _ = self.pipeline._quote(quote, row['symbol'], now)
        delta = bid / row['average_price'] - 1
        n = nanoseconds(now)
        return (n - row['first_fill_at'] >= 3600 * 10**9 or delta <= Decimal('-.01')
                or delta >= Decimal('.02') or self.snapshot.clock.next_close_ns - n <= 600 * 10**9)

    def exit(self, row, quote, now):
        self._control()
        self._match_quote(quote, row['symbol'], now)
        bid, _ = self.pipeline._quote(quote, row['symbol'], now)
        # Include terminal ledger identities: a partial exit makes a new exact-
        # residue reduction, while restart with unchanged evidence cannot replay.
        identity = json.dumps([self.lifecycle.binding, row['symbol'], row['order_ids']], separators=(',', ':'))
        key = 'exit-' + hashlib.sha256(identity.encode()).hexdigest()[:48]
        limit = bid.quantize(Decimal('.01'), rounding=ROUND_FLOOR)
        if limit < Decimal('1'):
            raise SafetyError('exit_price_invalid')
        proposal = Proposal(key, key, row['symbol'], 'sell', row['quantity'], limit)
        receipt = self.lifecycle.claim(proposal, now_ns=nanoseconds(now))
        self.receipts.append(receipt)
        return receipt

    def blocked(self):
        state = self.lifecycle.status()
        if state['stopped'] or state['attention']:
            return True
        risk = next((r for r in state['risk_sessions'] if r['session_close_ns'] == self.snapshot.clock.next_close_ns), None)
        if risk is not None and (risk['loss_latched'] or risk['entry_claims'] >= self.lifecycle.session_policy.max_entries):
            return True  # Entry-only gate. Runner processes reductions first.
        by_id = {o.client_order_id: o for o in self.snapshot.orders}
        return (any(r['cid'] not in by_id for r in state['intents'])
                or any(o.status not in TERMINAL for o in self.snapshot.orders))

    def session(self, now):
        self._control()
        n = nanoseconds(now)
        fresh(self.snapshot.observed_ns, n)
        fresh(self.snapshot.clock.timestamp_ns, n)
        return self.snapshot.clock.is_open and n < self.snapshot.clock.next_close_ns

    def event_ready(self, event, now):
        # Continuous feeds see events immediately. Do not permanently claim an
        # abstention before the existing five-minute signal window can exist.
        anchor = event.published_at.replace(second=0, microsecond=0)
        ready = anchor + timedelta(minutes=5 + (event.published_at != anchor))
        return now >= ready

    def _match_quote(self, strategy_quote, symbol, now):
        native = parse_latest_quotes(self.raw['quotes'], symbols=tuple(self.raw['quote_symbols']),
                                     feed=self.raw['feed'], now_ns=nanoseconds(now))
        quote = next((q for q in native if q.symbol == symbol), None)
        if quote is None or (quote.bid, quote.ask, quote.timestamp_ns) != (
                Decimal(strategy_quote.bid), Decimal(strategy_quote.ask), nanoseconds(strategy_quote.asof)):
            raise SafetyError('strategy_native_quote_mismatch')

    def evaluate(self, event, inputs, now):
        # Strategy and native quote must be the same acquired observation.
        self._match_quote(inputs.quote, event.symbol, now)
        return self.pipeline.evaluate(event, inputs.quote, inputs.stock_bars, inputs.benchmark_bars, now)

    def _entry_strategy_gates(self, row, payload, now):
        n = nanoseconds(now)
        if n - self.session_open_ns < 300 * 10**9 or self.snapshot.clock.next_close_ns - n < 4500 * 10**9:
            raise SafetyError('entry_session_window')
        state = self._control()
        if any(r['side'] == 'buy' and r['symbol'] == row['symbol']
               and r['claimed'] >= self.session_open_ns for r in state['intents']):
            raise SafetyError('symbol_session_entry_cap')
        by_id = {o.client_order_id: o for o in self.snapshot.orders}
        if any(r['cid'] not in by_id for r in state['intents']):
            raise SafetyError('unresolved_claim')
        owned = {}
        for r in state['intents']:
            o = by_id[r['cid']]
            owned[o.symbol] = owned.get(o.symbol, Decimal(0)) + o.filled * (1 if o.side == 'buy' else -1)
        owned = {symbol: qty for symbol, qty in owned.items() if qty}
        if owned != {p.symbol: p.quantity for p in self.snapshot.positions}:
            raise SafetyError('unmanaged_position')
        if len(owned) >= 2 or row['symbol'] in owned:
            raise SafetyError('position_cap')
        limit = Decimal(payload['limit_price'])
        if sum(owned.values(), Decimal(0)) * 1000 + limit > 2000:
            raise SafetyError('strategy_exposure_cap')

    def submit(self, decision_id, now):
        # A claim is NOT submission. The runner's legacy method name is preserved
        # solely to reuse orchestration; no HTTP/order payload is returned.
        self._control()
        with self.pipeline.db() as db:
            row = db.execute('SELECT * FROM demo_decisions WHERE id=?', (decision_id,)).fetchone()
        if row is None or row['action'] != 'enter' or row['binding'] != self.pipeline.binding:
            raise SafetyError('eligible_decision_required')
        if not row['created'] <= now.timestamp() <= row['expires']:
            raise SafetyError('expired_decision')
        payload = json.loads(row['payload'])
        # Stable episode identity excludes duplicate event IDs or corrected
        # versions from producing a second entry for the same fiscal episode.
        identity = json.dumps([self.pipeline.config.strategy_version, row['episode'], row['symbol']], separators=(',', ':'))
        key = 'entry-' + hashlib.sha256(identity.encode()).hexdigest()[:48]
        proposal = Proposal(key, key, row['symbol'], 'buy', Decimal('1'), Decimal(payload['limit_price']))
        try:
            self._entry_strategy_gates(row, payload, now)
            receipt = self.lifecycle.claim(proposal, now_ns=nanoseconds(now))
        except (LifecycleError, SafetyError) as exc:
            if str(exc) not in ('session_loss_entry_gate', 'session_entry_quota',
                                'entry_session_window', 'symbol_session_entry_cap',
                                'position_cap', 'strategy_exposure_cap', 'market_closed'):
                raise
            receipt = Receipt(key, key, state='entry_blocked')
        self.receipts.append(receipt)
        return receipt
