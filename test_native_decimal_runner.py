"""One runner: bound event -> decision -> Decimal claim -> native fills -> flat.
All provider-shaped responses are synthetic. Sockets are prohibited in this suite.
"""
import copy
import hashlib
import hmac
import json
import socket
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from active_demo_pipeline import DemoPipeline, Config, Quote, Bar, FakeBroker, SafetyError
from alpaca_schema import PAPER_ORIGIN, timestamp
from decimal_lifecycle import Lifecycle, SessionRiskPolicy, Proposal, LifecycleError
from demo_tick_runner import DemoTickRunner, RunnerConfig, RunnerLedger, MarketInputs, PipelineExecutionPort
from native_decimal_port import NativeDecimalExecutionPort, NativeFixtureObservations, nanoseconds
from trusted_event_boundary import TrustedEventBuilder, Issuer, SourceReceipt, ImmutableSnapshot, canonical, SCHEMA

FIXTURE = json.loads((Path(__file__).parent / 'fixtures/alpaca-native.json').read_text())


class NativeRunnerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.no_socket = patch.object(socket, 'socket', side_effect=AssertionError('network forbidden'))
        self.no_socket.start(); self.addCleanup(self.no_socket.stop)
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.now = datetime(2026, 10, 19, 13, 36, 1, tzinfo=timezone.utc)
        self.key = b'synthetic-fixture-not-a-credential'
        self.builder = TrustedEventBuilder(self.root / 'source.db',
            (Issuer('fixture-issuer', 'AAPL', ('https://issuer.example',)),),
            {'fixture': self.key}, lambda: self.now)
        self.data = dict(schema=SCHEMA, issuer_id='fixture-issuer', symbol='AAPL',
            episode_id='fixture-FY26Q4', version='1', published_at='2026-10-19T13:31:00Z',
            event_type='earnings', corrected=False)
        self.event = self.builder.build(*self.receipt()).event
        self.assertIsNotNone(self.event)
        self.fp = hashlib.sha256(FIXTURE['account']['id'].encode()).hexdigest()
        self.policy = SessionRiskPolicy(max_entries=3, max_loss=Decimal('100'))
        self.lifecycle = Lifecycle(self.root / 'native.db', self.fp, session_risk_policy=self.policy)
        self.evaluator = DemoPipeline(self.root / 'decisions.db', Config(enabled=True, allowlist=('AAPL',)))
        self.orders, self.positions = [], []
        self.bid, self.ask, self.equity = '200.10', '200.20', '100000'
        self.source = NativeFixtureObservations(self.observation())
        self.port = NativeDecimalExecutionPort(evaluator=self.evaluator, lifecycle=self.lifecycle, source=self.source)
        self.on_inputs = lambda: None
        self.ledger = RunnerLedger(self.root / 'runner.db')
        self.runner = self.make_runner()

    def receipt(self, data=None):
        raw = canonical(self.data if data is None else data); digest = hashlib.sha256(raw).hexdigest()
        r = SourceReceipt('fixture', 'https://issuer.example/release', digest,
                          '2026-10-19T13:31:01Z', '2026-10-19T13:31:02Z', '')
        r = replace(r, signature=hmac.new(self.key, canonical(r.payload()), hashlib.sha256).hexdigest())
        return raw, r, ImmutableSnapshot(raw, digest)

    def observation(self):
        f = copy.deepcopy(FIXTURE); at = self.now.isoformat()
        f['clock']['timestamp'] = at; f['account']['equity'] = self.equity
        f['quote']['quote'].update(t=at, bp=self.bid, ap=self.ask)
        return dict(session_open_ns=timestamp('2026-10-19T13:30:00Z'), account=f['account'], clock=f['clock'], positions=self.positions,
                    orders=self.orders, origin=PAPER_ORIGIN,
                    response_account_fingerprint=self.fp, observed_ns=nanoseconds(self.now),
                    collection_complete=True, quotes=f['quote'], quote_symbols=('AAPL',), feed='iex')

    def advance(self, seconds=1):
        self.now += timedelta(seconds=seconds)
        self.source.replace(self.observation())

    def events(self, now, limit): return [self.event]
    def event_eligible(self, event): return self.builder.event_eligible(event)
    def quote(self, symbol, now): return Quote(symbol, self.bid, self.ask, now, now)
    def inputs(self, event, now):
        self.on_inputs()
        def bars(symbol, end):
            return tuple(Bar(symbol, event.published_at + timedelta(minutes=i-1),
                event.published_at + timedelta(minutes=i), end if i == 5 else '100',
                event.published_at + timedelta(minutes=i)) for i in range(6))
        return MarketInputs(self.quote(event.symbol, now), bars(event.symbol, '100.5'), bars('SPY', '100.1'))

    def make_runner(self, enabled=True):
        return DemoTickRunner(eventsource=self, marketsource=self, executionport=self.port,
            ledger=self.ledger, clock=lambda: self.now, config=RunnerConfig(enabled=enabled))

    def latest(self): return self.lifecycle.status()['intents'][-1]

    def fill(self, row, amount, status, *, total_position):
        raw = copy.deepcopy(FIXTURE['orders'][0])
        at = self.now.isoformat()
        claimed = datetime.fromtimestamp(row['claimed'] / 10**9, timezone.utc).isoformat()
        raw.update(id=row['cid'] + '-provider', client_order_id=row['cid'], side=row['side'],
            qty=row['quantity'], filled_qty=amount, status=status, limit_price=row['price'],
            filled_avg_price=row['price'] if Decimal(amount) else None,
            created_at=claimed, submitted_at=claimed, updated_at=at,
            filled_at=at if status == 'filled' else None, canceled_at=at if status == 'canceled' else None)
        self.orders = [o for o in self.orders if o['client_order_id'] != row['cid']] + [raw]
        self.positions = [] if Decimal(total_position) == 0 else [dict(FIXTURE['positions'][0],
            qty=total_position, qty_available=total_position,
            market_value=str(Decimal(total_position) * Decimal(self.bid)))]
        self.source.replace(self.observation())

    def enter(self):
        result = self.runner.tick()
        self.assertEqual(result['phase'], 'complete')
        self.assertEqual(result['mode'], 'offline_native_decimal_claims')
        row = self.latest(); self.assertEqual(row['quantity'], '1')
        self.assertEqual(row['side'], 'buy')
        self.assertFalse(self.port.receipts[-1].executable)
        self.assertEqual(self.evaluator.records(), [])  # No integer execution used.
        return row

    def test_single_runner_fractional_entry_exit_partial_residue_flat_restart(self):
        entry = self.enter(); self.advance()
        self.fill(entry, '0.125000001', 'canceled', total_position='0.125000001')
        self.bid, self.ask = '205.00', '205.10'; self.advance()
        self.assertEqual(self.runner.tick()['exits'], 1)
        first_exit = self.latest(); self.assertEqual(first_exit['quantity'], '0.125000001')
        self.advance(); self.fill(first_exit, '0.025000001', 'canceled', total_position='0.100000000')
        self.assertEqual(self.runner.tick()['exits'], 1)
        second_exit = self.latest(); self.assertEqual(Decimal(second_exit['quantity']), Decimal('0.100000000'))
        self.advance(); self.fill(second_exit, '0.100000000', 'filled', total_position='0')
        self.assertEqual(self.runner.tick()['phase'], 'complete')
        self.assertEqual(self.port.candidates(), [])
        self.advance(); self.runner = self.make_runner()
        self.assertEqual(self.runner.tick()['phase'], 'complete')
        self.assertEqual(len(self.lifecycle.status()['intents']), 3)
        self.assertTrue(all(not r.executable for r in self.port.receipts))

    def test_early_event_deferred_without_consuming_identity(self):
        self.now -= timedelta(seconds=120); self.source.replace(self.observation())
        self.assertEqual(self.runner.tick()['phase'], 'complete')
        self.assertEqual(self.lifecycle.status()['intents'], [])
        with self.ledger.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM runner_events').fetchone()[0], 0)
        self.advance(120)
        self.enter()

    def test_quota_blocks_entry_without_attention_and_still_allows_exit(self):
        self.lifecycle = Lifecycle(self.root / 'quota.db', self.fp,
            session_risk_policy=SessionRiskPolicy(1, Decimal('100')))
        self.port = NativeDecimalExecutionPort(evaluator=self.evaluator, lifecycle=self.lifecycle, source=self.source)
        self.runner = self.make_runner()
        entry = self.enter(); self.advance(); self.fill(entry, '1', 'filled', total_position='1')
        self.assertEqual(self.runner.tick()['phase'], 'blocked')
        self.assertIsNone(self.ledger.status()['attention'])
        self.bid, self.ask = '205', '205.1'; self.advance()
        self.assertEqual(self.runner.tick()['exits'], 1)
        self.assertEqual(self.latest()['side'], 'sell')

    def test_loss_gate_does_not_latch_runner_attention(self):
        self.equity = '98000'; self.source.replace(self.observation())
        self.assertEqual(self.runner.tick()['phase'], 'blocked')
        self.assertEqual(self.lifecycle.status()['intents'], [])
        self.assertIsNone(self.ledger.status()['attention'])
        self.equity = '100000'; self.advance()
        self.assertEqual(self.runner.tick()['phase'], 'blocked')

    def test_session_close_during_market_acquisition_blocks_claim(self):
        obs = self.observation()
        obs['clock']['next_close'] = (self.now + timedelta(seconds=1)).isoformat()
        self.source.replace(obs)
        def delay(): self.now += timedelta(seconds=2)
        self.on_inputs = delay
        self.assertEqual(self.runner.tick()['phase'], 'complete')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_stop_during_market_acquisition_blocks_claim(self):
        self.on_inputs = self.evaluator.stop
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_native_runner_rejects_looser_loss_policy(self):
        loose = Lifecycle(self.root / 'loose.db', self.fp,
            session_risk_policy=SessionRiskPolicy(3, Decimal('1000')))
        with self.assertRaises(SafetyError):
            NativeDecimalExecutionPort(evaluator=self.evaluator, lifecycle=loose, source=self.source)

    def test_entry_near_close_is_blocked(self):
        obs = self.observation()
        obs['clock']['next_close'] = (self.now + timedelta(minutes=60)).isoformat()
        self.source.replace(obs)
        self.assertEqual(self.runner.tick()['phase'], 'complete')
        self.assertIsNone(self.ledger.status()['attention'])
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_external_position_blocks_entry(self):
        self.positions = [dict(FIXTURE['positions'][0], symbol='MSFT', qty='1', qty_available='1')]
        self.source.replace(self.observation())
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_missing_session_open_blocks_entry(self):
        obs = self.observation(); del obs['session_open_ns']; self.source.replace(obs)
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_lifecycle_rechecks_exact_session_close_inside_claim(self):
        obs = self.observation()
        obs['clock']['next_close'] = (self.now + timedelta(seconds=1)).isoformat()
        self.source.replace(obs); self.port.reconcile(self.now)
        with self.assertRaisesRegex(LifecycleError, 'market_closed'):
            self.lifecycle.claim(Proposal('close-race', 'close-race', 'AAPL', 'buy', Decimal('1'), Decimal('200.20')),
                now_ns=nanoseconds(self.now + timedelta(seconds=2)))
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_same_session_open_cannot_change(self):
        self.enter(); self.advance()
        obs = self.observation(); obs['session_open_ns'] -= 60 * 10**9; self.source.replace(obs)
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(len(self.lifecycle.status()['intents']), 1)

    def test_second_same_symbol_session_entry_is_not_claimed(self):
        entry = self.enter(); self.advance(); self.fill(entry, '1', 'filled', total_position='1')
        self.bid, self.ask = '205', '205.1'; self.advance()
        self.assertEqual(self.runner.tick()['exits'], 1)
        exit_row = self.latest(); self.advance(); self.fill(exit_row, '1', 'filled', total_position='0')
        self.runner.tick(); self.advance()
        self.event = self.builder.build(*self.receipt(dict(self.data, episode_id='second-episode'))).event
        self.assertIsNotNone(self.event)
        self.assertEqual(self.runner.tick()['phase'], 'complete')
        self.assertEqual(len(self.lifecycle.status()['intents']), 2)
        self.assertIsNone(self.ledger.status()['attention'])

    def test_subcent_quote_exit_preserves_original_sell_limit_rule(self):
        entry = self.enter(); self.advance(); self.fill(entry, '1', 'filled', total_position='1')
        self.bid, self.ask = '205.001', '205.002'; self.advance()
        self.assertEqual(self.runner.tick()['exits'], 1)
        self.assertEqual(Decimal(self.latest()['price']), Decimal('205.00'))
        self.assertIsNone(self.ledger.status()['attention'])

    def test_default_disabled(self):
        self.assertEqual(self.make_runner(False).tick()['phase'], 'disabled')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_missing_native_order_never_replays_after_restart(self):
        self.enter(); self.advance()
        self.assertEqual(self.make_runner().tick()['phase'], 'blocked')
        self.assertEqual(len(self.lifecycle.status()['intents']), 1)

    def test_pending_cancel_retains_reservation(self):
        entry = self.enter(); self.advance()
        self.fill(entry, '0.125000001', 'pending_cancel', total_position='0.125000001')
        self.bid, self.ask = '205', '205.1'; self.advance()
        self.assertEqual(self.runner.tick()['phase'], 'blocked')
        self.assertEqual(len(self.lifecycle.status()['intents']), 1)

    def test_expired_entry_cancel_claimed_once_no_assumed_success(self):
        entry = self.enter(); self.advance()
        self.fill(entry, '0', 'new', total_position='0'); self.advance(61)
        self.assertEqual(self.runner.tick()['phase'], 'blocked')
        self.assertEqual(len(self.port.receipts), 2)
        self.assertTrue(self.port.receipts[-1].logical_key.startswith('cancel:'))
        self.advance(); self.assertEqual(self.make_runner().tick()['phase'], 'blocked')
        self.assertEqual(len(self.port.receipts), 2)

    def test_loss_latch_does_not_block_owned_exit(self):
        entry = self.enter(); self.advance()
        self.fill(entry, '1', 'filled', total_position='1')
        self.bid, self.ask, self.equity = '205', '205.1', '98000'; self.advance()
        self.assertEqual(self.runner.tick()['exits'], 1)
        self.assertEqual(self.latest()['side'], 'sell')

    def test_global_stop_blocks_exits(self):
        entry = self.enter(); self.advance(); self.fill(entry, '1', 'filled', total_position='1')
        self.bid, self.ask = '205', '205.1'; self.advance(); self.lifecycle.stop()
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(len(self.lifecycle.status()['intents']), 1)

    def test_source_correction_during_inputs_blocks_claim(self):
        self.on_inputs = lambda: self.builder.build(*self.receipt(dict(self.data, version='2', corrected=True)))
        self.assertEqual(self.runner.tick()['phase'], 'complete')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_strategy_native_quote_mismatch_blocks_claim(self):
        self.bid, self.ask = '201', '201.1'  # Native observation still 200.10/200.20.
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_stale_observation_fails_closed(self):
        self.now += timedelta(seconds=11)
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_wrong_paper_origin_fails_closed(self):
        obs = self.observation(); obs['origin'] = 'https://api.alpaca.markets'
        self.source.replace(obs)
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_incomplete_collection_fails_closed(self):
        obs = self.observation(); obs['collection_complete'] = False; self.source.replace(obs)
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')

    def test_original_fake_broker_fence_unchanged(self):
        with self.assertRaises(SafetyError):
            DemoPipeline(self.root / 'reject.db', Config(True), self.port)
        with self.assertRaises(SafetyError):
            PipelineExecutionPort(self.evaluator)

    def test_readonly_observation_cannot_enter_fixture_execution_port(self):
        from test_native_observation_collector import CollectorTests
        harness = CollectorTests()
        harness.setUp()
        try:
            observation = harness.collector().collect()
            with self.assertRaisesRegex(SafetyError, 'exact_native_fixture_boundary_required'):
                NativeDecimalExecutionPort(evaluator=self.evaluator, lifecycle=self.lifecycle, source=observation)
        finally:
            harness.doCleanups()

    def test_arbitrary_source_cannot_enter_native_port(self):
        with self.assertRaises(SafetyError):
            NativeDecimalExecutionPort(evaluator=self.evaluator, lifecycle=self.lifecycle, source=object())

    def test_native_runner_requires_persistent_session_policy(self):
        legacy = Lifecycle(self.root / 'legacy.db', self.fp)
        with self.assertRaises(SafetyError):
            NativeDecimalExecutionPort(evaluator=self.evaluator, lifecycle=legacy, source=self.source)

    def test_runner_crash_marker_blocks_resume(self):
        self.ledger.begin(self.now, 120)
        self.assertEqual(self.make_runner().tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['intents'], [])

    def test_clock_regression_blocks_resume(self):
        self.enter(); self.now -= timedelta(seconds=1)
        self.assertEqual(self.make_runner().tick()['phase'], 'manual_attention')
        self.assertEqual(len(self.lifecycle.status()['intents']), 1)

    def test_snapshot_identity_mismatch_latches(self):
        obs = self.observation(); obs['response_account_fingerprint'] = 'f' * 64; self.source.replace(obs)
        self.assertEqual(self.runner.tick()['phase'], 'manual_attention')
        self.assertEqual(self.lifecycle.status()['attention'], 1)

if __name__ == '__main__': unittest.main()
