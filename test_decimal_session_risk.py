"""Offline session-risk regressions. No transports, sockets, or credentials."""
import copy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D, localcontext
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from alpaca_schema import PAPER_ORIGIN, timestamp
from decimal_lifecycle import Lifecycle, LifecycleError, Proposal, SessionRiskPolicy


FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'alpaca-native.json').read_text())
FP = hashlib.sha256(FIXTURE['account']['id'].encode()).hexdigest()
START = datetime(2026, 10, 19, 13, 31, 1, tzinfo=timezone.utc)
BASE = timestamp('2026-10-19T13:31:01Z')
POLICY = SessionRiskPolicy(max_entries=3, max_loss=D('1000'))


def stamp(seconds=0):
    return (START + timedelta(seconds=seconds)).isoformat().replace('+00:00', 'Z')


class SessionRiskTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'ledger.sqlite')
        self.l = Lifecycle(self.path, FP, session_risk_policy=POLICY)
        self.orders = []
        self.claimed = {}
        self.observe()

    def tearDown(self):
        self.tmp.cleanup()

    def observe(self, seconds=0, *, equity='100000', last_equity='100000',
                orders=None, quantity=None, is_open=True, next_close=None,
                clock_seconds=None, lifecycle=None):
        at = START + timedelta(seconds=seconds)
        f = copy.deepcopy(FIXTURE)
        f['account'].update(equity=equity, last_equity=last_equity)
        f['clock'].update(
            timestamp=stamp(seconds if clock_seconds is None else clock_seconds),
            is_open=is_open,
            next_close=next_close or at.replace(hour=20, minute=0, second=0).isoformat(),
            next_open=(at + timedelta(days=1)).replace(hour=13, minute=30, second=0).isoformat(),
        )
        f['quote']['quote']['t'] = stamp(seconds)
        positions = [] if quantity is None else [
            dict(f['positions'][0], qty=quantity, qty_available=quantity,
                 market_value=str(D(quantity) * D('200.2')))
        ]
        (lifecycle or self.l).observe(
            f['account'], f['clock'], positions,
            self.orders if orders is None else orders,
            origin=PAPER_ORIGIN, response_account_fingerprint=FP,
            observed_ns=BASE + seconds * 10**9, now_ns=BASE + seconds * 10**9,
            collection_complete=True, quotes=f['quote'], quote_symbols=('AAPL',),
        )

    def claim(self, cid='entry', seconds=0, *, side='buy', qty='1', lifecycle=None):
        receipt = (lifecycle or self.l).claim(
            Proposal(cid, cid, 'AAPL', side, D(qty), D('200.2')),
            now_ns=BASE + seconds * 10**9,
        )
        self.claimed[cid] = seconds
        return receipt

    def order(self, cid='entry', seconds=1, *, side='buy', qty='1',
              status='rejected', fill='0'):
        r = copy.deepcopy(FIXTURE['orders'][0])
        r.update(
            id='provider-' + cid, client_order_id=cid, side=side, qty=qty,
            status=status, filled_qty=fill,
            filled_avg_price='200.2' if D(fill) else None, limit_price='200.2',
            created_at=stamp(self.claimed[cid]), submitted_at=stamp(self.claimed[cid]),
            updated_at=stamp(seconds), canceled_at=None,
            filled_at=stamp(seconds) if status == 'filled' else None,
        )
        return r

    def reject(self, cid, seconds):
        self.orders.append(self.order(cid, seconds))
        self.observe(seconds)

    def risk(self, lifecycle=None):
        return (lifecycle or self.l).status()['risk_sessions'][-1]

    def seed_owned_entry(self):
        # A one-entry budget proves both quota and loss exemptions for exits.
        self.path = str(Path(self.tmp.name) / 'single-entry.sqlite')
        self.l = Lifecycle(self.path, FP, session_risk_policy=SessionRiskPolicy(1, D('1000')))
        self.observe()
        self.claim()
        self.orders = [self.order(status='filled', fill='1')]
        self.observe(1, quantity='1', equity='98000')

    def test_policy_is_immutable_and_validated(self):
        with self.assertRaises(FrozenInstanceError):
            POLICY.max_entries = 99
        with self.assertRaises(AttributeError):
            self.l.session_policy = SessionRiskPolicy(99, D('1000'))
        for entries, loss in [(True, D('1')), (0, D('1')), (-1, D('1')),
                              (1.0, D('1')), (2**63, D('1')), (1, 1000),
                              (1, D('NaN')), (1, D('Infinity')), (1, D('0'))]:
            with self.subTest(entries=entries, loss=loss):
                with self.assertRaises(LifecycleError):
                    SessionRiskPolicy(entries, loss)

    def test_policy_binding_survives_restart_and_cannot_downgrade(self):
        reopened = Lifecycle(self.path, FP, session_risk_policy=SessionRiskPolicy(3, D('1000.0')))
        self.assertEqual(reopened.status()['risk_policy'], {'max_entries': 3, 'max_loss': '1000'})
        for policy in [None, SessionRiskPolicy(4, D('1000')), SessionRiskPolicy(3, D('1001'))]:
            with self.subTest(policy=policy):
                with self.assertRaisesRegex(LifecycleError, 'session_risk_policy_mismatch'):
                    Lifecycle(self.path, FP, session_risk_policy=policy)

    def test_existing_legacy_ledger_cannot_gain_new_budget(self):
        path = str(Path(self.tmp.name) / 'legacy.sqlite')
        Lifecycle(path, FP)
        with self.assertRaisesRegex(LifecycleError, 'session_risk_policy_mismatch'):
            Lifecycle(path, FP, session_risk_policy=POLICY)
        self.assertIsNone(Lifecycle(path, FP).session_policy)

    def test_legacy_default_retains_no_entry_quota(self):
        path = str(Path(self.tmp.name) / 'legacy.sqlite')
        self.l = Lifecycle(path, FP)
        self.observe()
        for i in range(4):
            self.claim('entry-' + str(i), i)
            self.reject('entry-' + str(i), i + 1)
        self.assertEqual(self.l.status()['risk_sessions'], [])
        self.assertEqual(len(self.l.status()['intents']), 4)

    def test_rejected_and_uncertain_claims_permanently_use_budget(self):
        for i in range(3):
            self.assertFalse(self.claim('entry-' + str(i), i).executable)
            self.assertEqual(self.risk()['entry_claims'], i + 1)
            self.reject('entry-' + str(i), i + 1)
        self.l = Lifecycle(self.path, FP, session_risk_policy=POLICY)
        with self.assertRaisesRegex(LifecycleError, 'session_entry_quota'):
            self.claim('fourth', 3)
        self.assertEqual(self.risk()['entry_claims'], 3)

    def test_unresolved_claim_blocks_further_claims_and_uses_one_slot(self):
        self.claim()
        self.l = Lifecycle(self.path, FP, session_risk_policy=POLICY)
        self.observe(1)
        with self.assertRaisesRegex(LifecycleError, 'unresolved_claim'):
            self.claim('other', 1)
        self.assertEqual(self.risk()['entry_claims'], 1)

    def test_failed_validation_and_duplicate_do_not_consume_budget(self):
        with self.assertRaisesRegex(LifecycleError, 'entry_size_or_cash'):
            self.claim(qty='25')  # Existing $5,000 entry cap is unchanged.
        self.assertEqual(self.risk()['entry_claims'], 0)
        self.claim()
        with self.assertRaisesRegex(LifecycleError, 'already_claimed'):
            self.claim()
        self.assertEqual(self.risk()['entry_claims'], 1)

    def test_intent_and_counter_roll_back_together(self):
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER fail_counter BEFORE UPDATE OF entry_claims ON risk_sessions BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.claim()
        self.assertEqual(self.l.status()['intents'], [])
        self.assertEqual(self.risk()['entry_claims'], 0)

    def test_concurrent_shared_db_claims_cannot_overbook(self):
        self.claim('first')
        self.reject('first', 1)
        self.claim('second', 1)
        self.reject('second', 2)

        def reserve(i):
            ledger = Lifecycle(self.path, FP, session_risk_policy=POLICY)
            try:
                return ledger.claim(Proposal('race-' + str(i), 'race-' + str(i), 'AAPL',
                                             'buy', D('1'), D('200.2')), now_ns=BASE + 2 * 10**9)
            except LifecycleError:
                return None

        with ThreadPoolExecutor(max_workers=8) as workers:
            receipts = list(workers.map(reserve, range(8)))
        self.assertEqual(sum(r is not None for r in receipts), 1)
        self.assertEqual(self.risk()['entry_claims'], 3)
        self.assertEqual(len(self.l.status()['intents']), 3)

    def test_loss_threshold_is_exact_and_survives_recovery_and_restart(self):
        self.observe(1, equity='99000.000000001')
        self.assertEqual(self.risk()['loss_latched'], 0)
        self.observe(2, equity='99000')
        self.observe(3, equity='105000')
        self.l = Lifecycle(self.path, FP, session_risk_policy=POLICY)
        self.assertEqual(self.risk()['baseline_equity'], '100000')
        self.assertEqual(self.risk()['loss_latched'], 1)
        with self.assertRaisesRegex(LifecycleError, 'session_loss_entry_gate'):
            self.claim(seconds=3)

    def test_loss_calculation_ignores_callers_low_decimal_precision(self):
        with localcontext() as ctx:
            ctx.prec = 4
            self.observe(1, equity='99000.000000001')
        self.assertEqual(self.risk()['loss_latched'], 0)

    def test_baseline_is_last_equity_not_peak_equity(self):
        self.observe(1, equity='105000')
        self.observe(2, equity='104000')
        self.assertEqual(self.risk()['loss_latched'], 0)
        self.assertEqual(self.risk()['baseline_equity'], '100000')

    def test_same_session_baseline_change_fails_closed(self):
        with self.assertRaisesRegex(LifecycleError, 'session_baseline_changed'):
            self.observe(1, last_equity='98000', equity='98000')
        self.assertEqual(self.risk()['baseline_equity'], '100000')
        self.assertEqual(self.l.status()['attention'], 1)

    def test_same_observation_does_not_reset_state(self):
        self.claim()
        self.observe()
        self.assertEqual(self.risk()['entry_claims'], 1)
        self.assertEqual(len(self.l.status()['risk_sessions']), 1)

    def test_new_session_has_new_budget_and_keeps_old_latch(self):
        self.claim()
        self.orders = [self.order()]
        self.observe(1, equity='99000')
        self.observe(86400, equity='99000', last_equity='99000')
        sessions = self.l.status()['risk_sessions']
        self.assertEqual(len(sessions), 2)
        self.assertEqual((sessions[0]['entry_claims'], sessions[0]['loss_latched']), (1, 1))
        self.assertEqual((sessions[1]['entry_claims'], sessions[1]['loss_latched']), (0, 0))
        self.claim('next-session', 86400)
        self.assertEqual(self.risk()['entry_claims'], 1)

    def test_session_change_before_old_close_fails_closed(self):
        with self.assertRaisesRegex(LifecycleError, 'premature_session_change'):
            self.observe(1, next_close='2026-10-20T20:00:00Z')
        self.assertEqual(len(self.l.status()['risk_sessions']), 1)
        self.assertEqual(self.l.status()['attention'], 1)

    def test_session_identifier_regression_fails_closed(self):
        with self.assertRaisesRegex(LifecycleError, 'session_regression'):
            self.observe(1, next_close='2026-10-19T19:00:00Z')
        self.assertEqual(self.l.status()['attention'], 1)

    def test_closed_observation_never_resets_budget(self):
        self.claim()
        self.observe(1, is_open=False, next_close='2026-10-20T20:00:00Z')
        self.assertEqual(len(self.l.status()['risk_sessions']), 1)
        self.assertEqual(self.risk()['entry_claims'], 1)
        self.observe(2)
        self.assertEqual(self.risk()['entry_claims'], 1)

    def test_closed_loss_observation_can_only_latch_existing_session(self):
        self.observe(1, equity='99000', is_open=False,
                     next_close='2026-10-20T20:00:00Z')
        self.observe(2)
        self.assertEqual(len(self.l.status()['risk_sessions']), 1)
        self.assertEqual(self.risk()['loss_latched'], 1)
        with self.assertRaisesRegex(LifecycleError, 'session_loss_entry_gate'):
            self.claim(seconds=2)

    def test_initial_closed_observation_cannot_establish_baseline(self):
        ledger = Lifecycle(str(Path(self.tmp.name) / 'initial-closed.sqlite'), FP,
                           session_risk_policy=POLICY)
        self.observe(is_open=False, equity='99000', lifecycle=ledger)
        self.assertEqual(ledger.status()['risk_sessions'], [])
        with self.assertRaisesRegex(LifecycleError, 'market_closed'):
            self.claim(lifecycle=ledger)
        self.observe(1, equity='100000', lifecycle=ledger)
        self.assertEqual(self.risk(ledger)['baseline_equity'], '100000')
        self.assertEqual(self.risk(ledger)['entry_claims'], 0)

    def test_closed_clock_regression_fails_closed(self):
        self.observe(2, is_open=False, next_close='2026-10-20T20:00:00Z')
        with self.assertRaisesRegex(LifecycleError, 'session_clock_regression'):
            self.observe(3, clock_seconds=1)
        self.assertEqual(self.l.status()['attention'], 1)

    def test_midnight_does_not_reset_same_session_loss(self):
        ledger = Lifecycle(str(Path(self.tmp.name) / 'overnight.sqlite'), FP,
                           session_risk_policy=POLICY)
        before_midnight = int((datetime(2026, 10, 19, 23, 59, 59, tzinfo=timezone.utc) - START).total_seconds())
        close = '2026-10-20T01:00:00Z'
        self.observe(before_midnight, equity='99000', next_close=close, lifecycle=ledger)
        self.observe(before_midnight + 2, equity='100000', next_close=close, lifecycle=ledger)
        with self.assertRaisesRegex(LifecycleError, 'session_loss_entry_gate'):
            self.claim(seconds=before_midnight + 2, lifecycle=ledger)
        self.assertEqual(len(ledger.status()['risk_sessions']), 1)

    def test_owned_exit_exempt_from_quota_and_loss_only(self):
        self.seed_owned_entry()
        self.assertFalse(self.claim('exit', 1, side='sell').executable)
        self.assertEqual(self.risk()['entry_claims'], 1)
        self.assertEqual(self.risk()['loss_latched'], 1)

    def test_stop_still_blocks_owned_exit(self):
        self.seed_owned_entry()
        self.l.stop()
        with self.assertRaisesRegex(LifecycleError, 'stopped_or_attention'):
            self.claim('exit', 1, side='sell')

    def test_attention_still_blocks_owned_exit(self):
        self.seed_owned_entry()
        with self.assertRaisesRegex(LifecycleError, 'session_baseline_changed'):
            self.observe(2, quantity='1', last_equity='98000', equity='98000')
        with self.assertRaisesRegex(LifecycleError, 'stopped_or_attention'):
            self.claim('exit', 2, side='sell')

    def test_uncertainty_still_blocks_owned_exit(self):
        self.claim()
        self.observe(1, quantity='1')
        with self.assertRaisesRegex(LifecycleError, 'unresolved_claim'):
            self.claim('exit', 1, side='sell')


if __name__ == '__main__':
    unittest.main()
