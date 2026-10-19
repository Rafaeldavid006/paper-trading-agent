"""Offline synthetic interface checks, not historical market or performance evidence.

Bridges are deliberately test-only: publication must be supplied independently,
SEC acceptance is never silently promoted to dissemination, and only comparable
ledger observations can become replay outcome references.
"""
import copy
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from event_core import iso, utc
from sec_submissions import SecIngestor, Snapshot, SourceError
from hypothesis_ledger import HypothesisLedger
from historical_replay import EventVersion, TimedFact, OutcomeReference, replay_index, plan_splits

AT = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)
CIK = '0000123456'
FIXTURE = json.loads((Path(__file__).parent / 'fixtures/sec_submissions.json').read_text())


def replay_fixture(row, hypothesis, *, publication, version=1, episode='synthetic-episode'):
    # Explicit gate belongs in any future production bridge as well.
    if publication is None:
        raise ValueError('Independently evidenced publication required; SEC acceptance is insufficient')
    return EventVersion(event_id='fixture:' + row['accession'], version=version,
        episode_id=episode, published_at=publication, observed_at=utc(row['first_seen_at']),
        decision_at=utc(hypothesis['window_start']),
        horizon_end=utc(hypothesis['window_start']) + timedelta(minutes=hypothesis['horizon_minutes']),
        source_ref='sec-version:' + row['version_id'], category='synthetic-metadata',
        targets=(hypothesis['symbol'],), evidence_refs=(hypothesis['evidence'][0]['reference'],))


def outcome_fixture(event, observation):
    if not observation['same_window_comparable'] or observation['direction_match'] is None:
        raise ValueError('Unscored or unmatched observations are not replay labels')
    return OutcomeReference(event.event_id, event.horizon_end,
        TimedFact('synthetic-ledger-observation:' + str(observation['id']),
                  'fixture:ledger', utc(observation['recorded_at'])))


class ResearchIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = AT
        # No client exists: every snapshot is a labelled synthetic fixture.
        self.sec = SecIngestor(self.tmp.name + '/integration.db', None, [CIK], clock=lambda: self.now)
        self.ledger = HypothesisLedger(self.sec.store, {'TEST': 'synthetic-issuer', 'BENCH': 'synthetic-benchmark'})
        self.payload = copy.deepcopy(FIXTURE)
        self.row, self.event_id = self.ingest(self.payload)

    def ingest(self, payload, project=True):
        result = self.sec.record(Snapshot(CIK, payload, iso(self.now), False))
        with self.sec.store.db() as db:
            row = dict(db.execute('SELECT * FROM sec_records WHERE version_id=?', (result['versions'][0],)).fetchone())
        return row, self.sec.project(row['version_id'], 'TEST')[0] if project else None

    def hypothesis(self, **changes):
        args = dict(key='h1', event_version=self.event_id, symbol='TEST', entity_id='synthetic-issuer',
            relation='direct', evidence=[dict(reference='sec-version:' + self.row['version_id'],
                excerpt='Synthetic fixture, not an economic claim', published_at=iso(AT), known_at=iso(AT))],
            direction='up', horizon_minutes=5, window_start=AT, knowledge_cutoff=AT,
            invalidation='Source metadata correction', strategy_version='fixture-only', benchmark_symbol='BENCH')
        return self.ledger.record(**dict(args, **changes))

    def observe(self, h, stale=False):
        end = AT + timedelta(minutes=5)
        def points(price):
            return dict(baseline=dict(price=100, at=iso(AT), known_at=iso(AT)),
                        terminal=dict(price=price, at=iso(end), known_at=iso(end)))
        samples = {'TEST': points(102), 'BENCH': points(101)}
        if stale:
            samples['BENCH']['terminal']['at'] = iso(end - timedelta(seconds=1))
        self.now = AT + timedelta(minutes=6)
        return self.ledger.observe(key='o1', hypothesis_id=h['id'], benchmark_symbol='BENCH',
            samples=samples, coverage_complete=True, source='synthetic-only', max_staleness_seconds=2)

    def plan(self, events, outcomes=()):
        return plan_splits(events, outcomes=outcomes, as_of=AT+timedelta(hours=3),
                           train_end=AT+timedelta(hours=1), validation_end=AT+timedelta(hours=2))

    def test_full_frozen_fixture_chain(self):
        h = self.hypothesis()
        event = replay_fixture(self.row, h, publication=AT)
        observation = self.observe(h)
        result = self.plan([event], [outcome_fixture(event, observation)])
        self.assertEqual(len(result.train), 1)
        self.assertEqual(result.train[0].source_ref, 'sec-version:' + self.row['version_id'])
        self.assertEqual(result.train[0].targets, ('TEST',))
        self.assertNotIn('synthetic-ledger-observation', repr(result.train))
        self.assertFalse(observation['causal_effect_proven'])
        self.assertEqual(h['order_permission'], 'none')
        # Frozen request is unaffected by caller-owned fixture mutation.
        self.payload['name'] = 'mutated caller input'
        with self.sec.store.db() as db:
            persisted = json.loads(db.execute('SELECT payload FROM sec_snapshots LIMIT 1').fetchone()[0])
        self.assertEqual(persisted['name'], 'SYNTHETIC TEST ISSUER')
        self.assertEqual(self.ledger.audit()['entries'][0]['evidence'], h['evidence'])

    def test_acceptance_is_not_publication(self):
        h = self.hypothesis()
        with self.assertRaisesRegex(ValueError, 'publication'):
            replay_fixture(self.row, h, publication=None)
        with self.sec.store.db() as db:
            payload = json.loads(db.execute('SELECT payload FROM events WHERE version_id=?', (self.event_id,)).fetchone()[0])
        self.assertEqual(payload['timestamp_semantics'], 'sec_acceptance_not_verified_public_dissemination')

    def test_late_arrival_not_visible_at_old_acceptance(self):
        synthetic_verified_publication = AT - timedelta(days=1)
        event = replay_fixture(self.row, self.hypothesis(), publication=synthetic_verified_publication)
        self.assertEqual(replay_index([event], AT-timedelta(microseconds=1)), ())
        self.assertEqual(replay_index([event], AT)[0].available_at, AT)
        earlier = replace(event, decision_at=AT-timedelta(minutes=1))
        self.assertIn('unavailable_at_decision', dict(self.plan([earlier]).excluded)[earlier.event_id])

    def test_missing_acceptance_retained_not_coerced(self):
        self.payload['filings']['recent']['acceptanceDateTime'][0] = ''
        self.now += timedelta(minutes=1)
        row, _ = self.ingest(self.payload, project=False)
        self.assertIsNone(row['accepted_at'])
        with self.assertRaises(SourceError):
            self.sec.project(row['version_id'], 'TEST')
        with self.assertRaises(ValueError):
            replay_fixture(row, self.hypothesis(), publication=None)

    def test_correction_keeps_old_hypothesis_and_freezes_split(self):
        h = self.hypothesis()
        first = replay_fixture(self.row, h, publication=AT)
        self.now += timedelta(minutes=1)
        self.payload['filings']['recent']['items'][0] = '5.02'
        row, newer = self.ingest(self.payload)
        corrected = replay_fixture(row, h, publication=AT, version=2)
        self.assertNotEqual(newer, self.event_id)
        self.assertEqual(replay_index([corrected, first], AT)[0].version, 1)
        self.assertEqual(replay_index([first, corrected], self.now)[0].version, 2)
        with self.assertRaises(SourceError):
            self.sec.project(self.row['version_id'], 'TEST')
        observed = self.observe(h)
        self.assertIsNone(observed['direction_match'])
        self.assertTrue(observed['event_superseded'])
        self.assertEqual(self.ledger.audit()['entries'][0]['event_version'], self.event_id)
        with self.assertRaises(ValueError):
            outcome_fixture(first, observed)
        # Replay independently preserves the version knowable at decision.
        ref = OutcomeReference(first.event_id, first.horizon_end,
            TimedFact('synthetic independent outcome', 'fixture:independent', self.now))
        self.assertEqual(self.plan([corrected, first], [ref]).train[0].version, 1)

    def test_amendment_is_separate_accession_not_guessed_parent(self):
        h = self.hypothesis()
        self.now += timedelta(minutes=1)
        self.payload['filings']['recent']['form'][0] = '8-K/A'
        self.payload['filings']['recent']['accessionNumber'][0] = '0000123456-26-000002'
        row, amended = self.ingest(self.payload)
        self.assertNotEqual(amended, self.event_id)
        with self.sec.store.db() as db:
            payload = json.loads(db.execute('SELECT payload FROM events WHERE version_id=?', (amended,)).fetchone()[0])
        self.assertTrue(payload['amendment'])
        self.assertIsNone(payload['amends_accession'])
        self.assertNotEqual(replay_fixture(row, h, publication=AT).event_id,
                            replay_fixture(self.row, h, publication=AT).event_id)

    def test_stale_benchmark_never_becomes_outcome_label(self):
        h = self.hypothesis()
        event = replay_fixture(self.row, h, publication=AT)
        observation = self.observe(h, stale=True)
        self.assertFalse(observation['same_window_comparable'])
        self.assertIsNone(observation['excess_change_percentage_points'])
        with self.assertRaises(ValueError):
            outcome_fixture(event, observation)
        self.assertIn('missing_outcome_reference', dict(self.plan([event]).excluded)[event.event_id])

    def test_unknown_entity_does_not_expand_registry(self):
        h = self.hypothesis(symbol='UNKNOWN', entity_id='new-company')
        self.assertEqual(h['status'], 'rejected')
        self.assertIn('unknown_target', h['issues'])
        self.assertNotIn('UNKNOWN', self.ledger.targets)

    def test_naive_publication_not_silently_coerced(self):
        with self.assertRaises(ValueError):
            replay_fixture(self.row, self.hypothesis(), publication=AT.replace(tzinfo=None))

    def test_same_clock_source_revisions_require_explicit_replay_policy(self):
        h = self.hypothesis()
        first = replay_fixture(self.row, h, publication=AT)
        self.payload['filings']['recent']['items'][0] = '5.02'
        row, _ = self.ingest(self.payload)
        second = replay_fixture(row, h, publication=AT, version=2)
        # SEC orders local same-clock revisions by insertion; replay requires
        # strictly increasing availability. Never invent microseconds to fit.
        with self.assertRaisesRegex(ValueError, 'strictly increase'):
            replay_index([first, second], AT)

if __name__ == '__main__':
    unittest.main()
