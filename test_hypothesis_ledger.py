"""Deterministic offline fixtures; no strategy or return claims."""
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from event_core import Store, iso
from hypothesis_ledger import HypothesisLedger
from price_reaction import Reactions

AT = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = AT
        self.store = Store(self.tmp.name + '/audit.db', lambda: self.now)
        self.event = self.store.ingest(source='fixture', source_id='event-1', symbol='AAA',
            published_at=AT, payload={'actual': 10}, url='https://example.invalid/event')[0]
        self.ledger = HypothesisLedger(self.store, {'AAA': 'entity-a', 'BBB': 'entity-b', 'SPY': 'benchmark'})
        self.args = dict(key='h1', event_version=self.event, symbol='AAA', entity_id='entity-a',
            relation='direct', evidence=[dict(reference='fixture:event-1', excerpt='Synthetic evidence only',
            published_at=iso(AT), known_at=iso(AT))], direction='up', horizon_minutes=5,
            window_start=AT, knowledge_cutoff=AT, invalidation='A source correction invalidates this hypothesis',
            strategy_version='fixture-v1', benchmark_symbol='SPY')

    def tearDown(self):
        self.tmp.cleanup()

    def record(self, **changes):
        return self.ledger.record(**dict(self.args, **changes))

    def samples(self, terminal=110):
        def points(price):
            return {'baseline': {'price': 100, 'at': iso(AT), 'known_at': iso(AT)},
                    'terminal': {'price': price, 'at': iso(AT+timedelta(minutes=5)),
                                 'known_at': iso(AT+timedelta(minutes=5))}}
        return {'AAA': points(terminal), 'SPY': points(101)}

    def observe(self, h, **changes):
        self.now = max(self.now, AT+timedelta(minutes=6))
        args = dict(key='o1', hypothesis_id=h['id'], benchmark_symbol='SPY', samples=self.samples(),
                    coverage_complete=True, source='synthetic-fixture')
        return self.ledger.observe(**dict(args, **changes))

    def test_forward_fixture_and_full_audit(self):
        h = self.record()
        o = self.observe(h)
        self.assertEqual(h['timing'], 'prospective')
        self.assertTrue(o['same_window_comparable'])
        self.assertAlmostEqual(o['excess_change_percentage_points'], 9)
        self.assertTrue(o['direction_match'])
        audit = self.ledger.audit()
        self.assertEqual(audit['counts']['direction_matches'], 1)
        self.assertFalse(audit['causal_effect_proven'])
        self.assertFalse(o['causal_effect_proven'])
        self.assertEqual(o['order_permission'], 'none')

    def test_duplicate_retry_and_conflicting_key(self):
        h = self.record()
        self.now += timedelta(seconds=1)
        self.assertEqual(self.record(), h)
        with self.assertRaises(ValueError):
            self.record(direction='down')
        o = self.observe(h)
        self.assertEqual(self.observe(h), o)
        with self.assertRaises(ValueError):
            self.observe(h, samples=self.samples(99))
        self.assertEqual(self.ledger.audit()['counts']['entries'], 1)

    def test_immutable_tables(self):
        h = self.record()
        self.observe(h)
        for table in ('hypothesis_entries', 'hypothesis_outcomes'):
            for sql in (f'DELETE FROM {table}', f'UPDATE {table} SET record=record'):
                with self.assertRaises(sqlite3.IntegrityError):
                    with self.store.db() as c:
                        c.execute(sql)

    def test_late_hypothesis_not_scored(self):
        self.now += timedelta(minutes=1)
        h = self.record()
        self.assertEqual(h['timing'], 'retrospective')
        self.assertIsNone(self.observe(h)['direction_match'])
        self.assertEqual(self.ledger.audit()['counts']['unscored'], 1)

    def test_revision_is_append_only_and_no_hindsight_upgrade(self):
        h = self.record()
        self.observe(h)
        revised = self.record(key='h2', previous_id=h['id'], direction='down')
        self.assertEqual(revised['timing'], 'retrospective')
        self.assertIsNone(self.observe(revised, key='o2')['direction_match'])
        counts = self.ledger.audit()['counts']
        self.assertEqual((counts['roots'], counts['revisions'], counts['unscored']), (1, 1, 1))
        self.assertEqual(counts['direction_matches'], 0)
        with self.assertRaises(ValueError):
            self.record(key='fork', previous_id=h['id'])
        with self.assertRaises(ValueError):
            self.record(key='new-window', previous_id=revised['id'], window_start=self.now)

    def test_event_correction_invalidates_scoring_without_erasure(self):
        h = self.record()
        original = self.observe(h)
        self.store.ingest(source='fixture', source_id='event-1', symbol='AAA',
            published_at=AT, updated_at=self.now, payload={'actual': 1}, url='https://example.invalid/event')
        self.assertEqual(self.ledger.audit()['outcomes'][0]['direction_match'], original['direction_match'])
        self.assertEqual(self.ledger.audit()['counts']['direction_matches'], 0)
        self.assertEqual(self.ledger.audit()['counts']['superseded_event'], 1)
        self.assertIsNone(self.observe(h, key='o2')['direction_match'])
        self.assertIn('superseded_event_version', self.record(key='h2')['issues'])

    def test_unknown_target_and_event_are_retained_rejections(self):
        for key, changes, issue in [('unknown-symbol', {'symbol': 'XYZ'}, 'unknown_target'),
                ('wrong-entity', {'entity_id': 'other'}, 'unknown_target'),
                ('unknown-event', {'event_version': 'absent'}, 'unknown_event_version')]:
            h = self.record(key=key, **changes)
            self.assertEqual(h['status'], 'rejected')
            self.assertIn(issue, h['issues'])
        self.assertEqual(self.ledger.audit()['counts']['rejected'], 3)

    def test_future_cutoff_and_evidence_rejected(self):
        h = self.record(knowledge_cutoff=AT+timedelta(seconds=1))
        self.assertIn('future_knowledge_cutoff', h['issues'])
        citations = [dict(self.args['evidence'][0], known_at=iso(AT+timedelta(seconds=1)))]
        self.assertIn('evidence_not_known_at_cutoff', self.record(key='h2', evidence=citations)['issues'])

    def test_event_not_known_and_future_event_rejected(self):
        event = self.store.ingest(source='fixture', source_id='future', symbol='AAA',
            published_at=AT+timedelta(minutes=1), payload={}, url='https://example.invalid/future')[0]
        h = self.record(event_version=event)
        self.assertIn('event_not_known_at_cutoff', h['issues'])
        self.assertIn('window_before_event', h['issues'])

    def test_naive_times_rejected(self):
        with self.assertRaises(ValueError):
            self.record(window_start=AT.replace(tzinfo=None))
        with self.assertRaises(ValueError):
            self.record(knowledge_cutoff='2026-10-18T12:00:00')

    def test_clock_rollback_rejected_but_idempotent_retry_allowed(self):
        h = self.record()
        self.now -= timedelta(seconds=1)
        self.assertEqual(self.record(), h)
        with self.assertRaises(ValueError):
            self.record(key='clock-backwards')

    def test_missing_benchmark_never_invented(self):
        h = self.record()
        o = self.observe(h, samples={'AAA': self.samples()['AAA']})
        self.assertEqual(o['legs']['target']['status'], 'observed')
        self.assertIn('missing_terminal', o['legs']['benchmark']['issues'])
        self.assertFalse(o['same_window_comparable'])
        self.assertIsNone(o['excess_change_percentage_points'])
        self.assertIsNone(o['direction_match'])

    def test_partial_coverage_not_counted(self):
        h = self.record()
        o = self.observe(h, coverage_complete=False)
        self.assertIn('coverage_incomplete', o['legs']['target']['issues'])
        self.assertEqual(self.ledger.audit()['counts']['not_comparable'], 1)

    def test_unequal_and_equally_shifted_windows_not_comparable(self):
        h = self.record()
        samples = self.samples()
        samples['SPY']['terminal']['at'] = iso(AT+timedelta(minutes=4))
        o = self.observe(h, samples=samples, max_staleness_seconds=120)
        self.assertEqual(o['legs']['benchmark']['status'], 'observed')
        self.assertFalse(o['same_window_comparable'])
        samples['AAA']['terminal']['at'] = samples['SPY']['terminal']['at']
        self.assertFalse(self.observe(h, key='o2', samples=samples, max_staleness_seconds=120)['same_window_comparable'])

    def test_stale_future_and_after_window_samples(self):
        h = self.record()
        for i, (at, issue) in enumerate([(AT+timedelta(minutes=4), 'stale_terminal'),
                (AT+timedelta(minutes=7), 'future_or_invalid_timestamp'),
                (AT+timedelta(minutes=6), 'endpoint_after_window')]):
            samples = self.samples()
            samples['AAA']['terminal'].update(at=iso(at), known_at=iso(at))
            o = self.observe(h, key=str(i), samples=samples)
            self.assertIn(issue, o['legs']['target']['issues'])
            self.assertIsNone(o['direction_match'])

    def test_nonpositive_boolean_and_invalid_prices(self):
        h = self.record()
        for i, value in enumerate((0, -1, True, 'NaN', 'Infinity', 'not-a-price')):
            o = self.observe(h, key=str(i), samples=self.samples(value))
            self.assertIn('invalid_terminal', o['legs']['target']['issues'])
            self.assertIsNone(o['excess_change_percentage_points'])

    def test_nonfinite_json_refused(self):
        h = self.record()
        for value in (float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                self.observe(h, samples=self.samples(value))

    def test_incomplete_window(self):
        h = self.record()
        self.now = AT+timedelta(minutes=2)
        o = self.ledger.observe(key='early', hypothesis_id=h['id'], benchmark_symbol='SPY', samples={},
                                coverage_complete=False, source='fixture')
        self.assertIn('window_incomplete', o['legs']['target']['issues'])

    def test_all_roots_include_abstentions_rejections_unobserved_and_losses(self):
        h = self.record()
        self.record(key='abstain', disposition='abstained', reason='Insufficient context')
        self.record(key='reject', disposition='rejected', reason='Outside fixture scope')
        self.record(key='unobserved')
        self.observe(h, samples=self.samples(90))
        counts = self.ledger.audit()['counts']
        self.assertEqual((counts['roots'], counts['abstained'], counts['rejected']), (4, 1, 1))
        self.assertEqual((counts['unobserved'], counts['direction_mismatches'], counts['unscored']), (3, 1, 3))

    def test_new_observations_do_not_choose_best(self):
        h = self.record()
        self.observe(h)
        self.observe(h, key='corrected', samples=self.samples(90))
        audit = self.ledger.audit()
        self.assertEqual(len(audit['outcomes']), 2)
        self.assertEqual(audit['counts']['direction_matches'], 0)
        self.assertEqual(audit['counts']['direction_mismatches'], 1)

    def test_direct_indirect_and_directions(self):
        for n, direction in enumerate(('up', 'down', 'flat')):
            h = self.record(key='h'+str(n), direction=direction, relation='indirect', symbol='BBB', entity_id='entity-b')
            self.assertEqual(h['relation'], 'indirect')
            self.assertFalse(h['evidence_verified'])

    def test_horizons_and_input_validation(self):
        for horizon in (5, 15, 60):
            self.assertEqual(self.record(key=str(horizon), horizon_minutes=horizon)['horizon_minutes'], horizon)
        for changes in ({'horizon_minutes': True}, {'horizon_minutes': 10}, {'direction': 'buy'},
                        {'relation': 'causal'}, {'invalidation': ''}, {'disposition': 'abstained'}):
            with self.assertRaises(ValueError):
                self.record(**changes)

    def test_no_existing_reaction_changes(self):
        reactions = Reactions(self.store)
        with self.store.db() as c:
            c.execute('INSERT INTO price_reactions(event_version,report) VALUES (?,?)', (self.event, '{"original":true}'))
            before = [tuple(r) for r in c.execute('SELECT * FROM price_reactions')]
        self.observe(self.record())
        with self.store.db() as c:
            self.assertEqual(before, [tuple(r) for r in c.execute('SELECT * FROM price_reactions')])

    def test_audit_is_detached(self):
        h = self.record()
        audit = self.ledger.audit()
        audit['entries'][0]['evidence'][0]['excerpt'] = 'modified'
        self.assertNotEqual(self.ledger.audit()['entries'][0]['evidence'][0]['excerpt'], 'modified')
        self.assertEqual(self.record(), h)

    def test_benchmark_is_registered_before_observation(self):
        h = self.record()
        o = self.observe(h, benchmark_symbol='BBB')
        self.assertFalse(o['benchmark_matches_registration'])
        self.assertIsNone(o['direction_match'])
        with self.assertRaises(ValueError):
            self.record(key='swap-benchmark', previous_id=h['id'], benchmark_symbol='BBB')
        self.assertIn('invalid_benchmark', self.record(key='unknown-benchmark', benchmark_symbol='UNKNOWN')['issues'])

    def test_same_symbol_cannot_be_own_benchmark(self):
        self.assertFalse(self.observe(self.record(), benchmark_symbol='AAA')['same_window_comparable'])


if __name__ == '__main__':
    unittest.main()
