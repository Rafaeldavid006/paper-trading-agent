"""Seeded adversarial checks. Counts are scenarios, not independent market evidence."""
import copy
from dataclasses import replace
from datetime import timedelta
import random
import unittest

from event_core import iso
from historical_replay import EventVersion, TimedFact, OutcomeReference, replay_index, plan_splits
from sec_submissions import SourceError
import test_research_integration as integration
AT = integration.AT


class CompositionProperties(unittest.TestCase):
    def fixture(self):
        fixture = integration.ResearchIntegrationTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def test_seeded_revision_projection_and_idempotency_100_scenarios(self):
        rng = random.Random(410219)
        f = self.fixture()
        rows = [f.row]
        for index in range(100):
            with self.subTest(scenario=index):
                f.now += timedelta(seconds=rng.choice([0, 1, 60]))
                payload = copy.deepcopy(f.payload)
                payload['filings']['recent']['items'][0] = 'synthetic-revision-' + str(index)
                # Old SEC timestamps may regress; local insertion is the revision order.
                payload['filings']['recent']['acceptanceDateTime'][0] = iso(AT-timedelta(days=rng.randint(1, 20)))
                row, event = f.ingest(payload)
                rows.append(row)
                repeated, repeated_event = f.ingest(payload)
                self.assertEqual((repeated['version_id'], repeated_event), (row['version_id'], event))
                with self.assertRaises(SourceError):
                    f.sec.project(rng.choice(rows[:-1])['version_id'], 'TEST')
                with f.sec.store.db() as db:
                    stored = db.execute('SELECT first_seen_at FROM sec_records WHERE version_id=?', (f.row['version_id'],)).fetchone()[0]
                self.assertEqual(stored, f.row['first_seen_at'])

    def test_seeded_ledger_retry_and_future_knowledge_100_scenarios(self):
        rng = random.Random(410220)
        f = self.fixture()
        for index in range(100):
            with self.subTest(scenario=index):
                f.now = AT + timedelta(seconds=index)
                cutoff = f.now + timedelta(seconds=rng.randint(-2, 2))
                args = dict(key='generated-' + str(index), knowledge_cutoff=cutoff,
                            window_start=f.now, direction=rng.choice(['up', 'down', 'flat']))
                h = f.hypothesis(**args)
                self.assertEqual(f.hypothesis(**args), h)
                changed = dict(args, direction='down' if args['direction'] == 'up' else 'up')
                with self.assertRaises(ValueError):
                    f.hypothesis(**changed)
                self.assertEqual('future_knowledge_cutoff' in h['issues'], cutoff > f.now)
                if cutoff < AT:
                    self.assertIn('event_not_known_at_cutoff', h['issues'])
                self.assertEqual(h['order_permission'], 'none')
        self.assertEqual(f.ledger.audit()['counts']['entries'], 100)

    def test_seeded_replay_group_embargo_and_label_invariants_200_scenarios(self):
        rng = random.Random(410221)
        train_end, validation_end, as_of = [AT+timedelta(minutes=n) for n in (100, 200, 300)]
        for scenario in range(200):
            with self.subTest(scenario=scenario):
                versions, outcomes = [], []
                embargo = timedelta(minutes=rng.randint(0, 8))
                for index in range(rng.randint(3, 12)):
                    decision = AT+timedelta(minutes=rng.randint(0, 290))
                    observed = decision+timedelta(minutes=rng.randint(-5, 5))
                    horizon = decision+timedelta(minutes=rng.randint(1, 20))
                    event = EventVersion(str(index), 1, 'episode-'+str(rng.randint(0, 4)),
                        min(observed, decision), observed, decision, horizon, 'fixture:'+str(index),
                        'synthetic', ('TEST',), ('fixture:evidence',),
                        regime=TimedFact('synthetic regime', 'fixture:regime', decision+timedelta(minutes=rng.randint(-5, 5))))
                    versions.append(event)
                    if rng.random() < .8:
                        outcomes.append(OutcomeReference(event.event_id, horizon,
                            TimedFact('synthetic outcome', 'fixture:outcome', horizon+timedelta(minutes=rng.randint(0, 140)))))
                args = dict(as_of=as_of, train_end=train_end, validation_end=validation_end,
                            embargo=embargo, outcomes=outcomes)
                plan = plan_splits(versions, **args)
                rng.shuffle(versions)
                self.assertEqual(plan, plan_splits(versions, **args))
                refs = {r.event_id:r for r in outcomes}
                labels = {}
                for label, end in [('train', train_end), ('validation', validation_end), ('holdout', as_of)]:
                    for record in getattr(plan, label):
                        self.assertLessEqual(record.available_at, record.decision_at)
                        self.assertLessEqual(refs[record.event_id].reference.available_at, end)
                        self.assertLessEqual(record.horizon_end, as_of)
                        if label != 'holdout': self.assertLess(record.horizon_end, end)
                        if record.regime: self.assertLessEqual(record.regime.available_at, record.decision_at)
                        self.assertFalse(any(b <= record.decision_at < b+embargo for b in (train_end, validation_end)))
                        self.assertIn(labels.setdefault(record.episode_id, label), [label])
                self.assertEqual(sum(plan.counts[k] for k in ('train','validation','holdout','excluded')), len(versions))
                self.assertTrue(all(r.available_at <= as_of for r in replay_index(versions, as_of)))

if __name__ == '__main__':
    unittest.main()
