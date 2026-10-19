"""Synthetic fixtures only. No provider/market/network calls."""
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from historical_replay import EventVersion, TimedFact, OutcomeReference, replay_index, plan_splits


def day(n):
    return datetime(2024, 1, n, tzinfo=timezone.utc)


def event(name="a", n=2, **kw):
    values = dict(event_id=name, version=1, episode_id=name, published_at=day(n),
                  observed_at=day(n), decision_at=day(n), horizon_end=day(n + 1),
                  source_ref="fixture:source", category="earnings", targets=("TEST",),
                  evidence_refs=("fixture:evidence",))
    values.update(kw)
    return EventVersion(**values)


def outcome(e, **kw):
    values = dict(event_id=e.event_id, horizon_end=e.horizon_end,
                  reference=TimedFact("synthetic-window", "fixture:prices", e.horizon_end))
    values.update(kw)
    return OutcomeReference(**values)


def plan(events, **kw):
    values = dict(as_of=day(30), train_end=day(10), validation_end=day(20),
                  outcomes=tuple(outcome(e) for e in {e.event_id: e for e in events}.values()))
    values.update(kw)
    return plan_splits(events, **values)


class ReplayTests(unittest.TestCase):
    def test_unobserved_old_publication_not_available(self):
        self.assertEqual(replay_index([event(observed_at=day(9))], day(8)), ())

    def test_future_publication_not_available(self):
        self.assertEqual(replay_index([event(published_at=day(9))], day(8)), ())

    def test_exact_availability_is_inclusive(self):
        self.assertEqual(replay_index([event()], day(2))[0].version, 1)

    def test_correction_never_leaks_back(self):
        first = event()
        revised = replace(first, version=2, observed_at=day(5), category="corrected")
        self.assertEqual(replay_index([revised, first], day(4))[0].category, "earnings")
        self.assertEqual(replay_index([first, revised], day(5))[0].category, "corrected")

    def test_future_facts_redacted_not_merely_flagged(self):
        e = event(expectations=TimedFact("SECRET_FUTURE", "fixture:future", day(5)),
                  regime=TimedFact("FUTURE_REGIME", "fixture:future", day(6)))
        record = replay_index([e], day(3))[0]
        self.assertIsNone(record.expectations)
        self.assertIsNone(record.regime)
        self.assertNotIn("SECRET_FUTURE", repr(record))
        self.assertIn("expectations:not_yet_available", record.missing)

    def test_known_facts_kept_with_sources(self):
        fact = TimedFact("known", "fixture:expectation", day(1))
        self.assertEqual(replay_index([event(expectations=fact)], day(2))[0].expectations, fact)

    def test_sort_and_timezone_equivalence(self):
        cutoff = day(3).astimezone(timezone(timedelta(hours=3)))
        self.assertEqual([r.event_id for r in replay_index([event("b"), event("a")], cutoff)], ["a", "b"])

    def test_duplicate_version_rejected(self):
        with self.assertRaises(ValueError):
            replay_index([event(), event()], day(3))

    def test_reordered_version_availability_rejected(self):
        with self.assertRaises(ValueError):
            replay_index([event(), event(version=2, observed_at=day(1))], day(3))

    def test_identity_dimensions_immutable(self):
        for change in (dict(episode_id="changed"), dict(decision_at=day(1)), dict(horizon_end=day(4))):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replay_index([event(), event(version=2, observed_at=day(4), **change)], day(5))

    def test_naive_times_rejected(self):
        with self.assertRaises(ValueError):
            event(published_at=datetime(2024, 1, 2))
        with self.assertRaises(ValueError):
            replay_index([], datetime(2024, 1, 2))

    def test_bad_input_rejected(self):
        for change in (dict(version=True), dict(version=0), dict(event_id=" "),
                       dict(targets=["X"]), dict(horizon_end=day(2)), dict(regime="raw")):
            with self.subTest(change=change), self.assertRaises(ValueError):
                event(**change)


class SplitTests(unittest.TestCase):
    def test_three_temporal_splits(self):
        result = plan([event("h", 21), event("v", 11), event("t", 2)])
        self.assertEqual([r.event_id for r in result.train], ["t"])
        self.assertEqual([r.event_id for r in result.validation], ["v"])
        self.assertEqual([r.event_id for r in result.holdout], ["h"])
        self.assertEqual(result.counts["excluded"], 0)
        self.assertEqual(result.counts["total_events"], 3)

    def test_corrected_features_frozen_at_decision(self):
        original = event()
        corrected = replace(original, version=2, observed_at=day(5), category="revised")
        self.assertEqual(plan([corrected, original]).train[0].category, "earnings")

    def test_revision_available_at_decision_is_used(self):
        original = event(decision_at=day(4), horizon_end=day(5))
        corrected = replace(original, version=2, observed_at=day(4), category="revised")
        self.assertEqual(plan([corrected, original]).train[0].version, 2)

    def test_late_observed_event_not_backdated(self):
        result = plan([event(observed_at=day(5))])
        self.assertEqual(result.train, ())
        self.assertIn("unavailable_at_decision", dict(result.excluded)["a"])

    def test_horizon_touching_boundary_is_purged(self):
        for n in (9, 19):
            result = plan([event(n=n)])
            self.assertIn("horizon_crosses_boundary", dict(result.excluded)["a"])

    def test_embargo_at_each_boundary(self):
        events = [event("a", 10), event("b", 11), event("c", 12), event("d", 20), event("e", 22)]
        result = plan(events, embargo=timedelta(days=2))
        self.assertEqual([r.event_id for r in result.validation], ["c"])
        self.assertEqual([r.event_id for r in result.holdout], ["e"])
        self.assertEqual(set(dict(result.excluded)), {"a", "b", "d"})

    def test_whole_episode_purged_across_splits_even_if_one_missing(self):
        a, b = event("a", 2, episode_id="shared"), event("b", 12, episode_id="shared")
        result = plan([a, b], outcomes=[outcome(a)])
        self.assertEqual(result.train, ())
        self.assertEqual(result.validation, ())
        for reasons in dict(result.excluded).values():
            self.assertIn("episode_spans_splits", reasons)

    def test_episode_inside_one_split_kept(self):
        self.assertEqual(len(plan([event("a", 2, episode_id="same"), event("b", 4, episode_id="same")]).train), 2)

    def test_immature_horizon_excluded(self):
        self.assertIn("immature_horizon", dict(plan([event(n=29, horizon_end=day(31))]).excluded)["a"])

    def test_future_decision_excluded(self):
        self.assertIn("future_decision", dict(plan([event(n=29)], as_of=day(25)).excluded)["a"])

    def test_missing_outcomes_not_counted_as_zero_returns(self):
        result = plan([event()], outcomes=[])
        self.assertEqual(result.train, ())
        self.assertIn("missing_outcome_reference", dict(result.excluded)["a"])

    def test_future_outcome_reference_not_used(self):
        e = event()
        r = outcome(e, reference=TimedFact("late", "fixture:return", day(31)))
        result = plan([e], outcomes=[r])
        self.assertIn("outcome_not_yet_available", dict(result.excluded)["a"])
        self.assertEqual(result.outcome_references, ())

    def test_outcome_reference_does_not_enter_features(self):
        result = plan([event()])
        self.assertNotIn("synthetic-window", repr(result.train))
        self.assertEqual(len(result.outcome_references), 1)

    def test_future_fact_cutoff_is_decision_not_dataset_asof(self):
        e = event(regime=TimedFact("post-hoc", "fixture:regime", day(3)))
        result = plan([e])
        self.assertIsNone(result.train[0].regime)
        self.assertEqual(result.missingness["regime:not_yet_available"], 1)

    def test_accounting_and_determinism(self):
        events = [event("z", 2), event("v", 11), event("b", 10), event("h", 21)]
        a = plan(events, embargo=timedelta(days=1))
        b = plan(list(reversed(events)), embargo=timedelta(days=1))
        self.assertEqual(a, b)
        self.assertEqual(sum(a.counts[k] for k in ("train", "validation", "holdout", "excluded")), a.counts["total_events"])

    def test_bad_split_parameters_rejected(self):
        for args in (dict(train_end=day(21)), dict(as_of=day(19)), dict(embargo=timedelta(days=-1)), dict(embargo=3)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                plan([], **args)

    def test_bad_outcome_references_rejected(self):
        e = event()
        for refs in ([outcome(e), outcome(e)], [outcome(event("unknown"))],
                     [outcome(e, horizon_end=day(4), reference=TimedFact("x", "fixture:x", day(5)))]):
            with self.subTest(refs=refs), self.assertRaises(ValueError):
                plan([e], outcomes=refs)
        with self.assertRaises(ValueError):
            outcome(e, reference=TimedFact("x", "fixture:x", day(2)))

    def test_late_training_and_validation_labels_are_purged(self):
        for n, cutoff in ((2, 10), (11, 20)):
            e = event(n=n)
            late = outcome(e, reference=TimedFact("late-corrected-label", "fixture:late", day(25)))
            result = plan([e], outcomes=[late])
            self.assertIn("outcome_unavailable_at_split_cutoff", dict(result.excluded)["a"])
            self.assertEqual(result.train + result.validation, ())

    def test_outcome_available_exactly_at_split_cutoff_is_allowed(self):
        for n, cutoff, label in ((2, 10, "train"), (11, 20, "validation"), (21, 30, "holdout")):
            e = event(n=n)
            ref = outcome(e, reference=TimedFact("at-cutoff", "fixture:return", day(cutoff)))
            result = plan([e], outcomes=[ref])
            self.assertEqual(len(getattr(result, label)), 1)

    def test_empty_library(self):
        result = plan([])
        self.assertEqual(result.counts["total_events"], 0)
        self.assertEqual(result.train, ())


if __name__ == "__main__":
    unittest.main()
