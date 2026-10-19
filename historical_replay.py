"""Offline point-in-time event selection and purged, episode-grouped split planning.

No network, database, price calculation, model fitting, prediction, or order access.
All timestamps must be timezone-aware; supplied availability is an audited input,
not something this module can independently prove.
"""
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional


def _time(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timezone-aware datetime required")
    return value.astimezone(timezone.utc)


def _text(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Nonempty text required")


@dataclass(frozen=True)
class TimedFact:
    value: str
    source_ref: str
    available_at: datetime

    def __post_init__(self):
        _text(self.value)
        _text(self.source_ref)
        _time(self.available_at)


@dataclass(frozen=True)
class EventVersion:
    event_id: str
    version: int
    episode_id: str
    published_at: datetime
    observed_at: datetime
    decision_at: datetime
    horizon_end: datetime
    source_ref: str
    category: str
    targets: tuple = ()
    evidence_refs: tuple = ()
    expectations: Optional[TimedFact] = None
    regime: Optional[TimedFact] = None

    @property
    def available_at(self):
        # Conservative policy: never infer historical availability from publication.
        return max(_time(self.published_at), _time(self.observed_at))

    def __post_init__(self):
        for value in (self.event_id, self.episode_id, self.source_ref, self.category):
            _text(value)
        if type(self.version) is not int or self.version < 1:
            raise ValueError("Positive integer version required")
        for value in (self.published_at, self.observed_at, self.decision_at, self.horizon_end):
            _time(value)
        if self.horizon_end <= self.decision_at:
            raise ValueError("Horizon must end after the decision")
        for values in (self.targets, self.evidence_refs):
            if not isinstance(values, tuple):
                raise ValueError("Immutable tuple required")
            for value in values:
                _text(value)
        for fact in (self.expectations, self.regime):
            if fact is not None and not isinstance(fact, TimedFact):
                raise ValueError("TimedFact required")


@dataclass(frozen=True)
class ReplayRecord:
    """Feature-safe projection; unavailable fact values are not returned."""
    event_id: str
    version: int
    episode_id: str
    published_at: datetime
    observed_at: datetime
    available_at: datetime
    decision_at: datetime
    horizon_end: datetime
    source_ref: str
    category: str
    targets: tuple
    evidence_refs: tuple
    expectations: Optional[TimedFact]
    regime: Optional[TimedFact]
    missing: tuple


def _library(versions):
    by_event = defaultdict(list)
    for item in versions:
        if not isinstance(item, EventVersion):
            raise ValueError("EventVersion required")
        by_event[item.event_id].append(item)
    for items in by_event.values():
        items.sort(key=lambda v: v.version)
        if len({v.version for v in items}) != len(items):
            raise ValueError("Duplicate event/version identity")
        first = items[0]
        for i, item in enumerate(items):
            if (item.episode_id, item.decision_at, item.horizon_end) != (
                first.episode_id, first.decision_at, first.horizon_end
            ):
                raise ValueError("Episode, decision and horizon are immutable across versions")
            if i and item.available_at <= items[i - 1].available_at:
                raise ValueError("Version availability must strictly increase")
    return by_event


def _project(item, cutoff):
    facts, missing = {}, []
    for name in ("expectations", "regime"):
        fact = getattr(item, name)
        if fact is None:
            missing.append(name + ":absent")
            facts[name] = None
        elif fact.available_at > cutoff:
            missing.append(name + ":not_yet_available")
            facts[name] = None
        else:
            facts[name] = fact
    for name in ("targets", "evidence_refs"):
        if not getattr(item, name):
            missing.append(name + ":absent")
    return ReplayRecord(**{name: getattr(item, name) for name in (
        "event_id", "version", "episode_id", "published_at", "observed_at",
        "available_at", "decision_at", "horizon_end", "source_ref", "category",
        "targets", "evidence_refs")}, **facts, missing=tuple(missing))


def replay_index(versions, as_of):
    """Latest observable version per event at one cutoff; sorted deterministically.

    This is a research snapshot, not a training dataset. Use plan_splits to freeze
    each sample's features at its own decision time rather than today's cutoff.
    """
    cutoff = _time(as_of)
    result = []
    for _, items in sorted(_library(versions).items()):
        available = [v for v in items if v.available_at <= cutoff]
        if available:
            result.append(_project(available[-1], cutoff))
    return tuple(result)


@dataclass(frozen=True)
class OutcomeReference:
    """External return-window evidence, never mixed into the feature projection."""
    event_id: str
    horizon_end: datetime
    reference: TimedFact

    def __post_init__(self):
        _text(self.event_id)
        _time(self.horizon_end)
        if not isinstance(self.reference, TimedFact):
            raise ValueError("TimedFact required")
        if self.reference.available_at < self.horizon_end:
            raise ValueError("Outcome evidence cannot predate the window end")


@dataclass(frozen=True)
class SplitPlan:
    train: tuple
    validation: tuple
    holdout: tuple
    excluded: tuple
    counts: dict
    missingness: dict
    outcome_references: tuple


def plan_splits(versions, *, as_of, train_end, validation_end,
                embargo=timedelta(0), outcomes=()):
    """Expanding calendar splits; all boundary times are inclusive to the right.

    Excludes immature/missing outcomes, windows touching the next split, embargo
    windows, and whole episodes spanning split labels. Features are frozen at
    decision_at. Outcome refs remain in a separate audit-only collection.
    """
    as_of, train_end, validation_end = map(_time, (as_of, train_end, validation_end))
    if not train_end < validation_end <= as_of:
        raise ValueError("Require train_end < validation_end <= as_of")
    if not isinstance(embargo, timedelta) or embargo < timedelta(0):
        raise ValueError("Nonnegative timedelta embargo required")
    library = _library(versions)
    refs = {}
    for ref in outcomes:
        if not isinstance(ref, OutcomeReference) or ref.event_id in refs:
            raise ValueError("Unique OutcomeReference per event required")
        if ref.event_id not in library:
            raise ValueError("Unknown outcome event")
        if ref.horizon_end != library[ref.event_id][0].horizon_end:
            raise ValueError("Outcome horizon mismatch")
        refs[ref.event_id] = ref
    labels, projected, reasons = {}, {}, defaultdict(set)
    episodes = defaultdict(list)
    for event_id, items in sorted(library.items()):
        anchor = items[0]
        decision = anchor.decision_at
        label = "train" if decision < train_end else (
            "validation" if decision < validation_end else "holdout")
        labels[event_id] = label
        episodes[anchor.episode_id].append(event_id)
        available = [v for v in items if v.available_at <= min(decision, as_of)]
        if not available:
            reasons[event_id].add("unavailable_at_decision")
        else:
            projected[event_id] = _project(available[-1], decision)
        if decision > as_of:
            reasons[event_id].add("future_decision")
        if anchor.horizon_end > as_of:
            reasons[event_id].add("immature_horizon")
        if label == "train" and anchor.horizon_end >= train_end:
            reasons[event_id].add("horizon_crosses_boundary")
        if label == "validation" and anchor.horizon_end >= validation_end:
            reasons[event_id].add("horizon_crosses_boundary")
        if any(boundary <= decision < boundary + embargo for boundary in (train_end, validation_end)):
            reasons[event_id].add("embargo")
        ref = refs.get(event_id)
        if ref is None:
            reasons[event_id].add("missing_outcome_reference")
        else:
            label_cutoff = {"train": train_end, "validation": validation_end, "holdout": as_of}[label]
            if ref.reference.available_at > as_of:
                reasons[event_id].add("outcome_not_yet_available")
            if ref.reference.available_at > label_cutoff:
                reasons[event_id].add("outcome_unavailable_at_split_cutoff")
    for ids in episodes.values():
        if len({labels[event_id] for event_id in ids}) > 1:
            for event_id in ids:
                reasons[event_id].add("episode_spans_splits")
    splits = {name: [] for name in ("train", "validation", "holdout")}
    missing = Counter()
    for event_id, record in sorted(projected.items()):
        missing.update(record.missing)
        if not reasons[event_id]:
            splits[labels[event_id]].append(record)
    for records in splits.values():
        records.sort(key=lambda r: (r.decision_at, r.event_id))
    excluded = tuple((event_id, tuple(sorted(why))) for event_id, why in sorted(reasons.items()) if why)
    counts = {key: len(value) for key, value in splits.items()}
    counts.update(total_events=len(library), observable_at_decision=len(projected), excluded=len(excluded))
    used_refs = tuple(refs[r.event_id] for name in splits for r in splits[name])
    return SplitPlan(**{name: tuple(records) for name, records in splits.items()},
                     excluded=excluded, counts=counts, missingness=dict(sorted(missing.items())),
                     outcome_references=used_refs)
