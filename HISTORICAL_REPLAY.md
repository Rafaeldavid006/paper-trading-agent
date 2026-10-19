# Historical event replay, offline research scaffold

This additive helper builds a small event-library index and an auditable split plan. It does not fetch historical data, train a model, compute returns, select trades, or establish predictive power/profitability. Every test record and price-window reference is synthetic.

## Files and provenance

Only `historical_replay.py`, `test_historical_replay.py`, and this document are proposed repository additions. No changes to current application routes, workers, ledger, source adapters, storage, or deployments.

The test workspace was extracted from the existing local archive HEAD `a5fc02221627bd97e25fceda5b1b373b7b7819c1`, which is a synthetic local snapshot, NOT a remote base commit. Parent reports remote main `9c12e0497df2f9a1137f9638889b58d9262e5891` after the one-shot PR, previous deployed base `c889874d85e15ccc79d3e246691cbf419dc33b6c`. This helper has no imports from existing application modules. Full integration against a freshly pinned remote checkout remains required and is not claimed by these standalone tests.

## Small public API

- `EventVersion`: event ID, monotonically increasing integer version, explicit economic episode ID, publication and observed timestamps, fixed decision timestamp and horizon end, immutable source reference, category, tuples of related targets and evidence references. `expectations` and `regime` are optional `TimedFact` values, each with its own source and availability timestamp.
- `replay_index(versions, as_of)`: latest available version of each event at the requested point in time. Availability is the later of publication and first observation, NOT merely the earlier publication date. Returns deterministic, immutable `ReplayRecord` projections. Future optional fact values and their references are omitted, with missingness reasons retained.
- `OutcomeReference`: separate external evidence for exactly the declared return window. A reference is not a verified return measurement. It cannot be marked available before the horizon ends.
- `plan_splits(versions, as_of=..., train_end=..., validation_end=..., embargo=timedelta(...), outcomes=...)`: deterministic train/validation/holdout split planning. Each event's selected version and optional facts are frozen at that event's decision time, not the global dataset extraction date. Later corrections never become original-decision features.

All datetime inputs must have time zones. Unknown times must remain unknown upstream: do not invent midnight publication, treat SEC acceptance as verified publication, or backdate a newly fetched document. Events without defensible required timestamps cannot enter this API yet. A current fetch of an old document does not prove that the system had it historically; default conservative first-observation availability may exclude it from an old decision.

Example using synthetic data:

```python
from datetime import datetime, timedelta, timezone
from historical_replay import EventVersion, TimedFact, OutcomeReference, plan_splits

def t(day):
    return datetime(2024, 1, day, tzinfo=timezone.utc)

e = EventVersion(
    event_id="fixture-1", version=1, episode_id="fixture-episode",
    published_at=t(2), observed_at=t(2), decision_at=t(2), horizon_end=t(3),
    source_ref="fixture:original-v1", category="earnings", targets=("TEST",),
    evidence_refs=("fixture:original-v1",),
)
result = plan_splits(
    [e], as_of=t(30), train_end=t(10), validation_end=t(20),
    embargo=timedelta(days=1),
    outcomes=[OutcomeReference(e.event_id, e.horizon_end,
        TimedFact("fixture-window", "fixture:price-window", t(3)))],
)
assert result.counts["train"] == 1
```

## Leakage protections and semantics

1. Original publication and actual observed availability are preserved separately. A revision's availability must strictly increase with its version number; duplicate version identities and ambiguous same-time versions fail closed. Version numbers need not be consecutive.
2. Event episode, decision time, and horizon end are immutable across revisions. New horizon/decision hypotheses need separate sample identities and the same episode group, not overwritten timing. Curation must choose horizons and decisions before evaluating outcomes.
3. Split ranges are decisions before `train_end`, then decisions from `train_end` up to but excluding `validation_end`, then decisions at/after `validation_end`. Outcome horizons touching or crossing the following boundary are purged. This is deliberately conservative at exact equality.
4. Embargo excludes decisions in `[boundary, boundary + embargo)` after each boundary. Training and validation windows therefore cannot overlap later-split decision windows. Overlap inside one split is allowed and is NOT independent evidence.
5. All records sharing an explicit episode ID stay in one split. If an episode spans split labels, every record in that episode is excluded, even if another record already lacks outcomes. The module never guesses links from ticker or text. Entire supplied library episode assignments are audit metadata used only for purging, not predictive features; group discovery may be retrospective and should be versioned upstream.
6. A horizon must mature by `as_of`; its separate reference must also be available by `as_of` and the relevant label cutoff: `train_end` for train, `validation_end` for validation, `as_of` for holdout. Exact cutoff availability is allowed (the plan assumes fitting at that instant without execution latency). Later-known or corrected labels are excluded even when the economic horizon ended much earlier. Only one outcome reference per event is accepted; upstream must select an appropriate immutable label vintage, or the sample is conservatively excluded. Missing observations are never treated as zero returns or successful predictions. Outcome references are returned separately from feature records. The helper validates timing and identity, not reference authenticity or price quality.
7. Counts include total distinct events, events observable at their own decision, each split size, and distinct excluded events. All exclusion reasons are retained. `missingness` counts optional missing/unavailable fields among all observable-at-decision projections, including otherwise excluded records. It is not limited to retained training examples. Empty samples and tiny samples are explicit, not filled in.

## Integration boundaries and limitations

This deliberately does not duplicate `hypothesis_ledger.py` or the SEC ingestion store. Future integration can construct library records from immutable event versions and independently audited timing. SEC `accepted_at` is not necessarily publication; `first_seen_at`/snapshot `observed_at` remain indispensable. An amended filing must not be linked to another episode without explicit supported curation.

The caller is responsible for immutable content-addressed source/evidence references, correct episode IDs, publication/observed/fact availability provenance, asset identities, a survivorship-aware universe, delistings, point-in-time corporate actions, actual session calendars, timezone interpretation, expectations captured before release where relevant, regime-label provenance, realistic costs and benchmark matching. No code can infer those missing facts from these fixtures. Post-hoc episode classification is exclusion-only audit context; it is not proof of prospective availability.

The plan is not a model evaluation score or a guarantee against all leakage. It does not verify external return files, enforce independent episodes within a split, estimate effective sample size, prevent repeated holdout inspection, perform walk-forward fitting, tune hyperparameters, compute confidence intervals, or deduplicate related episodes under different IDs. Do not advertise a sample as independent merely because it survived splitting. A registry of frozen experiment choices and disciplined holdout use are still necessary.

`replay_index` is a general snapshot, not a training API: using a present-day snapshot to reconstruct past decisions would be incorrect. Use `plan_splits` for per-decision feature freezing. The library and split plan are offline diagnostic material, not ready-to-expose live feature endpoints; excluded IDs and reasons can describe future events relative to individual decisions.

## Reproducible checks

From the module directory:

```
python -m unittest test_historical_replay -v
python -m compileall -q historical_replay.py test_historical_replay.py
```

32 synthetic tests cover delayed observation, future publication, exact boundaries, revision isolation, time zones, source-backed fact availability, deterministic sorting/counts, invalid/mutable inputs, immutable timing and episode identity, overlap purging, both embargo boundaries, episode isolation even with missing outcomes, horizon maturity, future decisions, outcome timing and identity, and explicit missingness. No network or credentials are used.

Baseline validation: the initial plain `python -m unittest discover -v` lacked Flask (four import/route errors). Rerunning the final snapshot using the existing dependency directory, `PYTHONPATH=/tmp/paper-adapter-deps python -m unittest discover -v`, passed all 132 tests, including the 32 new replay tests. This is a full pass for the copied synthetic local archive, not the current remote deployment. The helper itself needs only the standard library; fresh-remote integration remains a separate check. Independent review also verified the late-label cutoff fix and the 132-test result.
