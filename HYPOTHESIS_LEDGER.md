# Offline hypothesis/outcome ledger

This additive module is **research scaffolding, fixture-only**. It does not generate
signals, place orders, call providers, open network connections, run workers, or
connect to the dashboard. There is no claim of alpha, causal effect, expected
profit, or live data verification. All existing 5/15/60-minute captures stay intact.

## What is recorded

`HypothesisLedger(Store(...), {'AAA': 'entity-a', 'SPY': 'benchmark'})` creates two
new SQLite tables through the existing `event_core.Store.db` interface. It reuses
`utc`, `iso` and `finite`; it does not change `event_core` or `price_reaction`.

`record` takes a stable idempotency key and records:
- exact event version, target symbol and stable entity ID;
- direct/indirect relationship tag and cited reference/excerpt with publication
  and actual knowledge times;
- direction (`up`, `down`, `flat`), one 5/15/60-minute horizon and an explicit
  forward window start, plus a **preregistered benchmark**;
- knowledge cutoff, server-side creation time, strategy version and a human-readable
  invalidation criterion;
- hypothesis, abstention or rejection, with reason required for the latter two.

Unknown targets/events, missing citations, future knowledge, evidence not known
at cutoff, and already superseded event versions are retained as rejected entries.
Malformed API inputs (invalid dates, malformed citation, invalid enum, missing
required text) raise `ValueError` before insertion. These programming errors are
not counted as research decisions. Callers must enroll every actual research
opportunity, including abstentions/rejections; the module cannot discover omitted
opportunities or promise an unbiased population.

The copied entity registry is explicit caller input, not live entity resolution.
Citations and relationship tags are not independently verified. The invalidation
text is stored, not interpreted as executable policy. Event-version corrections
are detected by source/source ID and the kernel's updated-time/row-order convention.
A correction excludes the old event's hypothesis from current scoring without
rewriting the original record or its historical observation.

## Time and revision policy

Prospective means the trusted `Store.clock` creation time is at or before the
explicit observation window start. It does **not** mean before the news event.
Late hypotheses remain visible as retrospective and never get direction scores.
Knowledge cutoff cannot exceed actual recording time; event publication/update/
receipt and evidence publication/knowledge times must support that cutoff.
Naive timestamps are rejected; aware timestamps are normalized to UTC. Clock
rollback relative to ledger writes is refused (identical-key retries still work).
This is an application audit trail, not cryptographic proof of real-time knowledge:
a caller controlling the clock/database can fabricate history.

A correction uses a new key plus `previous_id`. It appends a revision and cannot
fork or change target, horizon, window or benchmark. Only the newest revision is
used in aggregate counts; all old revisions stay visible. Revising after window
start creates a retrospective revision, not a hindsight upgrade. Creating a new
root to evade revisions cannot be prevented by this local API; callers must use
stable opportunity keys and review all roots.

Identical retry keys return the original record. Reusing a key with changed data
fails. `BEGIN IMMEDIATE` serializes duplicate/revision checks and inserts. SQL
triggers refuse UPDATE and DELETE of both ledger tables. An owner with raw DB
schema access can remove those triggers, so this is not tamper-evident storage.

## Outcome input and comparison

`observe` takes fixture samples for the target and preregistered benchmark. Each
sample has baseline/terminal points with `price`, `at`, and `known_at`; dates must
be JSON-compatible aware ISO strings. Input snapshots are retained in the database
and exposed by `audit`. Missing endpoints should be absent or null, not synthetic
prices. Inputs carry one shared source/feed description and adjustment convention;
callers must not combine different feeds/adjustments under one declaration.

Endpoints must be positive finite prices, known no later than observation time,
not timestamped after their own knowledge time, and not after the requested
endpoint. Incomplete windows, missing endpoints, unknown symbols, stale points,
invalid prices and incomplete coverage produce explicit issues and no comparative
score. A bounded tolerance (default zero, maximum 120 seconds) may retain a leg's
standalone observed change, but **only exact baseline and terminal timestamps at
the registered window boundaries are benchmark-comparable**. Tolerated stale
points carry `within_tolerance_but_stale` and endpoint lags; equally shifted windows
still cannot pass. A target cannot serve as its own benchmark. A benchmark changed
after registration cannot pass.

The module does not guess that a point is a completed bar, certify market-session
coverage, adjust corporate actions, or verify the caller's completeness assertion.
No existing `price_reaction` capture is imported automatically: its event-relative
windows and stale-baseline tolerance do not establish a prospective ledger window.
An eventual adapter must explicitly preserve raw provenance and fail closed on
unmatched endpoints, rather than recycling returns from another window.

## Counts and interpretation

`audit()` returns detached full append histories and counts over latest revisions
and latest observations **by insertion ID, never by best return**. It includes all
roots, revision count, rejected and abstained roots, prospective/retrospective
counts, unobserved, non-comparable, superseded-event, unscored, direction-match and
direction-mismatch counts. Historical positive observations remain inspectable
when a later observation corrects them downward. Counts cover the entire stored
ledger, not selected winners; no hit-rate denominator is silently reduced to rows
with complete data. Timing counts include non-hypothesis dispositions.

Direction matching uses the sign of raw target change, not excess return;
`flat` requires exactly zero, with no inferred threshold. Excess change is simply
target percentage change minus benchmark percentage change in percentage points.
It is not causal attribution, risk adjustment, realizable P&L or trading performance.
There are no costs, spreads, liquidity, significance tests or survivorship correction.

## Run and integration boundary

From the isolated workspace:

```sh
PYTHONPATH=/tmp/paper-adapter-deps python -m unittest test_hypothesis_ledger -v
PYTHONPATH=/tmp/paper-adapter-deps python -m unittest discover -v
python -m py_compile hypothesis_ledger.py test_hypothesis_ledger.py
```

The new tests use only deterministic temporary databases and synthetic prices.
They cover correction, duplicates, late hypotheses, unknown targets, future/
naive timestamps, clock rollback, missing benchmarks, unequal/equally shifted
windows, zero/negative/nonfinite prices, partial coverage, abstentions, rejections,
immutable history, benchmark preregistration and untouched price-reaction captures.

Only the new ledger module, tests, this document, base manifest and test report
belong to this deliverable. `LEDGER_BASE_SNAPSHOT.json` identifies the copied working
snapshot, with SHA-256 for each pre-existing source. Its local Git HEAD is a
materialization commit, **not a claim that this snapshot equals GitHub main**.
The parent reported remote base c889874d85e15ccc79d3e246691cbf419dc33b6c separately;
these bytes were not independently fetched/verified against that commit.
The final integration must rerun the full suite against the newest reviewed base.
No publishing, merge, deployment, credentials or live APIs are part of this work.
