# One finite GET-only diagnostic after restart-identity migration

## What is established

The completed storage migration allows the original inspector to read the old
audit. That record says `provider_read_failed`, count 4; it does not identify the
underlying cause. Count 4 names the intended positions step after account,
clock and calendar, but increments before the account guard's reservation.
An offline real-guard reproduction with nine existing account reservations
produces that same old code/count after only three HTTP starts. Transport/read,
JSON and other local guard failures can also be flattened. None is established
as the historical cause, and the old missing schema enum cannot be reconstructed.

## Disabled candidate and startup

- The new source gate is False; both approved UTC timestamps are None.
- Both completed migration gates are False. No attempt directory, backup,
  migration marker, code pin or enrollment record is changed or reused.
- Startup performs the existing readonly completed-attempt plus full-cohort
  verification before starting the unchanged inspection-only web application.
  Cancellation during that check cannot launch children.
- The obsolete pre-migration preflight is no longer invoked by startup. Its
  `migration_already_started` is not relabeled success. The original audit-only
  inspector runs after the optional diagnostic child is reaped.
- Historical probe, schema diagnostic, research, runtime and trading workers
  remain disabled, even if old environment flags request research.
- All seven frozen migration/core files, historical collector, parser, schema
  diagnostic and original audit inspector remain byte-identical.

## Separately approved execution

Select one future regular-session UTC interval of at most ten minutes when the
code is ready, then explicitly approve publication of the exact activated source
and one collection in that interval. There is no scheduler or wait-until-open
loop. A deployment outside the interval returns without claiming or sampling.
The activation preparation utility only prints a local diff; it does not publish,
deploy, edit the candidate or call providers.

One collection permits at most 12 GET starts in total, normally 8 with one order
page and no native-owned intents. Routes remain the existing exact Alpaca paper
account, clock, same-day calendar, positions, bounded all-status order pages and
NVDA IEX latest quotes. Existing injected credentials are used only by the
existing hard GET-only HTTP transport. No POST/DELETE, new access or paid service.
The collector retains its 10-second deadline; actual HTTP inactivity timeout is
at most 2 seconds and never longer than the remaining collection time. The
separate child has the existing 15-second kill/watchdog and is never respawned.

The local interval is checked before any claim and again inside the claim
transaction immediately before its first write. It is also checked immediately
before each underlying HTTP start. It bounds request starts, not when DNS/TLS or
responses finish. Provider clock/calendar validation still determines whether
the sampled session is actually open.

A new durable claim must precede credentials and every provider GET, including
account/clock checks. Consequently setup failure, cancellation, crash, provider
closed-session response or later expiry can consume the one attempt. There is
no retry, claim deletion, reset, automatic wait, recovery or trading authorization.
An expired/no-claim invocation will not automatically retry in a later window.

## Exact durable-write scope

1. A separate `native_get_diagnostic_claim` table/row in the existing anchor:
   fixed generation, code digest, claim time, bounded diagnostic audit/result.
   Every update compares its own generation, code digest, claim time and claimed
   status. Existing claims, audit history and business tables are untouched.
2. Normal reservations, high-water updates and expired-slot pruning in BOTH
   existing quota algorithms, each still capped at 12 per 60 seconds. Neither
   quota is reset/refilled/rebound. The collector's reservation commits before
   the account guard's separate reservation, as before.
3. Ordinary temporary SQLite rollback journals for those approved writes.
   Existing journals/WAL/SHM across all eight cohort databases cause refusal;
   no journal recovery, permission changes, file creation/reinitialization of
   missing state, or source/trading/incident write is authorized.

The existing collector constructor would create/chmod state. The dedicated
adapter instead requires the already verified migrated cohort and held read-only
account guard, opens the exact existing guard DB with mode=rw, inherits the
original reserve algorithm unchanged, and rechecks integrity before/after commit.
It explicitly redirects the collector's durable audit to the new receipt. It
does not append to the historical audit because that would invalidate the old
inspector's latest-two and unique-since-claim contract.

The isolated diagnostic child scopes and restores references in the collector
module for this adapter, the JSON observer and existing parser observers. It
does not replace global json.loads, modify source files, loosen a verifier or
run in the web process. Read-only guard authority still permits these explicit
quota/diagnostic bookkeeping writes; this is not a zero-write operation.

## Safe results and limitations

Only fixed enums, bounded counters and HTTP status are emitted: failure layer,
endpoint role, request ordinal, HTTP/transport/JSON/schema classification.
No raw URL, header, body, exception text, credential, account/order identifier or
holding is logged. Endpoint role is the active/most recent request context;
aggregate parser failures are identified by their parser, not proof that this
last endpoint caused them.

`open_attempts` counts urllib opener invocations, not proven provider receipts.
Reservation counters are confirmed completions, not exact durable totals after
an uncertain commit; `quota_outcome_uncertain` marks that case. The receipt status
`recorded` stores collector evidence before final cleanup, not an independent
integrity/success authority. Later integrity failure can still refuse the CLI
result. Any existing receipt consumes the generation regardless of its status.

A new sample can differ from the historical one. Successful observations do not
prove the historical cause, reset attention, authorize a repeat or make trading
ready. Original claims, budgets' safety semantics and all trading records remain.

## Approval still required

No external change or provider request was performed while preparing this code.
The next live approval must name the exact release, one selected UTC window,
existing Alpaca paper/data GETs (maximum 12), existing credential use and the two
bookkeeping write scopes above, plus migration-gate retirement/startup cleanup.
No original claim reset, research restart, migration retry, recovery or trading.
No manual same-instance shell/file editing during the short diagnostic interval;
the startup gate and cooperative locks do not revoke or prove absence of external
same-instance writers.
