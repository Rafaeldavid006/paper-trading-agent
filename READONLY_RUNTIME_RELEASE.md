# Continuous read-only runtime: bounded deployment candidate

This release closes the runtime programming gaps from NATIVE_PAPER_EXECUTION.md.
The native PAPER write adapter remains disabled. This release permits observations
and local decision previews only; it cannot place, cancel or claim an order.
It does not change the strategy to create trading activity.

## Exact deployment effect

The existing service startup now creates a separate read-only supervisor only
after its existing legacy renewal process exits successfully. The old probe's
worker, collector, schema, supervisor, generation and durable claim remain intact.
No probe is reset. No environment variable, key, service, subscription, feed or
research/model setting is added or changed. The existing research worker remains
unchanged and receives no events from this new private source ledger.

The new supervisor runs one child at a time, with no shell or argument overrides:

- --initialize: independent 15-second hard process deadline; no network or key read.
- --source: starts at least 20 seconds apart; independent 25-second hard deadline.
- --broker: starts at least 65 seconds apart; independent 15-second hard deadline.

Independent timers enforce deadlines even while the web/service loop is blocked.
A process failure, uncertainty or timeout halts that supervisor instance without
retry. Each data stage commits the runner dirty marker before any GET. Interrupted
stages remain latched across restarts. A later restart may schedule a NEW bounded
read only after a previously completed stage was durably clean; no old fixed-probe
attempt, order or uncertain stage is replayed. Shutdown kills the current child
and bounds its reap wait. Worker stdout contains only fixed sanitized summaries;
stdin/stderr are discarded and no raw exception/account/order/credential is emitted.

## One-time state enrollment, without server shell access

The initializer consumes the EXISTING nonsecret EVENT_DB locally, exactly from the
server environment. There is no guessed default and no new configuration. It must
be an existing safe absolute application database beneath /var/data with the
expected EventStore schema and an immutable historical event received before
2026-10-20T07:02:56Z. Missing/unsafe/empty/newly-created application storage refuses.
The legacy account binding and existing experiment lock must also already exist.

The initializer adds only its own native_runtime_enrollment table to the existing
application database. It commits a fixed-generation pending marker before creating
new state. It pins the original historical event's version/digest and application
file identity; no raw event payload or raw financial account ID is copied into the
marker. The exact configured application path becomes part of permanent binding.

It creates the following NEW owner-only 0600 files, without chmodding existing ones:

- /var/data/continuous-primary-source.sqlite
- /var/data/continuous-native-lifecycle.sqlite
- /var/data/native-account-guard.sqlite
- /var/data/continuous-demo-decisions.sqlite
- /var/data/continuous-demo-runner.sqlite
- /var/data/continuous-demo-generation.json
- /var/data/continuous-demo-state.sqlite

Complete schemas, account binding, file identities, header generation stamps,
manifest/seal cross-pins and initialized state are checked before every constructor
and stage. Any pending marker, missing/replaced/partial/corrupt cohort, altered
historical anchor, missing control row or lost budget refuses WITHOUT recreation.
The existing legacy account database, its intent history, probe database and old
lock are not modified. First enrollment waits for the old worker's successful exit
so its transient flock cannot unnecessarily poison the pending enrollment.

This detects loss and replacement, not malicious coherent rollback of every local
anchor and dataset. Complete storage loss lacks the required old account binding
and old application history, so it cannot auto-enroll. A whole consistent backup
rollback or hostile same-user rewrite still needs an independent external anchor
for stronger protection; no false anti-rollback claim is made.

## Hard no-order boundary

RuntimeReadOnlyHTTP rejects every method except GET, and rejects any body, before
its underlying fixed PAPER/IEX transport. Its default is disabled. The runtime's
NativePaperDispatcher is separately constructed with enabled=False. The read-only
path calls observer.collect() under a read_only account guard and never calls
claim, submit, cancel, expire or exit. Tests patch all those methods to fail and
still complete a valid signal preview. No configuration or CLI write mode exists.

Source collection uses only the existing pinned NVIDIA RSS/original-release GETs.
Broker observations reuse the complete native account/legacy reconciliation, then
validate IEX NVDA+SPY bars and NVDA asset metadata. All new-path broker/data GETs
share the existing 12-attempt/60-second durable budget, including failed attempts;
there is no retry, pagination bypass, SIP fallback or paid/model call. Public source
collection retains its separate bounded five-GET/collection maximum.

A closed market is a successful waiting result, not a code error: closed_session
reports market_inputs_valid=false and legacy_reconciled=false because the full
regular-session evidence was not collected. Opening minutes likewise wait for a
complete six-minute regular bar window. Neither condition claims readiness.

In regular hours, observed_readonly means the native evidence and bounded market
inputs were validated. It still always reports execution_ready=false and
order_permission=none. A preview_enter is only a local experimental signal; it
creates zero claims or orders. Already-previewed immutable decisions are not
rewritten with a later quote, and expired signal windows are not reopened.

## Finite release/activation gates

1. Independently review the exact runtime files and additive dependency package.
   Inspect the startup change and anchored enrollment. No new capability-setting,
   credential configuration or cost approval is bundled into the code change.
2. Publish/deploy ONLY the frozen read-only package onto deployed baseline
   27568c921f6982e849bf2fd76533cd687ac89945 through the existing authorized pipeline.
   Keep the same service, disk, API entitlement and environment. No server exec is
   needed: reviewed startup performs bounded one-time enrollment itself.
3. Check sanitized initialization/source/closed-session outcomes and unchanged
   application health. If enrollment is blocked, report its exact fixed reason.
   Do not reset a pending marker or delete/recreate state to make it pass.
4. At the next regular session, establish observed_readonly with complete live
   legacy exact-ID absence and terminal-flat evidence, native account match,
   IEX quotes/bars and eligible asset metadata. Closed-session evidence does NOT
   substitute. Missing data remains a factual gate, not a request for a prototype.
5. Only then review a narrowly bounded PAPER execution enablement against this
   same coherent runtime. The one-share, two-position/$2,000, four-entry/session,
   $100 observed-loss entry latch, regular-hour and exact-exit rules stay intact.
   The latch is not a guaranteed loss cap and limit exits may not fill. No
   profitability proof is required for a bounded PAPER plumbing experiment, but
   verified ownership/freshness and supervised exits are required.
6. The current strategy additionally needs a genuine eligible original event.
   The latest verified public sample had no NVDA earnings item. A real release
   must be received within 120 seconds and pass the five-minute signal plus the
   one-minute regular-hours decision window. After-hours releases do not carry
   into the next open. NVDA-only scope cannot promise a trade in an ordinary
   session. No news timestamp, primary-source trust or strategy gate is weakened
   to force activity. A different bounded pilot or strategy would need its own
   exact proposal, not an invented current signal.

Public entry points, new infrastructure, paid APIs, added subscriptions, real-money
origins, new keys and model calls are outside this release. Existing external/manual
account clients are not globally fenced, and broker snapshots are not atomic.

## Local evidence

All implementation/review tests use synthetic source/native payloads and injected
transports, with sockets and DNS prohibited. No live provider call, key retrieval,
external mutation or deployment occurred while building this package. The final
manifest, baseline diff, independent review and full/staged test logs accompany the
frozen package. Earlier component reports remain historical context; this document
supersedes their unimplemented coordinator/state-inventory/startup gap lists.
