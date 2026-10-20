# Native PAPER execution candidate, local review complete

## Result and scope

This candidate connects the existing original-source event contract, decision-only
strategy, tick runner, durable Decimal claims, complete native observations, and
PAPER submission/cancellation in one path. It is disabled by default and not wired
to service startup. No credential loading, environment changes, scheduler, API
expense, external publication, provider order or live-money route was added or
performed in this work.

Final local suite: **1,072 passed with sockets, direct connections and DNS blocked**.
Independent dispatch review: **28 adversarial tests passed**. The original-source
component also has its own independent review. Synthetic original RSS/HTML bytes
reach the actual strategy/market adapter/native request boundary in tests. They
are not represented as a real live earnings release or real broker execution.
All thirteen files of the deployed fixed read-only probe remain byte-identical.
The probe must not be reset or reused as execution permission.

Against deployed baseline 27568c921f6982e849bf2fd76533cd687ac89945, the minimal
candidate consists of nine new runtime modules, associated tests and two documents.
It changes no already-deployed runtime file and therefore has no startup effect.
The older fixture-only modules are reused dependencies, not competing workers.

## Connected components

- `PrimaryEarningsSource`: original NVDA RSS and matching original HTML, durable
  raw bytes/lineage/first receipt/correction quarantine. API and source constraints
  are described in `PRIMARY_EARNINGS_SOURCE.md`.
- `NativeMarketSource`: the same fresh native quote used for execution, plus a
  bounded IEX stock-and-SPY bar batch. Original nanosecond quote precision survives
  the strategy's microsecond datetime representation without being refreshed.
- `NativePaperExecutionPort`: existing five-minute decision logic and native
  Decimal lifecycle, retaining the exact original FakeBroker boundary elsewhere.
- `NativeExecutionObserver`: frozen native collector plus full-book legacy/native
  reconciliation. It never fabricates a provider order or old timestamp.
- `NativePaperDispatcher` and `NativePaperHTTP`: exact guarded PAPER-only native
  submission/cancel routes. Existing credentials must be injected by a future
  separately reviewed server caller. Constructors default disabled.
- `SharedAccountGuard`: account binding, existing legacy experiment lock, reserved
  SQLite writer lock, stop checks, shared new-path request budget, legacy risk and
  preserved historical ownership. No legacy ledger row is changed.
- `lifecycle_storage`: validated minimal private projection, no raw account ID or
  arbitrary provider extras. Private native/guard databases require exact 0600,
  regular owner-owned files, safe sidecars and stable identities. Public status is
  allowlisted; private status and raw observations must never be logged/published.

## Execution invariants

Entries remain exactly ONE share. Fractional quantities are supported only for
reducing a genuine fractional fill/residue, never fractional entry sizing. Limits
remain at most two held symbols, conservative gross exposure at most $2,000, four
combined legacy/native entry claims per session, one entry per symbol/session,
and a $100 observed-equity loss entry latch. The loss latch survives recovery and
restart. It is not a guaranteed loss cap, stop order, or automatic liquidation.

Orders are regular-session DAY LIMIT orders, extended_hours=false, limit $1 to
$1,000. Entry is prohibited before five minutes after session open or within 75
minutes of close. Native account clock/calendar, fresh quote, spread, cash,
complete ownership, asset eligibility and original-source gates all remain
mandatory. IEX is fixed; no SIP fallback, model call or subscription is added.
Existing experimental signal/exit thresholds are unchanged and unvalidated as an
investment edge. Limit orders may not fill; a shutdown, data failure or attention
latch can leave a position open and requires supervised reconciliation.

Every native claim precedes dispatch. Every POST/DELETE attempt is recorded as
uncertain durably BEFORE sending. Exceptions, conflicting response account IDs,
malformed responses and process interruption never authorize a retry. An accepted
cancel response is only a request, not proof of terminal state. Subsequent complete
native evidence must confirm the terminal fill and exact remaining ownership
before an exit. Exits can use exact nine-decimal residue only when available
position quantity matches and the asset is fractionable. Read-only reconciliation
never clears an attention/stop latch or replays a write.

All submitted legacy and native terminal orders must remain in the complete
all-status scan (strictly fewer than 500 rows). Parsed financial/state identity and
all seven normalized native timestamps are pinned. Legacy timestamp pinning begins
at the first accepted current authenticated evidence; old timestamps are not
invented. Missing history, changed terminal evidence, unknown working orders,
unmanaged holdings, mixed ownership or account mismatch blocks execution.

The old quote-moved reservation is retained unchanged. Only the original eligible
zero-fill, no-broker-ID reserved intent can receive a CURRENT authenticated exact
client-ID 404 absence receipt. Scan absence alone is insufficient. This is not
history deletion, cancellation, a terminal broker state or a new order. Legacy
buy reservations still count toward their exchange-session entry quota.

All new-path GET/POST/DELETE attempts, including failures, share at most 12 per
rolling 60 seconds in one fixed guard file. The frozen collector also retains its
own stricter read budget. Legacy mutation workers are excluded by the existing
lock. Manual clients, external API callers and unrelated existing read-only
observers are NOT globally quota-fenced; this is not broker-atomic account
control. No claim of a total account API budget or absolute race prevention is made.

## Exact remaining deployment work

This section is an implementation/review plan, not activation approval. Do not
publish or enable execution merely because the local library tests pass.

1. Add one small server-owned coordinator and independent process supervisor,
   reusing the existing subprocess/watchdog pattern but NOT altering or resetting
   the fixed probe. No new service, subscription or secret is needed. First scope
   is explicitly observation-only: original-source collection and native
   reconciliation through a read-only guard, zero POST/DELETE. It must serialize
   source collection, market acquisition, eligibility rechecks and any later tick;
   no concurrent source mutation during a decision/dispatch window.
2. Pin a fixed durable state bundle before the first runtime start: existing
   legacy database plus native lifecycle, account guard, decision, runner and
   primary-source databases. Distinguish reviewed first initialization from lost,
   replaced, partial or corrupt state. A missing file after provisioning must
   refuse, never recreate budgets or replay history. Native/guard inode binding
   is already present; whole-bundle persistent existence/identity guard and
   restrictive creation for decision/runner files remain coordinator work.
   Existing raw legacy lifecycle databases are refused, not silently migrated.
3. Enforce an independent hard process deadline for every source collection and
   account tick. Socket inactivity timeouts and post-call elapsed checks cannot
   bound DNS or a stalled/slow response. A tick holds the legacy writer lock, so a
   stop waiting on that lock takes effect after the bounded hold, not retroactively.
   Kill must leave source busy/runner dirty/native uncertainty markers intact;
   restart must not auto-clear or resend. Verify timeout, stop, crash and storage-
   replacement tests before releasing the coordinator.
4. Source polling must stay at least 15 seconds apart and refresh health before
   it expires at 30 seconds. Broker work must respect the durable 12/60-second
   budget, reserving room for reads and a possible write. Use event readiness to
   schedule the five-minute endpoint rather than consuming an event immediately.
   A fixed slow account poll can miss the one-minute decision window; missing it
   means abstention, not a backdated trade. Do not increase quotas to force a trade.
5. Present the exact coordinator diff, fixed paths, cadence, watchdog limits and
   sanitized output contract for independent review. Confirm the existing IEX
   entitlement and no incremental cost. Reuse only already server-held credentials
   and approved account identity; no extraction or persistent-access expansion.
   Publish/deploy observation-only only under reviewed scope and project approval.
6. Validate the deployed observer with current original RSS/HTML, complete native
   account/legacy evidence, regular-session quotes/bars/asset availability and stop
   behavior. Do not replay the old closed-session probe. An authenticated 404 for
   the original reservation and flat terminal renewal still need LIVE verification
   in this new reconciliation path. Invalid/absent evidence is a factual blocker.
7. Only after the runtime and observed checks pass, separately review the exact
   PAPER execution enablement scope and permissions. Keep real-money routes
   impossible, no added spending, and an attended first entry/partial-fill/cancel/
   exit monitoring plan. Any required new credential/security setting needs its
   applicable approval; it is not bundled into this library work.

## Why no trade is promised

The public source sample at 2026-10-20 06:49 UTC found **no earnings item** in the
current official NVIDIA RSS. The archived official release HTML is real source
material but does not prove historical first reception or supply the required
matching seconds-precision RSS timestamp. It cannot activate a signal today.

Only NVDA earnings are supported by this original-source adapter. A newly observed
release must satisfy the original 120-second first-availability condition, the
five-minute signal and one-minute decision window, and the regular-session entry
window. An after-hours release is ineligible and is NOT carried to the next open.
There is therefore no basis to expect an NVDA trade in an ordinary regular-hours
session, even when the system is operational. The correct result may be no trade.
Changing to next-day signals, additional issuers, synthetic events or another
strategy to create activity would be a separate proposal and is not part of this
candidate.

## Reproduce local verification

Run unittest discovery with the repository's installed dependencies while mocking
socket.socket, socket.create_connection and socket.getaddrinfo to raise. The final
full log, focused log, independent review, exact baseline diff and manifest are
provided with the candidate evidence. No test uses credentials or contacts a
provider. The minimal additive package is a disabled candidate, not a runnable
continuous service or an execution activation package.
