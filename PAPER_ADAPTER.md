# Isolated Alpaca paper adapter (disabled by default)

Base: `Rafaeldavid006/paper-trading-agent` main at
`a55a2e984f11c5e328a1dc03fb51ad643fb1670d`.

The generic adapter remains disabled by default. The separately approved fixed
one-shot Paper experiment is documented at the end of this file; it is not a strategy.
The authenticated research app now includes a Hebrew `/paper` status/control screen.
`create_app(..., paper_adapter=None)` still defaults to no adapter. Production
`wsgi.py` now explicitly injects a SEPARATE read-only connection, not the execution
adapter. It consumes existing server-held ALPACA_API_KEY/ALPACA_SECRET_KEY values
inside the server only and stores its snapshot at /var/data/paper-readonly.sqlite.
No credentials are retrieved/displayed through tooling or stored in the snapshot.
Local development/tests use mocks exclusively. The separately approved deployment
performs one bounded read-only Paper probe per web worker startup. The separate
approved experiment process alone can perform its fixed roundtrip. There is no
generic strategy, order-submission HTTP endpoint,
AI-text-to-order conversion, environment-variable activation, or live toggle.

## Included

- `PaperHTTP`: real standard-library HTTPS transport using only the exact origin
  `https://paper-api.alpaca.markets`. Live hosts, custom hosts, redirects, ambient
  proxies, and unrecognized endpoint paths are refused. Ten-second socket timeout;
  response body limit. Credentials are constructor inputs, not read from the
  environment or logged. Creating a transport sends nothing.
- `PaperAdapter`: defaults to `enabled=False`, empty symbol allowlist, and no
  verified account identity. Broker read-only `preflight()` works while disabled;
  `submit()` and cancellation cannot transmit writes while process capability is
  disabled. Submission additionally requires persistent explicit enable permission
  bound to the verified account and symbol allowlist.
- Explicit immutable `Intent` inputs, whole shares, buy or sell, regular-session
  day limit orders, minimum $1 price and cent precision. Stocks only, no options,
  crypto, shorting, fractional shares, market orders, leverage, or extended hours.
- Fixed caps: $5,000 per symbol, $20,000 gross, two new intents per rolling minute,
  cash-backed buys and held-share-backed sells. Existing positions count even
  outside the allowlist; only allowlisted symbols may receive new orders.
- Active orders reserve their FULL original quantity, not just unfilled quantity.
  This deliberately double-counts partial fills already reflected in positions:
  separate broker endpoints are not an atomic snapshot. It prevents stale-position
  / partial-fill races from underestimating exposure or overselling. Missing,
  unknown, unmanaged, malformed, or ambiguous orders block new submissions.
- Previous-close equity minus current account equity reaching a $1,000 daily loss blocks
  ALL new orders, including sells. The stop is latched by New York calendar date
  in SQLite once observed, even if equity later recovers. This is a gate on new
  orders, not a guaranteed maximum loss or continuous monitoring service.
- Durable SQLite intents committed BEFORE POST, deterministic client order IDs,
  exact-payload idempotency, fill progression checks, and restart reconciliation.
  A timeout, non-success POST, malformed response, or lookup 404 never triggers a
  duplicate POST. Unresolved intents block other new orders. Even rejected or
  canceled intents cannot be reused; a deliberately different intent needs a new key.
- `stop()` persists a kill latch; `stop(cancel_open=True)` additionally requests
  cancellation of managed paper orders with known broker IDs. It never liquidates.
  Final submit gate/POST and stop are serialized with a SQLite write lock. A stop
  can wait for an already-in-progress request; it cannot retract a transmitted
  order. Cancellation audits distinguish requested versus uncertain, and unknown
  submission identities are returned as unresolved. HTTP 204 means requested,
  NOT verified canceled; `reconcile()` must establish final status/partial fills.
  Resume requires an explicit current confirmation after a successful check; the
  UI adds a separate acknowledgement for clearing an existing kill latch.

## Verification and future activation prerequisites

Production is structurally read-only, irrespective of the user's broader demo
trading approval. Before any separately specified paper trial, independently
verify the account identity, review exact instruments/entry/exit conditions and
allowlist, and complete a separately reviewed execution configuration.
The code requires an active USD account with recognized multiplier 1, 2 or 4,
zero short market value, and no account/trading blocks. Margin capability alone
no longer fails the check. It does NOT authorize borrowing: negative cash,
long market value above equity, or position values above equity are rejected.
Buy reservations and new buys must fit actual cash; inflated margin buying power
is ignored. This does not change any account settings.

Use a dedicated account with no manual/external trades or cash transfers and ONE
shared durable local database for all adapter processes. External concurrent
actors cannot be synchronized by this library. Never delete/replace the database
or start independent databases for the same account: idempotency, reservations,
frequency limits and stops depend on its continuity. SQLite requires reliable
local filesystem locking, disk space, permissions and backups; these have not
been provisioned or verified in the deployed service.

Unresolved intents, account resets, corporate actions, adjusted/replaced orders,
withdrawals/deposits, corrections after terminal fills, and unfamiliar states
require operator reconciliation, not automatic replay. The current daily-loss
calculation includes cash-flow effects; do not use an account receiving cash
flows during a trial. No stream, continuous watchdog, stale-position feed proof,
or transactional multi-endpoint broker snapshot exists here. The broker clock
must be fresh, but it does not prove all account data is fresh. Account/positions
endpoint consistency and real provider responses still need controlled testing.

Exposure can rise above caps because of market movement. Existing orders can fill
while a stop/cancellation is underway; losses can exceed the daily threshold.
A failed stop call must not be represented as successful: database locks or I/O
errors can prevent persistence. Strict sell gating may require manual operator
risk management. Cancellation of external/unmanaged orders is not implemented.
The execution-capable dashboard control was deployed initially without an
adapter; the production read-only connection now selects a separate read-only
view instead. The execution library still has persistent permission/stop state. Cancellation remains a library-only operation,
not a website endpoint. The screen clearly says existing orders are NOT canceled
and positions are NOT liquidated by pressing stop.

## Authenticated control screen

Routes: `GET /paper`, `POST /paper/check`, `POST /paper/enable`, and
`POST /paper/stop`. All reuse existing server-revocable dashboard authentication;
POSTs require the session CSRF token. No credential input fields, account IDs,
provider exception text or secret transport headers are rendered. Existing secure,
HttpOnly/SameSite cookies, no-store headers and CSP remain unchanged. A navigation
link is added to the research dashboard. No change to its existing research flow.

- GET reads local state only; refresh, back and repeated navigation cannot contact
  the broker, enable anything, or mutate the broker account.
- Check first durably disables execution permission and invalidates prior check
  state, BEFORE broker GETs. Success leaves permission disabled. Exceptions and
  process interruption cannot preserve an old enable permission. A revision check
  prevents an older operation from overwriting a newer explicit control action.
- Enable requires explicit acknowledgement, a current form revision, and a
  successful check no older than 30 seconds. It performs fresh broker GETs again.
  No open or unresolved orders are allowed at enable time. Lost/stale sessions,
  replayed forms, stale broker clocks, loss latch, configuration mismatch and
  failed checks block activation. Enable also durably disables before provider
  reads so interruption cannot leave prior permission behind.
- Stop is a durable, repeat-safe local kill latch. It does not call cancellation,
  liquidation or order submission. A separate explicit resume checkbox is required
  before the backend can release an existing stop; restart/check cannot release it.
- Persisted permissions are not a running strategy: no worker invokes `submit()`.
  Process capability defaults off. A future separately approved deployment must
  inject an authorized adapter and retain the same database. An account or
  allowlist change cannot inherit an old enable permission.
- Daily loss observed in a valid account snapshot is latched before other
  readiness blockers, including existing open orders. Failure to store local
  state is reported as unconfirmed, never as a successful stop.

The UI intentionally does not expose a cancellation action in this phase. That
would transmit broker write requests. Its explanatory text distinguishes a
cancellation request from confirmed cancellation and mentions unknown/partial-fill
outcomes. Account setup remains a separate secure step.

## Tests (no broker network or credentials)

From the repository root:

    python -m pip install -r requirements.txt
    python -m unittest discover -v
    python -m unittest test_paper_adapter test_paper_controls -v
    python -m py_compile paper_adapter.py webapp.py test_paper_adapter.py test_paper_controls.py

The existing README refers to `-s tests`, but this pinned repository has its tests
at the root. New module uses only Python's standard library. Test fixture keys and
account IDs are dummy strings. Tests inject `MockPaper`; HTTP behavior tests mock
the opener and do not contact a broker. Threaded tests check stop/transmit locking. Control/UI tests cover authentication,
CSRF, stale checks/forms, interrupted checks/enable, persistent stop/resume,
configuration changes, unknown orders, margin-capable cash-only behavior,
read-only navigation, expired sessions, repeated POSTs and no secret/error leaks.

Phase-two verification: 151 tests pass (100 original, 26 adapter tests, 25 new
control/UI tests), plus compilation. Visual browser QA was NOT completed: local
browser startup lacked socket permission and the supported cloud browser rejected
local-file previews. No security warning was bypassed. Rendered mock HTML states
were produced and Flask test-client navigation/control tests passed; these do not
replace screenshots or an actual browser-layout check. At that phase, no actual account/transport integration test, deployment or
strategy validation was performed. Subsequent production read-only deployment
verification is reported separately; it never validates strategy or order execution.

Official API references consulted on 2026-10-18:
- https://docs.alpaca.markets/us/docs/paper-trading
- https://docs.alpaca.markets/us/docs/orders-at-alpaca

These documents explain paper isolation, client order lookup, partial fills and
cancellation behavior. Reading them is not verification of this account or an
end-to-end broker integration test. No profitability claim is made.


## Production read-only account connection

`paper_readonly.py` provides an independent `ReadOnlyPaperHTTP` boundary. It
rejects every method except GET, every request body and every endpoint except
account, positions, clock and bounded open-orders listing. It wraps the original
fixed-host HTTPS transport with redirects and proxies refused. The production
connection has no submit method; its enable method always rejects, and cancellation
is forbidden. Trading capability and permission always return false.

`wsgi.py` calls its factory once during worker startup. The factory consumes only
the already-authorized existing server environment keys. There is no new secret,
new environment setting, new resource or billing change. Missing credentials or
storage leave connection unavailable; authentication errors stop the check without
retry or changing credentials. Each startup probe is at most four GET requests;
there is no recurring scheduler or order probe. Startup logs contain only fixed
mode/result codes, never balances, positions, account identity, request bodies,
headers, secret values or raw provider/exception text. A startup result of
`connected_identity_unverified` proves a successful read on the fixed Paper API,
NOT that the owner has verified the intended account or approved a strategy.

The first successful read stores a private SHA-256 account-ID fingerprint plus
only a masked identity suffix. A later different fingerprint blocks the snapshot;
it never silently adopts a new account. Full account ID/number are neither stored
nor displayed; short identifiers are fully masked. The SQLite file is restricted
to owner-only mode 0600 and database symlinks are refused. The authenticated UI shows the masked suffix and explicitly asks
the owner to match it against Alpaca; it never marks identity verified automatically.
The first read does not set the execution adapter's expected account identity.

An authenticated CSRF-protected check refreshes balances, holdings and open orders.
GET navigation reads only the local snapshot, with a timestamp and stale-data
warning after 30 seconds. Snapshot fields are allowlisted; raw provider payloads
and order IDs are not stored or rendered. Interrupted checks invalidate prior
success before I/O. Malformed responses and incomplete/truncated listings fail closed. Account data is shown only through the existing authenticated screen,
never via a new public endpoint or startup logs. Read-only data can include
positions that would fail execution risk checks; connection success is not trading
readiness. The UI contains no activation or cancellation form in this mode.

Local verification after this change: 165 unit tests pass, including 14 new
read-only tests. They cover the independent write barrier, identity discovery/
change, sanitized logs, server factory wiring, auth/CSRF, stale/error/interrupted
reads, snapshot allowlisting, and absence of production execution capability.
No actual account credentials are used in local tests. Live startup connectivity
and public deployment checks must be verified separately after approved deployment.

## Approved one-shot Paper connectivity experiment (2026-10-18)

This is NOT a general strategy and does not enable the generic execution adapter.
The separate `paper_experiment_worker.py` child is bound to one immutable experiment
ID and the existing owner-matched account fingerprint. It can buy **one AAPL share**
with notional at most **$1,000 virtual**, then sell only the exact acquired quantity
once the buy is terminal. It will never create a second entry. The existing
$5,000/symbol, $20,000 gross, $1,000 daily-loss, cash-only/equities/no-short gates
remain in force. A daily-loss stop can also block the exit; this is not a promise
that a position will always be closed or that losses cannot exceed the limit.

Approval was given at 2026-10-18 20:08:05 UTC. Migration captures the existing
read-only account's `checked_at` into immutable `identity_first_seen` before a new
startup probe. A fingerprint first seen after approval, absent or malformed timing,
or a different fingerprint fails closed. Recreated account storage cannot silently
become approved. The immutable buy/sell client IDs are also queried at the broker
before first entry: anything other than explicit 404 blocks new entry. Do not delete,
reset, restore older copies of, or manually edit experiment state. Missing or damaged
state requires operator investigation, not an automatic new approval.

The worker checks the broker's fresh clock and waits for its regular session;
weekends, pre/post-market and `next_close` are independently gated. Current public
`next_open` is available in sanitized status logs and the authenticated screen.
No quote is fetched while waiting for the session. Fresh AAPL IEX quotes use only
`https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest?feed=iex`, existing server-held
keys, and no new data subscriptions. Quote age must be 0–10 seconds, positive,
uncrossed, spread <=1%. Buy price is ask rounded up to a cent; sell is bid rounded
down to a cent. Rounding and final quote movement cannot exceed 0.1%; $1,000 is a
hard cap, **not** an arbitrary marketable limit. No fill or profitable exit is promised.

A successful first eligible check establishes a 15-minute entry window, but a failed
safety/data check blocks rather than chasing/repricing. Orders are durable before
transmission. Restart, timeout and unknown results cause GET reconciliation, never a
second POST. Partial entry fills are not sold until the entry is terminal; fractional
exits require fractionable-asset permission. Any preexisting AAPL holding or open order
blocks entry. Exit requires the observed AAPL holding to equal the acquired quantity.
Assume no other actor trades this account during the experiment: broker snapshots
are not atomic, and unrelated manual activity can force manual attention.

Each working order gets at most one cancellation attempt after 60 seconds, only for
its validated broker ID and only during a regular session. Cancellation is not proof
of cancellation. GET reconciliation checks outcomes; unresolved submission is bounded
to 180 seconds and unresolved cancellation to 120 seconds after the attempt. A sell
that does not fully fill leaves `manual_attention`; there is no second exit attempt,
market order, liquidation or automatic retry. Completed means both recorded fills
match and a final account read shows no AAPL holding and no open orders.

The authenticated `/paper` screen reads local experiment status, never submits an
order. Its CSRF-protected stop latches the same database lock used by final broker
writes. Stop blocks **all new writes, including exit and cancellation**. Already sent
orders may still fill; holdings/open orders may need manual handling in Alpaca Paper.
Stop is not silently cleared at restart and has no screen reset. A blocked/terminal
experiment does not restart. The web and existing research process continue normally
when the one-shot child finishes. No secrets, account IDs, balances or holdings are
logged: only mode, phase, fixed reason and public market next-open time.

Tests use injected/mock transports exclusively. Authenticated production UI and real
order execution are not claimed verified by these tests. Deployment verification
must separately report sanitized worker status and actual terminal results.

### Unexpected experiment process exit

The supervisor distinguishes a normal terminal worker exit (zero) from an unexpected
nonzero/signal exit. It reports an abnormal exit once, without restarting the worker
or changing web/research supervision. Existing nonterminal experiment state becomes
`manual_attention` / `worker_failed`; all order IDs, fills and uncertain intent records
are retained. Already terminal outcomes are preserved. If existing storage cannot be
updated, the sanitized supervisor log reports `attention_recorded=false`; storage is
never recreated or reset. Intentional service shutdown does not emit this failure alert.

## One separately approved renewal, 2026-10-19 17:13:46 UTC

The original experiment stopped at `manual_attention / quote_moved`. Its durable
`paper_experiment` row and `intents` rows are never reset or reused. The owner approved
one additional attempt, not recurring retries. The new fixed generation
`approved-paper-aapl-renewal-20261019-171346` uses `paper_renewal` and
`renewal_intents` in the same private database, separate deterministic order IDs,
and the same cross-process lock, stop latch and daily-loss ledger.

`paper_renewal_gate.py` requires the original entry to be locally reserved, without
broker ID or any fill and with no prior sell, with the exact prior failure reason.
Before initial entry and again before transmission it verifies the same account
fingerprint, explicit 404 for BOTH original client order IDs, no AAPL holding and
no open orders. Unknown outcomes or unexpected prior state block renewal. The new
generation also checks its own fixed IDs against broker history before first entry.
It never changes or cancels an order from the original attempt or another actor.

`paper_renewal.py` deliberately isolates the reviewed engine logic rather than
changing the original path. Same one-share AAPL/$1,000 virtual ceiling, cash and
exposure gates, fresh IEX prices, regular session only and **unchanged 0.1% final
price-movement bound**. A repeated price movement can end this attempt too; there
is no automatic repricing or another generation. Exact acquired quantity is sold
only after entry becomes terminal. Uncertain outcomes are reconciled without a
second POST; terminal status remains terminal after restart.

`serve.py` starts only `paper_renewal_worker.py`, not both workers. Logs distinguish
`mode=paper_renewal`. The authenticated screen displays original and renewed states
separately and provides the same persistent stop, which also blocks exit/cancel.
A worker crash preserves original evidence and marks the active generation for
manual attention. No new source, permission, credential retrieval, paid service,
generic order endpoint or trading strategy is introduced.
