# Isolated Alpaca paper adapter (disabled by default)

Base: `Rafaeldavid006/paper-trading-agent` main at
`a55a2e984f11c5e328a1dc03fb51ad643fb1670d`.

This is development work on a draft PR, not a deployment or an activated trader.
The authenticated research app now includes a Hebrew `/paper` status/control screen.
`create_app(..., paper_adapter=None)` defaults to no adapter: the screen reports
unconfigured and cannot enable anything. `serve.py`, `wsgi.py`, and the research
workers do not construct or configure an adapter. No account was contacted and no
paper or real order was sent during development. No credentials were read,
created, or configured. There is no strategy, order-submission HTTP endpoint,
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

Nothing here authorizes activating orders. Before any separately approved paper
trial, securely provide paper-only credentials, independently verify the paper
account ID, review the exact intent and allowlist, and run read-only preflight.
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
The new dashboard control shows persistent permission/stop state and unresolved
intent count, but is not deployed. Cancellation remains a library-only operation,
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
replace screenshots or an actual browser-layout check. No actual account/transport
integration test, deployment or strategy validation was performed.

Official API references consulted on 2026-10-18:
- https://docs.alpaca.markets/us/docs/paper-trading
- https://docs.alpaca.markets/us/docs/orders-at-alpaca

These documents explain paper isolation, client order lookup, partial fills and
cancellation behavior. Reading them is not verification of this account or an
end-to-end broker integration test. No profitability claim is made.
