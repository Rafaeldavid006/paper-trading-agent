# Isolated Alpaca paper adapter (disabled by default)

Base: `Rafaeldavid006/paper-trading-agent` main at
`a55a2e984f11c5e328a1dc03fb51ad643fb1670d`.

This is a local development deliverable, not a deployment or an activated trader.
`paper_adapter.py` is not imported or invoked by `serve.py`, `wsgi.py`, `app.py`,
`webapp.py`, the research workers, or AI outputs. No account was contacted and no
paper or real order was sent. No credentials were read, created, or configured.
There is no CLI, environment-variable activation, live toggle, scheduler, strategy,
AI-text-to-order conversion, or dashboard stop button in this isolated MVP.

## Included

- `PaperHTTP`: real standard-library HTTPS transport using only the exact origin
  `https://paper-api.alpaca.markets`. Live hosts, custom hosts, redirects, ambient
  proxies, and unrecognized endpoint paths are refused. Ten-second socket timeout;
  response body limit. Credentials are constructor inputs, not read from the
  environment or logged. Creating a transport sends nothing.
- `PaperAdapter`: defaults to `enabled=False`, empty symbol allowlist, and no
  verified account identity. Broker read-only `preflight()` works while disabled;
  `submit()` and cancellation cannot transmit writes while disabled.
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
  No automatic reset/resume operation is provided.

## Verification and future activation prerequisites

Nothing here authorizes activating orders. Before any separately approved paper
trial, securely provide paper-only credentials, independently verify the paper
account ID, review the exact intent and allowlist, and run read-only preflight.
The code requires an active USD account with multiplier exactly 1, zero short
market value, and no account/trading blocks. A default margin-enabled paper
account may fail this intentionally strict check; this deliverable does not
change account settings or assume permission to do so.

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
A future dashboard control must show persistent stop state and unresolved orders;
this deliverable does not modify the running site's UI.

## Tests (no broker network or credentials)

From the repository root:

    python -m pip install -r requirements.txt
    python -m unittest discover -v
    python -m unittest test_paper_adapter -v
    python -m py_compile paper_adapter.py test_paper_adapter.py

The existing README refers to `-s tests`, but this pinned repository has its tests
at the root. New module uses only Python's standard library. Test fixture keys and
account IDs are dummy strings. Tests inject `MockPaper`; HTTP behavior tests mock
the opener and do not contact a broker. Threaded tests check stop/transmit locking.

Official API references consulted on 2026-10-18:
- https://docs.alpaca.markets/us/docs/paper-trading
- https://docs.alpaca.markets/us/docs/orders-at-alpaca

These documents explain paper isolation, client order lookup, partial fills and
cancellation behavior. Reading them is not verification of this account or an
end-to-end broker integration test. No profitability claim is made.
