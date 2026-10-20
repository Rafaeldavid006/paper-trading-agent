# Native read-only observation collector

Local additive candidate only. Disabled by default. No startup hook, environment
read, credential discovery, provider call during tests, deployment, subscription,
order submission/cancellation, or execution-port change was made.

## What is implemented

`NativeObservationCollector.collect()` consumes an injected GET-only interface and
an independent caller-owned durable ledger view. `NativeReadOnlyHTTP` is the narrow
optional wire transport: injected credentials only, literal HTTPS Paper/data hosts,
explicit relative routes, IEX only, no proxy or redirect, no retries or feed fallback,
524,288-byte response cap, strict duplicate-key/nonfinite JSON rejection and a
request inactivity timeout no greater than two seconds or remaining collection time.
The wire follows `call(method, path, body=None) -> (status, payload)` and has no
POST/DELETE method surface. It is not a broker execution adapter.

The collector:

1. Loads the complete supplied `OwnedLedgerView`, not ownership inferred from order
   names, prefixes, currently open orders, or the provider scan. The authenticated
   account's SHA-256 fingerprint must match the independently expected binding.
2. Reads native account, clock and the exact New York calendar date. It reuses the
   existing `parse_session` and compares against exact nanosecond native timestamps.
   A closed session returns `closed_session`; there is no fabricated session or quote.
3. Scans all order statuses, unfiltered by symbol, descending by submission time,
   with 100-row pages and exclusive `before_order_id` cursors. At most five pages and
   strictly fewer than 500 aggregate orders are accepted. Duplicate IDs, repeated
   cursors, reversed chronology, unsupported statuses, non-array/oversized pages,
   cursor rejection and an unresolved full boundary fail closed. Timestamp cursors
   are intentionally not used, avoiding an equal-timestamp boundary assumption.
4. Performs a separate by-client-ID GET for every supplied durable owned ID. Every
   result must appear in the scan and match it exactly. Prior parsed native states,
   if supplied by the ledger, go through `reconcile_order`; regressing/conflicting
   states and even otherwise-valid progress during the collection are refused.
5. Reads IEX multi-symbol latest quotes. Account and position rereads must match the
   opening parsed values. A second ledger load must match the first complete view.
6. Runs existing pure `alpaca_schema` parsers and exact nanosecond freshness checks
   before returning a separate, explicitly non-executable `NativeObservation`.

The account/position equality requirement is deliberately conservative: mark-price
changes can refuse an otherwise healthy active account. This is a bounded one-shot
read-validation seam, not evidence that continuous collection will always succeed.

## Shared quota and time boundary

All participating collector instances using the same durable SQLite database share
at most 12 reserved GET attempts in a rolling 60-second window. SQLite
`BEGIN IMMEDIATE` reserves and commits a slot before calling the provider. Failed,
interrupted and uncertain attempts still consume their slot. A durable high-water
clock rejects rollback across restarts. Corrupt/missing/inconsistent quota state,
locked/unavailable storage, or exhaustion refuses the collection rather than
resetting quota. An instance also has a nonblocking local collection lock.

The request cost is 7 fixed reads + number of order pages + number of owned IDs.
Thus one-page collections fit at most four owned IDs; five pages fit only zero.
Pagination, concurrent collectors and previously failed calls consume the same
headroom. Capacity refusal never silently drops an owned lookup. This quota covers
only callers participating in this DB, not the old shadow clients, one-shot clients,
other hosts, other DBs or unrelated API-key consumers.

A successful observation cannot span more than 10 seconds of either monotonic or
wall time; regressions and stale/future native timestamps are refused. Checks run
before and after every GET and before final acceptance. This is an acceptance
bound, NOT a hard wall-clock interruption guarantee: DNS or an uncooperative
injected call can block longer despite socket inactivity timeouts. A real external
process watchdog is still a production dependency. No unsafe fork was added to the
threaded app. No call returning after the deadline yields a valid observation.

## Completeness, privacy and non-execution

Coverage means: the unfiltered bounded API scan reached a short page according to
provider semantics, and every independently supplied durable client ID was checked.
The caller must supply all historical and pending owned IDs from a stable durable
ledger and increment its revision on changes. Optional prior parsed order states
must also be supplied when available. The before/after check cannot prove the loader
is truthful, cannot discover omitted ownership and is not a ledger lock after return.
The service must serialize later observation use against ledger mutations.

This is not a universal/atomic account snapshot, proof that no unseen event occurred,
proof of credential ownership, or cryptographic proof of source authenticity.
Native endpoint responses generally lack an account ID: consistent authenticated
transport context is trusted; any supplied conflicting account ID is rejected. IEX
provenance is bound to the explicit request, not invented provider metadata. Returned
feed metadata is checked if present. IEX is one exchange, not consolidated NBBO.

The raw account ID exists only in private in-memory canonical evidence bytes. Output
repr and summaries expose no IDs, balances or credentials. SQLite audit stores only
fixed statuses, attempt/order/owned counts, timestamps and a digest. Raw provider
errors are never emitted. Audit correction after a late failure is best-effort if storage itself fails; audit rows are diagnostic only, never execution or readiness authority. `native_arguments()` verifies the immutable evidence digest
and returns a fresh private copy with exact existing observation kwargs; Decimal
wire prices are losslessly normalized to decimal strings accepted by the parsers.
`session_open_ns` is separate. Do not log, serialize for clients, or persist this
private payload. The evidence digest is integrity bookkeeping, not authentication.

The output has no `read`, `submit` or `claim` method and is not a
`NativeFixtureObservations` instance. The unchanged exact-type fixture fence rejects
it in `NativeDecimalExecutionPort`. Converting it to a fixture wrapper or dispatching
it to a broker is explicitly outside this module and has not been implemented.

## Remaining production dependency

Before any read-only shadow wiring: independently review the candidate, inject
existing Paper credentials server-side without fetching/configuring secrets here,
verify the existing expected fingerprint and actual read/data entitlement, implement
a stable complete ledger loader, use one canonical durable DB with all participating
callers, attach the external watchdog, and explicitly enable only the read collector.
Account reset/key rotation must fail binding until separately reviewed; no automatic
rebinding or feed/plan upgrade exists.

Do NOT plug this directly into current `Lifecycle.observe`: that existing method
persists raw account dictionaries/IDs to SQLite. Its storage/privacy boundary needs
separate review before any native read-to-lifecycle wiring. The local output seam
closes parser compatibility only, not live execution or safe persistence. Production
needs only this new module and the pure `alpaca_schema` dependency plus existing
`shadow_market_inputs`, `paper_adapter`, and `paper_quotes`; no execution module is
imported by the collector itself.

## Verified official API references (2026-10-20)

- [Get All Orders](https://docs.alpaca.markets/us/reference/getallorders-1): all-status
  scan, maximum 500, descending direction, exclusive `before_order_id`, mutually
  exclusive cursor/time filters. If the deployed Paper endpoint does not support
  the documented cursor, stop rather than fall back to timestamp pagination.
- [Get Order by Client Order ID](https://docs.alpaca.markets/us/reference/getorderbyclientorderid):
  independent `/v2/orders:by_client_order_id?client_order_id=...` lookup.
- [Latest Quotes](https://docs.alpaca.markets/us/reference/stocklatestquotes-1):
  multi-symbol endpoint, explicit IEX feed, 401/403 entitlement/authentication and
  429 rate-limit outcomes. There is no automatic retry or subscription escalation.
- [US Calendar](https://docs.alpaca.markets/us/reference/legacycalendar): native
  `/v2/calendar`, exact date range and early-close times.
- [Market-data plans](https://docs.alpaca.markets/us/docs/about-market-data-api):
  Basic includes IEX; the particular account's entitlement and other consumers of
  its allowance remain unverified. No expenditure or plan change is authorized by
  this module, and the local 12/60s cap is not a provider-wide rate-limit guarantee.

## Fixture validation

`python -m unittest -v test_native_observation_collector`

The suite denies socket creation/connect and uses only local fixtures and SQLite.
It covers default-off zero I/O; host/method/body/path/redirect/feed restrictions;
strict/partial/oversized payloads; exact nanoseconds and stale/future/closed sessions;
account and position conflicts; bounded ID pagination and 499/500 boundaries;
owned missing/mismatch/regression and stable ledger checks; shared concurrent quota,
failures consuming budget, corrupt state and rollback; secret-safe audits; immutable
evidence, instance locking, deadline acceptance and the unchanged fixture fence.

Final local verification at handoff: 75 collector tests passed, 17 independent
review tests passed, and 785 full-candidate tests passed using the existing local
Flask dependency directory (`PYTHONPATH=/workspace/shared/paper-test-deps:$PWD`).
Post-audit deadline, provider-clock/quote freshness and session-close checks run
again at acceptance; a failed check replaces that collection's audit row with a
refusal and removes the evidence digest instead of retaining a success row.
