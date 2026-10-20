# One fixed-generation native read-only probe

This is the concrete server hook for the reviewed native observation collector.
It does not connect the collector to an execution lifecycle, activate a strategy,
add a service or subscription, or configure credentials.

## Exact release scope

Against the supplied shadow baseline
`aa4ab525ca6934be4461472b012ab71f5f3d4f57`:

- Add `native_observation_collector.py` and pure `alpaca_schema.py`.
- Add `native_observation_worker.py`: one opt-in `--once` CLI invocation, fixed
  generation `native-readonly-probe-20261020-g1`, no arbitrary generation or path
  arguments and no import-time action.
- Add `native_observation_supervisor.py`: one child, independent 15-second timer,
  kill before reporting a timeout, no respawn and no unbounded reap wait.
- Modify `serve.py` only to construct/start/poll/close this separate probe.
- Add focused tests, schema fixture and documentation. Existing order,
  renewal, research and shadow implementations remain unchanged.

The collector and callable worker still default disabled. The proposed `serve.py`
change explicitly runs the single fixed-generation probe on service startup.
This distinction is deliberate: deploying this exact hook is an explicit bounded
read, not deployment of a disconnected library. It is not a recurring monitor.

## Existing identity and credentials, no new configuration

Only the server child reads the existing `ALPACA_API_KEY` and
`ALPACA_SECRET_KEY`. Neither is retrieved through an assistant tool, exposed in a
command argument, configured anew, printed or persisted. The fixed provider
origins remain `https://paper-api.alpaca.markets` and
`https://data.alpaca.markets`; all permitted provider operations are GET and the
market-data feed is IEX only. There is no paid-feed, subscription, AI-call or
retry fallback. The fixed-generation probe consumes at most 12 GET attempts.

The existing `/var/data/paper-readonly.sqlite` is read with SQLite `mode=ro` and
`query_only`. The loader requires the original and renewal approval records,
their already-bound matching fingerprints, the historical identity cutoffs,
terminal experiment phases and complete durable intent registries. It never
seeds an identity, changes a stop, modifies an intent or instantiates an execution
adapter. Every row contributes to a private deterministic revision so changes
can invalidate the observation.

The full original reserved-but-untransmitted client ID is retained. The legacy
ledger lacks native order timestamps; the loader does not fabricate prior native
`Order` records. An absent reserved ID produces `owned_order_missing` if reached.
That is an honest completeness refusal, not a reason to filter the ID away.
Even if all supplied IDs are found, matching the native scan and lookup does not
reconcile their economic state against the incomplete legacy native history.
Every result therefore fixes `execution_ready: false` and
`legacy_state_reconciled: false`. An unexpectedly found original reservation is
not silently adopted or permission to trade; later execution work must reconcile
all known legacy payload/fill fields and investigate contradictions.

## Durable one-shot and watchdog

The private state file `/var/data/native-observation.sqlite` holds the one-shot
claim, collector request budget and redacted audit. The probe claim commits
before constructing a transport. After a claim, success, failure, interruption
and uncertainty are all consumed attempts; a service restart does not replay it.
No retry/reset/delete/rearm endpoint exists. Invalid/missing binding or
credentials refuse before any provider call.

The independent parent watchdog is not delayed by the existing main loop's
SQLite operations. Its deadline is 15 seconds from launch; normal OS scheduling
still applies. It issues kill without waiting for logs or an uncooperative
provider/DNS call. Shutdown reaping is bounded to one second. Process failure,
timeout and refusal do not restart the probe or alter the trading ledger.

The claim is durable, not tamper-proof: deleting/restoring this state file can
defeat replay history and must not be used to obtain another attempt. No
automated recovery from lost/reset persistent storage is claimed.

The 12-per-60-second request budget coordinates only participating callers in
this same new database. The preexisting web/research/renewal readers have their
own limits. No claim of an account-global quota is made.

## What sanitized live validation can prove

The child emits only fixed status/mode/order-permission, time, code hash,
proven identity-match flag and bounded counts. No account identifier, balances,
credential, client-order ID, provider error body or raw native snapshot is logged.

At the currently closed session, an expected successful transport check is
`closed_session`, `identity_match: true`, and two attempted GETs. This proves
that the account response matched the existing binding and the clock was closed.
It does **not** prove IEX entitlement, a current executable quote, all-order
completeness, primary-news provenance, trading readiness or profitability.

An open-session probe can truthfully refuse because of the retained never-
submitted reservation, changed account/positions, unavailable/stale quotes,
quota/deadline or any other strict collector condition. Do not relabel a refusal
as a successful full snapshot and do not force an out-of-session quote.

## Deployment and acceptance sequence

1. Verify the exact small release manifest/diff against the actual current
   deployed baseline. Keep all execution/lifecycle/strategy files out.
2. Review the fixed invocation, no replay, private-file checks, real GET allowlist,
   IEX-only policy and unchanged pricing/entitlement assumptions. No new
   credential or persistent-access configuration is part of this release.
3. Deploy the reviewed source using the existing service and existing settings.
   The existing service startup launches this one probe. Do not add an extra
   service or override command/environment to force a second attempt.
4. Read sanitized logs for the exact deployed hash and probe result. Check
   service health and that existing shadow/renewal status is unchanged.
5. If the outcome is absent/uncertain, inspect the sanitized claim/result rather
   than resetting it or replaying. If timeout occurred, treat it as consumed.

The task's local tests prohibit socket creation. A real provider read and
deployment are separate actions; no such action is recorded by this document.

## Gates after the probe

- Original earnings-release/exhibit bytes, parser lineage, dissemination time
  and correction handling are still required. SEC filing metadata or secondary
  Alpaca news must not be promoted to trusted primary earnings evidence.
- No native PAPER order submit/cancel transport is implemented or enabled.
- Shared account ownership and risk coordination with the existing one-shot
  worker/manual holdings remain necessary before continuous execution.
- A reviewed archival/completeness policy is needed beyond the bounded history.
- Do not persist raw collector kwargs in `Lifecycle` until its storage/privacy
  boundary is reviewed; the present lifecycle stores raw account dictionaries.

Current provider references: [Alpaca order pagination](https://docs.alpaca.markets/us/reference/getallorders-1)
and [Basic/IEX data plan](https://docs.alpaca.markets/us/docs/about-market-data-api).
The latter documents the free default Trading API plan, not verification of the
user's individual account entitlement.
