# Existing diagnostic audit inspection: separate proposed release

## Purpose and exact scope

The fixed schema diagnostic already completed on 2026-10-21 at 13:37:31 UTC with `collection_refused`, `parser=none`, `schema_code=none`, `requests=4`, `prior_requests=6`, `prior_orders=2`. Its attempt remains consumed. This supplement does not call that diagnostic again or request another provider sample.

Four production files change over immutable diagnostic commit `3fccb9103bb6ee2a01f2e0c961886bf8eca36e44`:

1. New `native_diagnostic_audit_inspection.py`: read already-existing claim/audit under the original sealed inventory.
2. New `native_diagnostic_audit_supervisor.py`: fixed independent fifteen-second child watchdog, no respawn.
3. New `inspection_wsgi.py`: actual web entry skips the legacy startup connection/probe, opens existing UI schema read-only during setup, refuses missing schema rather than creating it, and reports health as inspection-only/account-unverified.
4. Modified `serve.py`: literal inspection-only branch starts web plus this inspector, never the research, renewal, fixed probe, read-only collector or original diagnostic workers. Existing environment values and all frozen diagnostic/parser code remain unchanged.

**Publication requires the user's separate approval, explicitly including temporarily paused automatic research.** This local package is not authorization. It contains no recovery, finite pilot, order enablement or v3 modules. Restoring a prior startup branch is also a separate reviewed action, not an automatic action after inspection.

## Read-only and privacy boundary

The callable is disabled by default; the proposed unpublished startup explicitly invokes its sole fixed command. It uses only existing nonsecret EVENT_DB to locate the anchored application database. No credentials are accessed. SQLite connections use mode=ro/query_only, existing rollback-journal header checks and pinned inode/schema/account binding. The seal flock is opened read-only. No new marker, schema, journal, latch, grant, clock counter or state is initialized, reset, deleted or updated. Failure refuses rather than repairs. Supervisor exits/kills the child within its independent fifteen-second deadline.

The existing consumed claim must match its original generation, diagnostic code hash, status, four attempts and absent parser/schema enums. The original dirty halt and empty native lifecycle must remain. The latest native_read_audit row must be the sole audit at or after the claim, within fifteen seconds, with four attempts, zero owned/order counts and no digest. Its predecessor must be the six-attempt/two-order native_schema_refused record. Unknown status text becomes `unclassified`; ambiguous/missing audit becomes `audit_inspection_unavailable`.

Output is fixed enums, aggregate counters and explicit zero provider requests/no order permission/no reset/no state mutation. No account IDs, fingerprints, provider bodies, prices, amounts, timestamps, URLs, filesystem paths, environment or raw exceptions are output. The inspector can emit once per server process; a restart can repeat this harmless existing-data read. It is deliberately **not** a new durable one-shot claim.

This correlation is a unique time-window/count match, not a recorded collector row-ID linkage. The original diagnostic did not retain that linkage and its failure-audit write was best-effort. A consistent whole-store rollback is not authenticated by local inode/schema checks. Neither limitation is disguised as stronger proof.

The startup has no background provider callers. The dedicated web entry neither imports the legacy WSGI probe nor constructs a paper connection. Its setup requires the existing UI schema and performs no state writes; GET/HEAD health and unauthenticated HEAD / use no provider calls. Health explicitly says account_verified=false and background_research_paused=true. Existing user-driven web routes are not disabled or claimed provider-free. No environment/secret/dependency/service changes, subscriptions or new API expense are introduced.

## Meaning of the observed counters

The collector increments requests only after its own durable request reservation, before the account guard/transport. It counts reserved attempts, not successful responses or necessarily transmitted HTTP calls. The collector budget and the account guard budget are separate twelve-per-sixty-second gates, not a twenty-four-call allowance.

Four proves initial account, clock and calendar calls and their schema/session gates completed, and the first positions reservation was counted. It does not prove positions returned. The refusal could be after that fourth reservation (clock/guard/transport/response), in positions context checking, or in the fifth orders attempt's initial clock/budget checks before its increment. No orders HTTP call, quote call, order parser or positions parser was reached by this diagnostic. Position schema parsing occurs only after the later repeated positions call.

Prior counters come from the previous audit, not this diagnostic. Prior_orders=2 proves the complete first two-row order page passed parsing and associated checks; it is not two owned orders or a failure parsing that page. If the stored prior_owned_count is zero, the historical six-attempt native_schema_refused localizes to quote parsing or its internal schema helpers. A nonzero owned count leaves an independent order lookup possible. Neither localization identifies the actual schema code or root cause, and this supplement does not change schema validation to guess a fix.

Source locations in frozen code: collector `_get` lines404-433, collection sequence463-540, exception/audit558-577; diagnostic prior_halt105-116 and result handling151-159; CoordinatedWire123-128; account guard reserve375-390.

## Validation and interpretation

Final complete suite: 1,099 tests, zero failures/errors/skips, sockets/DNS prohibited and process environment cleared. Ten retained historical startup/validation tests explicitly select the old False branch only inside the test harness; this is compatibility coverage, not a claim that default inspection mode starts old workers. Default-mode tests assert literal True, no automatic research even with env=true, no legacy/probe/diagnostic/runtime launch, and no unused provider credential read. Original tests/diagnostic bytes are unchanged. Unmodified bare discovery has obsolete startup expectations; use the included explicit two-branch runner.

Sixteen new candidate tests plus forty independent adversarial tests cover real disposable SQLite state, no-write byte/inode/mode preservation, mode=ro connections, missing/replaced/WAL/locked stores, bad/duplicate/late audits, unknown-text redaction, fixed command/deadline, no network and no replay.

Live result is pending. If the stored refusal is available, use that exact fixed code to choose the next narrowly scoped local diagnosis. If unavailable or only generic provider_read_failed, do not assert a historical root cause or automatically retry broker reads. The original halt, consumed attempts, v3 disabled candidate, source requirements and trading-activation gates remain unchanged.
