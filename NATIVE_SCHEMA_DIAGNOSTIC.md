# Fixed one-shot native schema diagnostic

Purpose: identify the strict parser gate associated with the current live
`native_schema_refused` condition after the October 20, 2026 13:30:37 UTC
first-open runtime failure. The existing log does not identify the failed parser.
This diagnostic is **not recovery**, a schema fix, execution readiness, or order
activation. No validation condition is weakened.

## Exact bounded scope

- One fixed generation: `native-schema-diagnostic-20261020-133037-g1`.
- One separate child with the existing independent 15-second watchdog; no loop,
  automatic retry, new service, new secret, environment change or subscription.
- Startup occurs only after the original runtime supervisor is halted and its
  child has been reaped. Its failed initialization is not reset or bypassed.
- Requires the original complete state inventory, exact existing account binding,
  original runner dirty=1 and attention=`readonly_runtime_stage_failed`, no native
  intent/dispatch, and latest collector audit=`native_schema_refused`.
- A once-only claim is committed in the existing application DB before reading
  existing server-held credentials or constructing a transport. Failure, timeout
  or uncertainty leaves that claim consumed. Existing/empty/corrupt marker tables
  never authorize a new attempt.
- Uses the original frozen collector and schema bytes unchanged. In its isolated
  process only, wrappers record the first parser/freshness failure and bare-raise
  the same exception. Aliases are restored in `finally`.
- Only GET routes are available. The diagnostic uses both the same coordinated
  account guard budget and collector budget, without increasing/resetting either
  existing 12-request/60-second ceiling. No source/news or paid model calls occur.
- Original legacy, lifecycle, runner, decisions, source state and fixed probe are
  not advanced, rewritten, deleted or reset. Only the new application claim and
  normal shared request/audit records are written.

## Public output and interpretation

Output is limited to fixed reason/parser/schema-code enums, bounded prior/current
request/order counts (null when an interrupted outcome is unknown), and hard `order_permission=none`, `execution_ready=false`.
No account/order IDs, values, timestamps from provider data, response bodies,
URLs, credentials, exception text or tracebacks are emitted or persisted.

`schema_reproduced` identifies the current sample's exact allowlisted parser
failure. Matching the previous audit count supports localization but does not
prove an earlier transient quote was identical. `schema_not_reproduced` means
only that this current collector sample passed; it does not explain the past
failure, reconcile legacy history fully, clear attention, or grant readiness.
Unknown/error outcomes stay refused. There is no second automatic diagnostic.

Closed-session success previously validated only account and clock before
returning. Newly reached strict order-history, quote, position or final-freshness
checks remain candidate causes until live diagnostic evidence. A calendar parser
failure normally has the different `session_validation_refused` code.

## Release and stop conditions

Only the diagnostic modules, their tests/documentation and a narrow `serve.py`
startup hook are in this package. The optional PAPER pilot package is not included
and remains disabled/unpublished. Existing runtime/probe modules are unchanged.

Publisher must verify exact baseline/hash/scope, deploy the reviewed no-expense
read-only diagnostic under project authorization, then capture only its sanitized
result. Stop after this fixed attempt. Do not reset its marker, the old probe, or
runtime latches. A fix/recovery requires the observed reason, appropriate source
semantics, a separate reviewed diff and a deliberate reconciliation plan. If the
reason cannot be established safely, report it as unknown rather than relaxing
schema checks or retrying silently.
