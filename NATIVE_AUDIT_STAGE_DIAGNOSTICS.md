# Existing audit inspection: staged refusal diagnostics

## Scope

This is an inspector-only local refinement over the reviewed inspection release. One production file changes: native_diagnostic_audit_inspection.py. New tests/documentation accompany it. Existing web entry, temporary research pause, supervisor deadline, source/market/trading code, frozen original diagnostic, consumed claims, and all store contents remain unchanged.

Live evidence received from the publisher: the previous inspector returned audit_inspection_unavailable with no correlated audit/counts, provider_requests=0, state_mutated=false and runtime_reset_performed=false. That result does **not** identify a missing audit, storage cause or historical provider/schema cause. Its previous generic catch hid every read/validation failure, including failures after a matching record was found.

## Refusal output

All existing public fields remain. Three fixed-enum fields are added:

- inspection_stage: exact outer operation, including individual frozen-code checks, legacy bootstrap/read/close, bundle construction, pre-lock/locked/final inventory verification, seal operations, each evidence DB open, each claim/halt/lifecycle/audit gate, read-context close, or final FD close.
- inspection_role: fixed inventory role only. No path, account binding, raw IDs or filenames from state.
- inspection_code: allowlisted local validation code or fixed exception category. No exception text/repr, SQL, provider data, database value, timestamp, balance, credential or identifier.

Unknown exception strings and types are reduced to fixed categories. Source/schema/storage rejection codes are included only from explicitly recognized local exception classes and literal reviewed vocabularies. Disabled calls still do no work and carry none enums. Success carries complete/none/none.

The child temporarily observes the existing inventory read helpers, read-only registry validation and seal decoding. Every original helper/check executes unchanged. The wrappers are restored on exit. These are process-local observers, not modifications to original files or SQLite. First failure remains authoritative if later cleanup also fails. A final integrity/cleanup failure cannot retain a successful audit label.

The fixed fifteen-second independent supervisor remains unchanged. Catchable failures get these exact enums. A process forcibly killed before output cannot promise a final per-gate report; its existing supervisor instead reports the fixed timeout/process-failure outcome. This is not represented as proof of any storage or schema cause.

## Gates deliberately retained

Full sealed inventory verification still precedes evidence and runs again under lock and after matching evidence. It checks every role, not only the audit DB. It includes existing absolute EVENT_DB, safe modes/owners/link counts/sidecars, rollback-journal SQLite format (WAL still refuses), account/generation/schema/stamp/history continuity, source/lifecycle/guard/decision/runner controls, stored Decimal projections and exact immutable legacy registry shape. Safe hot-journal existence alone is not a verdict; any required recovery/unsafe open refuses.

The consumed claim still requires exact original generation/code/status/parser/schema/count. The original halt and empty native execution tables remain mandatory. The same latest-two audit, count, fifteen-second claim window, previous six-attempt/two-order record, NULL digests and unique-postclaim conditions remain. No gate is relaxed to force a match. Missing or ambiguous evidence is not guessed.

The original diagnostic pinned only itself indirectly plus collector and schema code; this inspector's code checks do not pin serve.py or the inspection web entry. The old inspection output therefore cannot be attributed to a changed serve hash. This read path has no current market-open or primary-news freshness gate; a closed market or old NVDA release is not itself an explanation for inspection refusal. No actual production-state cause is established by these code facts.

## Deployment and next interpretation

Publication of this changed output contract is separately parent/user-controlled. It makes zero provider requests, does not repeat the consumed collector diagnostic, does not initialize a journal, and does not reset or repair state. It leaves the approved inspection-only web/research pause intact with no automatic resumption.

On a staged failure, act only on the named read/validation gate. Code pin failure calls for exact deployed-byte reconciliation; inventory failure calls for scoped private evidence of that role; missing/ambiguous audit means the earlier failure record has not been recovered. None authorizes WAL conversion, chmod, data deletion, claim reset, trading activation, an API retry, or a guessed schema fix. A correlated existing refusal is only that collector's refusal, not automatically the historical schema root cause.

## Verification

The included two-branch runner preserves the previous ten historical startup/validation cases under the explicit old test branch; default inspection-mode tests remain separate. The new stage tests inject missing claim/audit, wrong correlation fields, source-content/file failures, final verification failure, unknown exception strings and wrapper-restoration checks. Independent review additionally injects all read/lock/cleanup phases, ensures a cleanup failure is attributed to its own fixed DB role, and reruns byte/inode/mode/SQLite-read-only/provider-free invariants. Exact final results and hashes accompany the release manifest.
