# Optional local observation audit, not a trading strategy

## Exact baseline and scope
Base: f8ddf43b1301e35288502bfba9546fbddf2a7ca9. Every baseline file was compared by Git blob hash with GitHub's recursive tree; all matched. Candidate excludes unrelated uncommitted local files. Only research_worker.py changes. Additions: live_shadow.py, shadow_event_contract.py, shadow_market_inputs.py, their tests, and documentation. No dependency, credentials, startup, orders, existing research cadence, or existing AI budgets change.

The post-cycle hook runs ONLY when ENABLE_SHADOW_OBSERVATIONS=true. It does not start the existing worker or enable its preexisting auto/web research gates. If the worker is not already running, this rollout must not enable it implicitly. No additional provider or AI requests are made by the hook. Existing source SQLite opens mode=ro; only a separate sibling shadow-observations.sqlite is written. A one-second lock wait, 100 input-event default limit (maximum 1000), 100000 record ceiling, and 64 MiB database limit bound the new audit. Limits fail closed without deleting data. No promised zero infrastructure resource use: local CPU/disk are consumed within current service resources.

shadow_event_contract.py is a pure data definition. No active_demo_pipeline or fake execution engine is shipped. shadow_market_inputs.py is disabled by default and never imported or called by production paths; it is a future GET-only schema adapter, not an activated feed. Do not enable or wire it as part of this rollout.

## Meaning of an observation
An existing lexical news trigger with recent healthy collection and stored completed-bar movement in an experimental 0.3%-2% band can be labeled observation_candidate. This is co-movement, not causal attribution, recommendation, profitable edge, or order. Every output has order_permission=none, primary verification false, and strict strategy abstention. Existing IEX observations are partial market coverage, not consolidated executable quotes. The original strategy version name is an informational contract identifier only; no strategy is run.

Actual collector receipt times are preserved. Reaction availability must follow event receipt and request. Feed, original event time, and raw adjustment provenance are checked. Event payload/hash/version are recomputed using the existing Store contract; this detects local inconsistency, not cryptographic authentication of the publisher. Future-received corrections cannot suppress an earlier as-of observation. Derived movement is recomputed from stored bars; stored summary claims are not trusted.

Identical source snapshots and identical status/reasons deduplicate, even across invocation time changes. Changed source health, inputs, or abstention reasons append a new audit row, never overwrite history. Returned/logged counts mean evaluated observations, not newly inserted rows. Up to the newest 100 recent events are covered per cycle, not a complete news universe.

## Activation and rollback boundaries
Publish/deploy only after explicit bounded approval. Add ENABLE_SHADOW_OBSERVATIONS=true only on the existing service if its research cycle is already running; no new worker, AI schedule, permissions, subscriptions, source, account, credential operation, or trades. Verify unchanged order paths and actual observation logs after deployment. Disable by removing/setting the flag false and restarting/redeploying the same service; preserve audit data. Enabling existing disabled research requires a separate cost/schedule decision.

Failures are logged as shadow_audit_failed with no raw exception/secret contents; existing cycle result handling proceeds. Malformed source records can fail that audit cycle rather than being silently accepted. Existing source collection and trading are unaffected by this hook except shared host resource usage and up to the bounded lock wait.

## Genuine remaining gaps
- No live audit or live record freshness was verified by this local-only review. Existing research worker enabled/running status must be checked before activation.
- No dashboard reader for audit records yet. Initial read-only review must retrieve counts/reasons safely; an authenticated UI is separate scope.
- No direct authenticated issuer receipt, paired SPY comparison, executable quotes, halt gate, prospective performance/outcome evidence, out-of-sample advantage or return promise. This is not readiness for autonomous trading.
- Existing research transport behavior is unchanged. A preexisting market-data request may follow its current code; this hook introduces none and should not be described as hardening every existing network path.
- Capacity exhaustion intentionally halts auditing, not research; no automatic pruning or silent data loss. Operational review of shadow_audit_failed is needed.

## Suggested bounded owner approval (Hebrew)
מאשר להעלות ולפרוס לשירות הקיים ב־Render עדכון תצפית בלבד, ולהפעיל בו תיעוד מקומי של מחזור המחקר שכבר רץ? העדכון לא ישלח עסקאות, לא יפעיל מקור מידע או מחזור AI נוסף, לא ישנה מפתחות או מנויים ולא יגדיל את התוכנית. הוא ישמור עד 64 MiB של תיעוד בדיסק הקיים. אם מחזור המחקר אינו פעיל, לא נפעיל אותו במסגרת האישור הזה. זה ניסוי איסוף ראיות, לא אסטרטגיה שהוכחה כמוכנה למסחר.
