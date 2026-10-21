# Untrusted identity comparisons after the anchor refusal

## Exact scope

Baseline: `c9bfcad9b13305a4db0d268200d21bd3fdded9f4`.

The live inspector identified `bundle_verify_before_lock / anchor / bundle_anchor_replaced`. The existing gate compares the enrollment row's stored `(device, inode)` with the current configured EVENT_DB file's pair. This proves a tuple mismatch, not data deletion, corruption, a harmless remount, or the original Alpaca schema cause.

This change adds `native_cohort_identity_evidence.py` and a narrow call from the existing inspector's exact anchor-refusal branch. The authoritative refusal, null audit fields, nonzero exit, zero provider requests, no permission, and no execution readiness remain unchanged. The supplemental reader is disabled unless explicitly called. It never calls the collector, diagnostic, provisioning, recovery, broker transport, or runtime verifier. No startup, supervisor, web entry, pause, deadline, environment, or secret changes occur.

## Reported evidence

The supplement uses only the existing configured inventory paths. Stored paths are compared privately, never followed. All output keys are fixed. Values are fixed enums or true/false/null; no raw paths, device/inode numbers, account identifiers, hashes, database payloads, timestamps, SQL or exception text are emitted.

- Safe-file access and observation-local identity stability for all ten roles.
- Separate device and inode equality for all fifteen stored identity-reference edges: enrollment to anchor/seal/manifest; seal to manifest; manifest binding to seal; eight manifest file records; guard binding to legacy/lifecycle.
- Agreement among stored references to the same role, without replacing any stored value.
- Existing seal contract, account/generation/configured-path binding, completed enrollment and manifest structure.
- Recomputed old manifest digest, compared separately with the seal and enrollment copies. This hashes the original stored records, not substituted current identities.
- Seven current schema hashes compared with old manifest hashes. The anchor hash covers the original reviewed application-table subset and enrollment SQL, not its entire schema. Manifest structure is checked separately because it has no historic schema hash.
- Five existing role generation stamps. Anchor, legacy, manifest and non-database roles are not falsely labeled as stamped.
- The single enrolled historical event's digest and original historical-date eligibility.

Null means unavailable or inapplicable; applicability flags and bounded stage/error codes distinguish these. Independent safe checks continue when another role is missing or malformed. Supplemental errors cannot erase the authoritative outer refusal.

All reads retain the original no-follow, owner/mode/link/sidecar, rollback-journal header, SQLite read-only, query-only and deadline guards. Current file identities are checked during and after read cleanup. No file, schema, journal, marker, lock file, permission or stored value is created, repaired, adopted or reset. An advisory lock is taken only on the existing seal file, without modifying it. The existing independent fifteen-second process deadline still applies.

## What this does not establish

Every result is permanently `trusted=false` and `runtime_gate_passed=false`. It does not turn a failed cohort into trusted audit evidence. A matching schema is not matching mutable content. The manifest contains no historic full-content digest for each mutable database. The historical event digest covers only one row. Unkeyed self-consistent hashes do not authenticate a coherent rollback or malicious rewrite. A lock on the current seal inode cannot fence a holder of an older inode. Cross-database snapshots are not atomic; observation-local file identity checks cannot prove absence of every concurrent in-place edit.

Even a uniform device-only mismatch plus matching references/schemas/history does not prove why the device changed. It narrows the next investigation. Mixed inode changes, disagreement between stored references, missing files, changed history or digest/schema failures remain separate evidence requiring review. No result authorizes re-enrollment, reanchoring, conversion to WAL, chmod, restoration, ledger deletion, broker retries, halt clearing, order submission or trading activation.

## Platform facts

Render documents persistence beneath the mounted disk path across deploys and restarts, but does not document a stable Linux device/inode identity guarantee across service lifetimes: https://render.com/docs/disks#disk-limitations-and-considerations

Linux describes st_dev as the containing filesystem's device ID and st_ino as unique within that filesystem: https://man7.org/linux/man-pages/man7/inode.7.html

These facts justify investigating a cross-release identity assumption, not assuming a harmless remount or removing the gate.

## Verification and publication boundary

Tests use disposable synthetic state, including device-only/inode-only changes, contradictory references, altered history/digests/schema/stamps, missing or unsafe files, poisoned stored paths, supplemental exceptions and the unchanged runtime refusal. They prohibit network access and compare file bytes, identities, modes and sidecar inventory before/after reads. Independent fault/privacy review accompanies the release.

Only the inspector, its new helper, a new regression test and this document are proposed for publication. Existing frozen artifacts and runtime validation remain unchanged. Publication is handled separately by the project publisher; this document records no deployment or recovery permission.
