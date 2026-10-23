# Restart identity v2: local, disabled migration candidate

Prepared 2026-10-21. Source base: the frozen local four-file identity-evidence supplement corresponding to live commit `c38bd48a4b9e6fc275a62dc649e5b88a5aa5ae93`, following `c9bfcad9b13305a4db0d268200d21bd3fdded9f4`. No new remote verification or publication was performed. The package records the exact local base and candidate file hashes.

## What this fixes, and what it does not

The enrolled device number is a mount-local value, but v1 treated it as durable across deployments in both the bundle and account guard. The observed pattern (15 changed devices, unchanged inodes, mutually consistent metadata) is consistent with device renumbering, not proof of historical content continuity. A blanket device-match waiver would weaken the original replacement check and is not used.

This candidate adopts an explicitly approved current snapshot as a new logical cohort. Thereafter a fresh verifier accepts device-only remounts if all original paths/inodes and all cohort markers remain intact. A retained bundle instance never repins after a physical identity change. Inode changes, missing files, mixed cohorts, incomplete migration and corruption still refuse. This does not promise recovery after a filesystem replacement or backup restoration that changes inodes.

No historic schema exception is recovered by this work. The original collector retained only `native_schema_refused`; its exact parser enum was discarded. The prior consumed diagnostic is unchanged and cannot be repeated by migration. A future bounded provider sample could produce a different error and needs a separately reviewed and authorized diagnostic candidate.

## Minimal identity architecture

- Reuse SQLite `application_id` and `user_version` as two positive 31-bit, role-specific markers, derived from a fresh random 256-bit cohort nonce, format and fixed generation. All eight SQLite roles are marked. These are probabilistic identity markers, not secrets or MACs.
- Add one exact `native_restart_identity` table in the existing application anchor. Its canonical immutable contract binds account, generation, ten paths/original identities, original manifest digest, historical anchor, schemas and pre-migration snapshot digest.
- Add exact cohort/role/contract markers to the existing seal and empty legacy lock, in place. Keep original stored device/inode references unchanged everywhere.
- Retain existing owner/mode/link/sidecar/path checks and current `(device, inode)` checks around reads, locks and state writes. Only persisted logical-reference comparisons use original enrolled tuples.
- The account guard receives the retained full-bundle verifier, not a caller-supplied identity map or `ignore_device` flag. It refreshes verification at hold/state/quota boundaries and retains physical source/lifecycle/guard pins. Standalone migrated-guard access refuses. Missing migrated state is never created.
- Existing enrollment in normal `--initialize` follows readonly full verification before any provisioning path. Existing dirty, stopped, attention, source-interrupted and budget gates remain in force.

Existing deterministic generation stamps/schema hashes are release identities, not cohort identities. Xattrs were rejected because they add filesystem/backup support assumptions. Per-file UUID tables would require changing every sealed business schema. Header markers reuse existing SQLite mechanisms; only the anchor gains a table. Header writes change bytes and SQLite bookkeeping, but not preexisting schemas or rows.

## Actionable inspection-startup preflight (no shell required)

The existing inspection-only startup supervisor now launches the fixed `native_restart_preflight.py --inspect-restart-preflight` child. It emits a bounded JSON start record and result to the existing process stdout/Render logs, releases its locks, then invokes the unchanged existing-audit inspector. The two readers never overlap their own seal locks. The existing independent 15-second watchdog bounds the whole child; no same-startup retry/respawn is added. A later restart may repeat only nonmutating local inspection. There is no migration, backup, provider or credential-reading call in this entrypoint, and no new HTTP route. `serve.py` and `inspection_wsgi.py` remain byte-identical.

`prepare()` performs no commit, backup creation or metadata/data write. It does take transient exclusive flocks and SQLite reserved writer locks; these can temporarily block cooperating writers. The report proves only writer exclusion during the snapshot. The existing web module uses readonly startup inspection but restores ordinary user-route storage behavior afterward, so a startup flag is not proof that all application writers are globally or continually quiescent.

Read the `mode=native_restart_identity_preflight` log:
- `snapshot_observed_not_apply_authorized`: all old-cohort checks and exact snapshot completed under all ten locks, with an `approval_snapshot_digest`, separate selected-source `preflight_code_digest`, count/size summaries and no raw paths, accounts or rows. `writer_exclusion_during_snapshot=true`; `global_quiescence_proven=false`, `continuing_quiescence_proven=false`, `execution_ready=false`.
- `preflight_refused`: a fixed redacted code identifies lock contention, missing/unsafe files, sidecars, header use, changed revision, or another failed check. No approval snapshot digest is emitted. Known SQLite extended busy/locked errors are classified without logging their text.
- `preflight_started` without a completed result, or supervisor watchdog/reap uncertainty: no verified snapshot and no approval evidence. Never infer success from child launch or a healthy web process.

The selected-source fingerprint identifies seven preflight-critical source files; it is not an attestation of every import or authorization to execute code. Approval must separately bind the reviewed deployment/complete package. The data approval digest retains exact bytes, modes, original inodes/paths/enrolled references, schemas and rows, but excludes only the newly observed mount-local `st_dev`. Current physical tuples stay in ephemeral session pins. Thus a second device-only remount does not itself invalidate the approved preimage; apply must still re-prepare and compare the exact semantic snapshot under fresh locks. Any changed byte, inode, mode, path, logical record or original reference refuses.

The logged digest is usable evidence for a later approval, not an application channel. This release has no apply startup hook/CLI/HTTP endpoint. A later explicitly approved server-side apply mechanism must supply the approved digest, freshly revalidate the exact preimage, and establish its own quiescent maintenance window; it must not treat this earlier report as continuing quiescence.

## Disabled one-time migration

`native_restart_migration.apply()` defaults to disabled and performs no I/O then. Nothing invokes it from startup, environment flags, a web route, a supervisor or a CLI. `prepare()` creates an in-memory private `Plan`; it does not constitute approval. The only apply interface requires an exact matching plan digest, explicit enable, explicit quiescence confirmation and an already existing empty private backup directory. Test paths/checkpoints are test-only injection; production paths remain fixed.

Preflight requires:
1. Every current file is safe and stable; all original inodes match and old/current device sets each contain exactly one device.
2. Full old manifest/history/schema/account/content/stamp checks pass. Guard legacy revision matches. Legacy, anchor and manifest header slots are exactly zero, and legacy lock is empty.
3. No migration root exists and no SQLite sidecar exists. A hot journal is refused without recovery. Readonly shared locks bridge acquisition of all reserved writer locks, before any writable opening could recover a journal.
4. A complete portable byte-hash snapshot and exact preexisting schema/row hashes match the approved plan under the existing seal/legacy flocks and all eight SQLite writer reservations.

Apply creates exclusive private backup files through a pinned directory descriptor, syncs and reads them back, checks hashes and directory identity, and syncs directory and parent. It then writes/syncs a prepared anchor record, stamps each DB (guard first), marks lock/seal, verifies all old row/schema hashes and markers, and CAS-publishes completed last. Existing claims, attempts, quotas, high-water clocks, latches, incidents, risk/trading records, account bindings and original reference tuples are never updated or deleted.

Crash outcomes are fail-closed. Before the prepared record, original state is unchanged but a partial/nonempty backup is not reusable. After prepared, a new call refuses: no automatic resume, rollback, cleanup, re-enrollment or provider retry. An actual process-exit test covers prepared, first guard stamp, seal and completion; every durability boundary also has an injected interruption test. Backups are review/forensic material, not permission to restore stale trading or quota state.

## Threat model and limits

This protects against accidental device renumbering, ordinary path replacement, unrelated inode reuse, mixed cohorts and interrupted metadata migration while retaining within-instance race checks. It does not authenticate a hostile same-user/root rewrite or detect a coherent whole-cohort rollback/in-place restoration with matching markers and identities. The old inode/device mechanism also did not establish immutable historical business content. No external monotonic authority is added. The new snapshot is a newly approved baseline, never a claim of proven pre-migration continuity.

A private backup on the same disk is not disaster recovery. A coherent restore can roll back claims, budgets and incidents, and requires an independent reconciliation and approval process not implemented here. Do not copy these backups over live files, clear the migration table, or switch back to old verifier code as an automatic recovery step.

## Remaining evidence and approval before any live action

Local preparation/testing is the only approved action so far. Do not publish, deploy or apply this package automatically.

1. Independently verify the exact live base and proposed source hashes; approve publishing/deploying this disabled candidate and its bounded lock-taking readonly startup preflight while `INSPECT_EXISTING_AUDIT_ONLY=True`, research paused, and all provider/trading startup paths off.
2. Read the actual startup preflight result from existing logs. A successful snapshot checks current full-row/byte consistency and unused live header slots, but not historic continuity or continuing quiescence. If refused or timed out, stop and address that exact blocker rather than infer readiness. Before apply, establish a separately approved quiescent maintenance window and backup destination; the code must re-prepare the same approved preimage under fresh locks.
3. Present the exact plan digest, ten affected files, precise metadata writes above, preserved business-state guarantees, failure behavior and new-baseline/restore limitations for explicit migration approval. Do not apply if paths/inodes, account/revision, history, schemas, original references, current snapshot, headers or sidecars do not match. Do not grant filesystem permission changes as part of this step.
4. Apply once only after that approval. Verify fresh readonly bundle and account guard, including a subsequent controlled restart/remount; continue inspection-only mode. This is not authorization to clear latches, reset claims/quota, enable a diagnostic request, research, recovery or trading.

Production device-remount behavior has not been exercised here. Local tests simulate a changed `st_dev` with unchanged actual files; any other live identity change must still stop. A future finite GET-only diagnostic would require its own coherent gate integration, explicit request/time bounds and authorization. The previously considered no-rebind diagnostic branch remains unsuitable and is not included.
