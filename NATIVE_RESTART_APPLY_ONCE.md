# Exact one-time startup invocation: disabled local candidate

This document supersedes only the invocation/approval section of `NATIVE_RESTART_IDENTITY.md`. The seven previously reviewed identity/migration/preflight production files are unchanged. The new launcher does not replace the migration protocol or weaken its checks.

## Status and exact scope

No publication, deployment, production backup, metadata migration, provider request or trade is authorized or performed by this preparation. Both `ENABLE_APPROVED_MIGRATION` and `EXTERNAL_WRITER_QUIESCENCE_CONFIRMED` are literal `False`; there is no environment, HTTP or arbitrary-path CLI switch. `INSPECT_EXISTING_AUDIT_ONLY` stays `True`. Deploying this disabled candidate alone performs no migration bookkeeping and leaves the existing inspection startup, health and report behavior unchanged.

The proposed live action is one exact source publication/deployment with those two literals changed to `True` only after explicit approval. `serve.py` then runs this fixed startup gate before web routes, research, the original audit child or any other worker can start. No remote shell, new credential, new access grant or additional service is needed. The existing Render stdout logs carry bounded redacted results.

## What the approval must identify

- Target: the existing `Rafaeldavid006/paper-trading-agent` service and its existing `/var/data` disk, not another deployment/account/data set.
- Reviewed source baseline: reported live/main commit `3a57ce4ff9598bf644469a1ea5c2f0c14a20e78d`. The local delta is only the four files listed in the package manifest. The publisher must compare the live repository tree with that manifest before applying the delta, rather than overwrite unrelated files from a local snapshot.
- Exact approved current preimage: `dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d`, 8 databases / 10 files / 1,896,794 bytes, from the successful 2026-10-23 04:13:27 UTC preflight.
- Unchanged seven-file core fingerprint: `da12f995c97a3502ab88c90482db37a109a576e9865d19f40bac2cbbf67210ad`. This fingerprint does not authenticate the new launcher or `serve.py`; the approval must also bind the exact reviewed package/activation diff and the resulting publication commit.
- Permission to create the single private attempt/backup root specified below, copy the ten original files there, and apply only the already-reviewed restart-identity metadata changes. All preexisting schema definitions and all existing business records, claims, quotas, incidents, latches and trading records must remain unchanged; the anchor gains only the new restart-identity table.
- Maintenance condition: do not open SSH/dashboard shells, edit files, use mutating application routes or start other tools against this service during the migration. This is an explicit operator condition, not a claim that access was revoked or that unknown sessions were enumerated.
- Accepted failure boundary: one attempt only; any incomplete/unknown attempt blocks startup rather than retry, silently rebase, restore, reset or repair. A partial migration or deploy failure can therefore require separately approved resolution. No provider sampling, recovery or trading is included.

## Existing files affected by the frozen metadata migration

The exact paths are bound by the approved snapshot. Eight SQLite files receive new cohort header markers; only the existing anchor also gains the identity table. The two JSON/lock files receive cohort markers in place. Existing stored device/inode reference rows are not rewritten.

- Anchor: the existing application database selected by `EVENT_DB` and already bound by the approved preflight digest. Its actual live path is not newly read or guessed in this local task; the fixture placeholder in source is never a production fallback.
- `/var/data/paper-readonly.sqlite`
- `/var/data/continuous-primary-source.sqlite`
- `/var/data/continuous-native-lifecycle.sqlite`
- `/var/data/native-account-guard.sqlite`
- `/var/data/continuous-demo-decisions.sqlite`
- `/var/data/continuous-demo-runner.sqlite`
- `/var/data/continuous-demo-state.sqlite`
- `/var/data/paper-readonly.sqlite.experiment.lock`
- `/var/data/continuous-demo-generation.json`

No application database is relocated or recreated. Any changed selected path fails the exact preimage comparison.

## Evidence for a narrow maintenance window

Read-only platform evidence supplied on 2026-10-23 shows only one workspace service including previews, one configured/runtime instance, and the 1 GB persistent disk `dsk-dba69sm1egvs739heavg` mounted at `/var/data`. Render documents that a persistent disk is exclusive to one runtime service instance and the old instance stops before the new one starts. Redeployment closes previous SSH sessions. New same-instance shell/dashboard sessions remain possible and were not enumerated.

Sources supplied and checked by the publisher: https://render.com/docs/disks and https://render.com/docs/ssh . These facts plus the explicit maintenance condition cover the intended cooperating-writer boundary. The new startup gate isolates this application's own workers; the frozen migration also takes both existing file locks and all eight SQLite writer reservations. Brief per-database commit/reacquire gaps mean locks alone do not establish global continuous quiescence. Final exact row digests prove preservation at verification points, not absence of a hostile writer. The external condition must be rechecked immediately before the approved deployment.

## Backup destination and limits

The fixed root is:

`/var/data/native-restart-identity-backup-dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d`

It is created exclusively as a private 0700 directory on the same disk. `attempt.json` is a private 0600 record. The ten original files and frozen migration manifest go in its private `snapshot/` child; each backup is created exclusively as 0600, read back, hash-checked and fsynced before the first cohort change. No external copy or disclosure occurs. This same-disk backup is not protection against losing the whole disk; no disk-level recovery guarantee is added.

Available nonprivileged space must be at least 64 MiB and available inodes at least 32, checked before reserving the attempt and again before backup creation. The approved original bytes total about 1.81 MiB; the margin also accommodates manifests and SQLite journals. This is a conservative availability check, not a reservation against unrelated writers or a guarantee under disk failure. Insufficient space/inodes causes refusal without resize, paid upgrade, deletion or cleanup.

The durable root itself is the replay fence. No existing entry is replaced, deleted or reused. Root/path/record ownership, modes, file type, link count, nofollow traversal and current descriptor/path identity checks reject malformed or substituted entries. Persistent records do not contain current device numbers, so another device-only remount does not create another migration requirement.

## One attempt, revalidation, cancellation and restart

1. Disabled gate returns immediately with zero application-state storage I/O. Enabled gate requires inspection-only startup, both explicit approval literals, the exact seven-file core fingerprint and the fixed arguments.
2. Before launching a child, the parent exclusively creates and fsyncs the fixed root and a `pending` record. A parent/child crash after that point consumes the attempt. No second parent launch is allowed merely because no change is known to have occurred.
3. The fixed child takes the attempt-record lock, verifies the exact record, and fsyncs `started` before any migration work. It calls frozen `prepare()` to recompute the current snapshot under all locks. Exact digest and size must match the approved preimage. No changed file is automatically accepted and no digest is silently rebased.
4. Frozen `apply()` independently reacquires all locks and compares the exact approved digest, inventory, snapshot and row digests again. It creates verified durable backups before applying only the new cohort/header/seal/lock metadata. It preserves old enrolled references and old business rows, and completes the anchor CAS last.
5. Only after apply succeeds does the child fsync `completed`. The parent must reap a successful child and independently verify both that exact completed attempt and the full migrated cohort before starting the existing inspection web/audit flow. A success record alone is not sufficient.
6. The child watchdog is 30 seconds, followed by a bounded kill/reap check. Cancellation, nonzero exit or uncertain reap prevents web startup. No retry/respawn occurs. The watchdog cannot promise a strict wall-clock bound under an uninterruptible filesystem/kernel stall; the small parent fence operations and readonly completed-startup verification are outside the child deadline.
7. A future restart with the exact completed record performs readonly full cohort verification and never launches apply again, including after a device-only remount. An absent/malformed/partial record or corrupt cohort stops startup. A crash after cohort completion but before the completed record is durable also stops, rather than guessing success or repairing. Disabling the source gate in a later reviewed inspection-only release is optional, not necessary to prevent repetition; no automatic configuration edit or second deployment is performed.

The phrases "snapshot changed/missing" refer to the approved current preimage before apply. The saved backup is verified and synced at creation, but its later retention is not a perpetual application identity prerequisite and is not revalidated on every successful restart. Never delete or edit the attempt root to force a retry. A Render deployment rollback does not roll back persistent metadata, and must not be treated as data recovery.

## Expected evidence and remaining limits

Successful logs report `migration_completed`, `metadata_outcome=completed`, `business_rows_preserved=true`, no provider requests, no order permission, no automatic retry and no historical-continuity claim. A child exit message by itself is not sufficient; startup additionally verifies the completed record and full cohort. Refusals use a bounded allowlisted code and conservatively report unknown metadata outcome when they cannot prove the state. Raw exceptions, credentials, account records and provider responses are not logged.

After successful apply, the existing readonly audit may pass the prior storage-identity gate. Its original application/schema failure remains a separate blocker, and the exact historic schema enum was not stored and cannot be reconstructed. The schema-code detail patch and any new finite GET diagnostic remain separate, unapproved scopes.

This migration adopts a newly approved current baseline. It does not prove historical mutable-content continuity, detect a coherent whole-store rollback by a privileged/same-user actor, or guarantee preservation through an arbitrary backup/restore that changes inodes. No trading readiness, claim reset, quota refresh or incident clearance is granted. Restoring a whole cohort or resolving a partial migration requires a separately reviewed plan and approval.
