# Confirmed SGID correction and separately approved attempt 2

## Confirmed cause, not a hypothesis

Live inspection on 2026-10-23 at 05:41:14 UTC observed the persistent parent directory with mode 02775/setgid, and the failed attempt root with mode 02700/setgid, current-runtime ownership, private 0700 access bits, stable identity and zero entries. The attempt record and backup child were absent; the migration marker was absent. The original exact 0700 comparison included special bits, so it rejected the inherited setgid bit before the first child launch. This inheritance case was missing from the initial invocation test coverage.

The 05:41:15 preflight again verified the original ten-file preimage digest `dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d`, eight databases and 1,896,794 bytes. Health was 200, inspection-only, research paused, no orders. This establishes current preimage equality at that checkpoint, not permanent continuity or permission to reuse the spent attempt.

Reported current source base: `ff72f7a5cfac599fee5303dcb349f37678dfdb1b`. This task prepares local code/tests only. Nothing is published, deployed, migrated, repaired, reset or permission-changed here.

## Minimal correction

- The launcher accepts exactly directory modes 0700 or 02700. It still requires directory type, current-runtime ownership, nofollow opening, safe parent traversal and matching current descriptor/path device+inode. There is no broad special-bit mask. Setuid, sticky, combinations such as 03700/04700/06700 and any group/other access bits remain refused.
- The migration backup-directory initial and repeated validations use the same exact mode pair; repeated checks also retain current ownership. Both the new attempt root and its snapshot child naturally inherit 02700 from the actual 02775 parent. No chmod, chown, umask change, permission widening or ownership rewrite is added.
- Regular attempt/backup files are still created 0600. The attempt-record validator is unchanged. No regular-file special-bit exception is introduced. The existing backup-file readback logic is not broadened by this directory-only change.
- The fixed backup/attempt root gains only the literal suffix `-attempt-2`. No automatic attempt number, timestamp, fallback namespace, deletion, renaming or cleanup is implemented. The old empty root is never reused or changed.
- `serve.py` returns to the original fixed preflight/audit supervisor after the apply gate. The previous survey module remains available only as a historical standalone diagnostic; its old-root/core constants are preserved.

Only one of the seven selected core files changes: `native_restart_migration.py`. The updated selected-code fingerprint is `af5e6a36b741ed0b6ed91903ac06a9175d23400bf7a36e24073169c1fac03862`. The launcher is pinned to that exact fingerprint. As before, this selected fingerprint is not an attestation of the launcher/serve source; approval must also bind the exact reviewed package and proposed activation diff.

## Exact new path and approval boundary

The existing failed root remains:

`/var/data/native-restart-identity-backup-dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d`

The ONLY new allowed root for a separately approved attempt is:

`/var/data/native-restart-identity-backup-dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d-attempt-2`

Both activation literals remain False in the candidate. The separate proposed activation patch changes only those two literals to True. No runtime request/environment switch can enable it. Approval of attempt 1 is not reused; any publication/deployment that applies this correction and starts attempt 2 requires explicit new approval identifying the new namespace, unchanged data digest, updated code digest and exact reviewed source.

The approval must cover accepting inherited SGID only alongside private 0700 access, creating the new private attempt/backup records, and one finite restart-identity metadata migration. It must preserve the no-manual-writers maintenance condition and all existing business rows, preexisting schema definitions, consumed claims, quotas, incidents and trading records. No state reset, original claim replay, permission change, recovery, provider request or trading activation is included.

## Preserved execution and failure behavior

The existing pre-web gate, minimum 64 MiB/32-free-inode checks, durable no-repeat root/record, 30-second child watchdog, kill/reap handling, exact preimage revalidation under locks, verified/fsynced backups, prepared-before-stamps ordering and completed-last CAS remain unchanged. Missing or changed files, unexpected modes/ownership, inode/path changes, sidecars, account/revision differences or any approved-preimage mismatch still refuse without silent rebasing.

New-root creation consumes attempt 2 even if a later step fails. Pending, started, torn or uncertain records stop subsequent startup rather than trigger a retry. A successful restart requires the exact completed record plus full cohort verification. A later device-only remount can pass readonly verification without migration; changed inodes or cohort mismatches still stop. A partially applied migration requires separate review and authorization, never an automatic resume/rollback/restore. The 30-second watchdog does not promise to bound uninterruptible filesystem/kernel stalls.

The actual Render disk handoff and one-instance facts must remain applicable, and the user must not introduce a new shell/manual file writer during the maintenance window. Neither those facts nor final row checks claim exclusion of hostile same-user writers. The old failed root's existence is not itself a lock or proof of quiescence.

After apply, the legacy preflight is expected to report that migration already exists; the subsequent original audit still runs and can provide the relevant trusted storage verification. Do not treat that expected legacy-preflight refusal as proof of a failed migration, nor claim that metadata success fixes the original schema failure. Migration success requires the launcher's completed result and full cohort checks. The exact historic schema enum was never persisted and remains unrecoverable.

This accepts a newly approved current baseline, not historical content continuity or protection against coherent whole-store rollback. Same-disk backup is not disaster recovery, and later backup retention is not a perpetual startup identity prerequisite.

## Local validation and scope

The new tests use an actual disposable 02775 parent, observe 02700 inheritance for both attempt and snapshot directories, perform full synthetic migration, compare every backup byte and business row, retain the old root's identity/mode/owner/group/contents, and verify a later remount does not spawn another attempt. They reject other special/access bits and wrong owner; inject a bad mode between backup checks; refuse a changed preimage; preserve the old empty fence; and exercise an actual process crash after the guard stamp with no restart replay.

Existing launcher cancellation/timeout/unknown-outcome, mixed-cohort, quota/claim, path substitution, corruption and crash/restart suites remain active. Historical survey tests change only assumptions that the survey is the current startup supervisor/current code pin; the historical reader's constants are not rewritten.
