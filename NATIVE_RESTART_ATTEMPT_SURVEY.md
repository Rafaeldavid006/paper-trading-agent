# Failed-attempt survey only: no repair or retry

## Verified state and remaining question

The activated deployment reported `startup_apply_refused / unsafe_attempt_directory` on 2026-10-23 at 04:39:30 UTC and again after Render's automatic restart at 04:39:34. Complete bounded deployment logs contained no `approved_attempt_reserved`, child migration result or child-reaped marker. Render restored the preceding inspection release without an assistant rollback.

At 04:39:42.302650888 UTC the restored release's preflight verified the original ten-file preimage: digest `dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d`, eight databases, 1,896,794 bytes, original inodes and unused migration header slots. Thus the original files matched the approved baseline at that checkpoint. This is not a claim that no transient action ever occurred, nor a guarantee that later application routes cannot change data. The new attempt directory is outside that ten-file digest; its contents and the precise failed directory predicate remain unknown.

The only source throw site for `unsafe_attempt_directory` is the attempt/backup directory checker. It compares directory type, runtime ownership, exact `S_IMODE == 0700` and descriptor/path identity. The reported PARENT reason can originate from six checks of the attempt root: initial/final reservation, initial/final preexisting-root verification, or initial/final post-child-success verification. Child-side errors have a different report reason. A single refusal line alone did not resolve this ordering; the additional complete logs and matching later preflight materially narrow the observed failure.

An actual local fixture reproduced a plausible mechanism: a setgid parent mode02700 caused `mkdir(mode0700)` to create a private directory with mode02700. The exact special-bit-inclusive comparison rejected it, leaving an empty root with no attempt record and no child launch. This inheritance case was absent from the earlier test coverage. It is a confirmed local compatibility hazard, not yet the confirmed live cause. The existing permission check is NOT changed by this candidate.

## Exact proposed survey release

The source delta against the activated release is only:

1. `native_restart_apply_once.py`: both activation literals change from True back to False. No other launcher logic changes.
2. `serve.py`: replace only the inspection-supervisor import with the fixed survey supervisor alias. Inspection-only remains True.
3. Add `native_restart_attempt_survey.py`.
4. Add `test_native_restart_attempt_survey.py`.
5. `test_native_restart_preflight.py`: update the actual startup command assertion for the survey wrapper, retaining all other startup/provider exclusions.
6. Add this runbook.

All seven core migration/preflight/identity files remain byte-identical to fingerprint `da12f995c97a3502ab88c90482db37a109a576e9865d19f40bac2cbbf67210ad`. The new survey is deliberately not a migration fix. It neither calls/imports the apply launcher nor interprets an observation as migration permission. The disabled launcher gate remains a no-application-state-I/O return at startup.

The existing startup command still starts the normal inspection web service. Its finite inspection child now emits the survey first, then runs the unchanged readonly preflight and original audit sequentially. It reuses the existing independent 15-second watchdog/reap/no-respawn lifecycle. No shell, credentials, endpoint, new service or access change is needed. The child command is fixed and uses Python's `-B` option.

## Readonly evidence emitted

A bounded JSON `mode=native_restart_attempt_survey` report contains:

- Fixed parent/attempt-root/backup directory observations: presence, directory/regular/symlink/other type, octal mode INCLUDING special bits, setgid/setuid/sticky booleans, owner matches current runtime, private0700 access bits, current descriptor/path stability.
- For attempt root and backup child: empty or nonempty and a bounded count (at most16, plus capped flag). No filenames are emitted.
- The fixed `attempt.json`: safe file metadata, an allowlisted status (`pending`, `started`, `completed`, `unrecognized`), and whether the entire bounded record equals the prior exact fixed contract. No raw record, arbitrary status, account values, uid/gid numbers, inode/device numbers, exception strings or secrets are logged.
- The existing application anchor: migration marker absent/prepared/completed if its exact schema/record validates; otherwise a safe unavailable category. The inspection uses SQLite mode=ro/query_only and refuses to open SQLite when any sidecar exists. It never recovers a journal, creates a DB, writes a marker or verifies operational readiness.
- Explicit `trusted=false`, no provider requests, no write attempt, no recovery authorization, no automatic retry and no trading permission.

The readonly survey observes directory special bits even if the operational validator rejects them. Observation does not relax the gate. Symlink/FIFO/oversized/malformed records are not followed or treated as valid. Directory observations are separate snapshots, not an atomic global-state attestation. Reads can affect ordinary filesystem access times; the preservation tests compare names, bytes, sizes, ownership, modes and physical identities, not atime.

The subsequent preflight provides a new independently locked ten-file snapshot. A newly observed digest is evidence only, not automatic approval to rebase the spent migration attempt.

## What remains separately authorized

This task prepares and tests local code only. Publishing/deploying this exact survey-only delta requires a new explicit approval. The spent migration authorization is not reused to deploy a reader or run migration again.

The approval should cover disabling both apply literals, one inspection-only deployment, and reading the bounded startup logs. It must NOT authorize chmod/chown, accepting special bits in the operational gate, deleting/renaming/clearing/reusing the attempt root, backup mutation, migration retries, new claims, recovery, provider sampling or trading.

After actual directory evidence, choose the smallest justified next step. Do not change mode masks or permissions merely because the setgid hypothesis reproduced locally. No repair candidate is included here. Preserve the failed attempt as evidence, even if empty.
