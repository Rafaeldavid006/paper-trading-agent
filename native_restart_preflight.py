"""Inspection-startup logs only. No apply, backup, credentials or provider calls.

The existing 15-second, no-respawn supervisor runs this fixed child. Preflight
and existing-audit inspection are sequential, so their own seal locks do not
contend. A new service start may repeat only these nonmutating local reads.
"""
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import native_restart_migration as migration

SOURCE_FILES = ('native_restart_preflight.py', 'native_restart_migration.py',
    'native_restart_identity.py', 'runtime_state_bundle.py', 'native_account_guard.py',
    'readonly_runtime_worker.py', 'native_diagnostic_audit_supervisor.py')
SAFE_CODES = {
    'restart_migration_sidecar_present': 'sidecar_present',
    'restart_migration_not_device_only': 'inode_or_device_cohort_mismatch',
    'restart_migration_header_slots_in_use': 'header_slots_in_use',
    'restart_migration_lock_not_empty': 'legacy_lock_not_empty',
    'restart_migration_already_started': 'migration_already_started',
    'restart_migration_binding_invalid': 'account_binding_invalid',
    'restart_migration_manifest_invalid': 'manifest_invalid',
    'restart_migration_reference_invalid': 'stored_references_invalid',
    'restart_migration_inventory_invalid': 'logical_inventory_invalid',
    'restart_migration_legacy_revision_changed': 'legacy_revision_changed',
    'restart_migration_file_changed': 'physical_identity_changed',
    'restart_migration_snapshot_too_large': 'snapshot_too_large',
    'bundle_file_replaced': 'physical_identity_changed',
    'restart_identity_file_changed': 'physical_identity_changed',
    'unsafe_bundle_file': 'unsafe_file',
    'bundle_database_corrupt': 'database_header_invalid',
    'bundle_anchor_history_changed': 'historical_anchor_changed',
    'bundle_generation_mismatch': 'generation_stamp_mismatch',
}


def report(reason, **extra):
    result = dict(mode='native_restart_identity_preflight', reason=reason,
        provider_requests=0, state_write_attempted=False, backup_created=False,
        migration_applied=False, execution_ready=False, order_permission='none',
        historical_continuity_proven=False, global_quiescence_proven=False,
        continuing_quiescence_proven=False, writer_exclusion_during_snapshot=False,
        snapshot_verified=False, approval_required=True)
    result.update(extra)
    return result


def source_digest():
    root=Path(__file__).resolve().parent
    return hashlib.sha256(json.dumps([(n,hashlib.sha256((root/n).read_bytes()).hexdigest())
        for n in SOURCE_FILES],separators=(',',':')).encode()).hexdigest()


def safe_error(exc):
    if isinstance(exc, BlockingIOError):return 'file_lock_busy'
    if isinstance(exc, FileNotFoundError):return 'existing_file_missing'
    if isinstance(exc, PermissionError):return 'existing_file_access_refused'
    sqlite_code=getattr(exc,'sqlite_errorcode',None)
    if isinstance(exc, sqlite3.OperationalError) and type(sqlite_code) is int and (sqlite_code & 0xff) in (
            sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED):return 'sqlite_writer_lock_busy'
    code=exc.args[0] if len(exc.args)==1 and type(exc.args[0]) is str else None
    return SAFE_CODES.get(code,'preflight_unavailable')


def run_once(*, enabled=False, paths=None, test_only=False):
    if type(enabled) is not bool or type(test_only) is not bool or (paths is not None and not test_only):
        return report('preflight_refused',code='invalid_arguments')
    if not enabled:return report('preflight_disabled')
    code_digest=None
    try:
        code_digest=source_digest()
        # prepare holds the existing seal/legacy flocks and reserved writer
        # locks on all eight DBs for its entire verified snapshot. It performs
        # no commits, schema changes, backup creation, repair or data writes.
        plan=migration.prepare(paths=paths,test_only=test_only)
        inventory=json.loads(plan.inventory);snapshot=json.loads(plan.snapshot)
        current=dict(plan.current)
        return report('snapshot_observed_not_apply_authorized',
            code='preflight_checks_passed',preflight_code_digest=code_digest,
            approval_snapshot_digest=plan.digest,approval_digest_scope='data_preimage_only',snapshot_verified=True,
            writer_exclusion_during_snapshot=True,
            verified_file_count=len(snapshot),verified_database_count=len(migration.DB_ROLES),
            snapshot_bytes=sum(item['size'] for item in snapshot.values()),
            original_inodes_match=True,account_and_revision_match=True,
            stored_references_consistent=True,schemas_and_history_valid=True,
            unused_header_slots_verified=True,sidecars_absent=True,
            device_drift_observed=any(current[role][0]!=item['device']
                for role,item in inventory['files'].items()))
    except Exception as exc:
        extra={'preflight_code_digest':code_digest} if code_digest is not None else {}
        return report('preflight_refused',code=safe_error(exc),**extra)


def emit(result):
    raw=json.dumps(result,sort_keys=True,separators=(',',':'),allow_nan=False)
    if len(raw)>4096:
        raw=json.dumps(report('preflight_refused',code='report_bound_exceeded'),sort_keys=True)
    print(raw,flush=True)


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if args!=['--inspect-restart-preflight']:
        emit(report('preflight_refused',code='invalid_arguments'));return 1
    emit(report('preflight_started'))
    result=run_once(enabled=True)
    emit(result)
    # Release all preflight locks before invoking the unchanged existing audit.
    # Audit failure is independent evidence, never a migration permission.
    from native_diagnostic_audit_inspection import main as inspect_existing
    audit_status=inspect_existing(['--inspect-existing-audit'])
    return 0 if result['snapshot_verified'] and audit_status==0 else 1


if __name__=='__main__':raise SystemExit(main())
