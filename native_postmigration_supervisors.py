"""Inspection startup after migration: completion check, audit, optional GET child.

No migration invocation. Both migration source gates must remain disabled.
Existing 15-second supervisor supplies cancellation, child reap and no respawn.
"""
import json
from pathlib import Path
import subprocess
import sys

import native_get_diagnostic as diagnostic
import native_restart_apply_once as migration_gate
from native_observation_supervisor import NativeProbeSupervisor


def verify_completed_storage(*,inspection_only,stop):
    """Retain synchronous completed-cohort check; do not call apply startup hook."""
    try:
        if inspection_only is not True or stop.is_set():return False
        if (migration_gate.ENABLE_APPROVED_MIGRATION is not False or
                migration_gate.EXTERNAL_WRITER_QUIESCENCE_CONFIRMED is not False):
            raise ValueError('migration_gate_not_retired')
        verified=migration_gate._completed_attempt(migration_gate.BACKUP_ROOT,
            migration_gate.APPROVED_SNAPSHOT_DIGEST,migration_gate.EXPECTED_SNAPSHOT_BYTES)
        return verified and not stop.is_set()
    except Exception:
        print(json.dumps(dict(mode='native_postmigration_startup',reason='completed_storage_refused',
            provider_requests=0,order_permission='none',automatic_retry=False)),flush=True)
        return False


class FixedSupervisor(NativeProbeSupervisor):
    target=None;argument=None;mode=None
    def __init__(self,*,enabled=False,popen=None,**kwargs):
        spawn=popen or subprocess.Popen
        def fixed(args):
            if (len(args)!=3 or args[0]!=sys.executable or args[2]!='--once' or
                    Path(args[1]).name!='native_observation_worker.py'):
                raise ValueError('diagnostic_command_refused')
            return spawn([sys.executable,str(Path(__file__).resolve().with_name(self.target)),self.argument],
                stdin=subprocess.DEVNULL,stdout=None,stderr=subprocess.DEVNULL,shell=False,close_fds=True)
        super().__init__(enabled=enabled,popen=fixed,**kwargs)
    def _report(self,reason):
        safe={'probe_start_failed','probe_watchdog_expired','probe_finished','probe_process_failed','probe_reap_pending'}
        self._emit(json.dumps(dict(mode=self.mode,reason=reason if reason in safe else 'supervisor_refused',
            order_permission='none',automatic_retry=False)),flush=True)
    @property
    def reaped(self):
        return not self.enabled or (self._started and self._finished and
                (self._child is None or self._child.poll() is not None))


class ExistingAuditSupervisor(FixedSupervisor):
    target='native_diagnostic_audit_inspection.py';argument='--inspect-existing-audit'
    mode='native_postmigration_existing_audit'


class GetDiagnosticSupervisor(FixedSupervisor):
    target='native_get_diagnostic.py';argument='--once';mode='native_get_diagnostic_supervisor'
    def __init__(self,*,enabled=None,**kwargs):
        super().__init__(enabled=diagnostic.ENABLE_APPROVED_GET_DIAGNOSTIC if enabled is None else enabled,**kwargs)
