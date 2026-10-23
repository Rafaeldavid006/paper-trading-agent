"""Inspection-only health service with separately approved finite PAPER children.

The completed storage gate runs synchronously before every child. Historical
probe, audit, GET, schema, research and recurring runtime invocations are retired.
A terminal finite phase keeps only the readonly health service alive, so service
restarts cannot be used as a substitute for durable phase/replay controls.
"""
import os
import json
import sqlite3
import time
from pathlib import Path
import signal
import subprocess
import sys
import threading
from native_observation_supervisor import NativeProbeSupervisor
from readonly_runtime_supervisor import ReadonlyRuntimeSupervisor
from native_schema_diagnostic_supervisor import SchemaDiagnosticSupervisor
from native_postmigration_supervisors import ExistingAuditSupervisor as DiagnosticAuditSupervisor
from native_postmigration_supervisors import GetDiagnosticSupervisor
from native_postmigration_supervisors import verify_completed_storage as run_startup_gate
from finite_pilot_supervisor import FinitePilotSupervisor
import finite_pilot_contract as finite_contract

# Web routes remain inspection-only even if separately compiled finite grants
# are supplied. This flag is not an audit-inspector or trading activation toggle.
INSPECT_EXISTING_AUDIT_ONLY = True


def validate_environment(env):
    if len(env.get('DASHBOARD_PASSWORD',''))<20:raise ValueError('DASHBOARD_PASSWORD must be at least 20 characters')
    if len(env.get('FLASK_SECRET_KEY',''))<32:raise ValueError('FLASK_SECRET_KEY must be at least 32 characters')
    path=Path(env.get('EVENT_DB','events.db'))
    if not path.parent.is_dir():raise ValueError('EVENT_DB parent directory must exist')
    port=env.get('PORT','8000')
    if not port.isdigit() or not 1<=int(port)<=65535:raise ValueError('invalid PORT')
    if not INSPECT_EXISTING_AUDIT_ONLY and env.get('ENABLE_AUTO_RESEARCH')=='true':
        if env.get('PERSISTENT_STORAGE_CONFIRMED')!='true' or not path.is_absolute():
            raise ValueError('automatic research requires confirmed persistent storage and absolute EVENT_DB')
        if env.get('ENABLE_WEB_RESEARCH')!='true':raise ValueError('web research must also be enabled')
        if any(not env.get(k) for k in ('OPENAI_API_KEY','OPENAI_MODEL','ALPACA_API_KEY','ALPACA_SECRET_KEY')):
            raise ValueError('automatic research credentials and model are missing')
    return str(path),port



def record_experiment_failure(exit_code, *, database='/var/data/paper-readonly.sqlite', emit=print):
    """Make unexpected child death visible without retrying or resetting anything."""
    recorded=False
    try:
        if Path(database).is_symlink() or not Path(database).is_file():
            raise ValueError('Existing regular state file required')
        # rw refuses to create missing state. Never seed an approval or a new run.
        connection=sqlite3.connect(Path(database).absolute().as_uri()+'?mode=rw',uri=True,timeout=15)
        try:
            with connection:
                connection.execute('BEGIN IMMEDIATE')
                # Preserve the old terminal record; report the active fixed generation.
                for table in ('paper_experiment','paper_renewal'):
                    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone():continue
                    result=connection.execute("UPDATE "+table+"""
                        SET phase='manual_attention',reason='worker_failed',updated=?
                        WHERE id=1 AND phase NOT IN ('completed','no_fill','blocked','manual_attention','stopped')""",(time.time(),))
                    recorded=recorded or result.rowcount==1
        finally:connection.close()
    except Exception:
        pass  # Raw exceptions can include private filesystem/provider details.
    emit(json.dumps(dict(mode='paper_one_shot_supervisor',reason='worker_failed',
                         exit_code=exit_code,attention_recorded=recorded)),flush=True)


def _finite_requested():
    """Only exact compiled grants; no environment or caller-supplied override."""
    pilot, recovery, proof = (finite_contract.APPROVED_PILOT,
                              finite_contract.APPROVED_RECOVERY,
                              finite_contract.APPROVED_INSPECTION)
    if proof is not None and (pilot is not None or recovery is not None):
        raise finite_contract.PilotRefused('mixed_inspection_scope')
    for grant, check in ((pilot, finite_contract.require_pilot_manifest),
                         (recovery, finite_contract.require_recovery_manifest),
                         (proof, finite_contract.require_inspection_manifest)):
        if grant is not None:
            check(grant)
    return any(grant is not None for grant in (pilot, recovery, proof))


def _finite_terminal(runtime):
    """One bounded status, never child output, identity or exception text."""
    reaped = runtime.reaped is True
    print(json.dumps(dict(mode='finite_paper_startup',
        reason='finite_stage_halted' if runtime.halted else 'finite_scope_completed',
        order_permission='none', execution_ready=False, automatic_retry=False,
        inspection_health_only=reaped, finite_child_reaped=reaped)), flush=True)


def main():
    try:
        _, port = validate_environment(os.environ)
        if INSPECT_EXISTING_AUDIT_ONLY is not True:
            raise ValueError('inspection_service_required')
        finite_requested = _finite_requested()
    except (ValueError, TypeError):
        print(json.dumps(dict(mode='finite_paper_startup', reason='startup_configuration_refused',
            order_permission='none', automatic_retry=False)), file=sys.stderr, flush=True)
        return 1
    stop = threading.Event()
    children = []
    finite_runtime = FinitePilotSupervisor(enabled=finite_requested)
    finite_terminal = False
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    try:
        # Keep the current synchronous, readonly completed-migration verifier.
        # Neither migration apply authority nor its frozen receipt is changed.
        if not run_startup_gate(inspection_only=True, stop=stop):
            return 0 if stop.is_set() else 1
        if stop.is_set():
            return 0
        children.append(subprocess.Popen([sys.executable, '-m', 'gunicorn',
            '--bind', '0.0.0.0:' + port, '--workers', '1', '--threads', '4',
            '--timeout', '120', 'inspection_wsgi:app'], stdin=subprocess.DEVNULL,
            shell=False, close_fds=True))
        # The frozen audit inspector intentionally is NOT invoked: recovery may
        # append audit rows/change the runner, making its historical exact-state
        # assumptions inapplicable. Its validation and code pin remain intact.
        # Likewise never repeat the consumed Oct23 GET diagnostic or old loops.
        if finite_requested and not stop.is_set():
            finite_runtime.start()
        while not stop.wait(.5):
            if finite_requested and not finite_terminal:
                finite_runtime.poll()
                if finite_runtime.completed or finite_runtime.halted:
                    finite_terminal = True
                    finite_runtime.close()  # Bounded reap; never schedule again.
                    _finite_terminal(finite_runtime)
            if any(child.poll() is not None for child in children):
                return 1
        return 0
    finally:
        finite_runtime.close()
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                try:
                    child.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    print(json.dumps(dict(mode='finite_paper_startup', reason='web_reap_pending',
                        order_permission='none', automatic_retry=False)), flush=True)


if __name__=='__main__':raise SystemExit(main())
