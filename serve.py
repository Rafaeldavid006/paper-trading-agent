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
import finite_recovery_successor_contract as successor_contract
from finite_recovery_successor_supervisor import SuccessorSupervisor
import finite_observation_contract as observation_contract
from finite_observation_supervisor import ObservationSupervisor
import finite_root_contract as root_contract
from finite_root_supervisor import RootSupervisor
from prepared_recovery_supervisor import PreparedRecoverySupervisor
from collector_supervisor import CollectorSupervisor

# Web routes remain inspection-only even if separately compiled finite grants
# are supplied. This flag is not an audit-inspector or trading activation toggle.
INSPECT_EXISTING_AUDIT_ONLY = True
REPORT_RECOVERY_STATE = True  # Compiled read-only diagnostic; no trading authority.


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


def _run_readonly_report(stop):
    """One owned read-only child; bounded deadline/reap, no retry on any outcome."""
    child = None
    reaped = False
    reason = 'report_start_failed'
    try:
        if stop.is_set():
            return True
        child = subprocess.Popen([sys.executable, str(Path(__file__).resolve().with_name(
            'finite_recovery_state_report.py')), '--read-existing-state'],
            stdin=subprocess.DEVNULL, stdout=None, stderr=subprocess.DEVNULL,
            shell=False, close_fds=True)
        deadline = time.monotonic() + 10
        while True:
            code = child.poll()
            if code is not None:
                reaped = True
                reason = 'report_finished' if type(code) is int and code == 0 else 'report_refused'
                break
            if stop.wait(.1):
                reason = 'report_cancelled'
                break
            if time.monotonic() >= deadline:
                reason = 'report_timeout'
                break
    except Exception:
        reason = 'report_process_uncertain'
    finally:
        if child is not None and not reaped:
            try:
                child.kill()
            except Exception:
                pass
            try:
                child.wait(timeout=1)
                reaped = True
            except Exception:
                reaped = False
        print(json.dumps(dict(mode='finite_recovery_report_supervisor', reason=reason,
            child_reaped=reaped, automatic_retry=False, provider_requests=0,
            order_permission='none')), flush=True)
    return reaped


def _run_inventory_report(stop):
    """One local diagnostic child; no retry and no alternate admission gate."""
    child = None
    reaped = False
    reason = 'inventory_start_uncertain'
    try:
        if stop.is_set():
            reaped = True
            reason = 'inventory_not_started'
            return True
        child = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve().with_name(
            'native_state_inventory.py')), '--read-existing-state'],
            stdin=subprocess.DEVNULL, stdout=None, stderr=subprocess.DEVNULL,
            shell=False, close_fds=True)
        deadline = time.monotonic() + 10
        while True:
            code = child.poll()
            if code is not None:
                reaped = True
                reason = 'inventory_finished' if type(code) is int and code == 0 else 'inventory_failed'
                break
            if stop.wait(.1):
                reason = 'inventory_cancelled'
                break
            if time.monotonic() >= deadline:
                reason = 'inventory_timeout'
                break
    except Exception:
        reason = 'inventory_process_uncertain'
    finally:
        if child is not None and not reaped:
            try:
                child.kill()
            except Exception:
                pass
            try:
                child.wait(timeout=1)
                reaped = True
            except Exception:
                reaped = False
        print(json.dumps(dict(mode='native_state_inventory_supervisor', reason=reason,
            child_reaped=reaped, automatic_retry=False, provider_requests=0,
            order_permission='none')), flush=True)
    return reaped


def _run_session_receipt(stop, action):
    """One local write-only receipt child after finite reap; no account IO/retry."""
    if action not in ('halt','close'):raise ValueError('invalid_session_receipt')
    child = None
    reaped = False
    reason = 'report_start_failed'
    try:
        if stop.is_set():
            return True
        child = subprocess.Popen([sys.executable, str(Path(__file__).resolve().with_name(
            'finite_pilot_session_terminal.py')), '--'+action],
            stdin=subprocess.DEVNULL, stdout=None, stderr=subprocess.DEVNULL,
            shell=False, close_fds=True)
        deadline = time.monotonic() + 10
        while True:
            code = child.poll()
            if code is not None:
                reaped = True
                reason = 'report_finished' if type(code) is int and code == 0 else 'report_refused'
                break
            if stop.wait(.1):
                reason = 'report_cancelled'
                break
            if time.monotonic() >= deadline:
                reason = 'report_timeout'
                break
    except Exception:
        reason = 'report_process_uncertain'
    finally:
        if child is not None and not reaped:
            try:
                child.kill()
            except Exception:
                pass
            try:
                child.wait(timeout=1)
                reaped = True
            except Exception:
                reaped = False
        print(json.dumps(dict(mode='finite_session_receipt_supervisor', action=action, reason=reason,
            child_reaped=reaped, automatic_retry=False, provider_requests=0,
            order_permission='none')), flush=True)
    return reaped


class SignalSafeStop:
    """Event-compatible stop observation without locks inside signal handlers.

    A Python signal can interrupt Event.wait while its condition lock is held;
    calling Event.set from that handler would deadlock the same thread. The
    handler therefore records only a flag. Normal waits retain their existing
    bounded timeout and observe the flag again after returning.
    """
    def __init__(self,event):
        self.event=event
        self.requested=False

    def set(self):
        self.requested=True

    def is_set(self):
        return self.requested or self.event.is_set()

    def wait(self,timeout=None):
        if self.requested:return True
        result=self.event.wait(timeout)
        return self.requested or result


def main():
    stop = SignalSafeStop(threading.Event())
    try:
        _, port = validate_environment(os.environ)
        if INSPECT_EXISTING_AUDIT_ONLY is not True:
            raise ValueError('inspection_service_required')
        prepared_runtime = PreparedRecoverySupervisor(external_stop=stop)
        prepared_requested = prepared_runtime.requested
        successor_requested = successor_contract.APPROVED_SUCCESSOR is not None
        if successor_requested:
            successor_contract.require_successor_manifest(successor_contract.APPROVED_SUCCESSOR)
        finite_requested = _finite_requested()
        root_requested=root_contract.APPROVED_ROOT is not None
        if root_requested:
            root_contract.require_root(root_contract.APPROVED_ROOT)
            now=time.time_ns()
            if type(now) is not int or now<=0:raise ValueError("invalid_root_clock")
            root_requested=now<root_contract.APPROVED_ROOT.expires_ns
        observation_grant=observation_contract.APPROVED_OBSERVATION
        observation_requested=observation_grant is not None
        if observation_requested:
            observation_contract.approved(observation_grant)
            if finite_requested or successor_requested:raise ValueError('mixed_observation_scope')
            now=time.time_ns()
            if type(now) is not int or now<=0:raise ValueError('invalid_observation_clock')
            # Expired historical authority is inspection-only: no new file/child.
            observation_requested=now<observation_grant.expires_ns
        admission_requested = (type(finite_contract.APPROVED_PILOT) is finite_contract.PilotManifest
                               and (finite_contract.APPROVED_PILOT.admission is not None
                                    or finite_contract.APPROVED_PILOT.session_admission is not None))
        session_requested = (type(finite_contract.APPROVED_PILOT) is finite_contract.PilotManifest
                             and finite_contract.APPROVED_PILOT.session_admission is not None)
        if successor_requested and finite_requested:
            raise ValueError('mixed_successor_scope')
        if prepared_requested and any(x is not None for x in (
                finite_contract.APPROVED_PILOT,finite_contract.APPROVED_RECOVERY,finite_contract.APPROVED_INSPECTION,
                successor_contract.APPROVED_SUCCESSOR,observation_contract.APPROVED_OBSERVATION,root_contract.APPROVED_ROOT)):
            raise ValueError('mixed_prepared_scope')
    except (ValueError, TypeError):
        print(json.dumps(dict(mode='finite_paper_startup', reason='startup_configuration_refused',
            order_permission='none', automatic_retry=False)), file=sys.stderr, flush=True)
        return 1
    children = []
    collector_runtime = CollectorSupervisor()
    finite_runtime = (prepared_runtime if prepared_requested else
                      RootSupervisor(external_stop=stop) if root_requested else
                      ObservationSupervisor(external_stop=stop) if observation_requested else
                      SuccessorSupervisor(enabled=True) if successor_requested
                      else FinitePilotSupervisor(enabled=finite_requested))
    runtime_requested = prepared_requested or finite_requested or successor_requested or observation_requested or root_requested
    finite_terminal = False
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    try:
        # Keep the current synchronous, readonly completed-migration verifier.
        # Neither migration apply authority nor its frozen receipt is changed.
        # Local bounded inventory cannot replace or relax the completed-state gate.
        if not _run_inventory_report(stop):
            return 1
        if stop.is_set():
            return 0
        if not run_startup_gate(inspection_only=True, stop=stop):
            return 0 if stop.is_set() else 1
        if stop.is_set():
            return 0
        from paper_account_diagnostic_supervisor import run_report as run_account_report
        if not run_account_report(stop):
            return 1
        if stop.is_set():
            return 0
        if REPORT_RECOVERY_STATE and not successor_requested and not admission_requested and not root_requested:
            if finite_requested or not _run_readonly_report(stop):
                return 1
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
        if runtime_requested and not stop.is_set():
            finite_runtime.start()
        if not stop.is_set():
            collector_runtime.start()
        session_close_pending = False
        while not stop.wait(.5):
            collector_runtime.poll()
            collector_status = collector_runtime.report_if_due()
            if collector_status is not None:
                collector_runtime.emit_report(collector_status)
            if runtime_requested and not finite_terminal:
                finite_runtime.poll()
                if finite_runtime.completed or finite_runtime.halted:
                    finite_terminal = True
                    finite_runtime.close()  # Bounded reap; never schedule again.
                    if prepared_requested:
                        print(json.dumps(dict(mode='prepared_recovery_startup',reason='worker_finished' if finite_runtime.completed else 'worker_halted',order_permission='none',automatic_retry=False,child_reaped=finite_runtime.reaped,broker_flatness_verified=False)),flush=True)
                    elif observation_requested:
                        print(json.dumps(dict(mode='finite_observation_startup',reason='observation_finished' if finite_runtime.completed else 'observation_halted',order_permission='none',automatic_retry=False,child_reaped=finite_runtime.reaped)),flush=True)
                    else:_finite_terminal(finite_runtime)
                    if not finite_runtime.reaped:
                        return 1
                    if session_requested and not stop.is_set():
                        if not _run_session_receipt(stop,'halt'):return 1
                        session_close_pending = True
                    if (prepared_requested or successor_requested or admission_requested or observation_requested or root_requested) and finite_runtime.reaped and not stop.is_set():
                        # Exactly one post-terminal read-only receipt check, not a
                        # recovery restart. A report failure never retries.
                        if not _run_readonly_report(stop):
                            return 1
            if session_close_pending and time.time_ns()>=finite_contract.APPROVED_PILOT.expires_ns:
                session_close_pending=False  # one bounded attempt, even if refused
                if not _run_session_receipt(stop,'close'):return 1
                if REPORT_RECOVERY_STATE and not _run_readonly_report(stop):return 1
            if any(child.poll() is not None for child in children):
                return 1
        return 0
    finally:
        # Signal independent owner and web before bounded existing finite cleanup.
        collector_runtime.begin_stop()
        for child in children:
            if child.poll() is None:
                child.terminate()
        finite_runtime.close()
        collector_runtime.close()
        collector_runtime.emit_report(collector_runtime.status())
        if (prepared_requested or observation_requested or root_requested) and not finite_runtime.reaped:
            print(json.dumps(dict(mode='finite_observation_startup',reason='child_reaping_uncertain',order_permission='none',automatic_retry=False)),flush=True)
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

        if not collector_runtime.reaped:return 1
        if (prepared_requested or observation_requested or root_requested) and not finite_runtime.reaped:return 1


if __name__=='__main__':raise SystemExit(main())
