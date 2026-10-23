"""Fixed bounded preflight plus existing-audit child. No provider or apply path."""
import json
from pathlib import Path
import subprocess
import sys
from native_observation_supervisor import NativeProbeSupervisor


class DiagnosticAuditSupervisor(NativeProbeSupervisor):
    def __init__(self, *, enabled=False, popen=None, **kwargs):
        spawn = popen or subprocess.Popen
        def fixed_spawn(args):
            if (len(args) != 3 or args[0] != sys.executable or args[2] != '--once'
                    or Path(args[1]).name != 'native_observation_worker.py'):
                raise ValueError('audit_inspection_command_refused')
            return spawn([sys.executable, str(Path(__file__).resolve().with_name(
                'native_restart_preflight.py')), '--inspect-restart-preflight'],
                stdin=subprocess.DEVNULL, stdout=None, stderr=subprocess.DEVNULL,
                shell=False, close_fds=True)
        super().__init__(enabled=enabled, popen=fixed_spawn, **kwargs)

    def _report(self, reason):
        safe = {'probe_start_failed', 'probe_watchdog_expired', 'probe_finished',
                'probe_process_failed', 'probe_reap_pending'}
        self._emit(json.dumps(dict(mode='native_diagnostic_audit_supervisor',
            reason=reason if reason in safe else 'audit_supervisor_refused',
            provider_requests=0, order_permission='none')), flush=True)
