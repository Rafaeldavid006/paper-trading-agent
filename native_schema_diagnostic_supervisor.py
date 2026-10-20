"""Fixed independent one-shot diagnostic using the existing 15-second watchdog."""
import json
from pathlib import Path
import subprocess
import sys
from native_observation_supervisor import NativeProbeSupervisor


class SchemaDiagnosticSupervisor(NativeProbeSupervisor):
    def __init__(self,*,enabled=False,popen=None,**kwargs):
        spawn=popen or subprocess.Popen
        def fixed_spawn(args):
            if (len(args)!=3 or args[0]!=sys.executable or args[2]!='--once'
                    or Path(args[1]).name!='native_observation_worker.py'):
                raise ValueError('diagnostic_command_refused')
            return spawn([sys.executable,str(Path(__file__).resolve().with_name('native_schema_diagnostic.py')),'--once'],
                stdin=subprocess.DEVNULL,stdout=None,stderr=subprocess.DEVNULL,shell=False,close_fds=True)
        super().__init__(enabled=enabled,popen=fixed_spawn,**kwargs)
    def _report(self,reason):
        safe={'probe_start_failed','probe_watchdog_expired','probe_finished','probe_process_failed','probe_reap_pending'}
        self._emit(json.dumps(dict(mode='native_schema_diagnostic_supervisor',
            reason=reason if reason in safe else 'diagnostic_supervisor_refused',order_permission='none')),flush=True)
