"""Local-only harness: actual candidate serve signals/loop + synthetic child processes."""
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock,patch
import contextlib

HERE=Path(__file__).resolve().parent
BASE=HERE
sys.path.insert(0,str(BASE));sys.path.insert(0,str(HERE))
import serve
import collector_supervisor as cs
root=Path(sys.argv[1]); real_popen=subprocess.Popen
finite=Mock(requested=False,reaped=True)
web=None

def spawn(args,**kwargs):
    global web
    if '-m' in args:
        web=real_popen([sys.executable,'-I','-c','import time;time.sleep(60)'],stdin=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,close_fds=True)
        (root/'web.pid').write_text(str(web.pid));return web
    child=real_popen(args,**kwargs)
    (root/'owner.pid').write_text(str(child.pid));return child

with contextlib.ExitStack() as stack:
    for n in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
        stack.enter_context(patch(n,side_effect=AssertionError('network forbidden')))
    stack.enter_context(patch.object(cs,'__file__',str(root/'collector_supervisor.py')))
    stack.enter_context(patch.object(serve,'validate_environment',return_value=('/tmp/fake','8000')))
    stack.enter_context(patch.object(serve,'run_startup_gate',return_value=True))
    stack.enter_context(patch.object(serve,'_run_readonly_report',return_value=True))
    stack.enter_context(patch.object(serve,'_finite_requested',return_value=False))
    stack.enter_context(patch.object(serve,'PreparedRecoverySupervisor',return_value=finite))
    stack.enter_context(patch.object(serve,'FinitePilotSupervisor',return_value=finite))
    stack.enter_context(patch.object(serve.successor_contract,'APPROVED_SUCCESSOR',None))
    stack.enter_context(patch.object(serve.observation_contract,'APPROVED_OBSERVATION',None))
    stack.enter_context(patch.object(serve.root_contract,'APPROVED_ROOT',None))
    stack.enter_context(patch.object(serve.finite_contract,'APPROVED_PILOT',None))
    stack.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
    code=serve.main()
    (root/'outcome.json').write_text(json.dumps({'code':code,'web_returncode':web.returncode,
                                               'finite_close_called':finite.close.called}))
    raise SystemExit(code)
