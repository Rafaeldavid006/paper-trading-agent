"""One-service supervisor: web UI and optional polling/research process share storage."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading


def validate_environment(env):
    if len(env.get('DASHBOARD_PASSWORD',''))<20:raise ValueError('DASHBOARD_PASSWORD must be at least 20 characters')
    if len(env.get('FLASK_SECRET_KEY',''))<32:raise ValueError('FLASK_SECRET_KEY must be at least 32 characters')
    path=Path(env.get('EVENT_DB','events.db'))
    if not path.parent.is_dir():raise ValueError('EVENT_DB parent directory must exist')
    port=env.get('PORT','8000')
    if not port.isdigit() or not 1<=int(port)<=65535:raise ValueError('invalid PORT')
    if env.get('ENABLE_AUTO_RESEARCH')=='true':
        if env.get('PERSISTENT_STORAGE_CONFIRMED')!='true' or not path.is_absolute():
            raise ValueError('automatic research requires confirmed persistent storage and absolute EVENT_DB')
        if env.get('ENABLE_WEB_RESEARCH')!='true':raise ValueError('web research must also be enabled')
        if any(not env.get(k) for k in ('OPENAI_API_KEY','OPENAI_MODEL','ALPACA_API_KEY','ALPACA_SECRET_KEY')):
            raise ValueError('automatic research credentials and model are missing')
    return str(path),port


def main():
    try:path,port=validate_environment(os.environ)
    except ValueError as exc:
        print(str(exc),file=sys.stderr);return 1
    stop=threading.Event();children=[];experiment=None
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    try:
        children.append(subprocess.Popen([sys.executable,'-m','gunicorn','--bind','0.0.0.0:'+port,
            '--workers','1','--threads','4','--timeout','120','wsgi:app']))
        if os.environ.get('ENABLE_AUTO_RESEARCH')=='true':
            children.append(subprocess.Popen([sys.executable,'research_worker.py','--db',path,'--interval','60']))
        # Fixed approved one-shot child, separate from research and the GET-only UI.
        # Its normal terminal exit must not restart or stop the web service.
        experiment=subprocess.Popen([sys.executable,'paper_experiment_worker.py'])
        while not stop.wait(.5):
            if any(child.poll() is not None for child in children):return 1
        return 0
    finally:
        if experiment is not None:children.append(experiment)
        for child in children:
            if child.poll() is None:child.terminate()
        for child in children:
            try:child.wait(timeout=10)
            except subprocess.TimeoutExpired:child.kill();child.wait()


if __name__=='__main__':raise SystemExit(main())
