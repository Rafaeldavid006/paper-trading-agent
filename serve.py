"""One-service supervisor: web UI and optional polling/research process share storage."""
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


def main():
    try:path,port=validate_environment(os.environ)
    except ValueError as exc:
        print(str(exc),file=sys.stderr);return 1
    stop=threading.Event();children=[];experiment=None;experiment_observed=False
    # One fixed-generation GET-only release probe. Its durable pre-network claim
    # prevents a restart/redeploy from replaying a completed or uncertain attempt.
    native_probe=NativeProbeSupervisor(enabled=True)
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    try:
        children.append(subprocess.Popen([sys.executable,'-m','gunicorn','--bind','0.0.0.0:'+port,
            '--workers','1','--threads','4','--timeout','120','wsgi:app']))
        if os.environ.get('ENABLE_AUTO_RESEARCH')=='true':
            children.append(subprocess.Popen([sys.executable,'research_worker.py','--db',path,'--interval','60']))
        # Fixed approved one-shot child, separate from research and the GET-only UI.
        # Its normal terminal exit must not restart or stop the web service.
        experiment=subprocess.Popen([sys.executable,'paper_renewal_worker.py'])
        native_probe.start()
        while not stop.wait(.5):
            native_probe.poll()
            if any(child.poll() is not None for child in children):return 1
            if not experiment_observed:
                code=experiment.poll()
                if code is not None:
                    experiment_observed=True
                    if code!=0 and not stop.is_set():record_experiment_failure(code)
        return 0
    finally:
        native_probe.close()
        if experiment is not None:children.append(experiment)
        for child in children:
            if child.poll() is None:child.terminate()
        for child in children:
            try:child.wait(timeout=10)
            except subprocess.TimeoutExpired:child.kill();child.wait()


if __name__=='__main__':raise SystemExit(main())
