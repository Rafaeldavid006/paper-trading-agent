"""Actual inspection_wsgi + real gunicorn/Flask; only disposable fixture DB/loopback."""
import hashlib
import gc
import warnings
import http.client
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

source, report_path = map(Path, sys.argv[1:3])
warning_categories = []
unraisable_types = []
warnings.showwarning = lambda message, category, filename, lineno, file=None, line=None: warning_categories.append(category.__name__)
warnings.simplefilter('always')
sys.unraisablehook = lambda event: unraisable_types.append(type(event.exc_value).__name__)
sys.path.insert(0, str(source))
with tempfile.TemporaryDirectory(prefix='web-fixture-') as fixture:
    database = Path(fixture) / 'events.sqlite'
    os.environ.clear()
    os.environ.update(EVENT_DB=str(database), DASHBOARD_PASSWORD='synthetic-password-not-a-real-credential',
                      FLASK_SECRET_KEY='synthetic-signing-key-not-a-real-credential',
                      HOME=fixture, TMPDIR=fixture, PYTHONDONTWRITEBYTECODE='1',
                      PYTHONWARNINGS='always::ResourceWarning')
    from event_core import Store
    from webapp import create_app
    create_app(Store(database), paper_adapter=None)
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    child = None
    child_log = tempfile.TemporaryFile()
    result = {'passed': False, 'source_requests': 0, 'order_permission': 'none',
              'scope': 'real_inspection_wsgi_with_synthetic_database'}
    try:
        child = subprocess.Popen([sys.executable, '-m', 'gunicorn', '--workers', '1',
                    '--bind', '127.0.0.1:18765', '--timeout', '10', 'inspection_wsgi:app'],
                    cwd=source, env=dict(os.environ), stdin=subprocess.DEVNULL,
                    stdout=child_log, stderr=child_log,
                    close_fds=True, start_new_session=True)
        deadline = time.monotonic() + 15
        ready = False
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise RuntimeError('gunicorn_exited_before_ready')
            connection = http.client.HTTPConnection('127.0.0.1', 18765, timeout=1)
            try:
                connection.request('GET', '/health')
                response = connection.getresponse()
                body = json.loads(response.read(65537))
                ready = response.status == 200 and body.get('mode') == 'inspection_only' and body.get('order_permission') == 'none'
            except (OSError, ValueError):
                pass
            finally:
                connection.close()
            if ready:
                break
            time.sleep(0.1)
        if not ready:
            raise RuntimeError('health_not_ready')
        connection = http.client.HTTPConnection('127.0.0.1', 18765, timeout=2)
        try:
            connection.request('POST', '/collect', body=b'')
            response = connection.getresponse()
            if response.status != 405:
                raise RuntimeError('mutation_route_not_refused')
            response.read(65537)
        finally:
            connection.close()
        result['fixture_database_unchanged'] = hashlib.sha256(database.read_bytes()).hexdigest() == before
        result['passed'] = result['fixture_database_unchanged']
    except Exception as exc:
        result['failure_type'] = type(exc).__name__
    finally:
        if child is not None:
            result['shutdown_forced'] = False
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                result['shutdown_forced'] = True
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=2)
            result['gunicorn_reaped'] = child.poll() is not None
            result['gunicorn_exit_code'] = child.returncode
            result['passed'] = result['passed'] and child.returncode == 0 and not result['shutdown_forced']
        child_log.seek(0)
        child_output = child_log.read()
        child_log.close()
        gc.collect()
        result['warnings_by_category'] = {name: warning_categories.count(name) for name in set(warning_categories)}
        result['unraisable_types'] = unraisable_types
        result['child_resource_warning_marker'] = b'ResourceWarning' in child_output
        result['child_unraisable_marker'] = b'Exception ignored in' in child_output
        result['passed'] = (result['passed'] and not result['warnings_by_category'].get('ResourceWarning')
            and not unraisable_types and not result['child_resource_warning_marker'] and not result['child_unraisable_marker'])
        report_path.write_text(json.dumps(result, sort_keys=True))
raise SystemExit(0 if result['passed'] and result.get('gunicorn_reaped') else 1)
