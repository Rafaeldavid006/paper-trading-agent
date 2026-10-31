"""One optional owned diagnostic child, no retry; must be reaped before startup."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def run_report(stop):
    if os.environ.get('PAPER_ACCOUNT_CHECK_ONCE_JSON') is None:
        return True
    child = None
    reaped = False
    reason = 'start_uncertain'
    try:
        if stop.is_set():
            reaped = True
            reason = 'cancelled_before_start'
            return True
        child = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve().with_name(
            'paper_account_diagnostic.py')), '--check-paper-once'], stdin=subprocess.DEVNULL,
            stdout=None, stderr=subprocess.DEVNULL, shell=False, close_fds=True)
        deadline = time.monotonic()+22
        while True:
            code = child.poll()
            if code is not None:
                reaped = True
                reason = 'finished' if code == 0 else 'refused_or_interrupted'
                break
            if stop.wait(.05):
                reason = 'cancelled'
                break
            if time.monotonic() >= deadline:
                reason = 'timeout'
                break
    except Exception:
        reason = 'process_uncertain'
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
        print(json.dumps(dict(mode='paper_account_diagnostic_supervisor', reason=reason,
            child_reaped=reaped, automatic_retry=False, order_permission='none')), flush=True)
    return reaped
