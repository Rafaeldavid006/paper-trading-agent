"""One fixed read-only child, no respawn; durable replay fence lives in child."""
import json
from pathlib import Path
import subprocess
import sys
import time
import threading


class NativeProbeSupervisor:
    """The server explicitly enables one release probe; imports/default do nothing."""
    def __init__(self, *, enabled=False, popen=None, monotonic=None, emit=print, timer_factory=None):
        if type(enabled) is not bool:
            raise ValueError('invalid_enable')
        self.enabled = enabled
        self._popen = popen or subprocess.Popen
        self._clock = monotonic or time.monotonic
        self._emit = emit
        self._child = None
        self._started = False
        self._finished = False
        self._deadline = None
        self._timer = None
        self._timer_factory = timer_factory or threading.Timer
        self._lock = threading.RLock()

    def _report(self, reason):
        self._emit(json.dumps({'mode': 'native_readonly_probe_supervisor',
                               'reason': reason, 'order_permission': 'none'}), flush=True)

    def start(self):
        if not self.enabled or self._started:
            return
        self._started = True
        try:
            self._deadline = self._clock() + 15.0
            path = str(Path(__file__).resolve().with_name('native_observation_worker.py'))
            self._child = self._popen([sys.executable, path, '--once'])
            self._timer = self._timer_factory(max(0, self._deadline - self._clock()), self._expire)
            self._timer.daemon = True
            self._timer.start()
        except Exception:
            if self._child is not None and self._child.poll() is None:
                self._child.kill()
            self._finished = True
            self._report('probe_start_failed')

    def _expire(self):
        with self._lock:
            if self._child is None or self._finished:
                return
            code = self._child.poll()
            self._finished = True
            if code is None:
                # Independent of the main loop's potentially blocked SQLite work.
                # No unbounded wait after kill; a later poll/close reaps the child.
                self._child.kill()
                self._report('probe_watchdog_expired')
            else:
                self._report('probe_finished' if code == 0 else 'probe_process_failed')

    def poll(self):
        with self._lock:
            if self._child is None:
                return
            code = self._child.poll()  # Also reaps a previously killed child.
            if self._finished:
                return
            if code is not None:
                self._finished = True
                self._timer.cancel()
                self._report('probe_finished' if code == 0 else 'probe_process_failed')
            elif self._clock() >= self._deadline:
                self._expire()

    def close(self):
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            if self._child is not None and self._child.poll() is None:
                self._child.kill()
                try:
                    self._child.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    self._report('probe_reap_pending')
            self._finished = True
