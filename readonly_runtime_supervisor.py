"""Default-off, serialized supervision of the separate read-only runtime.

This supervisor never opens storage, reads credentials, or contacts a provider.
The worker owns durable stage/uncertainty fencing; a successful ``--initialize``
is required before any source/broker child. The supervisor halt is permanent for
this instance, not a disk latch. Unfinished dirty worker state blocks a later
process until reviewed recovery. If the worker already committed completion,
or a supervisor-only failure left no unfinished dirty stage, a later process
may initialize and schedule a NEW bounded read-only observation. It never
replays the separate fixed probe and cannot place orders.

Only the fixed worker and three fixed flags are admitted. The worker's strictly
allowlisted stdout is inherited, never captured in an unbounded buffer; stderr
and stdin are discarded. The process environment is not inspected or replaced.
"""
import json
import math
from pathlib import Path
import subprocess
import sys
import threading
import time


SOURCE_INTERVAL_SECONDS = 20.0
BROKER_INTERVAL_SECONDS = 65.0
SOURCE_DEADLINE_SECONDS = 25.0
BROKER_DEADLINE_SECONDS = 15.0
INITIALIZE_DEADLINE_SECONDS = 15.0
REAP_TIMEOUT_SECONDS = 1.0

_DEADLINES = {
    'initialize': INITIALIZE_DEADLINE_SECONDS,
    'source': SOURCE_DEADLINE_SECONDS,
    'broker': BROKER_DEADLINE_SECONDS,
}
_INTERVALS = {
    'source': SOURCE_INTERVAL_SECONDS,
    'broker': BROKER_INTERVAL_SECONDS,
}


class ReadonlyRuntimeSupervisor:
    """Poll-driven cadence with a separate hard-deadline timer for each child.

    ``start()`` is idempotent and launches initialization only when explicitly
    enabled. ``poll()`` reaps and schedules at most one child per call. A stage
    exit of exactly integer zero means success, including an expected broker
    closed-session result. Every other outcome latches ``halted`` for this
    instance, with no automatic retry or in-process reset.
    ``close()`` cancels timers, kills any pending child, and bounds the reap wait.

    Cadence is measured between starts, never accelerated to replay missed
    ticks. When both stages are due the oldest due time wins (source wins ties),
    so a long source stage cannot starve broker observations.
    """

    def __init__(self, *, enabled=False, popen=None, monotonic=None,
                 emit=print, timer_factory=None):
        if type(enabled) is not bool:
            raise ValueError('invalid_enable')
        self._enabled = enabled
        self._popen = popen or subprocess.Popen
        self._clock = monotonic or time.monotonic
        self._emit = emit
        self._timer_factory = timer_factory or threading.Timer
        self._lock = threading.RLock()
        self._started = False
        self._initialized = False
        self._closed = False
        self._halted = False
        self._halt_reason = None
        self._child = None
        self._stage = None
        self._token = None
        self._timer = None
        self._deadline = None
        self._kill_attempted = False
        self._next_due = {}
        self._events = []
        self._last_clock = None

    @property
    def enabled(self):
        return self._enabled

    @property
    def halted(self):
        with self._lock:
            return self._halted

    @property
    def halt_reason(self):
        with self._lock:
            return self._halt_reason

    @property
    def initialized(self):
        with self._lock:
            return self._initialized

    @property
    def active_stage(self):
        with self._lock:
            return self._stage

    def _now_locked(self):
        value = self._clock()
        if (type(value) not in (int, float) or not math.isfinite(value)
                or (self._last_clock is not None and value < self._last_clock)):
            raise ValueError('invalid_clock')
        self._last_clock = value
        return value

    def _queue_locked(self, reason, stage):
        self._events.append({
            'mode': 'readonly_runtime_supervisor',
            'stage': stage,
            'reason': reason,
            'order_permission': 'none',
        })

    def _emit_events(self):
        # Neither a slow output sink nor unrelated main-loop SQLite work holds
        # the state lock needed by the independent deadline callback.
        with self._lock:
            events, self._events = self._events, []
        for event in events:
            try:
                self._emit(json.dumps(event), flush=True)
            except Exception:
                # Output failure must never disarm the watchdog or clear a halt.
                pass

    def _cancel_timer_locked(self):
        timer, self._timer = self._timer, None
        if timer is not None:
            try:
                timer.cancel()
            except Exception:
                # A stale callback is fenced by its per-child identity token.
                pass

    def _kill_locked(self):
        if self._child is not None and not self._kill_attempted:
            self._kill_attempted = True
            try:
                self._child.kill()
            except Exception:
                # The outcome remains uncertain and the instance stays halted.
                self._queue_locked('kill_uncertain', self._stage)

    def _halt_locked(self, reason):
        if not self._halted:
            self._halted = True
            self._halt_reason = reason
            self._queue_locked(reason, self._stage)
        self._cancel_timer_locked()

    def _clear_child_locked(self):
        self._cancel_timer_locked()
        self._child = None
        self._stage = None
        self._token = None
        self._deadline = None
        self._kill_attempted = False

    def _finish_locked(self, code):
        stage = self._stage
        if type(code) is not int or code != 0:
            self._halt_locked('process_failed')
            self._clear_child_locked()
            return
        if stage == 'initialize':
            try:
                now = self._now_locked()
            except Exception:
                self._halt_locked('clock_failed')
                self._clear_child_locked()
                return
            self._initialized = True
            self._next_due = {'source': now, 'broker': now}
        self._queue_locked('stage_finished', stage)
        self._clear_child_locked()

    def _launch_locked(self, stage, now):
        # All callers hold the same lock, and an unreaped/uncertain child is
        # never replaced. There are no command, environment, or path overrides.
        if self._child is not None or self._halted or self._closed:
            return
        self._stage = stage
        self._token = object()
        token = self._token
        self._deadline = now + _DEADLINES[stage]
        if stage in _INTERVALS:
            self._next_due[stage] = now + _INTERVALS[stage]
        try:
            path = str(Path(__file__).resolve().with_name('readonly_runtime_worker.py'))
            self._child = self._popen(
                [sys.executable, path, '--' + stage],
                stdin=subprocess.DEVNULL, stdout=None,
                stderr=subprocess.DEVNULL, shell=False, close_fds=True)
        except Exception:
            self._halt_locked('start_failed')
            return
        try:
            remaining = max(0.0, self._deadline - self._now_locked())
            self._timer = self._timer_factory(
                remaining, lambda: self._expire(token))
            self._timer.daemon = True
            self._timer.start()
        except Exception:
            self._halt_locked('watchdog_setup_failed')
            self._kill_locked()

    def _expire(self, token):
        with self._lock:
            if (self._child is None or self._token is not token
                    or self._halted or self._closed):
                return
            try:
                code = self._child.poll()
            except Exception:
                self._halt_locked('poll_uncertain')
                self._kill_locked()
                return
            if code is not None:
                self._finish_locked(code)
                return
            # Independent of the main loop and its potentially blocked SQLite
            # work. Never wait for process exit in the deadline callback.
            self._halt_locked('watchdog_expired')
            self._kill_locked()
        # The next poll/close emits queued fixed codes; the deadline thread
        # itself never waits on an output sink.

    def start(self):
        with self._lock:
            if (not self._enabled or self._started
                    or self._closed or self._halted):
                return
            self._started = True
            self._stage = 'initialize'
            try:
                now = self._now_locked()
            except Exception:
                self._halt_locked('clock_failed')
            else:
                self._launch_locked('initialize', now)
        self._emit_events()

    def poll(self):
        with self._lock:
            if self._closed or not self._started:
                return
            if self._child is not None:
                try:
                    code = self._child.poll()  # Nonblocking reap, including kills.
                except Exception:
                    self._halt_locked('poll_uncertain')
                    self._kill_locked()
                else:
                    if code is not None:
                        if self._halted:
                            self._clear_child_locked()
                        else:
                            self._finish_locked(code)
                    elif not self._halted:
                        try:
                            expired = self._now_locked() >= self._deadline
                        except Exception:
                            self._halt_locked('clock_failed')
                            self._kill_locked()
                        else:
                            if expired:
                                self._expire(self._token)
            if (self._initialized and self._child is None
                    and not self._halted):
                try:
                    now = self._now_locked()
                except Exception:
                    self._halt_locked('clock_failed')
                else:
                    due = [stage for stage in ('source', 'broker')
                           if now >= self._next_due[stage]]
                    if due:
                        stage = min(due, key=lambda value: self._next_due[value])
                        self._launch_locked(stage, now)
        self._emit_events()

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cancel_timer_locked()
            if self._child is not None:
                try:
                    code = self._child.poll()
                except Exception:
                    code = None
                    self._halt_locked('poll_uncertain')
                if code is None:
                    self._halt_locked('shutdown_interrupted')
                    self._kill_locked()
                    try:
                        self._child.wait(timeout=REAP_TIMEOUT_SECONDS)
                    except subprocess.TimeoutExpired:
                        self._queue_locked('reap_pending', self._stage)
                    except Exception:
                        self._queue_locked('reap_uncertain', self._stage)
                    else:
                        self._clear_child_locked()
                else:
                    if type(code) is not int or code != 0:
                        self._halt_locked('process_failed')
                    self._clear_child_locked()
        self._emit_events()
