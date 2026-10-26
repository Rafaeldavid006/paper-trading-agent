"""Fixed finite PAPER subprocess supervision, disabled without compiled approval.

Reuses the reviewed read-only supervisor's independent timer, token fencing,
bounded kill/reap and serialization pattern. No credentials, files or provider
calls are made here. Only fixed worker inspect/recover/initialize/source/tick flags exist. Recovery starts
only with its own compiled approval and never retries a failed/uncertain child.
There is no command override, reset, catch-up or renewal.
The actual immutable approval constants are None in this release. A caller's
``enabled=True`` alone cannot launch a process. Worker/anchor gates independently
bind exact scope and enforce durable counters and uncertainty across restarts.
"""
import json
import math
from pathlib import Path
import subprocess
import sys
import threading
import time
import finite_pilot_contract as contract


SOURCE_INTERVAL_SECONDS = 20.0
TICK_INTERVAL_SECONDS = 65.0
SOURCE_DEADLINE_SECONDS = 25.0
TICK_DEADLINE_SECONDS = 15.0
INITIALIZE_DEADLINE_SECONDS = 15.0
REAP_TIMEOUT_SECONDS = 1.0

_DEADLINES = {
    'inspect': 10.0,
    'recover': 15.0,
    'initialize': INITIALIZE_DEADLINE_SECONDS,
    'source': SOURCE_DEADLINE_SECONDS,
    'tick': TICK_DEADLINE_SECONDS,
}
_INTERVALS = {
    'source': SOURCE_INTERVAL_SECONDS,
    'tick': TICK_INTERVAL_SECONDS,
}


class FinitePilotSupervisor:
    """Poll-driven finite cadence with a separate hard-deadline timer per child.

    ``start()`` is idempotent and launches initialization only when explicitly
    enabled and an exact compiled approval exists. ``poll()`` reaps and schedules at most one child per call. A stage
    exit of exactly integer zero means success, including an expected broker
    closed-session result. Every other outcome latches ``halted`` for this
    instance, with no automatic retry or in-process reset.
    ``close()`` cancels timers, kills any pending child, and bounds the reap wait.

    Cadence is measured between starts, never accelerated to replay missed
    ticks. When both stages are due the oldest due time wins (source wins ties),
    so a long source stage cannot starve broker observations.
    """

    def __init__(self, *, enabled=False, popen=None, monotonic=None,
                 emit=print, timer_factory=None, wall_time_ns=None):
        if type(enabled) is not bool:
            raise ValueError('invalid_enable')
        self._approval = contract.APPROVED_PILOT
        self._recovery = contract.APPROVED_RECOVERY
        self._inspection = contract.APPROVED_INSPECTION
        self._enabled = enabled and (
            type(self._approval) is contract.PilotManifest
            or type(self._recovery) is contract.RecoveryManifest
            or type(self._inspection) is contract.InspectionManifest)
        self._pending_initial = ('inspect' if self._inspection is not None else
                                 'recover' if self._recovery is not None else 'initialize')
        self._recovered = False
        self._completed = False
        self._wall_clock = wall_time_ns or time.time_ns
        self._last_wall = None
        self._deadline_reason = 'watchdog_expired'
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

    @property
    def completed(self):
        with self._lock:
            return self._completed

    @property
    def reaped(self):
        """Local process outcome only; never turn uncertain kill into completion."""
        with self._lock:
            return self._child is None

    def _approval_for(self, stage):
        return self._inspection if stage == 'inspect' else self._recovery if stage == 'recover' else self._approval

    def _now_locked(self):
        value = self._clock()
        if (type(value) not in (int, float) or not math.isfinite(value)
                or (self._last_clock is not None and value < self._last_clock)):
            raise ValueError('invalid_clock')
        self._last_clock = value
        return value

    def _wall_now_locked(self):
        value = self._wall_clock()
        if (type(value) is not int or value <= 0
                or (self._last_wall is not None and value < self._last_wall)):
            raise ValueError('invalid_wall_clock')
        self._last_wall = value
        return value

    def _scope_locked(self, stage, *, allow_future=False):
        if (contract.APPROVED_PILOT is not self._approval
                or contract.APPROVED_RECOVERY is not self._recovery
                or contract.APPROVED_INSPECTION is not self._inspection):
            self._halt_locked('approval_changed'); self._kill_locked(); return None
        if self._inspection is not None and (self._approval is not None or self._recovery is not None):
            self._halt_locked('mixed_inspection_scope'); self._kill_locked(); return None
        approval = self._approval_for(stage)
        expected = (contract.InspectionManifest if stage == 'inspect' else
                    contract.RecoveryManifest if stage == 'recover' else contract.PilotManifest)
        if type(approval) is not expected:
            self._halt_locked('approval_missing'); self._kill_locked(); return None
        try:
            if stage == 'inspect': contract.require_inspection_manifest(approval)
            elif stage == 'recover': contract.require_recovery_manifest(approval)
            else: contract.require_pilot_manifest(approval)
        except Exception:
            self._halt_locked('approval_invalid'); self._kill_locked(); return None
        try:
            wall = self._wall_now_locked()
        except Exception:
            self._halt_locked('clock_failed'); self._kill_locked(); return None
        # An expired recovery approval may only inspect an already-applied exact
        # receipt. The worker enforces fresh action validity before any new
        # evidence/CAS; this fixed stage itself always has a 15s hard deadline.
        if stage != 'recover' and wall >= approval.expires_ns:
            self._halt_locked('scope_expired'); self._kill_locked(); return None
        if not allow_future and wall < approval.starts_ns:
            return None
        return wall

    def _queue_locked(self, reason, stage):
        self._events.append({
            'mode': 'finite_pilot_supervisor',
            'stage': stage,
            'reason': reason,
            'order_permission': 'none',
            'child_scope': ('finite_paper' if self._approval is not None
                            and stage in ('initialize', 'source', 'tick') else 'read_only'),
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
            self._halt_locked('process_failed'); self._clear_child_locked(); return
        if stage == 'inspect':
            self._completed = True
            self._pending_initial = None
        elif stage == 'recover':
            self._recovered = True
            if self._approval is None:
                self._completed = True
                self._pending_initial = None
            else:
                self._pending_initial = 'initialize'
        elif stage == 'initialize':
            try:
                now = self._now_locked()
            except Exception:
                self._halt_locked('clock_failed'); self._clear_child_locked(); return
            self._pending_initial = None
            self._initialized = True
            self._next_due = {'source': now, 'tick': now}
        self._queue_locked('stage_finished', stage)
        self._clear_child_locked()

    def _launch_locked(self, stage, now):
        if self._child is not None or self._halted or self._closed or self._completed:
            return
        if self._scope_locked(stage, allow_future=False) is None:
            return
        # Re-sample immediately before launch preparation, with all faults and
        # exact-boundary expiry handled before Popen (never negative timers).
        wall = self._scope_locked(stage, allow_future=False)
        if wall is None:
            return
        approval = self._approval_for(stage)
        remaining_scope = (approval.expires_ns - wall) / 1_000_000_000
        if (stage == 'initialize' and approval.admission is not None
                and wall < approval.admission.expires_ns):
            # A fresh admission child cannot outlive its first-five-minute
            # subwindow. After that boundary initialize may only verify an
            # already-APPLIED receipt; the worker forbids any new admission.
            remaining_scope = min(remaining_scope,(approval.admission.expires_ns-wall)/1_000_000_000)
        seconds = _DEADLINES[stage] if stage == 'recover' else min(_DEADLINES[stage], remaining_scope)
        if seconds <= 0:
            self._halt_locked('scope_expired'); return
        self._stage = stage
        self._token = object()
        token = self._token
        self._deadline_reason = ('scope_expired' if stage != 'recover'
                                 and remaining_scope <= _DEADLINES[stage] else 'watchdog_expired')
        self._deadline = now + seconds
        if stage in _INTERVALS:
            self._next_due[stage] = now + _INTERVALS[stage]
        try:
            filename = 'finite_pilot_inspection.py' if stage == 'inspect' else 'finite_pilot_worker.py'
            flag = '--initialize-proof-state' if stage == 'inspect' else '--' + stage
            path = str(Path(__file__).resolve().with_name(filename))
            self._child = self._popen(
                [sys.executable, path, flag], stdin=subprocess.DEVNULL,
                stdout=None, stderr=subprocess.DEVNULL, shell=False, close_fds=True)
        except Exception:
            self._halt_locked('start_failed'); return
        try:
            remaining = max(0.0, self._deadline - self._now_locked())
            self._timer = self._timer_factory(remaining, lambda: self._expire(token))
            self._timer.daemon = True
            self._timer.start()
        except Exception:
            self._halt_locked('watchdog_setup_failed'); self._kill_locked()

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
            self._halt_locked(self._deadline_reason)
            self._kill_locked()
        # The next poll/close emits queued fixed codes; the deadline thread
        # itself never waits on an output sink.

    def start(self):
        with self._lock:
            if not self._enabled or self._started or self._closed or self._halted:
                return
            self._started = True
            try:
                now = self._now_locked()
            except Exception:
                self._halt_locked('clock_failed')
            else:
                self._launch_locked(self._pending_initial, now)
        self._emit_events()

    def poll(self):
        with self._lock:
            if self._closed or not self._started or self._completed:
                return
            if self._child is not None:
                if not self._halted:
                    self._scope_locked(self._stage, allow_future=False)
                try:
                    code = self._child.poll()
                except Exception:
                    self._halt_locked('poll_uncertain'); self._kill_locked()
                else:
                    if code is not None:
                        if self._halted: self._clear_child_locked()
                        else: self._finish_locked(code)
                    elif not self._halted:
                        try:
                            expired = self._now_locked() >= self._deadline
                        except Exception:
                            self._halt_locked('clock_failed'); self._kill_locked()
                        else:
                            if expired: self._expire(self._token)
            if self._child is None and not self._halted and not self._completed:
                try:
                    now = self._now_locked()
                except Exception:
                    self._halt_locked('clock_failed')
                else:
                    if self._pending_initial is not None:
                        self._launch_locked(self._pending_initial, now)
                    elif self._initialized:
                        due = [stage for stage in ('source', 'tick') if now >= self._next_due[stage]]
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
