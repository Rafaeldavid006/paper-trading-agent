"""Bounded, no-restart collector owner supervision; no imports of collector/broker code."""
import json
import math
import re
import os
from pathlib import Path
import subprocess
import stat
import sys
import time

FRAME_LIMIT = 16384
HEARTBEAT_TIMEOUT = 90.0  # Owner heartbeat is 30 seconds; no restart or fallback.
STOP_GRACE = 3.0
REAP_GRACE = 1.0
STATUS_FIELDS = frozenset((
    'mode', 'state', 'reason', 'lease_id', 'order_permission', 'synthetic_only', 'active_collection',
    'live_http_requests', 'live_http_attempts_reserved', 'live_http_count_exact',
    'clock_verified', 'attempts', 'reserved_bytes', 'confirmed_attempts',
    'last_confirmed_at', 'reference_data_date', 'lifetime_requests_remaining',
    'lifetime_body_budget_remaining', 'last_attempt_at', 'last_attempt_phase',
    'next_due_at', 'lease_expires_at', 'requests_remaining', 'body_budget_remaining',
    'heartbeat_utc', 'heartbeat_at', 'owner_pid', 'source_available_at',
    'historical_available_at', 'original_verified', 'revision_status',
    'eligible_for_backtest', 'automatic_retry', 'transport_mode', 'child_reaped',
    'enrollment_receipt_sha256', 'status_observed_at', 'integrity_verified_at',
    'status_cached', 'slot_grace_seconds'))

# Diagnostic-only fixed scalar fields. No collector import or executable configuration.
SURVEY_ROLES = ('started', 'complete', 'manifest', 'ready', 'ledger', 'evidence', 'lock')
STATUS_FIELDS = STATUS_FIELDS | frozenset((
    'survey_complete', 'survey_stage', 'survey_reason', 'survey_errno', 'persisted_identity_checked',
    'mount_mode', 'mount_owned', 'mount_group_matches_egid', 'mount_group_in_supplementary',
    'mount_group_writable', 'mount_other_writable',
    'mount_setgid', 'mount_exclusive_write_gate', 'mount_free_bytes_ok',
    'mount_free_inodes_ok', 'mount_identity_stable', 'mount_boundary_unique',
    'mount_rw', 'mount_fs_type', 'mount_persistent_type', 'root_present',
    'root_kind', 'root_mode', 'root_owned', 'root_private', 'root_same_device',
    'root_identity_stable', 'fixed_entries_stable', 'databases_same_device') +
    tuple(role + suffix for role in SURVEY_ROLES for suffix in ('_present', '_kind', '_mode', '_owned', '_single_link')))



def sanitized_status(value):
    if not isinstance(value, dict) or value.get('order_permission') != 'none':
        raise ValueError('invalid_owner_status')
    result = {}
    for key in STATUS_FIELDS & value.keys():
        item = value[key]
        if key == 'enrollment_receipt_sha256' and item is not None:
            if type(item) is not str or re.fullmatch(r'[0-9a-f]{64}', item) is None:
                raise ValueError('invalid_enrollment_receipt_hash')
        if item is None or type(item) is bool:
            result[key] = item
        elif type(item) in (int, float) and math.isfinite(item) and abs(item) <= 10**20:
            result[key] = item
        elif type(item) is str and len(item) <= 128 and re.fullmatch(r'[A-Za-z0-9_.:+-]*', item):
            result[key] = item
        else:
            raise ValueError('invalid_owner_status_scalar')
    return result


class CollectorSupervisor:
    def __init__(self):
        self.child = None
        self.started = False
        self.reaped = True
        self.reason = 'not_started'
        self.failure_reason = None
        self.exit_code = None
        self._buffer = bytearray()
        self._last = None
        self._stop_at = None
        self._heartbeat = None
        self._reported = None
        self._reported_at = None

    def start(self):
        if self.started:
            return
        self.started = True  # Even uncertain failure is not retried.
        try:
            script = Path(__file__).resolve().parent / 'production_collector' / 'collector_owner.py'
            self.reaped = False  # Spawn uncertainty is never proven reaped.
            self.child = subprocess.Popen(
                [sys.executable, '-I', '-S', str(script), str(os.getpid())],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env={}, cwd=str(script.parent), shell=False, close_fds=True,
                start_new_session=True, bufsize=0)
            self.reaped = False
            self._heartbeat = time.monotonic()
            os.set_blocking(self.child.stdout.fileno(), False)
            self.reason = 'starting'
        except Exception:
            self.reason = self.failure_reason = 'owner_start_uncertain'
            self.begin_stop()

    def _read(self):
        if self.child is None or self.child.stdout is None:
            return
        # Per-poll bound: a noisy child must not starve paper/web supervision.
        try:
            data = os.read(self.child.stdout.fileno(), FRAME_LIMIT + 1)
        except BlockingIOError:
            return
        if not data:
            return
        self._buffer.extend(data)
        if len(self._buffer) > FRAME_LIMIT:
            raise ValueError('status_frame_bound')
        while b'\n' in self._buffer:
            line, _, rest = self._buffer.partition(b'\n')
            self._buffer = bytearray(rest)
            value = json.loads(line)
            self._last = sanitized_status(value)
            self._heartbeat = time.monotonic()
            self.reason = 'owner_reporting'

    def poll(self):
        if self.child is None or self.reaped:
            return
        try:
            if self._stop_at is None:
                try:
                    self._read()
                except Exception:
                    self.reason = self.failure_reason = "owner_status_uncertain"
                    self.begin_stop()
            code = self.child.poll()
            if code is not None:
                # The owner can write its terminal frame and exit between the
                # first nonblocking read and poll(). Drain once more before
                # closing stdout; each read remains bounded by FRAME_LIMIT + 1.
                try:
                    self._read()
                except Exception:
                    self.failure_reason = self.failure_reason or 'owner_status_uncertain'
                self.reaped = True
                self.exit_code = code
                if self._buffer:
                    self.failure_reason = 'owner_status_incomplete'
                self.reason = ('enrollment_complete' if code == 0 and not self.failure_reason
                               and (self._last or {}).get('state') == 'enrollment_complete'
                               and (self._last or {}).get('reason') == 'awaiting_collect_manifest'
                               and (self._last or {}).get('enrollment_receipt_sha256') is not None else
                               'owner_disabled' if code == 0 and (self._last or {}).get('state') == 'disabled' else
                               'owner_stopped' if self._stop_at is not None else 'owner_exited')
                self.child.stdout.close()
                return
            now = time.monotonic()
            if self._stop_at is None and now - self._heartbeat > HEARTBEAT_TIMEOUT:
                self.reason = self.failure_reason = 'owner_heartbeat_stale'
                self.begin_stop()
            if self._stop_at is not None and now - self._stop_at >= STOP_GRACE:
                self.child.kill()
        except Exception:
            self.reason = self.failure_reason = 'owner_status_or_process_uncertain'
            self.begin_stop()

    def status(self):
        # Old last observation is explicitly not evidence of a currently healthy owner.
        current = bool(not self.reaped and self._stop_at is None and self._heartbeat is not None
                       and time.monotonic() - self._heartbeat <= HEARTBEAT_TIMEOUT)
        return {'mode': 'separate_collector_supervisor', 'reason': self.reason,
                'owner_reaped': self.reaped, 'owner_exit_code': self.exit_code,
                'failure_reason': self.failure_reason, 'owner_status': self._last,
                'owner_status_current': current,
                'active_collection': bool(current and
                                          (self._last or {}).get('active_collection') is True),
                'heartbeat_age_seconds': None if self._heartbeat is None else
                    max(0., time.monotonic() - self._heartbeat),
                'automatic_retry': False, 'order_permission': 'none'}

    def report_if_due(self):
        value = self.status()
        # Heartbeat timestamp/age changes alone are not meaningful transition logs.
        stable = {k: v for k, v in value.items() if k != 'heartbeat_age_seconds'}
        stable['owner_status'] = {k: v for k, v in (self._last or {}).items()
                                  if k not in ('heartbeat_utc', 'heartbeat_at')}
        signature = json.dumps(stable, sort_keys=True, allow_nan=False)
        now = time.monotonic()
        if signature != self._reported or self._reported_at is None or now - self._reported_at >= 60:
            self._reported, self._reported_at = signature, now
            return value
        return None

    @staticmethod
    def emit_report(payload, *, fd=1):
        """Best-effort atomic Linux FIFO log, without changing shared stdout flags.

        /proc/self/fd reopen produces a distinct open-file description on Linux;
        dup()+setblocking would instead mutate the original stdout description.
        Unsupported sinks, full pipes or unavailable /proc are silently skipped.
        """
        out = None
        try:
            original = os.fstat(fd)
            if not stat.S_ISFIFO(original.st_mode):
                return False
            raw = (json.dumps(payload, separators=(',', ':'), allow_nan=False) + '\n').encode()
            if len(raw) > 4096:
                return False
            out = os.open('/proc/self/fd/' + str(fd), os.O_WRONLY | os.O_NONBLOCK | os.O_CLOEXEC)
            reopened = os.fstat(out)
            if (original.st_dev, original.st_ino) != (reopened.st_dev, reopened.st_ino):
                return False
            if len(raw) > os.fpathconf(out, 'PC_PIPE_BUF'):
                return False
            return os.write(out, raw) == len(raw)
        except (OSError, ValueError, TypeError):
            return False
        finally:
            if out is not None:
                os.close(out)

    def begin_stop(self):
        if self._stop_at is None:
            self._stop_at = time.monotonic()
            if self.child is not None and not self.reaped:
                try:
                    self.child.terminate()
                except ProcessLookupError:
                    pass
                except Exception:
                    self.reason = 'owner_terminate_uncertain'

    def close(self):
        self.begin_stop()
        if self.child is None or self.reaped:
            return
        try:
            self.child.wait(timeout=max(0., STOP_GRACE - (time.monotonic() - self._stop_at)))
        except subprocess.TimeoutExpired:
            try:
                self.child.kill()
            except ProcessLookupError:
                pass
            except Exception:
                self.reason = 'owner_kill_uncertain'
            try:
                self.child.wait(timeout=REAP_GRACE)
            except Exception:
                self.reason = 'owner_reap_uncertain'
                return
        except Exception:
            self.reason = 'owner_reap_uncertain'
            return
        self.reaped = True
        self.exit_code = self.child.returncode
        if self.child.stdout is not None:
            self.child.stdout.close()
        self.reason = 'owner_stopped'
