"""One fixed child, bounded IPC, shared stop, verified reap before acceptance.

No signal handlers are installed. The caller/service owns them. Process launch,
kernel scheduling and cleanup are not hard realtime guarantees.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time

from transport_contract import LIMIT, META_LIMIT, FIXTURES, decode


def wall_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class Result:
    meta: dict | None
    body: bytes
    received_at: str
    receive_monotonic: float
    reaped: bool | None
    reason: str
    child_pid: int | None
    exit_code: int | None
    ipc_bytes: int


def collect(mode, *, stop, deadline_mono, wall=wall_now, mono=time.monotonic,
            popen=subprocess.Popen, live_release=None, before_spawn=None):
    if mode == "live":
        from production_release import require_collection_release
        require_collection_release(live_release)
        if popen is not subprocess.Popen:
            raise ValueError("live_test_injection_refused")
    elif live_release is not None or mode not in FIXTURES:
        raise ValueError("live_collection_disabled")
    child = None
    reaped = None
    meta = None
    body = b""
    raw = bytearray()
    reason = "spawn_outcome_uncertain"
    exit_code = None
    interrupted = None
    try:
        if stop.is_set():
            return Result(None, b"", wall(), mono(), True, "cancelled_before_spawn", None, None, 0)
        if mono() >= deadline_mono:
            return Result(None, b"", wall(), mono(), True, "deadline_before_spawn", None, None, 0)
        if before_spawn is not None:
            before_spawn()
        if stop.is_set():
            return Result(None, b"", wall(), mono(), True, "cancelled_before_spawn", None, None, 0)
        if mono() >= deadline_mono:
            return Result(None, b"", wall(), mono(), True, "deadline_before_spawn", None, None, 0)
        child = popen([sys.executable, "-I", "-S", str(Path(__file__).with_name("source_worker.py")),
                       mode, str(os.getpid())], env={}, close_fds=True, shell=False,
                      start_new_session=True, stdin=subprocess.DEVNULL,
                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        reaped = False
        cap = LIMIT + META_LIMIT + 4
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            eof = False
            while True:
                if stop.is_set():
                    reason = "service_stop"; break
                remaining = deadline_mono - mono()
                if remaining <= 0:
                    reason = "transport_timeout"; break
                code = child.poll()
                if eof and type(code) is int:
                    reaped = True
                    exit_code = code
                    reason = "worker_failed"
                    if code == 0:
                        meta, body = decode(raw)
                        reason = "received"
                    break
                for key, _ in selector.select(min(0.02, remaining)):
                    chunk = os.read(key.fd, min(65536, cap - len(raw) + 1))
                    if chunk:
                        raw.extend(chunk)
                    else:
                        eof = True
                        selector.unregister(child.stdout)
                if len(raw) > cap:
                    reason = "ipc_bound"; break
    except BaseException as exc:
        reason = "spawn_outcome_uncertain" if child is None else "transport_contract_failure"
        if not isinstance(exc, Exception):
            interrupted = exc
    finally:
        if child is not None and reaped is not True:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except BaseException:
                try:
                    child.kill()
                except BaseException:
                    pass
            try:
                child.wait(timeout=1)
                reaped = type(child.returncode) is int
                exit_code = child.returncode
            except BaseException:
                reaped = False
        if child is not None and child.stdout is not None:
            child.stdout.close()
    if reaped is not True:
        reason = "transport_reap_uncertain"
    if stop.is_set():
        reason = "service_stop"
    ended_mono, ended_wall = mono(), wall()
    if ended_mono >= deadline_mono and reason == "received":
        reason = "deadline_after_cleanup"
    if interrupted is not None:
        # The orchestration layer catches this, marks uncertainty, and re-raises.
        raise interrupted
    if reason != "received":
        meta, body = None, b""
    return Result(meta, body, ended_wall, ended_mono, reaped, reason,
                  child.pid if child is not None else None, exit_code, len(raw))
