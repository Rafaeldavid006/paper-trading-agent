"""Fixed no-secret child: enroll-only bootstrap or long-lived pinned-state collection."""
import ctypes
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import resource
import signal
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import production_release as policy

HEARTBEAT_SECONDS = 30
INTEGRITY_INTERVAL = 3600
STATUS_LIMIT = 4096  # Linux PIPE_BUF; a nonblocking write is all-or-EAGAIN.


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def bind_parent(parent_pid):
    if sys.platform != "linux" or type(parent_pid) is not int or parent_pid <= 1:
        raise ValueError("linux_expected_parent_required")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent_pid:
        raise ValueError("parent_death_binding_failed")
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    # Worker needs a 256 MiB hard ceiling; owner keeps the lower soft limit.
    resource.setrlimit(resource.RLIMIT_AS, (128 * 1024 * 1024, 256 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
    resource.setrlimit(resource.RLIMIT_FSIZE, (40 * 1024 * 1024, 40 * 1024 * 1024))
    # Cumulative CPU caps incorrectly terminate renewable daemons after enough
    # healthy cycles. Lower scheduling priority; worker CPU remains hard-capped.
    # This is not a dedicated CPU-rate reservation/cgroup.
    os.nice(10)


def install_owner_guard():
    expected = str(Path(__file__).resolve().with_name("source_worker.py"))
    def guard(event, args):
        if event.startswith("socket.") or event in ("os.system", "os.posix_spawn", "os.fork", "os.forkpty"):
            raise RuntimeError("owner_external_operation_refused")
        if event == "subprocess.Popen":
            executable, argv, cwd, env = args
            if not (executable == sys.executable and env == {} and len(argv) == 6
                    and argv[1:4] == ["-I", "-S", expected] and argv[4] == "live"
                    and argv[5] == str(os.getpid())):
                raise RuntimeError("fixed_source_worker_required")
    sys.addaudithook(guard)


def emit_status(payload, *, fd=1):
    frame = {**payload, "mode": "separate_ecb_collector", "heartbeat_utc": utc_now(),
             "order_permission": "none"}
    raw = (json.dumps(frame, separators=(",", ":"), allow_nan=False) + "\n").encode()
    if len(raw) > STATUS_LIMIT:
        raise ValueError("owner_status_bound")
    try:
        os.set_blocking(fd, False)
        count = os.write(fd, raw)
    except BlockingIOError:
        return False
    if count != len(raw):
        raise ValueError("owner_status_partial_write")
    return True


def heartbeat_view(status, observed_at, verified_at):
    """A heartbeat is liveness, not a new storage/source observation."""
    value = {**status, "status_observed_at": observed_at,
             "integrity_verified_at": verified_at, "status_cached": True}
    expires = value.get("lease_expires_at")
    if expires and datetime.now(timezone.utc) >= datetime.fromisoformat(expires):
        value["active_collection"] = False
        value["state"] = "cached_lease_expired_pending_refresh"
    return value


def run(release, stop):
    """Reopen active state; optional enrollment phase never starts source IO."""
    policy.require_release(release)
    if sys.version_info[:3] != policy.REQUIRED_PYTHON:
        raise ValueError("exact_python_version_required")
    from enrollment import verify_enrollment, ensure_enrolled, enrollment_receipt_hash
    from recurring import Lease, LeaseCollector, epoch
    from owned_transport import wall_now
    if release.phase == "enroll":
        ensure_enrolled(release)
        emit_status({"state": "enrollment_complete", "reason": "awaiting_collect_manifest",
                     "enrollment_receipt_sha256": enrollment_receipt_hash(release),
                     "active_collection": False, "live_http_requests": 0,
                     "synthetic_only": False})
        return 0
    policy.require_collection_release(release)
    verify_enrollment(release)
    collector = LeaseCollector(release.root, release.collector_id, create=False,
                               live_release=release, stop=stop)
    try:
        # Constructor verifies ALL existing policies against the new snapshot
        # before any renewal append. Explicit deployment data grants are the only
        # source of new leases; no clock/environment-driven extension occurs.
        existing = {row[0] for row in collector.db.execute("SELECT lease_id FROM leases")}
        for approved in policy.approved_leases(release):
            if approved["lease_id"] not in existing:
                collector.add_lease(Lease(**approved))
        next_check = time.monotonic()
        status = None
        observed = verified = None
        while not stop.is_set():
            wall = epoch(wall_now())
            due = epoch(status["next_due_at"]) if status and status.get("next_due_at") else None
            expiry = epoch(status["lease_expires_at"]) if status and status.get("lease_expires_at") else None
            if (status is None or time.monotonic() >= next_check
                    or (due is not None and wall >= due)
                    or (expiry is not None and wall >= expiry)):
                verify_enrollment(release)
                status = collector.tick(mode="live")
                observed = verified = utc_now()
                next_check = time.monotonic() + INTEGRITY_INTERVAL
                emit_status({**status, "status_observed_at": observed,
                             "integrity_verified_at": verified, "status_cached": False})
            else:
                emit_status(heartbeat_view(status, observed, verified))
            wall = epoch(wall_now())
            waits = [HEARTBEAT_SECONDS, max(.01, next_check - time.monotonic())]
            for name in ("next_due_at", "lease_expires_at"):
                if status.get(name):
                    waits.append(max(.01, epoch(status[name]) - wall))
            stop.wait(max(.01, min(waits)))
        emit_status({**(status or {}), "state": "stopped", "reason": "local_shutdown",
                     "status_observed_at": observed, "integrity_verified_at": verified,
                     "status_cached": True, "active_collection": False})
        return 0
    finally:
        collector.close()


def main():
    stop = threading.Event()
    if len(sys.argv) != 2:
        raise ValueError("fixed_owner_arguments")
    bind_parent(int(sys.argv[1]))
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    install_owner_guard()
    if policy.APPROVED_RELEASE is None:
        emit_status({"state": "disabled", "reason": "compiled_release_not_installed",
                     "active_collection": False, "live_http_requests": 0, "synthetic_only": False})
        return 0
    return run(policy.APPROVED_RELEASE, stop)


if __name__ == "__main__":
    try:
        result = main()
    except Exception:
        try:
            emit_status({"state": "fenced", "reason": "owner_startup_or_runtime_refused",
                         "active_collection": False, "live_http_requests": None,
                         "synthetic_only": False})
        except Exception:
            pass
        result = 70
    raise SystemExit(result)
