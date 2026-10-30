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

# Closed diagnostic vocabulary: these are reviewed literal error codes, never
# exception text or caller-provided paths. Unknown failures remain generic.
OWNER_STAGES = frozenset(('release', 'enroll', 'enrollment_verify', 'storage_open',
                          'renewals', 'tick', 'shutdown'))
OWNER_STAGE = 'release'
OWNER_REFUSAL_CODES = frozenset((
    'explicit_bootstrap_context_required',
    'bootstrap_cohort_mismatch',
    'enrollment_anchor_root_mismatch',
    'explicit_incomplete_bootstrap_required',
    'absolute_nonsymlink_path_required',
    'anchor_path_changed',
    'anchor_physical_identity_changed',
    'approved_deployment_release_required',
    'approved_lease_count_bound',
    'bounded_component_required',
    'bounded_enrollment_window_required',
    'coherent_current_device_required',
    'cohort_evidence_nonce_mismatch',
    'cohort_evidence_schema_required',
    'cohort_evidence_version_required',
    'cohort_evidence_wal_required',
    'cohort_nodes_schema',
    'cohort_stamp_schema',
    'collection_must_disable_enrollment',
    'collection_never_bootstraps',
    'collection_phase_requires_pinned_enrollment',
    'collection_requires_enrollment_receipt_pin',
    'database_cohort_marker_guard_changed',
    'database_cohort_mismatch',
    'database_cohort_schema',
    'database_identity_changed_during_read',
    'database_integrity',
    'duplicate_or_overlapping_approved_leases',
    'duplicate_release_json_key',
    'enrolled_manifest_changed',
    'enrolled_root_missing_or_changed',
    'enrollment_cohort_mismatch',
    'enrollment_identity_schema',
    'enrollment_lineage_changed',
    'enrollment_not_compiled_approved',
    'enrollment_original_mount_changed',
    'enrollment_parent_pin_changed',
    'enrollment_phase_must_not_assume_receipt',
    'enrollment_phase_required_for_creation',
    'enrollment_receipt_pin_mismatch',
    'enrollment_release_mismatch',
    'enrollment_window_closed',
    'exact_python_version_required',
    'exclusive_owned_parent_required',
    'existing_or_partial_enrollment_no_retry',
    'explicit_cohort_evidence_contract_required',
    'explicit_enrollment_command_required',
    'finite_daily_lease_required',
    'fixed_collector_identity_required',
    'fixed_daily_utc_slot_required',
    'fixed_database_required',
    'fixed_lease_schema_required',
    'fixed_mount_required',
    'fixed_owner_arguments',
    'fixed_release_phase_required',
    'fixed_shared_root_required',
    'fresh_empty_cohort_evidence_required',
    'fresh_enrollment_schema_required',
    'lease_approval_reference',
    'lease_identifier',
    'linux_expected_parent_required',
    'live_lease_integer_required',
    'live_pause_on_failure_policy_required',
    'lock_cohort_mismatch',
    'lock_identity_changed',
    'mount_changed_before_enrollment',
    'mount_directory_required',
    'mountinfo_bound',
    'nonsymlink_storage_required',
    'original_cohort_device_mixed',
    'original_configuration_identity_mismatch',
    'owned_regular_path_required',
    'owner_status_bound',
    'owner_status_partial_write',
    'parent_death_binding_failed',
    'private_owned_storage_required',
    'private_single_link_file_required',
    'regular_bounded_release_data_required',
    'release_data_byte_bound',
    'release_data_changed_requires_restart',
    'release_data_schema',
    'release_identifier',
    'reviewed_rw_ext4_mount_required',
    'reviewed_shared_parent_required',
    'root_configuration_mismatch',
    'root_or_database_identity_mismatch',
    'root_path_changed',
    'runtime_code_changed',
    'session_physical_identity_changed',
    'single_component_required',
    'sqlite_full_integrity_required',
    'sqlite_wal_required',
    'storage_anchor_closed',
    'storage_budget',
    'storage_configuration_schema',
    'storage_nonce_required',
    'storage_read_bound',
    'storage_read_changed',
    'storage_reserve',
    'storage_schema_or_ready_mismatch',
    'storage_snapshot_changed',
))


def failure_reason(exc):
    stage = OWNER_STAGE if type(OWNER_STAGE) is str and OWNER_STAGE in OWNER_STAGES else 'unknown_stage'
    code = 'unexpected_failure'
    if type(exc) is ValueError and len(exc.args) == 1 and type(exc.args[0]) is str:
        if exc.args[0] in OWNER_REFUSAL_CODES:
            code = exc.args[0]
    elif isinstance(exc, OSError):
        code = 'os_error'
    return 'owner_refused.' + stage + '.' + code


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
    global OWNER_STAGE
    OWNER_STAGE = 'release'
    policy.require_release(release)
    if sys.version_info[:3] != policy.REQUIRED_PYTHON:
        raise ValueError("exact_python_version_required")
    from enrollment import verify_enrollment, ensure_enrolled, enrollment_receipt_hash
    from recurring import Lease, LeaseCollector, epoch
    from owned_transport import wall_now
    if release.phase == "enroll":
        OWNER_STAGE = 'enroll'
        ensure_enrolled(release)
        OWNER_STAGE = 'enrollment_verify'
        emit_status({"state": "enrollment_complete", "reason": "awaiting_collect_manifest",
                     "enrollment_receipt_sha256": enrollment_receipt_hash(release),
                     "active_collection": False, "live_http_requests": 0,
                     "synthetic_only": False})
        return 0
    policy.require_collection_release(release)
    OWNER_STAGE = 'enrollment_verify'
    verify_enrollment(release)
    OWNER_STAGE = 'storage_open'
    collector = LeaseCollector(release.root, release.collector_id, create=False,
                               live_release=release, stop=stop)
    try:
        # Constructor verifies ALL existing policies against the new snapshot
        # before any renewal append. Explicit deployment data grants are the only
        # source of new leases; no clock/environment-driven extension occurs.
        OWNER_STAGE = 'renewals'
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
                OWNER_STAGE = 'enrollment_verify'
                verify_enrollment(release)
                OWNER_STAGE = 'tick'
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
        OWNER_STAGE = 'shutdown'
        emit_status({**(status or {}), "state": "stopped", "reason": "local_shutdown",
                     "status_observed_at": observed, "integrity_verified_at": verified,
                     "status_cached": True, "active_collection": False})
        return 0
    finally:
        try:
            collector.close()
        except Exception:
            OWNER_STAGE = 'shutdown'
            raise


def main():
    global OWNER_STAGE
    OWNER_STAGE = 'release'
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
    except Exception as exc:
        try:
            emit_status({"state": "fenced", "reason": failure_reason(exc),
                         "active_collection": False, "live_http_requests": None,
                         "synthetic_only": False})
        except Exception:
            pass
        result = 70
    raise SystemExit(result)
