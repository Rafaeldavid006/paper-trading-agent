"""Two-phase enrollment; collect-phase owner startup is strictly reopen-only.

A sibling started marker is committed before any root creation. Partial work is
never auto-retried/reset. Even deleting the entire collector root cannot enroll
again while the sibling markers remain. Deleting ALL durable controls is outside
this trusted-process/corruption model; normal startup still refuses missing state.
"""
from dataclasses import asdict
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))

from filesystem import check, digest, fsync_dir, identity, write_new, MIN_FREE
import production_release as policy

PREFIX = ".research-collector-ecb-v1-enrollment"


def _paths(release):
    root = Path(release.root)
    return root, root.parent / (PREFIX + ".started.json"), root.parent / (PREFIX + ".complete.json")


def qualify_mount(root, *, _test_mount=False):
    """Read-only mount/ownership/capacity qualification before any bootstrap IO."""
    root = Path(root)
    mount = root.parent
    check(mount, True)
    if mount.stat().st_mode & 0o022:
        raise ValueError("mount_not_exclusively_writable")
    fs = os.statvfs(mount)
    if fs.f_bavail * fs.f_frsize < MIN_FREE or fs.f_favail < 128:
        raise ValueError("mount_capacity_reserve")
    if not _test_mount:
        if str(mount) != policy.MOUNT_ROOT:
            raise ValueError("fixed_mount_required")
        entries = []
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            left, right = line.split(" - ", 1)
            fields = left.split()
            point = fields[4].replace("\\040", " ").replace("\\011", "\t").replace("\\012", "\n").replace("\\134", "\\")
            if point == str(mount):
                entries.append((fields, right.split()))
        if len(entries) != 1 or "rw" not in entries[0][0][5].split(",") or entries[0][1][0] in ("tmpfs", "overlay", "ramfs"):
            raise ValueError("persistent_mount_boundary_required")
        fs_type = entries[0][1][0]
    else:
        fs_type = "synthetic_test_mount"
    return {"mount_identity": identity(mount), "uid": os.getuid(), "fs_type": fs_type,
            "free_bytes": fs.f_bavail * fs.f_frsize, "free_inodes": fs.f_favail}


def _read_marker(path):
    check(path)
    if path.stat().st_mode & 0o077 or path.stat().st_size > 8192:
        raise ValueError("private_bounded_enrollment_marker_required")
    return json.loads(path.read_text())


def _binding(release):
    return {"stable_profile_hash": digest(policy.stable_identity(release)), "enrollment_id": release.enrollment_id,
            "root": release.root, "collector_id": release.collector_id}


def verify_enrollment(release, *, _test_mount=False):
    policy.require_release(release)
    root, started, complete = _paths(release)
    evidence = qualify_mount(root, _test_mount=_test_mount)
    initial, done = _read_marker(started), _read_marker(complete)
    if initial.get("binding") != _binding(release) or done.get("binding") != _binding(release):
        raise ValueError("enrollment_release_mismatch")
    if done.get("started_hash") != digest(initial) or initial.get("mount_identity") != evidence["mount_identity"]:
        raise ValueError("enrollment_lineage_or_mount_changed")
    check(root, True)
    if done.get("root_identity") != identity(root):
        raise ValueError("enrolled_root_missing_or_changed")
    manifest = root / "manifest.json"
    check(manifest)
    if manifest.stat().st_size > 65536 or done.get("manifest_sha256") != sha256(manifest.read_bytes()).hexdigest():
        raise ValueError("enrolled_manifest_changed")
    if release.phase == "collect" and release.enrollment_receipt_sha256 != digest(done):
        raise ValueError("enrollment_receipt_pin_mismatch")
    return evidence


def enrollment_receipt_hash(release):
    verify_enrollment(release)
    _, _, complete = _paths(release)
    return digest(_read_marker(complete))


def enroll_once(release, *, _test_mount=False, _collector_factory=None, _fault=None):
    """Authorized enrollment; called only by enroll-phase startup or explicit CLI."""
    policy.require_release(release)
    if release.phase != "enroll" or policy.ENROLLMENT_APPROVED is not True:
        raise ValueError("enrollment_not_compiled_approved")
    now = time.time()
    if not release.enrollment_start <= now < release.enrollment_deadline:
        raise ValueError("enrollment_window_closed")
    root, started, complete = _paths(release)
    evidence = qualify_mount(root, _test_mount=_test_mount)
    if any(p.exists() or p.is_symlink() for p in (root, started, complete)):
        raise ValueError("existing_or_partial_enrollment_no_retry")
    initial = {"binding": _binding(release), "started_at": now,
               "mount_identity": evidence["mount_identity"]}
    write_new(started, initial)
    fsync_dir(root.parent)
    if _fault:
        _fault("after_started")
    if _collector_factory is None:
        from recurring import LeaseCollector
        _collector_factory = LeaseCollector
    collector = _collector_factory(root, release.collector_id, create=True, live_release=release)
    try:
        from recurring import Lease
        for approved in policy.approved_leases(release):
            collector.add_lease(Lease(**approved))
        collector.verify()
    finally:
        collector.close()
    if _fault:
        _fault("after_root")
    # Close/reopen is part of enrollment, not inferred from directory creation.
    collector = _collector_factory(root, release.collector_id, create=False, live_release=release)
    try:
        collector.verify()
        for name in ("ledger.sqlite", "evidence.sqlite"):
            db = sqlite3.connect((root / name).as_uri() + "?mode=rw", uri=True, timeout=2)
            try:
                if db.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
                    raise ValueError("sqlite_wal_required")
                db.execute("PRAGMA synchronous=FULL")
                if db.execute("PRAGMA synchronous").fetchone()[0] != 2 or db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise ValueError("sqlite_full_integrity_required")
            finally:
                db.close()
    finally:
        collector.close()
    done = {"binding": _binding(release), "started_hash": digest(initial),
            "root_identity": identity(root), "manifest_sha256": sha256((root / "manifest.json").read_bytes()).hexdigest()}
    write_new(complete, done)
    fsync_dir(root.parent)
    if _fault:
        _fault("after_complete")
    verify_enrollment(release, _test_mount=_test_mount)
    return {"state": "enrolled", "source_requests": 0, "order_permission": "none"}


def ensure_enrolled(release, *, _test_mount=False):
    """Reviewed first-start bootstrap only; ANY partial/existing state refuses reset."""
    policy.require_release(release)
    root, started, complete = _paths(release)
    present = [p.exists() or p.is_symlink() for p in (root, started, complete)]
    if not any(present):
        if release.phase != "enroll":
            raise ValueError("collection_never_bootstraps")
        enroll_once(release, _test_mount=_test_mount)
    return verify_enrollment(release, _test_mount=_test_mount)


def main():
    if sys.argv[1:] != ["--enroll-compiled-once"]:
        raise ValueError("explicit_enrollment_command_required")
    if sys.version_info[:3] != policy.REQUIRED_PYTHON:
        raise ValueError("exact_python_version_required")
    print(json.dumps(enroll_once(policy.APPROVED_RELEASE)), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print('{"state":"enrollment_refused","source_requests":0,"order_permission":"none"}', flush=True)
        raise SystemExit(70)
