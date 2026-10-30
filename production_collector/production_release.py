"""Fixed schema/profile + bounded deployment-data lease grants; inactive by default.

The data file is trusted deployment configuration, not a public API or signed
credential. It is deliberately non-executable and separate from pinned Python
code so a reviewed append-only grant can renew the same durable ledger.
"""
from dataclasses import dataclass
from hashlib import sha256
import json
import os
import stat
import time
from pathlib import Path
import re

MOUNT_ROOT = "/var/data"
ROOT = "/var/data/research-collector-ecb-v1"
COLLECTOR_ID = "research-collector-ecb-v1"
REQUIRED_PYTHON = (3, 14, 8)
LIVE_SLOT_GRACE = 6 * 3600
MANIFEST_PATH = Path(__file__).resolve().with_name("approved_release.json")
MAX_MANIFEST_BYTES = 65536
MAX_LEASES = 64


class FrozenDict(dict):
    """JSON/asdict-compatible scalar value with ordinary mutation disabled."""
    def _no(self, *a, **k):
        raise TypeError("approved_lease_is_immutable")
    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _no
    __ior__ = _no


@dataclass(frozen=True)
class Release:
    release_id: str
    root: str
    collector_id: str
    enrollment_id: str
    enrollment_deadline: int
    lease: dict
    renewals: tuple = ()
    enrollment_start: int | None = None
    phase: str = "enroll"
    enrollment_receipt_sha256: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "lease", FrozenDict(self.lease))
        object.__setattr__(self, "renewals", tuple(FrozenDict(p) for p in self.renewals))
        if self.enrollment_start is None:
            object.__setattr__(self, "enrollment_start", self.enrollment_deadline - 3600)


def _validate_lease(p):
    keys = {"lease_id", "start", "end", "cadence", "request_limit", "byte_limit",
            "approval_ref", "allow_transient_next_slot", "synthetic_only"}
    if type(p) is not FrozenDict or set(p) != keys:
        raise ValueError("fixed_lease_schema_required")
    for key in ("start", "end", "cadence", "request_limit", "byte_limit"):
        if type(p[key]) is not int or not 0 <= p[key] <= 2**53:
            raise ValueError("live_lease_integer_required")
    if not (0 < p["end"] - p["start"] <= 7 * 86400 and p["cadence"] == 86400
            and 1 <= p["request_limit"] <= 7 and p["byte_limit"] == p["request_limit"] * 65536):
        raise ValueError("finite_daily_lease_required")
    if p["start"] % 86400 != 18 * 3600:
        raise ValueError("fixed_daily_utc_slot_required")
    if p["synthetic_only"] is not False or p["allow_transient_next_slot"] is not False:
        raise ValueError("live_pause_on_failure_policy_required")
    if type(p["lease_id"]) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", p["lease_id"]):
        raise ValueError("lease_identifier")
    if type(p["approval_ref"]) is not str or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", p["approval_ref"]):
        raise ValueError("lease_approval_reference")


def _validate_release(value):
    if type(value) is not Release or value.root != ROOT or value.collector_id != COLLECTOR_ID:
        raise ValueError("fixed_collector_identity_required")
    for item in (value.release_id, value.enrollment_id):
        if type(item) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", item):
            raise ValueError("release_identifier")
    policies = (value.lease, *value.renewals)
    if not 1 <= len(policies) <= MAX_LEASES:
        raise ValueError("approved_lease_count_bound")
    ids = set()
    last_end = None
    for p in policies:
        _validate_lease(p)
        if p["lease_id"] in ids or (last_end is not None and p["start"] < last_end):
            raise ValueError("duplicate_or_overlapping_approved_leases")
        ids.add(p["lease_id"])
        last_end = p["end"]
    first = value.lease["start"]
    if (type(value.enrollment_start) is not int or type(value.enrollment_deadline) is not int
            or not first - 7*86400 <= value.enrollment_start < value.enrollment_deadline <= first + LIVE_SLOT_GRACE):
        raise ValueError("bounded_enrollment_window_required")
    if value.phase not in ("enroll", "collect"):
        raise ValueError("fixed_release_phase_required")
    receipt = value.enrollment_receipt_sha256
    if value.phase == "enroll":
        if receipt is not None:
            raise ValueError("enrollment_phase_must_not_assume_receipt")
    elif type(receipt) is not str or not re.fullmatch(r"[0-9a-f]{64}", receipt):
        raise ValueError("collection_requires_enrollment_receipt_pin")
    return value


def _manifest_bytes():
    fd = os.open(MANIFEST_PATH, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_MANIFEST_BYTES:
            raise ValueError("regular_bounded_release_data_required")
        raw = bytearray()
        while len(raw) <= MAX_MANIFEST_BYTES:
            part = os.read(fd, MAX_MANIFEST_BYTES + 1 - len(raw))
            if not part:
                return bytes(raw)
            raw.extend(part)
        raise ValueError("release_data_byte_bound")
    finally:
        os.close(fd)


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate_release_json_key")
        value[key] = item
    return value


def load_manifest():
    if not MANIFEST_PATH.exists() and not MANIFEST_PATH.is_symlink():
        return None, False, None
    try:
        raw = _manifest_bytes()
    except OSError:
        raise ValueError("regular_bounded_release_data_required") from None
    doc = json.loads(raw, object_pairs_hook=_unique_object)
    if (type(doc) is not dict or set(doc) != {"schema", "enrollment_approved", "release"}
            or doc["schema"] != 1 or type(doc["schema"]) is not int
            or type(doc["enrollment_approved"]) is not bool or type(doc["release"]) is not dict):
        raise ValueError("release_data_schema")
    value = _validate_release(Release(**doc["release"]))
    if value.phase == "collect" and doc["enrollment_approved"]:
        raise ValueError("collection_must_disable_enrollment")
    return value, doc["enrollment_approved"], sha256(raw).hexdigest()


APPROVED_RELEASE, ENROLLMENT_APPROVED, _MANIFEST_SHA256 = load_manifest()


def require_release(value):
    if value is None or value is not APPROVED_RELEASE:
        raise ValueError("approved_deployment_release_required")
    _validate_release(value)
    if _MANIFEST_SHA256 is not None and sha256(_manifest_bytes()).hexdigest() != _MANIFEST_SHA256:
        raise ValueError("release_data_changed_requires_restart")
    return value


def approved_leases(value):
    require_release(value)
    return (value.lease, *value.renewals)


def stable_identity(value):
    require_release(value)
    return {"profile_schema": 2, "storage_profile": "render_single_service_cooperating_group_v2", "storage_schema": 2, "root": value.root, "collector_id": value.collector_id,
            "enrollment_id": value.enrollment_id, "source_id": "ecb_daily_reference",
            "source_url": "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            "live_slot_grace": LIVE_SLOT_GRACE, "required_python": list(REQUIRED_PYTHON),
            "order_permission": "none"}


def require_collection_release(value):
    require_release(value)
    if value.phase != "collect" or ENROLLMENT_APPROVED:
        raise ValueError("collection_phase_requires_pinned_enrollment")
    return value


def release_manifest_hash(value):
    require_release(value)
    if _MANIFEST_SHA256 is not None:
        return _MANIFEST_SHA256
    # Tests construct in-memory grants; this hash is still deterministic data
    # provenance, never proof of a published/authorized production manifest.
    from dataclasses import asdict
    return sha256(json.dumps(asdict(value),sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()


def require_enrollment_release(value):
    require_release(value)
    if value.phase != "enroll" or ENROLLMENT_APPROVED is not True:
        raise ValueError("enrollment_phase_required_for_creation")
    if not value.enrollment_start <= time.time() < value.enrollment_deadline:
        raise ValueError("enrollment_window_closed")
    return value
