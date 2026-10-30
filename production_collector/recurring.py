"""Recurring fixed-source collector. Production requires a compiled finite release.

One stable ledger binds immutable leases, quotas and globally numbered attempts.
The unchanged strict parser/evidence store are composed with per-attempt mapping.
"""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time

from core_v2.core import Receipt
from direct_adapter import CaptureEvidence, capture_json, parse, stamp
from upstream.storage import durable_store as storage
from cohort_evidence import CohortEvidenceStore
from filesystem import PrivateRoot, canonical, digest, MIN_FREE, MAX_FILES
from owned_transport import collect, wall_now
from transport_contract import URL, LIMIT, FIXTURES

VERSION = "recurring-lease-collector-v3-storage2"
SOURCE_ID = "ecb_daily_reference"
PROFILE = "ecb_daily_reference"
MAX_LIFETIME_ATTEMPTS = 128
MAX_LIFETIME_RESERVED = 8 * 1024 * 1024
LIVE_SLOT_GRACE = 6 * 3600
SYNTHETIC_SLOT_GRACE = 5
TERMINAL = frozenset(("CONFIRMED", "FAILED", "CANCELLED"))
TRANSIENT = frozenset(("transport_timeout", "deadline_before_spawn", "deadline_after_cleanup", "http_5xx"))


def epoch(value):
    return datetime.fromisoformat(stamp(value)).timestamp()


def bounded_int(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("bounded_integer_required")


@dataclass(frozen=True)
class Lease:
    lease_id: str
    start: int
    end: int
    cadence: int
    request_limit: int
    byte_limit: int
    approval_ref: str
    allow_transient_next_slot: bool = False
    synthetic_only: bool = True

    def __post_init__(self):
        if type(self.lease_id) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.lease_id):
            raise ValueError("lease_id")
        bounded_int(self.start, 0, 2**53)
        bounded_int(self.end, self.start + 1, self.start + 7 * 86400)
        bounded_int(self.cadence, 1, 86400)
        bounded_int(self.request_limit, 1, 28)
        bounded_int(self.byte_limit, LIMIT, 28 * LIMIT)
        if type(self.approval_ref) is not str or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", self.approval_ref):
            raise ValueError("approval_reference")
        if type(self.synthetic_only) is not bool:
            raise ValueError("provenance_boolean")
        if not self.synthetic_only and (self.cadence != 86400 or self.request_limit > 7 or self.byte_limit > 7 * LIMIT):
            raise ValueError("live_daily_lease_bounds")
        if type(self.allow_transient_next_slot) is not bool:
            raise ValueError("transient_policy_boolean")

    @property
    def policy_hash(self):
        return digest({"lease": asdict(self), "source": SOURCE_ID, "url": URL,
                       "profile": PROFILE, "response_cap": LIMIT, "request_timeout": 10,
                       "synthetic_only": self.synthetic_only,
                       "slot_grace": SYNTHETIC_SLOT_GRACE if self.synthetic_only else LIVE_SLOT_GRACE})


class LeaseCollector:
    def __init__(self, root, collector_id, *, create=False, offline_test_enabled=False,
                 stop=None, wall=wall_now, mono=time.monotonic, live_release=None, _enrollment_nonce=None, _enrollment_mount_pin=None, _enrollment_reopen=False):
        self.live_release = live_release
        self.synthetic_only = live_release is None
        if live_release is not None:
            from production_release import require_release, stable_identity
            require_release(live_release)
            if create:
                from production_release import require_enrollment_release
                require_enrollment_release(live_release)
            if offline_test_enabled or str(root) != live_release.root or collector_id != live_release.collector_id:
                raise ValueError("live_release_binding")
        elif offline_test_enabled is not True:
            raise ValueError("live_collection_disabled_offline_only")
        if type(collector_id) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", collector_id):
            raise ValueError("collector_id")
        self.wall, self.mono = wall, mono
        self.stop_event = stop if stop is not None else threading.Event()
        self.root = self.db = self.store = None
        self.closed = False
        config = {"version": VERSION, "collector_id": collector_id, "source_id": SOURCE_ID,
                  "url": URL, "profile": PROFILE, "response_cap": LIMIT,
                  "max_lifetime_attempts": MAX_LIFETIME_ATTEMPTS,
                  "max_lifetime_reserved": MAX_LIFETIME_RESERVED,
                  "min_free": MIN_FREE, "max_files": MAX_FILES,
                  "slot_grace": SYNTHETIC_SLOT_GRACE if self.synthetic_only else LIVE_SLOT_GRACE,
                  "synthetic_only": self.synthetic_only, "live_authorized": not self.synthetic_only,
                  "release_hash": None if live_release is None else digest(stable_identity(live_release)), "order_permission": "none"}
        try:
            self.root = PrivateRoot(root, config, create=create, nonce=_enrollment_nonce, parent_pin=_enrollment_mount_pin)
            if live_release is not None and not create:
                from enrollment import verify_enrollment, verify_bootstrap_reopen
                phase = getattr(live_release, 'phase', None)
                if phase == 'collect' or (phase == 'enroll' and not _enrollment_reopen):
                    verify_enrollment(live_release, _anchor=self.root.anchor)
                elif phase == 'enroll':
                    if _enrollment_nonce is None or _enrollment_mount_pin is None:
                        raise ValueError('explicit_bootstrap_context_required')
                    verify_bootstrap_reopen(live_release,self.root.anchor,_enrollment_nonce)
                self.root.check_anchors()
            self.db = self.root.open_database("ledger.sqlite", timeout=2, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA synchronous=FULL")
            if create:
                self.db.execute("PRAGMA journal_mode=WAL")
                self.db.executescript('''
                  CREATE TABLE collector_storage_identity (id INTEGER PRIMARY KEY CHECK(id=1), schema INTEGER NOT NULL, nonce TEXT NOT NULL);
                  CREATE TABLE config (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
                  CREATE TABLE gates (id INTEGER PRIMARY KEY CHECK(id=1), fence TEXT, pause TEXT,
                    revoked INTEGER NOT NULL DEFAULT 0, last_claim_utc REAL);
                  CREATE TABLE leases (lease_id TEXT PRIMARY KEY, start INTEGER NOT NULL, end INTEGER NOT NULL,
                    policy TEXT NOT NULL, policy_hash TEXT NOT NULL UNIQUE);
                  CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lease_id TEXT NOT NULL REFERENCES leases(lease_id), source_id TEXT NOT NULL,
                    slot INTEGER NOT NULL, requested_at TEXT NOT NULL, request_mono REAL NOT NULL,
                    deadline_utc INTEGER NOT NULL, reserved_bytes INTEGER NOT NULL,
                    UNIQUE(source_id,slot));
                  CREATE TABLE missed (lease_id TEXT NOT NULL, slot INTEGER NOT NULL, PRIMARY KEY(lease_id,slot));
                  CREATE TABLE events (seq INTEGER PRIMARY KEY, attempt_id INTEGER REFERENCES attempts(id),
                    phase TEXT NOT NULL, payload TEXT NOT NULL, previous_hash TEXT NOT NULL, record_hash TEXT NOT NULL UNIQUE);
                  CREATE TABLE blobs (body_hash TEXT PRIMARY KEY, body BLOB NOT NULL);
                ''')
                for table in ("collector_storage_identity", "config", "leases", "attempts", "missed", "events", "blobs"):
                    for op in ("UPDATE", "DELETE"):
                        self.db.execute(f"CREATE TRIGGER no_{table}_{op} BEFORE {op} ON {table} BEGIN SELECT RAISE(ABORT,'immutable'); END")
                self.db.execute("INSERT INTO collector_storage_identity VALUES(1,2,?)", (self.root.nonce,))
                self.db.execute("INSERT INTO config VALUES(1,?)", (canonical(self.root.config),))
                self.db.execute("INSERT INTO gates(id) VALUES(1)")
            self.store = CohortEvidenceStore(self.root, create=create, clock=wall)
            if create:
                self.root.finalize()
            self.verify()
        except BaseException:
            self.close(); raise

    def close(self):
        if self.db is not None:
            self.db.close(); self.db = None
        if self.root is not None:
            self.root.close()
        self.closed = True

    def _event(self, phase, payload, attempt_id=None):
        # Must be called inside the caller's existing write transaction.
        if len(canonical(payload).encode()) > 32768:
            raise ValueError("event_metadata_bound")
        previous = self.db.execute("SELECT seq,record_hash FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        seq, prior = (previous[0] + 1, previous[1]) if previous else (1, "0" * 64)
        record = digest([seq, attempt_id, phase, payload, prior])
        self.db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)", (seq, attempt_id, phase, canonical(payload), prior, record))
        return seq

    def _write(self, fn):
        self.root.check_anchors()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            answer = fn()
            self.root.check_anchors()
            self.db.execute("COMMIT")
            self.root.check_anchors()
            return answer
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def _states(self):
        return {r["attempt_id"]: r["phase"] for r in self.db.execute("SELECT attempt_id,phase FROM events WHERE attempt_id IS NOT NULL ORDER BY seq")}

    def _unresolved(self, ignore=None):
        states = self._states()
        return [r[0] for r in self.db.execute("SELECT id FROM attempts") if r[0] != ignore and states.get(r[0]) not in TERMINAL]

    def _gate_reason(self, ignore=None):
        g = self.db.execute("SELECT * FROM gates WHERE id=1").fetchone()
        if g["revoked"]:
            return "operator_revoked"
        if g["fence"]:
            return "fenced:" + g["fence"]
        if g["pause"]:
            return "paused_source:" + g["pause"]
        if self._admission_pending():
            return "fenced:storage_admission_uncertain"
        if self._unresolved(ignore):
            return "fenced:unresolved_outcome"
        return None

    def _set_gate(self, *, fence=None, pause=None, revoke=False):
        if fence is not None:
            self.db.execute("UPDATE gates SET fence=COALESCE(fence,?) WHERE id=1", (fence,))
        if pause is not None:
            self.db.execute("UPDATE gates SET pause=COALESCE(pause,?) WHERE id=1", (pause,))
        if revoke:
            self.db.execute("UPDATE gates SET revoked=1 WHERE id=1")
        g = self.db.execute("SELECT fence,pause,revoked FROM gates WHERE id=1").fetchone()
        self._event("GATE", dict(g))

    def revoke(self):
        self.verify()
        self._write(lambda: self._set_gate(revoke=True))

    def _require_live_release(self):
        if not self.synthetic_only:
            from production_release import require_release, stable_identity
            require_release(self.live_release)
            if self.root is not None and digest(stable_identity(self.live_release)) != self.root.config["release_hash"]:
                raise ValueError("live_release_changed")

    def _require_lease_mode(self, lease):
        if lease.synthetic_only != self.synthetic_only:
            raise ValueError("lease_provenance_mismatch")
        self._require_live_release()
        if not self.synthetic_only:
            from production_release import approved_leases
            if asdict(lease) not in [dict(p) for p in approved_leases(self.live_release)]:
                raise ValueError("compiled_lease_required")

    def _live_lease_prefix(self):
        """Persisted authority is an exact chronological prefix, not a subset."""
        if self.synthetic_only:
            return ()
        from production_release import approved_leases
        approved = tuple(dict(p) for p in approved_leases(self.live_release))
        recorded = tuple(json.loads(row[0]) for row in self.db.execute(
            "SELECT policy FROM leases ORDER BY start,end,lease_id"))
        if recorded != approved[:len(recorded)] or len(recorded) > len(approved):
            raise ValueError("compiled_lease_required")
        return approved[len(recorded):]

    def add_lease(self, lease):
        if type(lease) is not Lease:
            raise ValueError("immutable_lease_required")
        self._require_lease_mode(lease)
        self.verify()
        def operation():
            if self._gate_reason():
                raise ValueError("renewal_cannot_clear_gate")
            old = self.db.execute("SELECT policy,policy_hash FROM leases WHERE lease_id=?", (lease.lease_id,)).fetchone()
            if old:
                if old[0] == canonical(asdict(lease)) and old[1] == lease.policy_hash:
                    return "already_recorded"
                raise ValueError("immutable_lease_conflict")
            if not self.synthetic_only:
                pending = self._live_lease_prefix()
                if not pending or asdict(lease) != pending[0]:
                    raise ValueError("compiled_lease_required")
            if self.db.execute("SELECT 1 FROM leases WHERE start<? AND end>?", (lease.end, lease.start)).fetchone():
                raise ValueError("overlapping_lease")
            if self.db.execute("SELECT COUNT(*) FROM leases").fetchone()[0] >= 64:
                raise ValueError("lease_count_bound")
            self.db.execute("INSERT INTO leases VALUES(?,?,?,?,?)", (lease.lease_id, lease.start, lease.end,
                                                                     canonical(asdict(lease)), lease.policy_hash))
            evidence = {"lease_id": lease.lease_id, "policy_hash": lease.policy_hash}
            if not self.synthetic_only:
                from production_release import release_manifest_hash
                evidence.update(release_id=self.live_release.release_id,
                                approval_manifest_sha256=release_manifest_hash(self.live_release))
            self._event("LEASE_ADDED", evidence)
            return "recorded"
        return self._write(operation)

    def _lease(self, lease_id):
        row = self.db.execute("SELECT * FROM leases WHERE lease_id=?", (lease_id,)).fetchone()
        if row is None:
            raise ValueError("unknown_lease")
        lease = Lease(**json.loads(row["policy"]))
        self._require_lease_mode(lease)
        if lease.policy_hash != row["policy_hash"] or (lease.lease_id, lease.start, lease.end) != (row["lease_id"], row["start"], row["end"]):
            raise ValueError("lease_policy_corrupt")
        return lease

    def _active(self, now):
        rows = self.db.execute("SELECT lease_id FROM leases WHERE start<=? AND end>?", (now, now)).fetchall()
        if len(rows) > 1:
            raise ValueError("overlapping_authority")
        return self._lease(rows[0][0]) if rows else None

    def _usage(self, lease_id=None):
        sql = "SELECT COUNT(*),COALESCE(SUM(reserved_bytes),0) FROM attempts"
        row = self.db.execute(sql + (" WHERE lease_id=?" if lease_id else ""), (lease_id,) if lease_id else ()).fetchone()
        return row[0], row[1]

    def _capacity(self):
        if self._admission_pending():
            raise ValueError("storage_admission_uncertain")
        # Persist intent before testing reserve. If reserve failure prevents the
        # later gate write, this unmatched marker still fences startup/renewal.
        started = self._write(lambda: self._event("ADMISSION_STARTED", {"scope": "storage_capacity"}))
        try:
            self.root.capacity()
        except (ValueError, OSError):
            self._write(lambda: self._set_gate(fence="storage_reserve_or_budget"))
            raise ValueError("storage_reserve_or_budget") from None
        self._write(lambda: self._event("ADMISSION_OK", {"started_seq": started}))

    def _admission_pending(self):
        pending = set()
        for row in self.db.execute("SELECT seq,phase,payload FROM events WHERE phase IN ('ADMISSION_STARTED','ADMISSION_OK') ORDER BY seq"):
            if row["phase"] == "ADMISSION_STARTED":
                pending.add(row["seq"])
            else:
                started = json.loads(row["payload"])["started_seq"]
                if started not in pending:
                    raise ValueError("storage_admission_lineage")
                pending.remove(started)
        return bool(pending)

    def _launch_clock_reason(self, attempt):
        self.root.check_anchors()
        current_mono, current_wall = self.mono(), epoch(self.wall())
        wallspan = current_wall - epoch(attempt["requested_at"])
        monospan = current_mono - attempt["request_mono"]
        if type(current_mono) not in (float, int) or not math.isfinite(current_mono) or wallspan < 0 or monospan < 0 or abs(wallspan - monospan) > .5:
            self._write(lambda: self._set_gate(fence="clock_discontinuity_before_spawn"))
            return "clock_discontinuity_before_spawn"
        lease = self._lease(attempt["lease_id"])
        if current_wall >= attempt["deadline_utc"] or monospan >= 10 or not lease.start <= current_wall < lease.end:
            return "deadline_before_spawn"
        return None

    def _payloads(self, attempt_id):
        return {r["phase"]: json.loads(r["payload"]) for r in self.db.execute("SELECT phase,payload FROM events WHERE attempt_id=? ORDER BY seq", (attempt_id,))}

    def _receipt(self, attempt, capture, raw):
        lease = self._lease(attempt["lease_id"])
        capture_json(capture)
        expected = (lease.lease_id, lease.policy_hash, attempt["id"], SOURCE_ID, URL, URL,
                    sha256(raw).hexdigest(), len(raw), 200)
        got = (capture.run_id, capture.configuration_hash, capture.attempt_id, capture.source_id,
               capture.requested_url, capture.final_url, capture.body_sha256, capture.body_bytes, capture.http_status)
        if got != expected or not 0 < len(raw) <= attempt["reserved_bytes"]:
            raise ValueError("capture_binding")
        if capture.requested_at != attempt["requested_at"] or capture.request_monotonic != attempt["request_mono"]:
            raise ValueError("capture_start_binding")
        if capture.representation != "http_entity_bytes_identity" or capture.evidence_class != ("synthetic_transport_test" if self.synthetic_only else "direct_http_caller_attestation"):
            raise ValueError("capture_provenance")
        if not capture.body_complete or not capture.owned_transport_reaped or not capture.tls_verified or capture.redirect_count != 0 or capture.outcome != "received":
            raise ValueError("capture_transport")
        duration = epoch(capture.received_at) - epoch(capture.requested_at)
        monospan = capture.receive_monotonic - capture.request_monotonic
        if duration < 0 or monospan < 0 or abs(duration - monospan) > .5 or epoch(capture.received_at) >= attempt["deadline_utc"] or monospan >= 10:
            raise ValueError("capture_clock_or_deadline")
        headers = dict(capture.headers)
        if headers.get("content-encoding", "identity").lower() != "identity":
            raise ValueError("capture_encoding")
        if "content-length" in headers and (not re.fullmatch(r"[0-9]{1,8}", headers["content-length"]) or int(headers["content-length"]) != len(raw)):
            raise ValueError("capture_length")
        if headers.get("content-type", "").lower().split(";", 1)[0].strip() not in ("application/xml", "text/xml"):
            raise ValueError("capture_media_type")
        charset = re.search(r"charset\s*=\s*([^;\s]+)", headers.get("content-type", ""), re.I)
        if charset and charset.group(1).strip('"\'').lower() not in ("utf-8", "utf8"):
            raise ValueError("capture_charset")
        receipt = Receipt(SOURCE_ID, URL, capture.requested_at, capture.received_at, "direct",
                          capture.body_sha256, 200, False, True, capture.request_monotonic, capture.receive_monotonic)
        receipt.validate(raw)
        return receipt

    def _confirmed_map(self):
        mapped = {}
        for attempt in self.db.execute("SELECT * FROM attempts"):
            p = self._payloads(attempt["id"])
            if "CONFIRMED" not in p:
                continue
            if set(p) != {"CLAIMED", "SPAWN_INTENT", "CAPTURED", "NORMALIZING", "COMPLETE", "CONFIRMED"}:
                raise ValueError("confirmed_phase_lineage")
            obj = dict(p["CAPTURED"]["capture"])
            obj["headers"] = tuple(tuple(x) for x in obj["headers"])
            cap = CaptureEvidence(**obj)
            bodyrow = self.db.execute("SELECT body FROM blobs WHERE body_hash=?", (cap.body_sha256,)).fetchone()
            if bodyrow is None:
                raise ValueError("capture_blob_missing")
            receipt = self._receipt(attempt, cap, bytes(bodyrow[0]))
            seq = p["COMPLETE"]["store_seq"]
            if seq in mapped:
                raise ValueError("store_mapping_duplicate")
            with self.store.connect() as db:
                row = db.execute("SELECT recorded_at,payload,record_hash FROM records WHERE seq=? AND kind='event_batch'", (seq,)).fetchone()
                confirms = [at for at, data in db.execute("SELECT recorded_at,payload FROM records WHERE kind='commit_confirmation'") if json.loads(data)["observation_seq"] == seq]
            if not row or row[2] != p["COMPLETE"]["store_hash"] or json.loads(row[1])["receipt"] != asdict(receipt) or len(confirms) != 1:
                raise ValueError("store_mapping_lineage")
            normalized = p["NORMALIZING"]["normalized_at"]
            confirmed = p["CONFIRMED"]["confirmed_at"]
            if normalized < cap.received_at or confirmed < max(normalized, row[0], confirms[0]):
                raise ValueError("confirmation_clock_lineage")
            mapped[seq] = {"attempt_id": attempt["id"], "lease_id": attempt["lease_id"],
                           "policy_hash": self._lease(attempt["lease_id"]).policy_hash,
                           "normalized_at": normalized, "confirmed_at": confirmed,
                           "capture_hash": digest(p["CAPTURED"]["capture"]),
                           "reference_data_date": parse(PROFILE, bytes(bodyrow[0]), receipt)[0].logical_key.split("|", 1)[0]}
        return mapped

    def verify(self):
        if self.closed:
            raise ValueError("collector_closed")
        self._require_live_release()
        self.root.verify()
        runtime = {"version": VERSION, "source_id": SOURCE_ID, "url": URL, "profile": PROFILE,
                   "response_cap": LIMIT, "max_lifetime_attempts": MAX_LIFETIME_ATTEMPTS,
                   "max_lifetime_reserved": MAX_LIFETIME_RESERVED,
                   "synthetic_only": self.synthetic_only, "live_authorized": not self.synthetic_only,
                   "slot_grace": SYNTHETIC_SLOT_GRACE if self.synthetic_only else LIVE_SLOT_GRACE}
        if any(self.root.config[k] != v for k, v in runtime.items()):
            raise ValueError("runtime_configuration_changed")
        if self.db.execute("SELECT value FROM config WHERE id=1").fetchone()[0] != canonical(self.root.config):
            raise ValueError("ledger_configuration_mismatch")
        self._live_lease_prefix()
        prior, gates = "0" * 64, {"fence": None, "pause": None, "revoked": 0}
        for seq, row in enumerate(self.db.execute("SELECT * FROM events ORDER BY seq"), 1):
            payload = json.loads(row["payload"])
            if row["seq"] != seq or row["previous_hash"] != prior or row["record_hash"] != digest([seq, row["attempt_id"], row["phase"], payload, prior]):
                raise ValueError("ledger_hash_chain")
            prior = row["record_hash"]
            if row["phase"] == "GATE":
                gates = payload
        actual = dict(self.db.execute("SELECT fence,pause,revoked FROM gates WHERE id=1").fetchone())
        if actual != gates:
            raise ValueError("gate_audit_mismatch")
        for row in self.db.execute("SELECT lease_id FROM leases"):
            lease = self._lease(row[0])
            matching = [json.loads(r[0]) for r in self.db.execute("SELECT payload FROM events WHERE phase='LEASE_ADDED'")
                        if json.loads(r[0]).get("lease_id") == lease.lease_id]
            expected = {"lease_id": lease.lease_id, "policy_hash": lease.policy_hash}
            if self.synthetic_only:
                valid = matching == [expected]
            else:
                valid = (len(matching) == 1 and set(matching[0]) == set(expected) | {"release_id", "approval_manifest_sha256"}
                         and all(matching[0][k] == v for k, v in expected.items())
                         and type(matching[0]["release_id"]) is str
                         and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", matching[0]["release_id"])
                         and type(matching[0]["approval_manifest_sha256"]) is str
                         and re.fullmatch(r"[0-9a-f]{64}", matching[0]["approval_manifest_sha256"]))
            if not valid:
                raise ValueError("lease_audit_lineage")
        for row in self.db.execute("SELECT * FROM blobs"):
            if not 0 < len(row["body"]) <= LIMIT or sha256(row["body"]).hexdigest() != row["body_hash"]:
                raise ValueError("capture_blob_corrupt")
        for attempt in self.db.execute("SELECT * FROM attempts"):
            p = self._payloads(attempt["id"])
            if p.get("CLAIMED", {}).get("attempt") != dict(attempt):
                raise ValueError("attempt_claim_lineage")
        last = max((epoch(r[0]) for r in self.db.execute("SELECT requested_at FROM attempts")), default=None)
        if self.db.execute("SELECT last_claim_utc FROM gates WHERE id=1").fetchone()[0] != last:
            raise ValueError("clock_watermark_lineage")
        self.store.verify()
        mapping = self._confirmed_map()
        with self.store.connect() as db:
            observations = {r[0] for r in db.execute("SELECT record_seq FROM observations")}
        complete = {p["COMPLETE"]["store_seq"] for aid in self._states() if "COMPLETE" in (p := self._payloads(aid))}
        if observations - complete and not any("NORMALIZING" in self._payloads(aid) for aid in self._unresolved()):
            raise ValueError("unmapped_store_observation")
        return mapping

    def status(self):
        mapping = self.verify()
        now = epoch(self.wall())
        reason = self._gate_reason()
        if not self.synthetic_only:
            from production_release import require_collection_release
            try:
                require_collection_release(self.live_release)
            except ValueError:
                reason = reason or "enrollment_only:collection_phase_not_approved"
        lease = self._active(now)
        total, reserved = self._usage()
        if reason:
            state = reason.split(":", 1)[0]
        elif total >= MAX_LIFETIME_ATTEMPTS or reserved + LIMIT > MAX_LIFETIME_RESERVED:
            state = "lifetime_quota_exhausted"
        elif lease is None:
            state = "waiting_lease" if self.db.execute("SELECT 1 FROM leases WHERE start>?", (now,)).fetchone() else "expired" if self.db.execute("SELECT 1 FROM leases").fetchone() else "no_lease"
        else:
            used, size = self._usage(lease.lease_id)
            state = "quota_exhausted" if used >= lease.request_limit or size + LIMIT > lease.byte_limit else "waiting_next_slot"
        used, size = self._usage(lease.lease_id) if lease else (0, 0)
        confirmed = max((v["confirmed_at"] for v in mapping.values()), default=None)
        next_due = None
        if state == "waiting_next_slot":
            slot = lease.start + int((now - lease.start) // lease.cadence) * lease.cadence
            consumed = self.db.execute("SELECT 1 FROM attempts WHERE source_id=? AND slot=?", (SOURCE_ID, slot)).fetchone()
            missed = self.db.execute("SELECT 1 FROM missed WHERE lease_id=? AND slot=?", (lease.lease_id, slot)).fetchone()
            next_due = slot + lease.cadence if consumed or missed or now - slot > (SYNTHETIC_SLOT_GRACE if self.synthetic_only else LIVE_SLOT_GRACE) else slot
            if next_due >= lease.end:
                next_due = None
        elif state == "waiting_lease":
            next_due = self.db.execute("SELECT MIN(start) FROM leases WHERE start>?", (now,)).fetchone()[0]
        last_attempt = self.db.execute("SELECT id,requested_at FROM attempts ORDER BY id DESC LIMIT 1").fetchone()
        return {"state": state, "reason": reason, "lease_id": lease.lease_id if lease else None,
                "slot_grace_seconds": SYNTHETIC_SLOT_GRACE if self.synthetic_only else LIVE_SLOT_GRACE,
                "attempts": total, "reserved_bytes": reserved, "confirmed_attempts": len(mapping),
                "last_confirmed_at": confirmed,
                "reference_data_date": max((v["reference_data_date"] for v in mapping.values()), default=None),
                "lifetime_requests_remaining": max(0, MAX_LIFETIME_ATTEMPTS - total),
                "lifetime_body_budget_remaining": max(0, MAX_LIFETIME_RESERVED - reserved),
                "last_attempt_at": last_attempt["requested_at"] if last_attempt else None,
                "last_attempt_phase": self._states().get(last_attempt["id"]) if last_attempt else None,
                "next_due_at": datetime.fromtimestamp(next_due, timezone.utc).isoformat() if next_due is not None else None,
                "lease_expires_at": datetime.fromtimestamp(lease.end, timezone.utc).isoformat() if lease else None,
                "requests_remaining": lease.request_limit - used if lease else 0,
                "body_budget_remaining": lease.byte_limit - size if lease else 0,
                "synthetic_only": self.synthetic_only,
                "active_collection": not self.synthetic_only and state == "waiting_next_slot" and not self.stop_event.is_set(),
                "live_http_requests": 0 if self.synthetic_only else None,
                "live_http_attempts_reserved": 0 if self.synthetic_only else total,
                "live_http_count_exact": self.synthetic_only,
                "clock_verified": False, "order_permission": "none"}

    def _claim(self, requested_at, requested_mono):
        self.verify()
        now = epoch(requested_at)
        # Read-only eligibility first: idle/expired/consumed/quota-bound ticks
        # create no admission journal churn. Transaction below repeats checks.
        if self._gate_reason():
            return None
        last = self.db.execute("SELECT last_claim_utc FROM gates WHERE id=1").fetchone()[0]
        if last is not None and now < last:
            self._write(lambda: self._set_gate(fence="clock_regression"))
            return None
        lease = self._active(now)
        if lease is None:
            return None
        used, size = self._usage(lease.lease_id)
        total, reserved = self._usage()
        if used >= lease.request_limit or size + LIMIT > lease.byte_limit or total >= MAX_LIFETIME_ATTEMPTS or reserved + LIMIT > MAX_LIFETIME_RESERVED:
            return None
        slot = lease.start + int((now - lease.start) // lease.cadence) * lease.cadence
        if self.db.execute("SELECT 1 FROM attempts WHERE source_id=? AND slot=?", (SOURCE_ID, slot)).fetchone() or self.db.execute("SELECT 1 FROM missed WHERE lease_id=? AND slot=?", (lease.lease_id, slot)).fetchone():
            return None
        self._capacity()
        def operation():
            if self._gate_reason():
                return None
            last = self.db.execute("SELECT last_claim_utc FROM gates WHERE id=1").fetchone()[0]
            if last is not None and now < last:
                self._set_gate(fence="clock_regression"); return None
            lease = self._active(now)
            if lease is None:
                return None
            used, size = self._usage(lease.lease_id)
            total, reserved = self._usage()
            if used >= lease.request_limit or size + LIMIT > lease.byte_limit or total >= MAX_LIFETIME_ATTEMPTS or reserved + LIMIT > MAX_LIFETIME_RESERVED:
                return None
            slot = lease.start + int((now - lease.start) // lease.cadence) * lease.cadence
            if now - slot > (SYNTHETIC_SLOT_GRACE if self.synthetic_only else LIVE_SLOT_GRACE):
                self.db.execute("INSERT OR IGNORE INTO missed VALUES(?,?)", (lease.lease_id, slot)); return None
            if self.db.execute("SELECT 1 FROM attempts WHERE source_id=? AND slot=?", (SOURCE_ID, slot)).fetchone() or self.db.execute("SELECT 1 FROM missed WHERE lease_id=? AND slot=?", (lease.lease_id, slot)).fetchone():
                return None
            cur = self.db.execute("INSERT INTO attempts(lease_id,source_id,slot,requested_at,request_mono,deadline_utc,reserved_bytes) VALUES(?,?,?,?,?,?,?)",
                                  (lease.lease_id, SOURCE_ID, slot, requested_at, requested_mono, min(int(now) + 10, lease.end), LIMIT))
            attempt = self.db.execute("SELECT * FROM attempts WHERE id=?", (cur.lastrowid,)).fetchone()
            self.db.execute("UPDATE gates SET last_claim_utc=? WHERE id=1", (now,))
            self._event("CLAIMED", {"attempt": dict(attempt)}, attempt["id"])
            return dict(attempt)
        return self._write(operation)

    def _pre_spawn(self, attempt):
        # Same sole owner; recheck all durable components, then gates in the claim
        # transaction. An unresolved earlier mapping can never be ignored here.
        self.verify()
        self._capacity()
        clock_reason = self._launch_clock_reason(attempt)
        if clock_reason:
            return clock_reason
        def operation():
            if self._gate_reason(ignore=attempt["id"]):
                return "pre_spawn_fenced"
            if self._states().get(attempt["id"]) != "CLAIMED":
                raise ValueError("spawn_requires_owned_claim")
            self._event("SPAWN_INTENT", {"fixed_worker": True}, attempt["id"])
            return None
        return self._write(operation)

    def _finish_failure(self, attempt, reason, audit, *, cancelled=False, uncertain=False):
        lease = self._lease(attempt["lease_id"])
        def operation():
            phase = "UNCERTAIN" if uncertain else "CANCELLED" if cancelled else "FAILED"
            self._event(phase, {"reason": reason, "transport": audit}, attempt["id"])
            if uncertain:
                self._set_gate(fence=reason)
            elif not cancelled and not (reason in TRANSIENT and lease.allow_transient_next_slot):
                self._set_gate(pause=reason)
        self._write(operation)

    def tick(self, *, mode="ok", timeout=10, _fault=None, _popen=None):
        if (mode not in FIXTURES if self.synthetic_only else mode != "live") or type(timeout) not in (float, int) or not math.isfinite(timeout) or not 0 < timeout <= 10:
            raise ValueError("offline_mode_or_timeout")
        if not self.synthetic_only and (_popen is not None or _fault is not None):
            raise ValueError("live_test_injection_refused")
        if not self.synthetic_only:
            from production_release import require_collection_release
            require_collection_release(self.live_release)
        self.verify()
        if self.stop_event.is_set():
            return {**self.status(), "action": "local_shutdown"}
        try:
            requested_mono, requested_at = self.mono(), stamp(self.wall())
            if type(requested_mono) not in (int, float) or not math.isfinite(requested_mono) or requested_mono < 0:
                raise ValueError("monotonic_clock")
        except Exception:
            self._write(lambda: self._set_gate(fence="clock_input_invalid"))
            raise ValueError("clock_input_invalid") from None
        attempt = self._claim(requested_at, requested_mono)
        if attempt is None:
            return {**self.status(), "action": "no_claim"}
        stage = "claimed"
        try:
            if _fault:
                _fault("after_claim")
            if self.stop_event.is_set():
                self._finish_failure(attempt, "local_shutdown_before_spawn", {}, cancelled=True)
                return {**self.status(), "action": "cancelled"}
            pre_spawn_reason = self._pre_spawn(attempt)
            if pre_spawn_reason:
                self._finish_failure(attempt, pre_spawn_reason, {}, cancelled=pre_spawn_reason != "deadline_before_spawn")
                return {**self.status(), "action": "blocked_before_spawn"}
            if _fault:
                _fault("after_spawn_intent")
            # A test hook or concurrently requested operator stop must not slip
            # between the durable preflight and actual process creation.
            self.verify()
            self._capacity()
            launch_reason = self._launch_clock_reason(attempt)
            if launch_reason or self._gate_reason(ignore=attempt["id"]):
                reason = launch_reason or "pre_spawn_fenced"
                self._finish_failure(attempt, reason, {}, cancelled=reason != "deadline_before_spawn")
                return {**self.status(), "action": "blocked_before_spawn"}
            kwargs = {} if _popen is None else {"popen": _popen}
            stage = "transport"
            result = collect(mode, stop=self.stop_event, deadline_mono=requested_mono + min(timeout, attempt["deadline_utc"] - epoch(requested_at)),
                             wall=self.wall, mono=self.mono, live_release=self.live_release, before_spawn=self.root.check_anchors, **kwargs)
            audit = {k: v for k, v in asdict(result).items() if k not in ("body", "meta")}
            if result.reaped is not True:
                self._finish_failure(attempt, "transport_reap_uncertain", audit, uncertain=True)
                return {**self.status(), "action": "fenced"}
            if result.reason in ("service_stop", "cancelled_before_spawn") or self.stop_event.is_set():
                self._finish_failure(attempt, "local_shutdown", audit, cancelled=True)
                return {**self.status(), "action": "cancelled"}
            if result.reason != "received" or result.meta is None:
                self._finish_failure(attempt, result.reason, audit)
                return {**self.status(), "action": "failed"}
            status = result.meta["status"]
            if status != 200:
                reason = "access_or_rate_denial" if status in (401, 403, 429) else "http_5xx" if 500 <= status <= 599 else "http_or_redirect_failure"
                self._finish_failure(attempt, reason, {**audit, "http_status": status})
                return {**self.status(), "action": "failed"}
            lease = self._lease(attempt["lease_id"])
            body = result.body
            capture = CaptureEvidence(lease.lease_id, lease.policy_hash, attempt["id"], SOURCE_ID, URL, result.meta["url"],
                                      requested_at, result.received_at, requested_mono, result.receive_monotonic,
                                      sha256(body).hexdigest(), len(body), status, "http_entity_bytes_identity",
                                      ("synthetic_transport_test" if self.synthetic_only else "direct_http_caller_attestation"), result.meta["body_complete"], 0, True,
                                      result.meta["tls_verified"], "received", tuple(sorted(result.meta["headers"].items())))
            stage = "validation"
            receipt = self._receipt(attempt, capture, body)
            serialized = json.loads(capture_json(capture))
            def persist_capture():
                self.db.execute("INSERT OR IGNORE INTO blobs VALUES(?,?)", (capture.body_sha256, body))
                self._event("CAPTURED", {"capture": serialized, "transport": audit}, attempt["id"])
            self._write(persist_capture)
            if _fault:
                _fault("after_capture")
            parsed = parse(PROFILE, body, receipt)
            normalized = stamp(self.wall())
            if normalized < capture.received_at:
                raise ValueError("normalization_clock_regression")
            self._write(lambda: self._event("NORMALIZING", {"normalized_at": normalized}, attempt["id"]))
            stage = "store_uncertain"
            if _fault:
                _fault("before_store")
            spec = storage.SourceSpec(SOURCE_ID, storage.SourceClass.FX, (URL,), PROFILE, 21600, True,
                                      ("Synthetic offline source binding; informational reference rates only" if self.synthetic_only else "Source: European Central Bank; derived normalized reference data, informational not transaction prices; raw bytes preserved"))
            try:
                _, saved = self.store.source(SOURCE_ID)
            except KeyError:
                self.store.register_source(spec, reviewer="offline_source_binding" if self.synthetic_only else "compiled_live_release", rationale="Synthetic only; no source/runtime grant" if self.synthetic_only else "Finite approved ECB research collection; no execution permission")
            else:
                if storage.spec_dict(saved) != storage.spec_dict(spec):
                    raise ValueError("source_binding_mismatch")
            seq = self.store.append_batch(body, storage.Receipt(**asdict(receipt)),
                                          [storage.ParsedEvent(**asdict(p)) for p in parsed], VERSION)
            if _fault:
                _fault("after_store")
            with self.store.connect() as db:
                row = db.execute("SELECT record_hash FROM records WHERE seq=? AND kind='event_batch'", (seq,)).fetchone()
            if row is None:
                raise ValueError("store_batch_missing")
            self._write(lambda: self._event("COMPLETE", {"store_seq": seq, "store_hash": row[0]}, attempt["id"]))
            if _fault:
                _fault("after_complete")
            confirmed = stamp(self.wall())
            with self.store.connect() as db:
                times = [at for at, data in db.execute("SELECT recorded_at,payload FROM records WHERE kind='commit_confirmation'") if json.loads(data)["observation_seq"] == seq]
            if len(times) != 1 or confirmed < max(normalized, times[0]):
                raise ValueError("confirmation_clock_regression")
            self._write(lambda: self._event("CONFIRMED", {"confirmed_at": confirmed}, attempt["id"]))
            return {**self.status(), "action": "confirmed", "attempt_id": attempt["id"]}
        except BaseException as exc:
            # Abrupt process-death tests deliberately retain the durable boundary.
            # Unresolved state itself fences even if a catch/fence write never ran.
            if not isinstance(exc, Exception):
                raise
            if stage == "validation":
                reason = "clock_failure" if "clock" in str(exc) else "capture_or_parser_rejected"
                self._finish_failure(attempt, reason, {}, uncertain=reason == "clock_failure")
            else:
                self._finish_failure(attempt, "integration_outcome_uncertain", {}, uncertain=True)
            return {**self.status(), "action": "fenced" if stage != "validation" else "rejected", "failure_class": type(exc).__name__}

    def observations(self, cutoff):
        mapping = self.verify()
        cutoff = stamp(cutoff)
        active_collection = self.status()["active_collection"]
        out = []
        for item in self.store.events_as_observed(cutoff):
            observations = [{"storage": obs, "collector": mapping[obs["record_seq"]], "synthetic": self.synthetic_only}
                            for obs in item["observations"] if obs["record_seq"] in mapping
                            and mapping[obs["record_seq"]]["confirmed_at"] <= cutoff
                            and mapping[obs["record_seq"]]["normalized_at"] <= cutoff]
            if observations:
                out.append({"event": item["event"], "observations": observations,
                            "source_available_at": None, "historical_available_at": None,
                            "original_verified": False, "clock_verified": False, "revision_status": "unknown",
                            "eligible_for_backtest": False, "active_collection": active_collection,
                            "synthetic": self.synthetic_only, "order_permission": "none"})
        return out

    def serve(self, *, mode="ok", timeout=10, max_wait=60):
        """Reusable in-process loop; caller owns signals. No automatic grant creation.

        Expiry/fence/quota leaves read-only health available, waiting on stop. Tests
        can use short leases; source cadence and authority remain persisted data.
        """
        if not 0 < max_wait <= 60:
            raise ValueError("wait_bound")
        while not self.stop_event.is_set():
            status = self.tick(mode=mode, timeout=timeout)
            until = epoch(status["next_due_at"]) - epoch(self.wall()) if status["next_due_at"] else max_wait
            self.stop_event.wait(max(.01, min(max_wait, until)))
        return {**self.status(), "action": "local_shutdown"}


if __name__ == "__main__":
    raise SystemExit("use the fixed production owner; no direct activation interface")
