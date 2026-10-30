"""Disabled, offline-tested normalization seam. No HTTP or collector entrypoint.

Transport evidence is a trusted-caller attestation, NOT authenticated provenance.
Never feed web-tool extraction or saved-fixture replays into this direct seam.
"""
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from html.parser import HTMLParser
import json
import math
from pathlib import Path
import re
import sqlite3
from types import MappingProxyType
import xml.etree.ElementTree as ET

from core_v2.core import Registry, Receipt, ParsedEvent, SourceSpec, SourceClass, utc
from upstream.storage import durable_store as storage

VERSION = "direct-source-adapter-offline-v1"
FAQ = "https://www.microsoft.com/en-us/investor/faq"
ECB = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
PROFILES = {"faq_snapshot": (FAQ, SourceClass.ISSUER, ("text/html",)),
            "ecb_daily_reference": (ECB, SourceClass.FX, ("application/xml", "text/xml"))}
HEADER_NAMES = frozenset(("content-type", "content-length", "content-encoding", "date",
                          "last-modified", "etag", "age", "cache-control"))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def stamp(value):
    return utc(value).isoformat(timespec="microseconds")


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class Rejected(ValueError):
    """Safe, fixed reason code; never raw transport exception text."""


@dataclass(frozen=True)
class CaptureEvidence:
    run_id: str
    configuration_hash: str
    attempt_id: int
    source_id: str
    requested_url: str
    final_url: str
    requested_at: str
    received_at: str
    request_monotonic: float
    receive_monotonic: float
    body_sha256: str
    body_bytes: int
    http_status: int
    # Only response entity bytes received without content decoding are accepted.
    representation: str
    evidence_class: str
    body_complete: bool
    redirect_count: int
    owned_transport_reaped: bool
    tls_verified: bool
    outcome: str
    headers: tuple[tuple[str, str], ...]


def capture_json(evidence):
    if type(evidence) is not CaptureEvidence:
        raise Rejected("capture_evidence_required")
    # Check shallow types BEFORE dataclass recursion/JSON serialization. Transport
    # metadata is bounded input too; annotations alone do not enforce Python types.
    strings = ("run_id", "configuration_hash", "source_id", "requested_url", "final_url",
               "requested_at", "received_at", "body_sha256", "representation", "evidence_class", "outcome")
    integers = ("attempt_id", "body_bytes", "http_status", "redirect_count")
    booleans = ("body_complete", "owned_transport_reaped", "tls_verified")
    for field in strings:
        value = getattr(evidence, field)
        if type(value) is not str or len(value) > 2048:
            raise Rejected("capture_field_bound")
    for field in integers:
        value = getattr(evidence, field)
        if type(value) is not int or not 0 <= value <= 2**63 - 1:
            raise Rejected("capture_integer")
    if any(type(getattr(evidence, field)) is not bool for field in booleans):
        raise Rejected("capture_boolean")
    for field in ("request_monotonic", "receive_monotonic"):
        value = getattr(evidence, field)
        if type(value) not in (int, float) or not math.isfinite(value):
            raise Rejected("capture_monotonic")
    # Reject secret-bearing/unknown header fields before any durable capture write.
    if type(evidence.headers) is not tuple or len(evidence.headers) > len(HEADER_NAMES):
        raise Rejected("header_bounds")
    names = set()
    for pair in evidence.headers:
        if type(pair) is not tuple or len(pair) != 2:
            raise Rejected("header_shape")
        name, value = pair
        if type(name) is not str or name not in HEADER_NAMES or name in names or type(value) is not str or len(value) > 1024:
            raise Rejected("header_contract")
        if any(ord(c) < 32 or ord(c) > 126 for c in value):
            raise Rejected("header_control_character")
        names.add(name)
    try:
        raw = canonical(asdict(evidence))
    except (TypeError, ValueError):
        raise Rejected("capture_serialization") from None
    if len(raw.encode()) > 16384:
        raise Rejected("capture_bound")
    return raw


class _Title(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.html = False
        self.in_title = False
        self.titles = 0
        self.parts = []
        self.title_size = 0

    def handle_starttag(self, tag, attrs):
        if tag == "html":
            self.html = True
        if tag == "title":
            self.titles += 1
            self.in_title = True

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False

    def handle_data(self, data):
        if self.in_title:
            self.title_size += len(data)
            if self.title_size > 1024:
                raise Rejected("html_title_bound")
            self.parts.append(data)


def parse(profile, raw, receipt):
    """Fresh bodies supported. This is not the retrospective issuer earnings parser."""
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise Rejected("unsupported_utf8") from None
    if "\x00" in text:
        raise Rejected("nul_content")
    if profile == "faq_snapshot":
        parser = _Title()
        parser.feed(text)
        parser.close()
        title = " ".join("".join(parser.parts).split())
        if not parser.html or parser.titles != 1 or parser.in_title or not title:
            raise Rejected("html_document_shape")
        # The title is untrusted data, not instructions or verified economic news.
        return [ParsedEvent("MSFT", "reference_document_snapshot", FAQ, None,
                            "unknown", None, (), title[:500])]
    if profile != "ecb_daily_reference":
        raise Rejected("unknown_parser_profile")
    if "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
        raise Rejected("xml_declarations_refused")
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        raise Rejected("xml_malformed") from None
    ges = "{http://www.gesmes.org/xml/2002-08-01}"
    ns = "{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}"
    if root.tag != ges + "Envelope":
        raise Rejected("ecb_envelope")
    containers = root.findall(ns + "Cube")
    if len(containers) != 1 or len(containers[0]) != 1:
        raise Rejected("ecb_snapshot_count")
    dated = containers[0][0]
    if dated.tag != ns + "Cube" or set(dated.attrib) != {"time"}:
        raise Rejected("ecb_date_shape")
    day = dated.attrib["time"]
    try:
        parsed_day = date.fromisoformat(day)
    except ValueError:
        raise Rejected("ecb_date") from None
    if day != parsed_day.isoformat() or parsed_day > utc(receipt.received_at).date():
        raise Rejected("future_or_noncanonical_reference_date")
    rows = list(dated)
    if not 0 < len(rows) <= 100:
        raise Rejected("ecb_rows_bound")
    seen = set()
    events = []
    for row in rows:
        if row.tag != ns + "Cube" or set(row.attrib) != {"currency", "rate"} or len(row):
            raise Rejected("ecb_row_shape")
        code, rate = row.attrib["currency"], row.attrib["rate"]
        if not re.fullmatch("[A-Z]{3}", code) or code == "EUR" or code in seen:
            raise Rejected("ecb_currency")
        if not re.fullmatch(r"(?:0|[1-9][0-9]{0,5})(?:\.[0-9]{1,12})?", rate) or Decimal(rate) <= 0:
            raise Rejected("ecb_rate")
        seen.add(code)
        # The dataset date is NOT a claimed publication timestamp.
        events.append(ParsedEvent("EUR/" + code, "fx_daily_reference", day + "|" + code,
                                  None, "unknown", None, (),
                                  f"Reference date {day}: {rate} {code} per EUR; reference only"))
    return events


class DirectSourceAdapter:
    def __init__(self, path, scheduler, store, profiles, *, offline_test_enabled=False, clock=now):
        if type(offline_test_enabled) is not bool:
            raise ValueError("exact_boolean_required")
        if type(profiles) is not dict or not profiles or len(profiles) > 2:
            raise ValueError("profile_configuration")
        for sid, profile in profiles.items():
            if profile not in PROFILES or sid not in scheduler.sources:
                raise ValueError("profile_configuration")
            if scheduler.sources[sid].exact_url != PROFILES[profile][0]:
                raise ValueError("profile_url_mismatch")
        self.enabled = offline_test_enabled
        self.scheduler, self.store, self.clock = scheduler, store, clock
        self.profiles = MappingProxyType(dict(profiles))
        self.path = Path(path)
        files = (Path(__file__), Path(storage.__file__),
                 Path(__file__).parent / "upstream/scheduler/bounded_scheduler.py",
                 Path(__file__).parent / "core_v2/core.py",
                 Path(__file__).parent / "modular_ingestion/core.py")
        self.config = {"schema": 1, "adapter_version": VERSION, "profiles": dict(self.profiles),
                       "scheduler_db": str(scheduler.path.resolve()),
                       "store_db": str(store.path.resolve()), "run_id": scheduler.policy.run_id,
                       "scheduler_hash": scheduler.configuration_hash,
                       "code_hashes": {str(p.relative_to(Path(__file__).parent)): sha256(p.read_bytes()).hexdigest() for p in files},
                       "active_collection": False, "clock_verified": False}
        self._configuration_json = canonical(self.config)
        if self.path.is_symlink() or not self.path.parent.is_dir():
            raise ValueError("adapter_path")
        if self.path.resolve() in (scheduler.path.resolve(), store.path.resolve()):
            raise ValueError("separate_database_required")
        if self.path.exists():
            probe = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)
            try:
                row = probe.execute("SELECT config FROM adapter_metadata WHERE id=1").fetchone()
                if not row or row[0] != canonical(self.config):
                    raise ValueError("adapter_identity_mismatch")
            except sqlite3.Error:
                raise ValueError("not_adapter_database") from None
            finally:
                probe.close()
        self.db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS adapter_metadata (
            id INTEGER PRIMARY KEY CHECK(id=1), config TEXT NOT NULL, halted INTEGER NOT NULL DEFAULT 0, reason TEXT);
          CREATE TABLE IF NOT EXISTS mappings (
            attempt_id INTEGER PRIMARY KEY, state TEXT NOT NULL, source_id TEXT NOT NULL,
            body_hash TEXT NOT NULL, capture_json TEXT NOT NULL, capture_hash TEXT NOT NULL,
            normalized_at TEXT, observation_seq INTEGER, store_record_hash TEXT,
            confirmed_by TEXT, error TEXT);
          CREATE TRIGGER IF NOT EXISTS no_capture_edit BEFORE UPDATE OF
            attempt_id,source_id,body_hash,capture_json,capture_hash ON mappings
            BEGIN SELECT RAISE(ABORT,'immutable_capture'); END;
          CREATE TRIGGER IF NOT EXISTS no_mapping_delete BEFORE DELETE ON mappings
            BEGIN SELECT RAISE(ABORT,'immutable_mapping'); END;
          CREATE TRIGGER IF NOT EXISTS no_config_edit BEFORE UPDATE OF config ON adapter_metadata
            BEGIN SELECT RAISE(ABORT,'immutable_configuration'); END;
        ''')
        self.db.execute("INSERT OR IGNORE INTO adapter_metadata(id,config) VALUES(1,?)", (canonical(self.config),))
        self._check_configuration()

    def close(self):
        self.db.close()

    def _check_configuration(self):
        metadata = self.db.execute("SELECT config FROM adapter_metadata WHERE id=1").fetchone()
        if not metadata or metadata[0] != self._configuration_json or canonical(self.config) != self._configuration_json:
            raise Rejected("adapter_configuration_changed")
        if dict(self.profiles) != self.config["profiles"] or str(self.scheduler.path.resolve()) != self.config["scheduler_db"] or str(self.store.path.resolve()) != self.config["store_db"]:
            raise Rejected("adapter_runtime_configuration_changed")
        for sid, profile in self.profiles.items():
            if profile not in PROFILES or sid not in self.scheduler.sources or self.scheduler.sources[sid].exact_url != PROFILES[profile][0]:
                raise Rejected("profile_url_mismatch")
        row = self.scheduler.db.execute("SELECT config,config_hash FROM metadata WHERE id=1").fetchone()
        if not row or row["config_hash"] != self.config["scheduler_hash"] or sha256(row["config"].encode()).hexdigest() != row["config_hash"]:
            raise Rejected("scheduler_configuration_changed")
        current = json.loads(row["config"])
        if current["policy"] != asdict(self.scheduler.policy) or current["sources"] != [asdict(s) for s in sorted(self.scheduler.sources.values(), key=lambda s: s.source_id)]:
            raise Rejected("scheduler_runtime_configuration_changed")

    def _accepted(self, attempt_id):
        if type(attempt_id) is not int or attempt_id <= 0:
            raise Rejected("attempt_id")
        row = self.scheduler.db.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
        if not row or row["state"] != "received" or row["status"] != 200 or row["source_id"] not in self.profiles:
            raise Rejected("accepted_response_required")
        source = self.scheduler.sources[row["source_id"]]
        if source.approved is not True:
            raise Rejected("unapproved_source")
        raw = bytes(row["body"])
        if not 0 < len(raw) <= min(row["reserved_bytes"], source.response_cap, 1024 * 1024):
            raise Rejected("response_body_bound")
        if sha256(raw).hexdigest() != row["body_hash"]:
            raise Rejected("scheduler_body_hash")
        return row, source, raw

    def _validate(self, evidence, row, source, raw):
        expected = (self.config["run_id"], self.config["scheduler_hash"], row["id"], source.source_id,
                    source.exact_url, source.exact_url, row["body_hash"], len(raw), 200)
        got = (evidence.run_id, evidence.configuration_hash, evidence.attempt_id, evidence.source_id,
               evidence.requested_url, evidence.final_url, evidence.body_sha256, evidence.body_bytes, evidence.http_status)
        if got != expected or type(evidence.attempt_id) is not int or type(evidence.body_bytes) is not int or type(evidence.http_status) is not int:
            raise Rejected("capture_binding_mismatch")
        if evidence.representation != "http_entity_bytes_identity":
            raise Rejected("not_origin_entity_bytes")
        if evidence.evidence_class not in ("synthetic_transport_test", "direct_http_caller_attestation"):
            raise Rejected("direct_evidence_required")
        if evidence.body_complete is not True or evidence.owned_transport_reaped is not True or evidence.tls_verified is not True:
            raise Rejected("incomplete_or_uncertain_transport")
        if type(evidence.redirect_count) is not int or evidence.redirect_count != 0 or evidence.outcome != "received":
            raise Rejected("redirect_or_failed_transport")
        times = (evidence.request_monotonic, evidence.receive_monotonic)
        if any(type(t) not in (int, float) or not math.isfinite(t) or t < 0 for t in times):
            raise Rejected("monotonic_required")
        try:
            start, end = utc(evidence.requested_at), utc(evidence.received_at)
        except (TypeError, ValueError):
            raise Rejected("capture_timestamp") from None
        if int(start.timestamp()) != row["started"] or int(end.timestamp()) != row["finished"]:
            raise Rejected("scheduler_clock_binding")
        wall = (end - start).total_seconds()
        mono = times[1] - times[0]
        if wall < 0 or mono < 0 or abs(wall - mono) > 0.5:
            raise Rejected("clock_discontinuity")
        if end.timestamp() >= row["deadline"] or mono >= row["deadline"] - row["started"]:
            raise Rejected("transport_deadline")
        headers = dict(evidence.headers)
        if headers.get("content-encoding", "identity").lower() != "identity":
            raise Rejected("encoded_body_not_supported")
        if "content-length" in headers and (not re.fullmatch("[0-9]{1,8}", headers["content-length"]) or int(headers["content-length"]) != len(raw)):
            raise Rejected("content_length_mismatch")
        media = headers.get("content-type", "").lower().split(";", 1)[0].strip()
        if media not in PROFILES[self.profiles[source.source_id]][2]:
            raise Rejected("content_type_mismatch")
        charset = re.search(r"charset\s*=\s*([^;\s]+)", headers.get("content-type", ""), re.I)
        if charset and charset.group(1).strip('"\'').lower() not in ("utf-8", "utf8"):
            raise Rejected("unsupported_charset")
        # UTC consistency is NOT synchronized clock-health evidence.
        receipt = Receipt(source.source_id, source.exact_url, stamp(evidence.requested_at),
                          stamp(evidence.received_at), "direct", row["body_hash"], 200,
                          False, True, *times)
        receipt.validate(raw)
        return receipt

    def ingest(self, attempt_id, evidence, *, _after_store=None, _before_confirm=None):
        if not self.enabled:
            raise Rejected("adapter_disabled")
        self._check_configuration()
        row, source, raw = self._accepted(attempt_id)
        try:
            serialized = capture_json(evidence)
        except Exception:
            # Malformed metadata is terminal for this attempt too. Do not store
            # unknown/secret-bearing headers or raw exception messages.
            tombstone = canonical({"capture_rejected": True})
            self.db.execute("BEGIN IMMEDIATE")
            try:
                previous = self.db.execute("SELECT state FROM mappings WHERE attempt_id=?", (attempt_id,)).fetchone()
                if previous and previous[0] != "complete":
                    raise Rejected("uncertain_or_rejected_attempt_no_retry")
                self.db.execute("INSERT OR IGNORE INTO mappings(attempt_id,state,source_id,body_hash,capture_json,capture_hash,error) VALUES(?,'rejected',?,?,?,?,?)",
                                (attempt_id, source.source_id, row["body_hash"], tombstone,
                                 sha256(tombstone.encode()).hexdigest(), "capture_contract_rejected"))
                self.db.execute("UPDATE adapter_metadata SET halted=1,reason='capture_contract_rejected' WHERE id=1")
                self.db.execute("COMMIT")
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                raise
            raise Rejected("capture_contract_rejected") from None
        capture_hash = sha256(serialized.encode()).hexdigest()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            previous = self.db.execute("SELECT * FROM mappings WHERE attempt_id=?", (attempt_id,)).fetchone()
            if previous:
                if previous["state"] != "complete":
                    raise Rejected("uncertain_or_rejected_attempt_no_retry")
                if previous["body_hash"] != row["body_hash"] or previous["capture_hash"] != capture_hash:
                    raise Rejected("attempt_evidence_conflict")
                self.db.execute("COMMIT")
                return {"observation_seq": previous["observation_seq"], "duplicate_call": True,
                        "confirmation": "confirmed" if previous["confirmed_by"] else "pending"}
            if self.db.execute("SELECT halted FROM adapter_metadata WHERE id=1").fetchone()[0]:
                raise Rejected("adapter_halted")
            # A crash can prevent the catch block from persisting halted=1.
            # The durable claim itself is sufficient to fence all later appends.
            if self.db.execute("SELECT 1 FROM mappings WHERE state!='complete' OR confirmed_by IS NULL LIMIT 1").fetchone():
                raise Rejected("unresolved_mapping_no_further_ingest")
            self.db.execute("INSERT INTO mappings(attempt_id,state,source_id,body_hash,capture_json,capture_hash) VALUES(?,'claimed',?,?,?,?)",
                            (attempt_id, source.source_id, row["body_hash"], serialized, capture_hash))
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise
        stage = "validation"
        try:
            receipt = self._validate(evidence, row, source, raw)
            profile = self.profiles[source.source_id]
            core_spec = SourceSpec(source.source_id, PROFILES[profile][1], (source.exact_url,),
                                   profile, source.cadence, True, "disabled offline-tested direct normalization; not collection authority")
            reg = Registry()
            reg.register_source(core_spec)
            reg.register_adapter(profile, VERSION, lambda b, r: parse(profile, b, r))
            events = reg.ingest(source.source_id, raw, receipt)
            normalized = stamp(self.clock())  # AFTER complete parse/validation.
            if utc(normalized) < utc(receipt.received_at):
                raise Rejected("normalization_before_receipt")
            self.db.execute("UPDATE mappings SET normalized_at=? WHERE attempt_id=?", (normalized, attempt_id))
            stage = "store_append"
            spec = storage.SourceSpec(**{**asdict(core_spec), "source_class": storage.SourceClass(core_spec.source_class.value)})
            try:
                _, existing = self.store.source(source.source_id)
            except KeyError:
                self.store.register_source(spec, reviewer="offline_direct_adapter_configuration",
                                           rationale="Exact scheduler and parser profile; no live authority")
            else:
                if storage.spec_dict(existing) != storage.spec_dict(spec):
                    raise Rejected("store_source_configuration_mismatch")
            seq = self.store.append_batch(raw, storage.Receipt(**asdict(receipt)),
                                          [storage.ParsedEvent(**asdict(e.parsed)) for e in events], VERSION)
            with self.store.connect() as db:
                stored = db.execute("SELECT record_hash FROM records WHERE seq=? AND kind='event_batch'", (seq,)).fetchone()
            if stored is None:
                raise Rejected("stored_batch_missing")
            if _after_store:
                _after_store()
            self.db.execute("UPDATE mappings SET state='complete',observation_seq=?,store_record_hash=? WHERE attempt_id=? AND state='claimed'",
                            (seq, stored[0], attempt_id))
            if _before_confirm:
                _before_confirm()
            self.confirm_mapping(attempt_id)
            return {"observation_seq": seq, "duplicate_call": False, "confirmation": "confirmed"}
        except BaseException as exc:
            # Once storage was called its commit outcome may be uncertain; do not retry.
            reason = str(exc) if isinstance(exc, Rejected) else "normalization_failure" if stage == "validation" else "storage_or_confirmation_uncertain"
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute("UPDATE adapter_metadata SET halted=1,reason=? WHERE id=1", (reason,))
                self.db.execute("UPDATE mappings SET state=CASE WHEN ?='validation' THEN 'rejected' ELSE state END,error=? WHERE attempt_id=?",
                                (stage, reason, attempt_id))
                self.db.execute("COMMIT")
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                raise
            raise

    def confirm_mapping(self, attempt_id):
        """Explicit post-commit confirmation only; never retries an uncertain append."""
        if not self.enabled:
            raise Rejected("adapter_disabled")
        self._check_configuration()
        row = self.db.execute("SELECT * FROM mappings WHERE attempt_id=?", (attempt_id,)).fetchone()
        if not row or row["state"] != "complete":
            raise Rejected("mapping_not_complete")
        if row["confirmed_by"] is not None:
            return
        self.store.verify()
        with self.store.connect() as db:
            batch = db.execute("SELECT record_hash,recorded_at FROM records WHERE seq=? AND kind='event_batch'", (row["observation_seq"],)).fetchone()
            confirmations = [t for t, payload in db.execute("SELECT recorded_at,payload FROM records WHERE kind='commit_confirmation'")
                             if json.loads(payload)["observation_seq"] == row["observation_seq"]]
        if not batch or batch[0] != row["store_record_hash"] or len(confirmations) != 1:
            raise Rejected("store_mapping_lineage")
        confirmed = stamp(self.clock())
        if confirmed < max(row["normalized_at"], batch[1], confirmations[0]):
            raise Rejected("confirmation_clock_regression")
        self.db.execute("UPDATE mappings SET confirmed_by=? WHERE attempt_id=? AND confirmed_by IS NULL", (confirmed, attempt_id))

    def events_as_locally_observed(self, as_of):
        """Only conservative confirmed local visibility, never historical tradability."""
        self._check_configuration()
        self.store.verify()
        cutoff = stamp(as_of)
        mappings = {}
        for row in self.db.execute("SELECT * FROM mappings WHERE state='complete' AND confirmed_by<=? AND normalized_at<=?", (cutoff, cutoff)):
            captured = json.loads(row["capture_json"])
            captured["headers"] = tuple(tuple(p) for p in captured["headers"])
            evidence = CaptureEvidence(**captured)
            serialized = capture_json(evidence)
            if sha256(serialized.encode()).hexdigest() != row["capture_hash"]:
                raise Rejected("capture_hash_mismatch")
            attempt, source, raw = self._accepted(row["attempt_id"])
            receipt = self._validate(evidence, attempt, source, raw)
            with self.store.connect() as db:
                batch = db.execute("SELECT record_hash,payload FROM records WHERE seq=? AND kind='event_batch'", (row["observation_seq"],)).fetchone()
            if not batch or batch[0] != row["store_record_hash"] or json.loads(batch[1])["receipt"] != asdict(receipt):
                raise Rejected("store_mapping_lineage")
            mappings[row["observation_seq"]] = dict(row)
        out = []
        for item in self.store.events_as_observed(as_of):
            observations = []
            for observation in item["observations"]:
                mapping = mappings.get(observation["record_seq"])
                if mapping:
                    provenance = json.loads(mapping["capture_json"])
                    observations.append({"storage": observation, "adapter": mapping,
                                         "synthetic": provenance["evidence_class"] == "synthetic_transport_test",
                                         "provenance_authentication": "caller_attested_not_authenticated"})
            if observations:
                out.append({"event": item["event"], "observations": observations,
                            "evidence_class": "offline_tested_direct_normalization",
                            "source_available_at": None, "historical_available_at": None,
                            "original_verified": False, "revision_status": "unknown",
                            "clock_verified": False, "eligible_for_backtest": False,
                            "active_collection": False, "order_permission": "none"})
        return out


if __name__ == "__main__":
    raise SystemExit("disabled; offline normalization library only; no HTTP implementation")
