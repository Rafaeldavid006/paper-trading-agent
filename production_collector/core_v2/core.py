"""Offline evidence registry. No network, trading or automatic source approval."""
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from urllib.parse import urlsplit
import json

class SourceClass(str, Enum):
    ISSUER = 'issuer'
    FILINGS = 'filings'
    FX = 'fx'
    WEATHER = 'weather'
    MACRO = 'macro'
    REGULATORY = 'regulatory'
    CONFLICT = 'conflict'
    LEADER = 'leader_statement'
    ELECTION = 'election'
    HISTORY = 'market_history'
    METALS = 'metals'
    OTHER = 'other'

def utc(value):
    if not isinstance(value, str): raise ValueError('timestamp_required')
    d = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if d.tzinfo is None: raise ValueError('timezone_required')
    return d.astimezone(timezone.utc)

def pinned_url(value):
    u = urlsplit(value)
    if u.scheme != 'https' or not u.hostname or u.username or u.password or u.port or u.query or u.fragment:
        raise ValueError('exact_https_url_required')
    return value

@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    source_class: SourceClass
    urls: tuple[str, ...]
    adapter: str | None
    max_age_seconds: int
    approved: bool = False
    coverage_note: str = 'unverified'
    def __post_init__(self):
        if type(self.approved) is not bool: raise ValueError('boolean_approval_required')
        if not self.source_id or not isinstance(self.source_class, SourceClass): raise ValueError('source_identity')
        if type(self.max_age_seconds) is not int or self.max_age_seconds <= 0: raise ValueError('freshness_policy')
        if type(self.urls) is not tuple or not self.urls or len(set(self.urls)) != len(self.urls): raise ValueError('url_set')
        for url in self.urls: pinned_url(url)

@dataclass(frozen=True)
class Receipt:
    source_id: str
    url: str
    requested_at: str
    received_at: str
    mode: str  # retrospective | direct | tool
    body_sha256: str
    http_status: int | None
    clock_verified: bool = False
    origin_bytes: bool = False
    request_monotonic: float | None = None
    receive_monotonic: float | None = None
    def validate(self, raw):
        pinned_url(self.url)
        if type(self.clock_verified) is not bool or type(self.origin_bytes) is not bool: raise ValueError('boolean_attestation_required')
        if type(raw) is not bytes or not 0 < len(raw) <= 2_000_000: raise ValueError('body_bounds')
        if utc(self.requested_at) > utc(self.received_at): raise ValueError('receipt_order')
        if self.mode not in ('retrospective', 'direct', 'tool'): raise ValueError('mode')
        if self.body_sha256 != sha256(raw).hexdigest(): raise ValueError('body_hash')
        if self.mode == 'tool' and (self.origin_bytes or self.http_status is not None): raise ValueError('tool_not_origin')
        if self.mode != 'tool' and (not self.origin_bytes or self.http_status != 200): raise ValueError('origin_capture_required')
        if self.mode == 'direct':
            import math
            times = (self.request_monotonic, self.receive_monotonic)
            if any(type(v) not in (int,float) or not math.isfinite(v) for v in times): raise ValueError('monotonic_required')
            if times[1] < times[0]: raise ValueError('monotonic_order')
            wall = (utc(self.received_at)-utc(self.requested_at)).total_seconds()
            if abs(wall-(times[1]-times[0])) > 2: raise ValueError('clock_discontinuity')

@dataclass(frozen=True)
class ParsedEvent:
    entity: str
    event_type: str
    logical_key: str
    publisher_claim: str | None
    publisher_precision: str
    # Date claims are not silently midnight timestamps. Text is never an instruction.
    publisher_timestamp: str | None = None
    sectors: tuple[str, ...] = ()
    fact_summary: str = ''
    def __post_init__(self):
        if type(self.sectors) is not tuple or any(type(s) is not str for s in self.sectors): raise ValueError('immutable_sector_labels')
        if len(self.fact_summary)>2000: raise ValueError('summary_bound')

@dataclass(frozen=True)
class Event:
    schema: str
    event_id: str
    version_id: str
    source_id: str
    source_class: str
    adapter_version: str
    parsed: ParsedEvent
    receipt: Receipt
    original_verified: bool = field(default=False, init=False)
    revision_status: str = field(default='unknown', init=False)
    source_available_at: None = field(default=None, init=False)
    historical_first_seen_at: None = field(default=None, init=False)
    eligible_for_backtest: bool = field(default=False, init=False)
    order_permission: str = field(default='none', init=False)

class Registry:
    def __init__(self):
        self.sources = {}; self.adapters = {}; self.candidates = {}; self.events = {}; self.observations = []; self.observation_links = []
    def register_adapter(self, name, version, adapter):
        if name in self.adapters or not version or not callable(adapter): raise ValueError('adapter_registration')
        self.adapters[name] = (version, adapter)
    def register_source(self, spec):
        if spec.source_id in self.sources: raise ValueError('source_already_registered')
        self.sources[spec.source_id] = spec
    def propose_source(self, url, source_class, discovered_via, reason):
        """Discovery adds a review candidate, never a source, URL permission or fetch."""
        pinned_url(url); pinned_url(discovered_via)
        if not isinstance(source_class, SourceClass) or not reason: raise ValueError('candidate_metadata')
        key = sha256(url.encode()).hexdigest()
        self.candidates.setdefault(key, {'url':url, 'source_class':source_class.value,
            'discovered_via':discovered_via, 'reason':reason, 'status':'pending_review'})
        return key
    def ingest(self, source_id, raw, receipt, *, recorded_at=None):
        spec = self.sources[source_id]
        if not spec.approved: raise ValueError('source_not_approved')
        if receipt.source_id != source_id or receipt.url not in spec.urls: raise ValueError('source_url_mismatch')
        receipt.validate(raw)
        if receipt.mode == 'tool': raise ValueError('tool_observation_not_raw_adapter_input')
        if spec.adapter not in self.adapters: raise ValueError('adapter_missing')
        version, adapter = self.adapters[spec.adapter]
        from itertools import islice
        parsed = list(islice(adapter(raw, receipt), 1001))
        if len(parsed)>1000: raise ValueError('event_batch_bound')
        staged = []
        for p in parsed:
            if not isinstance(p,ParsedEvent) or not p.entity or not p.event_type or not p.logical_key: raise ValueError('event_identity')
            if p.publisher_precision not in ('unknown','date','minute','second'): raise ValueError('publication_precision')
            if p.publisher_precision == 'date' and p.publisher_claim:
                from datetime import date
                try: day = date.fromisoformat(p.publisher_claim)
                except ValueError: day = None  # issuer free-text date retained, not parsed as UTC
                if day is not None and day>utc(receipt.received_at).date(): raise ValueError('future_date_claim')
            if p.publisher_timestamp is not None:
                if p.publisher_precision not in ('minute','second'): raise ValueError('date_not_timestamp')
                if utc(p.publisher_timestamp)>utc(receipt.received_at): raise ValueError('future_publisher_claim')
            eid=sha256(json.dumps([source_id,p.entity,p.event_type,p.logical_key],separators=(',',':')).encode()).hexdigest()
            vid=sha256(json.dumps([eid,receipt.body_sha256,version],separators=(',',':')).encode()).hexdigest()
            staged.append(Event('research-event-v1',eid,vid,source_id,spec.source_class.value,version,p,receipt))
        unique = {}
        for event in staged:
            prior = unique.get(event.version_id) or self.events.get(event.version_id)
            if prior is not None and prior.parsed != event.parsed: raise ValueError('conflicting_same_version')
            unique[event.version_id] = event
        recorded_at = recorded_at or datetime.now(timezone.utc).isoformat()
        if utc(recorded_at)<utc(receipt.received_at): raise ValueError('recorded_before_receipt')
        # Stamp after parsing, never before potentially slow adapter execution.
        # Validate whole batch before mutation. Keep ALL receipts, including duplicates.
        self.observations.append(receipt)
        self.observation_links.append((receipt,tuple(unique),recorded_at))
        for event in staged: self.events.setdefault(event.version_id,event)
        return tuple(self.events[e.version_id] for e in staged)
    def first_direct_observed(self, event_id, *, as_of=None):
        """Earliest verified direct receipt ACTUALLY linked to this event.
        This is not publication/source availability. as_of also gates recording.
        """
        cutoff=utc(as_of) if as_of is not None else None
        times=[]
        for receipt,version_ids,recorded_at in self.observation_links:
            if receipt.mode!='direct' or receipt.clock_verified is not True:continue
            if cutoff is not None and (utc(receipt.received_at)>cutoff or utc(recorded_at)>cutoff):continue
            if any(self.events[v].event_id==event_id for v in version_ids):times.append(receipt.received_at)
        return min(times,key=utc) if times else None
    def observed_as_of(self, as_of):
        """Return only versions observed AND recorded by cutoff, never trade ready.
        Includes retrospective versions only from their actual modern receipt,
        never from publication/event dates. Not a provenance authentication API.
        """
        cutoff=utc(as_of);out=[];seen=set()
        for receipt,version_ids,recorded_at in sorted(self.observation_links,key=lambda x:(utc(x[0].received_at),utc(x[2]))):
            if utc(receipt.received_at)>cutoff or utc(recorded_at)>cutoff:continue
            for vid in version_ids:
                if vid not in seen:
                    out.append(replace(self.events[vid],receipt=receipt));seen.add(vid)
        return tuple(out)
    def freshness(self,event,as_of):
        now=utc(as_of); seen=utc(event.receipt.received_at)
        if now<seen: return 'not_yet_observed'
        if event.receipt.mode!='direct' or not event.receipt.clock_verified: return 'unknown'
        return 'recent_observation' if (now-seen).total_seconds()<=self.sources[event.source_id].max_age_seconds else 'stale_observation'
    def coverage(self):
        return {sid:{'approved_offline_ingestion':s.approved,'adapter_registered':s.adapter in self.adapters,
            'observed_versions':sum(e.source_id==sid for e in self.events.values()),
            'active_collection':False,'note':s.coverage_note} for sid,s in self.sources.items()}
