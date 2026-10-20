"""Offline provenance boundary. No network, credentials, broker, or deployment wiring.

A valid receipt proves only possession of the configured collector signing key.
It does not prove that an issuer published bytes. See TRUSTED_EVENT_BOUNDARY.md.
"""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
import sqlite3
from types import MappingProxyType
from urllib.parse import urlsplit
from active_demo_pipeline import Event

MAX_BYTES = 262144
SCHEMA = 'issuer-earnings-envelope-v1'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})', value):
        raise ValueError('ambiguous_timestamp')
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_json_key')
        result[key] = value
    return result


@dataclass(frozen=True)
class ImmutableSnapshot:
    source_bytes: bytes
    sha256: str


@dataclass(frozen=True)
class Issuer:
    issuer_id: str
    symbol: str
    origins: tuple[str, ...]


@dataclass(frozen=True)
class SourceReceipt:
    collector_id: str
    source_url: str
    snapshot_hash: str
    acquired_at: str
    available_at: str
    signature: str

    def payload(self):
        return {k: getattr(self, k) for k in ('collector_id', 'source_url', 'snapshot_hash', 'acquired_at', 'available_at')}


@dataclass(frozen=True)
class BuildResult:
    event: Event | None
    reason: str


class TrustedEventBuilder:
    """Configured trust anchors must be controlled by application, not event callers.

    build(raw_release, source_receipt, issuer_registry, immutable_snapshot)
    is intentionally stricter than the requested conceptual API: the registry is
    pinned at construction so callers cannot supply their own issuer approval.
    """
    def __init__(self, database, issuer_registry, collector_keys, now):
        if str(database) == ':memory:':
            raise ValueError('durable_ledger_required')
        if not issuer_registry or not collector_keys or not callable(now):
            raise ValueError('trust_configuration_required')
        self.registry = {}
        for issuer in issuer_registry:
            if type(issuer) is not Issuer or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', issuer.issuer_id) or not re.fullmatch(r'[A-Z]{1,5}', issuer.symbol):
                raise ValueError('invalid_registry')
            if issuer.issuer_id in self.registry or not issuer.origins:
                raise ValueError('invalid_registry')
            for origin in issuer.origins:
                u = urlsplit(origin)
                if u.scheme != 'https' or not u.hostname or u.username or u.password or u.port not in (None,443) or u.path or u.query or u.fragment or u.netloc != u.hostname:
                    raise ValueError('invalid_registry_origin')
            self.registry[issuer.issuer_id] = issuer
        self.registry = MappingProxyType(self.registry)
        self.keys = MappingProxyType(dict(collector_keys))
        if any(not isinstance(k,str) or type(v) is not bytes or len(v)<32 for k,v in self.keys.items()):
            raise ValueError('invalid_collector_key')
        self.database, self.now = str(database), now
        self.binding = hashlib.sha256(canonical([vars(x) for x in sorted(self.registry.values(),key=lambda x:x.issuer_id)])).hexdigest()
        with sqlite3.connect(self.database) as c:
            c.executescript('CREATE TABLE IF NOT EXISTS trusted_boundary_config(binding TEXT PRIMARY KEY); CREATE TABLE IF NOT EXISTS trusted_episodes(episode TEXT PRIMARY KEY, digest TEXT NOT NULL, version TEXT NOT NULL, status TEXT NOT NULL); CREATE TABLE IF NOT EXISTS trusted_events(event_id TEXT PRIMARY KEY, payload TEXT NOT NULL);')
            prior = c.execute('SELECT binding FROM trusted_boundary_config').fetchall()
            if prior and prior != [(self.binding,)]:
                raise ValueError('registry_binding_changed')
            c.execute('INSERT OR IGNORE INTO trusted_boundary_config VALUES(?)',(self.binding,))

    def build(self, raw_release, source_receipt, immutable_snapshot):
        """Return Event or stable rejection. No network or caller verification flag."""
        try:
            return self._build(raw_release, source_receipt, immutable_snapshot)
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, OverflowError):
            return BuildResult(None, 'invalid_or_untrusted_input')
        except sqlite3.Error:
            return BuildResult(None, 'ledger_unavailable')

    def _build(self, raw, receipt, snapshot):
        if type(raw) is not bytes or not 0 < len(raw) <= MAX_BYTES or type(snapshot) is not ImmutableSnapshot or type(snapshot.source_bytes) is not bytes:
            return BuildResult(None, 'invalid_snapshot')
        digest = hashlib.sha256(raw).hexdigest()
        if raw != snapshot.source_bytes or digest != snapshot.sha256:
            return BuildResult(None, 'snapshot_mismatch')
        if type(receipt) is not SourceReceipt or receipt.snapshot_hash != digest or receipt.collector_id not in self.keys:
            return BuildResult(None, 'untrusted_receipt')
        expected = hmac.new(self.keys[receipt.collector_id], canonical(receipt.payload()), hashlib.sha256).hexdigest()
        if not isinstance(receipt.signature,str) or not hmac.compare_digest(expected,receipt.signature):
            return BuildResult(None, 'untrusted_receipt')
        acquired, available = timestamp(receipt.acquired_at), timestamp(receipt.available_at)
        now = self.now()
        if not isinstance(now, datetime) or now.utcoffset() is None or not acquired <= available <= now:
            return BuildResult(None, 'invalid_receipt_time')
        release = json.loads(raw.decode('utf-8'), object_pairs_hook=strict_object, parse_constant=lambda x: (_ for _ in ()).throw(ValueError()))
        fields = {'schema','issuer_id','symbol','episode_id','version','published_at','event_type','corrected'}
        if type(release) is not dict or set(release) != fields or release['schema'] != SCHEMA:
            return BuildResult(None, 'unsupported_release_schema')
        issuer = self.registry.get(release['issuer_id'])
        if not issuer or issuer.symbol != release['symbol']:
            return BuildResult(None, 'issuer_mismatch')
        u = urlsplit(receipt.source_url)
        if u.scheme != 'https' or u.username or u.password or u.fragment or u.port not in (None,443) or '\\' in receipt.source_url or any(ord(c)<33 for c in receipt.source_url) or f'https://{u.netloc}' not in issuer.origins:
            return BuildResult(None, 'source_origin_mismatch')
        if release['event_type'] != 'earnings' or type(release['corrected']) is not bool:
            return BuildResult(None, 'ineligible_event')
        for field in ('episode_id','version'):
            if not isinstance(release[field],str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,96}',release[field]):
                return BuildResult(None, 'invalid_event_identity')
        published = timestamp(release['published_at'])
        if not published <= acquired <= available:
            return BuildResult(None, 'late_or_future_release')
        episode = hashlib.sha256(canonical([issuer.issuer_id,release['episode_id']])).hexdigest()
        event_id = hashlib.sha256(canonical([episode,digest])).hexdigest()
        event = Event(event_id=event_id, episode_id=episode, version=digest, symbol=issuer.symbol, published_at=published, observed_at=available, source_url=receipt.source_url, source_hash=digest, verified=True, corrected=False, timestamp_precision='second', event_type='earnings')
        with sqlite3.connect(self.database, timeout=5) as c:
            c.execute('BEGIN IMMEDIATE')
            prior = c.execute('SELECT digest,version,status FROM trusted_episodes WHERE episode=?',(episode,)).fetchone()
            if release['corrected'] or (prior and (prior[0] != digest or prior[1] != release['version'])):
                c.execute('INSERT INTO trusted_episodes VALUES(?,?,?,?) ON CONFLICT(episode) DO UPDATE SET status=excluded.status',(episode,digest,release['version'],'quarantined'))
                return BuildResult(None,'corrected_or_conflicting_episode')
            if prior:
                return BuildResult(None,'quarantined_episode' if prior[2]=='quarantined' else 'duplicate_episode')
            if (available-published).total_seconds()>120:
                return BuildResult(None, 'late_or_future_release')
            c.execute('INSERT INTO trusted_episodes VALUES(?,?,?,?)',(episode,digest,release['version'],'accepted'))
            c.execute('INSERT INTO trusted_events VALUES(?,?)',(event_id,self._event_payload(event)))
        return BuildResult(event, 'accepted_collector_attested')

    def episode_eligible(self, episode_id):
        """Mandatory recheck before decisions: a later correction can quarantine."""
        try:
            with sqlite3.connect(self.database) as c:
                row = c.execute('SELECT status FROM trusted_episodes WHERE episode=?',(episode_id,)).fetchone()
            return row == ('accepted',)
        except sqlite3.Error:
            return False

    @staticmethod
    def _event_payload(event):
        values = asdict(event)
        for key in ('published_at','observed_at'):
            values[key] = values[key].isoformat()
        return canonical(values).decode()

    def event_eligible(self, event):
        """Exact ledger binding; use instead of trusting Event.verified directly."""
        if type(event) is not Event:
            return False
        try:
            with sqlite3.connect(self.database) as c:
                row = c.execute('SELECT e.payload FROM trusted_events e JOIN trusted_episodes p ON p.episode=? WHERE e.event_id=? AND p.status=?',(event.episode_id,event.event_id,'accepted')).fetchone()
            return row is not None and row[0] == self._event_payload(event)
        except (sqlite3.Error,TypeError,ValueError,AttributeError):
            return False
