"""Opt-in, read-only SEC metadata ingestion. No scheduling, signals or orders.

Recent submissions are a bounded snapshot, NOT a complete historical feed.
Only explicit-offset acceptance timestamps can be projected into event_core.
"""
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import math
import re
import sqlite3
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, build_opener, HTTPRedirectHandler

from event_core import Store, iso, utc

ADAPTER_VERSION = 'sec-submissions-v1'
MAX_BYTES = 8 * 1024 * 1024
MAX_ROWS = 10000


class SourceError(ValueError):
    """A source fetch or payload is unusable; never interpreted as no filings."""


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def cik_number(value):
    if isinstance(value, bool) or not re.fullmatch(r'[0-9]{1,10}', str(value)):
        raise ValueError('CIK must contain 1–10 digits')
    if int(value) == 0:
        raise ValueError('CIK must be positive')
    return str(int(value)).zfill(10)


def source_url(cik):
    return f'https://data.sec.gov/submissions/CIK{cik_number(cik)}.json'


def acceptance_time(value):
    # Do not invent midnight or an offset for date-only/naive source values.
    if not value:
        return None
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})', value):
        return None
    try:
        return iso(value)
    except (ValueError, OverflowError):
        return None


def parse_submissions(payload, cik):
    cik = cik_number(cik)
    if not isinstance(payload, dict) or 'cik' not in payload:
        raise SourceError('missing issuer identity')
    try:
        if cik_number(payload['cik']) != cik:
            raise SourceError('issuer identity mismatch')
    except ValueError as exc:
        raise SourceError('invalid issuer identity') from exc
    filings = payload.get('filings')
    if not isinstance(filings, dict) or not isinstance(filings.get('recent'), dict):
        raise SourceError('missing recent filings snapshot')
    recent = filings['recent']
    required = ('accessionNumber', 'form', 'filingDate', 'primaryDocument')
    if any(not isinstance(recent.get(k), list) for k in required):
        raise SourceError('missing required column')
    n = len(recent['accessionNumber'])
    if n > MAX_ROWS:
        raise SourceError('snapshot exceeds row limit; no silent truncation')
    if any(not isinstance(v, list) or len(v) != n for v in recent.values()):
        raise SourceError('inconsistent parallel arrays; possible truncation')
    rows = []
    seen = {}
    for i in range(n):
        row = {k: v[i] for k, v in recent.items()}
        if any(not isinstance(row[k], str) for k in required):
            raise SourceError('invalid required field type')
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', row['accessionNumber']):
            raise SourceError('invalid accession number')
        if not row['form'].strip() or len(row['form']) > 100:
            raise SourceError('invalid form')
        try:
            if date.fromisoformat(row['filingDate']).isoformat() != row['filingDate']:
                raise ValueError()
        except ValueError as exc:
            raise SourceError('invalid filing date') from exc
        doc = row['primaryDocument']
        parts = doc.split('/')
        if (not doc or len(doc) > 2048 or '\\' in doc or
                any(part in ('', '.', '..') for part in parts) or
                any(ord(char) < 32 or ord(char) == 127 for char in doc)):
            raise SourceError('invalid primary document relative path')
        if len(canonical(row).encode()) > 128 * 1024:
            raise SourceError('oversized filing record')
        accession = row['accessionNumber']
        fingerprint = digest(row)
        if accession in seen and seen[accession] != fingerprint:
            raise SourceError('conflicting duplicate accession in one snapshot')
        seen[accession] = fingerprint
        rows.append({'cik': cik, 'accession': accession, 'raw': row,
                     'accepted_at': acceptance_time(row.get('acceptanceDateTime')),
                     'amendment': row['form'].endswith('/A'),
                     'document_url': f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{quote(doc, safe="/")}'})
    archives = filings.get('files', [])
    if not isinstance(archives, list):
        raise SourceError('invalid historical file metadata')
    return rows, bool(archives)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SourceError('redirect refused; expected official SEC endpoint')


@dataclass(frozen=True)
class Snapshot:
    cik: str
    payload: dict
    fetched_at: str
    cache_hit: bool


class SecClient:
    """One client per process, serialized <=1 request/sec including retries.

    Share the client across callers. Cross-process/global SEC limits require an
    external coordinator; this adapter deliberately does not schedule itself.
    """
    def __init__(self, path, user_agent, *, cache_seconds=300, attempts=3,
                 timeout=20, opener=None, clock=None, monotonic=None, sleep=None):
        if (not isinstance(user_agent, str) or len(user_agent) > 256 or
                '\n' in user_agent or '\r' in user_agent or
                not re.search(r'[^\s@]+@[^\s@]+\.[^\s@]+', user_agent) or
                len(user_agent.split()) < 2):
            raise ValueError('descriptive organization/application and contact email required')
        if type(attempts) is not int or not 1 <= attempts <= 5:
            raise ValueError('attempts must be 1–5')
        if not math.isfinite(cache_seconds) or not 0 <= cache_seconds <= 86400:
            raise ValueError('cache TTL must be 0–86400 seconds')
        if not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError('timeout must be 0–120 seconds')
        self.path, self.user_agent = str(path), user_agent
        self.cache_seconds, self.attempts, self.timeout = cache_seconds, attempts, timeout
        self.opener = opener or build_opener(NoRedirect()).open
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic, self.sleep = monotonic or time.monotonic, sleep or time.sleep
        self.lock, self.last_request = threading.Lock(), None
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE IF NOT EXISTS sec_cache (cik TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, payload TEXT NOT NULL)')

    def fetch(self, cik):
        cik = cik_number(cik)
        with self.lock:
            now = utc(self.clock())
            with sqlite3.connect(self.path) as db:
                cached = db.execute('SELECT fetched_at,payload FROM sec_cache WHERE cik=?', (cik,)).fetchone()
            try:
                cache_fresh = cached and 0 <= (now - utc(cached[0])).total_seconds() < self.cache_seconds
            except (ValueError, TypeError, OverflowError):
                cache_fresh = False
            if cache_fresh:
                try:
                    payload = json.loads(cached[1])
                    parse_submissions(payload, cik)
                    return Snapshot(cik, payload, cached[0], True)
                except (ValueError, TypeError):
                    pass  # Corrupt local cache is never trusted as source evidence.
            for attempt in range(self.attempts):
                if self.last_request is not None:
                    self.sleep(max(0, 1 - (self.monotonic() - self.last_request)))
                self.last_request = self.monotonic()
                request = Request(source_url(cik), headers={
                    'User-Agent': self.user_agent, 'Accept': 'application/json',
                    'Accept-Encoding': 'identity'})
                try:
                    with self.opener(request, timeout=self.timeout) as response:
                        if response.status != 200:
                            raise SourceError(f'unexpected HTTP status {response.status}')
                        raw = response.read(MAX_BYTES + 1)
                        if len(raw) > MAX_BYTES:
                            raise SourceError('response exceeds byte limit')
                        try:
                            payload = json.loads(raw, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
                            parse_submissions(payload, cik)
                        except (ValueError, TypeError, UnicodeError) as exc:
                            raise SourceError('invalid SEC JSON payload') from exc
                    stamp = iso(self.clock())
                    with sqlite3.connect(self.path) as db:
                        db.execute('INSERT OR REPLACE INTO sec_cache VALUES (?,?,?)',
                                   (cik, stamp, canonical(payload)))
                    return Snapshot(cik, payload, stamp, False)
                except HTTPError as exc:
                    exc.close()
                    if exc.code not in (429, 500, 502, 503, 504) or attempt + 1 == self.attempts:
                        raise SourceError(f'SEC HTTP {exc.code}; no valid snapshot') from exc
                    delay = self._retry_delay(exc.headers.get('Retry-After'), attempt)
                except (URLError, TimeoutError, ConnectionError, OSError) as exc:
                    if attempt + 1 == self.attempts:
                        raise SourceError('SEC network failure; no valid snapshot') from exc
                    delay = 2 ** attempt
                self.sleep(delay)
        raise SourceError('no valid snapshot')

    def _retry_delay(self, value, attempt):
        delay = 2 ** attempt
        if value:
            try:
                delay = max(delay, float(value))
            except ValueError:
                try:
                    delay = max(delay, (parsedate_to_datetime(value) - utc(self.clock())).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    pass
        if not math.isfinite(delay) or delay > 120:
            raise SourceError('server backoff exceeds bounded run; retry later')
        return max(0, delay)


class SecIngestor:
    """Durable immutable metadata plus optional explicit event_core projection."""
    def __init__(self, path, client, issuers, *, clock=None):
        if not isinstance(issuers, (list, tuple)) or not 1 <= len(issuers) <= 25:
            raise ValueError('explicit list of 1–25 CIKs required')
        self.issuers = tuple(dict.fromkeys(cik_number(c) for c in issuers))
        self.client = client
        self.store = Store(path, clock=clock)
        with self.store.db() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS sec_snapshots (
              snapshot_id TEXT PRIMARY KEY, cik TEXT, fetched_at TEXT,
              observed_at TEXT, source_url TEXT, adapter_version TEXT,
              payload TEXT, has_older_files INTEGER);
            CREATE TABLE IF NOT EXISTS sec_records (
              version_id TEXT PRIMARY KEY, cik TEXT, accession TEXT,
              content_hash TEXT, first_seen_at TEXT, accepted_at TEXT,
              document_url TEXT, amendment INTEGER, payload TEXT);
            CREATE TABLE IF NOT EXISTS sec_snapshot_records (
              snapshot_id TEXT, version_id TEXT, PRIMARY KEY(snapshot_id,version_id));
            CREATE TABLE IF NOT EXISTS sec_event_map (
              version_id TEXT, symbol TEXT, event_version TEXT,
              PRIMARY KEY(version_id,symbol));
            ''')

    def poll_once(self):
        results = []
        for cik in self.issuers:
            try:
                snapshot = self.client.fetch(cik)
                result = self.record(snapshot)
                self.store.heartbeat('sec:' + cik, 'ok')
                results.append(result)
            except (SourceError, ValueError) as exc:
                self.store.heartbeat('sec:' + cik, 'error')
                results.append({'cik': cik, 'status': 'error', 'error': str(exc)})
        return results

    def record(self, snapshot):
        if snapshot.cik not in self.issuers:
            raise ValueError('issuer is not opted in')
        rows, older = parse_submissions(snapshot.payload, snapshot.cik)
        fetched = iso(snapshot.fetched_at)
        observed = iso(self.store.clock())
        if utc(fetched) > utc(observed):
            raise SourceError('future snapshot retrieval timestamp')
        sid = digest({'cik': snapshot.cik, 'payload': snapshot.payload, 'fetched_at': fetched})
        versions, added = [], 0
        with self.store.db() as db:
            db.execute('INSERT OR IGNORE INTO sec_snapshots VALUES (?,?,?,?,?,?,?,?)',
                       (sid, snapshot.cik, fetched, observed, source_url(snapshot.cik),
                        ADAPTER_VERSION, canonical(snapshot.payload), int(older)))
            for row in rows:
                content_hash = digest(row['raw'])
                vid = digest({'cik': row['cik'], 'accession': row['accession'], 'content_hash': content_hash})
                added += db.execute('INSERT OR IGNORE INTO sec_records VALUES (?,?,?,?,?,?,?,?,?)',
                    (vid, row['cik'], row['accession'], content_hash, observed,
                     row['accepted_at'], row['document_url'], int(row['amendment']), canonical(row['raw']))).rowcount
                db.execute('INSERT OR IGNORE INTO sec_snapshot_records VALUES (?,?)', (sid, vid))
                versions.append(vid)
        return {'cik': snapshot.cik, 'status': 'ok', 'snapshot_id': sid,
                'versions': versions, 'new_versions': added, 'cache_hit': snapshot.cache_hit,
                'coverage': 'recent_only', 'older_files_available': older}

    def project(self, version_id, symbol):
        """Explicit caller-verified symbol mapping. Acceptance != dissemination.

        event_core publication slot is acceptance time, flagged in payload.
        Missing/naive times are retained but NOT projected. No metric extracted.
        """
        if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,19}', symbol):
            raise ValueError('explicit verified symbol required')
        with self.store.db() as db:
            row = db.execute('SELECT * FROM sec_records WHERE version_id=?', (version_id,)).fetchone()
        if row is None or row['cik'] not in self.issuers:
            raise ValueError('unknown or non-opted-in record')
        with self.store.db() as db:
            latest = db.execute(
                'SELECT version_id FROM sec_records WHERE cik=? AND accession=? ORDER BY rowid DESC LIMIT 1',
                (row['cik'], row['accession'])).fetchone()
            latest_observation = db.execute(
                'SELECT MAX(first_seen_at) FROM sec_records WHERE cik=? AND accession=?',
                (row['cik'], row['accession'])).fetchone()[0]
        if row['first_seen_at'] < latest_observation:
            raise SourceError('local observation clock regressed; projection refused')
        if latest['version_id'] != version_id:
            raise SourceError('stale source revision cannot be projected')
        if not row['accepted_at']:
            raise SourceError('precise offset-aware acceptance timestamp unavailable')
        if utc(row['accepted_at']) > utc(row['first_seen_at']):
            raise SourceError('acceptance timestamp is in the future')
        payload = {'sec_metadata': json.loads(row['payload']), 'cik': row['cik'],
                   'source_version': version_id, 'adapter_version': ADAPTER_VERSION,
                   'timestamp_semantics': 'sec_acceptance_not_verified_public_dissemination',
                   'updated_at_semantics': 'local_metadata_observation_not_sec_correction_time',
                   'source_url': source_url(row['cik']), 'amendment': bool(row['amendment']),
                   'amends_accession': None, 'research_only': True}
        event, created = self.store.ingest(source='sec_submissions',
            source_id=row['cik'] + ':' + row['accession'] + ':' + symbol, symbol=symbol,
            published_at=row['accepted_at'], updated_at=row['first_seen_at'], payload=payload,
            url=row['document_url'], verified_primary=True)
        with self.store.db() as db:
            db.execute('INSERT OR IGNORE INTO sec_event_map VALUES (?,?,?)', (version_id, symbol, event))
        return event, created
