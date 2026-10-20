"""One issuer, original-source evidence only. Explicit collection; no startup/orders.

Trust boundary: the application owns transport, clock, database and this parser.
No key is created, no model output is used, and historical availability is never
inferred from publisher dates. See PRIMARY_EARNINGS_SOURCE.md before integration.
"""
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime, format_datetime
from html.parser import HTMLParser
from http.client import HTTPException
import hashlib
import json
import re
import sqlite3
import time
from urllib import error, request
import xml.etree.ElementTree as ET

from active_demo_pipeline import Event

ORIGIN = 'https://nvidianews.nvidia.com'
FEED_URL = ORIGIN + '/rss.xml'
PARSER = 'nvidia-original-earnings-v1'
ISSUER = 'nvidia-cik-1045810'
MAX_BYTES = 1048576
MAX_ITEMS = 100
MAX_RELEASES = 4
NS = 1_000_000_000
QUARTERS = {'first': 1, 'second': 2, 'third': 3, 'fourth': 4}
_ROUTE = re.compile(re.escape(ORIGIN) + r'/news/nvidia-announces-financial-results-for-(first|second|third|fourth)-quarter(-and)?-(?:fiscal-)?(20\d{2})')
_TITLE = re.compile(r'NVIDIA Announces Financial Results for (First|Second|Third|Fourth) Quarter(?: and)?(?: Fiscal)? (20\d{2})')
_RSS_DATE = re.compile(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) 20\d{2} \d{2}:\d{2}:\d{2} GMT')


class EvidenceRefused(ValueError):
    """Fixed local reason only, never arbitrary remote content."""


def require(ok, reason):
    if not ok:
        raise EvidenceRefused(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def stamp_ns(value):
    require(type(value) is datetime and value.utcoffset() is not None, 'invalid_local_clock')
    delta = value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * NS + delta.microseconds * 1000


def utc(ns):
    # Round UP, never make complete reception appear earlier than it was.
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=(ns + 999) // 1000)


def event_payload(event):
    data = asdict(event)
    for key in ('published_at', 'observed_at'):
        data[key] = data[key].isoformat()
    return canonical(data).decode()


def decode_event(payload):
    data = json.loads(payload)
    for key in ('published_at', 'observed_at'):
        data[key] = datetime.fromisoformat(data[key])
    return Event(**data)


def release_url(url):
    require(type(url) is str and _ROUTE.fullmatch(url) is not None, 'forbidden_source_url')
    return url


@dataclass(frozen=True)
class HTTPDocument:
    url: str
    body: bytes
    content_type: str


@dataclass(frozen=True)
class CollectionResult:
    reason: str
    event_count: int = 0
    request_count: int = 0


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise EvidenceRefused('redirect_refused')


class NvidiaReadOnlyHTTP:
    """Pinned HTTPS GET only; no auth, redirects, retries or paid APIs."""
    def __init__(self, *, enabled=False):
        require(type(enabled) is bool, 'invalid_enable')
        self.enabled = enabled
        self._opener = request.build_opener(_NoRedirect())

    def get(self, url):
        require(self.enabled is True, 'transport_disabled')
        if url != FEED_URL:
            release_url(url)
        try:
            req = request.Request(url, method='GET', headers={
                'User-Agent': 'PaperEventEvidence/1.0 (read-only original-source research)',
                'Accept': 'application/rss+xml, application/xml, text/xml' if url == FEED_URL else 'text/html',
                'Accept-Encoding': 'identity'})
            with self._opener.open(req, timeout=4) as response:
                require(response.status == 200, 'http_status_refused')
                require(response.geturl() == url, 'redirect_refused')
                require(response.headers.get('Content-Encoding', 'identity') in ('identity', ''), 'encoding_refused')
                length = response.headers.get('Content-Length')
                if length is not None:
                    require(bool(re.fullmatch(r'\d{1,8}', length)) and 0 < int(length) <= MAX_BYTES, 'source_size_refused')
                body = response.read(MAX_BYTES + 1)
                require(0 < len(body) <= MAX_BYTES, 'source_size_refused')
                require(length is None or int(length) == len(body), 'partial_source')
                return HTTPDocument(url, body, response.headers.get('Content-Type', ''))
        except EvidenceRefused:
            raise
        except (error.URLError, OSError, ValueError, HTTPException):
            raise EvidenceRefused('source_read_failed') from None


def rss_time(text):
    require(type(text) is str and _RSS_DATE.fullmatch(text) is not None, 'publisher_timestamp_precision')
    value = parsedate_to_datetime(text)
    require(format_datetime(value, usegmt=True) == text, 'publisher_timestamp_invalid')
    return value


def one(node, tag):
    items = node.findall(tag)
    require(len(items) == 1 and len(items[0]) == 0 and items[0].text is not None, 'ambiguous_feed_field')
    return items[0].text.strip()


def parse_feed(raw, on_item=None):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, 'source_size_refused')
    try:
        text = raw.decode('utf-8', errors='strict')
    except UnicodeError:
        raise EvidenceRefused('invalid_feed_encoding') from None
    require('\x00' not in text and '<!DOCTYPE' not in text.upper() and '<!ENTITY' not in text.upper(), 'unsafe_xml')
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        raise EvidenceRefused('invalid_feed') from None
    require(root.tag == 'rss' and len(root.findall('channel')) == 1, 'invalid_feed')
    channel = root.find('channel')
    require(one(channel, 'title') == 'NVIDIA Newsroom' and one(channel, 'link') == ORIGIN, 'feed_identity_mismatch')
    items = channel.findall('item')
    require(len(items) <= MAX_ITEMS, 'feed_item_overflow')
    result, seen = [], set()
    for item in items:
        title = one(item, 'title')
        url = one(item, 'link')
        relevant = 'financial results' in title.lower() or 'nvidia-announces-financial-results' in url
        if not relevant:
            continue
        release_url(url)
        match = _TITLE.fullmatch(title)
        require(match is not None, 'unrecognized_earnings_item')
        route = _ROUTE.fullmatch(url)
        quarter, year = QUARTERS[match[1].lower()], int(match[2])
        require(QUARTERS[route[1]] == quarter and int(route[3]) == year, 'feed_identity_mismatch')
        require(url not in seen, 'duplicate_earnings_item')
        seen.add(url)
        require(one(item, 'guid') == url and one(item, 'contentType') == 'releases', 'feed_identity_mismatch')
        require([x.text for x in item.findall('categories/category')] == ['Press Releases'], 'feed_identity_mismatch')
        published, modified = rss_time(one(item, 'pubDate')), rss_time(one(item, 'modDate'))
        require(modified >= published, 'publisher_timestamp_invalid')
        extracted = dict(url=url, title=title, fiscal_year=year, quarter=quarter,
                         published_at=published.isoformat(), modified_at=modified.isoformat(),
                         corrected=modified > published)
        if on_item is not None:
            on_item(extracted)  # Monotone revocations survive a later batch failure.
        result.append(extracted)
    require(len(result) <= MAX_RELEASES, 'release_batch_overflow')
    return tuple(result)


class _Article(HTMLParser):
    VOID = frozenset(('area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'))
    INERT = frozenset(('script', 'style', 'template'))

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.canonical, self.titles, self.bodies = [], [], []
        self.capture, self.suppressed = None, []

    def handle_starttag(self, tag, attrs):
        if self.suppressed:
            if tag in self.INERT:
                self.suppressed.append(tag)
            return
        if tag in self.INERT:
            self.suppressed.append(tag)
            return
        keys = [x[0] for x in attrs]
        require(len(keys) == len(set(keys)), 'ambiguous_html_attribute')
        values = dict(attrs)
        if tag == 'link' and values.get('rel') == 'canonical':
            self.canonical.append(values.get('href'))
        classes = (values.get('class') or '').split()
        kind = 'title' if tag == 'h1' and 'article-title' in classes else ('body' if tag == 'div' and 'article-body' in classes else None)
        if kind:
            require(self.capture is None, 'ambiguous_article_structure')
            self.capture = [kind, [tag], []]
        elif self.capture and tag not in self.VOID:
            self.capture[1].append(tag)

    def handle_endtag(self, tag):
        if self.suppressed:
            if tag == self.suppressed[-1]:
                self.suppressed.pop()
            return
        if tag in self.VOID:
            return
        if self.capture:
            require(self.capture[1] and self.capture[1][-1] == tag, 'ambiguous_article_structure')
            self.capture[1].pop()
            if not self.capture[1]:
                kind, _, texts = self.capture
                (self.titles if kind == 'title' else self.bodies).append(' '.join(' '.join(texts).split()))
                self.capture = None

    def handle_data(self, text):
        if self.capture and not self.suppressed:
            self.capture[2].append(text)


def parse_article(raw, item):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, 'source_size_refused')
    parser = _Article()
    try:
        parser.feed(raw.decode('utf-8', errors='strict'))
        parser.close()
    except UnicodeError:
        raise EvidenceRefused('invalid_html_encoding') from None
    require(parser.capture is None and not parser.suppressed and parser.canonical == [item['url']] and parser.titles == [item['title']]
            and len(parser.bodies) == 1, 'article_identity_mismatch')
    body = parser.bodies[0]
    pattern = (r'NVIDIA \(NASDAQ: NVDA\) today reported (?:record )?revenue for (?:the )?'
               r'(first|second|third|fourth) quarter ended ([A-Z][a-z]+ \d{1,2}, 20\d{2})')
    matches = re.findall(pattern, body)
    require(len(matches) == 1 and QUARTERS[matches[0][0]] == item['quarter'], 'reporting_period_ambiguous')
    try:
        end = datetime.strptime(matches[0][1], '%B %d, %Y').date()
    except ValueError:
        raise EvidenceRefused('reporting_period_ambiguous') from None
    pub = datetime.fromisoformat(item['published_at']).date()
    require(end < pub and (pub - end).days <= 100 and end.year in (item['fiscal_year'] - 1, item['fiscal_year']), 'reporting_period_invalid')
    corrected = item['corrected'] or bool(re.search(r'\b(corrected|correction|restatement|revised results)\b', body, re.I))
    return dict(item, period_end=end.isoformat(), corrected=corrected,
                episode_id=f'{ISSUER}:FY{item["fiscal_year"]}Q{item["quarter"]}')


class PrimaryEarningsSource:
    """Source contract for DemoTickRunner. Collection and decisions must serialize.

    Injected transports/clocks are trusted application dependencies, not untrusted
    input. Caller-provided dictionaries or booleans cannot become verified Events.
    """
    def __init__(self, database, *, transport, now_ns=time.time_ns):
        require(str(database) != ':memory:' and callable(now_ns), 'durable_source_required')
        self.database, self.transport, self.now_ns = str(database), transport, now_ns
        self._failed = False
        self._last_sample = 0
        with sqlite3.connect(self.database) as c:
            c.executescript('''
              CREATE TABLE IF NOT EXISTS primary_meta(id INTEGER PRIMARY KEY CHECK(id=1), binding TEXT NOT NULL, last_clock INTEGER, success_ns INTEGER, busy INTEGER NOT NULL);
              CREATE TABLE IF NOT EXISTS primary_snapshots(hash TEXT PRIMARY KEY, body BLOB NOT NULL);
              CREATE TABLE IF NOT EXISTS primary_receipts(id INTEGER PRIMARY KEY, source_url TEXT NOT NULL, snapshot_hash TEXT NOT NULL, received_ns INTEGER NOT NULL);
              CREATE TABLE IF NOT EXISTS primary_lineage(hash TEXT PRIMARY KEY, episode TEXT NOT NULL, parent_hash TEXT, payload TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS primary_episodes(episode TEXT PRIMARY KEY, latest_hash TEXT NOT NULL, status TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS primary_events(event_id TEXT PRIMARY KEY, episode TEXT NOT NULL, lineage_hash TEXT NOT NULL, payload TEXT NOT NULL);
            ''')
            c.execute('INSERT OR IGNORE INTO primary_meta VALUES(1,?,NULL,NULL,0)', (PARSER,))
            require(c.execute('SELECT binding FROM primary_meta').fetchall() == [(PARSER,)], 'source_binding_mismatch')

    def _now(self):
        ns = self.now_ns()
        require(type(ns) is int and 0 < ns <= 2**63 - 1, 'invalid_local_clock')
        if ns < self._last_sample:
            self._failed = True
            raise EvidenceRefused('local_clock_regression')
        self._last_sample = ns
        return ns

    def _get(self, url):
        document = self.transport.get(url)
        received = self._now()  # Complete response first; then local receipt. Never HTTP Date.
        require(type(document) is HTTPDocument and document.url == url, 'transport_document_mismatch')
        allowed = ('application/rss+xml', 'application/xml', 'text/xml') if url == FEED_URL else ('text/html',)
        require(document.content_type.split(';', 1)[0].strip().lower() in allowed, 'content_type_refused')
        require(type(document.body) is bytes and 0 < len(document.body) <= MAX_BYTES, 'source_size_refused')
        sha = digest(document.body)
        with sqlite3.connect(self.database) as c:
            previous = c.execute('SELECT last_clock FROM primary_meta WHERE id=1').fetchone()[0]
            require(previous is None or received >= previous, 'local_clock_regression')
            c.execute('UPDATE primary_meta SET last_clock=? WHERE id=1', (received,))
            c.execute('INSERT OR IGNORE INTO primary_snapshots VALUES(?,?)', (sha, document.body))
            require(c.execute('SELECT body FROM primary_snapshots WHERE hash=?', (sha,)).fetchone()[0] == document.body, 'snapshot_corrupt')
            row = c.execute('INSERT INTO primary_receipts(source_url,snapshot_hash,received_ns) VALUES(?,?,?)', (url, sha, received)).lastrowid
        return document.body, sha, received, row

    def _revoke(self, item, feed_hash, feed_receipt, received, *, raw_hash=None, receipt=None):
        """Recognized primary evidence may revoke before the full batch succeeds.

        This path can NEVER accept an event. A corrected first RSS item creates
        a permanent tombstone even if fetching its original HTML subsequently fails.
        """
        fiscal_id = f'{ISSUER}:FY{item["fiscal_year"]}Q{item["quarter"]}'
        episode = digest(canonical([ISSUER, fiscal_id]))
        with sqlite3.connect(self.database) as c:
            c.execute('BEGIN IMMEDIATE')
            prior = c.execute('SELECT latest_hash,status FROM primary_episodes WHERE episode=?', (episode,)).fetchone()
            old = json.loads(c.execute('SELECT payload FROM primary_lineage WHERE hash=?', (prior[0],)).fetchone()[0]) if prior else None
            changed = old is not None and (any(old['release'].get(key) != value for key, value in item.items())
                                          or (raw_hash is not None and old.get('original_hash') not in (None, raw_hash)))
            if not item['corrected'] and not changed:
                return
            payload = dict(parser=PARSER, release=item, identity=old['identity'] if old else '',
                           original_hash=raw_hash, feed_hash=feed_hash, original_receipt_id=receipt,
                           feed_receipt_id=feed_receipt, received_ns=received,
                           revocation='corrected_or_conflicting_primary_evidence')
            sha = digest(canonical(payload))
            c.execute('INSERT OR IGNORE INTO primary_lineage VALUES(?,?,?,?)', (sha, episode, prior[0] if prior else None, canonical(payload).decode()))
            c.execute("INSERT INTO primary_episodes VALUES(?,?,'quarantined') ON CONFLICT(episode) DO UPDATE SET latest_hash=excluded.latest_hash,status='quarantined'", (episode, sha))

    def collect(self):
        count = requests = 0
        acquired_busy = False
        try:
            require(not self._failed, 'source_latched')
            start = self._now()
            with sqlite3.connect(self.database, timeout=5) as c:
                c.execute('BEGIN IMMEDIATE')
                last, busy = c.execute('SELECT last_clock,busy FROM primary_meta WHERE id=1').fetchone()
                require(not busy, 'interrupted_or_concurrent_collection')
                require(last is None or start >= last, 'local_clock_regression')
                require(last is None or start - last >= 15 * NS, 'collection_rate_limited')
                c.execute('UPDATE primary_meta SET busy=1,success_ns=NULL,last_clock=? WHERE id=1', (start,))
                acquired_busy = True
            requests += 1
            feed, feed_hash, feed_ns, feed_receipt = self._get(FEED_URL)
            items = parse_feed(feed, on_item=lambda item: self._revoke(item, feed_hash, feed_receipt, feed_ns))
            # During its usable horizon, disappearance is not evidence of no
            # correction. Require every previously accepted active release to remain.
            with sqlite3.connect(self.database) as c:
                active = c.execute("SELECT e.payload FROM primary_events e JOIN primary_episodes p USING(episode) WHERE p.status='accepted'").fetchall()
            seen = {item['url'] for item in items}
            require(all(decode_event(row[0]).source_url in seen for row in active
                        if 0 <= start - stamp_ns(decode_event(row[0]).published_at) <= 420 * NS), 'active_release_missing')
            pending = []
            for item in items:
                requests += 1
                raw, raw_hash, received, receipt = self._get(item['url'])
                self._revoke(item, feed_hash, feed_receipt, received, raw_hash=raw_hash, receipt=receipt)
                release = parse_article(raw, item)
                self._revoke(release, feed_hash, feed_receipt, received, raw_hash=raw_hash, receipt=receipt)
                published = datetime.fromisoformat(release['published_at'])
                require(stamp_ns(published) <= feed_ns <= received, 'future_release')
                pending.append((release, raw_hash, received, receipt))
            # Final availability is sampled only after all original receipts have
            # been durably saved and all parsing has completed. Insertion and the
            # health marker then commit atomically; no partial batch is delivered.
            finish = self._now()
            require(finish >= feed_ns and finish - start <= 20 * NS, 'collection_deadline_or_clock')
            with sqlite3.connect(self.database) as c:
                c.execute('BEGIN IMMEDIATE')
                finish = self._now()  # Also account for waiting on the durable write lock.
                require(finish - start <= 20 * NS, 'collection_deadline_or_clock')
                last = c.execute('SELECT last_clock FROM primary_meta WHERE id=1').fetchone()[0]
                require(finish >= last, 'local_clock_regression')
                for release, raw_hash, received, receipt in pending:
                    published = datetime.fromisoformat(release['published_at'])
                    episode = digest(canonical([ISSUER, release['episode_id']]))
                    # Excludes unrelated whole-feed drift and receipt timestamps;
                    # binds exact selected item extraction and original HTML bytes.
                    identity = digest(canonical([PARSER, release, raw_hash]))
                    prior = c.execute('SELECT latest_hash,status FROM primary_episodes WHERE episode=?', (episode,)).fetchone()
                    payload = dict(parser=PARSER, release=release, original_hash=raw_hash, feed_hash=feed_hash,
                                   original_receipt_id=receipt, feed_receipt_id=feed_receipt,
                                   received_ns=received, available_ns=finish, identity=identity)
                    lineage_hash = digest(canonical(payload))
                    c.execute('INSERT INTO primary_lineage VALUES(?,?,?,?)', (lineage_hash, episode, prior[0] if prior else None, canonical(payload).decode()))
                    old_identity = None
                    if prior:
                        old_identity = json.loads(c.execute('SELECT payload FROM primary_lineage WHERE hash=?', (prior[0],)).fetchone()[0])['identity']
                    quarantine = release['corrected'] or (prior and (prior[1] == 'quarantined' or old_identity != identity))
                    late = finish - stamp_ns(published) > 120 * NS
                    status = 'quarantined' if quarantine else ('accepted' if prior and prior[1] == 'accepted' else ('late' if late else 'accepted'))
                    c.execute('INSERT INTO primary_episodes VALUES(?,?,?) ON CONFLICT(episode) DO UPDATE SET latest_hash=excluded.latest_hash,status=excluded.status', (episode, lineage_hash, status))
                    if not prior and status == 'accepted':
                        event = Event(event_id=digest(canonical([episode, identity])), episode_id=episode, version=identity,
                                      symbol='NVDA', published_at=published, observed_at=utc(finish), source_url=release['url'],
                                      source_hash=raw_hash, verified=True, corrected=False, timestamp_precision='second', event_type='earnings')
                        c.execute('INSERT INTO primary_events VALUES(?,?,?,?)', (event.event_id, episode, lineage_hash, event_payload(event)))
                        count += 1
                c.execute('UPDATE primary_meta SET busy=0,success_ns=?,last_clock=? WHERE id=1', (finish, finish))
            return CollectionResult('collected_primary_evidence' if items else 'no_earnings_in_feed', count, requests)
        except (EvidenceRefused, sqlite3.Error, TypeError, ValueError, AttributeError, KeyError, OverflowError):
            # Any incomplete batch disables all delivery, including prior events.
            # An interrupted collection cannot silently be reset by another instance.
            if acquired_busy:
                try:
                    with sqlite3.connect(self.database) as c:
                        c.execute('UPDATE primary_meta SET busy=0,success_ns=NULL WHERE id=1')
                except sqlite3.Error:
                    self._failed = True
            else:
                self._failed = True
            return CollectionResult('source_collection_refused', 0, requests)

    def _healthy(self, c, now):
        row = c.execute('SELECT success_ns,busy,last_clock FROM primary_meta WHERE id=1').fetchone()
        return not self._failed and row and not row[1] and row[0] is not None and row[2] <= now and 0 <= now - row[0] <= 30 * NS

    def events(self, now, limit):
        try:
            require(type(limit) is int and 1 <= limit <= 100, 'invalid_limit')
            at = stamp_ns(now)
            require(abs(self._now() - at) <= NS, 'invalid_local_clock')
            with sqlite3.connect(self.database) as c:
                if not self._healthy(c, at):
                    return ()
                rows = c.execute("SELECT e.payload FROM primary_events e JOIN primary_episodes p USING(episode) WHERE p.status='accepted' ORDER BY e.event_id").fetchall()
            return tuple(e for row in rows if 0 <= at - stamp_ns((e := decode_event(row[0])).published_at) <= 420 * NS and stamp_ns(e.observed_at) <= at)[:limit]
        except (EvidenceRefused, sqlite3.Error, ValueError, TypeError, AttributeError):
            return ()

    def event_eligible(self, event):
        try:
            require(type(event) is Event, 'invalid_event')
            at = self._now()
            with sqlite3.connect(self.database) as c:
                if not self._healthy(c, at) or not 0 <= at - stamp_ns(event.published_at) <= 420 * NS or stamp_ns(event.observed_at) > at:
                    return False
                row = c.execute("SELECT e.payload FROM primary_events e JOIN primary_episodes p USING(episode) WHERE e.event_id=? AND p.status='accepted'", (event.event_id,)).fetchone()
                return row is not None and row[0] == event_payload(event)
        except (EvidenceRefused, sqlite3.Error, ValueError, TypeError, AttributeError):
            return False
