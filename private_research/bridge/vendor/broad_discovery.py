"""Offline, all-symbol discovery ledger. No HTTP, credentials, scheduler or orders.

Consumes supplied Alpaca-shaped news pages; each accepted page and continuation
commit together. Request descriptors are plans, never executed by this module.
Coverage means provider pagination exhaustion, NOT complete market coverage.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit

VERSION = 'broad-discovery-offline-v1'
APPLICATION_ID = 0x42524456  # BRDV: wrong-file namespace marker, NOT authentication.
TABLES = {'discovery_meta','universe','windows','pages','articles','mentions','page_articles','gaps'}
ENDPOINT = 'https://data.alpaca.markets/v1beta1/news'
SYMBOL = re.compile(r'[A-Z][A-Z0-9.-]{0,11}')
STAMP = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(?P<f>\d{1,9}))?(?:Z|[+-]\d{2}:\d{2})')
MODES = ('fixture', 'imported_capture')


class Refused(ValueError):
    pass


def require(ok, code):
    if not ok:
        raise Refused(code)


def timestamp(value):
    require(type(value) is str and STAMP.fullmatch(value), 'timestamp_required')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(result.tzinfo is not None and result.utcoffset() is not None, 'timezone_required')
        fraction = (STAMP.fullmatch(value).group('f') or '').ljust(9, '0')
        # Never round future sub-microsecond information backwards.
        return result.astimezone(timezone.utc) + timedelta(microseconds=bool(int(fraction[6:])))
    except (ValueError, OverflowError):
        raise Refused('invalid_timestamp') from None


def iso(value):
    return timestamp(value).isoformat(timespec='microseconds')


def encode(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError):
        raise Refused('invalid_json_value') from None


def digest(value):
    return sha256(encode(value).encode()).hexdigest()


def _pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'duplicate_json_key')
        result[key] = value
    return result


def _text(value, maximum, code, empty=False):
    require(type(value) is str and (empty or bool(value)) and len(value) <= maximum
            and '\x00' not in value, code)
    return value


class Ledger:
    """A separate research DB only. The constructor never opens a broker ledger.

    `clock` is trusted caller/test code, sampled for local ingestion timestamps.
    Imported capture receipts are attestations, not authenticated origin timing.
    `max_database_bytes` limits SQLite main pages, not total process/disk usage.
    """
    def __init__(self, path, *, seed_start, clock=None, overlap_seconds=300,
                 max_database_bytes=32 * 1024 * 1024, create_new=False):
        self.path = Path(path)
        require(str(path) != ':memory:' and not self.path.is_symlink(), 'separate_regular_file_required')
        require(type(overlap_seconds) is int and 0 <= overlap_seconds <= 86400, 'overlap_bound')
        require(type(max_database_bytes) is int and max_database_bytes >= 1024 * 1024, 'database_budget')
        self.clock = clock or (lambda: datetime.now(timezone.utc).isoformat())
        self.seed, self.overlap = iso(seed_start), overlap_seconds
        self.max_database_bytes = max_database_bytes
        require(type(create_new) is bool, 'explicit_create_flag')
        if create_new:
            require(not self.path.exists(), 'new_ledger_already_exists')
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
        else:
            require(self.path.is_file(), 'existing_ledger_required')
        stat = self.path.stat()
        self.identity = (stat.st_dev, stat.st_ino)
        if not create_new:
            # A read-only SQLite query cannot recover a hot rollback journal.
            # Inspect a bounded raw header before letting SQLite recover our
            # namespace, then validate tables/config below. This is accidental
            # wrong-file discrimination, not hostile-file authentication.
            with self.path.open('rb') as source:
                header = source.read(100)
            require(len(header) == 100 and header[:16] == b'SQLite format 3\x00'
                    and int.from_bytes(header[68:72], 'big') == APPLICATION_ID, 'unrelated_database_marker')
        with self.db() as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            require((create_new and not tables) or tables == TABLES, 'unrelated_database')
            if not tables:
                c.execute('PRAGMA application_id=' + str(APPLICATION_ID))
                schema = '''
                CREATE TABLE discovery_meta(id INTEGER PRIMARY KEY CHECK(id=1), version TEXT NOT NULL, seed TEXT NOT NULL, overlap INTEGER NOT NULL, budget INTEGER NOT NULL, covered_end TEXT, high_clock TEXT NOT NULL, mode TEXT);
                CREATE TABLE universe(version TEXT PRIMARY KEY, observed_at TEXT NOT NULL, evidence_ref TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE windows(id TEXT PRIMARY KEY, start TEXT NOT NULL, end TEXT NOT NULL, mode TEXT NOT NULL, universe_version TEXT, generation INTEGER NOT NULL, page INTEGER NOT NULL, token TEXT, status TEXT NOT NULL);
                CREATE UNIQUE INDEX one_open_window ON windows(status) WHERE status='pending';
                CREATE TABLE pages(request_id TEXT PRIMARY KEY, window_id TEXT NOT NULL, generation INTEGER NOT NULL, page INTEGER NOT NULL, body_sha256 TEXT NOT NULL, evidence_ref TEXT NOT NULL, imported_at TEXT NOT NULL, receipt_at TEXT NOT NULL, raw BLOB NOT NULL, result TEXT NOT NULL);
                CREATE TABLE articles(version TEXT PRIMARY KEY, article_id TEXT NOT NULL, updated_at TEXT NOT NULL, created_at TEXT NOT NULL, first_imported_at TEXT NOT NULL, first_receipt_at TEXT NOT NULL, evidence_mode TEXT NOT NULL, universe_version TEXT, payload TEXT NOT NULL);
                CREATE INDEX article_identity ON articles(article_id,updated_at);
                CREATE TABLE mentions(version TEXT NOT NULL, symbol TEXT NOT NULL, membership TEXT NOT NULL, PRIMARY KEY(version,symbol));
                CREATE TABLE page_articles(request_id TEXT NOT NULL, version TEXT NOT NULL, PRIMARY KEY(request_id,version));
                CREATE TABLE gaps(id INTEGER PRIMARY KEY, window_id TEXT NOT NULL, reason TEXT NOT NULL, opened_at TEXT NOT NULL, resolved_at TEXT);
                '''
                for statement in schema.split(';'):
                    if statement.strip():
                        c.execute(statement)
                now = self._now()
                require(self.seed <= now, 'future_seed')
                c.execute('INSERT INTO discovery_meta VALUES(1,?,?,?,?,NULL,?,NULL)',
                          (VERSION, self.seed, self.overlap, max_database_bytes, now))
            row = c.execute('SELECT * FROM discovery_meta WHERE id=1').fetchone()
            require(row is not None and (row['version'], row['seed'], row['overlap'], row['budget']) ==
                    (VERSION, self.seed, self.overlap, max_database_bytes), 'ledger_configuration_changed')

    @contextmanager
    def db(self):
        self._check_identity()
        c = sqlite3.connect(self.path.absolute().as_uri() + '?mode=rw', uri=True, timeout=10)
        c.row_factory = sqlite3.Row
        try:
            c.execute('PRAGMA synchronous=FULL')
            require(c.execute('PRAGMA journal_mode').fetchone()[0] == 'delete', 'unexpected_journal_mode')
            page_size = c.execute('PRAGMA page_size').fetchone()[0]
            c.execute('PRAGMA max_page_count=' + str(self.max_database_bytes // page_size))
            c.execute('BEGIN IMMEDIATE')
            yield c
            self._check_identity()
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    def _check_identity(self):
        try:
            stat = self.path.stat()
            require(not self.path.is_symlink() and (stat.st_dev, stat.st_ino) == self.identity, 'ledger_replaced')
        except OSError:
            raise Refused('existing_ledger_required') from None

    def _now(self):
        return iso(self.clock())

    def _advance_clock(self, c):
        now = self._now()
        high = c.execute('SELECT high_clock FROM discovery_meta WHERE id=1').fetchone()[0]
        require(now >= high, 'clock_rollback')
        c.execute('UPDATE discovery_meta SET high_clock=? WHERE id=1', (now,))
        return now

    def install_universe(self, assets, *, evidence_ref):
        """Version a supplied asset master; no ticker/sector count restriction.

        All valid rows retained, including inactive/non-tradable/unknown-sector.
        Membership does not permit trading; unknown news symbols are also kept.
        """
        _text(evidence_ref, 2048, 'evidence_reference')
        require(type(assets) is list and bool(assets), 'asset_master_required')
        snapshot = encode(assets)
        require(len(snapshot.encode()) <= 8 * 1024 * 1024, 'asset_input_byte_budget')
        assets = json.loads(snapshot)  # Own nested data; caller cannot mutate a committed version.
        normalized, seen_id, seen_symbol = [], set(), set()
        for item in assets:
            require(type(item) is dict, 'asset_shape')
            symbol, asset_id = item.get('symbol'), item.get('id')
            require(type(symbol) is str and SYMBOL.fullmatch(symbol), 'asset_symbol')
            _text(asset_id, 128, 'asset_id')
            require(asset_id not in seen_id and symbol not in seen_symbol, 'duplicate_asset')
            seen_id.add(asset_id); seen_symbol.add(symbol)
            require(type(item.get('tradable')) is bool, 'tradable_boolean')
            require(item.get('status') in ('active', 'inactive'), 'asset_status')
            _text(item.get('class'), 40, 'asset_class')
            sector = item.get('sector')
            if sector is not None:
                _text(sector, 100, 'sector_label')
                _text(item.get('sector_source'), 2048, 'sector_evidence_required')
            normalized.append(dict(item))
        normalized.sort(key=lambda a: (a['class'], a['symbol'], a['id']))
        with self.db() as c:
            now = self._advance_clock(c)
            version = digest([normalized, evidence_ref, now])
            c.execute('INSERT OR IGNORE INTO universe VALUES(?,?,?,?)',
                      (version, now, evidence_ref, encode(normalized)))
        return version

    def begin_window(self, *, end, mode='imported_capture', universe_version=None):
        """Pin interval end once. Resume pending work instead of jumping to now."""
        require(mode in MODES, 'offline_evidence_mode')
        end = iso(end)
        with self.db() as c:
            now = self._advance_clock(c)
            bound_mode = c.execute('SELECT mode FROM discovery_meta WHERE id=1').fetchone()[0]
            require(bound_mode is None or bound_mode == mode, 'ledger_evidence_mode_changed')
            if bound_mode is None:
                c.execute('UPDATE discovery_meta SET mode=? WHERE id=1', (mode,))
            pending = c.execute("SELECT * FROM windows WHERE status='pending'").fetchone()
            if pending:
                require(mode == pending['mode'] and universe_version == pending['universe_version'], 'pending_window_scope_changed')
                return self._request(pending)
            require(end <= now, 'future_window')
            if universe_version is not None:
                require(c.execute('SELECT 1 FROM universe WHERE version=?', (universe_version,)).fetchone(), 'unknown_universe')
            covered = c.execute('SELECT covered_end FROM discovery_meta WHERE id=1').fetchone()[0]
            start = max(self.seed, (timestamp(covered) - timedelta(seconds=self.overlap)).isoformat(timespec='microseconds')) if covered else self.seed
            require(start < end and (covered is None or covered < end), 'nonadvancing_window')
            wid = digest([VERSION, start, end, mode, universe_version])
            c.execute('INSERT INTO windows VALUES(?,?,?,?,?,0,0,NULL,?)',
                      (wid, start, end, mode, universe_version, 'pending'))
            return self._request(c.execute('SELECT * FROM windows WHERE id=?', (wid,)).fetchone())

    def _request(self, row):
        params = dict(start=row['start'], end=row['end'], sort='asc', limit=50, include_content='false')
        if row['token'] is not None:
            params['page_token'] = row['token']
        # Intentionally no symbols parameter: do not impose a fixed watchlist.
        request = dict(window_id=row['id'], generation=row['generation'], page=row['page'],
                       endpoint=ENDPOINT, params=params, execution_permission='none')
        request['request_id'] = digest(request)
        return request

    def pending_request(self):
        with self.db() as c:
            row = c.execute("SELECT * FROM windows WHERE status='pending'").fetchone()
            return None if row is None else self._request(row)

    def accept_page(self, request, raw, *, receipt_at, evidence_ref):
        require(type(raw) is bytes and 0 < len(raw) <= 2 * 1024 * 1024, 'news_page_byte_budget')
        require(type(request) is dict, 'request_shape')
        request = json.loads(encode(request))  # One immutable-by-ownership descriptor throughout commit.
        _text(evidence_ref, 2048, 'evidence_reference')
        receipt_at = iso(receipt_at)
        try:
            body = json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                              parse_constant=lambda _: (_ for _ in ()).throw(Refused('nonfinite_json')))
        except (UnicodeError, json.JSONDecodeError):
            raise Refused('invalid_news_json') from None
        require(type(body) is dict and type(body.get('news')) is list and len(body['news']) <= 50
                and 'next_page_token' in body, 'news_page_shape')
        token = body.get('next_page_token')
        if token is not None:
            _text(token, 8192, 'page_token')
        articles = []
        for item in body['news']:
            require(type(item) is dict and type(item.get('id')) is int and item['id'] >= 0, 'article_identity')
            created, updated = iso(item.get('created_at')), iso(item.get('updated_at'))
            require(created <= updated <= receipt_at, 'article_time_order')
            _text(item.get('headline'), 10000, 'headline', empty=True)
            url = item.get('url')
            if url is not None:
                _text(url, 8192, 'article_url', empty=True)
                if url:
                    parsed = urlsplit(url)
                    require(parsed.scheme in ('https','http') and bool(parsed.hostname)
                            and parsed.username is None and parsed.password is None, 'unsafe_article_url')
            symbols = item.get('symbols')
            require(type(symbols) is list, 'article_symbols')
            for symbol in symbols:
                _text(symbol, 128, 'article_symbol_tag')
            require(len(encode(item).encode()) <= 256 * 1024, 'article_byte_budget')
            articles.append((item, created, updated))
        raw_hash = sha256(raw).hexdigest()
        with self.db() as c:
            now = self._advance_clock(c)
            require(receipt_at <= now, 'future_receipt')
            prior = c.execute('SELECT * FROM pages WHERE request_id=?', (request.get('request_id'),)).fetchone()
            if prior:
                require(prior['body_sha256'] == raw_hash and prior['receipt_at'] == receipt_at
                        and prior['evidence_ref'] == evidence_ref, 'conflicting_page_replay')
                # Still validate descriptor; request_id alone is not authority.
                require(request.get('request_id') == digest({k:v for k,v in request.items() if k != 'request_id'}), 'request_digest')
                return json.loads(prior['result'])
            row = c.execute("SELECT * FROM windows WHERE status='pending'").fetchone()
            require(row is not None and request == self._request(row), 'stale_or_changed_request')
            require(receipt_at >= row['end'], 'receipt_before_window_end')
            require(token is None or token != row['token'], 'pagination_token_stalled')
            if token is not None:
                used = c.execute('SELECT result FROM pages WHERE window_id=? AND generation=?', (row['id'], row['generation']))
                require(all(json.loads(p[0]).get('next_page_token') != token for p in used), 'pagination_token_cycle')
            universe = {}
            if row['universe_version']:
                universe = {a['symbol']: a for a in json.loads(c.execute('SELECT payload FROM universe WHERE version=?', (row['universe_version'],)).fetchone()[0])}
            inserted = 0
            versions = []
            for item, created, updated in articles:
                vid = digest([row['mode'], 'alpaca-news', item])
                versions.append(vid)
                present = c.execute('SELECT 1 FROM articles WHERE version=?', (vid,)).fetchone()
                if not present:
                    c.execute('INSERT INTO articles VALUES(?,?,?,?,?,?,?,?,?)',
                              (vid, str(item['id']), updated, created, now, receipt_at, row['mode'], row['universe_version'], encode(item)))
                    inserted += 1
                    for symbol in dict.fromkeys(item['symbols']):
                        asset = universe.get(symbol)
                        membership = ('unresolved_provider_tag' if not SYMBOL.fullmatch(symbol) else
                                      'unknown' if asset is None else
                                      'active_us_equity' if asset['class']=='us_equity' and asset['status']=='active' else
                                      'outside_active_us_equity')
                        c.execute('INSERT INTO mentions VALUES(?,?,?)', (vid, symbol, membership))
            exhausted = token is None
            result = dict(new_versions=inserted, article_versions=versions, next_page_token=token,
                          provider_window_exhausted=exhausted, market_coverage_complete=False,
                          order_permission='none', evidence_mode=row['mode'])
            c.execute('INSERT INTO pages VALUES(?,?,?,?,?,?,?,?,?,?)',
                      (request['request_id'], row['id'], row['generation'], row['page'], raw_hash,
                       evidence_ref, now, receipt_at, raw, encode(result)))
            for vid in set(versions):
                c.execute('INSERT INTO page_articles VALUES(?,?)', (request['request_id'], vid))
            c.execute('UPDATE windows SET page=page+1,token=?,status=? WHERE id=?',
                      (token, 'exhausted' if exhausted else 'pending', row['id']))
            if exhausted:
                c.execute('UPDATE discovery_meta SET covered_end=? WHERE id=1', (row['end'],))
                c.execute('UPDATE gaps SET resolved_at=? WHERE window_id=? AND resolved_at IS NULL', (now, row['id']))
            return result

    def restart_pending_window(self, reason):
        """Explicit recovery for an expired cursor: replay SAME bounds from page 0.

        Does not skip an incomplete interval or erase recorded article versions.
        This only resets the local plan; it grants no provider retry permission.
        """
        require(reason in ('expired_cursor', 'disconnect', 'provider_replay_required'), 'recovery_reason')
        with self.db() as c:
            now = self._advance_clock(c)
            row = c.execute("SELECT * FROM windows WHERE status='pending'").fetchone()
            require(row is not None, 'no_pending_window')
            c.execute('INSERT INTO gaps(window_id,reason,opened_at) VALUES(?,?,?)', (row['id'], reason, now))
            c.execute('UPDATE windows SET generation=generation+1,page=0,token=NULL WHERE id=?', (row['id'],))
            return self._request(c.execute('SELECT * FROM windows WHERE id=?', (row['id'],)).fetchone())

    def status(self):
        with self.db() as c:
            meta = dict(c.execute('SELECT * FROM discovery_meta').fetchone())
            pending = c.execute("SELECT * FROM windows WHERE status='pending'").fetchone()
            pending_view = None if pending is None else {k:pending[k] for k in pending.keys() if k != 'token'}
            if pending_view is not None:
                pending_view['continuation_pending'] = pending['token'] is not None
            return dict(version=VERSION, initial_seed=meta['seed'], exhausted_through=meta['covered_end'],
                        pending=pending_view,
                        open_recovery_gaps=c.execute('SELECT COUNT(*) FROM gaps WHERE resolved_at IS NULL').fetchone()[0],
                        evidence_mode=meta['mode'],
                        article_versions=c.execute('SELECT COUNT(*) FROM articles').fetchone()[0],
                        mentions=c.execute('SELECT COUNT(*) FROM mentions').fetchone()[0],
                        active_collection=False, provider_entitlement='unknown',
                        market_coverage_complete=False, order_permission='none')

    def event_core_rows(self, *, include_fixtures=False):
        """Projection dictionaries for existing Store.ingest, never automatic writes.

        Untagged articles remain in this ledger, not fabricated into a stock.
        Source content remains unverified and cannot authorize execution.
        """
        require(type(include_fixtures) is bool, 'fixture_flag')
        with self.db() as c:
            rows = c.execute('SELECT a.*,m.symbol,m.membership FROM articles a JOIN mentions m ON a.version=m.version ORDER BY a.first_imported_at,a.version,m.symbol').fetchall()
        projected = []
        for row in rows:
            if row['evidence_mode'] == 'fixture' and not include_fixtures:
                continue
            if not SYMBOL.fullmatch(row['symbol']):
                continue  # Opaque provider tags remain retained, not invented equity tickers.
            item = json.loads(row['payload'])
            item['_discovery'] = dict(version=row['version'], evidence_mode=row['evidence_mode'],
                first_imported_at=row['first_imported_at'], receipt_at=row['first_receipt_at'],
                universe_version=row['universe_version'], membership=row['membership'],
                revision_status='provider_version_unverified', source_url_missing=not bool(item.get('url')),
                order_permission='none', original_verified=False)
            if row['evidence_mode'] == 'fixture':
                item['synthetic'] = True
            projected.append(dict(source='alpaca-news' if row['evidence_mode'] != 'fixture' else 'synthetic-broad-news',
                source_id=row['article_id']+':'+row['symbol'], symbol=row['symbol'],
                published_at=row['created_at'], updated_at=row['updated_at'],
                payload=item, url=item.get('url') or 'https://data.alpaca.markets/v1beta1/news', verified_primary=False))
        return projected

    def revision_states(self):
        """Mutable current view, separate from immutable projected event payloads."""
        with self.db() as c:
            rows = c.execute('SELECT version,article_id,evidence_mode,updated_at FROM articles').fetchall()
        counts, latest = {}, {}
        for row in rows:
            key = (row['article_id'], row['evidence_mode'])
            counts[key + (row['updated_at'],)] = counts.get(key + (row['updated_at'],), 0) + 1
            latest[key] = max(latest.get(key, row['updated_at']), row['updated_at'])
        return {row['version']: ('conflicting_same_timestamp'
                    if counts[(row['article_id'],row['evidence_mode'],row['updated_at'])] > 1
                    else 'superseded' if row['updated_at'] < latest[(row['article_id'],row['evidence_mode'])]
                    else 'provider_version_unverified') for row in rows}

    def unresolved_mentions(self):
        """Opaque tags and canonical but unknown tickers for later evidence mapping."""
        with self.db() as c:
            return [dict(r) for r in c.execute("SELECT * FROM mentions WHERE membership IN ('unknown','unresolved_provider_tag') ORDER BY version,symbol")]
