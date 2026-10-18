"""Read-only bar observations. No executable quotes, causal claims, or orders."""
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import timedelta
from event_core import utc, iso, finite
from ai_research import NoRedirect


def summarize(event_at, observed_at, bars):
    event_at, observed_at = utc(event_at), utc(observed_at)
    if event_at > observed_at:
        raise ValueError('future event')
    clean = {}
    for bar in bars:
        start = utc(bar['t'])
        if start.second or start.microsecond:
            raise ValueError('unaligned minute bar')
        end = start + timedelta(minutes=1)
        close = finite(bar['c'])
        if close <= 0:
            raise ValueError('invalid close')
        if end <= observed_at:
            if end in clean and clean[end]['close'] != close:
                raise ValueError('conflicting duplicate bar')
            clean[end] = {'end': iso(end), 'close': close}
    ordered = [clean[t] for t in sorted(clean)]
    pre = [b for b in ordered if utc(b['end']) <= event_at]
    baseline = pre[-1] if pre else None
    if baseline and (event_at - utc(baseline['end'])).total_seconds() > 300:
        baseline = None
    result = {'status': 'no_baseline' if not baseline else 'waiting_post_event',
              'baseline': baseline, 'latest': None, 'horizons': [],
              'order_permission': 'none', 'event_verified': False}
    post = [b for b in ordered if utc(b['end']) > event_at]
    if baseline and post:
        latest = dict(post[-1])
        latest['change_pct'] = 100 * (latest['close'] / baseline['close'] - 1)
        latest['age_seconds'] = (observed_at - utc(latest['end'])).total_seconds()
        result.update(status='observed', latest=latest)
    for minutes in (5, 15, 60):
        target = event_at + timedelta(minutes=minutes)
        row = {'minutes': minutes, 'status': 'pending', 'change_pct': None}
        if target <= observed_at:
            matches = [b for b in post if utc(b['end']) <= target and
                       (target - utc(b['end'])).total_seconds() <= 120]
            row['status'] = 'missing_data'
            if baseline and matches:
                row.update(status='observed', change_pct=100 * (matches[-1]['close'] / baseline['close'] - 1),
                           bar_end=matches[-1]['end'])
        result['horizons'].append(row)
    return result


def fetch_bars(symbol, start, end, feed):
    if not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,11}', symbol) or feed not in ('iex', 'sip'):
        raise ValueError('invalid market data request')
    key, secret = os.environ.get('ALPACA_API_KEY'), os.environ.get('ALPACA_SECRET_KEY')
    if not key or not secret:
        raise ValueError('missing market data credentials')
    params = dict(start=iso(start), end=iso(end), timeframe='1Min', limit=1000,
                  adjustment='raw', feed=feed, sort='asc', asof='-')
    req = urllib.request.Request('https://data.alpaca.markets/v2/stocks/' + symbol + '/bars?' +
        urllib.parse.urlencode(params), headers={'APCA-API-KEY-ID':key, 'APCA-API-SECRET-KEY':secret})
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=10) as response:
        raw = response.read(1000001)
        if len(raw) > 1000000:
            raise ValueError('market response too large')
        data = json.loads(raw)
    if data.get('next_page_token'):
        raise ValueError('incomplete market data coverage')
    if not isinstance(data.get('bars'), (list, type(None))):
        raise ValueError('invalid bars')
    if data.get('symbol') not in (None, symbol):
        raise ValueError('wrong symbol')
    return data.get('bars') or []


class Reactions:
    def __init__(self, store):
        self.store = store
        with store.db() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS price_reactions(
                id INTEGER PRIMARY KEY, event_version TEXT, requested_at TEXT,
                received_at TEXT, feed TEXT, report TEXT, raw_bars TEXT)''')
            c.execute('CREATE INDEX IF NOT EXISTS reactions_event ON price_reactions(event_version,id)')

    def latest(self, version):
        with self.store.db() as c:
            row = c.execute('SELECT * FROM price_reactions WHERE event_version=? ORDER BY id DESC LIMIT 1',
                            (version,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result['report'] = json.loads(result['report'])
        result.pop('raw_bars', None)
        return result

    def measure(self, version, transport=None):
        now = utc(self.store.clock())
        previous = self.latest(version)
        # Prevent repeated clicks and background cycles from flooding the data API.
        if previous and (now - utc(previous['requested_at'])).total_seconds() < 300:
            return previous
        with self.store.db() as c:
            event = c.execute('SELECT * FROM events WHERE version_id=?', (version,)).fetchone()
        if not event:
            raise ValueError('unknown event')
        at = utc(event['published_at'])
        if max(utc(event[k]) for k in ('published_at','updated_at','received_at')) > now:
            raise ValueError('future event')
        feed = os.environ.get('PRICE_DATA_FEED','iex').lower()
        if feed not in ('iex', 'sip'):
            raise ValueError('invalid price feed')
        # Request only an intraday window and completed minutes with a safety lag.
        start = at - timedelta(minutes=20)
        end = min(now - timedelta(minutes=2), at + timedelta(minutes=60))
        bars = []
        report = {'status':'waiting_data','order_permission':'none'}
        if end > start:
            try:
                bars = (transport or fetch_bars)(event['symbol'], start, end, feed)
                if not isinstance(bars,list) or len(bars)>1000:
                    raise ValueError('invalid bar count')
                bars = [b for b in bars if start <= utc(b['t']) <= end]
                report = summarize(at, end, bars)
            except urllib.error.HTTPError as exc:
                report = {'status':'provider_http_'+str(int(exc.code)), 'order_permission':'none'}
                bars = []
            except Exception:
                report = {'status':'data_error','order_permission':'none'}
                bars = []
        if report.get('latest'):
            report['latest']['age_at_request_seconds']=(now-utc(report['latest']['end'])).total_seconds()
        report.update(event_at=iso(at), data_cutoff=iso(end), feed=feed,
                      consolidated=feed=='sip', adjustment='raw',
                      executable_quote=False, causal_effect_proven=False)
        with self.store.db() as c:
            c.execute('INSERT INTO price_reactions(event_version,requested_at,received_at,feed,report,raw_bars) VALUES (?,?,?,?,?,?)',
                (version,iso(now),iso(self.store.clock()),feed,json.dumps(report),json.dumps(bars)))
        return self.latest(version)

    def follow_up(self, transport=None):
        """One due event per worker cycle; a bounded 90-minute observation period."""
        cutoff = iso(utc(self.store.clock()) - timedelta(minutes=90))
        due = iso(utc(self.store.clock()) - timedelta(minutes=5))
        with self.store.db() as c:
            row = c.execute('''SELECT e.version_id FROM events e JOIN discovery_runs d
                ON d.event_version=e.version_id WHERE e.published_at>=? AND d.state='completed'
                AND NOT EXISTS(SELECT 1 FROM price_reactions p WHERE p.event_version=e.version_id AND p.requested_at>?)
                ORDER BY COALESCE((SELECT MAX(p.requested_at) FROM price_reactions p
                    WHERE p.event_version=e.version_id),'') ASC,e.published_at DESC LIMIT 1''', (cutoff,due)).fetchone()
        return self.measure(row['version_id'],transport) if row else None
