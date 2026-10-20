"""Disabled, GET-only real-schema bridge for shadow observations. No order methods.
No environment reads, retries, subscriptions, scheduling or production wiring.
Availability means actual receipt, never inferred from a historical bar's end.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib import request, error
from urllib.parse import urlencode, urlsplit, parse_qs
from zoneinfo import ZoneInfo
from collections import deque
import json
import re
import threading
import time
from paper_adapter import NoRedirect, SafetyError
from paper_quotes import _price, _unique_object, _invalid_constant, _TIMESTAMP

UNIVERSE = ('AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'MU', 'SPY')
DATA = 'https://data.alpaca.markets'
PAPER = 'https://paper-api.alpaca.markets'
LIMIT = 262144

def require(ok, reason):
    if not ok: raise SafetyError(reason)

def aware(value):
    require(isinstance(value, datetime) and value.utcoffset() is not None, 'invalid_time')
    return value.astimezone(timezone.utc)

def timestamp(value):
    require(isinstance(value, str) and _TIMESTAMP.fullmatch(value), 'invalid_provider_timestamp')
    try:
        result = aware(datetime.fromisoformat(value.replace('Z', '+00:00')))
        # Round sub-microsecond timestamps UP, preventing future data rounding back.
        fraction = (_TIMESTAMP.fullmatch(value).group('fraction') or '').ljust(9, '0')
        return result + timedelta(microseconds=bool(int(fraction[6:])))
    except (ValueError, OverflowError): raise SafetyError('invalid_provider_timestamp') from None

@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: str
    ask: str
    asof: datetime
    received_at: datetime

@dataclass(frozen=True)
class Bar:
    symbol: str
    start: datetime
    end: datetime
    close: str
    available_at: datetime

@dataclass(frozen=True)
class Session:
    open: datetime
    close: datetime
    received_at: datetime


def parse_quote(payload, symbol, *, received_at, now):
    received_at, now = aware(received_at), aware(now)
    require(symbol in UNIVERSE and isinstance(payload, dict) and payload.get('symbol') == symbol, 'quote_symbol')
    q = payload.get('quote')
    require(isinstance(q, dict), 'missing_quote')
    bid, ask, at = _price(q.get('bp')), _price(q.get('ap')), timestamp(q.get('t'))
    floor_at = aware(datetime.fromisoformat(q['t'].replace('Z', '+00:00')))
    require(at <= received_at <= now and now-floor_at <= timedelta(seconds=10), 'stale_or_future_quote')
    require(ask >= bid and (ask-bid)/((ask+bid)/2) <= Decimal('.002'), 'quote_spread')
    return Quote(symbol, str(bid), str(ask), at, received_at)


def parse_bars(payload, symbol, *, start, end, received_at, now):
    """Require every minute in [start,end); provider end parameter is inclusive."""
    start, end, received_at, now = map(aware, (start, end, received_at, now))
    require(symbol in UNIVERSE and isinstance(payload, dict) and payload.get('symbol') == symbol, 'bars_symbol')
    require(payload.get('next_page_token') is None, 'incomplete_pagination')
    require(start < end <= received_at <= now and end-start <= timedelta(minutes=60), 'bar_window')
    require(not start.second and not start.microsecond and not end.second and not end.microsecond, 'minute_alignment')
    raw = payload.get('bars')
    require(isinstance(raw, list) and len(raw) == int((end-start).total_seconds()/60), 'incomplete_bars')
    result=[]
    for i, row in enumerate(raw):
        require(isinstance(row, dict), 'invalid_bar')
        at = timestamp(row.get('t'))
        require(at == start+timedelta(minutes=i), 'bar_order_gap_or_duplicate')
        close = _price(row.get('c'))
        result.append(Bar(symbol, at, at+timedelta(minutes=1), str(close), received_at))
    return tuple(result)


def parse_session(clock, calendar, *, received_at, now):
    received_at, now = aware(received_at), aware(now)
    require(isinstance(clock, dict) and clock.get('is_open') is True, 'market_closed_or_unknown')
    at = timestamp(clock.get('timestamp'))
    require(at <= received_at <= now and now-at <= timedelta(seconds=10), 'stale_clock')
    require(isinstance(calendar, list) and len(calendar)==1 and isinstance(calendar[0], dict), 'calendar_missing')
    row=calendar[0]
    try:
        date=now.astimezone(ZoneInfo('America/New_York')).date().isoformat()
        require(row['date']==date, 'calendar_date')
        require(all(isinstance(row[k],str) and re.fullmatch(r'\d{2}:\d{2}',row[k]) for k in ('open','close')), 'calendar_time')
        op=datetime.fromisoformat(date+'T'+row['open']).replace(tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc)
        cl=datetime.fromisoformat(date+'T'+row['close']).replace(tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc)
        require(op <= at <= now < cl and timestamp(clock.get('next_close')) == cl, 'session_mismatch')
        require(timestamp(clock.get('next_open')) > now, 'next_open_mismatch')
    except (KeyError,ValueError): raise SafetyError('invalid_calendar') from None
    return Session(op,cl,received_at)


class ShadowMarketHTTP:
    """Explicit enable required; one instance, at most 12 requests/minute.

    Instance budget is NOT a cross-process/account budget. Production caller must
    provide a shared limiter if multiple instances/workers use the same keys.
    No pagination following: partial coverage fails instead of enlarging budget.
    """
    def __init__(self, key, secret, *, enabled=False, monotonic=time.monotonic):
        require(type(enabled) is bool, 'invalid_enable')
        require(all(isinstance(x,str) and x and all(33<=ord(c)<=126 for c in x) for x in (key,secret)), 'invalid_credentials')
        self.enabled=enabled
        self._headers={'APCA-API-KEY-ID':key,'APCA-API-SECRET-KEY':secret,'Accept':'application/json'}
        self._opener=request.build_opener(request.ProxyHandler({}),NoRedirect())
        self._times=deque();self._lock=threading.Lock();self._clock=monotonic

    def _get(self, origin, path):
        require(self.enabled is True, 'shadow_transport_disabled')
        require(origin in (DATA,PAPER), 'invalid_origin')
        require((origin == PAPER and (path == '/v2/clock' or re.fullmatch(r'/v2/calendar\?start=\d{4}-\d{2}-\d{2}&end=\d{4}-\d{2}-\d{2}', path))) or
                (origin == DATA and any(path == '/v2/stocks/'+s+'/quotes/latest?feed=iex' or
                 (path.startswith('/v2/stocks/'+s+'/bars?timeframe=1Min&') and '&feed=iex&' in path) for s in UNIVERSE)), 'invalid_route')
        with self._lock:
            at=self._clock()
            while self._times and at-self._times[0]>=60:self._times.popleft()
            require(len(self._times)<12, 'shadow_request_budget')
            self._times.append(at)
        if origin == DATA and '/bars?' in path:
            params=parse_qs(urlsplit(path).query,strict_parsing=True)
            require(set(params)=={'timeframe','start','end','limit','adjustment','feed','sort','asof'} and
                    all(len(v)==1 for v in params.values()) and
                    all(params[k]==[v] for k,v in dict(timeframe='1Min',limit='60',adjustment='raw',feed='iex',sort='asc',asof='-').items()), 'invalid_bar_route')
        url=origin+path
        try:
            with self._opener.open(request.Request(url,headers=self._headers,method='GET'),timeout=5) as response:
                require(response.geturl()==url and response.status==200, 'provider_response')
                raw=response.read(LIMIT+1)
                require(len(raw)<=LIMIT, 'response_too_large')
                return json.loads(raw,parse_float=Decimal,parse_constant=_invalid_constant,object_pairs_hook=_unique_object)
        except error.HTTPError as exc:
            code=exc.code;exc.close()
            raise SafetyError('provider_http_'+str(code)) from None
        except SafetyError:raise
        except Exception:raise SafetyError('provider_read_failed') from None

    def quote(self,symbol):
        require(symbol in UNIVERSE,'symbol_not_allowed')
        return self._get(DATA,'/v2/stocks/'+symbol+'/quotes/latest?feed=iex')

    def bars(self,symbol,start,end):
        start,end=aware(start),aware(end)
        require(symbol in UNIVERSE and timedelta(0)<end-start<=timedelta(minutes=60),'invalid_bar_request')
        require(not start.second and not start.microsecond and not end.second and not end.microsecond,'minute_alignment')
        params=dict(timeframe='1Min',start=start.isoformat(),end=(end-timedelta(microseconds=1)).isoformat(),limit=60,adjustment='raw',feed='iex',sort='asc',asof='-')
        return self._get(DATA,'/v2/stocks/'+symbol+'/bars?'+urlencode(params))

    def clock(self):return self._get(PAPER,'/v2/clock')

    def calendar(self,day):
        require(isinstance(day,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',day),'invalid_day')
        try:datetime.strptime(day,'%Y-%m-%d')
        except ValueError:raise SafetyError('invalid_day') from None
        return self._get(PAPER,'/v2/calendar?'+urlencode(dict(start=day,end=day)))
