"""Polling recorder, not a real-time stream and not a trading process.
python monitor.py --db events.db --symbols SPY,QQQ
Required: ALPACA_API_KEY, ALPACA_SECRET_KEY. No secrets are logged.
"""
import argparse,json,os,re,signal,threading,urllib.request,urllib.parse,urllib.error
from datetime import datetime,timezone,timedelta
from event_core import Store
STOP=threading.Event()
NEWS_ENDPOINT='https://data.alpaca.markets/v1beta1/news'
MAX_NEWS_BYTES=2*1024*1024

class NewsSourceError(ValueError):
    """Sanitized error: never contains credentials or provider response text."""

class _NoNewsRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # urllib raises HTTPError; the caller closes it without reading.

def _news_page(params, key, secret):
    """Fixed endpoint, no proxies/redirects; bounded UTF-8 JSON response."""
    try:
        if (set(params)-{'symbols','start','limit','sort','include_content','page_token'}
                or params.get('limit') != 50 or params.get('sort') != 'asc'
                or params.get('include_content') != 'false'
                or not isinstance(params.get('start'), str)
                or not isinstance(params.get('symbols'), str)):
            raise ValueError()
        start=datetime.fromisoformat(params['start'])
        if start.tzinfo is None or start.utcoffset() is None:raise ValueError()
        symbols=params['symbols'].split(',')
        if not 1 <= len(symbols) <= 20 or any(not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,11}', s) for s in symbols):
            raise ValueError()
        if 'page_token' in params and (not isinstance(params['page_token'], str) or len(params['page_token']) > 8192):
            raise ValueError()
        url=NEWS_ENDPOINT+'?'+urllib.parse.urlencode(params)
        req=urllib.request.Request(url, headers={'APCA-API-KEY-ID':key,'APCA-API-SECRET-KEY':secret})
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoNewsRedirect())
        with opener.open(req, timeout=10) as response:
            if response.status != 200 or response.geturl() != url:
                raise ValueError()
            chunks=[]; size=0
            while True:
                chunk=response.read(min(65536, MAX_NEWS_BYTES+1-size))
                if not chunk:break
                size+=len(chunk)
                if size > MAX_NEWS_BYTES:raise ValueError()
                chunks.append(chunk)
            data=json.loads(b''.join(chunks).decode('utf-8'), parse_constant=_reject_constant)
            if not isinstance(data, dict) or not isinstance(data.get('news'), list):
                raise ValueError()
            token=data.get('next_page_token')
            if token is not None and (not isinstance(token, str) or len(token) > 8192):
                raise ValueError()
            return data
    except urllib.error.HTTPError as exc:
        # Do not read, retain, or expose the provider's error body.
        try:exc.close()
        except Exception:pass
        raise NewsSourceError('News source request failed') from None
    except Exception:
        raise NewsSourceError('News source request failed') from None

def _reject_constant(value):
    raise ValueError()


def tick(store,symbols):
    key=os.environ.get('ALPACA_API_KEY','');secret=os.environ.get('ALPACA_SECRET_KEY','')
    if not key or not secret:raise ValueError('Configure Alpaca credentials outside code')
    start=datetime.now(timezone.utc)-timedelta(hours=24)
    params={'symbols':symbols,'start':start.isoformat(),'limit':50,'sort':'asc','include_content':'false'}
    count=0
    # Bounded pagination; hitting cap is explicitly a coverage failure.
    for page in range(10):
        data=_news_page(params,key,secret)
        for item in data.get('news',[]):
            for symbol in item.get('symbols',[]):
                if symbol not in symbols.split(','):continue
                _,inserted=store.ingest(source='alpaca-news',source_id=str(item['id'])+':'+symbol,symbol=symbol,published_at=item['created_at'],updated_at=item.get('updated_at'),url=item['url'],payload=item,verified_primary=False)
                count+=inserted
        token=data.get('next_page_token')
        if not token:store.heartbeat('alpaca-news','ok');return count
        params['page_token']=token
    store.heartbeat('alpaca-news','coverage_incomplete')
    return count

def main():
    p=argparse.ArgumentParser();p.add_argument('--db',required=True);p.add_argument('--symbols',default='SPY,QQQ');p.add_argument('--interval',type=int,default=30);p.add_argument('--once',action='store_true');args=p.parse_args()
    if args.interval<15:p.error('interval must be at least 15 seconds')
    import re
    symbols=args.symbols.upper()
    if len(symbols.split(','))>20 or any(not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,11}',s) for s in symbols.split(',')):p.error('1-20 valid symbols required')
    store=Store(args.db)
    for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,lambda *_:STOP.set())
    while not STOP.is_set():
        try:
            count=tick(store,symbols);print(json.dumps({'new_versions':count,'status':'recorded'}),flush=True)
        except Exception:
            store.heartbeat('alpaca-news','error');print('{"status":"source_error_no_signal"}',flush=True)
        if args.once:break
        STOP.wait(args.interval)

if __name__=='__main__':main()
