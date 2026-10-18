"""Polling recorder, not a real-time stream and not a trading process.
python monitor.py --db events.db --symbols SPY,QQQ
Required: ALPACA_API_KEY, ALPACA_SECRET_KEY. No secrets are logged.
"""
import argparse,json,os,signal,threading,urllib.request,urllib.parse
from datetime import datetime,timezone,timedelta
from event_core import Store
STOP=threading.Event()

def tick(store,symbols):
    key=os.environ.get('ALPACA_API_KEY','');secret=os.environ.get('ALPACA_SECRET_KEY','')
    if not key or not secret:raise ValueError('Configure Alpaca credentials outside code')
    start=datetime.now(timezone.utc)-timedelta(hours=24)
    params={'symbols':symbols,'start':start.isoformat(),'limit':50,'sort':'asc','include_content':'false'}
    count=0
    # Bounded pagination; hitting cap is explicitly a coverage failure.
    for page in range(10):
        req=urllib.request.Request('https://data.alpaca.markets/v1beta1/news?'+urllib.parse.urlencode(params),headers={'APCA-API-KEY-ID':key,'APCA-API-SECRET-KEY':secret})
        with urllib.request.urlopen(req,timeout=10) as response:data=json.load(response)
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
