"""Opt-in polling and web research worker. Run as a separate supervised process."""
import argparse
import os
import signal
import threading
from datetime import timedelta
from event_core import Store,iso,utc
from discovery import Discovery
from monitor import tick


def cycle(store,*,collector=tick,transport=None):
    if os.environ.get('ENABLE_AUTO_RESEARCH')!='true' or os.environ.get('ENABLE_WEB_RESEARCH')!='true':
        raise ValueError('automatic and web research must be explicitly enabled')
    symbols=os.environ.get('WATCHLIST','SPY,QQQ').upper()
    import re
    if not 1<=len(symbols.split(','))<=20 or any(not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,11}',s) for s in symbols.split(',')):
        raise ValueError('invalid watchlist')
    limit=int(os.environ.get('AI_MAX_CALLS_PER_DAY','5'))
    if not 1<=limit<=100:raise ValueError('invalid daily AI limit')
    discovery=Discovery(store)
    collector(store,symbols)
    now=utc(store.clock());cutoff=iso(now-timedelta(minutes=15))
    with store.db() as c:
        health=c.execute("SELECT * FROM health WHERE source='alpaca-news'").fetchone()
        if not health or health['status']!='ok' or not health['last_success'] or (now-utc(health['last_success'])).total_seconds()>120:
            return {'status':'blocked_source_coverage','order_permission':'none'}
        rows=c.execute('''SELECT e.version_id FROM events e WHERE e.published_at>=? AND e.published_at<=?
            AND NOT EXISTS(SELECT 1 FROM discovery_runs d WHERE d.event_version=e.version_id)
            AND NOT EXISTS(SELECT 1 FROM events n WHERE n.source=e.source AND n.source_id=e.source_id
                AND (n.updated_at>e.updated_at OR (n.updated_at=e.updated_at AND n.rowid>e.rowid)))
            ORDER BY e.published_at DESC LIMIT 1''',(cutoff,iso(now))).fetchall()
    if not rows:return {'status':'no_recent_pending_event','order_permission':'none'}
    result=discovery.run(rows[0]['version_id'],transport=transport,max_daily_calls=limit)
    return {'status':result['state'],'event_version':result['event_version'],'order_permission':'none'}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--db',required=True);parser.add_argument('--once',action='store_true')
    parser.add_argument('--interval',type=int,default=60);args=parser.parse_args()
    if args.interval<30:parser.error('interval must be at least 30 seconds')
    stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    store=Store(args.db)
    import json
    while not stop.is_set():
        try:
            result=cycle(store);store.heartbeat('research-worker','ok');print(json.dumps(result),flush=True)
        except Exception:
            store.heartbeat('research-worker','error');print('{"status":"research_blocked_or_failed"}',flush=True)
        if args.once:break
        stop.wait(args.interval)


if __name__=='__main__':main()
