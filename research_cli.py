"""Explicit operator commands; nothing runs or spends on import."""
import argparse
import json
from event_core import Store
from event_types import ROUTES
from pipeline import Pipeline


def main():
    parser=argparse.ArgumentParser(description='Event research only; no broker orders')
    parser.add_argument('--db',required=True)
    sub=parser.add_subparsers(dest='command',required=True)
    pending=sub.add_parser('events')
    pending.add_argument('--limit',type=int,default=10)
    prepare=sub.add_parser('prepare')
    prepare.add_argument('event_version')
    prepare.add_argument('--types',nargs='+',choices=sorted(ROUTES),default=['unknown'])
    show=sub.add_parser('show');show.add_argument('run_id')
    review=sub.add_parser('review',help='Makes one explicitly requested paid AI call if configured')
    review.add_argument('run_id')
    args=parser.parse_args()
    store=Store(args.db);pipeline=Pipeline(store)
    try:
        if args.command=='events':
            if not 1<=args.limit<=100:parser.error('limit must be 1-100')
            with store.db() as c:
                result=[dict(r) for r in c.execute('''SELECT version_id,symbol,source,published_at,
                    updated_at,received_at,url FROM events ORDER BY received_at DESC,rowid DESC LIMIT ?''',
                    (args.limit,))]
        elif args.command=='prepare':
            result=pipeline.prepare(args.event_version,event_types=args.types)
        elif args.command=='show':result=pipeline.get(args.run_id)
        else:result=pipeline.review(args.run_id)
    except ValueError as exc:
        parser.exit(2,str(exc)+'\n')
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
