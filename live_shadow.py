"""Read existing research SQLite snapshots, append separate observation audits.
No network/broker transport and no writes to the research database.
News observations are NOT the direct-earnings execution strategy.
"""
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
import hashlib
import json
import sqlite3
from event_core import utc, iso
from triage import screen
from price_reaction import summarize
from shadow_event_contract import Event, STRICT_VERSION

VERSION = 'existing-news-movement-observation-v1'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str, allow_nan=False)


def inspect_event(event, reaction, health, now, superseded=False):
    """No trade recommendation: identify observable co-movement worth reviewing."""
    now = utc(now)
    screening = screen(event)
    reasons = []
    if event['source'] != 'alpaca-news': reasons.append('unsupported_source')
    if superseded: reasons.append('superseded_event_version')
    published, received, updated = (utc(event[k]) for k in ('published_at','received_at','updated_at'))
    if max(published, received, updated) > now: reasons.append('future_event')
    if not published <= updated <= received: reasons.append('invalid_event_availability')
    try:
        payload = json.loads(event['payload'])
        digest = hashlib.sha256(json.dumps({'payload':payload,'published':iso(published),'updated':iso(updated),'url':event['url'],'symbol':event['symbol'],'primary':bool(event['verified_primary'])},sort_keys=True,allow_nan=False).encode()).hexdigest()
        version = hashlib.sha256((event['source']+'\0'+event['source_id']+'\0'+digest).encode()).hexdigest()
        if digest != event['content_hash'] or version != event['version_id']:
            reasons.append('event_provenance_hash_mismatch')
    except (ValueError, TypeError, KeyError):
        reasons.append('invalid_event_provenance')
    if (now-published).total_seconds() > 5400: reasons.append('outside_90_minute_observation_window')
    if not health or health['status'] != 'ok' or not health['last_success'] or not 0 <= (now-utc(health['last_success'])).total_seconds() <= 120:
        reasons.append('source_coverage_unhealthy_or_stale')
    if not screening['auto_eligible']: reasons.append('no_lexical_event_trigger')
    report = None
    if reaction is None:
        reasons.append('no_stored_market_observation')
    else:
        try:
            requested, available = utc(reaction['requested_at']), utc(reaction['received_at'])
            original = json.loads(reaction['report'])
            cutoff = utc(original['data_cutoff'])
            if not published <= cutoff <= requested <= available <= now or received > requested:
                raise ValueError('invalid availability')
            if not 0 <= (now-available).total_seconds() <= 360:
                reasons.append('stale_market_observation')
            if reaction['feed'] not in ('iex','sip') or original['feed'] != reaction['feed']:
                raise ValueError('feed mismatch')
            if utc(original['event_at']) != published or original.get('adjustment') != 'raw':
                raise ValueError('event provenance mismatch')
            bars = json.loads(reaction['raw_bars'])
            if not isinstance(bars,list) or len(bars)>1000: raise ValueError('invalid bars')
            report = summarize(published, cutoff, bars)
            latest = report.get('latest')
            if not latest or (now-utc(latest['end'])).total_seconds()>600:
                reasons.append('missing_or_old_completed_bars')
            elif not .3 <= latest['change_pct'] <= 2:
                reasons.append('outside_observational_movement_band')
        except (ValueError, TypeError, KeyError, OverflowError):
            reasons.append('invalid_market_observation')
    # The boolean in the generic store is not a collector receipt or issuer proof.
    # Never upgrade secondary news to a trusted earnings event, even if it is true.
    strict_event = Event(event_id=event['source']+':'+event['source_id'],
        episode_id=event['source']+':'+event['source_id'], version=event['version_id'],
        symbol=event['symbol'], published_at=published, observed_at=received,
        source_url=event['url'],source_hash=event['content_hash'],verified=False,
        corrected=updated>published,event_type='news')
    return {'rule_version':VERSION,'event_version':event['version_id'],
        'symbol':event['symbol'],'observed_at':iso(now),'order_permission':'none',
        'status':'observation_candidate' if not reasons else 'abstain',
        'reasons':reasons,'screening':screening,'movement':report,
        'reaction_id':None if reaction is None else reaction['id'],
        'strict_pipeline':{'strategy_version':STRICT_VERSION,'action':'abstain',
            'reasons':['no_trusted_primary_earnings_receipt','no_executable_quote','no_paired_spy_bars'],
            'event':asdict(strict_event)},
        'causality_or_profitable_edge_proven':False}


def run(source_db, audit_db, now, limit=100):
    """Bounded read transaction; separate durable audit; safe repeated invocation."""
    source, audit = Path(source_db).resolve(), Path(audit_db).resolve()
    if audit.exists() and source.samefile(audit): raise ValueError('separate_audit_database_required')
    if source == audit: raise ValueError('separate_audit_database_required')
    if audit.exists() and audit.stat().st_size >= 64 * 1024 * 1024:
        raise ValueError('shadow_audit_capacity_reached')
    if not 1 <= limit <= 1000: raise ValueError('invalid_limit')
    now = utc(now)
    c = sqlite3.connect(source.as_uri()+'?mode=ro', uri=True, timeout=1)
    c.row_factory = sqlite3.Row
    try:
        c.execute('BEGIN')
        names={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {'events','health'}.issubset(names): raise ValueError('research_schema_missing')
        health=c.execute("SELECT * FROM health WHERE source='alpaca-news'").fetchone()
        rows=c.execute('SELECT rowid AS ordinal,* FROM events WHERE received_at<=? AND published_at>=? ORDER BY received_at DESC,rowid DESC LIMIT ?',
            (iso(now),iso(now-timedelta(minutes=90)),limit)).fetchall()
        result=[]
        for row in rows:
            event=dict(row)
            newer=c.execute('SELECT 1 FROM events WHERE source=? AND source_id=? AND received_at<=? AND (updated_at>? OR (updated_at=? AND rowid>?)) LIMIT 1',
                (event['source'],event['source_id'],iso(now),event['updated_at'],event['updated_at'],event['ordinal'])).fetchone()
            reaction=c.execute('SELECT * FROM price_reactions WHERE event_version=? AND received_at<=? ORDER BY id DESC LIMIT 1',
                (event['version_id'],iso(now))).fetchone() if 'price_reactions' in names else None
            report=inspect_event(event,dict(reaction) if reaction else None,dict(health) if health else None,now,bool(newer))
            # Input digest includes health and actual source bytes, not only row IDs.
            fingerprint=hashlib.sha256(canonical([VERSION,event,dict(reaction) if reaction else None,dict(health) if health else None,report['status'],report['reasons']]).encode()).hexdigest()
            result.append((fingerprint,report))
    finally:
        c.close()
    with sqlite3.connect(audit, timeout=1) as out:
        pages = 64 * 1024 * 1024 // out.execute('PRAGMA page_size').fetchone()[0]
        out.execute('PRAGMA max_page_count=' + str(pages))
        out.execute('CREATE TABLE IF NOT EXISTS shadow_observations(id TEXT PRIMARY KEY,event_version TEXT NOT NULL,observed_at TEXT NOT NULL,report TEXT NOT NULL)')
        out.execute('BEGIN IMMEDIATE')
        if out.execute('SELECT count(*) FROM shadow_observations').fetchone()[0] + len(result) > 100000:
            raise ValueError('shadow_audit_capacity_reached')
        for fingerprint,report in result:
            out.execute('INSERT OR IGNORE INTO shadow_observations VALUES(?,?,?,?)',
                (fingerprint,report['event_version'],report['observed_at'],canonical(report)))
    return [report for _,report in result]


if __name__=='__main__':
    import argparse
    from datetime import datetime, timezone
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-db',required=True);p.add_argument('--audit-db',required=True)
    args=p.parse_args()
    reports=run(args.source_db,args.audit_db,datetime.now(timezone.utc))
    print(canonical({'observations':len(reports),'candidates':sum(r['status']=='observation_candidate' for r in reports),'order_permission':'none'}))
