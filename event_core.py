"""Auditable event-research kernel. No broker order functions, no profit claims."""
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import sqlite3
from contextlib import contextmanager


def utc(value):
    if isinstance(value,str):value=datetime.fromisoformat(value.replace('Z','+00:00'))
    if not isinstance(value,datetime) or value.tzinfo is None:raise ValueError('timezone required')
    return value.astimezone(timezone.utc)


def iso(value):return utc(value).isoformat(timespec='microseconds')

def finite(value):
    if isinstance(value,bool):raise ValueError('boolean is not a measurement')
    number=float(value)
    if not math.isfinite(number):raise ValueError('finite value required')
    return number

@dataclass(frozen=True)
class Quote:
    symbol:str
    bid:float
    ask:float
    exchange_at:datetime
    received_at:datetime
    consolidated:bool
    source:str

@dataclass(frozen=True)
class Policy:
    # Research parameters to calibrate on training data; not a demonstrated edge.
    max_event_age_s:int=120
    max_quote_age_s:int=3
    max_baseline_age_s:int=60
    max_spread:float=.005
    max_already_moved:float=.03
    min_positive_surprise:float=.05

class Store:
    def __init__(self,path,clock=None):
        self.path=str(path)
        self.clock=clock or (lambda:datetime.now(timezone.utc))
        with self.db() as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS events(
            version_id TEXT PRIMARY KEY, source TEXT, source_id TEXT, symbol TEXT,
            published_at TEXT, updated_at TEXT, received_at TEXT, url TEXT,
            verified_primary INTEGER, content_hash TEXT, payload TEXT,
            UNIQUE(source,source_id,content_hash));
            CREATE TABLE IF NOT EXISTS expectations(
            id INTEGER PRIMARY KEY, symbol TEXT, metric TEXT, period TEXT, unit TEXT,
            value REAL, published_at TEXT, recorded_at TEXT, source TEXT, url TEXT);
            CREATE TABLE IF NOT EXISTS decisions(
            id INTEGER PRIMARY KEY, event_version TEXT, decided_at TEXT, status TEXT,
            reasons TEXT, evidence TEXT);
            CREATE TABLE IF NOT EXISTS invalidations(
            id INTEGER PRIMARY KEY, decision_id INTEGER, new_event_version TEXT, recorded_at TEXT);
            CREATE TABLE IF NOT EXISTS health(
            source TEXT PRIMARY KEY, last_attempt TEXT, last_success TEXT, status TEXT);
            ''')
    @contextmanager
    def db(self):
        c=sqlite3.connect(self.path,timeout=10);c.row_factory=sqlite3.Row
        try:yield c;c.commit()
        except Exception:c.rollback();raise
        finally:c.close()
    def ingest(self,*,source,source_id,symbol,published_at,payload,url,updated_at=None,verified_primary=False):
        received=utc(self.clock()); published=utc(published_at)
        updated=utc(updated_at or published)
        if not all(isinstance(x,str) and x for x in (source,source_id,symbol,url)):raise ValueError('identity required')
        raw=json.dumps(payload,ensure_ascii=False,sort_keys=True,allow_nan=False)
        if len(raw.encode())>262144:raise ValueError('payload exceeds 256 KiB')
        digest=hashlib.sha256(json.dumps({'payload':payload,'published':iso(published),'updated':iso(updated),'url':url,'symbol':symbol,'primary':bool(verified_primary)},sort_keys=True,allow_nan=False).encode()).hexdigest()
        version=hashlib.sha256((source+'\0'+source_id+'\0'+digest).encode()).hexdigest()
        with self.db() as c:
            old=c.execute('SELECT version_id FROM events WHERE version_id=?',(version,)).fetchone()
            if old:return version,False
            c.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)',(version,source,source_id,symbol,iso(published),iso(updated),iso(received),url,int(verified_primary),digest,raw))
            # Append invalidations, preserving old decisions and event versions.
            previous=c.execute('SELECT d.id FROM decisions d JOIN events e ON e.version_id=d.event_version WHERE e.source=? AND e.source_id=? AND e.updated_at<=? AND d.status=?',(source,source_id,iso(updated),'research_candidate')).fetchall()
            for decision in previous:
                c.execute('INSERT INTO invalidations(decision_id,new_event_version,recorded_at) VALUES (?,?,?)',(decision['id'],version,iso(received)))
        return version,True
    def expectation(self,*,symbol,metric,period,unit,value,published_at,source,url):
        recorded=utc(self.clock());published=utc(published_at)
        if published>recorded:raise ValueError('future expectation publication')
        with self.db() as c:
            return c.execute('INSERT INTO expectations(symbol,metric,period,unit,value,published_at,recorded_at,source,url) VALUES (?,?,?,?,?,?,?,?,?)',(symbol,metric,period,unit,finite(value),iso(published),iso(recorded),source,url)).lastrowid
    def baseline(self,event,metric,period,unit):
        # Both source-publication and actual recording must predate the event.
        with self.db() as c:
            row=c.execute('SELECT * FROM expectations WHERE symbol=? AND metric=? AND period=? AND unit=? AND published_at<? AND recorded_at<? ORDER BY recorded_at DESC,id DESC LIMIT 1',(event['symbol'],metric,period,unit,event['published_at'],event['published_at'])).fetchone()
            return dict(row) if row else None
    def evaluate(self,version,*,quote=None,pre_event_quote=None,market_open=False,halted=None,policy=Policy()):
        at=utc(self.clock())
        with self.db() as c:
            row=c.execute('SELECT * FROM events WHERE version_id=?',(version,)).fetchone()
            if not row:raise ValueError('unknown event')
            event=dict(row)
            superseding=c.execute('SELECT version_id FROM events WHERE source=? AND source_id=? AND version_id!=? AND (updated_at>? OR (updated_at=? AND rowid>(SELECT rowid FROM events WHERE version_id=?))) AND received_at<=?',(event['source'],event['source_id'],version,event['updated_at'],event['updated_at'],version,iso(at))).fetchone()
            already=c.execute('SELECT id FROM decisions WHERE event_version=? AND status=?',(version,'research_candidate')).fetchone()
        reasons=[]; evidence={'event_version':version,'source':event['source'],'url':event['url'],'policy':policy.__dict__}
        published=utc(event['published_at']);received=utc(event['received_at']);updated=utc(event['updated_at'])
        if superseding:reasons.append('superseded_version')
        if already:reasons.append('candidate_already_recorded')
        if max(published,received,updated)>at:reasons.append('future_timestamp')
        if (at-published).total_seconds()>policy.max_event_age_s:reasons.append('event_too_old')
        if not event['verified_primary']:reasons.append('primary_source_not_verified')
        if not market_open:reasons.append('market_not_confirmed_open')
        if halted is not False:reasons.append('halt_status_unknown_or_halted')
        payload=json.loads(event['payload']);baseline=None
        if not isinstance(payload,dict):payload={}
        try:
            baseline=self.baseline(event,payload['metric'],payload['period'],payload['unit'])
            actual=finite(payload['actual'])
            if not baseline:reasons.append('no_pre_event_expectation')
            elif baseline['value']<=0 or actual<=0:reasons.append('metric_requires_separate_negative_value_model')
            else:
                surprise=actual/baseline['value']-1
                evidence.update(expectation=baseline,actual=actual,surprise=surprise)
                if surprise<policy.min_positive_surprise:reasons.append('insufficient_positive_surprise')
        except (KeyError,ValueError,TypeError):reasons.append('structured_metric_missing_or_invalid')
        for q,label in ((quote,'current'),(pre_event_quote,'baseline')):
            if q is None:reasons.append(label+'_quote_missing');continue
            try:
                bid,ask=finite(q.bid),finite(q.ask)
                exchange,receipt=utc(q.exchange_at),utc(q.received_at)
                if q.symbol!=event['symbol']:reasons.append(label+'_symbol_mismatch')
                if not q.consolidated:reasons.append(label+'_feed_not_consolidated')
                if bid<=0 or ask<=0 or bid>ask:reasons.append(label+'_quote_invalid')
                if receipt>at or exchange>receipt:reasons.append(label+'_quote_clock_invalid')
                if label=='current':
                    if exchange<published:reasons.append('current_quote_predates_event')
                    if (at-exchange).total_seconds()>policy.max_quote_age_s:reasons.append('current_quote_stale')
                    if ask>0 and (ask-bid)/ask>policy.max_spread:reasons.append('spread_too_wide')
                else:
                    if receipt>=published or exchange>=published:reasons.append('baseline_quote_not_known_before_event')
                    if (published-exchange).total_seconds()>policy.max_baseline_age_s:reasons.append('baseline_quote_stale')
                evidence[label+'_quote']={'symbol':q.symbol,'bid':bid,'ask':ask,'exchange_at':iso(exchange),'received_at':iso(receipt),'source':q.source,'consolidated':q.consolidated}
            except (ValueError,TypeError):reasons.append(label+'_quote_invalid')
        if quote and pre_event_quote:
            try:
                movement=finite(quote.ask)/finite(pre_event_quote.ask)-1
                evidence['already_moved']=movement
                if abs(movement)>policy.max_already_moved:reasons.append('price_already_moved')
            except (ValueError,TypeError,ZeroDivisionError):pass
        # A candidate is a research output, never an order or a claim of positive EV.
        status='research_candidate' if not reasons else 'hold'
        with self.db() as c:
            decision=c.execute('INSERT INTO decisions(event_version,decided_at,status,reasons,evidence) VALUES (?,?,?,?,?)',(version,iso(at),status,json.dumps(reasons),json.dumps(evidence,ensure_ascii=False))).lastrowid
        return {'decision_id':decision,'status':status,'reasons':reasons,'evidence':evidence}
    def heartbeat(self,source,status):
        stamp=iso(self.clock())
        with self.db() as c:
            prior=c.execute('SELECT last_success FROM health WHERE source=?',(source,)).fetchone()
            success=stamp if status=='ok' else (prior['last_success'] if prior else None)
            c.execute('INSERT OR REPLACE INTO health VALUES (?,?,?,?)',(source,stamp,success,status))
