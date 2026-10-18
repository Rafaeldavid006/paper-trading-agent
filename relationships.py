"""Point-in-time relationship research. Paths are hypotheses, never trade orders."""
import hashlib,json
from collections import deque
from datetime import datetime,timezone
from urllib.parse import urlparse
from event_core import iso,utc
from event_types import research_brief

RELATIONS={'buys_from','uses_technology','competes_with','substitutes_for','sells_to','finances','exposed_to_rate','exposed_to_currency','regulated_by','operates_in','depends_on_logistics','uses_commodity'}
class Relationships:
    def __init__(self,store):
        self.store=store
        with store.db() as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS research_evidence(
             id TEXT PRIMARY KEY,url TEXT,title TEXT,published_at TEXT,recorded_at TEXT,excerpt TEXT,synthetic INTEGER);
            CREATE TABLE IF NOT EXISTS research_edges(
             id TEXT PRIMARY KEY,origin TEXT,target TEXT,relation TEXT,evidence_id TEXT,
             status TEXT,valid_from TEXT,valid_until TEXT,recorded_at TEXT,materiality TEXT,
             FOREIGN KEY(evidence_id) REFERENCES research_evidence(id));
            CREATE TABLE IF NOT EXISTS research_runs(
             id TEXT PRIMARY KEY,created_at TEXT,as_of TEXT,event_entity TEXT,event_text TEXT,result TEXT);
            ''')
    def evidence(self,*,url,title,published_at,excerpt,synthetic=False):
        now=utc(self.store.clock());published=utc(published_at)
        if published>now:raise ValueError('evidence not yet published')
        if urlparse(url).scheme!='https' or not urlparse(url).hostname:raise ValueError('HTTPS source URL required')
        if not title.strip() or not excerpt.strip():raise ValueError('title and excerpt required')
        payload=dict(url=url,title=title,published_at=iso(published),excerpt=excerpt,synthetic=bool(synthetic))
        eid=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()
        with self.store.db() as c:
            c.execute('INSERT OR IGNORE INTO research_evidence VALUES (?,?,?,?,?,?,?)',(eid,url,title,iso(published),iso(now),excerpt,int(synthetic)))
        return eid
    def edge(self,*,origin,target,relation,evidence_id,status='hypothesis',valid_from=None,valid_until=None,materiality='unknown'):
        if relation not in RELATIONS or status not in {'reported','hypothesis'}:raise ValueError('invalid relationship')
        if origin==target or not origin.strip() or not target.strip():raise ValueError('distinct entities required')
        now=utc(self.store.clock());start=utc(valid_from or now);end=utc(valid_until) if valid_until else None
        if end and end<=start:raise ValueError('invalid validity interval')
        with self.store.db() as c:
            row=c.execute('SELECT * FROM research_evidence WHERE id=?',(evidence_id,)).fetchone()
            if not row:raise ValueError('evidence required even for hypotheses')
            payload=dict(origin=origin,target=target,relation=relation,evidence=evidence_id,status=status,start=iso(start),end=iso(end) if end else None,materiality=materiality)
            edge_id=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()
            c.execute('INSERT OR IGNORE INTO research_edges VALUES (?,?,?,?,?,?,?,?,?,?)',(edge_id,origin,target,relation,evidence_id,status,iso(start),iso(end) if end else None,iso(now),materiality))
        return edge_id
    def visible_edges(self,as_of,allow_synthetic=False):
        at=iso(as_of)
        with self.store.db() as c:
            rows=c.execute('''SELECT e.*,v.url,v.title,v.excerpt,v.synthetic,v.published_at
                FROM research_edges e JOIN research_evidence v ON e.evidence_id=v.id
                WHERE e.recorded_at<=? AND v.recorded_at<=? AND v.published_at<=?
                AND e.valid_from<=? AND (e.valid_until IS NULL OR e.valid_until>?)''',(at,at,at,at,at)).fetchall()
        return [dict(r) for r in rows if allow_synthetic or not r['synthetic']]
    def investigate(self,*,entity,event_text,as_of=None,max_hops=2,allow_synthetic=False,event_types=None):
        brief=research_brief(event_types)
        at=utc(as_of or self.store.clock())
        if at>utc(self.store.clock()):raise ValueError('cannot research using a future knowledge cutoff')
        if not 1<=max_hops<=3:raise ValueError('max_hops must be 1-3')
        if not event_text.strip():raise ValueError('event description required')
        edges=self.visible_edges(at,allow_synthetic)
        outgoing={}
        for edge in edges:outgoing.setdefault(edge['origin'],[]).append(edge)
        queue=deque([(entity,[],{entity})]);branches=[]
        while queue and len(branches)<40:
            current,path,seen=queue.popleft()
            if len(path)>=max_hops:continue
            for edge in sorted(outgoing.get(current,[]),key=lambda x:x['id']):
                if edge['target'] in seen:continue
                chain=path+[edge]
                facts=all(e['status']=='reported' for e in chain)
                branches.append({
                    'target':edge['target'],'hops':len(chain),'path':chain,
                    'status':'reported_relationship_research_candidate' if facts else 'unverified_relationship_hypothesis',
                    'direction':'undetermined_until_event_and_economics_verified',
                    'questions':brief['questions'],
                    'counterarguments':brief['counterarguments'],
                    'execution':'blocked_no_valuation_of_event_or_market_execution_evidence'})
                queue.append((edge['target'],chain,seen|{edge['target']}))
        result={'as_of':iso(at),'entity':entity,'event_text':event_text,'event_verified':False,
            'synthetic_mode':allow_synthetic,'research_brief':brief,'branches':branches,'order_permission':'none',
            'limitations':['The input event text has not been independently verified.','Reported means a supplied document describes a relationship, not that the kernel independently authenticated it.','No profitability, materiality, market data or public listing has been established.','No cross-source semantic deduplication or automatic entity resolution.'],
            'next_questions':[] if branches else ['Find primary sources establishing relevant economic exposures and counterparties for this event.']}
        run_id=hashlib.sha256((iso(self.store.clock())+json.dumps(result,sort_keys=True)).encode()).hexdigest()
        with self.store.db() as c:c.execute('INSERT OR IGNORE INTO research_runs VALUES (?,?,?,?,?,?)',(run_id,iso(self.store.clock()),iso(at),entity,event_text,json.dumps(result,ensure_ascii=False)))
        result['run_id']=run_id
        return result
