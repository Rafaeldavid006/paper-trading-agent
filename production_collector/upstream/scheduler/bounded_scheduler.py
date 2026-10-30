"""Offline bounded scheduler building block; no network implementation or daemon.
Explicit tick calls only. Transport must honor deadline/cancellation and own children.
"""
from dataclasses import dataclass, asdict
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlsplit
import ipaddress,json,re,sqlite3

def integer(value,low=0):
    if type(value) is not int or value<low:raise ValueError('bounded_integer_required')
    return value

def url(value):
    if type(value) is not str or not 1<=len(value)<=2048 or any(ord(c)<=32 or ord(c)>=127 for c in value) or '\\' in value:raise ValueError('url')
    u=urlsplit(value)
    if u.scheme!='https' or not u.hostname or u.username or u.password or u.port or u.query or u.fragment:raise ValueError('exact_public_https_url')
    host=u.hostname
    if '.' not in host or host.endswith(('.local','.internal')):raise ValueError('public_hostname')
    try:ipaddress.ip_address(host)
    except ValueError:return value
    raise ValueError('literal_ip_not_allowed')

@dataclass(frozen=True)
class Source:
    source_id:str
    exact_url:str
    cadence:int
    request_cap:int
    byte_cap:int
    response_cap:int
    declared_timeliness:str='unknown'
    approved:bool=False
    def __post_init__(self):
        if not re.fullmatch('[a-zA-Z0-9_-]{1,64}',self.source_id):raise ValueError('source_id')
        url(self.exact_url)
        for x in (self.cadence,self.request_cap,self.byte_cap,self.response_cap):integer(x,1)
        if self.response_cap>self.byte_cap or self.response_cap>1024*1024:raise ValueError('source_byte_bounds')
        if self.declared_timeliness not in ('live','delayed','historical','unknown'):raise ValueError('timeliness_label')
        if type(self.approved) is not bool:raise ValueError('approval_boolean')

@dataclass(frozen=True)
class Policy:
    run_id:str
    start:int
    end:int
    request_cap:int
    byte_cap:int
    request_timeout:int=10
    lateness_grace:int=5
    def __post_init__(self):
        if not re.fullmatch('[a-zA-Z0-9_-]{1,64}',self.run_id):raise ValueError('run_id')
        for x in (self.start,self.end,self.request_cap,self.byte_cap,self.request_timeout,self.lateness_grace):integer(x)
        if self.end<=self.start or self.end-self.start>86400 or not self.request_cap or not self.byte_cap or not 1<=self.request_timeout<=10:raise ValueError('policy_bounds')
        if self.request_cap>10000 or self.byte_cap>100*1024*1024:raise ValueError('global_bounds')

@dataclass(frozen=True)
class Request:
    run_id:str
    configuration_hash:str
    attempt_id:int
    source_id:str
    url:str
    started_at:int
    deadline:int
    max_bytes:int

@dataclass(frozen=True)
class Response:
    url:str
    status:int
    body:bytes
    received_at:int
    owned_transport_reaped:bool

class Scheduler:
    def __init__(self,path,policy,sources,*,enabled=False):
        if type(enabled) is not bool:raise ValueError('enabled_boolean')
        if not isinstance(policy,Policy) or type(sources) is not tuple or not 1<=len(sources)<=128 or any(not isinstance(s,Source) for s in sources):raise ValueError('configuration')
        if len({s.source_id for s in sources})!=len(sources):raise ValueError('duplicate_source')
        self.policy=policy;self.sources={s.source_id:s for s in sources};self.enabled=enabled
        self.path=Path(path)
        if self.path.is_symlink():raise ValueError('symlink_db')
        if self.path.exists():
            probe=sqlite3.connect(self.path.absolute().as_uri()+'?mode=ro',uri=True)
            try:
                marker=probe.execute('SELECT config,config_hash FROM metadata WHERE id=1').fetchone()
                if marker is None or sha256(marker[0].encode()).hexdigest()!=marker[1] or set(json.loads(marker[0]))!={'policy','sources'}:raise ValueError('not_scheduler_database')
            except (sqlite3.Error,KeyError,TypeError,json.JSONDecodeError) as exc:
                raise ValueError('not_scheduler_database') from None
            finally:probe.close()
        self.db=sqlite3.connect(self.path,timeout=2,isolation_level=None)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS metadata (id INTEGER PRIMARY KEY CHECK(id=1),config TEXT NOT NULL,config_hash TEXT NOT NULL,stopped INTEGER NOT NULL DEFAULT 0,reason TEXT,last_tick INTEGER);
        CREATE TABLE IF NOT EXISTS sources (source_id TEXT PRIMARY KEY,blocked INTEGER NOT NULL DEFAULT 0,reason TEXT);
        CREATE TABLE IF NOT EXISTS attempts (id INTEGER PRIMARY KEY,source_id TEXT NOT NULL,slot INTEGER NOT NULL,started INTEGER NOT NULL,deadline INTEGER NOT NULL,reserved_bytes INTEGER NOT NULL,state TEXT NOT NULL,finished INTEGER,status INTEGER,body BLOB,body_hash TEXT,error TEXT,UNIQUE(source_id,slot));
        CREATE TABLE IF NOT EXISTS missed (source_id TEXT NOT NULL,slot INTEGER NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(source_id,slot));
        CREATE TABLE IF NOT EXISTS candidates (url TEXT PRIMARY KEY,discovered_via TEXT NOT NULL,status TEXT NOT NULL CHECK(status='pending_review'));
        ''')
        config=json.dumps({'policy':asdict(policy),'sources':[asdict(s) for s in sorted(sources,key=lambda s:s.source_id)]},sort_keys=True,separators=(',',':'))
        digest=sha256(config.encode()).hexdigest()
        self.configuration_hash=digest
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row=self.db.execute('SELECT config,config_hash FROM metadata WHERE id=1').fetchone()
            if row and (row['config']!=config or row['config_hash']!=digest):raise ValueError('immutable_configuration_mismatch')
            if not row:
                self.db.execute('INSERT INTO metadata(id,config,config_hash) VALUES(1,?,?)',(config,digest))
                self.db.executemany('INSERT INTO sources(source_id) VALUES(?)',[(s,) for s in self.sources])
            self.db.execute('COMMIT')
        except BaseException:self.db.execute('ROLLBACK');self.db.close();raise
    def close(self):self.db.close()
    def stopped(self):return bool(self.db.execute('SELECT stopped FROM metadata WHERE id=1').fetchone()[0])
    def stop(self,reason='operator_stop'):
        if type(reason) is not str or not reason or len(reason)>120:raise ValueError('stop_reason')
        self.db.execute('UPDATE metadata SET stopped=1,reason=? WHERE id=1',(reason,))
    def propose(self,candidate_url,discovered_via):
        url(candidate_url);url(discovered_via)
        if discovered_via not in {s.exact_url for s in self.sources.values() if s.approved}:raise ValueError('unapproved_parent')
        with self.db:
            if self.db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0]>=1000:raise ValueError('candidate_cap')
            self.db.execute('INSERT OR IGNORE INTO candidates VALUES(?,?,?)',(candidate_url,discovered_via,'pending_review'))
    def claim(self,now):
        integer(now)
        if not self.enabled:return None
        self.db.execute('BEGIN IMMEDIATE')
        try:
            meta=self.db.execute('SELECT * FROM metadata WHERE id=1').fetchone()
            if meta['last_tick'] is not None and now<meta['last_tick']:
                self.db.execute("UPDATE metadata SET stopped=1,reason='wall_clock_regression' WHERE id=1")
                self.db.execute('COMMIT');return None
            self.db.execute('UPDATE metadata SET last_tick=? WHERE id=1',(now,))
            if meta['stopped'] or now<self.policy.start or now>=self.policy.end:
                self.db.execute('COMMIT');return None
            # Durable active attempt is NOT automatically timed out/retried after restart.
            # It blocks all further requests until separately reviewed recovery.
            if self.db.execute("SELECT 1 FROM attempts WHERE state='claimed' LIMIT 1").fetchone():
                self.db.execute('COMMIT');return None
            total=self.db.execute('SELECT COUNT(*),COALESCE(SUM(reserved_bytes),0) FROM attempts').fetchone()
            if total[0]>=self.policy.request_cap:
                self.db.execute('COMMIT');return None
            due=[]
            for sid,s in sorted(self.sources.items()):
                if not s.approved or self.db.execute('SELECT blocked FROM sources WHERE source_id=?',(sid,)).fetchone()[0]:continue
                slot=(now-self.policy.start)//s.cadence
                nominal=self.policy.start+slot*s.cadence
                if now-nominal>self.policy.lateness_grace:
                    self.db.execute('INSERT OR IGNORE INTO missed VALUES(?,?,?)',(sid,slot,'late_slot_no_catchup'));continue
                if self.db.execute('SELECT 1 FROM attempts WHERE source_id=? AND slot=?',(sid,slot)).fetchone() or self.db.execute('SELECT 1 FROM missed WHERE source_id=? AND slot=?',(sid,slot)).fetchone():continue
                used=self.db.execute('SELECT COUNT(*),COALESCE(SUM(reserved_bytes),0),COALESCE(MAX(started),-1) FROM attempts WHERE source_id=?',(sid,)).fetchone()
                if used[0]>=s.request_cap or used[1]+s.response_cap>s.byte_cap or total[1]+s.response_cap>self.policy.byte_cap:continue
                due.append((used[2],sid,slot,s))
            if not due:self.db.execute('COMMIT');return None
            _,sid,slot,s=min(due)
            deadline=min(now+self.policy.request_timeout,self.policy.end)
            cur=self.db.execute("INSERT INTO attempts(source_id,slot,started,deadline,reserved_bytes,state) VALUES(?,?,?,?,?,'claimed')",(sid,slot,now,deadline,s.response_cap))
            rid=cur.lastrowid
            self.db.execute('COMMIT')
            return Request(self.policy.run_id,self.configuration_hash,rid,sid,s.exact_url,now,deadline,s.response_cap)
        except BaseException:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise
    def finish(self,request,response=None,*,error=None):
        if not isinstance(request,Request):raise ValueError('request_required')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row=self.db.execute('SELECT * FROM attempts WHERE id=?',(request.attempt_id,)).fetchone()
            if not row or row['state']!='claimed':raise ValueError('not_owned_claim')
            s=self.sources[row['source_id']]
            if request!=Request(self.policy.run_id,self.configuration_hash,row['id'],row['source_id'],s.exact_url,row['started'],row['deadline'],row['reserved_bytes']):raise ValueError('request_tampered')
            state='failed';status=None;finished=None;body=None;digest=None;reason=error
            if response is not None:
                if not isinstance(response,Response):raise ValueError('response_type')
                status=response.status;finished=response.received_at
                if type(status) is not int or type(finished) is not int:reason='response_fields'
                elif response.owned_transport_reaped is not True:reason='transport_reap_uncertain'
                elif response.url!=request.url:reason='redirect_or_url_mismatch'
                elif type(response.body) is not bytes or len(response.body)>request.max_bytes:reason='body_cap'
                elif finished<request.started_at:reason='response_clock_regression'
                elif finished>=request.deadline:reason='deadline_exceeded'
                elif status in (401,403,429):reason='access_or_rate_denial'
                elif status!=200:reason='http_failure'
                elif self.stopped():reason='stopped_inflight'
                elif not response.body:reason='empty_body'
                else:
                    state='received';reason=None;body=response.body;digest=sha256(body).hexdigest()
            if reason is not None and (type(reason) is not str or len(reason)>120):reason='invalid_error'
            if response is None and reason is None:reason='transport_failed'
            # Any failure quarantines source; no automatic retry even next cadence.
            if state!='received':self.db.execute('UPDATE sources SET blocked=1,reason=? WHERE source_id=?',(reason,s.source_id))
            if reason in ('transport_reap_uncertain','response_clock_regression'):
                self.db.execute('UPDATE metadata SET stopped=1,reason=? WHERE id=1',(reason,))
            self.db.execute('UPDATE attempts SET state=?,finished=?,status=?,body=?,body_hash=?,error=? WHERE id=?',(state,finished,status,body,digest,reason,request.attempt_id))
            self.db.execute('COMMIT');return state
        except BaseException:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise
    def tick(self,now,transport):
        request=self.claim(now)
        if request is None:return None
        try:response=transport(request,self.stopped)
        except BaseException as exc:
            # An exception does not prove the injected transport owns/reaped a child.
            self.finish(request,error='transport_reap_uncertain')
            if not isinstance(exc,Exception):raise
            return 'failed'
        return self.finish(request,response)
    def snapshot(self):
        return {'active_service':False,'enabled_for_explicit_ticks':self.enabled,'stopped':self.stopped(),
            'request_attempts':self.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0],
            'reserved_bytes':self.db.execute('SELECT COALESCE(SUM(reserved_bytes),0) FROM attempts').fetchone()[0],
            'received':self.db.execute("SELECT COUNT(*) FROM attempts WHERE state='received'").fetchone()[0],
            'uncertain_claims':self.db.execute("SELECT COUNT(*) FROM attempts WHERE state='claimed'").fetchone()[0],
            'declared_timeliness':{sid:s.declared_timeliness for sid,s in self.sources.items()},
            'verified_timeliness':'unknown','original_verified':False,'order_permission':'none'}
