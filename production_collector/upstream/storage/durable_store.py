"""SQLite evidence journal v3: offline only; no collection or trading authority.
Append-only APIs/triggers and hash chain detect ordinary corruption, not a hostile
DB administrator. Times and approvals are caller attestations, not authentication.
"""
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import json
import os
import sqlite3
import sys

RESEARCH_ROOT = Path(__file__).resolve().parents[2]
if str(RESEARCH_ROOT) not in sys.path:
    sys.path.insert(0, str(RESEARCH_ROOT))
from modular_ingestion.core import Registry, SourceSpec, SourceClass, Receipt, ParsedEvent, utc, pinned_url

SCHEMA_VERSION = 3
APPLICATION_ID = 0x45564433
TABLES = ('records', 'blobs', 'versions', 'observations')

def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)

def digest(obj):
    return sha256(canonical(obj).encode()).hexdigest()

def timestamp(value):
    return utc(value).isoformat(timespec='microseconds')

def clock_now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')

def spec_dict(spec):
    d = asdict(spec); d['source_class'] = spec.source_class.value
    return json.loads(canonical(d))

def source_from_dict(d):
    d = dict(d); d['urls'] = tuple(d['urls']); d['source_class'] = SourceClass(d['source_class'])
    return SourceSpec(**d)

class IntegrityError(ValueError):
    pass

class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try: return super().__exit__(*args)
        finally: self.close()

class EvidenceStore:
    def __init__(self, path, *, clock=clock_now):
        self.path = Path(path)
        if self.path.is_symlink() or not self.path.parent.is_dir():
            raise ValueError('regular_local_database_path_required')
        self.clock = clock
        if self.path.exists():
            self._check_identity()  # Read-only, before journal pragma or any DDL.
        else:
            fd=os.open(self.path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            os.close(fd)
        self._initializing=True
        with self.connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, SCHEMA_VERSION): raise ValueError('unsupported_schema')
            db.executescript('''
            CREATE TABLE IF NOT EXISTS records (
              seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, recorded_at TEXT NOT NULL,
              payload TEXT NOT NULL, previous_hash TEXT NOT NULL, record_hash TEXT NOT NULL UNIQUE);
            CREATE TABLE IF NOT EXISTS blobs (
              body_sha256 TEXT PRIMARY KEY, body BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS versions (
              version_id TEXT PRIMARY KEY, event_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS observations (
              record_seq INTEGER PRIMARY KEY REFERENCES records(seq), receipt_json TEXT NOT NULL,
              version_ids_json TEXT NOT NULL, FOREIGN KEY(record_seq) REFERENCES records(seq));
            ''')
            for table in TABLES:
                for op in ('UPDATE', 'DELETE'):
                    db.execute(f"CREATE TRIGGER IF NOT EXISTS immutable_{table}_{op} BEFORE {op} ON {table} BEGIN SELECT RAISE(ABORT,'append_only'); END")
            db.execute(f'PRAGMA application_id={APPLICATION_ID}')
            db.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
        self._initializing=False

    def _check_identity(self):
        if self.path.is_symlink() or not self.path.is_file():raise ValueError('research_database_identity_required')
        db=sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)
        try:
            if db.execute('PRAGMA application_id').fetchone()[0]!=APPLICATION_ID or db.execute('PRAGMA user_version').fetchone()[0]!=SCHEMA_VERSION:
                raise ValueError('not_current_research_database')
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            expected={'records':['seq','kind','recorded_at','payload','previous_hash','record_hash'],
                'blobs':['body_sha256','body'],'versions':['version_id','event_json'],
                'observations':['record_seq','receipt_json','version_ids_json']}
            if tables!=set(expected):raise ValueError('research_schema_identity_mismatch')
            for table,columns in expected.items():
                if [r[1] for r in db.execute('PRAGMA table_info('+table+')')]!=columns:raise ValueError('research_columns_mismatch')
        finally:db.close()

    def connect(self):
        if not getattr(self,'_initializing',False):self._check_identity()
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None, factory=ClosingConnection)
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA synchronous=FULL')
        return db

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _append(self, db, kind, payload):
        recorded = timestamp(self.clock())
        previous = db.execute('SELECT seq,recorded_at,record_hash FROM records ORDER BY seq DESC LIMIT 1').fetchone()
        seq, prior_hash = (previous[0]+1, previous[2]) if previous else (1, '0'*64)
        if previous and recorded < previous[1]: raise ValueError('commit_clock_regressed')
        record_hash = digest([seq, kind, recorded, payload, prior_hash])
        db.execute('INSERT INTO records VALUES (?,?,?,?,?,?)', (seq, kind, recorded, canonical(payload), prior_hash, record_hash))
        return seq, recorded

    def register_source(self, spec, *, reviewer, rationale):
        if not isinstance(spec, SourceSpec) or not reviewer or not rationale: raise ValueError('source_review_required')
        with self.transaction() as db:
            return self._append(db, 'source_spec', {'spec':spec_dict(spec), 'reviewer':reviewer, 'rationale':rationale})[0]

    def source(self, source_id, db=None):
        own = db is None
        if own: db = self.connect()
        try:
            for seq, raw in db.execute("SELECT seq,payload FROM records WHERE kind='source_spec' ORDER BY seq DESC"):
                p=json.loads(raw)
                if p['spec']['source_id']==source_id: return seq, source_from_dict(p['spec'])
            raise KeyError(source_id)
        finally:
            if own: db.close()

    def propose_candidate(self, url, source_class, *, discovered_via, reason):
        pinned_url(url); pinned_url(discovered_via)
        if not isinstance(source_class,SourceClass) or not reason: raise ValueError('candidate_metadata')
        cid=sha256(url.encode()).hexdigest()
        with self.transaction() as db:
            self._append(db,'candidate',dict(candidate_id=cid,url=url,source_class=source_class.value,discovered_via=discovered_via,reason=reason,status='pending_review'))
        return cid

    def record_review(self, candidate_id, decision, *, reviewer, rationale):
        if decision not in ('approve_for_offline_research','reject','defer') or not reviewer or not rationale:
            raise ValueError('review_contract')
        with self.transaction() as db:
            candidates = [json.loads(r[0])['candidate_id'] for r in db.execute("SELECT payload FROM records WHERE kind='candidate'")]
            if candidate_id not in candidates: raise KeyError(candidate_id)
            return self._append(db,'review',dict(candidate_id=candidate_id,decision=decision,reviewer=reviewer,rationale=rationale,
                automatic_activation=False))[0]

    def append_batch(self, raw, receipt, parsed_events, adapter_version, *, _before_commit=None, _before_confirm=None):
        """Each invocation is a NEW observation, even for duplicate bytes/events.
        No idempotency ambiguity: callers must inspect after uncertain commit.
        """
        if not isinstance(receipt,Receipt): raise ValueError('receipt_type')
        parsed_events=tuple(parsed_events)
        if len(parsed_events)>1000: raise ValueError('batch_limit')
        with self.transaction() as db:
            source_seq,spec=self.source(receipt.source_id,db)
            registry=Registry()
            if not spec.adapter: raise ValueError('adapter_required')
            registry.register_source(spec)
            registry.register_adapter(spec.adapter,adapter_version,lambda body,r:parsed_events)
            events=registry.ingest(spec.source_id,raw,receipt)
            stable=[]
            for event in events:
                d=asdict(event); d.pop('receipt')
                d=json.loads(canonical(d))
                old=db.execute('SELECT event_json FROM versions WHERE version_id=?',(event.version_id,)).fetchone()
                if old and old[0]!=canonical(d): raise ValueError('conflicting_version')
                stable.append(d)
            payload={'source_record_seq':source_seq,'spec':spec_dict(spec),'receipt':asdict(receipt),
                     'body_sha256':receipt.body_sha256,'events':stable}
            seq,recorded=self._append(db,'event_batch',payload)
            if timestamp(receipt.received_at)>recorded: raise ValueError('receipt_after_commit')
            old=db.execute('SELECT body FROM blobs WHERE body_sha256=?',(receipt.body_sha256,)).fetchone()
            if old is not None and bytes(old[0])!=raw: raise IntegrityError('body_hash_collision_or_corruption')
            db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(receipt.body_sha256,raw))
            for d in stable: db.execute('INSERT OR IGNORE INTO versions VALUES (?,?)',(d['version_id'],canonical(d)))
            db.execute('INSERT INTO observations VALUES (?,?,?)',(seq,canonical(asdict(receipt)),canonical([d['version_id'] for d in stable])))
            if _before_commit: _before_commit() # test-only fault hook, no external effect
        if _before_confirm: _before_confirm() # test-only fault between durable batch and confirmation
        self._confirm_observation(seq)
        return seq

    def append_tool_observation(self, extraction, receipt, *, source_reference, summary):
        """Durable lower-grade extraction, never origin bytes or parsed Event."""
        receipt.validate(extraction)
        if receipt.mode!='tool' or not source_reference or not summary or len(summary)>4000: raise ValueError('tool_contract')
        with self.transaction() as db:
            source_seq,spec=self.source(receipt.source_id,db)
            if not spec.approved or receipt.url not in spec.urls: raise ValueError('source_not_approved')
            payload={'source_record_seq':source_seq,'receipt':asdict(receipt),'extraction_sha256':receipt.body_sha256,
                'source_reference':source_reference,'summary':summary,'origin_body_sha256':None,
                'source_available_at':None,'original_verified':False,'eligible_for_backtest':False,'order_permission':'none'}
            seq,recorded=self._append(db,'tool_observation',payload)
            if timestamp(receipt.received_at)>recorded: raise ValueError('receipt_after_commit')
            db.execute('INSERT OR IGNORE INTO blobs VALUES (?,?)',(receipt.body_sha256,extraction))
            db.execute('INSERT INTO observations VALUES (?,?,?)',(seq,canonical(asdict(receipt)),'[]'))
        self._confirm_observation(seq)
        return seq

    def _confirm_observation(self, observation_seq):
        # Called only after the batch transaction has actually committed. This
        # clock is a conservative durable-by bound, NOT the exact commit time.
        with self.transaction() as db:
            if not db.execute('SELECT 1 FROM observations WHERE record_seq=?',(observation_seq,)).fetchone():
                raise KeyError(observation_seq)
            confirmed={json.loads(x[0])['observation_seq'] for x in db.execute("SELECT payload FROM records WHERE kind='commit_confirmation'")}
            if observation_seq not in confirmed:
                self._append(db,'commit_confirmation',{'observation_seq':observation_seq,'meaning':'batch_was_already_committed_before_this_clock_sample'})

    def confirm_pending(self):
        """Explicit recovery only. Never backdate a previously unconfirmed batch."""
        with self.connect() as db:
            pending=[x[0] for x in db.execute('SELECT record_seq FROM observations')]
        for seq in pending: self._confirm_observation(seq)

    def events_as_observed(self, as_of):
        """Conservative local durable-by view, NOT historical tradability.
        Requires receipt AND post-batch-commit confirmation at/before cutoff.
        Backfilled and unconfirmed records cannot become earlier knowledge.
        """
        cutoff=timestamp(as_of); result={}
        with self.connect() as db:
            db.execute('BEGIN')
            confirmed={json.loads(payload)['observation_seq']:recorded for recorded,payload in db.execute(
                "SELECT recorded_at,payload FROM records WHERE kind='commit_confirmation' AND recorded_at<=?",(cutoff,))}
            for seq,recorded,receipt_json,ids in db.execute('SELECT r.seq,r.recorded_at,o.receipt_json,o.version_ids_json FROM observations o JOIN records r ON r.seq=o.record_seq ORDER BY r.seq'):
                if seq not in confirmed:continue
                r=json.loads(receipt_json)
                if timestamp(r['received_at'])>cutoff: continue
                for vid in json.loads(ids):
                    event=json.loads(db.execute('SELECT event_json FROM versions WHERE version_id=?',(vid,)).fetchone()[0])
                    entry=result.setdefault(vid,{'event':event,'evidence_class':'local_observation_history','observations':[],
                        'historical_available_at':None,'eligible_for_backtest':False,'order_permission':'none'})
                    entry['observations'].append({'record_seq':seq,'recorded_at':recorded,'durable_confirmed_by':confirmed[seq],'receipt':r})
        return list(result.values())

    def tool_observations_as_of(self, as_of, *, include_extraction=False):
        """Read-only lower-grade tool evidence; never yields raw-origin Events.
        Extraction bytes are opt-in and are not publisher HTML/body attestation.
        """
        if type(include_extraction) is not bool:raise ValueError('boolean_extraction_option')
        cutoff=timestamp(as_of);result=[]
        with self.connect() as db:
            db.execute('BEGIN')
            confirmed={json.loads(payload)['observation_seq']:recorded for recorded,payload in db.execute(
                "SELECT recorded_at,payload FROM records WHERE kind='commit_confirmation' AND recorded_at<=?",(cutoff,))}
            for seq,recorded,payload in db.execute("SELECT seq,recorded_at,payload FROM records WHERE kind='tool_observation' ORDER BY seq"):
                p=json.loads(payload)
                if seq not in confirmed or timestamp(p['receipt']['received_at'])>cutoff:continue
                item={'record_seq':seq,'recorded_at':recorded,'durable_confirmed_by':confirmed[seq],
                    'evidence_class':'tool_mediated_observation','metadata':p,
                    'origin_body_sha256':None,'origin_fetch_at':None,'cache_status':'unknown',
                    'historical_available_at':None,'eligible_for_backtest':False,'original_verified':False,'order_permission':'none'}
                if include_extraction:
                    row=db.execute('SELECT body FROM blobs WHERE body_sha256=?',(p['extraction_sha256'],)).fetchone()
                    if row is None:raise IntegrityError('missing_extraction')
                    extraction=bytes(row[0])
                    if sha256(extraction).hexdigest()!=p['extraction_sha256']:raise IntegrityError('extraction_hash_mismatch')
                    item['extraction_bytes']=extraction
                result.append(item)
        return result

    def freshness(self, version_id, as_of):
        items=[e for e in self.events_as_observed(as_of) if e['event']['version_id']==version_id]
        if not items: return 'not_locally_observed_as_of'
        valid=[o for o in items[0]['observations'] if o['receipt']['mode']=='direct' and o['receipt']['clock_verified'] is True]
        if not valid: return 'unknown'
        last=max(valid,key=lambda o:timestamp(o['receipt']['received_at']))
        with self.connect() as db:
            p=json.loads(db.execute('SELECT payload FROM records WHERE seq=?',(last['record_seq'],)).fetchone()[0])
        age=(utc(as_of)-utc(last['receipt']['received_at'])).total_seconds()
        return 'recent_observation' if age<=p['spec']['max_age_seconds'] else 'stale_observation'

    def verify(self):
        """Validate chain, content hashes and all materialized records in snapshot."""
        with self.connect() as db:
            db.execute('BEGIN')
            if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise IntegrityError('sqlite_integrity')
            previous='0'*64; previous_time=None; expected_versions={};expected_observations={};expected_blobs=set();sources={};confirmed=set()
            records=list(db.execute('SELECT seq,kind,recorded_at,payload,previous_hash,record_hash FROM records ORDER BY seq'))
            for expected_seq,(seq,kind,recorded,raw,prior,hashed) in enumerate(records,1):
                p=json.loads(raw)
                if seq!=expected_seq or prior!=previous or digest([seq,kind,recorded,p,prior])!=hashed:raise IntegrityError('record_chain')
                if timestamp(recorded)!=recorded or previous_time and recorded<previous_time:raise IntegrityError('commit_order')
                previous,previous_time=hashed,recorded
                if kind=='source_spec': sources[seq]=spec_dict(source_from_dict(p['spec']))
                elif kind in ('event_batch','tool_observation'):
                    receipt=Receipt(**p['receipt']);key=p.get('body_sha256',p.get('extraction_sha256'))
                    blob=db.execute('SELECT body FROM blobs WHERE body_sha256=?',(key,)).fetchone()
                    if blob is None:raise IntegrityError('missing_blob')
                    receipt.validate(bytes(blob[0])); expected_blobs.add(key)
                    if timestamp(receipt.received_at)>recorded:raise IntegrityError('receipt_after_commit')
                    spec=sources.get(p['source_record_seq'])
                    if spec is None or spec['source_id']!=receipt.source_id or receipt.url not in spec['urls'] or spec['approved'] is not True:raise IntegrityError('source_reference')
                    ids=[]
                    if kind=='event_batch':
                        if p['spec']!=spec:raise IntegrityError('source_spec_changed')
                        reg=Registry();reg.register_source(source_from_dict(spec))
                        events=p['events'];versions={e['adapter_version'] for e in events}
                        if len(versions)>1:raise IntegrityError('mixed_adapter_versions')
                        # Empty event batches are valid source observations.
                        adapter_version=next(iter(versions),'empty-batch')
                        parsed=[ParsedEvent(**{**e['parsed'],'sectors':tuple(e['parsed']['sectors'])}) for e in events]
                        reg.register_adapter(spec['adapter'],adapter_version,lambda raw,r:parsed)
                        regenerated=reg.ingest(receipt.source_id,bytes(blob[0]),receipt)
                        for original,event in zip(events,regenerated):
                            stable=asdict(event);stable.pop('receipt');stable=json.loads(canonical(stable))
                            if stable!=original:raise IntegrityError('event_reconstruction')
                            vid=event.version_id;ids.append(vid)
                            if vid in expected_versions and expected_versions[vid]!=canonical(stable):raise IntegrityError('conflicting_version')
                            expected_versions[vid]=canonical(stable)
                    else:
                        if receipt.mode!='tool' or p['origin_body_sha256'] is not None or p['original_verified'] is not False or p['eligible_for_backtest'] is not False:raise IntegrityError('tool_upgrade')
                    expected_observations[seq]=(canonical(p['receipt']),canonical(ids))
                elif kind=='commit_confirmation':
                    target=p['observation_seq']
                    if target not in expected_observations or target in confirmed:raise IntegrityError('confirmation_reference')
                    confirmed.add(target)
                elif kind not in ('candidate','review'):raise IntegrityError('unknown_record_kind')
            if dict(db.execute('SELECT version_id,event_json FROM versions'))!=expected_versions:raise IntegrityError('version_index_mismatch')
            if {r[0]:(r[1],r[2]) for r in db.execute('SELECT * FROM observations')}!=expected_observations:raise IntegrityError('observation_index_mismatch')
            if {r[0] for r in db.execute('SELECT body_sha256 FROM blobs')}!=expected_blobs:raise IntegrityError('blob_index_mismatch')
            return {'records':len(records),'versions':len(expected_versions),'observations':len(expected_observations),
                    'confirmed_observations':len(confirmed),'unconfirmed_observations':len(expected_observations)-len(confirmed),
                    'head_hash':previous,'integrity':'verified','active_collection':False,'order_permission':'none'}
