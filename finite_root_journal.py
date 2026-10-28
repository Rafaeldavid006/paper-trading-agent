"""One-root append journal; v1 history remains immutable. No schema auto-provision."""
from contextlib import contextmanager
import json,math
import finite_pilot_contract as c
import finite_root_contract as rc
from finite_pilot_contract import require,digest,NS
from finite_pilot_anchor import AnchorJournal,_bounded,_json,_schema
from finite_recovery_state_report import _anchor_digest

def clean_runner(row):
    require(type(row) in (tuple,list) and len(row)==6 and type(row[0]) is int and row[0]==1
        and type(row[1]) in (int,float) and math.isfinite(row[1]) and row[1]>0
        and type(row[2]) is int and type(row[3]) is int and tuple(row[2:5])==(0,0,None)
        and row[5] in ('recovered_readonly','complete'),'root_clean_runner_required')
    return tuple(row)

def append(db,sid,kind,at,payload):
    g=db.execute('SELECT COALESCE(MAX(global_seq),0)+1 FROM '+rc.EVENTS).fetchone()[0]
    q=db.execute('SELECT COALESCE(MAX(session_seq),0)+1 FROM '+rc.EVENTS+' WHERE session_id=?',(sid,)).fetchone()[0]
    db.execute('INSERT INTO '+rc.EVENTS+' VALUES(?,?,?,?,?,?)',(g,sid,q,kind,at,_json(payload)))

def load(db,root):
    rc.validate_reference(root);_schema(db)
    require(_bounded(db,'SELECT * FROM '+rc.ROOTS,limit=1)==[(root.root_id,digest(root.document()),_json(root.document()))],'root_registry_changed')
    require(_anchor_digest(db,project_root_legacy=True)==root.current_anchor_sha256,'root_legacy_history_changed')
    sessions=_bounded(db,'SELECT * FROM '+rc.SESSIONS+' ORDER BY child_index',limit=2)
    require(bool(sessions) and len(sessions)<=2,'root_sessions_missing')
    rows={};docs={}
    for i,(sid,rid,index,doc) in enumerate(sessions):
        m=root.children[i]
        require(type(index) is int and index==i and rid==root.root_id and sid==m.pilot_id and doc==_json(m.document()),'root_child_document_changed')
        docs[sid]=m;rows[sid]=dict(id=sid,ticks=0,writes=0,sources=0,broker=0,admission_gets=0,dirty=0,last_ns=0,last_tick_ns=0,last_source_ns=0,stage='',seq=0,event_seq=0,prepared=None,applied=None,smoke_entry=None,halted=False,ended=None,write_keys=[],write_rows=[],broker_rows=[])
    events=_bounded(db,'SELECT * FROM '+rc.EVENTS+' ORDER BY global_seq',limit=2000)
    require([v[0] for v in events]==list(range(1,len(events)+1)),'root_global_sequence')
    total=dict(ticks=0,writes=0,sources=0,broker=0);active=None;global_time=0;imported=False
    for g,sid,q,kind,at,raw in events:
        require(sid in rows and type(q) is int and type(at) is int and root.starts_ns<=at<root.expires_ns and at>=global_time,'root_event_invalid')
        global_time=at;m=docs[sid];r=rows[sid];a=m.root_binding
        require(q==r['event_seq']+1 and at>=r['last_ns'],'root_child_sequence');r['event_seq']=q;r['last_ns']=at
        p=json.loads(raw);require(type(p) is dict and _json(p)==raw,'root_event_payload')
        if kind=='ROOT_IMPORTED':
            require(g==1 and q==1 and a.index==0 and not imported and p==dict(predecessor=root.predecessor_terminal_sha256,anchor=root.current_anchor_sha256),'root_import_invalid');imported=True
        elif kind=='PREPARED':
            require(imported and active is None and r['prepared'] is None and set(p)=={'old','new','cohort','protected','guard','history','predecessor','anchor'},'root_prepared_invalid')
            require(m.starts_ns<=at<a.expires_ns and all(c._hash(p[k]) for k in ('cohort','protected','guard','history','predecessor','anchor')),'root_prepared_invalid')
            old=clean_runner(p['old']);clean_runner(p['new']);require(type(p['new'][1]) is float and p['new']==[old[0],at/NS,*old[2:]] and old[1]<=at/NS,'root_prepared_cas_invalid')
            if a.index==0:
                require(p['anchor']==root.current_anchor_sha256 and p['cohort']==root.current_cohort_sha256 and p['protected']==root.protected_state_sha256 and digest(old)==root.current_runner_sha256 and p['predecessor']==root.predecessor_terminal_sha256,'root_initial_pins_changed')
            else:
                prev=rows[root.children[0].pilot_id]['ended'];require(prev is not None and p['cohort']==prev['cohort'] and p['protected']==prev['protected'] and digest(old)==prev['runner'] and p['history']==prev['history'] and p['predecessor']==digest(prev),'root_predecessor_not_ended')
            r['prepared']=p;active=sid
        elif kind=='APPLIED':
            require(active==sid and r['prepared'] is not None and r['applied'] is None and not r['halted'] and m.starts_ns<=at<a.expires_ns,'root_applied_invalid')
            require(set(p)=={'prepared_digest','evidence','guard','history','observed_ns'} and p['prepared_digest']==digest(r['prepared']) and p['history']==r['prepared']['history'] and all(c._hash(p[k]) for k in ('evidence','guard','history')) and type(p['observed_ns']) is int and m.starts_ns<=p['observed_ns']<=at and at-p['observed_ns']<=10*NS and 1<=r['admission_gets']<=12,'root_applied_evidence_invalid');r['applied']=p
        elif kind=='HALT':
            require(active==sid and r['prepared'] is not None and not r['halted'] and p=={} and at<a.close_expires_ns,'root_halt_invalid');r['halted']=True
        elif kind=='LOCAL_ENDED_STRUCTURED':
            require(active==sid and r['halted'] and r['applied'] is not None and not r['dirty'] and r['ended'] is None and m.expires_ns<=at<a.close_expires_ns,'root_end_invalid')
            require(set(p)=={'cohort','runner','protected','history','local_only'} and p['local_only'] is True and all(c._hash(p[k]) for k in ('cohort','runner','protected','history')),'root_end_payload');r['ended']=p;active=None
        elif kind=='SMOKE_ENTRY':
            require(root.smoke is not None and active==sid and a.index==0 and r['applied'] is not None and not r['halted'] and r['dirty']==1 and r['stage']=='tick' and m.starts_ns<=at<m.expires_ns,'smoke_entry_inactive')
            expected='smoke-buy-'+digest(root.document())[:32]
            require(not any(rr['smoke_entry'] for rr in rows.values()) and type(p.get('stage_seq')) is int and p==dict(cid=expected,stage_seq=r['seq']),'smoke_entry_consumed')
            r['smoke_entry']=p
        elif kind=='BROKER':
            require(active==sid and r['prepared'] is not None and not r['halted'] and m.starts_ns<=at<m.expires_ns and set(p)=={'method','target','stage_seq','admission'} and type(p['stage_seq']) is int and type(p['admission']) is bool and p['method'] in ('GET','POST','DELETE') and type(p['target']) is str and 0<len(p['target'])<=512,'root_broker_invalid')
            if p['admission']:
                require(r['applied'] is None and p['method']=='GET' and p['stage_seq']==0 and at<a.expires_ns,'root_admission_broker_invalid');r['admission_gets']+=1;require(r['admission_gets']<=12,'root_admission_get_cap')
            else:
                require(r['applied'] is not None and r['dirty']==1 and r['stage']=='tick' and p['stage_seq']==r['seq'],'root_broker_stage_invalid')
                if p['method']!='GET':require(r['write_rows'] and r['write_rows'][-1]==(at,p['method'],p['target'],r['seq']),'root_write_broker_unmatched')
            r['broker']+=1;total['broker']+=1;r['broker_rows'].append((at,p))
        else:
            require(active==sid and r['applied'] is not None and not r['halted'] and m.starts_ns<=at<m.expires_ns,'root_stage_inactive')
            if kind=='STAGE_BEGIN':
                require(set(p)=={'stage'} and p['stage'] in ('source','tick') and not r['dirty'] and (root.smoke is None or p['stage']=='tick'),'root_stage_invalid')
                stage=p['stage'];last=r['last_'+stage+'_ns'];cad=m.source_cadence_ns if stage=='source' else m.cadence_ns
                require(not last or at-last>=cad,'pilot_cadence_wait');r['last_'+stage+'_ns']=at;r['dirty']=1;r['stage']=stage;r['seq']+=1
                if stage=='tick':r['ticks']+=1;total['ticks']+=1
            elif kind=='STAGE_END':
                require(p=={'stage_seq':r['seq']} and type(p['stage_seq']) is int and r['dirty']==1,'root_stage_end_invalid');r['dirty']=0;r['stage']=''
            elif kind=='SOURCE':
                require(p=={'stage_seq':r['seq']} and type(p['stage_seq']) is int and r['dirty']==1 and r['stage']=='source','root_source_invalid');r['sources']+=1;total['sources']+=1
            elif kind=='WRITE':
                require(set(p)=={'stage_seq','method','target'} and type(p['stage_seq']) is int and p['stage_seq']==r['seq'] and r['dirty']==1 and r['stage']=='tick' and p['method'] in ('POST','DELETE') and type(p['target']) is str and 0<len(p['target'])<=512,'root_write_invalid')
                if root.smoke is not None:
                    require(a.index==0 and r['smoke_entry'] is not None,'smoke_write_without_entry_authority')
                    if p['method']=='POST':require(p['target'] in ('smoke-buy-'+digest(root.document())[:32],'smoke-sell-'+digest(root.document())[:32]),'smoke_write_identity')
                key=(p['method'],p['target']);require(key not in [k for rr in rows.values() for k in rr['write_keys']],'root_write_replay');r['write_keys'].append(key);r['write_rows'].append((at,*key,r['seq']));r['writes']+=1;total['writes']+=1
            else:require(False,'root_event_kind_invalid')
        require(r['ticks']<=m.max_ticks and r['writes']<=m.max_write_attempts and r['sources']<=m.max_source_requests,'root_child_cap')
        require(total['ticks']<=root.max_ticks and total['writes']<=root.max_writes and total['sources']<=root.max_sources and total['broker']<=root.max_broker,'root_aggregate_cap')
    require(imported and all(r['prepared'] is not None for r in rows.values()),'root_orphan_session')
    # Each durable write requires the matching final-wire broker reservation.
    for r in rows.values():
        for at,method,target,seq in r['write_rows']:
            require(sum(p['method']==method and p['target']==target and p['stage_seq']==seq and t==at for t,p in r['broker_rows'])==1,'root_write_reservation_missing')
    return docs,rows,total,events

class RootJournal(AnchorJournal):
    def _row(self,db,manifest):
        self._binding_row(db,manifest);root=rc.require_root(rc.APPROVED_ROOT);rc.require_child(manifest)
        docs,rows,counts,_=load(db,root);require(manifest.pilot_id in docs,'root_child_not_enrolled')
        row=dict(rows[manifest.pilot_id]);row['authority_counts']=counts
        require(row['applied'] is not None and not row['halted'] and row['ended'] is None,'root_child_inactive');return row
    @staticmethod
    def _exhausted(row,manifest):
        root=rc.APPROVED_ROOT;t=row['authority_counts']
        if root.smoke is not None:return row['ticks']>=manifest.max_ticks or t['ticks']>=root.max_ticks or t['broker']>=root.max_broker
        return AnchorJournal._exhausted(row,manifest) or t['ticks']>=root.max_ticks or t['writes']>=root.max_writes or t['sources']>=root.max_sources or t['broker']>=root.max_broker
    def claim_smoke_entry(self,m):
        rc.smoke_scope(m);self.check_stage(m,'tick')
        with self._db(write=True) as db:
            r=self._row(db,m);append(db,m.pilot_id,'SMOKE_ENTRY',self._now(m,r['last_ns']),dict(cid=rc.smoke_cid('buy'),stage_seq=r['seq']));self._row(db,m)
    def smoke_entry_claimed(self,m):
        rc.smoke_scope(m)
        with self._db() as db:
            _,rows,_,_=load(db,rc.APPROVED_ROOT);return any(r['smoke_entry'] is not None for r in rows.values())
    def begin_stage(self,m,stage):
        self.startup_gate(m);require(stage in ('source','tick'),'root_stage_invalid')
        with self._db(write=True) as db:
            row=self._row(db,m);at=self._now(m,row['last_ns']);append(db,m.pilot_id,'STAGE_BEGIN',at,dict(stage=stage));self._row(db,m)
        return self.check_stage(m,stage)
    def finish_stage(self,m,stage):
        self.check_stage(m,stage);self._controls()
        with self._db(write=True) as db:
            row=self._row(db,m);append(db,m.pilot_id,'STAGE_END',self._now(m,row['last_ns']),dict(stage_seq=row['seq']));row=self._row(db,m)
        row['budget_exhausted']=self._exhausted(row,m);return row
    def reserve_source_request(self,m):
        self.check_stage(m,'source')
        with self._db(write=True) as db:
            row=self._row(db,m);require(not self._exhausted(row,m),'root_source_cap');append(db,m.pilot_id,'SOURCE',self._now(m,row['last_ns']),dict(stage_seq=row['seq']));self._row(db,m)
        self.check_stage(m,'source');return True
    def reserve_broker_request(self,m,method,target,*,admission=False):
        rc.require_child(m);require(method=='GET','root_get_reservation_required')
        self._verify(m)
        with self._db(write=True) as db:
            self._binding_row(db,m);_,rows,total,_=load(db,rc.APPROVED_ROOT);r=rows[m.pilot_id]
            at=self._now(m,r['last_ns']);require(total['broker']<rc.APPROVED_ROOT.max_broker,'root_broker_cap')
            append(db,m.pilot_id,'BROKER',at,dict(method=method,target=target,stage_seq=0 if admission else r['seq'],admission=admission));load(db,rc.APPROVED_ROOT)
    @contextmanager
    def write_attempt(self,m,method,target):
        self.check_stage(m,'tick')
        with self._db(write=True) as db:
            r=self._row(db,m);root=rc.APPROVED_ROOT
            require(r['writes']<m.max_write_attempts and r['authority_counts']['writes']<root.max_writes and r['authority_counts']['broker']<root.max_broker,'root_write_cap')
            at=self._now(m,r['last_ns']);append(db,m.pilot_id,'WRITE',at,dict(stage_seq=r['seq'],method=method,target=target))
            append(db,m.pilot_id,'BROKER',at,dict(method=method,target=target,stage_seq=r['seq'],admission=False));self._row(db,m)
        self.check_stage(m,'tick')
        with self._db(write=True) as db:
            r=self._row(db,m);require(r['dirty']==1 and r['stage']=='tick','root_stage_inactive');self._now(m,r['last_ns']);yield
