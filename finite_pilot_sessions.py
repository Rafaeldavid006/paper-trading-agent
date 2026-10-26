"""Append-only bounded sessions; legacy evidence is never updated or reset."""
from contextlib import contextmanager
import json,math,re
import finite_pilot_contract as c
from finite_pilot_contract import require,digest,NS
from finite_pilot_anchor import AnchorJournal,_bounded,_json,SCHEMA as LEGACY_SCHEMA
from finite_pilot_session_schema import SESSIONS,EVENTS,SCHEMA


def archived_manifest(document):
    raw=json.loads(document) if type(document) is str else dict(document)
    require(type(raw) is dict,'session_manifest_invalid')
    if raw.get('admission') is not None:raw['admission']=c.PilotAdmission(**raw['admission'])
    if raw.get('session_admission') is not None:raw['session_admission']=c.SessionAdmission(**raw['session_admission'])
    raw['symbols']=tuple(raw['symbols'])
    return c.PilotManifest(**raw)


def validate_session_reference(manifest):
    a=manifest.session_admission
    require(type(a) is c.SessionAdmission and manifest.admission is None,'typed_session_required')
    a.__post_init__()
    root=archived_manifest(a.root_document)
    require(root.session_admission is None and root.admission is not None
        and root.pilot_id!=manifest.pilot_id and digest(root.document())==a.authority_id,
        'session_root_invalid')
    from finite_recovery_successor_contract import SuccessorManifest,APPROVED_SUCCESSOR
    SuccessorManifest(**json.loads(root.admission.successor_document))
    require(c.APPROVED_RECOVERY is None and c.APPROVED_INSPECTION is None and APPROVED_SUCCESSOR is None,
        'session_exclusive_required')
    require(all(getattr(root,k)==getattr(manifest,k) for k in (
        'bundle_generation','bundle_seal_sha256','anchor_pre_enrollment_sha256','pre_enrollment_cohort_sha256',
        'symbols','cadence_ns','source_cadence_ns','max_positions','max_notional_usd','max_shares','max_entry_claims','max_loss_usd')),
        'session_root_binding_changed')
    require(root.expires_ns<=manifest.starts_ns,'session_predecessor_window_active')
    return root


def legacy_rows_digest(db):
    # All old finite tables including recovery/successor/admission: explicit rows
    # and exact SQL, never a wildcard permission to introduce unknown objects.
    objects=db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name LIKE 'finite_pilot_%' AND name NOT IN (?,?) ORDER BY type,name",(SESSIONS,EVENTS)).fetchall()
    result=[]
    for kind,name,table,sql in objects:
        require(kind=='table' and re.fullmatch('[a-z0-9_]+',name),'session_legacy_schema_invalid')
        rows=_bounded(db,'SELECT * FROM '+name,limit=1000)
        result.append((kind,name,table,sql,sorted(rows,key=_json)))
    return digest(result)


def legacy_counts(db,root):
    rows=_bounded(db,'SELECT * FROM finite_pilot_enrollment',limit=1)
    require(rows==[(1,digest(root.document()),_json(root.document()))],'session_root_enrollment_changed')
    control=_bounded(db,'SELECT * FROM finite_pilot_control',limit=1)
    require(len(control)==1,'session_root_control_invalid')
    r=control[0]
    require(len(r)==10 and r[0]==1 and r[4]==0 and r[8]==''
        and all(type(r[i]) is int and r[i]>=0 for i in (1,2,3,4,5,6,7,9)), 'session_predecessor_unclean')
    stages=_bounded(db,'SELECT * FROM finite_pilot_stages ORDER BY seq',limit=200)
    sources=_bounded(db,'SELECT * FROM finite_pilot_sources ORDER BY seq',limit=120)
    writes=_bounded(db,'SELECT * FROM finite_pilot_writes',limit=8)
    require(not writes and r[2]==0 and r[1]==sum(x[1]=='tick' for x in stages)
        and r[3]==len(sources) and r[9]==len(stages)
        and [x[0] for x in stages]==list(range(1,len(stages)+1)),'session_legacy_counters_invalid')
    last={'source':0,'tick':0}
    for seq,stage,at,end in stages:
        require(stage in last and type(at) is int and type(end) is int
            and root.starts_ns<=at<=end<root.expires_ns and end<=r[5], 'session_predecessor_unclean')
        cadence=root.source_cadence_ns if stage=='source' else root.cadence_ns
        require(not last[stage] or at-last[stage]>=cadence,'session_legacy_cadence_invalid');last[stage]=at
    require((r[6],r[7])==(last['tick'],last['source']),'session_legacy_counters_invalid')
    for seq,at,stage_seq in sources:
        require(type(seq) is int and 1<=seq<=len(sources) and type(stage_seq) is int and 1<=stage_seq<=len(stages)
            and stages[stage_seq-1][1]=='source' and stages[stage_seq-1][2]<=at<=stages[stage_seq-1][3],
            'session_legacy_sources_invalid')
    require([x[0] for x in sources]==list(range(1,len(sources)+1)),'session_legacy_sources_invalid')
    return dict(ticks=r[1],writes=r[2],sources=r[3])


def _load(db,manifest):
    root=validate_session_reference(manifest);a=manifest.session_admission
    sessions=_bounded(db,'SELECT * FROM '+SESSIONS+' ORDER BY session_id',limit=16)
    events=_bounded(db,'SELECT * FROM '+EVENTS+' ORDER BY global_seq',limit=4000)
    require([x[0] for x in events]==list(range(1,len(events)+1)),'session_global_sequence_invalid')
    docs={};rows={};allcounts=legacy_counts(db,root)
    for sid,authority,dh,document in sessions:
        m=archived_manifest(document)
        require(sid==m.pilot_id and authority==a.authority_id and digest(m.document())==dh
            and _json(m.document())==document,'session_manifest_changed')
        if sid==root.pilot_id:require(document==a.root_document,'session_root_changed')
        else:
            validate_session_reference(m)
            require(m.session_admission.authority_id==a.authority_id
,'session_authority_changed')
        docs[sid]=m
        rows[sid]=dict(id=sid,ticks=0,writes=0,sources=0,dirty=0,last_ns=0,last_tick_ns=0,last_source_ns=0,stage='',seq=0,
            event_seq=0,prepared=None,applied=None,halted=False,closed=False,clean=False,terminal_digest=None,closed_cohort=None,closure_evidence=None,write_keys=[])
    require(root.pilot_id in docs,'session_root_missing')
    descendants=[m for sid,m in docs.items() if sid!=root.pilot_id]
    require(bool(descendants),'session_descendant_missing')
    first=min(descendants,key=lambda m:m.starts_ns)
    from finite_recovery_state_report import session_legacy_anchor_digest
    require(session_legacy_anchor_digest(db)==first.session_admission.current_anchor_sha256,
        'session_legacy_changed')

    last_global_ns=0
    for g,sid,seq,kind,at,payload in events:
        require(type(at) is int and at>=last_global_ns,'session_global_clock_regression')
        last_global_ns=at
        require(sid in docs and type(seq) is int and type(at) is int,'session_event_invalid')
        m=docs[sid];r=rows[sid];p=json.loads(payload)
        require(type(p) is dict and _json(p)==payload and len(payload)<=16384 and seq==r['event_seq']+1
            and at>=r['last_ns'],'session_event_invalid')
        r['event_seq']=seq;r['last_ns']=at
        if sid==root.pilot_id:
            require(seq==1 and kind=='LEGACY_CLOSED' and at>=root.expires_ns
                and set(p)=={'counts','legacy_rows','evidence','terminal'}
                and p['counts']==allcounts and p['legacy_rows']==legacy_rows_digest(db)
                and c._hash(p['evidence']) and c._hash(p['terminal']),'session_root_closure_invalid')
            r.update(closed=True,clean=True,terminal_digest=p['terminal'],closure_evidence=p['evidence']);continue
        close_exp=m.session_admission.close_expires_ns
        require(m.starts_ns<=at<(close_exp if kind in ('HALT','CLOSE') else m.expires_ns),'session_event_window_invalid')
        require(not r['closed'],'session_event_after_close')
        if kind=='PREPARED':
            require(seq==1 and set(p)=={'old','new','guard','protected','predecessor','evidence'}
                and at<m.session_admission.expires_ns and all(c._hash(p[k]) for k in ('guard','protected','predecessor','evidence')),
                'session_prepared_invalid')
            from finite_pilot_session_admission import _session_clean_runner
            _session_clean_runner(p['old']);_session_clean_runner(p['new'])
            require(type(p['old']) is list and type(p['new']) is list and type(p['new'][1]) is float,'session_prepared_invalid')
            require(p['predecessor']==m.session_admission.predecessor_digest
                and p['protected']==m.session_admission.protected_state_sha256
                and p['evidence']==m.session_admission.closure_evidence_sha256
                and p['new'][1]>=p['old'][1],'session_prepared_invalid')
            predecessors=[name for name,other in docs.items() if digest(other.document())==p['predecessor']]
            require(len(predecessors)==1 and rows[predecessors[0]]['closed']
                and rows[predecessors[0]]['clean']
                and rows[predecessors[0]]['terminal_digest']==m.session_admission.predecessor_terminal_digest
                and not any(x['prepared'] is not None and not x['closed'] for name,x in rows.items() if name!=sid),
                'session_predecessor_not_closed')
            prior=rows[predecessors[0]]
            require((predecessors[0]==root.pilot_id and prior['closure_evidence']==m.session_admission.closure_evidence_sha256)
                or (predecessors[0]!=root.pilot_id and prior['closed_cohort']==m.session_admission.current_cohort_sha256),
                'session_predecessor_state_changed')
            require(digest(p['old'])==m.session_admission.current_runner_sha256
                and p['new']==[p['old'][0],at/NS,*p['old'][2:]],'session_prepared_invalid')
            r['prepared']=p
        elif kind=='APPLIED':
            require(seq==2 and r['prepared'] is not None and set(p)=={'prepared_digest','guard','evidence'}
                and at<m.session_admission.expires_ns and p['prepared_digest']==digest(r['prepared'])
                and c._hash(p['guard']) and c._hash(p['evidence']),'session_applied_invalid');r['applied']=p
        elif kind=='HALT':
            require(r['prepared'] is not None and not r['halted'] and set(p)=={'reason'}
                and p['reason']=='supervisor_terminal','session_halt_invalid');r['halted']=True
        elif kind=='CLOSE':
            require(at>=m.expires_ns and set(p)=={'clean','local_state'} and type(p['clean']) is bool
                and c._hash(p['local_state']),'session_close_invalid')
            require(not p['clean'] or r['applied'] is not None and not r['dirty'] and r['writes']==0,
                'session_close_invalid')
            r.update(closed=True,clean=p['clean'],closed_cohort=p['local_state'],terminal_digest=digest((g,sid,seq,kind,at,payload)))
        else:
            require(r['applied'] is not None and not r['halted'],'session_not_active')
            if kind=='STAGE_BEGIN':
                require(set(p)=={'stage'} and p['stage'] in ('source','tick') and not r['dirty'],'session_stage_invalid')
                stage=p['stage'];last=r['last_'+stage+'_ns'];cad=m.source_cadence_ns if stage=='source' else m.cadence_ns
                require(not last or at-last>=cad,'pilot_cadence_wait')
                r['seq']+=1;r.update(dirty=1,stage=stage);r['last_'+stage+'_ns']=at
                if stage=='tick':r['ticks']+=1;allcounts['ticks']+=1
            elif kind=='STAGE_END':
                require(set(p)=={'stage_seq'} and type(p['stage_seq']) is int and r['dirty'] and p['stage_seq']==r['seq'],'session_stage_invalid')
                r.update(dirty=0,stage='')
            elif kind=='SOURCE':
                require(set(p)=={'stage_seq'} and type(p['stage_seq']) is int and r['dirty'] and r['stage']=='source' and p['stage_seq']==r['seq'],'session_source_invalid')
                r['sources']+=1;allcounts['sources']+=1
            elif kind=='WRITE':
                require(set(p)=={'stage_seq','method','target'} and type(p['stage_seq']) is int and r['dirty'] and r['stage']=='tick' and p['stage_seq']==r['seq']
                    and p['method'] in ('POST','DELETE') and type(p['target']) is str
                    and re.fullmatch(r'[A-Za-z0-9_./:-]{1,192}',p['target']),'session_write_invalid')
                key=(p['method'],p['target']);require(key not in r['write_keys'],'pilot_write_replay');r['write_keys'].append(key)
                r['writes']+=1;allcounts['writes']+=1
            else:require(False,'session_event_kind_invalid')
        require(r['ticks']<=m.max_ticks and r['writes']<=m.max_write_attempts and r['sources']<=m.max_source_requests,
            'session_budget_corrupt')
        require(allcounts['ticks']<=root.max_ticks and allcounts['writes']<=root.max_write_attempts
            and allcounts['sources']<=root.max_source_requests,'session_authority_budget_exhausted')
    active=[sid for sid,r in rows.items() if sid!=root.pilot_id and not r['closed']]
    require(len(active)<=1,'session_multiple_active')
    # Every successor references the preceding immutable closure, not just any old clean row.
    ordered=sorted((sid for sid in docs if sid!=root.pilot_id),key=lambda sid:next(x[0] for x in events if x[1]==sid))
    previous=root.pilot_id
    for sid in ordered:
        a2=docs[sid].session_admission;prev=rows[previous]
        require(prev['closed'] and prev['clean'] and a2.predecessor_digest==digest(docs[previous].document())
            and a2.predecessor_terminal_digest==prev['terminal_digest'] and docs[previous].expires_ns<=docs[sid].starts_ns,
            'session_predecessor_not_closed')
        previous=sid
    return docs,rows,allcounts,events


def _append(db,sid,kind,at,payload):
    g=db.execute('SELECT COALESCE(MAX(global_seq),0)+1 FROM '+EVENTS).fetchone()[0]
    seq=db.execute('SELECT COALESCE(MAX(session_seq),0)+1 FROM '+EVENTS+' WHERE session_id=?',(sid,)).fetchone()[0]
    db.execute('INSERT INTO '+EVENTS+' VALUES(?,?,?,?,?,?)',(g,sid,seq,kind,at,_json(payload)))


class SessionJournal(AnchorJournal):
    def _row(self,db,manifest):
        self._binding_row(db,manifest)
        docs,rows,counts,_=_load(db,manifest)
        require(manifest.pilot_id in docs and docs[manifest.pilot_id].document()==manifest.document(),'session_not_enrolled')
        row=dict(rows[manifest.pilot_id]);row['authority_counts']=counts
        require(row['applied'] is not None and not row['halted'] and not row['closed'],'session_not_active')
        return row

    @staticmethod
    def _exhausted(row,manifest):
        root=validate_session_reference(manifest);a=row['authority_counts']
        return AnchorJournal._exhausted(row,manifest) or a['ticks']>=root.max_ticks or a['writes']>=root.max_write_attempts or a['sources']>=root.max_source_requests

    def begin_stage(self,manifest,stage):
        self.startup_gate(manifest);require(stage in ('source','tick'),'pilot_stage_invalid')
        with self._db(write=True) as db:
            row=self._row(db,manifest);now=self._now(manifest,row['last_ns'])
            last=row['last_'+stage+'_ns'];cad=manifest.source_cadence_ns if stage=='source' else manifest.cadence_ns
            require(not last or now-last>=cad,'pilot_cadence_wait')
            _append(db,manifest.pilot_id,'STAGE_BEGIN',now,dict(stage=stage));self._row(db,manifest)
        return self.check_stage(manifest,stage)

    def finish_stage(self,manifest,stage):
        self.check_stage(manifest,stage);self._controls()
        with self._db(write=True) as db:
            row=self._row(db,manifest);now=self._now(manifest,row['last_ns'])
            _append(db,manifest.pilot_id,'STAGE_END',now,dict(stage_seq=row['seq']));result=self._row(db,manifest)
        result['budget_exhausted']=self._exhausted(result,manifest);return result

    def reserve_source_request(self,manifest):
        self.check_stage(manifest,'source')
        with self._db(write=True) as db:
            row=self._row(db,manifest);require(not self._exhausted(row,manifest),'pilot_source_cap')
            _append(db,manifest.pilot_id,'SOURCE',self._now(manifest,row['last_ns']),dict(stage_seq=row['seq']));self._row(db,manifest)
        self.check_stage(manifest,'source')
        return True

    @contextmanager
    def write_attempt(self,manifest,method,target):
        self.check_stage(manifest,'tick')
        with self._db(write=True) as db:
            row=self._row(db,manifest);root=validate_session_reference(manifest)
            require(row['writes']<manifest.max_write_attempts and row['authority_counts']['writes']<root.max_write_attempts,'pilot_write_cap')
            _append(db,manifest.pilot_id,'WRITE',self._now(manifest,row['last_ns']),dict(stage_seq=row['seq'],method=method,target=target));self._row(db,manifest)
        self.check_stage(manifest,'tick')
        with self._db(write=True) as db:
            row=self._row(db,manifest)
            require(row['dirty']==1 and row['stage']=='tick','pilot_stage_not_active')
            self._now(manifest,row['last_ns']);yield
