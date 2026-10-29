"""Exact, isolated finite PAPER successor. No reused root or environment authority."""
from dataclasses import dataclass, asdict
from pathlib import Path
import hashlib,json,re
from finite_pilot_contract import require,digest,NS,_hash,_ns
from prepared_successor_contract import AdmissionGrant
JOURNAL_NAME='finite-prepared-execution.sqlite3'
JOURNAL_PATH='/var/data/'+JOURNAL_NAME
@dataclass(frozen=True)
class ExecutionGrant:
    execution_id:str
    starts_ns:int
    admission_expires_ns:int
    expires_ns:int
    admission_document:str
    admission_journal_sha256:str
    current_cohort_sha256:str
    reviewed_code_sha256:str
    approval_reference:str
    max_ticks:int
    max_broker:int
    max_writes:int=4
    max_sources:int=0
    cadence_ns:int=65*NS
    symbol:str='NVDA'
    session_mode:str='regular'
    max_entry_claims:int=1
    max_positions:int=1
    max_shares:int=1
    max_notional_usd:str='1000'
    max_loss_usd:str='100'
    cancel_after_ns:int=65*NS
    def __post_init__(self):
        require(type(self.execution_id) is str and re.fullmatch('[A-Za-z0-9_.:-]{1,96}',self.execution_id),'execution_id_invalid')
        require(all(_ns(getattr(self,k)) for k in ('starts_ns','admission_expires_ns','expires_ns')) and self.starts_ns<self.admission_expires_ns<=self.expires_ns and self.admission_expires_ns-self.starts_ns<=300*NS and self.expires_ns-self.starts_ns<=900*NS,'execution_window_invalid')
        require(all(_hash(getattr(self,k)) for k in ('admission_journal_sha256','current_cohort_sha256','reviewed_code_sha256')),'execution_pins_invalid')
        require(type(self.approval_reference) is str and re.fullmatch('[A-Za-z0-9_./:@+-]{1,192}',self.approval_reference),'execution_approval_invalid')
        old=admission(self)
        from finite_root_contract import from_document
        root=from_document(old.predecessor_document)
        require(self.execution_id not in (old.admission_id,root.root_id) and old.starts_ns<=self.starts_ns and root.smoke is not None,'execution_predecessor_invalid')
        require(type(self.max_ticks) is int and 3<=self.max_ticks<=root.max_ticks and type(self.max_broker) is int and 30<=self.max_broker<=root.max_broker,'execution_caps_invalid')
        require(all(type(getattr(self,k)) is int and getattr(self,k)==v for k,v in (('max_writes',4),('max_sources',0),('cadence_ns',65*NS),('max_entry_claims',1),('max_positions',1),('max_shares',1),('cancel_after_ns',65*NS))) and self.max_writes<=root.max_writes and self.symbol=='NVDA' and self.session_mode=='regular' and self.max_notional_usd=='1000' and self.max_loss_usd=='100','execution_fixed_scope')
    def document(self):return asdict(self)
def admission(grant):
    require(type(grant.admission_document) is str and len(grant.admission_document)<=150000,'execution_admission_invalid')
    raw=json.loads(grant.admission_document);require(type(raw) is dict and json.dumps(raw,sort_keys=True,separators=(',',':'))==grant.admission_document,'execution_admission_invalid')
    return AdmissionGrant(**raw)
def code_digest():
    files=sorted(p for p in Path(__file__).resolve().parent.glob('*.py') if not p.name.startswith('test_') and p.name!='prepared_execution_activation.py')
    require(1<=len(files)<=256,'execution_source_inventory')
    return digest([(p.name,hashlib.sha256(p.read_bytes()).hexdigest()) for p in files])
def approved(grant):
    import prepared_execution_activation as a,prepared_successor_activation as sa
    import finite_pilot_contract as pc,finite_root_contract as rc,finite_recovery_successor_contract as sc,finite_observation_contract as oc
    require(type(grant) is ExecutionGrant and grant is a.APPROVED_EXECUTION,'execution_not_approved');grant.__post_init__()
    require(all(x is None for x in (sa.APPROVED_ADMISSION,pc.APPROVED_PILOT,pc.APPROVED_RECOVERY,pc.APPROVED_INSPECTION,rc.APPROVED_ROOT,sc.APPROVED_SUCCESSOR,oc.APPROVED_OBSERVATION)),'execution_exclusive_required')
    require(code_digest()==grant.reviewed_code_sha256,'execution_source_changed');return grant
def window(grant,at,previous=0,*,admitting=False):
    approved(grant)
    require(type(at) is int and grant.starts_ns<=at<(grant.admission_expires_ns if admitting else grant.expires_ns) and at>=previous,'execution_expired_or_regressed');return at
def cid(grant,side):
    approved(grant);require(side in ('buy','sell'),'execution_side_invalid')
    return 'successor-'+side+'-'+digest(grant.document())[:32]
