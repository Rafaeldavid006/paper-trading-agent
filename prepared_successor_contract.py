"""A new, one-shot read-only admission. Never a reset, execution grant or renewal."""
from dataclasses import dataclass, asdict
from pathlib import Path
import hashlib, json, re
from finite_pilot_contract import require, digest, NS, _hash, _ns

JOURNAL_NAME = 'finite-prepared-successor.sqlite3'
JOURNAL_PATH = '/var/data/' + JOURNAL_NAME

@dataclass(frozen=True)
class AdmissionGrant:
    admission_id: str
    starts_ns: int
    expires_ns: int
    predecessor_document: str
    anchor_sha256: str
    runner_sha256: str
    cohort_sha256: str
    protected_sha256: str
    seal_sha256: str
    reviewed_code_sha256: str
    approval_reference: str
    max_gets: int = 12
    max_writes: int = 0
    max_ticks: int = 0
    max_sources: int = 0
    session_mode: str = 'regular'
    def __post_init__(self):
        require(type(self.admission_id) is str and re.fullmatch('[A-Za-z0-9_.:-]{1,96}',self.admission_id),'successor_id_invalid')
        require(_ns(self.starts_ns) and _ns(self.expires_ns) and 0 < self.expires_ns-self.starts_ns <= 300*NS,'successor_window_invalid')
        require(all(_hash(getattr(self,k)) for k in ('anchor_sha256','runner_sha256','cohort_sha256','protected_sha256','seal_sha256','reviewed_code_sha256')),'successor_pins_invalid')
        require(type(self.approval_reference) is str and re.fullmatch('[A-Za-z0-9_./:@+-]{1,192}',self.approval_reference),'successor_approval_invalid')
        require(type(self.predecessor_document) is str and len(self.predecessor_document)<=131072,'successor_predecessor_invalid')
        from finite_root_contract import from_document, validate_reference
        old=from_document(self.predecessor_document);validate_reference(old)
        require(json.dumps(old.document(),sort_keys=True,separators=(',',':'))==self.predecessor_document,'successor_predecessor_invalid')
        require(old.expires_ns<=self.starts_ns and old.root_id!=self.admission_id,'successor_predecessor_not_expired')
        require(all(type(getattr(self,k)) is int and getattr(self,k)==v for k,v in (('max_gets',12),('max_writes',0),('max_ticks',0),('max_sources',0))) and self.session_mode=='regular','successor_readonly_fixed_scope')
    def document(self): return asdict(self)

def code_digest():
    # Activation is intentionally outside its own source hash. Exact compiled
    # object identity authorizes it; every runtime source file remains pinned.
    root=Path(__file__).resolve().parent
    files=sorted(p for p in root.glob('*.py') if not p.name.startswith('test_') and p.name!='prepared_successor_activation.py')
    require(1<=len(files)<=256,'successor_code_inventory')
    return digest([(p.name,hashlib.sha256(p.read_bytes()).hexdigest()) for p in files])

def approved(grant):
    import prepared_successor_activation as a
    import finite_pilot_contract as pc
    import finite_root_contract as rc
    import finite_recovery_successor_contract as sc
    import finite_observation_contract as oc
    require(type(grant) is AdmissionGrant and grant is a.APPROVED_ADMISSION,'successor_not_approved')
    grant.__post_init__()
    require(all(x is None for x in (pc.APPROVED_PILOT,pc.APPROVED_RECOVERY,pc.APPROVED_INSPECTION,rc.APPROVED_ROOT,sc.APPROVED_SUCCESSOR,oc.APPROVED_OBSERVATION)),'successor_exclusive_required')
    require(code_digest()==grant.reviewed_code_sha256,'successor_code_changed')
    return grant

def window(grant,at,previous=0):
    approved(grant)
    require(type(at) is int and grant.starts_ns<=at<grant.expires_ns and at>=previous,'successor_expired_or_regressed')
    return at
