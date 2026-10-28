"""Separate, default-disabled GET observation authority; never trading authority."""
from dataclasses import dataclass, asdict
import hashlib,json,re
NS=10**9
JOURNAL='/var/data/finite-observation.sqlite3'
class ObservationRefused(ValueError):pass
def require(ok,code):
    if not ok:raise ObservationRefused(code)
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def hash_ok(v):return type(v) is str and re.fullmatch('[0-9a-f]{64}',v) is not None
@dataclass(frozen=True)
class ObservationGrant:
    observation_id:str
    starts_ns:int
    expires_ns:int
    seal_sha256:str
    anchor_sha256:str
    cohort_sha256:str
    protected_sha256:str
    runner_sha256:str
    predecessor_sha256:str
    terminal_sha256:str
    allow_create_journal:bool
    max_cycles:int=40
    max_gets_per_cycle:int=12
    max_gets:int=480
    cadence_ns:int=65*NS
    def __post_init__(self):
        require(type(self.observation_id) is str and re.fullmatch('[A-Za-z0-9_-]{1,96}',self.observation_id),'invalid_id')
        require(all(type(v) is int and 0<v<2**63 for v in (self.starts_ns,self.expires_ns)), 'invalid_window')
        require(0<self.expires_ns-self.starts_ns<=3600*NS,'invalid_window')
        require(all(hash_ok(getattr(self,k)) for k in ('seal_sha256','anchor_sha256','cohort_sha256','protected_sha256','runner_sha256','predecessor_sha256','terminal_sha256')),'invalid_pin')
        require(type(self.allow_create_journal) is bool and self.allow_create_journal,'explicit_provision_required')
        require(type(self.max_cycles) is int and 1<=self.max_cycles<=40 and type(self.max_gets_per_cycle) is int and 1<=self.max_gets_per_cycle<=12 and type(self.max_gets) is int and 1<=self.max_gets<=480 and self.max_gets<=self.max_cycles*self.max_gets_per_cycle,'invalid_caps')
        require(type(self.cadence_ns) is int and self.cadence_ns>=65*NS,'invalid_cadence')
    def document(self):return asdict(self)
APPROVED_OBSERVATION:ObservationGrant|None=None

def approved(grant):
    require(type(grant) is ObservationGrant and grant is APPROVED_OBSERVATION,'observation_disabled')
    grant.__post_init__()
    return grant

def window(grant,now,previous=0):
    approved(grant)
    require(type(now) is int and grant.starts_ns<=now<grant.expires_ns and now>=previous,'observation_window_closed')
    return now
