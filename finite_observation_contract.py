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
APPROVED_OBSERVATION:ObservationGrant|None=ObservationGrant(**{'observation_id': 'observation-nvda-20261027T133600Z', 'starts_ns': 1793108160000000000, 'expires_ns': 1793111760000000000, 'seal_sha256': '4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6', 'anchor_sha256': '9edd3afb37f832ffdaf791a4735b5a9ec25886eaa38084dcb039c267528dfda6', 'cohort_sha256': '327f3585c7c501bcc8890c98fc0c8d2efcc2f80da59a7517ece82967c3f1ea34', 'protected_sha256': 'ec23cf4b7a453d95014838b9a89f33b46eab1ee629a6b13cebdebd0eebb55169', 'runner_sha256': '7c7e6312a4fe7c00d9255d009ccaa3ade56820a1ea9c1ad2457e55b919b4401e', 'predecessor_sha256': '060ad011f16ac1d1f9482fbba234636ff4ccb917cd293f85f8185baffbe303c6', 'terminal_sha256': '6d5c6318a49ffb1e442ac5ff5a68e0a5c52999c7ef008ff7dc3b53e19fc7950f', 'allow_create_journal': True, 'max_cycles': 40, 'max_gets_per_cycle': 12, 'max_gets': 480, 'cadence_ns': 65000000000})

def approved(grant):
    require(type(grant) is ObservationGrant and grant is APPROVED_OBSERVATION,'observation_disabled')
    grant.__post_init__()
    return grant

def window(grant,now,previous=0):
    approved(grant)
    require(type(now) is int and grant.starts_ns<=now<grant.expires_ns and now>=previous,'observation_window_closed')
    return now
