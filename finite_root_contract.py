"""One explicit new root/two children; default disabled, no automatic root renewal."""
from dataclasses import dataclass
import json,re
import finite_pilot_contract as c
from finite_pilot_contract import require,digest,NS
ROOTS='finite_pilot_roots_v2';SESSIONS='finite_pilot_children_v2';EVENTS='finite_pilot_events_v2'
SCHEMA={
 ROOTS:'CREATE TABLE '+ROOTS+'(root_id TEXT PRIMARY KEY,digest TEXT NOT NULL UNIQUE,document TEXT NOT NULL)',
 SESSIONS:'CREATE TABLE '+SESSIONS+'(session_id TEXT PRIMARY KEY,root_id TEXT NOT NULL,child_index INTEGER NOT NULL,document TEXT NOT NULL,UNIQUE(root_id,child_index))',
 EVENTS:'CREATE TABLE '+EVENTS+'(global_seq INTEGER PRIMARY KEY,session_id TEXT NOT NULL,session_seq INTEGER NOT NULL,kind TEXT NOT NULL,at_ns INTEGER NOT NULL,payload TEXT NOT NULL,UNIQUE(session_id,session_seq))',
}
@dataclass(frozen=True)
class ExecutionSmokeScope:
    mode:str='execution_smoke_not_event_research'
    symbol:str='NVDA'
    session_mode:str='regular'
    max_entry_claims:int=1
    max_positions:int=1
    max_shares:int=1
    max_notional_usd:str='1000'
    max_writes:int=4
    cancel_after_ns:int=65*NS
    def __post_init__(self):
        require(type(self.mode) is str and self.mode=='execution_smoke_not_event_research' and type(self.symbol) is str and self.symbol=='NVDA' and type(self.session_mode) is str and self.session_mode in ('regular','iex_premarket')
            and all(type(getattr(self,k)) is int and getattr(self,k)==v for k,v in
                (('max_entry_claims',1),('max_positions',1),('max_shares',1),('max_writes',4),('cancel_after_ns',65*NS)))
            and self.max_notional_usd=='1000','smoke_fixed_scope_required')

@dataclass(frozen=True)
class RootGrant:
    root_id:str
    starts_ns:int
    expires_ns:int
    children:tuple
    predecessor_document:str
    predecessor_terminal_sha256:str
    current_anchor_sha256:str
    current_cohort_sha256:str
    current_runner_sha256:str
    protected_state_sha256:str
    reviewed_code_sha256:str
    approval_reference:str
    max_ticks:int=10
    max_writes:int=8
    max_sources:int=120
    max_broker:int=144
    smoke:ExecutionSmokeScope|None=None
    def __post_init__(self):
        require(type(self.root_id) is str and re.fullmatch('[A-Za-z0-9_.:-]{1,96}',self.root_id),'root_id_invalid')
        require(c._ns(self.starts_ns) and c._ns(self.expires_ns) and 0<self.expires_ns-self.starts_ns<=2400*NS,'root_window_invalid')
        require(all(c._hash(getattr(self,k)) for k in ('predecessor_terminal_sha256','current_anchor_sha256','current_cohort_sha256','current_runner_sha256','protected_state_sha256','reviewed_code_sha256')),'root_pins_invalid')
        require(type(self.approval_reference) is str and re.fullmatch('[A-Za-z0-9_./:@+-]{1,192}',self.approval_reference),'root_approval_invalid')
        require(type(self.children) is tuple and len(self.children)==2,'two_children_required')
        for i,m in enumerate(self.children):
            require(type(m) is c.PilotManifest,'typed_child_required');m.__post_init__();a=m.root_binding
            require(a is not None and a.root_id==self.root_id and a.index==i and self.starts_ns<=m.starts_ns<m.expires_ns<a.close_expires_ns<=self.expires_ns,'root_child_mismatch')
            require(m.max_ticks<=(8 if i==0 else 2),'root_child_tick_allocation')
            if i:
                before=self.children[0]
                require(before.pilot_id!=m.pilot_id and before.root_binding.close_expires_ns<=m.starts_ns,'root_child_overlap')
                require(all(getattr(before,k)==getattr(m,k) for k in ('bundle_generation','bundle_seal_sha256','anchor_pre_enrollment_sha256','pre_enrollment_cohort_sha256','symbols','max_positions','max_notional_usd','max_shares','max_entry_claims','max_loss_usd')),'root_child_binding_changed')
        require(type(self.predecessor_document) is str and len(self.predecessor_document)<=65536,'root_predecessor_invalid')
        d=json.loads(self.predecessor_document);require(type(d) is dict and json.dumps(d,sort_keys=True,separators=(',',':'))==self.predecessor_document,'root_predecessor_invalid')
        if self.smoke is not None:
            require(type(self.smoke) is ExecutionSmokeScope,'typed_smoke_required');self.smoke.__post_init__()
            require(self.max_sources==0 and self.max_writes==4,'smoke_root_caps_required')
        for key,cap in (('max_ticks',10),('max_writes',8),('max_sources',120),('max_broker',144)):
            require(type(getattr(self,key)) is int and (0 if key=='max_sources' and self.smoke is not None else 1)<=getattr(self,key)<=cap,'root_cap_invalid')
    def document(self):
        return {k:([m.document() for m in self.children] if k=='children' else dict(self.smoke.__dict__) if k=='smoke' else getattr(self,k)) for k in self.__dataclass_fields__ if k!='smoke' or self.smoke is not None}

def from_document(value):
    raw=json.loads(value) if type(value) is str else dict(value)
    if raw.get('smoke') is not None:raw['smoke']=ExecutionSmokeScope(**raw['smoke'])
    children=[]
    for item in raw['children']:
        item=dict(item);item['symbols']=tuple(item['symbols']);item['root_binding']=c.RootChild(**item['root_binding']);children.append(c.PilotManifest(**item))
    raw['children']=tuple(children);return RootGrant(**raw)

APPROVED_ROOT:RootGrant|None=from_document({'root_id': 'paper-execution-smoke-iex-premarket-20261029T120100Z', 'starts_ns': 1793275260000000000, 'expires_ns': 1793277660000000000, 'children': [{'pilot_id': 'paper-execution-smoke-iex-premarket-20261029T120100Z-child-0', 'starts_ns': 1793275260000000000, 'expires_ns': 1793276450000000000, 'bundle_generation': 'ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668', 'bundle_seal_sha256': '4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6', 'symbols': ('NVDA',), 'max_ticks': 8, 'max_write_attempts': 4, 'max_source_requests': 117, 'approval_artifact_sha256': '460ee50aeb80d1823ffa7bbe42ecdf7174a025bba0633bc9f25fe722a31b4fd0', 'approval_reference': 'Sentinel_9fe6992649e08191b71adc595ea67409', 'cadence_ns': 65000000000, 'source_cadence_ns': 20000000000, 'max_positions': 2, 'max_notional_usd': '2000', 'max_shares': 1, 'max_entry_claims': 4, 'max_loss_usd': '100', 'anchor_pre_enrollment_sha256': '5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b', 'pre_enrollment_cohort_sha256': '8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753', 'admission': None, 'root_binding': {'root_id': 'paper-execution-smoke-iex-premarket-20261029T120100Z', 'index': 0, 'expires_ns': 1793275560000000000, 'close_expires_ns': 1793276460000000000}}, {'pilot_id': 'paper-execution-smoke-iex-premarket-20261029T120100Z-child-1', 'starts_ns': 1793276460000000000, 'expires_ns': 1793277650000000000, 'bundle_generation': 'ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668', 'bundle_seal_sha256': '4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6', 'symbols': ('NVDA',), 'max_ticks': 2, 'max_write_attempts': 4, 'max_source_requests': 117, 'approval_artifact_sha256': '460ee50aeb80d1823ffa7bbe42ecdf7174a025bba0633bc9f25fe722a31b4fd0', 'approval_reference': 'Sentinel_9fe6992649e08191b71adc595ea67409', 'cadence_ns': 65000000000, 'source_cadence_ns': 20000000000, 'max_positions': 2, 'max_notional_usd': '2000', 'max_shares': 1, 'max_entry_claims': 4, 'max_loss_usd': '100', 'anchor_pre_enrollment_sha256': '5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b', 'pre_enrollment_cohort_sha256': '8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753', 'admission': None, 'root_binding': {'root_id': 'paper-execution-smoke-iex-premarket-20261029T120100Z', 'index': 1, 'expires_ns': 1793276760000000000, 'close_expires_ns': 1793277660000000000}}], 'predecessor_document': '{"admission":null,"anchor_pre_enrollment_sha256":"5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b","approval_artifact_sha256":"ba032940a897361d3046ba2563295e8fd4747117d4daf52f31269c4ca847b080","approval_reference":"proposed-reusable-paper-session-20261026T181000Z","bundle_generation":"ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668","bundle_seal_sha256":"4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6","cadence_ns":65000000000,"expires_ns":1793039400000000000,"max_entry_claims":4,"max_loss_usd":"100","max_notional_usd":"2000","max_positions":2,"max_shares":1,"max_source_requests":117,"max_ticks":10,"max_write_attempts":8,"pilot_id":"finite-paper-session-nvda-20261026T181000Z","pre_enrollment_cohort_sha256":"8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753","session_admission":{"authority_id":"b90a6f7dcc6ff8c9483ef4afdcaa363083a0329ee4ebbddcd03b494dac214edc","close_expires_ns":1793039460000000000,"closure_evidence_sha256":"7d78b31d2caf0557fb8626275a4d0330156d4cb27578c2d0753cb0f1e35ed445","current_anchor_sha256":"1ac18739cb0db7d51a445aa9c628bd3e7759cd6020c4bf305fdcbd847aae9bb2","current_cohort_sha256":"e3540434ac4961bb1b87c597f8586beda5a83bfbf700ec301747850af4161d87","current_runner_sha256":"28035941273d28871861606606571f4e75d9ac22340fdaa47cfcd9c4f347d3fd","expires_ns":1793038500000000000,"predecessor_digest":"b90a6f7dcc6ff8c9483ef4afdcaa363083a0329ee4ebbddcd03b494dac214edc","predecessor_terminal_digest":"3a90b4331cb5f4332b9ceda2ebab734407226988e7a072ff880da5ff722ff8e2","protected_state_sha256":"baf670cbe91376b004f20eb61984ef88701722cf04ca40a3053637e631b84e45","root_document":"{\\"admission\\":{\\"current_anchor_sha256\\":\\"0bdf19b6a4b7aac6fa0a57649af150250264daefceae061cf311bc988f4ac8cb\\",\\"current_cohort_sha256\\":\\"5d00d2d32e842fd43388fbb888c307563753b727b2bc7180b604cc8d2466cd64\\",\\"current_runner_sha256\\":\\"85bcf056cb0f4d62af76ded5c2a6fc55683e8b82d36a5f3fe7227633d9bebba1\\",\\"expires_ns\\":1793034900000000000,\\"protected_state_sha256\\":\\"64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282\\",\\"successor_digest\\":\\"f2a9f148b3a32736a34208617524255232c10e443048dae4bdf2d6e976456b47\\",\\"successor_document\\":\\"{\\\\\\"anchor_pre_enrollment_sha256\\\\\\":\\\\\\"5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b\\\\\\",\\\\\\"approval_reference\\\\\\":\\\\\\"Sentinel_e82bb87119ac8191a0fb9d63f1be4a1a\\\\\\",\\\\\\"bundle_generation\\\\\\":\\\\\\"ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668\\\\\\",\\\\\\"bundle_seal_sha256\\\\\\":\\\\\\"4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6\\\\\\",\\\\\\"current_anchor_sha256\\\\\\":\\\\\\"ba3e72ce37ff7dcb08ed6c4c3770e946687dfad22a0608893b68f268372623bd\\\\\\",\\\\\\"diagnostic_artifact_sha256\\\\\\":\\\\\\"bd1ccf86179aeb6e6fb24cc2777d8075688dab0a61b52a87e5156eff61e6920b\\\\\\",\\\\\\"diagnostic_result_sha256\\\\\\":\\\\\\"dc13d9f33b3ff33e65806a9bf4249d6c6c5db72abf3ea2d8b44e0aa865d43ba9\\\\\\",\\\\\\"expires_ns\\\\\\":1793028300000000000,\\\\\\"historical_cause_unproven\\\\\\":true,\\\\\\"incident_id\\\\\\":\\\\\\"native_schema_refused:2026-10-20T13:30:37Z\\\\\\",\\\\\\"original_runner_sha256\\\\\\":\\\\\\"745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f\\\\\\",\\\\\\"pre_enrollment_cohort_sha256\\\\\\":\\\\\\"8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753\\\\\\",\\\\\\"predecessor_digest\\\\\\":\\\\\\"02f3bf75c43620512b56e4eaa2b7e3ff46a3dd9e1ffa4de42b11cd0162f42433\\\\\\",\\\\\\"predecessor_document\\\\\\":\\\\\\"{\\\\\\\\\\\\\\"anchor_pre_enrollment_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"approval_reference\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"paper-recovery-pilot-20261026T133600Z-135600Z\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"bundle_generation\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"bundle_seal_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"diagnostic_artifact_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"bc144b008e1253ede852167cecb15ab0d154c41d133906a934117522c95df2d8\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"diagnostic_result_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"9a82ac11335f74bf9796c3b43301f19e56f4b2b1ee66903aa9cd69b00f522ed2\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"expires_ns\\\\\\\\\\\\\\":1793022060000000000,\\\\\\\\\\\\\\"historical_cause_unproven\\\\\\\\\\\\\\":true,\\\\\\\\\\\\\\"incident_id\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"native_schema_refused:2026-10-20T13:30:37Z\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"original_runner_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"pre_enrollment_cohort_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"protected_state_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"recovery_basis\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"operator_approved_fresh_baseline_adoption\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"review_authorization_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"abae85848dcf7a428999a72053e4711b70367e5e864b8a9adfbef5a3ab5cfd92\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"reviewed_fix_sha256\\\\\\\\\\\\\\":\\\\\\\\\\\\\\"5da292a1f9342d81feab409fda1285e199c372b5f6763cbb6872533e111ce370\\\\\\\\\\\\\\",\\\\\\\\\\\\\\"starts_ns\\\\\\\\\\\\\\":1793021760000000000}\\\\\\",\\\\\\"protected_state_sha256\\\\\\":\\\\\\"64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282\\\\\\",\\\\\\"recovery_basis\\\\\\":\\\\\\"operator_approved_fresh_baseline_adoption\\\\\\",\\\\\\"review_authorization_sha256\\\\\\":\\\\\\"fe0b3a2443cf42ca9c2a7d3388872138bacbb5a5ede6d4cc6f2ef0057b24dae9\\\\\\",\\\\\\"reviewed_fix_sha256\\\\\\":\\\\\\"6b49ae7d6e03d4730f3444fda42c87680e70fa2033e39dfb284f8e1aebefd70d\\\\\\",\\\\\\"scope\\\\\\":\\\\\\"one_shot_recovery_only\\\\\\",\\\\\\"starts_ns\\\\\\":1793028000000000000,\\\\\\"successor_id\\\\\\":\\\\\\"finite-recovery-successor-20261026T152000Z\\\\\\"}\\"},\\"anchor_pre_enrollment_sha256\\":\\"5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b\\",\\"approval_artifact_sha256\\":\\"ae403e4451accb661b2ec4a3bb5f2d3278eb56a5543ecdd913629c5a703cfd37\\",\\"approval_reference\\":\\"Sentinel_a8e455ea53e4819197c66b17c640d491-redated-20261026T171000Z\\",\\"bundle_generation\\":\\"ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668\\",\\"bundle_seal_sha256\\":\\"4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6\\",\\"cadence_ns\\":65000000000,\\"expires_ns\\":1793035800000000000,\\"max_entry_claims\\":4,\\"max_loss_usd\\":\\"100\\",\\"max_notional_usd\\":\\"2000\\",\\"max_positions\\":2,\\"max_shares\\":1,\\"max_source_requests\\":120,\\"max_ticks\\":10,\\"max_write_attempts\\":8,\\"pilot_id\\":\\"finite-paper-nvda-admission-20261026T171000Z\\",\\"pre_enrollment_cohort_sha256\\":\\"8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753\\",\\"source_cadence_ns\\":20000000000,\\"starts_ns\\":1793034600000000000,\\"symbols\\":[\\"NVDA\\"]}"},"source_cadence_ns":20000000000,"starts_ns":1793038200000000000,"symbols":["NVDA"]}', 'predecessor_terminal_sha256': '6d5c6318a49ffb1e442ac5ff5a68e0a5c52999c7ef008ff7dc3b53e19fc7950f', 'current_anchor_sha256': '9edd3afb37f832ffdaf791a4735b5a9ec25886eaa38084dcb039c267528dfda6', 'current_cohort_sha256': 'ab6ce28c5f253d02b8bfb7caef427c9ea47c1a6933eacadd717948ef1e1a023e', 'current_runner_sha256': '7c7e6312a4fe7c00d9255d009ccaa3ade56820a1ea9c1ad2457e55b919b4401e', 'protected_state_sha256': 'ec23cf4b7a453d95014838b9a89f33b46eab1ee629a6b13cebdebd0eebb55169', 'reviewed_code_sha256': '460ee50aeb80d1823ffa7bbe42ecdf7174a025bba0633bc9f25fe722a31b4fd0', 'approval_reference': 'Sentinel_9fe6992649e08191b71adc595ea67409', 'max_ticks': 10, 'max_writes': 4, 'max_sources': 0, 'max_broker': 144, 'smoke': {'mode': 'execution_smoke_not_event_research', 'symbol': 'NVDA', 'session_mode': 'iex_premarket', 'max_entry_claims': 1, 'max_positions': 1, 'max_shares': 1, 'max_notional_usd': '1000', 'max_writes': 4, 'cancel_after_ns': 65000000000}})

def validate_reference(root):
    require(type(root) is RootGrant,'typed_root_required');root.__post_init__()
    from finite_pilot_sessions import archived_manifest
    old=archived_manifest(root.predecessor_document)
    require(old.session_admission is not None and old.root_binding is None and old.expires_ns<=root.starts_ns,'v1_closed_predecessor_required')
    for m in root.children:
        require(all(getattr(m,k)==getattr(old,k) for k in ('bundle_generation','bundle_seal_sha256','anchor_pre_enrollment_sha256','pre_enrollment_cohort_sha256','symbols','max_positions','max_notional_usd','max_shares','max_entry_claims','max_loss_usd')),'root_founding_binding_changed')
    return old

def require_root(root):
    require(type(root) is RootGrant and root is APPROVED_ROOT,'root_not_approved')
    from finite_recovery_successor_contract import APPROVED_SUCCESSOR
    from finite_observation_contract import APPROVED_OBSERVATION
    require(c.APPROVED_PILOT is None and c.APPROVED_RECOVERY is None and c.APPROVED_INSPECTION is None and APPROVED_SUCCESSOR is None and APPROVED_OBSERVATION is None,'root_exclusive_required')
    validate_reference(root);return root

def child(index):
    root=require_root(APPROVED_ROOT);require(type(index) is int and index in (0,1),'root_child_invalid');return root.children[index]
def require_child(manifest):
    root=require_root(APPROVED_ROOT)
    require(any(manifest is m for m in root.children),'exact_root_child_required');return manifest

def smoke_scope(manifest):
    require_child(manifest);root=require_root(APPROVED_ROOT)
    require(type(root.smoke) is ExecutionSmokeScope,'smoke_not_approved');root.smoke.__post_init__();return root.smoke

def smoke_cid(side):
    root=require_root(APPROVED_ROOT);require(side in ('buy','sell') and root.smoke is not None,'smoke_identity_required')
    return 'smoke-'+side+'-'+digest(root.document())[:32]

def smoke_execution_policy(manifest):
    scope=smoke_scope(manifest)
    if scope.session_mode=='regular':return None
    from execution_session_policy import policy_for_root
    return policy_for_root()

# Recovery development artifact: no historical or new trading activation.
APPROVED_ROOT = None
