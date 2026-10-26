"""Disabled, exact one-shot successor authority. No loader/environment grants."""
from dataclasses import dataclass
import json
import re
import finite_pilot_contract as original
from finite_pilot_contract import require, digest, NS

TABLE = 'finite_pilot_successor_journal'
SQL = ('CREATE TABLE finite_pilot_successor_journal('
       'seq INTEGER PRIMARY KEY CHECK(seq IN (1,2)),status TEXT NOT NULL,at_ns INTEGER NOT NULL,'
       'manifest_digest TEXT NOT NULL,document TEXT NOT NULL,predecessor_digest TEXT NOT NULL,'
       'old_row TEXT NOT NULL,new_row TEXT NOT NULL,protected_digest TEXT NOT NULL,'
       'guard_digest TEXT NOT NULL,evidence_digest TEXT)')

@dataclass(frozen=True)
class SuccessorManifest(original.RecoveryManifest):
    predecessor_document: str = ''
    predecessor_digest: str = ''
    current_anchor_sha256: str = ''
    scope: str = 'one_shot_recovery_only'
    successor_id: str = ''

    def __post_init__(self):
        super().__post_init__()
        require(type(self.successor_id) is str and re.fullmatch('[A-Za-z0-9_.:-]{1,96}',self.successor_id),
                'successor_id_required')
        require(self.expires_ns - self.starts_ns <= 300 * NS, 'successor_window_invalid')
        require(self.scope == 'one_shot_recovery_only', 'successor_scope_invalid')
        require(original._hash(self.predecessor_digest) and original._hash(self.current_anchor_sha256),
                'successor_predecessor_required')
        require(type(self.predecessor_document) is str and len(self.predecessor_document) <= 8192,
                'successor_predecessor_required')
        try:
            document = json.loads(self.predecessor_document)
            predecessor = original.RecoveryManifest(**document)
        except (ValueError, TypeError):
            raise original.PilotRefused('successor_predecessor_invalid') from None
        require(json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False)
                == self.predecessor_document and digest(document) == self.predecessor_digest,
                'successor_predecessor_invalid')
        require(predecessor.expires_ns <= self.starts_ns, 'successor_predecessor_not_expired')
        require(all(getattr(predecessor, key) == getattr(self, key) for key in
                    ('incident_id','bundle_generation','bundle_seal_sha256','original_runner_sha256',
                     'protected_state_sha256','anchor_pre_enrollment_sha256','pre_enrollment_cohort_sha256')),
                'successor_historical_binding_changed')

# Exact user-authorized1520UTC overlay; publication requires final reviewed parent GO.
APPROVED_SUCCESSOR = SuccessorManifest(
    anchor_pre_enrollment_sha256='5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b',
    approval_reference='Sentinel_e82bb87119ac8191a0fb9d63f1be4a1a',
    bundle_generation='ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668',
    bundle_seal_sha256='4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6',
    current_anchor_sha256='ba3e72ce37ff7dcb08ed6c4c3770e946687dfad22a0608893b68f268372623bd',
    diagnostic_artifact_sha256='bd1ccf86179aeb6e6fb24cc2777d8075688dab0a61b52a87e5156eff61e6920b',
    diagnostic_result_sha256='dc13d9f33b3ff33e65806a9bf4249d6c6c5db72abf3ea2d8b44e0aa865d43ba9',
    expires_ns=1793028300000000000,
    historical_cause_unproven=True,
    incident_id='native_schema_refused:2026-10-20T13:30:37Z',
    original_runner_sha256='745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f',
    pre_enrollment_cohort_sha256='8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753',
    predecessor_digest='02f3bf75c43620512b56e4eaa2b7e3ff46a3dd9e1ffa4de42b11cd0162f42433',
    predecessor_document='{"anchor_pre_enrollment_sha256":"5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b","approval_reference":"paper-recovery-pilot-20261026T133600Z-135600Z","bundle_generation":"ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668","bundle_seal_sha256":"4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6","diagnostic_artifact_sha256":"bc144b008e1253ede852167cecb15ab0d154c41d133906a934117522c95df2d8","diagnostic_result_sha256":"9a82ac11335f74bf9796c3b43301f19e56f4b2b1ee66903aa9cd69b00f522ed2","expires_ns":1793022060000000000,"historical_cause_unproven":true,"incident_id":"native_schema_refused:2026-10-20T13:30:37Z","original_runner_sha256":"745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f","pre_enrollment_cohort_sha256":"8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753","protected_state_sha256":"64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282","recovery_basis":"operator_approved_fresh_baseline_adoption","review_authorization_sha256":"abae85848dcf7a428999a72053e4711b70367e5e864b8a9adfbef5a3ab5cfd92","reviewed_fix_sha256":"5da292a1f9342d81feab409fda1285e199c372b5f6763cbb6872533e111ce370","starts_ns":1793021760000000000}',
    protected_state_sha256='64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282',
    recovery_basis='operator_approved_fresh_baseline_adoption',
    review_authorization_sha256='fe0b3a2443cf42ca9c2a7d3388872138bacbb5a5ede6d4cc6f2ef0057b24dae9',
    reviewed_fix_sha256='6b49ae7d6e03d4730f3444fda42c87680e70fa2033e39dfb284f8e1aebefd70d',
    scope='one_shot_recovery_only',
    starts_ns=1793028000000000000,
    successor_id='finite-recovery-successor-20261026T152000Z',
)

def require_successor_manifest(manifest):
    require(type(APPROVED_SUCCESSOR) is SuccessorManifest and manifest is APPROVED_SUCCESSOR,
            'successor_not_approved')
    require(original.APPROVED_RECOVERY is None and original.APPROVED_PILOT is None
            and original.APPROVED_INSPECTION is None, 'successor_other_grant_present')
    manifest.__post_init__()
    return manifest
