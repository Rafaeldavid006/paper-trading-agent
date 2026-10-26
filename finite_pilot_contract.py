"""Immutable, separately reviewed LOCAL contracts. NO grant is supplied here.

There is deliberately no loader, JSON grant, environment switch or CLI input.
Production code must use the exact compiled object, not an equivalent caller
object. Synthetic tests may replace module constants for disposable cohorts.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import re

NS = 10**9
INCIDENT_ID = 'native_schema_refused:2026-10-20T13:30:37Z'
INSPECTION_GENERATION = hashlib.sha256(b'finite-state-inspection-local-v3-20261021').hexdigest()


class PilotRefused(ValueError):
    """Fixed, non-sensitive failure code."""


def require(ok, code):
    if not ok:
        raise PilotRefused(code)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def _hash(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def _ns(value):
    return type(value) is int and 0 < value < 2**63


def _common(value):
    require(_ns(value.starts_ns) and _ns(value.expires_ns)
            and 0 < value.expires_ns - value.starts_ns <= 3600 * NS,
            'finite_window_required')
    require(all(_hash(getattr(value, k)) for k in
                ('bundle_generation', 'bundle_seal_sha256',
                 'anchor_pre_enrollment_sha256', 'pre_enrollment_cohort_sha256')),
            'manifest_binding_invalid')
    require(type(value.approval_reference) is str
            and re.fullmatch('[A-Za-z0-9_./:@+-]{1,192}', value.approval_reference),
            'approval_reference_invalid')


@dataclass(frozen=True)
class PilotAdmission:
    """Immutable fresh-session binding; authority is only enclosing PilotManifest."""
    successor_document: str
    successor_digest: str
    current_anchor_sha256: str
    current_cohort_sha256: str
    current_runner_sha256: str
    protected_state_sha256: str
    expires_ns: int

    def __post_init__(self):
        require(all(_hash(getattr(self, key)) for key in (
            'successor_digest',
            'current_anchor_sha256','current_cohort_sha256','current_runner_sha256',
            'protected_state_sha256')), 'admission_pins_required')
        require(_ns(self.expires_ns) and type(self.successor_document) is str
                and len(self.successor_document)<=16384, 'admission_reference_required')
        # Keep literal compiled construction import-order independent. Full
        # archived semantic validation is mandatory in require_pilot_manifest
        # after modules initialize, before any state/credential/provider access.
        try:
            doc=json.loads(self.successor_document)
        except (TypeError,ValueError):
            raise PilotRefused('admission_successor_reference_invalid') from None
        require(type(doc) is dict and json.dumps(doc,sort_keys=True,separators=(',',':'),allow_nan=False)==self.successor_document
                and digest(doc)==self.successor_digest, 'admission_successor_reference_invalid')


@dataclass(frozen=True)
class PilotManifest:
    pilot_id: str
    starts_ns: int
    expires_ns: int
    bundle_generation: str
    bundle_seal_sha256: str
    symbols: tuple
    max_ticks: int
    max_write_attempts: int
    max_source_requests: int
    approval_artifact_sha256: str
    approval_reference: str
    cadence_ns: int = 65 * NS
    source_cadence_ns: int = 20 * NS
    max_positions: int = 2
    max_notional_usd: str = '2000'
    max_shares: int = 1
    max_entry_claims: int = 4
    max_loss_usd: str = '100'
    anchor_pre_enrollment_sha256: str = ''
    pre_enrollment_cohort_sha256: str = ''
    admission: PilotAdmission | None = None

    def __post_init__(self):
        _common(self)
        if self.admission is not None:
            require(type(self.admission) is PilotAdmission, 'typed_admission_required')
            self.admission.__post_init__()
            require(self.starts_ns < self.admission.expires_ns <= min(self.expires_ns,self.starts_ns+300*NS)
                    and self.expires_ns-self.starts_ns<=1200*NS, 'admission_window_invalid')
            reference=json.loads(self.admission.successor_document)
            require(all(reference.get(key)==getattr(self,key) for key in (
                'bundle_generation','bundle_seal_sha256','anchor_pre_enrollment_sha256',
                'pre_enrollment_cohort_sha256')) and reference.get('protected_state_sha256')==self.admission.protected_state_sha256,
                'admission_founding_binding_changed')
        require(_hash(self.pre_enrollment_cohort_sha256), 'initial_cohort_digest_required')
        require(type(self.pilot_id) is str and re.fullmatch('[A-Za-z0-9_.:-]{1,96}', self.pilot_id),
                'pilot_id_invalid')
        require(type(self.symbols) is tuple and self.symbols == ('NVDA',), 'nvda_only_required')
        for name, cap in (('max_ticks', 10), ('max_write_attempts', 8), ('max_source_requests', 120)):
            require(type(getattr(self, name)) is int and 1 <= getattr(self, name) <= cap,
                    'finite_cap_invalid')
        require(_hash(self.approval_artifact_sha256), 'approval_artifact_required')
        require(type(self.cadence_ns) is int and self.cadence_ns == 65 * NS
                and type(self.source_cadence_ns) is int and self.source_cadence_ns == 20 * NS,
                'fixed_cadence_required')
        require(type(self.max_positions) is int and self.max_positions == 2
                and self.max_notional_usd == '2000' and type(self.max_shares) is int
                and self.max_shares == 1 and type(self.max_entry_claims) is int
                and self.max_entry_claims == 4 and self.max_loss_usd == '100', 'fixed_risk_required')

    def document(self):
        return asdict(self)


@dataclass(frozen=True)
class RecoveryManifest:
    """Explicit approval to adopt a freshly reconciled current baseline.

    This incident resolution does not establish the historical failure's cause.
    reviewed_fix_sha256 identifies the reviewed current safety/code artifact,
    not evidence that storage or a schema change caused or cured that failure.
    """
    incident_id: str
    starts_ns: int
    expires_ns: int
    bundle_generation: str
    bundle_seal_sha256: str
    original_runner_sha256: str
    protected_state_sha256: str
    diagnostic_artifact_sha256: str
    diagnostic_result_sha256: str
    reviewed_fix_sha256: str
    review_authorization_sha256: str
    approval_reference: str
    anchor_pre_enrollment_sha256: str = ''
    pre_enrollment_cohort_sha256: str = ''
    recovery_basis: str = 'operator_approved_fresh_baseline_adoption'
    historical_cause_unproven: bool = True

    def __post_init__(self):
        _common(self)
        require(type(self.recovery_basis) is str
                and self.recovery_basis == 'operator_approved_fresh_baseline_adoption'
                and self.historical_cause_unproven is True, 'recovery_basis_invalid')
        require(self.incident_id == INCIDENT_ID, 'wrong_recovery_incident')
        require(_hash(self.original_runner_sha256), 'exact_incident_runner_digest_required')
        require(all(_hash(getattr(self, name)) for name in (
            'protected_state_sha256', 'diagnostic_artifact_sha256', 'diagnostic_result_sha256',
            'reviewed_fix_sha256', 'review_authorization_sha256')), 'recovery_artifacts_required')

    def document(self):
        return asdict(self)


@dataclass(frozen=True)
class InspectionManifest:
    inspection_id: str
    starts_ns: int
    expires_ns: int
    bundle_generation: str
    approval_artifact_sha256: str
    approval_reference: str

    def __post_init__(self):
        require(self.inspection_id == INSPECTION_GENERATION, 'inspection_generation_invalid')
        require(_ns(self.starts_ns) and _ns(self.expires_ns)
                and 0 < self.expires_ns - self.starts_ns <= 3600 * NS, 'finite_window_required')
        require(_hash(self.bundle_generation) and _hash(self.approval_artifact_sha256),
                'inspection_binding_invalid')
        require(type(self.approval_reference) is str
                and re.fullmatch('[A-Za-z0-9_./:@+-]{1,192}', self.approval_reference),
                'approval_reference_invalid')

    def document(self):
        return asdict(self)


@dataclass(frozen=True)
class RecoveryEvidence:
    # Not authority. The anchor accepts this only from the reviewed worker
    # validator while holding its own seal/account locks, then verifies deltas.
    observation: object
    started_ns: int
    finished_ns: int
    request_count: int
    legacy_terminal_digests: tuple = ()
    legacy_absence_receipts: tuple = ()


# USER APPROVED EXACT SCOPE. REVIEWED PUBLICATION REQUIRES PARENT GO.
APPROVED_PILOT = PilotManifest(
    pilot_id='finite-paper-nvda-admission-20261026T171000Z',
    starts_ns=1793034600000000000,
    expires_ns=1793035800000000000,
    bundle_generation='ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668',
    bundle_seal_sha256='4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6',
    symbols=('NVDA',),
    max_ticks=10,
    max_write_attempts=8,
    max_source_requests=120,
    approval_artifact_sha256='ae403e4451accb661b2ec4a3bb5f2d3278eb56a5543ecdd913629c5a703cfd37',
    approval_reference='Sentinel_a8e455ea53e4819197c66b17c640d491-redated-20261026T171000Z',
    anchor_pre_enrollment_sha256='5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b',
    pre_enrollment_cohort_sha256='8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753',
    admission=PilotAdmission(
        successor_document='{"anchor_pre_enrollment_sha256":"5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b","approval_reference":"Sentinel_e82bb87119ac8191a0fb9d63f1be4a1a","bundle_generation":"ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668","bundle_seal_sha256":"4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6","current_anchor_sha256":"ba3e72ce37ff7dcb08ed6c4c3770e946687dfad22a0608893b68f268372623bd","diagnostic_artifact_sha256":"bd1ccf86179aeb6e6fb24cc2777d8075688dab0a61b52a87e5156eff61e6920b","diagnostic_result_sha256":"dc13d9f33b3ff33e65806a9bf4249d6c6c5db72abf3ea2d8b44e0aa865d43ba9","expires_ns":1793028300000000000,"historical_cause_unproven":true,"incident_id":"native_schema_refused:2026-10-20T13:30:37Z","original_runner_sha256":"745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f","pre_enrollment_cohort_sha256":"8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753","predecessor_digest":"02f3bf75c43620512b56e4eaa2b7e3ff46a3dd9e1ffa4de42b11cd0162f42433","predecessor_document":"{\\"anchor_pre_enrollment_sha256\\":\\"5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b\\",\\"approval_reference\\":\\"paper-recovery-pilot-20261026T133600Z-135600Z\\",\\"bundle_generation\\":\\"ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668\\",\\"bundle_seal_sha256\\":\\"4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6\\",\\"diagnostic_artifact_sha256\\":\\"bc144b008e1253ede852167cecb15ab0d154c41d133906a934117522c95df2d8\\",\\"diagnostic_result_sha256\\":\\"9a82ac11335f74bf9796c3b43301f19e56f4b2b1ee66903aa9cd69b00f522ed2\\",\\"expires_ns\\":1793022060000000000,\\"historical_cause_unproven\\":true,\\"incident_id\\":\\"native_schema_refused:2026-10-20T13:30:37Z\\",\\"original_runner_sha256\\":\\"745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f\\",\\"pre_enrollment_cohort_sha256\\":\\"8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753\\",\\"protected_state_sha256\\":\\"64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282\\",\\"recovery_basis\\":\\"operator_approved_fresh_baseline_adoption\\",\\"review_authorization_sha256\\":\\"abae85848dcf7a428999a72053e4711b70367e5e864b8a9adfbef5a3ab5cfd92\\",\\"reviewed_fix_sha256\\":\\"5da292a1f9342d81feab409fda1285e199c372b5f6763cbb6872533e111ce370\\",\\"starts_ns\\":1793021760000000000}","protected_state_sha256":"64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282","recovery_basis":"operator_approved_fresh_baseline_adoption","review_authorization_sha256":"fe0b3a2443cf42ca9c2a7d3388872138bacbb5a5ede6d4cc6f2ef0057b24dae9","reviewed_fix_sha256":"6b49ae7d6e03d4730f3444fda42c87680e70fa2033e39dfb284f8e1aebefd70d","scope":"one_shot_recovery_only","starts_ns":1793028000000000000,"successor_id":"finite-recovery-successor-20261026T152000Z"}',
        successor_digest='f2a9f148b3a32736a34208617524255232c10e443048dae4bdf2d6e976456b47',
        current_anchor_sha256='0bdf19b6a4b7aac6fa0a57649af150250264daefceae061cf311bc988f4ac8cb',
        current_cohort_sha256='5d00d2d32e842fd43388fbb888c307563753b727b2bc7180b604cc8d2466cd64',
        current_runner_sha256='85bcf056cb0f4d62af76ded5c2a6fc55683e8b82d36a5f3fe7227633d9bebba1',
        protected_state_sha256='64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282',
        expires_ns=1793034900000000000,
    ),
)
APPROVED_RECOVERY = None
APPROVED_INSPECTION = None


def require_pilot_manifest(manifest):
    require(type(APPROVED_PILOT) is PilotManifest and manifest is APPROVED_PILOT,
            'pilot_not_approved')
    manifest.__post_init__()
    if manifest.admission is not None:
        from finite_recovery_successor_contract import APPROVED_SUCCESSOR,SuccessorManifest
        try:
            SuccessorManifest(**json.loads(manifest.admission.successor_document))
        except (TypeError,ValueError):
            raise PilotRefused('admission_successor_reference_invalid') from None
        require(APPROVED_RECOVERY is None and APPROVED_INSPECTION is None and APPROVED_SUCCESSOR is None,
                'admission_exclusive_pilot_required')
    return manifest


def require_recovery_manifest(manifest):
    require(type(APPROVED_RECOVERY) is RecoveryManifest and manifest is APPROVED_RECOVERY,
            'recovery_not_approved')
    manifest.__post_init__()
    return manifest


def require_inspection_manifest(manifest):
    require(type(APPROVED_INSPECTION) is InspectionManifest and manifest is APPROVED_INSPECTION,
            'inspection_not_approved')
    manifest.__post_init__()
    return manifest
