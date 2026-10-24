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

    def __post_init__(self):
        _common(self)
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


APPROVED_PILOT = PilotManifest(
    pilot_id='finite-paper-nvda-20261026T133600Z',
    starts_ns=1793021760000000000,
    bundle_generation='ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668',
    bundle_seal_sha256='4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6',
    anchor_pre_enrollment_sha256='5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b',
    pre_enrollment_cohort_sha256='8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753',
    approval_reference='paper-recovery-pilot-20261026T133600Z-135600Z',
    expires_ns=1793022960000000000,
    symbols=('NVDA',),
    max_ticks=10,
    max_write_attempts=8,
    max_source_requests=120,
    approval_artifact_sha256='abae85848dcf7a428999a72053e4711b70367e5e864b8a9adfbef5a3ab5cfd92',
)

APPROVED_RECOVERY = RecoveryManifest(
    incident_id=INCIDENT_ID,
    starts_ns=1793021760000000000,
    bundle_generation='ec8ce6d2e16684e597b5756fe26a1e529adae2fb554fc5548b4ab38f3194e668',
    bundle_seal_sha256='4baf35d188353e7cb4ddcfbc37f9cd2951dddf1bd15489fc3b997db062046ba6',
    anchor_pre_enrollment_sha256='5f3cb268d21565caa6a62532fa4f4b70003b7f76d3e5af2578cfa9c43ba46b0b',
    pre_enrollment_cohort_sha256='8646636181a876b57652140eedec8cbc2cff51f10cc973da9265eb4aee838753',
    approval_reference='paper-recovery-pilot-20261026T133600Z-135600Z',
    expires_ns=1793022060000000000,
    original_runner_sha256='745d53479c6b1016463b516f0599bc16676451385a66696f886e7eb17de2c33f',
    protected_state_sha256='64dd23845dc41ea82dc5eacb4e2e3d935a4170306bea7578bc50e6d1dba70282',
    diagnostic_artifact_sha256='bc144b008e1253ede852167cecb15ab0d154c41d133906a934117522c95df2d8',
    diagnostic_result_sha256='9a82ac11335f74bf9796c3b43301f19e56f4b2b1ee66903aa9cd69b00f522ed2',
    reviewed_fix_sha256='5da292a1f9342d81feab409fda1285e199c372b5f6763cbb6872533e111ce370',
    review_authorization_sha256='abae85848dcf7a428999a72053e4711b70367e5e864b8a9adfbef5a3ab5cfd92',
    recovery_basis='operator_approved_fresh_baseline_adoption',
    historical_cause_unproven=True,
)

APPROVED_INSPECTION = None


def require_pilot_manifest(manifest):
    require(type(APPROVED_PILOT) is PilotManifest and manifest is APPROVED_PILOT,
            'pilot_not_approved')
    manifest.__post_init__()
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
