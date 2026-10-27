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
class SessionAdmission:
    """Pinned append-only session transition; never an implicit renewal."""
    root_document: str
    authority_id: str
    predecessor_digest: str
    predecessor_terminal_digest: str
    closure_evidence_sha256: str
    current_anchor_sha256: str
    current_cohort_sha256: str
    current_runner_sha256: str
    protected_state_sha256: str
    expires_ns: int
    close_expires_ns: int

    def __post_init__(self):
        require(all(_hash(getattr(self,k)) for k in (
            'authority_id','predecessor_digest','predecessor_terminal_digest',
            'closure_evidence_sha256','current_anchor_sha256',
            'current_cohort_sha256','current_runner_sha256','protected_state_sha256')),
            'session_pins_required')
        require(_ns(self.expires_ns) and _ns(self.close_expires_ns)
            and type(self.root_document) is str and len(self.root_document)<=32768,
            'session_reference_required')
        try: doc=json.loads(self.root_document)
        except (TypeError,ValueError): raise PilotRefused('session_reference_invalid') from None
        require(type(doc) is dict and digest(doc)==self.authority_id
            and json.dumps(doc,sort_keys=True,separators=(',',':'),allow_nan=False)==self.root_document
            and 'session_admission' not in doc, 'session_reference_invalid')


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
    session_admission: SessionAdmission | None = None

    def __post_init__(self):
        _common(self)
        if self.session_admission is not None:
            a=self.session_admission
            require(type(a) is SessionAdmission and self.admission is None,'session_exclusive_required')
            a.__post_init__()
            require(self.starts_ns<a.expires_ns<=min(self.expires_ns,self.starts_ns+300*NS)
                and self.expires_ns-self.starts_ns<=1200*NS
                and self.expires_ns<a.close_expires_ns<=self.expires_ns+60*NS,
                'session_window_invalid')
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
        result=asdict(self)
        if self.session_admission is None: result.pop('session_admission')
        return result


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


# LOCAL PROPOSED OVERLAY. NOT AUTHORIZED FOR PUBLICATION OR EXECUTION.
APPROVED_PILOT = None
APPROVED_RECOVERY = None
APPROVED_INSPECTION = None


def require_pilot_manifest(manifest):
    require(type(APPROVED_PILOT) is PilotManifest and manifest is APPROVED_PILOT,
            'pilot_not_approved')
    manifest.__post_init__()
    if manifest.session_admission is not None:
        from finite_pilot_sessions import validate_session_reference
        validate_session_reference(manifest)
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
