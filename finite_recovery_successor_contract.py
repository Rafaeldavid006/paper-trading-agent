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

APPROVED_SUCCESSOR = None

def require_successor_manifest(manifest):
    require(type(APPROVED_SUCCESSOR) is SuccessorManifest and manifest is APPROVED_SUCCESSOR,
            'successor_not_approved')
    require(original.APPROVED_RECOVERY is None and original.APPROVED_PILOT is None
            and original.APPROVED_INSPECTION is None, 'successor_other_grant_present')
    manifest.__post_init__()
    return manifest
