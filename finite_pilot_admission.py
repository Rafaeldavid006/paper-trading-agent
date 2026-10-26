"""One-shot fresh admission inside exact finite pilot initialize; no recovery reset."""
from contextlib import ExitStack
import json
import math
import time
import finite_pilot_contract as contract
from finite_pilot_contract import require,digest,NS
from finite_pilot_anchor import AnchorJournal,_bounded,_json,_manifest_row,_eligible_runner
from finite_pilot_admission_schema import TABLE,SQL
import finite_recovery_successor_contract as successor
import runtime_state_bundle as inventory
from readonly_runtime_worker import serialized
from native_account_guard import SharedAccountGuard
from native_paper_execution import ExecutionObservation


def require_admission(manifest):
    contract.require_pilot_manifest(manifest)
    require(type(manifest.admission) is contract.PilotAdmission,'typed_admission_required')
    return manifest


def successor_rows(db,manifest):
    """Archived reference only. Never installs the old successor as authority."""
    a=manifest.admission
    reference=successor.SuccessorManifest(**json.loads(a.successor_document))
    rows=_bounded(db,'SELECT * FROM '+successor.TABLE+' ORDER BY seq',limit=2)
    require(len(rows)==2,'admission_successor_incomplete')
    first,last=rows
    require(first[:2]==(1,'prepared') and last[:2]==(2,'applied')
            and first[3]==a.successor_digest and first[4]==a.successor_document
            and first[5]==reference.predecessor_digest and last[3:9]==first[3:9]
            and first[8]==a.protected_state_sha256 and first[10] is None
            and contract._hash(first[9]) and contract._hash(last[9]) and contract._hash(last[10])
            and type(first[2]) is int and type(last[2]) is int
            and reference.starts_ns<=first[2]<=last[2]<reference.expires_ns,
            'admission_successor_changed')
    old=json.loads(first[6]);new=json.loads(first[7])
    require(digest(_eligible_runner(old))==reference.original_runner_sha256
            and new==[1,first[2]/NS,0,0,None,'recovered_readonly'] and new[1]>=old[1],
            'admission_successor_changed')
    require(_bounded(db,'SELECT * FROM finite_pilot_recovery',limit=1)
            ==[(1,reference.predecessor_digest,reference.predecessor_document)]
            and not _bounded(db,'SELECT * FROM finite_pilot_recovery_journal',limit=2),
            'admission_predecessor_changed')
    return rows


def _clean_runner(row):
    require(type(row) in (tuple,list) and len(row)==6 and type(row[0]) is int and row[0]==1
            and type(row[1]) in (int,float) and math.isfinite(row[1]) and row[1]>0
            and tuple(row[2:])==(0,0,None,'recovered_readonly'), 'admission_clean_runner_required')
    return tuple(row)


def admission_gate(db,manifest):
    """Normal source/tick gate stays closed unless exact admission APPLIED exists."""
    require_admission(manifest)
    _manifest_row(db,'finite_pilot_enrollment',manifest)
    linked=successor_rows(db,manifest)
    rows=_bounded(db,'SELECT * FROM '+TABLE+' ORDER BY seq',limit=2)
    require(len(rows)==2,'admission_incomplete')
    first,last=rows
    require(first[:2]==(1,'prepared') and last[:2]==(2,'applied')
            and first[3]==digest(manifest.document()) and first[4]==digest(linked)
            and last[3:8]==first[3:8] and first[7]==manifest.admission.protected_state_sha256
            and first[9] is None and contract._hash(first[8]) and contract._hash(last[8])
            and contract._hash(last[9]) and type(first[2]) is int and type(last[2]) is int
            and manifest.starts_ns<=first[2]<=last[2]<manifest.admission.expires_ns,
            'admission_receipt_invalid')
    old=_clean_runner(json.loads(first[5]));new=json.loads(first[6])
    require(digest(old)==manifest.admission.current_runner_sha256
            and tuple(old)==tuple(json.loads(linked[1][7]))
            and new==[old[0],first[2]/NS,*old[2:]] and new[1]>=old[1],
            'admission_receipt_invalid')
    return rows


class AdmissionJournal(AnchorJournal):
    def _now_admission(self,manifest,previous=0):
        now=self._now(manifest,previous)
        require(now<manifest.admission.expires_ns,'admission_window_closed')
        return now

    def _first_state(self,db,manifest):
        self._binding_row(db,manifest)
        require(all(not _bounded(db,'SELECT * FROM '+table,limit=1) for table in
            ('finite_pilot_enrollment','finite_pilot_control','finite_pilot_stages',
             'finite_pilot_writes','finite_pilot_sources')), 'admission_pilot_already_consumed')
        require(not db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(TABLE,)).fetchone(),
                'admission_attempt_consumed')
        linked=successor_rows(db,manifest)
        from finite_recovery_state_report import current_anchor_digest
        require(current_anchor_digest(db)==manifest.admission.current_anchor_sha256,
                'admission_anchor_changed')
        return linked

    def admit(self,manifest,*,guard,validate):
        require_admission(manifest)
        from finite_recovery_state_report import current_anchor_digest
        import finite_pilot_admission as canonical
        require(validate is canonical.validate_admission_evidence,'reviewed_admission_validator_required')
        require(type(guard) is SharedAccountGuard and guard.binding==self.bundle.binding
            and guard.legacy_database==self.bundle.paths['legacy']
            and guard.lifecycle_database==self.bundle.paths['lifecycle']
            and guard.state_database==self.bundle.paths['guard'],'recovery_guard_mismatch')
        with serialized(self.bundle):
            self._verify(manifest)
            with self._db() as db:
                self._binding_row(db,manifest)
                if _bounded(db,'SELECT * FROM finite_pilot_enrollment',limit=1):
                    admission_gate(db,manifest)
                    return {'phase':'already_admitted_readonly'}
                self._first_state(db,manifest)
            with guard.hold(now_ns=self._now_admission(manifest),read_only=True):
                self._controls()
                with inventory._database(self.bundle.paths['runner'],self.bundle._pinned['runner']) as db:
                    old=_clean_runner(inventory._singleton(db,'runner_control'))
                a=manifest.admission
                require(digest(old)==a.current_runner_sha256,'admission_runner_changed')
                require(self.cohort_state_digest()==a.current_cohort_sha256,'admission_cohort_changed')
                protected=self.protected_state_digest()
                require(protected==a.protected_state_sha256,'admission_protected_changed')
                application=self.application_state_digest();before=self._guard_snapshot()
                with self._db() as db:linked=self._first_state(db,manifest)
                require(old==tuple(json.loads(linked[1][7])) and digest(before)==linked[1][9],
                        'admission_poststate_changed')
                prepared=self._now_admission(manifest)
                new=(old[0],prepared/NS,*old[2:])
                require(new[1]>=old[1],'admission_clock_regression')
                receipt=(1,'prepared',prepared,digest(manifest.document()),digest(linked),
                         _json(old),_json(new),protected,digest(before),None)
                with self._db(write=True) as db:
                    self._first_state(db,manifest)
                    self._now_admission(manifest,prepared)
                    # Single atomic admission claim. A failure leaves either
                    # no pilot rows or a durable consumed PREPARED, never a
                    # separately reusable enrollment with a fresh lease.
                    db.execute(SQL)
                    require(db.execute('UPDATE finite_pilot_binding SET pilot_digest=? WHERE id=1 AND pilot_digest IS NULL',
                        (receipt[3],)).rowcount==1,'admission_pilot_already_consumed')
                    db.execute('INSERT INTO finite_pilot_enrollment VALUES(1,?,?)',
                               (receipt[3],_json(manifest.document())))
                    db.execute("INSERT INTO finite_pilot_control VALUES(1,0,0,0,0,0,0,0,'',0)")
                    db.execute('INSERT INTO '+TABLE+' VALUES(?,?,?,?,?,?,?,?,?,?)',receipt)
                    claimed_anchor=current_anchor_digest(db)
                self._checkpoint('admission_prepared')
                evidence=validate(manifest,bundle=self.bundle,guard=guard,now_ns=self.now_ns)
                require(type(evidence) is contract.RecoveryEvidence and type(evidence.observation) is ExecutionObservation,
                        'typed_recovery_evidence_required')
                now=self._now_admission(manifest,prepared)
                require(contract._ns(evidence.started_ns) and contract._ns(evidence.finished_ns)
                    and prepared<=evidence.started_ns<=evidence.observation.observed_ns<=evidence.finished_ns<=now
                    and now-evidence.started_ns<=10*NS and type(evidence.request_count) is int
                    and 1<=evidence.request_count<=12 and contract._hash(evidence.observation.native_digest)
                    and evidence.observation.legacy_revision==guard.legacy_revision,'admission_evidence_invalid')
                self._checkpoint('admission_validated')
                with ExitStack() as stack:
                    handles={role:stack.enter_context(inventory._database(self.bundle.paths[role],
                        self.bundle._pinned[role],write=True,public_read=role=='anchor'))
                        for role in ('runner','decision','lifecycle','source','guard','anchor')}
                    self._verify(manifest);self._controls();guard.check()
                    require(self.protected_state_digest()==protected,'admission_protected_changed')
                    require(self.application_state_digest()==application,'admission_application_changed')
                    after=self._guard_snapshot();self._guard_delta(before,after,evidence,prepared,guard)
                    now=self._now_admission(manifest,now)
                    require(now-evidence.started_ns<=10*NS,'admission_evidence_invalid')
                    db=handles['anchor'];self._binding_row(db,manifest);_manifest_row(db,'finite_pilot_enrollment',manifest)
                    require(current_anchor_digest(db)==claimed_anchor,'admission_anchor_changed')
                    require(digest(successor_rows(db,manifest))==receipt[4]
                        and _bounded(db,'SELECT * FROM '+TABLE+' ORDER BY seq',limit=2)==[receipt],
                        'admission_receipt_changed')
                    runner=handles['runner']
                    require(runner.execute('UPDATE runner_control SET heartbeat=? WHERE id=? AND heartbeat=? AND dirty=? '
                        'AND stopped=? AND attention IS ? AND stage=?',(new[1],*old)).rowcount==1
                        and inventory._singleton(runner,'runner_control')==new,'admission_compare_and_set_failed')
                    self._checkpoint('admission_cas_before_commit');runner.commit()
                    self._checkpoint('admission_cas_committed');runner.execute('BEGIN IMMEDIATE')
                    require(inventory._singleton(runner,'runner_control')==new,'admission_runner_changed')
                    self._verify(manifest);self._controls()
                    require(self.protected_state_digest()==protected and self.application_state_digest()==application,
                            'admission_poststate_changed')
                    final=self._now_admission(manifest,now)
                    require(final-evidence.started_ns<=10*NS,'admission_evidence_invalid')
                    self._binding_row(db,manifest);_manifest_row(db,'finite_pilot_enrollment',manifest)
                    require(current_anchor_digest(db)==claimed_anchor,'admission_anchor_changed')
                    require(digest(successor_rows(db,manifest))==receipt[4]
                        and _bounded(db,'SELECT * FROM '+TABLE+' ORDER BY seq',limit=2)==[receipt],
                        'admission_receipt_changed')
                    evidence_digest=digest((evidence.started_ns,evidence.finished_ns,evidence.request_count,
                        evidence.observation.observed_ns,evidence.observation.native_digest))
                    db.execute('INSERT INTO '+TABLE+' VALUES(?,?,?,?,?,?,?,?,?,?)',
                        (2,'applied',final,*receipt[3:8],digest(after),evidence_digest))
                    self._checkpoint('admission_applied_before_commit');db.commit();self._checkpoint('admission_applied')
        return {'phase':'admitted'}


# Separate exact typed evidence entry; old recovery/successor callback gates stay intact.
from decimal import Decimal
import os
from decimal_lifecycle import Lifecycle,SessionRiskPolicy
from native_paper_execution import NativeExecutionObserver,CoordinatedWire
from alpaca_schema import parse_latest_quotes,fresh
from finite_pilot_transport import _read
from finite_pilot_admission_transport import AdmissionReadOnlyTransport

def _credentials(environ):
    key,secret=environ.get('ALPACA_API_KEY'),environ.get('ALPACA_SECRET_KEY')
    require(bool(key) and bool(secret),'credentials_missing')
    return key,secret

def validate_admission_evidence(manifest, *, bundle, guard, now_ns):
    """One fresh GET observation while the exact clean recovered runner is retained.

    Only AdmissionJournal.admit invokes this function in production,
    after committing the one-shot prepared record and taking seal/account holds.
    No lifecycle.observe, risk update, claim, dispatch, reset, or parsing change.
    """
    require_admission(manifest)
    bundle.verify()
    guard.check()
    started = now_ns()
    require(manifest.starts_ns <= started < manifest.admission.expires_ns,'admission_window_closed')
    guard.admit_cycle(now_ns=started)
    # This exact read-only incident predates native ownership. A broader
    # position/order incident requires a different attended recovery protocol.
    with _read(bundle.paths['lifecycle']) as db:
        require(all(db.execute('SELECT COUNT(*) FROM ' + table).fetchone() == (0,)
                    for table in ('intents', 'cancels', 'native_dispatch')),
                'incident_native_history_not_empty')
    key, secret = _credentials(os.environ)
    transport = AdmissionReadOnlyTransport(key, secret, manifest=manifest, now_ns=now_ns,
                                          started_ns=started)
    life = Lifecycle(bundle.paths['lifecycle'], bundle.binding, allowlist=('NVDA',),
                     session_risk_policy=SessionRiskPolicy(4, Decimal('100')))
    observer = NativeExecutionObserver(life, guard, CoordinatedWire(transport, guard, now_ns),
                                        now_ns=now_ns)
    observation = observer.collect()
    require(type(observation) is ExecutionObservation, 'typed_recovery_observation_required')
    raw = observation.arguments()
    finished = now_ns()
    snapshot = life._parse(raw, finished)
    require(not snapshot.positions and not snapshot.orders, 'incident_owned_flatness_unproven')
    parse_latest_quotes(raw['quotes'], symbols=tuple(raw['quote_symbols']), feed=raw['feed'], now_ns=finished)
    fresh(observation.observed_ns, finished)
    require(snapshot.clock.is_open and observation.session_open_ns <= finished < snapshot.clock.next_close_ns
            and observation.legacy_revision == guard.legacy_revision
            and 0 <= finished - started <= 10 * 10**9, 'fresh_regular_recovery_required')
    guard.check()
    bundle.verify()
    receipts = tuple(sorted((r.client_order_id, r.observed_ns, r.evidence_digest)
                            for r in guard._last_reconciliation.absence_receipts))
    terminals = transport.legacy_terminal_digests(guard.submitted_client_ids)
    return contract.RecoveryEvidence(observation, started, finished, transport.request_count,
                                     terminals, receipts)

