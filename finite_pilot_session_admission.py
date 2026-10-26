"""Exact zero-order session admission; historical singleton records are immutable."""
from contextlib import ExitStack
import json,math,time
import finite_pilot_contract as contract
from finite_pilot_contract import require,digest,NS
from finite_pilot_anchor import _bounded,_json
from finite_pilot_sessions import SessionJournal,validate_session_reference,legacy_rows_digest,legacy_counts,_load,_append
from finite_pilot_session_schema import SESSIONS,EVENTS,SCHEMA
from finite_recovery_state_report import current_anchor_digest
import runtime_state_bundle as inventory
from readonly_runtime_worker import serialized
from native_account_guard import SharedAccountGuard
from native_paper_execution import ExecutionObservation

def require_admission(manifest):
    contract.require_pilot_manifest(manifest)
    validate_session_reference(manifest)
    return manifest

def _session_clean_runner(row):
    require(type(row) in (tuple,list) and len(row)==6 and row[0]==1 and type(row[0]) is int
        and type(row[1]) in (int,float) and math.isfinite(row[1]) and row[1]>0
        and type(row[2]) is int and type(row[3]) is int and tuple(row[2:5])==(0,0,None)
        and type(row[5]) is str and row[5] in ('recovered_readonly','complete'),
        'session_clean_runner_required')
    return tuple(row)

class SessionAdmissionJournal(SessionJournal):
    def _now_admission(self,manifest,previous=0):
        now=self._now(manifest,previous)
        require(now<manifest.session_admission.expires_ns,'admission_window_closed')
        return now

    def _first_state(self,db,manifest):
        self._binding_row(db,manifest)
        a=manifest.session_admission;root=validate_session_reference(manifest)
        counts=legacy_counts(db,root)
        require(current_anchor_digest(db)==a.current_anchor_sha256,'admission_anchor_changed')
        present=db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(SESSIONS,)).fetchone()
        if present:
            docs,rows,counts,events=_load(db,manifest)
            require(manifest.pilot_id not in docs and all(v['closed'] and v['clean'] for v in rows.values()),'session_attempt_consumed')
            last=max(rows,key=lambda sid:rows[sid]['last_ns'])
            require(digest(docs[last].document())==a.predecessor_digest
                and rows[last]['terminal_digest']==a.predecessor_terminal_digest
                and rows[last]['closed_cohort']==a.current_cohort_sha256,'session_predecessor_not_closed')
        else:
            require(a.predecessor_digest==a.authority_id,'session_predecessor_not_closed')
            from finite_recovery_state_report import _admission_report
            from finite_pilot_admission import _clean_runner
            with inventory._database(self.bundle.paths['runner'],self.bundle._pinned['runner']) as runner:
                old=_clean_runner(inventory._singleton(runner,'runner_control'))
            report=_admission_report(db,old)
            require(report['pilot_admission_state']=='applied'
                and report['pilot_admission_manifest_digest']==a.authority_id
                and report['runner_matches_admission_prepared_new'],'session_legacy_admission_invalid')
        require(counts['ticks']<root.max_ticks and counts['writes']<root.max_write_attempts
            and counts['sources']<root.max_source_requests,'session_authority_budget_exhausted')
        return root

    def admit(self,manifest,*,guard,validate):
        require_admission(manifest)
        from finite_recovery_state_report import current_anchor_digest
        import finite_pilot_session_admission as canonical
        require(validate is canonical.validate_admission_evidence,'reviewed_admission_validator_required')
        require(type(guard) is SharedAccountGuard and guard.binding==self.bundle.binding
            and guard.legacy_database==self.bundle.paths['legacy']
            and guard.lifecycle_database==self.bundle.paths['lifecycle']
            and guard.state_database==self.bundle.paths['guard'],'recovery_guard_mismatch')
        with serialized(self.bundle):
            self._verify(manifest)
            with self._db() as db:
                self._binding_row(db,manifest)
                if db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(SESSIONS,)).fetchone():
                    docs,rows,_,_=_load(db,manifest)
                    if manifest.pilot_id in docs:
                        self._row(db,manifest)
                        return {'phase':'already_admitted_readonly'}
                self._first_state(db,manifest)
            with guard.hold(now_ns=self._now_admission(manifest),read_only=True):
                self._controls()
                with inventory._database(self.bundle.paths['runner'],self.bundle._pinned['runner']) as db:
                    old=_session_clean_runner(inventory._singleton(db,'runner_control'))
                a=manifest.session_admission
                require(digest(old)==a.current_runner_sha256,'admission_runner_changed')
                require(self.cohort_state_digest()==a.current_cohort_sha256,'admission_cohort_changed')
                protected=self.protected_state_digest()
                require(protected==a.protected_state_sha256,'admission_protected_changed')
                application=self.application_state_digest();before=self._guard_snapshot()
                with self._db() as db:self._first_state(db,manifest)
                prepared=self._now_admission(manifest)
                new=(old[0],prepared/NS,*old[2:])
                require(new[1]>=old[1],'admission_clock_regression')
                receipt=dict(old=list(old),new=list(new),protected=protected,guard=digest(before),
                    predecessor=a.predecessor_digest,evidence=a.closure_evidence_sha256)
                with self._db(write=True) as db:
                    root=self._first_state(db,manifest)
                    self._now_admission(manifest,prepared)
                    if not db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(SESSIONS,)).fetchone():
                        for sql in SCHEMA.values():db.execute(sql)
                        db.execute('INSERT INTO '+SESSIONS+' VALUES(?,?,?,?)',
                            (root.pilot_id,a.authority_id,a.authority_id,a.root_document))
                        _append(db,root.pilot_id,'LEGACY_CLOSED',prepared,dict(counts=legacy_counts(db,root),
                            legacy_rows=legacy_rows_digest(db),evidence=a.closure_evidence_sha256,terminal=a.predecessor_terminal_digest))
                    db.execute('INSERT INTO '+SESSIONS+' VALUES(?,?,?,?)',
                        (manifest.pilot_id,a.authority_id,digest(manifest.document()),_json(manifest.document())))
                    _append(db,manifest.pilot_id,'PREPARED',prepared,receipt)
                    _load(db,manifest)
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
                    db=handles['anchor'];self._binding_row(db,manifest);_load(db,manifest)
                    require(current_anchor_digest(db)==claimed_anchor,'admission_anchor_changed')
                    require(_load(db,manifest)[1][manifest.pilot_id]['prepared']==receipt, 'admission_receipt_changed')
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
                    self._binding_row(db,manifest);_load(db,manifest)
                    require(current_anchor_digest(db)==claimed_anchor,'admission_anchor_changed')
                    require(_load(db,manifest)[1][manifest.pilot_id]['prepared']==receipt, 'admission_receipt_changed')
                    evidence_digest=digest((evidence.started_ns,evidence.finished_ns,evidence.request_count,
                        evidence.observation.observed_ns,evidence.observation.native_digest))
                    _append(db,manifest.pilot_id,'APPLIED',final,dict(prepared_digest=digest(receipt),guard=digest(after),evidence=evidence_digest))
                    _load(db,manifest)
                    self._checkpoint('admission_applied_before_commit');db.commit();self._checkpoint('admission_applied')
        return {'phase':'admitted'}


# Separate exact typed evidence entry; old recovery/successor callback gates stay intact.
from decimal import Decimal
import os
from decimal_lifecycle import Lifecycle,SessionRiskPolicy
from native_paper_execution import NativeExecutionObserver,CoordinatedWire
from alpaca_schema import parse_latest_quotes,fresh
from finite_pilot_transport import _read
from finite_pilot_session_transport import SessionReadOnlyTransport

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
    require(manifest.starts_ns <= started < manifest.session_admission.expires_ns,'admission_window_closed')
    guard.admit_cycle(now_ns=started)
    # This exact read-only incident predates native ownership. A broader
    # position/order incident requires a different attended recovery protocol.
    with _read(bundle.paths['lifecycle']) as db:
        require(all(db.execute('SELECT COUNT(*) FROM ' + table).fetchone() == (0,)
                    for table in ('intents', 'cancels', 'native_dispatch')),
                'incident_native_history_not_empty')
    key, secret = _credentials(os.environ)
    transport = SessionReadOnlyTransport(key, secret, manifest=manifest, now_ns=now_ns,
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

