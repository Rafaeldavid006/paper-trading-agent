"""One fresh, read-only successor admission for an expired PREPARED-only root.

No runner CAS, root mutation, lifecycle reset, order dispatcher, execution gate,
serve hook, retry, or renewal. Success is evidence, not permission to trade.
"""
from pathlib import Path
import hashlib,json,os,sys,threading,time
import prepared_successor_activation as activation
import prepared_successor_contract as c
import runtime_state_bundle as inventory
from finite_pilot_anchor import AnchorJournal
from finite_recovery_state_report import current_anchor_digest
from finite_root_contract import from_document
from finite_root_journal import load,clean_runner
from readonly_runtime_worker import serialized
from native_account_guard import SharedAccountGuard
from finite_observation_worker import ReadOnlyLifecycle
from native_paper_execution import NativeExecutionObserver,CoordinatedWire,ExecutionObservation
from finite_pilot_contract import RecoveryEvidence
from alpaca_schema import parse_latest_quotes,fresh
from prepared_successor_journal import Journal
from prepared_successor_transport import Transport

def _checkpoint(name):
    """No-op crash-test seam; never a callback supplied by an activation."""

def resolve(grant,*,paths=None,test_only=False):
    c.approved(grant);c.require(type(test_only) is bool and (test_only or paths is None),'successor_fixed_paths')
    seal_path=(inventory.FIXED_PATHS if paths is None else paths)['seal'];identity=inventory._private(seal_path)
    fd=os.open(seal_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd);c.require((info.st_dev,info.st_ino)==identity and info.st_size<=4096,'successor_seal_changed');raw=os.read(fd,4097)
    finally:os.close(fd)
    c.require(inventory._private(seal_path)==identity and len(raw)<=4096 and hashlib.sha256(raw).hexdigest()==grant.seal_sha256,'successor_seal_changed')
    seal=inventory._load(raw)
    c.require(type(seal) is dict and seal.get('generation')==inventory.GENERATION and c._hash(seal.get('fingerprint')),'successor_seal_invalid')
    bundle=inventory.RuntimeStateBundle(seal['fingerprint'],paths=paths,test_only=test_only)
    bundle.verify();c.require(bundle._pinned['seal']==identity,'successor_seal_changed');return bundle

def verify(bundle,grant,journal,*,initial=False):
    """Strict incident eligibility; any uncertain order/CAS is a different protocol."""
    c.approved(grant);journal._verify();journal._controls()
    c.require(journal._seal_digest()==grant.seal_sha256,'successor_seal_changed')
    old=from_document(grant.predecessor_document)
    with journal._db() as db:
        c.require(current_anchor_digest(db)==grant.anchor_sha256,'successor_predecessor_changed')
        _,rows,counts,events=load(db,old)
    c.require(len(rows)==1 and old.children[0].pilot_id in rows,'successor_exact_first_child_required')
    r=rows[old.children[0].pilot_id]
    c.require(r['prepared'] is not None and r['applied'] is None and r['ended'] is None and not r['halted'] and not r['dirty'] and r['smoke_entry'] is None and counts['ticks']==counts['writes']==counts['sources']==0 and 0<=counts['broker']<=12 and all(e[3] in ('ROOT_IMPORTED','PREPARED','BROKER') for e in events),'successor_exact_prepared_required')
    with inventory._database(bundle.paths['runner'],bundle._pinned['runner']) as db:runner=clean_runner(inventory._singleton(db,'runner_control'))
    c.require(tuple(r['prepared']['old'])==runner and tuple(r['prepared']['new'])!=runner and c.digest(runner)==grant.runner_sha256,'successor_runner_not_old')
    with inventory._database(bundle.paths['lifecycle'],bundle._pinned['lifecycle']) as db:
        c.require(all(db.execute('SELECT COUNT(*) FROM '+name).fetchone()==(0,) for name in ('intents','cancels','native_dispatch')),'successor_uncertain_native_history')
    c.require(journal.protected_state_digest()==grant.protected_sha256,'successor_protected_changed')
    if initial:c.require(journal.cohort_state_digest()==grant.cohort_sha256,'successor_current_cohort_changed')
    return counts

def collect(grant,bundle,guard,journal,attempt,now_ns,monotonic_ns,*,test_only=False,transport_factory=None,environ=None):
    guard.check();journal._controls();started=now_ns();guard.admit_cycle(now_ns=started)
    # Full-book pagination, plus independent reserved-ID lookups and rereads.
    # The budget is global AND successor-local. A refusal consumes reservations.
    pages=max(1,(len(guard.submitted_client_ids)+99)//100)
    c.require(7+pages+len(guard.reserved_client_ids)<=grant.max_gets,'successor_history_lookup_budget')
    environment=os.environ if environ is None else environ
    key,secret=environment.get('ALPACA_API_KEY'),environment.get('ALPACA_SECRET_KEY')
    c.require(bool(key) and bool(secret),'successor_credentials_missing')
    wire=Transport(key,secret,grant=grant,journal=attempt,now_ns=now_ns,started_ns=started,controls=journal._controls,test_only=test_only,transport_factory=transport_factory)
    life=ReadOnlyLifecycle(bundle)
    observation=NativeExecutionObserver(life,guard,CoordinatedWire(wire,guard,now_ns),now_ns=now_ns,monotonic_ns=monotonic_ns).collect()
    c.require(type(observation) is ExecutionObservation,'successor_typed_observation')
    finished=now_ns();raw=observation.arguments();snapshot=life._parse(raw,finished)
    # Observer has already reconciled the full original account book. Only
    # proven terminal-flat legacy rows are excluded; no native order is allowed.
    c.require(not snapshot.positions and not snapshot.orders,'successor_full_account_not_flat')
    parse_latest_quotes(raw['quotes'],symbols=tuple(raw['quote_symbols']),feed=raw['feed'],now_ns=finished);fresh(observation.observed_ns,finished)
    c.require(snapshot.clock.is_open and observation.session_open_ns<=finished<snapshot.clock.next_close_ns and observation.legacy_revision==guard.legacy_revision and started<=observation.observed_ns<=finished and 0<=finished-started<=10*c.NS,'successor_fresh_regular_required')
    guard.check();journal._controls();bundle.verify()
    receipts=tuple(sorted((r.client_order_id,r.observed_ns,r.evidence_digest) for r in guard._last_reconciliation.absence_receipts))
    return RecoveryEvidence(observation,started,finished,wire.request_count,wire.legacy_terminal_digests(guard.submitted_client_ids),receipts)

def run(*,paths=None,test_only=False,now_ns=time.time_ns,monotonic_ns=time.monotonic_ns,transport_factory=None,environ=None):
    grant=c.approved(activation.APPROVED_ADMISSION)
    c.require(type(test_only) is bool and (test_only or (paths is None and transport_factory is None and environ is None)),'successor_production_override')
    c.window(grant,now_ns());bundle=resolve(grant,paths=paths,test_only=test_only)
    journal=AnchorJournal(bundle,now_ns);attempt=None;timer=None
    with serialized(bundle):
        verify(bundle,grant,journal,initial=True)
        path=str(Path(bundle.paths['seal']).parent/c.JOURNAL_NAME)
        c.require(test_only or path==c.JOURNAL_PATH,'successor_fixed_journal')
        # Existing path is a consumed attempt regardless of its contents or grant.
        # No supplied ID selects a new ledger, and no code deletes/repairs it.
        attempt=Journal(path,grant,now_ns,bundle.paths['seal'],_checkpoint)
        try:
            if not test_only:
                timer=threading.Timer(15,lambda:os._exit(124));timer.daemon=True;timer.start()
            guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
            with guard.hold(now_ns=now_ns(),read_only=True):
                verify(bundle,grant,journal,initial=True)
                before=journal._guard_snapshot();prepared=attempt.last;mono=monotonic_ns()
                c.require(type(mono) is int and mono>0,'successor_monotonic_invalid')
                evidence=collect(grant,bundle,guard,journal,attempt,now_ns,monotonic_ns,test_only=test_only,transport_factory=transport_factory,environ=environ)
                _checkpoint('evidence_collected');verify(bundle,grant,journal)
                now=c.window(grant,now_ns(),prepared);finished_mono=monotonic_ns()
                c.require(type(finished_mono) is int and 0<=finished_mono-mono<=10*c.NS and prepared<=evidence.started_ns<=evidence.observation.observed_ns<=evidence.finished_ns<=now and now-evidence.started_ns<=10*c.NS,'successor_evidence_stale')
                after=journal._guard_snapshot();journal._guard_delta(before,after,evidence,prepared,guard)
                c.require(attempt.gets==evidence.request_count,'successor_request_mismatch')
                attempt.complete(evidence,c.digest(after));_checkpoint('before_return')
                verify(bundle,grant,journal);attempt.verify()
                return dict(mode='prepared_successor_admission',phase='applied_readonly',provider_requests=attempt.gets,predecessor_counts=verify(bundle,grant,journal),predecessor_unchanged=True,fresh_account_flatness_at_ns=evidence.observation.observed_ns,execution_permission='none',automatic_retry=False)
        finally:
            if timer is not None:timer.cancel();timer.join()
            attempt.close()

def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    try:
        c.require(args==['--admit-readonly'],'successor_arguments');result=run();print(json.dumps(result,sort_keys=True),flush=True);return 0
    except Exception as exc:
        # Preserve the reviewed fixed schema detail, never provider values.
        from native_observation_collector import CollectionRefused,schema_failure_detail
        result=dict(mode='prepared_successor_admission',phase='refused',execution_permission='none',automatic_retry=False)
        if type(exc) is CollectionRefused and exc.code=='native_schema_refused':
            result.update(error_code='native_schema_refused',**schema_failure_detail(exc.schema_code,exc.schema_stage))
        print(json.dumps(result,sort_keys=True),flush=True);return 1
if __name__=='__main__':
    import prepared_successor_worker as canonical
    raise SystemExit(canonical.main())
