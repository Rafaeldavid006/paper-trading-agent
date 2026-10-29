"""Fixed-phase, categorically default-disabled finite PAPER runtime wiring.

No command-line path, approval, receipt, environment toggle, or timestamp can
activate this module. Both reviewed compiled manifests remain None. This is
local engineering: no grant, recovery, state reset, scheduling, or activation is
performed by importing it or by starting the service.
"""
from contextlib import contextmanager
from decimal import Decimal
import json
import os
import sqlite3
import sys
import time

import finite_pilot_contract as contract
from finite_pilot_anchor import AnchorJournal, resolve_approved_bundle
from finite_pilot_transport import (FinitePilotTransport, FiniteSourceTransport,
                                    RecoveryReadOnlyTransport, _read)
from active_demo_pipeline import Config, DemoPipeline
from alpaca_schema import SchemaError, fresh, parse_latest_quotes
from decimal_lifecycle import Lifecycle, SessionRiskPolicy
from demo_tick_runner import DemoTickRunner, RunnerConfig, RunnerLedger
from native_account_guard import AccountGuardRefused, SharedAccountGuard
from native_observation_collector import CollectionRefused
from native_paper_execution import (CoordinatedWire, NativeExecutionObserver,
                                    NativePaperDispatcher, ExecutionObservation,
                                    DispatchRefused, _datetime_ceil)
from native_paper_runner import BoundedPaperRunner, NativeMarketSource, NativePaperExecutionPort
from primary_earnings_source import PrimaryEarningsSource
from readonly_runtime_worker import serialized
from runtime_state_bundle import RuntimeStateBundle, RuntimeStateRefused

PHASES = frozenset(('initialize', 'source', 'tick', 'recover'))
TICK_OUTCOMES = frozenset(('complete', 'blocked', 'closed_session',
                          'waiting_regular_bar_window', 'waiting_request_budget'))
PUBLIC_REASONS = frozenset(('compiled_pilot_approval_missing', 'compiled_recovery_approval_missing',
    'invalid_arguments', 'pilot_stage_refused', 'pilot_enrolled', 'source_collected',
    'bounded_tick_finished', 'incident_recovered', 'incident_recovery_previously_applied',
    'unresolved_exposure_requires_operator'))


# This deliberately fixed public vocabulary is independent of exception text,
# provider responses, paths, account/client identifiers and manifest contents.
# Unknown or wrapped failures remain unknown instead of inventing a diagnosis.
ERROR_CODES = frozenset((
    'root_stored_execution_policy_mismatch',
    'smoke_buy_limit_not_current_ask',
    'smoke_cancel_too_early',
    'smoke_consumed_entry_without_intent',
    'smoke_disabled',
    'smoke_dispatcher_required',
    'smoke_entry_consumed',
    'smoke_entry_envelope',
    'smoke_entry_inactive',
    'smoke_entry_not_claimed',
    'smoke_entry_price_or_spread',
    'smoke_exact_identity',
    'smoke_existing_write_context',
    'smoke_exit_limit_not_current_bid',
    'smoke_fixed_scope_required',
    'smoke_identity_required',
    'smoke_initial_flat_required',
    'smoke_nonterminal_history',
    'smoke_not_approved',
    'smoke_owned_cancel',
    'smoke_owned_order_missing',
    'smoke_position_envelope',
    'smoke_quantity_cap',
    'smoke_quote_required',
    'smoke_quote_spread',
    'smoke_root_caps_required',
    'smoke_runner_begin_refused',
    'smoke_second_child_readonly',
    'smoke_stage_timeout',
    'smoke_write_identity',
    'smoke_write_route',
    'smoke_write_without_entry_authority',
    'smoke_zero_fill_flatness_unproven',
    'two_children_required',
    'typed_child_required',
    'typed_root_required',
    'typed_smoke_required',
    'v1_closed_predecessor_required',
    'unclassified_failure', 'operation_timeout', 'state_sqlite_refused',
    'exact_root_child_required',
    'root_admission_broker_invalid',
    'root_admission_expired',
    'root_admission_get_cap',
    'root_admission_history_changed',
    'root_aggregate_cap',
    'root_anchor_changed',
    'root_applied_evidence_invalid',
    'root_applied_invalid',
    'root_approval_invalid',
    'root_arguments',
    'root_attempt_consumed',
    'root_broker_cap',
    'root_broker_id_changed',
    'root_broker_invalid',
    'root_broker_stage_invalid',
    'root_canonical_validator_required',
    'root_cap_invalid',
    'root_cas_failed',
    'root_cas_poststate_changed',
    'root_child_binding_changed',
    'root_child_cap',
    'root_child_document_changed',
    'root_child_inactive',
    'root_child_invalid',
    'root_child_mismatch',
    'root_child_not_enrolled',
    'root_child_overlap',
    'root_child_sequence',
    'root_child_tick_allocation',
    'root_clean_runner_required',
    'root_clock_regression',
    'root_dispatch_foreign',
    'root_dispatch_history_incomplete',
    'root_dispatch_outcome_uncertain',
    'root_dispatch_stage_unproven',
    'root_duplicate_cid',
    'root_end_invalid',
    'root_end_payload',
    'root_event_invalid',
    'root_event_kind_invalid',
    'root_event_payload',
    'root_evidence_stale',
    'root_exclusive_required',
    'root_expired',
    'root_factory_override',
    'root_flat_terminal_required',
    'root_foreign_or_missing_order',
    'root_founding_binding_changed',
    'root_fresh_regular_observation',
    'root_get_cap',
    'root_get_only',
    'root_get_reservation_required',
    'root_global_sequence',
    'root_guard_mismatch',
    'root_halt_invalid',
    'root_history_bound',
    'root_history_changed',
    'root_history_lookup_budget',
    'root_id_invalid',
    'root_import_invalid',
    'root_initial_anchor_changed',
    'root_initial_pins_changed',
    'root_legacy_history_changed',
    'root_legacy_history_incomplete',
    'root_not_approved',
    'root_orphan_session',
    'root_owned_identity_changed',
    'root_owned_residual_exposure',
    'root_pins_invalid',
    'root_poststate_changed',
    'root_predecessor_invalid',
    'root_predecessor_not_ended',
    'root_prepared_cas_invalid',
    'root_prepared_invalid',
    'root_prestate_changed',
    'root_quote_scope',
    'root_registry_changed',
    'root_remaining_budget_required',
    'root_reviewed_transport_required',
    'root_runner_changed',
    'root_sessions_missing',
    'root_single_open_required',
    'root_source_cap',
    'root_source_invalid',
    'root_stage_end_invalid',
    'root_stage_inactive',
    'root_stage_invalid',
    'root_start_consumed',
    'root_structured_end_required',
    'root_submission_uncertain',
    'root_terminal_action',
    'root_terminal_consumed',
    'root_terminal_window',
    'root_test_override',
    'root_typed_evidence_required',
    'root_typed_observation_required',
    'root_v1_bridge_required',
    'root_window_invalid',
    'root_write_broker_unmatched',
    'root_write_cap',
    'root_write_history_unmatched',
    'root_write_invalid',
    'root_write_replay',
    'root_write_reservation_missing',

    'native_schema_refused', 'tick_completion_unproven', 'finite_scope_exhausted',
    'unexpected_tick_outcome', 'credentials_missing', 'source_interrupted',
    'source_stage_refused', 'existing_attention_preserved',
    'bundle_file_missing', 'bundle_file_replaced', 'bundle_inventory_or_schema_changed',
    'bundle_anchor_incomplete_or_changed', 'bundle_anchor_history_changed',
    'bundle_cohort_changed', 'anchor_manifest_changed', 'foundation_incomplete',
    'anchor_binding_changed', 'pilot_stage_incomplete', 'pilot_budget_exhausted',
    'pilot_cadence_wait', 'pilot_attempt_cap', 'pilot_stage_cap',
    'pilot_window_expired', 'pilot_clock_regression', 'pilot_not_approved',
    'recovery_not_approved', 'recovery_incomplete', 'recovery_receipt_invalid',
    'recovery_window_expired', 'recovery_request_cap', 'recovery_journal_orphaned',
    'incident_native_history_not_empty', 'incident_owned_flatness_unproven',
    'fresh_regular_recovery_required', 'exact_incident_runner_required',
    'complete_legacy_terminal_evidence_required', 'bounded_recovery_history_required',
    'legacy_not_terminal_flat', 'legacy_reserved_absence_unproven',
    'legacy_submitted_order_missing', 'legacy_order_mismatch', 'legacy_terminal_changed',
    'legacy_worker_busy', 'legacy_writer_busy', 'cycle_budget_wait',
    'shared_request_budget_exhausted', 'budget_clock_regression', 'budget_state_corrupt',
    'budget_storage_unavailable', 'collection_deadline_exceeded', 'collection_failed',
    'authentication_or_entitlement_failed', 'account_changed_during_collection',
    'conflicting_response_account', 'invalid_provider_payload', 'invalid_provider_status',
    'local_clock_regression', 'order_changed_during_collection',
    'order_pagination_incomplete', 'aggregate_orders_overflow',
    'owned_ledger_changed_during_collection', 'owned_ledger_unavailable',
    'prepared_request_mismatch', 'runner_write_control_refused',
    'pipeline_write_control_refused', 'committed_native_uncertainty_required',
    'production_overrides_forbidden', 'invalid_test_mode',
    'session_admission',
    'session_applied_invalid',
    'session_attempt_consumed',
    'session_authority_budget_exhausted',
    'session_authority_changed',
    'session_budget_corrupt',
    'session_clean_runner_required',
    'session_clock_regression',
    'session_close_before_expiry',
    'session_close_invalid',
    'session_close_window_invalid',
    'session_event_after_close',
    'session_event_invalid',
    'session_event_kind_invalid',
    'session_event_window_invalid',
    'session_exclusive_required',
    'session_global_clock_regression',
    'session_global_sequence_invalid',
    'session_halt_invalid',
    'session_legacy_admission_invalid',
    'session_legacy_cadence_invalid',
    'session_legacy_changed',
    'session_legacy_counters_invalid',
    'session_legacy_schema_invalid',
    'session_legacy_sources_invalid',
    'session_manifest_changed',
    'session_manifest_invalid',
    'session_multiple_active',
    'session_not_active',
    'session_not_enrolled',
    'session_order_history_present',
    'session_predecessor_not_closed',
    'session_predecessor_state_changed',
    'session_predecessor_unclean',
    'session_predecessor_window_active',
    'session_prepared_invalid',
    'session_root_binding_changed',
    'session_root_changed',
    'session_root_closure_invalid',
    'session_root_control_invalid',
    'session_root_enrollment_changed',
    'session_root_invalid',
    'session_root_missing',
    'session_source_invalid',
    'session_stage_invalid',
    'session_terminal_action_invalid',
    'session_write_invalid',
    'typed_admission_required','admission_successor_incomplete','admission_successor_changed',
    'admission_predecessor_changed','admission_clean_runner_required','admission_incomplete',
    'admission_receipt_invalid','admission_window_closed','admission_pilot_already_consumed',
    'admission_attempt_consumed','admission_anchor_changed','reviewed_admission_validator_required',
    'admission_runner_changed','admission_cohort_changed','admission_protected_changed',
    'admission_poststate_changed','admission_clock_regression','admission_evidence_invalid',
    'admission_application_changed','admission_receipt_changed','admission_compare_and_set_failed',
    'admission_exclusive_pilot_required',
))


def failure(phase, exc=None, *, code=None):
    """Only literal allowlisted detail escapes; never stringify an exception."""
    if code is None:
        if type(exc) is CollectionRefused:
            candidate = exc.code
        elif type(exc) in (contract.PilotRefused, AccountGuardRefused,
                            RuntimeStateRefused, DispatchRefused):
            candidate = exc.args[0] if len(exc.args) == 1 else None
        elif type(exc) is SchemaError:
            candidate = 'native_schema_refused'
        elif type(exc) is TimeoutError:
            candidate = 'operation_timeout'
        elif type(exc) in (sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError,
                            sqlite3.IntegrityError, sqlite3.ProgrammingError):
            candidate = 'state_sqlite_refused'
        else:
            candidate = None
        code = candidate if type(candidate) is str and candidate in ERROR_CODES else 'unclassified_failure'
    result = public('manual_handoff', 'pilot_stage_refused', error_stage=phase, error_code=code)
    if code == 'native_schema_refused':
        from native_observation_collector import schema_failure_detail
        if type(exc) is CollectionRefused:
            result.update(schema_failure_detail(exc.schema_code, exc.schema_stage))
        elif type(exc) is SchemaError:
            literal = exc.args[0] if len(exc.args) == 1 else None
            result.update(schema_failure_detail(literal, 'outside_collector'))
        else:
            result.update(schema_failure_detail(None, None))
    return result


def require(ok, code):
    if not ok:
        raise contract.PilotRefused(code)


def public(phase, reason, *, outcome=None, error_stage=None, error_code=None, **counts):
    require(phase in PHASES | {'disabled', 'manual_handoff', 'invalid'}
            and reason in PUBLIC_REASONS, 'unsafe_public_summary')
    require(set(counts) <= {'source_events', 'source_requests', 'events', 'exits'}
            and all(type(v) is int and v >= 0 for v in counts.values()), 'unsafe_public_summary')
    require(outcome is None or outcome in TICK_OUTCOMES, 'unsafe_public_summary')
    require((error_stage is None and error_code is None) or
            (type(error_stage) is str and error_stage in PHASES and
             type(error_code) is str and error_code in ERROR_CODES), 'unsafe_public_summary')
    permission = 'none'
    if type(contract.APPROVED_PILOT) is contract.PilotManifest:
        if phase in ('initialize', 'source', 'tick'):
            permission = 'finite_paper_scope'
        elif phase == 'manual_handoff':
            permission = 'revoked'
    result = dict(mode='finite_paper_pilot', phase=phase, reason=reason,
                  order_permission=permission, execution_ready=False,
                  historical_cause_proven=False, **counts)
    import finite_root_contract as rc
    if type(rc.APPROVED_ROOT) is rc.RootGrant and type(rc.APPROVED_ROOT.smoke) is rc.ExecutionSmokeScope:
        result.update(mode='finite_paper_execution_smoke',research_signal=False,session_mode=rc.APPROVED_ROOT.smoke.session_mode,order_permission='finite_paper_smoke_scope' if phase in ('initialize','tick') else 'revoked' if phase=='manual_handoff' else 'none')
    if outcome is not None:
        result['outcome'] = outcome
    if error_stage is not None:
        result.update(error_stage=error_stage, error_code=error_code)
    if phase == 'manual_handoff':
        result.update(owned_exposure='not_proven_flat', action='operator_reconciliation_required')
    return result


def _exhausted(row, manifest):
    if manifest.root_binding is not None:
        from finite_root_journal import RootJournal
        return RootJournal._exhausted(row,manifest)
    if manifest.session_admission is not None:
        from finite_pilot_sessions import SessionJournal
        return SessionJournal._exhausted(row,manifest)
    return (row['ticks'] >= manifest.max_ticks or row['writes'] >= manifest.max_write_attempts
            or row['sources'] >= manifest.max_source_requests)


def _pristine(bundle):
    """Read existing state BEFORE credential access or auto-creating classes."""
    with _read(bundle.paths['runner']) as db:
        rows = db.execute('SELECT dirty,stopped,attention FROM runner_control').fetchall()
        require(rows == [(0, 0, None)], 'existing_attention_preserved')
    with _read(bundle.paths['lifecycle']) as db:
        require(db.execute('SELECT stopped,attention FROM control').fetchall() == [(0, 0)],
                'existing_attention_preserved')
    with _read(bundle.paths['decision']) as db:
        require(db.execute('SELECT stopped FROM demo_control').fetchall() == [(0,)]
                and db.execute('SELECT 1 FROM demo_attention LIMIT 1').fetchone() is None,
                'existing_attention_preserved')
    with _read(bundle.paths['source']) as db:
        require(db.execute('SELECT busy FROM primary_meta').fetchall() == [(0,)],
                'source_interrupted')


def _local_components(bundle, now_ns, test_only):
    # Caller has verified the complete sealed inventory, including every schema.
    life = Lifecycle(bundle.paths['lifecycle'], bundle.binding, allowlist=('NVDA',),
                     session_risk_policy=SessionRiskPolicy(4, Decimal('100')))
    guard = SharedAccountGuard(bundle.paths['legacy'], bundle.paths['lifecycle'], bundle.binding,
                               enabled=True, state_database=bundle.paths['guard'], test_only=test_only,
                               runtime_bundle=bundle)
    pipeline = DemoPipeline(bundle.paths['decision'], Config(enabled=True, allowlist=('NVDA',)))
    ledger = RunnerLedger(bundle.paths['runner'])
    return life, guard, pipeline, ledger


def _credentials(environ):
    key, secret = environ.get('ALPACA_API_KEY'), environ.get('ALPACA_SECRET_KEY')
    require(bool(key) and bool(secret), 'credentials_missing')
    return key, secret


class _NoSourceCollection:
    def get(self, url):
        raise contract.PilotRefused('source_phase_required')


def _tick(bundle, journal, manifest, *, environ, now_ns, monotonic_ns,
          test_only, broker_factory):
    life, guard, pipeline, ledger = _local_components(bundle, now_ns, test_only)
    # No credential access occurs until compiled authority, sealed inventory,
    # anchored enrollment/startup, and all existing stop states have passed.
    key, secret = _credentials(environ)
    wire = FinitePilotTransport(key, secret, manifest=manifest, journal=journal,
                                test_only=test_only, transport_factory=broker_factory)
    source = PrimaryEarningsSource(bundle.paths['source'], transport=_NoSourceCollection(), now_ns=now_ns)
    dispatch = NativePaperDispatcher(life, guard, wire, enabled=True, now_ns=now_ns,
                                     monotonic_ns=monotonic_ns)
    port = NativePaperExecutionPort(evaluator=pipeline, lifecycle=life, dispatcher=dispatch)
    runner = DemoTickRunner(eventsource=source, marketsource=NativeMarketSource(port), executionport=port,
                            ledger=ledger, clock=lambda: _datetime_ceil(now_ns()),
                            config=RunnerConfig(enabled=True))
    coordinator = BoundedPaperRunner(runner, enabled=True)
    wire.bind(coordinator)
    native_scope = dispatch.tick_scope
    completed = []
    waiting_before_stage = []

    @contextmanager
    def anchored_scope():
        # Reuse the actual native scope rather than polling the broker twice or
        # substituting a second account lock. The coordinator remains unchanged.
        with native_scope():
            # Recovery uses the same quota window. A clean capacity wait must
            # precede the durable finite tick claim, while retaining the exact
            # account hold used by the coordinator's own subsequent admission.
            try:
                guard.admit_cycle(now_ns=now_ns())
            except AccountGuardRefused as exc:
                if str(exc) == 'cycle_budget_wait':
                    _pristine(bundle)
                    waiting_before_stage.append(True)
                raise
            journal.begin_stage(manifest, 'tick')
            yield dispatch
            # The coordinator catches strategy/transport errors. Its durable
            # dirty/attention/stop state is therefore checked before completion.
            _pristine(bundle)
            completed.append(journal.finish_stage(manifest, 'tick'))

    dispatch.tick_scope = anchored_scope
    result = coordinator.tick()
    if (waiting_before_stage == [True] and not completed
            and result.get('phase') == 'waiting_request_budget'):
        # No anchor, runner heartbeat, event or order claim was made. Existing
        # deadlines/cadence still apply; a later stale heartbeat is not refreshed.
        _pristine(bundle)
        return public('tick', 'bounded_tick_finished', outcome='waiting_request_budget',
                      events=0, exits=0)
    # An exception during cycle admission can yield a benign native budget wait
    # while leaving our anchor dirty. Only the actual completion receipt counts.
    if len(completed) != 1:
        return failure('tick', code='tick_completion_unproven')
    if _exhausted(completed[0], manifest):
        return failure('tick', code='finite_scope_exhausted')
    if result.get('phase') not in TICK_OUTCOMES:
        return failure('tick', code='unexpected_tick_outcome')
    return public('tick', 'bounded_tick_finished', outcome=result['phase'],
                  events=result.get('events', 0), exits=result.get('exits', 0))


def validate_recovery_evidence(manifest, *, bundle, guard, now_ns):
    """One fresh native GET observation while original runner halt is retained.

    Only AnchorJournal.recover_incident invokes this function in production,
    after committing the one-shot prepared record and taking seal/account holds.
    No lifecycle.observe, risk update, claim, dispatch, reset, or parsing change.
    """
    contract.require_recovery_manifest(manifest)
    bundle.verify()
    guard.check()
    started = now_ns()
    guard.admit_cycle(now_ns=started)
    # This exact read-only incident predates native ownership. A broader
    # position/order incident requires a different attended recovery protocol.
    with _read(bundle.paths['lifecycle']) as db:
        require(all(db.execute('SELECT COUNT(*) FROM ' + table).fetchone() == (0,)
                    for table in ('intents', 'cancels', 'native_dispatch')),
                'incident_native_history_not_empty')
    key, secret = _credentials(os.environ)
    transport = RecoveryReadOnlyTransport(key, secret, manifest=manifest, now_ns=now_ns,
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


def run_phase(phase, *, paths=None, test_only=False, environ=None, now_ns=None,
              monotonic_ns=None, broker_factory=None, source_factory=None, root_index=None):
    """Synthetic path/factory injection is test-only; no approval override exists."""
    require(phase in PHASES, 'invalid_arguments')
    manifest = contract.APPROVED_RECOVERY if phase == 'recover' else contract.APPROVED_PILOT
    if root_index is not None:
        from finite_root_contract import child
        require(phase in ('initialize','source','tick'),'root_phase_invalid');manifest=child(root_index)
        import finite_root_contract as rc
        require(rc.APPROVED_ROOT.smoke is None or phase!='source','smoke_source_forbidden')
    # This first branch must precede all filesystem, SQL, clock, credential and
    # network access, including construction of any state or transport object.
    if manifest is None:
        return public('disabled', 'compiled_recovery_approval_missing' if phase == 'recover'
                      else 'compiled_pilot_approval_missing')
    if phase == 'recover':
        contract.require_recovery_manifest(manifest)
    else:
        contract.require_pilot_manifest(manifest)
    require(type(test_only) is bool, 'invalid_test_mode')
    require(test_only or all(v is None for v in (paths, environ, now_ns, monotonic_ns,
                                                broker_factory, source_factory)),
            'production_overrides_forbidden')
    clock = time.time_ns if now_ns is None else now_ns
    monotonic = time.monotonic_ns if monotonic_ns is None else monotonic_ns
    env = os.environ if environ is None else environ
    bundle = resolve_approved_bundle(manifest, paths=paths, test_only=test_only)
    bundle.verify()
    journal = AnchorJournal(bundle, clock)
    if phase!='recover' and manifest.root_binding is not None:
        from finite_root_journal import RootJournal
        journal=RootJournal(bundle,clock)
        if phase=='initialize':
            from finite_root_worker import AdmissionJournal
            guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
            admission=AdmissionJournal(bundle,clock)
            admission.admit(manifest,guard=guard,environ=environ,test_only=test_only,broker_factory=broker_factory)
            admission.startup_gate(manifest)
            return public('initialize','pilot_enrolled')
    if phase!='recover' and manifest.session_admission is not None:
        from finite_pilot_sessions import SessionJournal
        journal=SessionJournal(bundle,clock)
    if phase == 'recover':
        # Recovery owns its own exclusive seal/account transaction. Ordinary
        # startup/initialize/source/tick never invoke it implicitly.
        guard = SharedAccountGuard(bundle.paths['legacy'], bundle.paths['lifecycle'], bundle.binding,
                                   enabled=True, state_database=bundle.paths['guard'], test_only=test_only,
                               runtime_bundle=bundle)
        journal.enroll_recovery(manifest)
        result = journal.recover_incident(manifest, guard=guard, validate=validate_recovery_evidence)
        return public('recover', 'incident_recovery_previously_applied'
                      if result['phase'] == 'already_recovered_readonly' else 'incident_recovered')
    if phase == 'initialize' and manifest.session_admission is not None:
        from finite_pilot_session_admission import SessionAdmissionJournal,validate_admission_evidence
        guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,
            enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
        admission=SessionAdmissionJournal(bundle,clock)
        admission.admit(manifest,guard=guard,validate=validate_admission_evidence)
        admission.startup_gate(manifest)
        return public('initialize','pilot_enrolled')
    if phase == 'initialize' and manifest.admission is not None:
        from finite_pilot_admission import AdmissionJournal,validate_admission_evidence
        guard = SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,
            enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
        admission = AdmissionJournal(bundle,clock)
        admission.admit(manifest,guard=guard,validate=validate_admission_evidence)
        admission.startup_gate(manifest)
        return public('initialize','pilot_enrolled')
    with serialized(bundle):
        if phase == 'initialize':
            # Enroll only an existing immutable cohort. Never create/re-create a
            # bundle and never clean the incident's dirty/attention/stop state.
            _pristine(bundle)
            _, guard, _, _ = _local_components(bundle, clock, test_only)
            with guard.hold(now_ns=clock(), read_only=True):
                journal.enroll_pilot(manifest)
                row = journal.startup_gate(manifest)
                _pristine(bundle)
            return public('manual_handoff', 'unresolved_exposure_requires_operator') if _exhausted(row, manifest) else public('initialize', 'pilot_enrolled')
        row = journal.startup_gate(manifest)
        _pristine(bundle)
        if _exhausted(row, manifest):
            return public('manual_handoff', 'unresolved_exposure_requires_operator')
        if phase == 'source':
            _, guard, _, _ = _local_components(bundle, clock, test_only)
            with guard.hold(now_ns=clock(), read_only=True):
                journal.begin_stage(manifest, 'source')
                transport = FiniteSourceTransport(manifest=manifest, journal=journal, guard=guard,
                                                 test_only=test_only, transport_factory=source_factory)
                source = PrimaryEarningsSource(bundle.paths['source'], transport=transport, now_ns=clock)
                result = source.collect()
                require(result.reason in ('collected_primary_evidence', 'no_earnings_in_feed'),
                        'source_stage_refused')
                _pristine(bundle)
                row = journal.finish_stage(manifest, 'source')
            if _exhausted(row, manifest):
                return public('manual_handoff', 'unresolved_exposure_requires_operator')
            return public('source', 'source_collected', source_events=result.event_count,
                          source_requests=result.request_count)
        if manifest.root_binding is not None:
            import finite_root_contract as rc
            if rc.APPROVED_ROOT.smoke is not None:
                from finite_execution_smoke import run_tick
                return run_tick(bundle,journal,manifest,environ=env,now_ns=clock,monotonic_ns=monotonic,test_only=test_only,broker_factory=broker_factory)
        return _tick(bundle, journal, manifest, environ=env, now_ns=clock, monotonic_ns=monotonic,
                     test_only=test_only, broker_factory=broker_factory)


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    root_index=None
    if len(args)==2 and args[1] in ('--root-child=0','--root-child=1'):
        root_index=int(args[1][-1]);args=args[:1]
    phase = args[0][2:] if len(args) == 1 and type(args[0]) is str and args[0].startswith('--') else ''
    if phase not in PHASES:
        print(json.dumps(public('invalid', 'invalid_arguments'), sort_keys=True), flush=True)
        return 2
    try:
        result = run_phase(phase) if root_index is None else run_phase(phase,root_index=root_index)
        print(json.dumps(result, sort_keys=True), flush=True)
        return 1 if result['phase'] == 'manual_handoff' else 0
    except Exception as exc:
        # Only fixed stage/code fields; historical cause remains unproven.
        print(json.dumps(failure(phase, exc), sort_keys=True), flush=True)
        return 1


if __name__ == '__main__':
    # Script entry must call the canonical imported implementation: the anchor
    # deliberately validates the exact canonical recovery callback identity.
    import finite_pilot_worker as _canonical_worker
    raise SystemExit(_canonical_worker.main())
