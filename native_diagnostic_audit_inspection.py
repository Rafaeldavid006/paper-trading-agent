"""Read the already-consumed diagnostic's existing audit. Never collect again.

No new claim, journal, repair, initializer, credentials or provider calls. The
fixed startup child runs at most once per server process; a restart may repeat
this nonmutating inspection. It is NOT a durable one-time execution grant.
"""
from contextlib import ExitStack, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import sqlite3
import runtime_state_bundle as bundle_module
from native_observation_worker import ProbeRefused
from lifecycle_storage import StorageError
from alpaca_schema import SchemaError

from runtime_state_bundle import RuntimeStateBundle, FIXED_PATHS, _database, _private
import native_schema_diagnostic as frozen

FROZEN_DIAGNOSTIC_SHA = 'd27203d78fd10e74aaccb1266a7f789c64b7fff6b3fc5674e921808e045c0997'
MAX_CLAIM_TO_AUDIT_NS = 15 * 10**9  # Original diagnostic supervisor bound.
# Literal output vocabulary, not imported or copied from arbitrary database text.
AUDIT_CODES = frozenset((
    'budget_clock_regression', 'budget_state_corrupt',
    'budget_storage_unavailable', 'collection_deadline_exceeded',
    'collection_failed', 'conflicting_response_account',
    'invalid_local_clock', 'invalid_provider_payload', 'invalid_provider_status',
    'local_clock_regression', 'authentication_or_entitlement_failed',
    'provider_http_error', 'provider_rate_limited', 'provider_read_failed',
    'redirect_refused', 'response_too_large', 'partial_response',
    'shared_request_budget_exhausted', 'unsupported_encoding',
    'forbidden_method', 'forbidden_origin', 'forbidden_route',
    'invalid_database_file', 'invalid_timeout', 'transport_disabled',
))


STAGES = frozenset(('none', 'arguments', 'code_diagnostic', 'code_collector', 'code_schema',
    'legacy_header', 'legacy_binding', 'legacy_close', 'bundle_construct', 'bundle_verify_before_lock',
    'seal_open', 'seal_lock', 'seal_identity', 'bundle_verify_locked',
    'open_anchor', 'open_guard', 'open_runner', 'open_lifecycle',
    'claim_schema', 'claim_record', 'runner_halt', 'native_empty',
    'audit_rows', 'audit_shape', 'audit_time', 'audit_current', 'audit_prior',
    'audit_unique', 'bundle_verify_final', 'close_read_transactions', 'seal_close', 'complete'))
ROLES = frozenset(('none', 'bundle', 'anchor', 'legacy', 'legacy_lock', 'source',
                  'lifecycle', 'guard', 'decision', 'runner', 'seal', 'manifest'))
LOCAL_CODES = frozenset(('inspection_refused', 'invalid_arguments', 'code_pin_mismatch',
    'legacy_binding_unavailable', 'seal_identity_mismatch', 'claim_schema_missing',
    'claim_row_count', 'claim_contract_mismatch', 'runner_halt_mismatch',
    'native_state_not_empty', 'audit_history_incomplete', 'audit_shape_invalid',
    'audit_window_mismatch', 'audit_current_mismatch', 'audit_prior_mismatch',
    'audit_match_not_unique', 'file_missing', 'permission_refused', 'lock_busy',
    'os_error', 'sqlite_operational_error', 'sqlite_database_error', 'value_error',
    'missing_key', 'type_error', 'unexpected_error', 'diagnostic_refused'))
BUNDLE_CODES = frozenset(('bundle_account_mismatch', 'bundle_already_or_partially_provisioned', 'bundle_anchor_binding_mismatch', 'bundle_anchor_corrupt', 'bundle_anchor_history_changed', 'bundle_anchor_incomplete_or_changed', 'bundle_anchor_provision_incomplete', 'bundle_anchor_replaced', 'bundle_anchor_schema_invalid', 'bundle_anchor_unavailable', 'bundle_budget_state_corrupt', 'bundle_database_corrupt', 'bundle_decision_state_corrupt', 'bundle_dispatch_state_corrupt', 'bundle_existing_generation', 'bundle_existing_history_required', 'bundle_file_missing', 'bundle_file_replaced', 'bundle_generation_mismatch', 'bundle_generation_or_account_mismatch', 'bundle_guard_binding_mismatch', 'bundle_guard_state_corrupt', 'bundle_intent_state_corrupt', 'bundle_inventory_or_schema_changed', 'bundle_lifecycle_state_corrupt', 'bundle_manifest_binding_mismatch', 'bundle_manifest_corrupt', 'bundle_manifest_replaced', 'bundle_provision_failed', 'bundle_provision_incomplete', 'bundle_risk_state_corrupt', 'bundle_runner_state_corrupt', 'bundle_schema_incomplete', 'bundle_schema_invalid', 'bundle_session_state_corrupt', 'bundle_source_state_corrupt', 'bundle_state_corrupt', 'bundle_state_unavailable', 'bundle_state_uninitialized', 'canonical_bundle_path_required', 'complete_inventory_required', 'existing_anchor_enrollment_required', 'existing_event_db_configuration_required', 'existing_event_db_persistent_path_required', 'fixed_bundle_paths_required', 'invalid_account_binding', 'invalid_bundle_initializer', 'invalid_bundle_path', 'invalid_test_mode', 'legacy_lock_path_mismatch', 'reviewed_provision_required', 'separate_bundle_files_required', 'unsafe_bundle_file'))
STORAGE_CODES = frozenset(('invalid_binding', 'inconsistent_ledger', 'source_unavailable',
    'state_unavailable', 'probe_failed', 'invalid_stored_projection', 'projection_too_large'))
ERROR_CODES = LOCAL_CODES | BUNDLE_CODES | STORAGE_CODES | frozen.SCHEMA_CODES | {'none'}


class InspectionRefused(ValueError):
    pass


class InspectionFailure(InspectionRefused):
    """Only sanitized local enums; never retains the original exception."""
    def __init__(self, stage, role, code):
        self.stage = stage if stage in STAGES else 'none'
        self.role = role if role in ROLES else 'none'
        self.code = code if code in ERROR_CODES else 'unexpected_error'
        super().__init__('audit_inspection_unavailable')


def require(ok, code='inspection_refused'):
    if not ok:
        raise InspectionRefused(code if code in LOCAL_CODES else 'inspection_refused')


def _safe_error(exc):
    if type(exc) in (InspectionRefused, bundle_module.RuntimeStateRefused, ProbeRefused,
                     StorageError, SchemaError, ValueError):
        code = exc.args[0] if len(exc.args) == 1 and type(exc.args[0]) is str else None
        if code in ERROR_CODES:
            return code
    for kind, code in ((FileNotFoundError, 'file_missing'), (PermissionError, 'permission_refused'),
                       (BlockingIOError, 'lock_busy'), (sqlite3.OperationalError, 'sqlite_operational_error'),
                       (sqlite3.DatabaseError, 'sqlite_database_error'), (OSError, 'os_error'),
                       (KeyError, 'missing_key'), (TypeError, 'type_error'), (ValueError, 'value_error')):
        if isinstance(exc, kind):
            return code
    return 'unexpected_error'


class Trace:
    def __init__(self):
        self.stage, self.role, self.first = 'arguments', 'none', None
        self.paths = {}
    def enter(self, stage, role='none'):
        require(stage in STAGES and role in ROLES)
        self.stage, self.role = stage, role
    def fail(self, exc, role=None):
        if self.first is None:
            self.first = (self.stage, role if role in ROLES else self.role, _safe_error(exc))
    def exception(self, exc):
        self.fail(exc)
        return InspectionFailure(*self.first)


@contextmanager
def instrument_bundle_reads(trace):
    """Dedicated child only. Observe original checks, never suppress or replace them.

    First failure wins even if bundle.verify wraps it or cleanup later fails.
    Private paths are used only to choose a literal role, never emitted. Helpers
    are restored on every exit; no original module bytes or store are changed.
    """
    original_private, original_db = bundle_module._private, bundle_module._database
    original_require = bundle_module._require
    original_seal = RuntimeStateBundle._read_seal
    original_registry = bundle_module.load_owned_ledger
    def role(path):
        return trace.paths.get(str(path), trace.role)
    def private(path, **kwargs):
        previous = trace.role
        trace.role = role(path)
        try:
            return original_private(path, **kwargs)
        except Exception as exc:
            trace.fail(exc); raise
        finally:
            trace.role = previous
    @contextmanager
    def database(path, identity, **kwargs):
        previous = trace.role
        current = role(path)
        trace.role = current
        try:
            with original_db(path, identity, **kwargs) as db:
                try:
                    yield db
                finally:
                    # ExitStack closes several yielded DB contexts in reverse.
                    # Restore THIS role before the original exit checks run.
                    trace.role = current
        except Exception as exc:
            trace.fail(exc, current); raise
        finally:
            trace.role = previous
    def checked(ok, code):
        try:
            return original_require(ok, code)
        except Exception as exc:
            trace.fail(exc); raise
    def read_seal(self, identity):
        previous = trace.role
        trace.role = 'seal'
        try:
            return original_seal(self, identity)
        except Exception as exc:
            trace.fail(exc); raise
        finally:
            trace.role = previous
    def read_registry(*args, **kwargs):
        previous = trace.role
        trace.role = 'legacy'
        try:
            return original_registry(*args, **kwargs)
        except Exception as exc:
            trace.fail(exc); raise
        finally:
            trace.role = previous
    bundle_module._private, bundle_module._database, bundle_module._require = private, database, checked
    RuntimeStateBundle._read_seal = read_seal
    bundle_module.load_owned_ledger = read_registry
    try:
        yield
    finally:
        bundle_module._private, bundle_module._database, bundle_module._require = original_private, original_db, original_require
        RuntimeStateBundle._read_seal = original_seal
        bundle_module.load_owned_ledger = original_registry


def report(reason, *, audit_code='none', prior_owned_count=None,
           inspection_stage='none', inspection_role='none', inspection_code='none'):
    require(reason in ('inspection_disabled', 'existing_audit_observed',
                       'audit_inspection_unavailable', 'invalid_arguments'))
    require(audit_code in AUDIT_CODES | {'none', 'unclassified'})
    require(prior_owned_count is None or type(prior_owned_count) is int
            and 0 <= prior_owned_count <= 4)
    require(inspection_stage in STAGES and inspection_role in ROLES and inspection_code in ERROR_CODES)
    observed = reason == 'existing_audit_observed'
    require(observed or (audit_code == 'none' and prior_owned_count is None))
    return dict(mode='native_diagnostic_existing_audit', reason=reason,
                audit_code=audit_code, diagnostic_attempts=4 if observed else None,
                prior_attempts=6 if observed else None,
                prior_order_count=2 if observed else None,
                prior_owned_count=prior_owned_count,
                correlation='unique_post_claim_audit' if observed else 'none',
                inspection_stage=inspection_stage, inspection_role=inspection_role,
                inspection_code=inspection_code,
                provider_requests=0, order_permission='none', execution_ready=False,
                runtime_reset_performed=False, state_mutated=False)


def _frozen_code(trace=None):
    here = Path(__file__).parent
    for name, expected, stage in (
        ('native_schema_diagnostic.py', FROZEN_DIAGNOSTIC_SHA, 'code_diagnostic'),
        ('native_observation_collector.py', frozen.COLLECTOR_SHA, 'code_collector'),
        ('alpaca_schema.py', frozen.SCHEMA_SHA, 'code_schema'),
    ):
        if trace is not None: trace.enter(stage)
        require(hashlib.sha256((here / name).read_bytes()).hexdigest() == expected, 'code_pin_mismatch')


def _bounded_int(value, maximum):
    return type(value) is int and 0 <= value <= maximum


def _read_evidence(anchor, guard, runner, lifecycle, trace=None):
    trace = trace or Trace()
    trace.enter('claim_schema', 'anchor')
    require(frozen._table(anchor), 'claim_schema_missing')
    trace.enter('claim_record', 'anchor')
    claims = anchor.execute('SELECT id,generation,code_hash,claimed_ns,status,parser,'
                            'schema_code,requests FROM native_schema_diagnostic_claim').fetchmany(2)
    require(len(claims) == 1, 'claim_row_count')
    claim = claims[0]
    require(claim[:3] == (1, frozen.GENERATION, FROZEN_DIAGNOSTIC_SHA)
            and _bounded_int(claim[3], 2**63 - 1) and claim[3] > 0
            and claim[4:] == ('collection_refused', 'none', 'none', 4), 'claim_contract_mismatch')
    trace.enter('runner_halt', 'runner')
    require(runner.execute('SELECT dirty,attention FROM runner_control WHERE id=1').fetchall()
            == [(1, 'readonly_runtime_stage_failed')], 'runner_halt_mismatch')
    trace.enter('native_empty', 'lifecycle')
    for table in ('intents', 'native_dispatch', 'cancels'):
        require(lifecycle.execute('SELECT COUNT(*) FROM ' + table).fetchone() == (0,), 'native_state_not_empty')

    trace.enter('audit_rows', 'guard')
    recent = guard.execute('SELECT rowid,at_ns,status,attempts,owned_count,order_count,digest '
                           'FROM native_read_audit ORDER BY rowid DESC LIMIT 2').fetchall()
    require(len(recent) == 2, 'audit_history_incomplete')
    current, prior = recent
    trace.enter('audit_shape', 'guard')
    require(all(_bounded_int(row[0], 2**63 - 1) and row[0] > 0
                and _bounded_int(row[1], 2**63 - 1) and row[1] > 0
                and _bounded_int(row[3], 12) and _bounded_int(row[4], 4)
                and _bounded_int(row[5], 499) for row in recent), 'audit_shape_invalid')
    trace.enter('audit_time', 'guard')
    require(current[0] > prior[0] and prior[1] < claim[3]
            <= current[1] <= claim[3] + MAX_CLAIM_TO_AUDIT_NS, 'audit_window_mismatch')
    trace.enter('audit_current', 'guard')
    require(current[3:] == (4, 0, 0, None), 'audit_current_mismatch')
    trace.enter('audit_prior', 'guard')
    require(prior[2] == 'native_schema_refused' and prior[3] == 6
            and prior[5:] == (2, None), 'audit_prior_mismatch')
    trace.enter('audit_unique', 'guard')
    require(guard.execute('SELECT rowid FROM native_read_audit WHERE at_ns>=? LIMIT 2',
                         (claim[3],)).fetchall() == [(current[0],)], 'audit_match_not_unique')
    code = current[2] if type(current[2]) is str and current[2] in AUDIT_CODES else 'unclassified'
    return report('existing_audit_observed', audit_code=code, prior_owned_count=prior[4],
                  inspection_stage='complete')


def run_once(*, enabled=False, paths=None, test_only=False):
    trace = Trace()
    fd, stack = None, None
    try:
        require(type(enabled) is bool and type(test_only) is bool and (paths is None or test_only), 'invalid_arguments')
        if not enabled:
            return report('inspection_disabled')
        _frozen_code(trace)
        resolved = FIXED_PATHS if paths is None else paths
        trace.paths = {str(v): k for k, v in resolved.items() if k in ROLES}
        trace.enter('legacy_header', 'legacy')
        legacy = str(resolved['legacy'])
        with _database(legacy, _private(legacy)) as db:
            try:
                trace.enter('legacy_binding', 'legacy')
                rows = db.execute('SELECT fingerprint FROM paper_readonly WHERE id=1').fetchmany(2)
                require(len(rows) == 1, 'legacy_binding_unavailable')
                binding = rows[0][0]
            except Exception as exc:
                trace.fail(exc); raise
            finally:
                trace.enter('legacy_close', 'legacy')
        trace.enter('bundle_construct', 'anchor')
        bundle = RuntimeStateBundle(binding, paths=paths, test_only=test_only)
        trace.paths = {str(v): k for k, v in bundle.paths.items()}
        with instrument_bundle_reads(trace):
            trace.enter('bundle_verify_before_lock', 'bundle')
            bundle.verify()
            trace.enter('seal_open', 'seal')
            fd = os.open(bundle.paths['seal'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            trace.enter('seal_lock', 'seal')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            trace.enter('seal_identity', 'seal')
            require((os.fstat(fd).st_dev, os.fstat(fd).st_ino) == bundle._pinned['seal'], 'seal_identity_mismatch')
            trace.enter('bundle_verify_locked', 'bundle')
            bundle.verify()
            stack = ExitStack()
            dbs = {}
            for role in ('anchor', 'guard', 'runner', 'lifecycle'):
                trace.enter('open_' + role, role)
                dbs[role] = stack.enter_context(bundle_module._database(bundle.paths[role], bundle._pinned[role],
                                  public_read=role == 'anchor'))
            try:
                result = _read_evidence(**dbs, trace=trace)
                trace.enter('bundle_verify_final', 'bundle')
                bundle.verify()
            except Exception as exc:
                trace.fail(exc); raise
            finally:
                trace.enter('close_read_transactions', 'bundle')
                closing, stack = stack, None
                closing.close()
            trace.enter('seal_close', 'seal')
            closing_fd, fd = fd, None
            os.close(closing_fd)
            return result
    except Exception as exc:
        trace.fail(exc)
    finally:
        # First failure remains authoritative if closing another resource fails.
        if stack is not None:
            try:
                trace.enter('close_read_transactions', 'bundle')
                closing, stack = stack, None
                closing.close()
            except Exception as exc: trace.fail(exc)
        if fd is not None:
            try:
                trace.enter('seal_close', 'seal')
                closing_fd, fd = fd, None
                os.close(closing_fd)
            except Exception as exc: trace.fail(exc)
    raise InspectionFailure(*trace.first) from None


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if args != ['--inspect-existing-audit']:
        result = report('invalid_arguments', inspection_stage='arguments', inspection_code='invalid_arguments')
    else:
        try:
            result = run_once(enabled=True)
        except InspectionFailure as exc:
            result = report('audit_inspection_unavailable', inspection_stage=exc.stage,
                            inspection_role=exc.role, inspection_code=exc.code)
            if (exc.stage, exc.role, exc.code) == ('bundle_verify_before_lock',
                    'anchor', 'bundle_anchor_replaced'):
                # Comparisons only. Never resume the failed verifier or read the
                # audit as trusted evidence after an identity refusal.
                try:
                    from native_cohort_identity_evidence import inspect_identity_evidence
                    result['identity_evidence'] = inspect_identity_evidence(enabled=True)
                except Exception:
                    # A supplemental-reader failure cannot erase the original
                    # refusal. Never emit its exception or partial raw values.
                    result['identity_evidence'] = dict(status='untrusted_unavailable',
                        stage='supplement', code='supplement_unavailable', trusted=False,
                        runtime_gate_passed=False, provider_requests=0,
                        state_mutated=False, runtime_reset_performed=False)
        except Exception as exc:
            result = report('audit_inspection_unavailable', inspection_stage='arguments',
                            inspection_code=_safe_error(exc))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result['reason'] == 'existing_audit_observed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
