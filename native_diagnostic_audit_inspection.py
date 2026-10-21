"""Read the already-consumed diagnostic's existing audit. Never collect again.

No new claim, journal, repair, initializer, credentials or provider calls. The
fixed startup child runs at most once per server process; a restart may repeat
this nonmutating inspection. It is NOT a durable one-time execution grant.
"""
from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys

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


class InspectionRefused(ValueError):
    pass


def require(ok):
    if not ok:
        raise InspectionRefused('audit_inspection_unavailable')


def report(reason, *, audit_code='none', prior_owned_count=None):
    require(reason in ('inspection_disabled', 'existing_audit_observed',
                       'audit_inspection_unavailable', 'invalid_arguments'))
    require(audit_code in AUDIT_CODES | {'none', 'unclassified'})
    require(prior_owned_count is None or type(prior_owned_count) is int
            and 0 <= prior_owned_count <= 4)
    observed = reason == 'existing_audit_observed'
    require(observed or (audit_code == 'none' and prior_owned_count is None))
    return dict(mode='native_diagnostic_existing_audit', reason=reason,
                audit_code=audit_code, diagnostic_attempts=4 if observed else None,
                prior_attempts=6 if observed else None,
                prior_order_count=2 if observed else None,
                prior_owned_count=prior_owned_count,
                correlation='unique_post_claim_audit' if observed else 'none',
                provider_requests=0, order_permission='none', execution_ready=False,
                runtime_reset_performed=False, state_mutated=False)


def _frozen_code():
    here = Path(__file__).parent
    for name, expected in (
        ('native_schema_diagnostic.py', FROZEN_DIAGNOSTIC_SHA),
        ('native_observation_collector.py', frozen.COLLECTOR_SHA),
        ('alpaca_schema.py', frozen.SCHEMA_SHA),
    ):
        require(hashlib.sha256((here / name).read_bytes()).hexdigest() == expected)


def _bounded_int(value, maximum):
    return type(value) is int and 0 <= value <= maximum


def _read_evidence(anchor, guard, runner, lifecycle):
    require(frozen._table(anchor))
    claims = anchor.execute('SELECT id,generation,code_hash,claimed_ns,status,parser,'
                            'schema_code,requests FROM native_schema_diagnostic_claim').fetchmany(2)
    require(len(claims) == 1)
    claim = claims[0]
    require(claim[:3] == (1, frozen.GENERATION, FROZEN_DIAGNOSTIC_SHA)
            and _bounded_int(claim[3], 2**63 - 1) and claim[3] > 0
            and claim[4:] == ('collection_refused', 'none', 'none', 4))
    require(runner.execute('SELECT dirty,attention FROM runner_control WHERE id=1').fetchall()
            == [(1, 'readonly_runtime_stage_failed')])
    for table in ('intents', 'native_dispatch', 'cancels'):
        require(lifecycle.execute('SELECT COUNT(*) FROM ' + table).fetchone() == (0,))

    # A read transaction pins each DB snapshot. Require exactly one record after
    # the claim and that it is the most recent record. Missing, duplicate,
    # future/unrelated or partial rows never become a guessed attribution.
    recent = guard.execute('SELECT rowid,at_ns,status,attempts,owned_count,order_count,digest '
                           'FROM native_read_audit ORDER BY rowid DESC LIMIT 2').fetchall()
    require(len(recent) == 2)
    current, prior = recent
    require(all(_bounded_int(row[0], 2**63 - 1) and row[0] > 0
                and _bounded_int(row[1], 2**63 - 1) and row[1] > 0
                and _bounded_int(row[3], 12) and _bounded_int(row[4], 4)
                and _bounded_int(row[5], 499) for row in recent))
    require(current[0] > prior[0] and prior[1] < claim[3]
            <= current[1] <= claim[3] + MAX_CLAIM_TO_AUDIT_NS)
    require(current[3:] == (4, 0, 0, None))
    require(prior[2] == 'native_schema_refused' and prior[3] == 6
            and prior[5:] == (2, None))
    require(guard.execute('SELECT rowid FROM native_read_audit WHERE at_ns>=? LIMIT 2',
                         (claim[3],)).fetchall() == [(current[0],)])
    # Unknown values are redacted, not emitted or interpreted. Known fixed code
    # is the collector's refusal, not necessarily a lower-level root cause.
    code = current[2] if type(current[2]) is str and current[2] in AUDIT_CODES else 'unclassified'
    return report('existing_audit_observed', audit_code=code, prior_owned_count=prior[4])


def run_once(*, enabled=False, paths=None, test_only=False):
    require(type(enabled) is bool and type(test_only) is bool and (paths is None or test_only))
    if not enabled:
        return report('inspection_disabled')
    _frozen_code()
    resolved = FIXED_PATHS if paths is None else paths
    # The existing account binding is used privately; no fingerprint or IDs are
    # returned. Bundle resolves only existing nonsecret EVENT_DB in production.
    legacy = str(resolved['legacy'])
    with _database(legacy, _private(legacy)) as db:
        rows = db.execute('SELECT fingerprint FROM paper_readonly WHERE id=1').fetchmany(2)
        require(len(rows) == 1)
        binding = rows[0][0]
    bundle = RuntimeStateBundle(binding, paths=paths, test_only=test_only)
    bundle.verify()
    fd = os.open(bundle.paths['seal'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        require((os.fstat(fd).st_dev, os.fstat(fd).st_ino) == bundle._pinned['seal'])
        bundle.verify()
        with ExitStack() as stack:
            dbs = {role: stack.enter_context(_database(bundle.paths[role], bundle._pinned[role],
                        public_read=role == 'anchor'))
                   for role in ('anchor', 'guard', 'runner', 'lifecycle')}
            result = _read_evidence(**dbs)
            bundle.verify()
        return result
    finally:
        os.close(fd)


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if args != ['--inspect-existing-audit']:
        result = report('invalid_arguments')
    else:
        try:
            result = run_once(enabled=True)
        except Exception:
            result = report('audit_inspection_unavailable')
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result['reason'] == 'existing_audit_observed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
