"""Default-disabled one-shot recovery successor. No pilot/startup/retry path.

Explicit --recover atomically provisions the exact journal and consumes the
attempt before provider access. Original enrollment and receipts are immutable.
"""
from contextlib import ExitStack
from decimal import Decimal
import hashlib
import json
import math
import os
import re
import sys
import time
import finite_pilot_contract as contract
import finite_recovery_successor_contract as successor
from finite_pilot_contract import require, digest, NS
from finite_pilot_anchor import AnchorJournal, _bounded, _schema, _json
import runtime_state_bundle as inventory
from readonly_runtime_worker import serialized
from native_account_guard import SharedAccountGuard
from native_paper_execution import ExecutionObservation, NativeExecutionObserver, CoordinatedWire
from decimal_lifecycle import Lifecycle, SessionRiskPolicy
from alpaca_schema import parse_latest_quotes, fresh
from finite_pilot_transport import _read
from finite_recovery_successor_transport import SuccessorReadOnlyTransport


def _require(ok):
    require(ok, 'successor_anchor_snapshot_invalid')


def current_anchor_digest(db):
    """Versioned logical snapshot of every schema object and table row.

    Exact successor table is excluded only after strict schema validation.
    Caller owns a pinned read transaction. No writes,
    row omission, account output, or old founding-file-hash substitution.
    Rowids are evidence where present; WITHOUT ROWID tables use stored columns.
    Explicit value tags preserve SQLite NULL/integer/real/text/blob distinctions.
    """
    _schema(db)
    objects=_bounded(db,"SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name",limit=256)
    objects=[r for r in objects if r[1] != successor.TABLE]
    _require(all(r[0] in ('table','index') and type(r[1]) is str
                 and (r[3] is None or type(r[3]) is str) for r in objects))
    _require(all(r[3] is not None and not r[3].lstrip().upper().startswith('CREATE VIRTUAL')
                 for r in objects if r[0]=='table'))
    table_flags=db.execute('PRAGMA main.table_list').fetchmany(257)
    _require(table_flags and len(table_flags)<=256
             and all(len(r)>=6 and r[0]=='main' and type(r[1]) is str
                     and r[4] in (0,1) for r in table_flags))
    flags={r[1]:r[4] for r in table_flags}
    _require(len(flags)==len(table_flags))
    remaining_rows=20000
    remaining_bytes=16*1024*1024
    h=hashlib.sha256()
    def typed(value):
        if value is None:return ['null']
        if type(value) is int:return ['integer',str(value)]
        if type(value) is float:
            _require(math.isfinite(value))
            return ['real',value.hex()]
        if type(value) is str:return ['text',value]
        if type(value) is bytes:return ['blob',value.hex()]
        raise ValueError('report_refused')
    def encoded(value):
        return json.dumps(value,ensure_ascii=True,separators=(',',':'),allow_nan=False).encode('ascii')
    def add(value):
        nonlocal remaining_bytes
        raw=encoded(value);remaining_bytes-=len(raw)
        _require(remaining_bytes>=0)
        h.update(str(len(raw)).encode('ascii')+b':'+raw)
    add('anchor-logical-v1')
    add([[typed(v) for v in r] for r in objects])
    for kind,name,_,sql in objects:
        if kind!='table':continue
        quoted='"'+name.replace('"','""')+'"'
        columns=db.execute('PRAGMA table_xinfo('+quoted+')').fetchall()
        _require(columns and len(columns)<=128)
        names={r[1].lower() for r in columns}
        _require(name in flags)
        without_rowid=bool(flags[name])
        aliases=[v for v in ('_rowid_','rowid','oid') if v not in names]
        _require(without_rowid or bool(aliases))
        selector='*' if without_rowid else aliases[0]+',*'
        rows=_bounded(db,'SELECT '+selector+' FROM '+quoted,limit=remaining_rows)
        remaining_rows-=len(rows)
        # Sort canonical encoded rows, not SQLite collation or query-plan order.
        ordered=sorted((encoded([typed(v) for v in row]) for row in rows))
        add([name,'without-rowid' if without_rowid else 'with-rowid',len(ordered)])
        for row in ordered:
            remaining_bytes-=len(row);_require(remaining_bytes>=0)
            h.update(str(len(row)).encode('ascii')+b':'+row)
    return h.hexdigest()


def resolve_successor_bundle(manifest, *, paths=None, test_only=False):
    """Private account lookup against an already approved seal/foundation.

    Never returns a public proof, creates a manifest, reads credentials, or
    permits path overrides in production. Account identity stays inside the
    private verified bundle rather than in public hash-only inspection output.
    """
    successor.require_successor_manifest(manifest)
    require(type(test_only) is bool and (test_only or paths is None), 'fixed_paths_required')
    resolved = inventory.FIXED_PATHS if paths is None else paths
    path = resolved['seal']
    identity = inventory._private(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        stat = os.fstat(fd)
        require((stat.st_dev, stat.st_ino) == identity, 'manifest_seal_mismatch')
        raw = os.read(fd, 4097)
    finally:
        os.close(fd)
    require(len(raw) <= 4096 and hashlib.sha256(raw).hexdigest() == manifest.bundle_seal_sha256,
            'manifest_seal_mismatch')
    seal = inventory._load(raw)
    require(type(seal) is dict and seal.get('generation') == manifest.bundle_generation
            and contract._hash(seal.get('fingerprint')), 'manifest_seal_mismatch')
    bundle = inventory.RuntimeStateBundle(seal['fingerprint'], paths=paths, test_only=test_only)
    bundle.verify()
    require(bundle._pinned['seal'] == identity, 'manifest_seal_mismatch')
    journal = AnchorJournal(bundle)
    journal._verify(manifest)
    with journal._db() as db:
        journal._binding_row(db, manifest)
    return bundle


def _credentials(environ):
    key, secret = environ.get('ALPACA_API_KEY'), environ.get('ALPACA_SECRET_KEY')
    require(bool(key) and bool(secret), 'credentials_missing')
    return key, secret


def validate_successor_evidence(manifest, *, bundle, guard, now_ns):
    """One fresh native GET observation while original runner halt is retained.

    Only SuccessorJournal.recover invokes this function in production,
    after committing the one-shot prepared record and taking seal/account holds.
    No lifecycle.observe, risk update, claim, dispatch, reset, or parsing change.
    """
    successor.require_successor_manifest(manifest)
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
    transport = SuccessorReadOnlyTransport(key, secret, manifest=manifest, now_ns=now_ns,
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


class SuccessorJournal(AnchorJournal):
    def _original(self, db, manifest):
        self._binding_row(db, manifest)
        require(_bounded(db, 'SELECT * FROM finite_pilot_recovery', limit=1)
                == [(1, manifest.predecessor_digest, manifest.predecessor_document)],
                'successor_predecessor_changed')
        require(not _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal', limit=2),
                'successor_original_attempt_exists')
        require(all(not _bounded(db, 'SELECT * FROM ' + table, limit=1) for table in
                    ('finite_pilot_enrollment','finite_pilot_control','finite_pilot_stages',
                     'finite_pilot_writes','finite_pilot_sources')), 'successor_pilot_state_present')
        require(current_anchor_digest(db) == manifest.current_anchor_sha256,
                'successor_current_anchor_changed')

    def recover(self, manifest, *, guard, validate):
        successor.require_successor_manifest(manifest)
        import finite_recovery_successor_worker as canonical
        require(validate is canonical.validate_successor_evidence, 'reviewed_successor_validator_required')
        require(type(guard) is SharedAccountGuard and guard.binding == self.bundle.binding
                and guard.legacy_database == self.bundle.paths['legacy']
                and guard.lifecycle_database == self.bundle.paths['lifecycle']
                and guard.state_database == self.bundle.paths['guard'], 'recovery_guard_mismatch')
        with serialized(self.bundle):
            self._verify(manifest)
            with self._db() as db:
                self._original(db, manifest)
                require(not db.execute('SELECT 1 FROM sqlite_master WHERE name=?',
                                       (successor.TABLE,)).fetchone(), 'successor_attempt_consumed')
            with guard.hold(now_ns=self._now(manifest), read_only=True):
                original_runner = self._controls(recovery=manifest)['runner']
                protected = self.protected_state_digest()
                require(protected == manifest.protected_state_sha256, 'recovery_protected_state_changed')
                application_before = self.application_state_digest()
                before = self._guard_snapshot()
                require(self.cohort_state_digest() == manifest.pre_enrollment_cohort_sha256,
                        'recovery_initial_cohort_changed')
                prepared = self._now(manifest)
                proposed = (1, prepared / NS, 0, 0, None, 'recovered_readonly')
                require(proposed[1] >= original_runner[1], 'recovery_clock_regression')
                document = _json(manifest.document())
                receipt = (1, 'prepared', prepared, digest(manifest.document()), document,
                           manifest.predecessor_digest, _json(original_runner), _json(proposed),
                           protected, digest(before), None)
                with self._db(write=True) as db:
                    self._original(db, manifest)
                    require(not db.execute('SELECT 1 FROM sqlite_master WHERE name=?',
                                           (successor.TABLE,)).fetchone(), 'successor_attempt_consumed')
                    self._now(manifest, prepared)
                    # Schema provision and durable attempt are one transaction.
                    # There is never an enrolled/pending reusable successor.
                    db.execute(successor.SQL)
                    db.execute('INSERT INTO ' + successor.TABLE + ' VALUES(?,?,?,?,?,?,?,?,?,?,?)', receipt)
                self._checkpoint('successor_prepared')
                evidence = validate(manifest, bundle=self.bundle, guard=guard, now_ns=self.now_ns)
                require(type(evidence) is contract.RecoveryEvidence
                        and type(evidence.observation) is ExecutionObservation, 'typed_recovery_evidence_required')
                now = self._now(manifest, prepared)
                require(contract._ns(evidence.started_ns) and contract._ns(evidence.finished_ns)
                        and prepared <= evidence.started_ns <= evidence.observation.observed_ns
                        <= evidence.finished_ns <= now and now - evidence.started_ns <= 10 * NS
                        and type(evidence.request_count) is int and 1 <= evidence.request_count <= 12
                        and contract._hash(evidence.observation.native_digest)
                        and evidence.observation.legacy_revision == guard.legacy_revision,
                        'recovery_evidence_stale_or_invalid')
                self._checkpoint('successor_validated')
                with ExitStack() as stack:
                    handles = {}
                    for role in ('runner', 'decision', 'lifecycle', 'source', 'guard', 'anchor'):
                        handles[role] = stack.enter_context(inventory._database(
                            self.bundle.paths[role], self.bundle._pinned[role], write=True, public_read=role == 'anchor'))
                    self._verify(manifest)
                    self._controls(recovery=manifest)
                    guard.check()
                    require(self.protected_state_digest() == protected, 'recovery_protected_state_changed')
                    require(self.application_state_digest() == application_before, 'recovery_application_state_changed')
                    after = self._guard_snapshot()
                    self._guard_delta(before, after, evidence, prepared, guard)
                    now = self._now(manifest, now)
                    require(now - evidence.started_ns <= 10 * NS, 'recovery_evidence_stale_or_invalid')
                    db = handles['anchor']
                    self._original(db, manifest)
                    require(_bounded(db, 'SELECT * FROM ' + successor.TABLE + ' ORDER BY seq', limit=2)
                            == [receipt], 'successor_receipt_changed')
                    runner = handles['runner']
                    changed = runner.execute("UPDATE runner_control SET heartbeat=?,dirty=0,attention=NULL,stage='recovered_readonly' "
                        'WHERE id=? AND heartbeat=? AND dirty=? AND stopped=? AND attention=? AND stage=?',
                        (proposed[1], *original_runner)).rowcount
                    require(changed == 1 and inventory._singleton(runner, 'runner_control') == proposed,
                            'recovery_compare_and_set_failed')
                    self._checkpoint('successor_cas_before_commit')
                    runner.commit()
                    self._checkpoint('successor_cas_committed')
                    runner.execute('BEGIN IMMEDIATE')
                    require(inventory._singleton(runner, 'runner_control') == proposed, 'recovery_runner_changed')
                    self._verify(manifest)
                    require(self.protected_state_digest() == protected, 'recovery_protected_state_changed')
                    require(self.application_state_digest() == application_before, 'recovery_application_state_changed')
                    self._controls(active=False)
                    final_ns = self._now(manifest, now)
                    require(final_ns - evidence.started_ns <= 10 * NS, 'recovery_evidence_stale_or_invalid')
                    self._original(db, manifest)
                    require(_bounded(db, 'SELECT * FROM ' + successor.TABLE + ' ORDER BY seq', limit=2)
                            == [receipt], 'successor_receipt_changed')
                    evidence_digest = digest((evidence.started_ns, evidence.finished_ns, evidence.request_count,
                                              evidence.observation.observed_ns, evidence.observation.native_digest))
                    applied = (2, 'applied', final_ns, receipt[3], receipt[4], receipt[5], receipt[6],
                               receipt[7], protected, digest(after), evidence_digest)
                    db.execute('INSERT INTO ' + successor.TABLE + ' VALUES(?,?,?,?,?,?,?,?,?,?,?)', applied)
                    self._checkpoint('successor_applied_before_commit')
                    db.commit()
                    self._checkpoint('successor_applied')
        return dict(mode='finite_recovery_successor', phase='recovered_readonly',
                    execution_ready=False, order_permission='none', automatic_retry=False)


def run_successor(*, paths=None, test_only=False, now_ns=None):
    manifest = successor.APPROVED_SUCCESSOR
    if manifest is None:
        return dict(mode='finite_recovery_successor', phase='disabled', order_permission='none',
                    execution_ready=False, automatic_retry=False)
    successor.require_successor_manifest(manifest)
    require(type(test_only) is bool and (test_only or (paths is None and now_ns is None)),
            'production_overrides_forbidden')
    clock = time.time_ns if now_ns is None else now_ns
    # Reject early/expired before opening the bundle or credentials.
    now = clock()
    require(type(now) is int and manifest.starts_ns <= now < manifest.expires_ns, 'successor_window_closed')
    bundle = resolve_successor_bundle(manifest, paths=paths, test_only=test_only)
    journal = SuccessorJournal(bundle, clock)
    guard = SharedAccountGuard(bundle.paths['legacy'], bundle.paths['lifecycle'], bundle.binding,
                               enabled=True, state_database=bundle.paths['guard'], test_only=test_only,
                               runtime_bundle=bundle)
    return journal.recover(manifest, guard=guard, validate=validate_successor_evidence)


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if args != ['--recover']:
        print(json.dumps(dict(mode='finite_recovery_successor', phase='invalid', order_permission='none')))
        return 2
    try:
        result = run_successor()
    except Exception:
        # Never emit exception text, paths, account data or private receipt rows.
        result = dict(mode='finite_recovery_successor', phase='manual_handoff',
                      execution_ready=False, order_permission='none', automatic_retry=False,
                      owned_exposure='not_proven_flat')
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    import finite_recovery_successor_worker as _canonical_worker
    raise SystemExit(_canonical_worker.main())
