"""One-time LOCAL empty-journal initialization and hash proof; disabled and unpublished.

This is not the frozen diagnostic. It performs no provider/source requests,
credential access, state recovery, order action, pilot/recovery enrollment, or
runtime-cohort enrollment. It does write an empty pilot journal and a one-shot
inspection receipt, and is not read-only overall. Only a
separately reviewed compiled inspection manifest can admit the fixed phase.
"""
from contextlib import ExitStack
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time

import finite_pilot_contract as contract
from finite_pilot_anchor import AnchorJournal
from native_account_guard import SharedAccountGuard
from native_observation_worker import load_owned_ledger
from readonly_runtime_worker import serialized
import runtime_state_bundle as inventory

TABLE = 'finite_inspection_receipt'
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_WORK_NS = 8 * 10**9
SQL = ('CREATE TABLE finite_inspection_receipt(id INTEGER PRIMARY KEY CHECK(id=1),'
       'generation TEXT NOT NULL,manifest_digest TEXT NOT NULL,status TEXT NOT NULL,'
       'claimed_ns INTEGER NOT NULL,completed_ns INTEGER,anchor_device INTEGER NOT NULL,'
       'anchor_inode INTEGER NOT NULL)')
PROOF_KEYS = frozenset(('original_runner_sha256', 'protected_state_sha256',
                       'pre_enrollment_cohort_sha256', 'bundle_seal_sha256',
                       'anchor_pre_enrollment_sha256'))


def require(ok, code):
    if not ok:
        raise contract.PilotRefused(code)


def public(phase, proof=None):
    require(phase in ('disabled', 'inspection_complete', 'inspection_consumed', 'inspection_refused', 'invalid'),
            'inspection_summary_invalid')
    change = ('empty_journal_initialization' if phase == 'inspection_complete' else
              'journal_initialization_unconfirmed' if phase == 'inspection_refused' else 'none')
    result = dict(mode='finite_proof_initialization', phase=phase, order_permission='none',
                  state_change=change, execution_ready=False, publication_authorized=False)
    if proof is not None:
        require(phase == 'inspection_complete' and type(proof) is dict and set(proof) == PROOF_KEYS
                and all(contract._hash(value) for value in proof.values()), 'inspection_summary_invalid')
        result['proof'] = dict(proof)
    return result


def _checkpoint(name):
    """Crash-test seam only. No callback supplied by production callers."""


def _hash_file(bundle, role):
    identity = bundle._pinned[role]
    require(inventory._private(bundle.paths[role], public_read=role == 'anchor') == identity,
            'inspection_file_changed')
    fd = os.open(bundle.paths[role], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require((info.st_dev, info.st_ino) == identity and 0 < info.st_size <= MAX_FILE_BYTES,
                'inspection_file_bound')
        result = hashlib.sha256()
        total = 0
        while True:
            raw = os.read(fd, 65536)
            if not raw:
                break
            total += len(raw)
            require(total <= MAX_FILE_BYTES, 'inspection_file_bound')
            result.update(raw)
        require(total == info.st_size, 'inspection_file_changed')
        return result.hexdigest()
    finally:
        os.close(fd)


def _controls(handles, guard):
    """Local incident shape, not a claim about the broker's current holdings."""
    row = inventory._singleton(handles['runner'], 'runner_control')
    require(len(row) == 6 and row[0] == 1 and type(row[1]) in (int, float)
            and math.isfinite(row[1]) and row[1] > 0 and row[2:] ==
            (1, 0, 'readonly_runtime_stage_failed', 'started'), 'inspection_incident_required')
    native = inventory._singleton(handles['lifecycle'], 'control')
    require(native[3:5] == (0, 0), 'inspection_native_control_refused')
    require(all(handles['lifecycle'].execute('SELECT COUNT(*) FROM ' + table).fetchone() == (0,)
                for table in ('intents', 'cancels', 'native_dispatch')), 'inspection_native_history_refused')
    require(inventory._singleton(handles['source'], 'primary_meta')[4] == 0,
            'inspection_source_busy')
    require(inventory._singleton(handles['decision'], 'demo_control')[1] == 0
            and handles['decision'].execute('SELECT 1 FROM demo_attention LIMIT 1').fetchone() is None
            and handles['decision'].execute('SELECT 1 FROM demo_intents LIMIT 1').fetchone() is None,
            'inspection_decision_control_refused')
    guard.check()
    # Read from the already-held query-only legacy transaction; never alter its
    # risk/session/ownership state or normalize a pre-existing stop.
    require(inventory._singleton(guard._source, 'paper_readonly')[8] == 0
            and inventory._singleton(guard._source, 'safety')[1] == 0
            and all(inventory._singleton(guard._source, table)[4] != 'stopped'
                    for table in ('paper_experiment', 'paper_renewal')), 'inspection_legacy_stopped')
    return row


def _receipt(db, bundle, manifest):
    rows = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name LIKE 'finite_inspection_%' "
                      "OR tbl_name=? ORDER BY name", (TABLE,)).fetchall()
    if not rows:
        return None
    require(rows == [('table', TABLE, TABLE, SQL)],
            'inspection_journal_invalid')
    records = db.execute('SELECT * FROM ' + TABLE).fetchmany(2)
    require(len(records) == 1, 'inspection_journal_invalid')
    row = records[0]
    require(row[:3] == (1, contract.INSPECTION_GENERATION, contract.digest(manifest.document()))
            and row[6:] == bundle._reference_identity('anchor') and row[3] in ('claimed', 'complete')
            and type(row[4]) is int and manifest.starts_ns <= row[4] < manifest.expires_ns
            and ((row[3] == 'claimed' and row[5] is None) or
                 (row[3] == 'complete' and type(row[5]) is int and row[4] <= row[5] < manifest.expires_ns)),
            'inspection_journal_invalid')
    return row


def run_inspection(*, paths=None, test_only=False, now_ns=None):
    manifest = contract.APPROVED_INSPECTION
    if manifest is None:
        return public('disabled')
    contract.require_inspection_manifest(manifest)
    require(manifest.bundle_generation == inventory.GENERATION, 'inspection_generation_mismatch')
    require(contract.APPROVED_PILOT is None and contract.APPROVED_RECOVERY is None,
            'inspection_exclusive_scope_required')
    require(type(test_only) is bool and (test_only or (paths is None and now_ns is None)),
            'inspection_production_override_forbidden')
    wall = time.time_ns if now_ns is None else now_ns
    started = wall()
    mono_started = time.monotonic_ns()
    last = started

    def current():
        nonlocal last
        now = wall()
        require(type(started) is int and type(now) is int and now >= last
                and manifest.starts_ns <= started <= now < manifest.expires_ns
                and now - started <= MAX_WORK_NS
                and 0 <= time.monotonic_ns() - mono_started <= MAX_WORK_NS,
                'inspection_window_or_deadline')
        last = now
        return now

    current()
    resolved = inventory.FIXED_PATHS if paths is None else paths
    # Existing identity is read privately; it is never an output or new grant.
    binding = load_owned_ledger(resolved['legacy']).account_fingerprint
    # Production preserves the reviewed nonsecret EVENT_DB route and its strict
    # under-/var/data, no-symlink, existing-history constraints. No fallback path.
    bundle = inventory.RuntimeStateBundle(binding, paths=paths, test_only=test_only)
    bundle.verify()
    with serialized(bundle):
        guard = SharedAccountGuard(bundle.paths['legacy'], bundle.paths['lifecycle'], bundle.binding,
                                   enabled=True, state_database=bundle.paths['guard'], test_only=test_only,
                                   runtime_bundle=bundle)
        with guard.hold(now_ns=current(), read_only=True), ExitStack() as stack:
            handles = {role: stack.enter_context(inventory._database(
                bundle.paths[role], bundle._pinned[role], write=True, public_read=role == 'anchor'))
                for role in ('runner', 'decision', 'lifecycle', 'source', 'guard', 'manifest', 'anchor')}
            bundle.verify()
            anchor = handles['anchor']
            previous = _receipt(anchor, bundle, manifest)
            if previous is not None:
                require(previous[3] == 'complete', 'inspection_attempt_consumed')
                return public('inspection_consumed')  # Never re-emit old hashes.
            require(not anchor.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'finite_pilot_%' LIMIT 1").fetchone(),
                    'inspection_pre_pilot_required')
            original = _controls(handles, guard)
            journal = AnchorJournal(bundle, wall)
            before = journal.cohort_state_digest()
            claimed = current()
            anchor.execute(SQL)
            anchor.execute('INSERT INTO ' + TABLE + " VALUES(1,?,?,'claimed',?,NULL,?,?)",
                           (contract.INSPECTION_GENERATION, contract.digest(manifest.document()), claimed,
                            *bundle._reference_identity('anchor')))
            anchor.commit()  # Claim survives interruption before proof exists.
            _checkpoint('claimed')
            anchor.execute('BEGIN IMMEDIATE')
            require(_receipt(anchor, bundle, manifest)[3] == 'claimed', 'inspection_receipt_changed')
            require(_controls(handles, guard) == original and journal.cohort_state_digest() == before,
                    'inspection_state_changed')
            journal.initialize_foundation(manifest, anchor_db=anchor)
            _checkpoint('foundation_created')
            completed = current()
            require(anchor.execute('UPDATE ' + TABLE + " SET status='complete',completed_ns=? WHERE id=1 AND status='claimed'",
                                   (completed,)).rowcount == 1, 'inspection_receipt_changed')
            journal.finalize_foundation(manifest, anchor_db=anchor)
            anchor.commit()  # Completion also precedes hashing/output; no replay.
            _checkpoint('completed')
            anchor.execute('BEGIN IMMEDIATE')
            require(_receipt(anchor, bundle, manifest)[3] == 'complete', 'inspection_receipt_changed')
            bundle.verify()
            require(_controls(handles, guard) == original and journal.cohort_state_digest() == before,
                    'inspection_state_changed')
            # No output hash is stored inside this journal, avoiding circularity.
            # Protected digest includes the final inspection receipt.
            founding = journal.founding_proofs()
            require(founding['pre_enrollment_cohort_sha256'] == before, 'inspection_foundation_changed')
            proof = dict(original_runner_sha256=contract.digest(original),
                         protected_state_sha256=journal.protected_state_digest(),
                         bundle_seal_sha256=_hash_file(bundle, 'seal'), **founding)
            current()
            _checkpoint('before_return')
            return public('inspection_complete', proof)


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if args != ['--initialize-proof-state']:
        print(json.dumps(public('invalid'), sort_keys=True), flush=True)
        return 2
    try:
        result = run_inspection()
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    except Exception:
        print(json.dumps(public('inspection_refused'), sort_keys=True), flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
