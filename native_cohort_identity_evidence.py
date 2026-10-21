"""Untrusted, nonmutating identity comparisons after an inventory refusal.

This never verifies/adopts a cohort and never reads diagnostic/audit payloads.
Old identities are compared, never substituted into the runtime verifier. All
outputs are fixed labels and booleans/null. No raw identifier or hash is output.
"""
from contextlib import ExitStack
import fcntl
import os
import re

import runtime_state_bundle as bundle

ROLES = tuple(bundle.FIXED_PATHS)
FILE_ROLES = tuple(sorted((*bundle.DB_ROLES, 'legacy_lock', 'anchor')))
EDGES = (
    ('enrollment_anchor', 'anchor'), ('enrollment_seal', 'seal'),
    ('enrollment_manifest', 'manifest'), ('seal_manifest', 'manifest'),
    ('manifest_seal', 'seal'),
    *((('manifest_' + role), role) for role in FILE_ROLES),
    ('guard_legacy', 'legacy'), ('guard_lifecycle', 'lifecycle'),
)
CHECKS = ('seal_contract', 'anchor_schema', 'anchor_binding', 'anchor_completed',
          'anchor_history_digest', 'manifest_structure', 'manifest_binding',
          'manifest_digest_to_seal', 'manifest_digest_to_anchor',
          'manifest_paths', 'guard_reference_shape', 'guard_account_paths')
PHASES = frozenset(('none', 'arguments', 'code', 'legacy_binding', 'construct',
    'file_access', 'seal_lock', 'seal_contract', 'database_open', 'anchor_schema',
    'anchor_record', 'anchor_history', 'manifest_structure', 'manifest_record',
    'guard_record', 'schema', 'generation_stamp', 'final_identity', 'cleanup', 'complete'))
ERRORS = frozenset(('none', 'invalid_arguments', 'file_missing', 'permission_refused',
    'lock_busy', 'os_error', 'sqlite_operational_error', 'sqlite_database_error',
    'value_error', 'missing_key', 'type_error', 'unexpected_error',
    'identity_changed_during_read', 'evidence_shape_refused'))


def blank(status='disabled'):
    return dict(status=status, trusted=False, runtime_gate_passed=False,
        full_mutable_content_continuity='not_pinned',
        provider_requests=0, state_mutated=False, runtime_reset_performed=False,
        checks={name: None for name in CHECKS},
        roles={role: dict(safe_access=None, observed_identity_stable=None,
            schema_match=None, generation_stamp_match=None,
            schema_applicable=role in (*bundle.DB_ROLES, 'anchor'),
            generation_stamp_applicable=role in bundle.NEW_ROLES,
            stage='none', code='none') for role in ROLES},
        identity_references={name: dict(device_match=None, inode_match=None,
            stored_references_agree=None) for name, _ in EDGES},
        stage='none', code='none')


def _pair(value):
    return (type(value) in (tuple, list) and len(value) == 2
            and all(type(x) is int and x >= 0 for x in value))


def _hex(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def _need(ok):
    if not ok:
        raise ValueError('evidence_shape_refused')


def inspect_identity_evidence(*, enabled=False, paths=None, test_only=False):
    """Compare only. Success means a bounded report, not accepted continuity.

    Current safe-file identities protect these reads within this process. They
    do not replace stored identities or authorize trusting a replaced cohort.
    A current seal flock cannot fence a holder of an older seal inode.
    """
    from native_diagnostic_audit_inspection import _safe_error, _frozen_code, Trace
    result = blank()
    phase, active_role = 'arguments', None
    stack, fd = None, None
    identities, dbs, old, refs = {}, {}, {}, {}

    def error(exc, role=None):
        code = ('evidence_shape_refused' if type(exc) is ValueError
                and exc.args == ('evidence_shape_refused',) else _safe_error(exc))
        if code not in ERRORS and code not in bundle_codes:
            code = 'unexpected_error'
        if role in ROLES:
            row = result['roles'][role]
            if row['code'] == 'none': row.update(stage=phase, code=code)
        if result['code'] == 'none': result.update(stage=phase, code=code)

    def attempt(role, callback):
        try:
            return callback()
        except Exception as exc:
            error(exc, role)
            return None

    # Reuse the already reviewed literal vocabulary, never exception messages.
    from native_diagnostic_audit_inspection import BUNDLE_CODES
    bundle_codes = BUNDLE_CODES
    try:
        _need(type(enabled) is bool and type(test_only) is bool and (paths is None or test_only))
        if not enabled: return result
        result['status'] = 'untrusted_partial'
        phase = 'code'; _frozen_code(Trace())
        phase, active_role = 'legacy_binding', 'legacy'
        legacy = bundle.FIXED_PATHS['legacy'] if paths is None else paths['legacy']
        with bundle._database(legacy, bundle._private(legacy)) as db:
            rows = db.execute('SELECT fingerprint FROM paper_readonly WHERE id=1').fetchmany(2)
            _need(len(rows) == 1 and _hex(rows[0][0]))
            fingerprint = rows[0][0]
        phase, active_role = 'construct', 'anchor'
        cohort = bundle.RuntimeStateBundle(fingerprint, paths=paths, test_only=test_only)
        phase = 'file_access'
        for role in ROLES:
            identity = attempt(role, lambda role=role: cohort._identity(role))
            result['roles'][role]['safe_access'] = identity is not None
            if identity is not None: identities[role] = identity
        phase, active_role = 'seal_lock', 'seal'
        _need('seal' in identities)
        fd = os.open(cohort.paths['seal'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        _need((os.fstat(fd).st_dev, os.fstat(fd).st_ino) == identities['seal'])
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        phase = 'seal_contract'
        def read_seal():
            value = cohort._read_seal(identities['seal'])
            _need(_pair(value['manifest_identity']) and _hex(value['manifest_digest']))
            return value
        seal = attempt('seal', read_seal)
        result['checks']['seal_contract'] = seal is not None
        if seal is not None and _pair(seal['manifest_identity']):
            refs['seal_manifest'] = tuple(seal['manifest_identity'])
        stack = ExitStack()
        phase = 'database_open'
        for role in (*bundle.DB_ROLES, 'anchor', 'manifest'):
            if role in identities:
                db = attempt(role, lambda role=role: stack.enter_context(bundle._database(
                    cohort.paths[role], identities[role], public_read=role == 'anchor')))
                if db is not None: dbs[role] = db
        anchor = None
        if 'anchor' in dbs:
            phase, active_role = 'anchor_schema', 'anchor'
            anchor_schema = attempt('anchor', lambda: cohort._anchor_schema(dbs['anchor']))
            result['checks']['anchor_schema'] = anchor_schema is not None
            if anchor_schema is not None:
                phase = 'anchor_record'
                def read_anchor():
                    row = bundle._singleton(dbs['anchor'], bundle._ANCHOR_TABLE)
                    _need(len(row) == 14)
                    return row
                anchor = attempt('anchor', read_anchor)
                if anchor is not None:
                    result['checks']['anchor_binding'] = anchor[:4] == (1, bundle.VERSION,
                        bundle.GENERATION, cohort._anchor_binding())
                    result['checks']['anchor_completed'] = anchor[8] == 'completed'
                    for name, value in (('enrollment_anchor', anchor[4:6]),
                            ('enrollment_seal', anchor[9:11]), ('enrollment_manifest', anchor[11:13])):
                        if _pair(value): refs[name] = tuple(value)
                        else: error(ValueError('evidence_shape_refused'), 'anchor')
                    phase = 'anchor_history'
                    if _hex(anchor[6]) and _hex(anchor[7]):
                        def check_history():
                            history = dbs['anchor'].execute('SELECT * FROM events WHERE version_id=?',
                                (anchor[6],)).fetchmany(2)
                            return (len(history) == 1
                                and bundle._digest(history[0]) == anchor[7]
                                and cohort._historical(history[0]))
                        result['checks']['anchor_history_digest'] = attempt('anchor', check_history)
                    else: result['checks']['anchor_history_digest'] = False
        records, binding = None, None
        if 'manifest' in dbs:
            phase, active_role = 'manifest_structure', 'manifest'
            def read_manifest():
                db = dbs['manifest']
                _need(db.execute('PRAGMA quick_check(1)').fetchall() == [('ok',)])
                _need(db.execute("SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
                    == [('table', 'bundle_binding'), ('table', 'bundle_files')])
                b = bundle._singleton(db, 'bundle_binding')
                r = db.execute('SELECT role,path,device,inode,schema_hash FROM bundle_files ORDER BY role').fetchmany(9)
                _need(len(b) == 7 and len(r) == len(FILE_ROLES)
                    and tuple(x[0] for x in r) == FILE_ROLES)
                _need(all(_pair(x[2:4]) and type(x[1]) is str
                    and (x[4] is None if x[0] == 'legacy_lock' else _hex(x[4])) for x in r))
                _need(_pair(b[5:7]))
                return b, r
            observed = attempt('manifest', read_manifest)
            result['checks']['manifest_structure'] = observed is not None
            if observed is not None:
                binding, records = observed
                phase = 'manifest_record'
                result['checks']['manifest_binding'] = binding[:5] == (1, bundle.VERSION,
                    bundle.GENERATION, fingerprint, 1)
                result['checks']['manifest_paths'] = all(row[1] == cohort.paths[row[0]] for row in records)
                digest = attempt('manifest', lambda: bundle._digest([binding, records]))
                if digest is not None:
                    if seal is not None: result['checks']['manifest_digest_to_seal'] = digest == seal['manifest_digest']
                    if anchor is not None: result['checks']['manifest_digest_to_anchor'] = digest == anchor[13]
                refs['manifest_seal'] = tuple(binding[5:7])
                for role, path, device, inode, schema in records:
                    old[role] = ((device, inode), schema)
                    refs['manifest_' + role] = (device, inode)
        guard_schema = None
        if 'guard' in dbs:
            phase, active_role = 'schema', 'guard'
            # Validate the object as the original table before SELECT. A view
            # could otherwise redirect this limited metadata read into audit.
            guard_schema = attempt('guard', lambda: bundle._schema(dbs['guard'], 'guard'))
        if guard_schema is not None:
            phase, active_role = 'guard_record', 'guard'
            def read_guard():
                row = bundle._singleton(dbs['guard'], 'account_guard_binding')
                _need(len(row) == 11 and _pair(row[4:6]) and _pair(row[7:9]))
                return row
            guard = attempt('guard', read_guard)
            result['checks']['guard_reference_shape'] = guard is not None
            if guard is not None:
                result['checks']['guard_account_paths'] = (guard[:4] == (1, 1, fingerprint, cohort.paths['legacy'])
                    and guard[6] == cohort.paths['lifecycle'])
                refs['guard_legacy'], refs['guard_lifecycle'] = tuple(guard[4:6]), tuple(guard[7:9])
        for role in (*bundle.DB_ROLES, 'anchor'):
            if role not in dbs: continue
            phase, active_role = 'schema', role
            schema = guard_schema if role == 'guard' else attempt(role,
                lambda role=role: cohort._anchor_schema(dbs[role])
                if role == 'anchor' else bundle._schema(dbs[role], role))
            if schema is not None and role in old: result['roles'][role]['schema_match'] = schema == old[role][1]
            if role in bundle.NEW_ROLES:
                phase = 'generation_stamp'
                stamp = attempt(role, lambda role=role: (dbs[role].execute('PRAGMA application_id').fetchone()[0],
                    dbs[role].execute('PRAGMA user_version').fetchone()[0]))
                if stamp is not None: result['roles'][role]['generation_stamp_match'] = stamp == bundle._stamp(role)
        for name, role in EDGES:
            row = result['identity_references'][name]
            if name in refs and role in identities:
                row['device_match'] = refs[name][0] == identities[role][0]
                row['inode_match'] = refs[name][1] == identities[role][1]
            related = [refs[n] for n, r in EDGES if r == role and n in refs]
            expected = sum(r == role for _, r in EDGES)
            if name in refs and len(related) == expected:
                row['stored_references_agree'] = all(value == related[0] for value in related)
        phase = 'final_identity'
        for role, identity in identities.items():
            current = attempt(role, lambda role=role: cohort._identity(role))
            result['roles'][role]['observed_identity_stable'] = current == identity
        result['status'] = 'untrusted_comparisons_observed'
    except Exception as exc:
        error(exc, active_role)
    finally:
        phase = 'cleanup'
        if stack is not None:
            try: stack.close()
            except Exception as exc: error(exc)
        if fd is not None:
            try: os.close(fd)
            except Exception as exc: error(exc, 'seal')
    if identities:
        phase = 'final_identity'
        for role, identity in identities.items():
            current = attempt(role, lambda role=role: cohort._identity(role))
            same = current == identity
            previous = result['roles'][role]['observed_identity_stable']
            result['roles'][role]['observed_identity_stable'] = same and previous is not False
            if not same or previous is False:
                result['roles'][role].update(stage=phase, code='identity_changed_during_read')
                if result['code'] == 'none':
                    result.update(stage=phase, code='identity_changed_during_read')
    if result['code'] != 'none': result['status'] = 'untrusted_partial'
    if result['status'] == 'untrusted_comparisons_observed': result['stage'] = 'complete'
    return result
