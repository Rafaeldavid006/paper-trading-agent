"""One bounded local inventory; never repair, migration or trading authority.

Output contains only fixed categories, counts and booleans. Existing identities
are verified privately by the unchanged RuntimeStateBundle. No credential,
account identifier, row text, path, digest or exception text is emitted. This
does not prove external writer quiescence or current broker flatness.
"""
from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import stat
import sys
import time

import runtime_state_bundle as inventory

SIDECARS = ('-journal', '-wal', '-shm')
EXTRAS = {'prepared_admission': 'finite-prepared-successor.sqlite3',
          'prepared_execution': 'finite-prepared-execution.sqlite3',
          'paper_account_check': 'paper-account-check-once-v1.jsonl'}
MAX_SECONDS = 8


def require(ok):
    if not ok:
        raise ValueError('inventory_refused')


def _paths(paths, test_only):
    require(type(test_only) is bool)
    if not test_only:
        require(paths is None)
        paths = dict(inventory.FIXED_PATHS)
        paths['anchor'] = os.environ.get('EVENT_DB')
    else:
        require(type(paths) is dict and set(paths) == set(inventory.FIXED_PATHS))
        paths = dict(paths)
    for path in paths.values():
        require(type(path) is str and Path(path).is_absolute()
                and str(Path(path)) == path and '..' not in Path(path).parts)
        require(Path('/tmp' if test_only else '/var/data') in Path(path).parents)
        inventory._parents(path)
    require(len(set(paths.values())) == len(paths)
            and paths['legacy_lock'] == paths['legacy'] + '.experiment.lock')
    if test_only:
        require(len({str(Path(p).parent) for p in paths.values()}) == 1)
    for role, name in EXTRAS.items():
        paths[role] = str(Path(paths['seal']).parent / name)
    require(len(set(paths.values())) == len(paths)
            and not any(p+s in paths.values() for p in paths.values() for s in SIDECARS))
    return paths


def _metadata(path):
    try:
        value = os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return {'kind': 'absent'}, None
    except OSError:
        return {'kind': 'unavailable'}, None
    kind = ('regular' if stat.S_ISREG(value.st_mode) else
            'symlink' if stat.S_ISLNK(value.st_mode) else 'other')
    # No device, inode, permissions or arbitrary names leave the process.
    result = {'kind': kind}
    if kind == 'regular':
        result['bytes'] = value.st_size
    stamp = (value.st_dev, value.st_ino, value.st_size,
             value.st_mtime_ns, value.st_ctime_ns, value.st_mode, value.st_nlink)
    return result, stamp


def _snapshot(paths):
    result, stamps = {}, {}
    for role, path in paths.items():
        meta, stamps[(role, '')] = _metadata(path)
        sides = {}
        for suffix in SIDECARS:
            sides[suffix[1:]], stamps[(role, suffix)] = _metadata(path + suffix)
        result[role] = dict(file=meta, sidecars=sides)
    return result, stamps


def _scalar(db, sql):
    rows = db.execute(sql).fetchmany(2)
    require(len(rows) == 1 and len(rows[0]) == 1
            and type(rows[0][0]) is int and 0 <= rows[0][0] <= 2**53-1)
    return rows[0][0]


def _summary(dbs):
    """Only literal reviewed statements; never serialize stored text/JSON."""
    queries = {
        'legacy': {
            'stopped': 'SELECT stopped FROM safety WHERE id=1',
            'claims': 'SELECT COUNT(*) FROM intents',
            'renewal_claims': 'SELECT COUNT(*) FROM renewal_intents',
            'loss_days': 'SELECT COUNT(*) FROM loss_days',
        },
        'source': {
            'busy': 'SELECT busy FROM primary_meta WHERE id=1',
            'receipts': 'SELECT COUNT(*) FROM primary_receipts',
            'snapshots': 'SELECT COUNT(*) FROM primary_snapshots',
            'lineage_rows': 'SELECT COUNT(*) FROM primary_lineage',
            'episodes': 'SELECT COUNT(*) FROM primary_episodes',
            'events': 'SELECT COUNT(*) FROM primary_events',
        },
        'lifecycle': {
            'stopped': 'SELECT stopped FROM control WHERE id=1',
            'attention': 'SELECT attention FROM control WHERE id=1',
            'claims': 'SELECT COUNT(*) FROM intents',
            'unanswered_claims': 'SELECT COUNT(*) FROM intents WHERE response IS NULL',
            'cancels': 'SELECT COUNT(*) FROM cancels',
            'dispatches': 'SELECT COUNT(*) FROM native_dispatch',
            'nonobserved_dispatches': "SELECT COUNT(*) FROM native_dispatch WHERE state!='observed'",
            'loss_days': 'SELECT COUNT(*) FROM loss_days',
            'risk_sessions': 'SELECT COUNT(*) FROM risk_sessions',
            'loss_latched_sessions': 'SELECT COUNT(*) FROM risk_sessions WHERE loss_latched!=0',
            'charged_entry_claims': 'SELECT COALESCE(SUM(entry_claims),0) FROM risk_sessions',
        },
        'runner': {
            'dirty': 'SELECT dirty FROM runner_control WHERE id=1',
            'stopped': 'SELECT stopped FROM runner_control WHERE id=1',
            'attention_present': 'SELECT attention IS NOT NULL FROM runner_control WHERE id=1',
            'events': 'SELECT COUNT(*) FROM runner_events',
        },
        'decision': {
            'stopped': 'SELECT stopped FROM demo_control WHERE id=1',
            'attention_rows': 'SELECT COUNT(*) FROM demo_attention',
            'intents': 'SELECT COUNT(*) FROM demo_intents',
            'loss_days': 'SELECT COUNT(*) FROM demo_loss',
        },
        'guard': {
            'account_attempts': 'SELECT COUNT(*) FROM account_guard_attempts',
            'native_read_attempts': 'SELECT COUNT(*) FROM native_read_attempts',
            'read_audit_rows': 'SELECT COUNT(*) FROM native_read_audit',
        },
    }
    return {role: {key: _scalar(dbs[role], sql) for key, sql in items.items()}
            for role, items in queries.items()}


def report(*, paths=None, test_only=False):
    started = time.monotonic()
    result = dict(mode='native_state_inventory', version=1, status='refused',
                  provider_requests=0, state_writes=0, order_permission='none',
                  activation_authorized=False, broker_state_verified=False,
                  writer_quiescence_proven=False, cooperative_fences_acquired=False,
                  original_cohort_verified=False, snapshot_stable=False)
    try:
        paths = _paths(paths, test_only)
        files, before = _snapshot(paths)
        result['files'] = files
        if any(s['kind'] != 'absent' for f in files.values() for s in f['sidecars'].values()):
            result['status'] = 'sidecars_or_unavailable_no_sql'
            return result
        require(all(files[r]['file']['kind'] == 'regular' for r in inventory.FIXED_PATHS))
        require(all(files[r]['file']['kind'] in ('regular', 'absent') for r in EXTRAS))
        with ExitStack() as stack:
            # Existing cooperative fences only; no file is created or opened rw.
            # Source is additionally serialized where source producers cooperate.
            for role in ('seal', 'legacy_lock', 'source'):
                identity = inventory._private(paths[role])
                fd = os.open(paths[role], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                stack.callback(os.close, fd)
                info = os.fstat(fd)
                require((info.st_dev, info.st_ino) == identity)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result['cooperative_fences_acquired'] = True
            fd = os.open(paths['seal'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                raw = os.read(fd, 4097)
            finally:
                os.close(fd)
            require(len(raw) <= 4096)
            seal = inventory._load(raw)
            core = {r: paths[r] for r in inventory.FIXED_PATHS}
            bundle = inventory.RuntimeStateBundle(seal['fingerprint'],
                paths=core if test_only else None, test_only=test_only)
            require(bundle.verify())  # Original identity/schema/economic verifier.
            dbs = {role: stack.enter_context(inventory._database(paths[role],
                   bundle._pinned[role], public_read=role == 'anchor'))
                   for role in inventory.DB_ROLES}
            summary = _summary(dbs)
            require(bundle.verify())
            after_files, after = _snapshot(paths)
            require(before == after and files == after_files
                    and 0 <= time.monotonic()-started < MAX_SECONDS)
            result.update(status='observed', original_cohort_verified=True,
                          source_parser_identity_verified=True,
                          snapshot_stable=True, retained=summary)
    except Exception:
        # No exception message, arbitrary database text or partial SQL result.
        result['status'] = 'refused'
        result.pop('retained', None)
    return result


def main(argv=None):
    if (sys.argv[1:] if argv is None else argv) != ['--read-existing-state']:
        return 2
    # Linux child only: bound impact on the existing paid 512 MiB service.
    # Do not change limits in an importing parent, and never emit a core dump.
    try:
        import resource
        for kind, limit in ((resource.RLIMIT_AS, 128*1024*1024),
                            (resource.RLIMIT_CPU, 6), (resource.RLIMIT_CORE, 0)):
            _, hard = resource.getrlimit(kind)
            cap = limit if hard == resource.RLIM_INFINITY else min(limit, hard)
            resource.setrlimit(kind, (cap, cap))
    except Exception:
        print('{"mode":"native_state_inventory","status":"limits_refused"}', flush=True)
        return 0
    result = report()
    raw = json.dumps(result, sort_keys=True, separators=(',', ':'))
    if len(raw) > 12288:
        raw = '{"mode":"native_state_inventory","status":"output_refused"}'
    print(raw, flush=True)
    return 0  # Diagnostic result is not an alternate startup admission gate.


if __name__ == '__main__':
    raise SystemExit(main())
