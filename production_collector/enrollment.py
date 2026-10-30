"""Fresh schema-2 enrollment only; no migration, reset, or implicit retry.

Started marker is O_EXCL committed/fsynced before any root creation. Cross-deploy
coherent device remap keeps original identities/nonce/receipt unchanged. It does
not establish historical freshness against a coherent complete-disk rollback.
"""
from hashlib import sha256
import json
import os
from pathlib import Path
import secrets
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from filesystem import Anchor, digest, read_manifest, read_db_nonce, STORAGE_SCHEMA
import production_release as policy

PREFIX = '.research-collector-ecb-v1-enrollment'


def _paths(release):
    root = Path(release.root)
    return root, root.parent / (PREFIX + '.started.json'), root.parent / (PREFIX + '.complete.json')


def qualify_mount(root, *, _test_mount=False):
    root = Path(root)
    if not _test_mount and str(root.parent) != policy.MOUNT_ROOT:
        raise ValueError('fixed_mount_required')
    with Anchor(root) as anchor:
        fs = anchor.capacity()
        info = os.fstat(anchor.mount_fd)
        return {'mount_identity':[info.st_dev,info.st_ino], 'uid':os.getuid(),
                'fs_type':anchor.fs_type, 'storage_profile':anchor.profile,
                'free_bytes':fs.f_bavail*fs.f_frsize, 'free_inodes':fs.f_favail}


def _read_marker(path):
    with Anchor(Path(path).parent / policy.COLLECTOR_ID) as anchor:
        return json.loads(anchor.read(Path(path).name,8192,root=False))


def _binding(release):
    return {'stable_profile_hash':digest(policy.stable_identity(release)), 'enrollment_id':release.enrollment_id,
            'root':release.root, 'collector_id':release.collector_id}


def _evidence(anchor):
    anchor.check()
    fs=anchor.capacity();info=os.fstat(anchor.mount_fd)
    return {'mount_identity':[info.st_dev,info.st_ino], 'uid':os.getuid(),
            'fs_type':anchor.fs_type, 'storage_profile':anchor.profile,
            'free_bytes':fs.f_bavail*fs.f_frsize, 'free_inodes':fs.f_favail}


def _verify_enrollment_anchor(release, anchor):
    root, started, complete = _paths(release)
    if anchor.path != root:
        raise ValueError('enrollment_anchor_root_mismatch')
    evidence = _evidence(anchor)
    initial = json.loads(anchor.read(started.name,8192,root=False))
    done = json.loads(anchor.read(complete.name,8192,root=False))
    if (type(initial) is not dict or set(initial) != {'schema','binding','started_at','mount_identity','nonce','storage_profile'}
            or type(done) is not dict or set(done) != {'schema','binding','started_hash','root_identity','manifest_sha256','nonce'}
            or type(initial.get('schema')) is not int or initial['schema'] != STORAGE_SCHEMA
            or type(done.get('schema')) is not int or done['schema'] != STORAGE_SCHEMA):
        raise ValueError('fresh_enrollment_schema_required')
    for record, key in ((initial,'mount_identity'),(done,'root_identity')):
        if type(record[key]) is not list or len(record[key]) != 2 or any(type(v) is not int or v < 0 for v in record[key]):
            raise ValueError('enrollment_identity_schema')
    if initial['binding'] != _binding(release) or done['binding'] != _binding(release):
        raise ValueError('enrollment_release_mismatch')
    if release.phase == 'collect' and release.enrollment_receipt_sha256 != digest(done):
        raise ValueError('enrollment_receipt_pin_mismatch')
    if (done['started_hash'] != digest(initial) or initial['storage_profile'] != evidence['storage_profile']
            or done['nonce'] != initial['nonce']):
        raise ValueError('enrollment_lineage_changed')
    if anchor.root_fd is None:
        anchor.open_root()
    manifest = read_manifest(anchor)
    current = anchor.snapshot()
    if (done['root_identity'] != manifest['config']['root_identity']
            or done['root_identity'][1] != current['root']['inode']):
        raise ValueError('enrolled_root_missing_or_changed')
    if initial['mount_identity'] != manifest['config']['parent_identity']:
        raise ValueError('enrollment_original_mount_changed')
    if (done['manifest_sha256'] != sha256(anchor.read('manifest.json')).hexdigest()
            or done['nonce'] != manifest['nonce']
            or manifest['config'].get('release_hash') != digest(policy.stable_identity(release))):
        raise ValueError('enrolled_manifest_changed')
    for name in ('ledger.sqlite','evidence.sqlite'):
        if read_db_nonce(anchor,name) != initial['nonce']:
            raise ValueError('enrollment_cohort_mismatch')
    anchor.check()
    return {**evidence, "enrollment_receipt_sha256":digest(done)}



def verify_enrollment(release, *, _test_mount=False, _anchor=None):
    policy.require_release(release)
    root, _, _ = _paths(release)
    if _anchor is not None:
        return _verify_enrollment_anchor(release,_anchor)
    if not _test_mount and str(root.parent) != policy.MOUNT_ROOT:
        raise ValueError('fixed_mount_required')
    with Anchor(root) as anchor:
        return _verify_enrollment_anchor(release,anchor)


def verify_bootstrap_reopen(release, anchor, nonce):
    policy.require_enrollment_release(release)
    root, started, complete = _paths(release)
    if anchor.path != root or anchor.exists(complete.name):
        raise ValueError('explicit_incomplete_bootstrap_required')
    initial=json.loads(anchor.read(started.name,8192,root=False))
    manifest=read_manifest(anchor)
    if (initial.get('schema') != STORAGE_SCHEMA or initial.get('binding') != _binding(release)
            or initial.get('nonce') != nonce or manifest['nonce'] != nonce
            or initial.get('mount_identity') != manifest['config']['parent_identity']):
        raise ValueError('bootstrap_cohort_mismatch')
    anchor.check()


def enrollment_receipt_hash(release):
    return verify_enrollment(release)['enrollment_receipt_sha256']


def enroll_once(release, *, _test_mount=False, _collector_factory=None, _fault=None):
    policy.require_release(release)
    if release.phase != 'enroll' or policy.ENROLLMENT_APPROVED is not True:
        raise ValueError('enrollment_not_compiled_approved')
    now = time.time()
    if not release.enrollment_start <= now < release.enrollment_deadline:
        raise ValueError('enrollment_window_closed')
    root, started, complete = _paths(release)
    evidence = qualify_mount(root,_test_mount=_test_mount)
    nonce = secrets.token_hex(32)
    with Anchor(root) as anchor:
        retained = os.fstat(anchor.mount_fd)
        if [retained.st_dev,retained.st_ino] != evidence['mount_identity'] or anchor.profile != evidence['storage_profile']:
            raise ValueError('mount_changed_before_enrollment')
        if any(anchor.exists(name) for name in (root.name,started.name,complete.name)):
            raise ValueError('existing_or_partial_enrollment_no_retry')
        initial = {'schema':STORAGE_SCHEMA, 'binding':_binding(release), 'started_at':now,
                   'mount_identity':evidence['mount_identity'], 'nonce':nonce,
                   'storage_profile':evidence['storage_profile']}
        anchor.write_new(started.name,initial,root=False)
        if _fault:
            _fault('after_started')
        anchor.check()
        if _collector_factory is None:
            from recurring import LeaseCollector
            _collector_factory = LeaseCollector
        collector = _collector_factory(root,release.collector_id,create=True,live_release=release,_enrollment_nonce=nonce,_enrollment_mount_pin=evidence["mount_identity"])
        try:
            from recurring import Lease
            for approved in policy.approved_leases(release):
                collector.add_lease(Lease(**approved))
            collector.verify()
        finally:
            collector.close()
        anchor.check()
        if _fault:
            _fault('after_root')
        collector = _collector_factory(root,release.collector_id,create=False,live_release=release,
                                       _enrollment_reopen=True,_enrollment_nonce=nonce,
                                       _enrollment_mount_pin=evidence['mount_identity'])
        try:
            collector.verify()
            for name in ('ledger.sqlite','evidence.sqlite'):
                db = collector.root.open_database(name)
                try:
                    if db.execute('PRAGMA journal_mode').fetchone()[0].lower() != 'wal':
                        raise ValueError('sqlite_wal_required')
                    db.execute('PRAGMA synchronous=FULL')
                    if db.execute('PRAGMA synchronous').fetchone()[0] != 2 or db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                        raise ValueError('sqlite_full_integrity_required')
                finally:
                    db.close()
            manifest_raw = collector.root.anchor.read('manifest.json')
            done = {'schema':STORAGE_SCHEMA, 'binding':_binding(release), 'started_hash':digest(initial),
                    'root_identity':collector.root.config['root_identity'],
                    'manifest_sha256':sha256(manifest_raw).hexdigest(), 'nonce':nonce}
            anchor.write_new(complete.name,done,root=False)
            collector.root.check_anchors()
        finally:
            collector.close()
        if _fault:
            _fault('after_complete')
        anchor.check()
    verify_enrollment(release,_test_mount=_test_mount)
    return {'state':'enrolled','source_requests':0,'order_permission':'none'}


def ensure_enrolled(release, *, _test_mount=False):
    policy.require_release(release)
    root, started, complete = _paths(release)
    with Anchor(root) as anchor:
        present = [anchor.exists(p.name) for p in (root,started,complete)]
    if not any(present):
        if release.phase != 'enroll':
            raise ValueError('collection_never_bootstraps')
        enroll_once(release,_test_mount=_test_mount)
    return verify_enrollment(release,_test_mount=_test_mount)


def main():
    if sys.argv[1:] != ['--enroll-compiled-once']:
        raise ValueError('explicit_enrollment_command_required')
    if sys.version_info[:3] != policy.REQUIRED_PYTHON:
        raise ValueError('exact_python_version_required')
    print(json.dumps(enroll_once(policy.APPROVED_RELEASE)),flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('{"state":"enrollment_refused","source_requests":0,"order_permission":"none"}',flush=True)
        raise SystemExit(70)
