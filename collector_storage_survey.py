"""Fixed metadata-only survey: no enrollment, SQLite, contents, secrets or network.

Scalar diagnostics describe a point-in-time local observation, not persisted
identity continuity or protection from hostile same-user writers/kernel stalls.
"""
import ctypes
from datetime import datetime, timezone
import json
import os
import resource
import signal
import stat
import sys

ROOT_NAME = 'research-collector-ecb-v1'
STARTED = '.research-collector-ecb-v1-enrollment.started.json'
COMPLETE = '.research-collector-ecb-v1-enrollment.complete.json'
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
MOUNTINFO_LIMIT = 256 * 1024
ROLES = ('started', 'complete', 'manifest', 'ready', 'ledger', 'evidence', 'lock')
SURVEY_FIELDS = frozenset((
    'survey_complete', 'survey_stage', 'survey_reason', 'survey_errno', 'persisted_identity_checked',
    'mount_mode', 'mount_owned', 'mount_group_matches_egid', 'mount_group_in_supplementary',
    'mount_group_writable', 'mount_other_writable',
    'mount_setgid', 'mount_exclusive_write_gate', 'mount_free_bytes_ok',
    'mount_free_inodes_ok', 'mount_identity_stable', 'mount_boundary_unique',
    'mount_rw', 'mount_fs_type', 'mount_persistent_type', 'root_present',
    'root_kind', 'root_mode', 'root_owned', 'root_private', 'root_same_device',
    'root_identity_stable', 'fixed_entries_stable', 'databases_same_device') +
    tuple(role + suffix for role in ROLES for suffix in ('_present', '_kind', '_mode', '_owned', '_single_link')))


STAGES = frozenset(('not_started', 'parent_argument', 'parent_binding', 'resource_limits',
                    'readonly_guard', 'fixed_mount_open', 'mountinfo_read', 'mount_metadata',
                    'capacity', 'mountinfo', 'enrollment_marker_metadata', 'root_metadata',
                    'root_open', 'collector_file_metadata', 'repeat_metadata', 'anchor_recheck'))
REFUSAL_CODES = frozenset(('mountinfo_bound', 'mount_directory_required', 'root_identity_changed',
                         'mutation_or_external_refused', 'write_open_refused', 'fixed_parent_argument',
                         'parent_refused', 'parent_binding_failed'))
LAST_STAGE = 'not_started'


class Refused(Exception):
    pass


def failure_payload(exc):
    reason = ('os_error' if isinstance(exc, OSError) else
              exc.args[0] if type(exc) is Refused and len(exc.args) == 1 and type(exc.args[0]) is str and exc.args[0] in REFUSAL_CODES else
              'unexpected_refusal')
    number = getattr(exc, 'errno', None) if isinstance(exc, OSError) else None
    return {'survey_complete': False, 'survey_stage': LAST_STAGE if LAST_STAGE in STAGES else 'unexpected_stage',
            'survey_reason': reason, 'survey_errno': number if type(number) is int and 0 <= number <= 4095 else None,
            'persisted_identity_checked': False}


def physical(info):
    return None if info is None else (info.st_dev, info.st_ino)


def metadata(fd, name):
    try:
        return os.stat(name, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def unchanged(before, after):
    if before is None or after is None:
        return before is after
    return (physical(before), before.st_mode, before.st_uid, before.st_gid, before.st_nlink,
            before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (
            physical(after), after.st_mode, after.st_uid, after.st_gid, after.st_nlink,
            after.st_size, after.st_mtime_ns, after.st_ctime_ns)


def kind(info):
    if info is None:
        return 'absent'
    if stat.S_ISDIR(info.st_mode):
        return 'directory'
    if stat.S_ISREG(info.st_mode):
        return 'regular'
    if stat.S_ISLNK(info.st_mode):
        return 'symlink'
    return 'other'


def describe(report, role, info):
    report.update({role + '_present': info is not None, role + '_kind': kind(info),
                   role + '_mode': None if info is None else stat.S_IMODE(info.st_mode),
                   role + '_owned': None if info is None else info.st_uid == os.getuid(),
                   role + '_single_link': None if info is None else info.st_nlink == 1})


def mount_description(raw):
    if len(raw) > MOUNTINFO_LIMIT:
        raise Refused('mountinfo_bound')
    lines = raw.splitlines()
    if len(lines) > 4096 or any(len(line) > 4096 for line in lines):
        raise Refused('mountinfo_bound')
    entries = []
    for line in lines:
        left, separator, right = line.partition(b' - ')
        fields, tail = left.split(), right.split()
        if separator and len(fields) >= 6 and tail and fields[4] == b'/var/data':
            entries.append((fields[5].split(b','), tail[0]))
    unique = len(entries) == 1
    fs = entries[0][1] if unique else b'unknown'
    return {'mount_boundary_unique': unique,
            'mount_rw': bool(unique and b'rw' in entries[0][0]),
            'mount_fs_type': fs.decode('ascii') if fs in (b'ext4', b'xfs', b'btrfs', b'zfs', b'tmpfs', b'overlay', b'ramfs') else 'other',
            'mount_persistent_type': bool(unique and fs not in (b'tmpfs', b'overlay', b'ramfs'))}


def survey_fd(fd, mountinfo):
    """Fixed names only. Tests supply a disposable directory FD, never CLI paths."""
    global LAST_STAGE
    LAST_STAGE = 'mount_metadata'
    report = {key: None for key in SURVEY_FIELDS}
    report.update(survey_complete=False, survey_stage='mount_metadata', persisted_identity_checked=False)
    mount = os.fstat(fd)
    if not stat.S_ISDIR(mount.st_mode):
        raise Refused('mount_directory_required')
    mode = stat.S_IMODE(mount.st_mode)
    report.update(mount_mode=mode, mount_owned=mount.st_uid == os.getuid(),
                  mount_group_matches_egid=mount.st_gid == os.getegid(),
                  mount_group_in_supplementary=mount.st_gid in os.getgroups(),
                  mount_group_writable=bool(mode & 0o020), mount_other_writable=bool(mode & 0o002),
                  mount_setgid=bool(mode & stat.S_ISGID), mount_exclusive_write_gate=not bool(mode & 0o022))
    LAST_STAGE = 'capacity'
    capacity = os.fstatvfs(fd)
    LAST_STAGE = 'mountinfo'
    report.update(mount_free_bytes_ok=capacity.f_bavail * capacity.f_frsize >= 64 * 1024 * 1024,
                  mount_free_inodes_ok=capacity.f_favail >= 128, **mount_description(mountinfo))
    LAST_STAGE = 'enrollment_marker_metadata'
    tracked = []
    for role, name in (('started', STARTED), ('complete', COMPLETE)):
        info = metadata(fd, name)
        describe(report, role, info)
        tracked.append((fd, name, info))
    LAST_STAGE = 'root_metadata'
    root = metadata(fd, ROOT_NAME)
    report.update(survey_stage='collector_metadata', root_present=root is not None, root_kind=kind(root),
                  root_mode=None if root is None else stat.S_IMODE(root.st_mode),
                  root_owned=None if root is None else root.st_uid == os.getuid(),
                  root_private=None if root is None else not bool(root.st_mode & 0o077),
                  root_same_device=None if root is None else root.st_dev == mount.st_dev)
    child = None
    try:
        if root is not None and stat.S_ISDIR(root.st_mode):
            LAST_STAGE = 'root_open'
            child = os.open(ROOT_NAME, DIR_FLAGS, dir_fd=fd)
            if physical(os.fstat(child)) != physical(root):
                raise Refused('root_identity_changed')
            LAST_STAGE = 'collector_file_metadata'
            db = []
            for role, name in (('manifest', 'manifest.json'), ('ready', 'ready.json'),
                               ('ledger', 'ledger.sqlite'), ('evidence', 'evidence.sqlite'), ('lock', 'owner.lock')):
                info = metadata(child, name)
                describe(report, role, info)
                tracked.append((child, name, info))
                if role in ('ledger', 'evidence'):
                    db.append(info)
            report['databases_same_device'] = (all(info.st_dev == mount.st_dev for info in db)
                                               if all(info is not None for info in db) else None)
        LAST_STAGE = 'repeat_metadata'
        report['fixed_entries_stable'] = all(unchanged(info, metadata(parent, name)) for parent, name, info in tracked)
        named_root = metadata(fd, ROOT_NAME)
        report['root_identity_stable'] = unchanged(root, named_root) and (child is None or physical(os.fstat(child)) == physical(root))
        report['mount_identity_stable'] = unchanged(mount, os.fstat(fd))
        stable = all(report[key] for key in ('fixed_entries_stable', 'root_identity_stable', 'mount_identity_stable'))
        report.update(survey_complete=stable, survey_stage='complete' if stable else 'metadata_changed')
        return report
    finally:
        if child is not None:
            os.close(child)


def open_fixed_mount():
    fds = []
    try:
        fds.append(os.open('/', DIR_FLAGS))
        fds.append(os.open('var', DIR_FLAGS, dir_fd=fds[-1]))
        fds.append(os.open('data', DIR_FLAGS, dir_fd=fds[-1]))
        return fds
    except BaseException:
        for fd in reversed(fds):
            os.close(fd)
        raise


def read_mountinfo():
    fd = os.open('/proc/self/mountinfo', os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        parts, left = [], MOUNTINFO_LIMIT + 1
        while left:
            part = os.read(fd, left)
            if not part:
                break
            parts.append(part)
            left -= len(part)
        return b''.join(parts)
    finally:
        os.close(fd)


def install_readonly_guard():
    def guard(event, args):
        if event.startswith(('socket.', 'subprocess.', 'os.exec', 'os.spawn')) or event in (
                'os.system', 'os.fork', 'os.forkpty', 'os.remove', 'os.rename', 'os.mkdir',
                'os.rmdir', 'os.chmod', 'os.chown', 'os.link', 'os.symlink', 'os.truncate', 'os.listdir', 'os.scandir'):
            raise Refused('mutation_or_external_refused')
        if event == 'open' and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            raise Refused('write_open_refused')
    sys.addaudithook(guard)


def main():
    global LAST_STAGE
    LAST_STAGE = 'parent_argument'
    if len(sys.argv) != 2 or not sys.argv[1].isascii() or not sys.argv[1].isdecimal():
        raise Refused('fixed_parent_argument')
    parent = int(sys.argv[1])
    if sys.platform != 'linux' or parent <= 1 or os.getppid() != parent:
        raise Refused('parent_refused')
    LAST_STAGE = 'parent_binding'
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent:
        raise Refused('parent_binding_failed')
    LAST_STAGE = 'resource_limits'
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_AS, (128 * 1024 * 1024, 128 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (32, 32))
    resource.setrlimit(resource.RLIMIT_CPU, (3, 3))
    signal.alarm(10)
    LAST_STAGE = 'readonly_guard'
    install_readonly_guard()
    LAST_STAGE = 'fixed_mount_open'
    fds = open_fixed_mount()
    try:
        LAST_STAGE = 'mountinfo_read'
        mountinfo = read_mountinfo()
        report = survey_fd(fds[-1], mountinfo)
        LAST_STAGE = 'anchor_recheck'
        anchor_stable = (physical(metadata(fds[0], 'var')) == physical(os.fstat(fds[1]))
                         and physical(metadata(fds[1], 'data')) == physical(os.fstat(fds[2])))
        if not anchor_stable:
            report.update(survey_complete=False, survey_stage='anchor_changed', mount_identity_stable=False)
        return report
    finally:
        for fd in reversed(fds):
            os.close(fd)


def emit(payload, code):
    payload.update(mode='separate_collector_storage_survey', state='diagnostic_only',
                   reason='readonly_storage_metadata',
                   heartbeat_utc=datetime.now(timezone.utc).isoformat(timespec='microseconds'),
                   order_permission='none', live_http_requests=0, active_collection=False, automatic_retry=False)
    raw = (json.dumps(payload, separators=(',', ':'), allow_nan=False) + '\n').encode()
    if len(raw) > 4096:
        return 70
    try:
        os.set_blocking(1, False)
        if os.write(1, raw) != len(raw):
            return 70
    except OSError:
        return 70
    return code


if __name__ == '__main__':
    try:
        result = main()
        code = 0
    except BaseException as exc:
        result = failure_payload(exc)
        code = 70
    raise SystemExit(emit(result, code))
