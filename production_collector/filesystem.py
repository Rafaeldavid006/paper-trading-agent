"""Fresh schema-2 collector storage, cooperating-service trust model.

Render's disk is single-service, not a hostile same-service-writer sandbox.
Nofollow anchors and session pins detect observed replacement; SQLite still
uses pathnames. Complete-cohort rollback requires an external monotonic witness
and is deliberately not claimed here. No migration or identity rewriting.
"""
import fcntl
from hashlib import sha256
import json
import errno
import os
from pathlib import Path
import secrets
import re
import sqlite3
import stat

MIN_FREE = 64 * 1024 * 1024
MAX_FILES = 32 * 1024 * 1024
DATABASES = ("ledger.sqlite", "evidence.sqlite")
STORAGE_SCHEMA = 2
STORAGE_PROFILE = 'render_single_service_cooperating_group_v2'
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
PRIVATE_FILES = ('owner.lock', *DATABASES, 'manifest.json', 'ready.json')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return sha256(canonical(value).encode()).hexdigest()


def identity(path):
    s = path.lstat()
    return [s.st_dev, s.st_ino]


def check(path, directory=False):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError("absolute_nonsymlink_path_required")
    s = path.lstat()
    if not (stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode)) or s.st_uid != os.getuid():
        raise ValueError("owned_regular_path_required")
    if not directory and (stat.S_IMODE(s.st_mode) != 0o600 or s.st_nlink != 1):
        raise ValueError('private_single_link_file_required')


def fsync_dir(path):
    fd = os.open(path, DIR_FLAGS)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_new(path, obj):
    # Compatibility helper for non-runtime callers; runtime uses Anchor.write_new.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(canonical(obj).encode()); f.flush(); os.fsync(f.fileno())


def code_hashes():
    root = Path(__file__).resolve().parent
    return {str(p.relative_to(root)): sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*.py")) if "tests" not in p.parts}


def stamp(s):
    return {'device': s.st_dev, 'inode': s.st_ino, 'uid': s.st_uid,
            'gid': s.st_gid, 'mode': stat.S_IMODE(s.st_mode),
            'kind': 'directory' if stat.S_ISDIR(s.st_mode) else 'regular' if stat.S_ISREG(s.st_mode) else 'unsafe',
            'links': s.st_nlink}


def _stable(s):
    value = stamp(s)
    value.pop('links')
    return value


def _private(s, directory=False):
    if directory:
        okay = stat.S_ISDIR(s.st_mode) and stat.S_IMODE(s.st_mode) in (0o700, 0o2700)
    else:
        okay = stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode) == 0o600 and s.st_nlink == 1
    if not okay or s.st_uid != os.getuid():
        raise ValueError('private_owned_storage_required')


def mount_type():
    fd = os.open('/proc/self/mountinfo', READ_FLAGS)
    try:
        chunks, length = [], 0
        while length <= 256 * 1024:
            chunk = os.read(fd, 256 * 1024 + 1 - length)
            if not chunk:
                break
            chunks.append(chunk); length += len(chunk)
    finally:
        os.close(fd)
    raw = b''.join(chunks)
    lines = raw.splitlines()
    if len(raw) > 256 * 1024 or len(lines) > 4096 or any(len(line) > 4096 for line in lines):
        raise ValueError('mountinfo_bound')
    matches = []
    for line in lines:
        left, sep, right = line.partition(b' - ')
        fields, tail = left.split(), right.split()
        if sep and len(fields) >= 6 and len(tail) >= 3 and fields[4] == b'/var/data':
            matches.append((fields, tail))
    if len(matches) != 1 or b'rw' not in matches[0][0][5].split(b',') or b'rw' not in matches[0][1][2].split(b',') or matches[0][1][0] != b'ext4':
        raise ValueError('reviewed_rw_ext4_mount_required')
    return 'ext4'


def parent_policy(info, *, shared):
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('mount_directory_required')
    if shared:
        if not (stat.S_IMODE(info.st_mode) == 0o2775 and info.st_uid != os.getuid()
                and info.st_gid == os.getegid() and info.st_gid in os.getgroups()):
            raise ValueError('reviewed_shared_parent_required')
        return STORAGE_PROFILE
    if info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise ValueError('exclusive_owned_parent_required')
    return 'exclusive_owned_v2'


class Anchor:
    """Nofollow absolute ancestor chain; not an atomic SQLite VFS sandbox."""
    def __init__(self, path):
        self.path = Path(path)
        self.fds, self.edges, self.original = [], [], []
        self.root_fd = None
        if not self.path.is_absolute() or '..' in self.path.parts or self.path.parent == self.path:
            raise ValueError('absolute_nonsymlink_path_required')
        try:
            self.fds.append(os.open('/', DIR_FLAGS))
            for name in self.path.parent.parts[1:]:
                parent = self.fds[-1]
                fd = os.open(name, DIR_FLAGS, dir_fd=parent)
                self.fds.append(fd); self.edges.append((parent, name, fd))
            self.original = [_stable(os.fstat(fd)) for fd in self.fds]
            self.check()
            if str(self.path.parent) == '/var/data' and self.path.name != 'research-collector-ecb-v1':
                raise ValueError('fixed_shared_root_required')
            self.profile = parent_policy(os.fstat(self.mount_fd), shared=str(self.path.parent) == '/var/data')
            self.fs_type = mount_type() if str(self.path.parent) == '/var/data' else 'synthetic_test_mount'
            self.capacity()
        except BaseException:
            self.close(); raise

    @property
    def mount_fd(self):
        return self.fds[-1]

    def capacity(self):
        fs = os.fstatvfs(self.mount_fd)
        if fs.f_bavail * fs.f_frsize < MIN_FREE or fs.f_favail < 128:
            raise ValueError('storage_reserve')
        return fs

    def check(self):
        if not self.fds:
            raise ValueError('storage_anchor_closed')
        for fd, old in zip(self.fds, self.original):
            if _stable(os.fstat(fd)) != old:
                raise ValueError('anchor_physical_identity_changed')
        for parent, name, fd in self.edges:
            if _stable(os.stat(name, dir_fd=parent, follow_symlinks=False)) != _stable(os.fstat(fd)):
                raise ValueError('anchor_path_changed')
        if self.root_fd is not None:
            s = os.fstat(self.root_fd)
            _private(s, True)
            if stamp(os.stat(self.path.name, dir_fd=self.mount_fd, follow_symlinks=False)) != stamp(s):
                raise ValueError('root_path_changed')
        return True

    def exists(self, name, *, root=False):
        self.check()
        try:
            os.stat(name, dir_fd=self.root_fd if root else self.mount_fd, follow_symlinks=False)
            return True
        except FileNotFoundError:
            return False

    def open_root(self, *, create=False):
        self.check()
        if create:
            os.mkdir(self.path.name, mode=0o700, dir_fd=self.mount_fd)
            os.fsync(self.mount_fd)
        self.root_fd = os.open(self.path.name, DIR_FLAGS, dir_fd=self.mount_fd)
        self.check()

    def write_new(self, name, obj, *, root=False):
        if '/' in name or name in ('', '.', '..'):
            raise ValueError('single_component_required')
        self.check()
        parent = self.root_fd if root else self.mount_fd
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent)
        try:
            _private(os.fstat(fd))
            raw = canonical(obj).encode()
            with os.fdopen(fd, 'wb', closefd=False) as stream:
                stream.write(raw); stream.flush(); os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(parent)
        self.check()

    def read(self, name, limit=65536, *, root=True):
        if '/' in name or name in ('', '.', '..') or not 1 <= limit <= 65536:
            raise ValueError('bounded_component_required')
        self.check()
        parent = self.root_fd if root else self.mount_fd
        try:
            fd = os.open(name, READ_FLAGS, dir_fd=parent)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError('nonsymlink_storage_required') from None
            raise
        try:
            before = os.fstat(fd); _private(before)
            if before.st_size > limit:
                raise ValueError('storage_read_bound')
            raw = bytearray()
            while len(raw) <= limit:
                chunk = os.read(fd, limit + 1 - len(raw))
                if not chunk:
                    break
                raw.extend(chunk)
            after = os.fstat(fd)
            named = os.stat(name, dir_fd=parent, follow_symlinks=False)
            def full(s):
                return stamp(s), s.st_size, s.st_mtime_ns, s.st_ctime_ns
            if len(raw) > limit or full(before) != full(after) or full(after) != full(named):
                raise ValueError('storage_read_changed')
            self.check()
            return bytes(raw)
        finally:
            os.close(fd)

    def snapshot(self):
        self.check()
        nodes = {'mount': stamp(os.fstat(self.mount_fd)), 'root': stamp(os.fstat(self.root_fd))}
        for name in ('owner.lock', *DATABASES):
            s = os.stat(name, dir_fd=self.root_fd, follow_symlinks=False); _private(s)
            nodes[name] = stamp(s)
        if len({v['device'] for v in nodes.values()}) != 1:
            raise ValueError('coherent_current_device_required')
        self.check()
        again = {'mount':stamp(os.fstat(self.mount_fd)), 'root':stamp(os.fstat(self.root_fd))}
        for name in ('owner.lock', *DATABASES):
            again[name] = stamp(os.stat(name,dir_fd=self.root_fd,follow_symlinks=False))
        if again != nodes:
            raise ValueError('storage_snapshot_changed')
        self.check()
        return nodes

    def check_sidecars(self, name):
        if name not in DATABASES:
            raise ValueError('fixed_database_required')
        self.check()
        for suffix in ('-wal','-shm','-journal'):
            try:
                info = os.stat(name+suffix,dir_fd=self.root_fd,follow_symlinks=False)
            except FileNotFoundError:
                continue
            _private(info)
        self.check()

    def close(self):
        if self.root_fd is not None:
            os.close(self.root_fd); self.root_fd = None
        for fd in reversed(self.fds):
            os.close(fd)
        self.fds = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def validate_nodes(original, current):
    if type(original) is not dict or set(original) != set(current):
        raise ValueError('cohort_nodes_schema')
    if len({v.get('device') for v in original.values()}) != 1:
        raise ValueError('original_cohort_device_mixed')
    for role, now in current.items():
        prior = original[role]
        if type(prior) is not dict or set(prior) != set(now) or any(type(prior[k]) is not type(now[k]) for k in now):
            raise ValueError('cohort_stamp_schema')
        if any(prior[k] < 0 for k in ('device','inode','uid','gid','mode','links')) or prior['inode'] == 0 or prior['links'] == 0:
            raise ValueError('cohort_stamp_schema')
        exclude = {'device', 'links'} if role == 'mount' else {'device'}
        if {k:v for k,v in prior.items() if k not in exclude} != {k:v for k,v in now.items() if k not in exclude}:
            raise ValueError('root_or_database_identity_mismatch')


def read_manifest(anchor):
    manifest = json.loads(anchor.read('manifest.json'))
    ready = json.loads(anchor.read('ready.json', 1024))
    if (type(manifest) is not dict or set(manifest) != {'schema', 'config', 'nodes', 'nonce'}
            or type(manifest['schema']) is not int or manifest['schema'] != STORAGE_SCHEMA
            or type(manifest['nonce']) is not str or len(manifest['nonce']) != 64
            or any(c not in '0123456789abcdef' for c in manifest['nonce'])
            or ready != {'manifest_hash': digest(manifest)}):
        raise ValueError('storage_schema_or_ready_mismatch')
    if json.loads(anchor.read('owner.lock', 1024)) != {'schema': STORAGE_SCHEMA, 'nonce': manifest['nonce']}:
        raise ValueError('lock_cohort_mismatch')
    validate_nodes(manifest['nodes'], anchor.snapshot())
    config = manifest['config']
    if type(config) is not dict or config.get('storage_schema') != STORAGE_SCHEMA or config.get('storage_nonce') != manifest['nonce']:
        raise ValueError('storage_configuration_schema')
    for key, role in (('root_identity','root'), ('parent_identity','mount')):
        value = config.get(key)
        expected = [manifest['nodes'][role]['device'],manifest['nodes'][role]['inode']]
        if type(value) is not list or len(value) != 2 or any(type(v) is not int for v in value) or value != expected:
            raise ValueError('original_configuration_identity_mismatch')
    return manifest


def read_db_nonce(anchor, name):
    if name not in DATABASES:
        raise ValueError('fixed_database_required')
    anchor.check()
    # File existence/type/ownership is rechecked before an explicitly no-create open.
    nodes = anchor.snapshot()
    anchor.check_sidecars(name)
    db = sqlite3.connect((anchor.path / name).as_uri() + '?mode=ro', uri=True, timeout=2)
    try:
        rows = db.execute('SELECT id,schema,nonce FROM collector_storage_identity').fetchall()
        if len(rows) != 1 or rows[0][:2] != (1, STORAGE_SCHEMA):
            raise ValueError('database_cohort_schema')
        prefix, message = ('no_', 'immutable') if name == 'ledger.sqlite' else ('immutable_', 'append_only')
        for operation in ('UPDATE','DELETE'):
            trigger = prefix+'collector_storage_identity_'+operation
            found = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(trigger,)).fetchone()
            expected = f"CREATE TRIGGER {trigger} BEFORE {operation} ON collector_storage_identity BEGIN SELECT RAISE(ABORT,'{message}'); END"
            normalize = lambda text: re.sub(r'\s+','',text).rstrip(';').lower()
            if found is None or normalize(found[0]) != normalize(expected):
                raise ValueError('database_cohort_marker_guard_changed')
        result = rows[0][2]
    finally:
        db.close()
    anchor.check_sidecars(name)
    if anchor.snapshot() != nodes:
        raise ValueError('database_identity_changed_during_read')
    return result


class PinnedConnection(sqlite3.Connection):
    """Refuse subsequent SQL after an observed physical pin failure."""
    def _checked(self, method, *args, **kwargs):
        guard = getattr(self, '_storage_guard', None)
        if guard is not None:
            guard()
        result = method(*args, **kwargs)
        if guard is not None:
            guard()
        return result

    def execute(self, *args, **kwargs):
        return self._checked(super().execute, *args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._checked(super().executemany, *args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self._checked(super().executescript, *args, **kwargs)

    def commit(self):
        return self._checked(super().commit)

    def rollback(self):
        return self._checked(super().rollback)

    def __enter__(self):
        guard = getattr(self, '_storage_guard', None)
        if guard is not None:
            guard()
        return super().__enter__()

    def __exit__(self, *args):
        return self._checked(super().__exit__, *args)


class PrivateRoot:
    def __init__(self, path, config, *, create, nonce=None, parent_pin=None):
        self.path, self.fd, self.anchor = Path(path), None, None
        self._pins = None
        self._manifest = None
        try:
            self.anchor = Anchor(self.path)
            if parent_pin is not None:
                current_parent = os.fstat(self.anchor.mount_fd)
                if [current_parent.st_dev,current_parent.st_ino] != parent_pin:
                    raise ValueError("enrollment_parent_pin_changed")
            self.anchor.open_root(create=create)
            if create:
                self.nonce = nonce if nonce is not None else secrets.token_hex(32)
                if type(self.nonce) is not str or len(self.nonce) != 64 or any(c not in '0123456789abcdef' for c in self.nonce):
                    raise ValueError('storage_nonce_required')
                self.anchor.write_new('owner.lock', {'schema': STORAGE_SCHEMA, 'nonce': self.nonce}, root=True)
                for name in DATABASES:
                    self.anchor.check()
                    fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.anchor.root_fd)
                    try:
                        _private(os.fstat(fd)); os.fsync(fd)
                    finally:
                        os.close(fd)
                os.fsync(self.anchor.root_fd)
                nodes = self.anchor.snapshot()
                self.config = {**config, 'storage_schema': STORAGE_SCHEMA, 'storage_profile': self.anchor.profile,
                               'storage_nonce': self.nonce, 'root': str(self.path),
                               'root_identity': [nodes['root']['device'],nodes['root']['inode']],
                               'parent_identity': [nodes['mount']['device'],nodes['mount']['inode']], 'code_hashes':code_hashes()}
            else:
                self._manifest = read_manifest(self.anchor)
                self.nonce = self._manifest['nonce']
                self.config = self._manifest['config']
                expected = {**config, 'storage_schema':STORAGE_SCHEMA, 'storage_profile':self.anchor.profile,
                            'storage_nonce':self.nonce, 'root':str(self.path), 'code_hashes':code_hashes()}
                if (type(self.config) is not dict or set(self.config) != set(expected) | {'root_identity','parent_identity'}
                        or any(self.config.get(k) != v for k,v in expected.items())):
                    raise ValueError('root_configuration_mismatch')
            self.fd = os.open('owner.lock', os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.anchor.root_fd)
            _private(os.fstat(self.fd))
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._pins = self.anchor.snapshot()
            if stamp(os.fstat(self.fd)) != self._pins['owner.lock']:
                raise ValueError('lock_identity_changed')
            if not create:
                self.verify()
        except BaseException:
            self.close(); raise

    def check_anchors(self):
        self.anchor.check()
        current = self.anchor.snapshot()
        if self._pins is not None:
            for role, old in self._pins.items():
                exclude = {'links'} if role == 'mount' else set()
                if {k:v for k,v in old.items() if k not in exclude} != {k:v for k,v in current[role].items() if k not in exclude}:
                    raise ValueError('session_physical_identity_changed')
        if self.fd is not None and stamp(os.fstat(self.fd)) != current['owner.lock']:
            raise ValueError('lock_identity_changed')
        return True

    def open_database(self, name, *, readonly=False, factory=PinnedConnection, isolation_level=None, timeout=2):
        if name not in DATABASES:
            raise ValueError('fixed_database_required')
        self.check_anchors()
        self.anchor.check_sidecars(name)
        db = sqlite3.connect((self.path / name).as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                             uri=True, timeout=timeout, isolation_level=isolation_level, factory=factory)
        try:
            self.check_anchors()
            self.anchor.check_sidecars(name)
            if isinstance(db, PinnedConnection):
                db._storage_guard = self.check_anchors
            return db
        except BaseException:
            db.close(); raise

    def finalize(self):
        self.check_anchors()
        for name in DATABASES:
            if read_db_nonce(self.anchor,name) != self.nonce:
                raise ValueError('database_cohort_mismatch')
        self._manifest = {'schema':STORAGE_SCHEMA, 'config':self.config, 'nodes':self.anchor.snapshot(), 'nonce':self.nonce}
        self.anchor.write_new('manifest.json', self._manifest, root=True)
        self.anchor.write_new('ready.json', {'manifest_hash':digest(self._manifest)}, root=True)
        self.check_anchors()

    def verify(self):
        self.check_anchors()
        if self.config['code_hashes'] != code_hashes():
            raise ValueError('runtime_code_changed')
        manifest = read_manifest(self.anchor)
        if manifest != self._manifest or manifest['config'] != self.config:
            raise ValueError('root_or_database_identity_mismatch')
        for name in DATABASES:
            for suffix in ('-wal','-shm','-journal'):
                if self.anchor.exists(name+suffix,root=True):
                    _private(os.stat(name+suffix,dir_fd=self.anchor.root_fd,follow_symlinks=False))
            if read_db_nonce(self.anchor,name) != self.nonce:
                raise ValueError('database_cohort_mismatch')
            db = self.open_database(name,readonly=True)
            try:
                if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise ValueError('database_integrity')
                db.execute('SELECT 1 FROM '+('config' if name=='ledger.sqlite' else 'records')+' LIMIT 1')
            finally:
                db.close()
        self.check_anchors()

    def capacity(self):
        self.check_anchors(); self.anchor.capacity()
        if sum(p.stat().st_size for p in self.path.iterdir() if p.is_file()) > MAX_FILES:
            raise ValueError('storage_budget')
        self.check_anchors()

    def close(self):
        if self.fd is not None:
            os.close(self.fd); self.fd = None
        if self.anchor is not None:
            self.anchor.close(); self.anchor = None
