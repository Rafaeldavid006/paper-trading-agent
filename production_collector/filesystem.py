"""Private immutable startup identity. Ordinary-corruption guard, not hostile-FS isolation."""
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat

MIN_FREE = 64 * 1024 * 1024
MAX_FILES = 32 * 1024 * 1024
DATABASES = ("ledger.sqlite", "evidence.sqlite")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return sha256(canonical(value).encode()).hexdigest()


def identity(path):
    s = path.lstat()
    return [s.st_dev, s.st_ino]


def check(path, directory=False):
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError("absolute_nonsymlink_path_required")
    s = path.lstat()
    if not (stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode)) or s.st_uid != os.getuid():
        raise ValueError("owned_regular_path_required")


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_new(path, obj):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(canonical(obj).encode()); f.flush(); os.fsync(f.fileno())


def code_hashes():
    root = Path(__file__).resolve().parent
    return {str(p.relative_to(root)): sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*.py")) if "tests" not in p.parts}


class PrivateRoot:
    def __init__(self, path, config, *, create):
        self.path = Path(path)
        self.fd = None
        check(self.path.parent, True)
        if shutil.disk_usage(self.path.parent).free < MIN_FREE:
            raise ValueError("storage_reserve")
        if create:
            self.path.mkdir(mode=0o700)
            fsync_dir(self.path.parent)
        check(self.path, True)
        if self.path.stat().st_mode & 0o077:
            raise ValueError("private_root_required")
        try:
            flags = os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT | os.O_EXCL if create else 0)
            if not create:
                check(self.path / "owner.lock")
            self.fd = os.open(self.path / "owner.lock", flags, 0o600)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.config = {**config, "root": str(self.path), "root_identity": identity(self.path),
                           "parent_identity": identity(self.path.parent), "code_hashes": code_hashes()}
            if not create:
                self.verify()
        except BaseException:
            self.close(); raise

    def finalize(self):
        for name in DATABASES:
            os.chmod(self.path / name, 0o600)
        manifest = {"config": self.config, "databases": {n: identity(self.path / n) for n in DATABASES}}
        write_new(self.path / "manifest.json", manifest)
        fsync_dir(self.path)
        write_new(self.path / "ready.json", {"manifest_hash": digest(manifest)})
        fsync_dir(self.path)

    def verify(self):
        if self.config["code_hashes"] != code_hashes():
            raise ValueError("runtime_code_changed")
        check(self.path, True)
        for n in (*DATABASES, "manifest.json", "ready.json"):
            check(self.path / n)
        for n in DATABASES:
            for suffix in ("-wal", "-shm", "-journal"):
                p = self.path / (n + suffix)
                if p.exists() or p.is_symlink():
                    check(p)
        if (self.path / "manifest.json").stat().st_size > 65536 or (self.path / "ready.json").stat().st_size > 1024:
            raise ValueError("manifest_bound")
        manifest = json.loads((self.path / "manifest.json").read_text())
        ready = json.loads((self.path / "ready.json").read_text())
        if manifest != {"config": self.config, "databases": {n: identity(self.path / n) for n in DATABASES}} or ready != {"manifest_hash": digest(manifest)}:
            raise ValueError("root_or_database_identity_mismatch")
        for name in DATABASES:
            db = sqlite3.connect((self.path / name).as_uri() + "?mode=ro", uri=True, timeout=2)
            try:
                if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise ValueError("database_integrity")
                db.execute("SELECT 1 FROM " + ("config" if name == "ledger.sqlite" else "records") + " LIMIT 1")
            finally:
                db.close()

    def capacity(self):
        if shutil.disk_usage(self.path).free < MIN_FREE:
            raise ValueError("storage_reserve")
        if sum(p.stat().st_size for p in self.path.iterdir() if p.is_file()) > MAX_FILES:
            raise ValueError("storage_budget")

    def close(self):
        if self.fd is not None:
            os.close(self.fd); self.fd = None
