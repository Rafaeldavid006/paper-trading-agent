"""Trusted CI launcher. No project imports; root only creates restrictive staging."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tempfile

TOOLS = ('guest.py', 'suite.py', 'smoke.py', 'namespace_entry.py')

def load_hashes(path):
    data = json.loads(path.read_text())
    if set(data) != {'baseline', 'candidate'}:
        raise ValueError('unexpected_hash_manifest')
    for entries in data.values():
        if not entries:
            raise ValueError('empty_hash_manifest')
        for name, digest in entries.items():
            p = PurePosixPath(name)
            if p.is_absolute() or '..' in p.parts or not name or str(p) != name:
                raise ValueError('unsafe_source_path')
            if len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
                raise ValueError('invalid_source_digest')
            if name.endswith(('approved_release.json', '.env')):
                raise ValueError('private_or_active_input_refused')
    return data

def stage(source, dest, expected):
    dest.mkdir()
    for name, digest in expected.items():
        origin = source / name
        if any(p.is_symlink() for p in (origin, *origin.parents)) or not origin.is_file():
            raise ValueError('nonregular_source')
        body = origin.read_bytes()
        if hashlib.sha256(body).hexdigest() != digest:
            raise ValueError('source_hash_changed')
        target = dest / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        target.chmod(0o444)
    for directory in sorted((p for p in dest.rglob('*') if p.is_dir()), reverse=True):
        directory.chmod(0o555)
    dest.chmod(0o555)

def main():
    if os.geteuid() != 0 or sys.version_info[:3] != (3, 14, 8):
        raise SystemExit('exact_runtime_privileged_launcher_required')
    python = Path(sys.argv[1]).resolve(strict=True)
    if python != Path(sys.executable).resolve():
        raise SystemExit('interpreter_mismatch')
    baseline, candidate = (Path(p).resolve(strict=True) for p in sys.argv[2:4])
    support = candidate / '.qualification'
    expected = load_hashes(support / 'source_hashes.json')
    # No discovery of host files, copying of .git, or inherited application env.
    with tempfile.TemporaryDirectory(prefix='offline-runtime-') as temporary:
        root = Path(temporary)
        root.chmod(0o755)
        for label, source in (('baseline', baseline), ('candidate', candidate)):
            stage(source, root / label, expected[label])
        control = root / 'control'
        control.mkdir()
        for name in (*TOOLS, 'requirements.txt', 'source_hashes.json'):
            p = support / name
            if p.is_symlink() or not p.is_file():
                raise ValueError('invalid_control_file')
            shutil.copyfile(p, control / name)
            (control / name).chmod(0o444)
        control.chmod(0o555)
        work = root / 'work'
        work.mkdir(mode=0o700)
        os.chown(work, 65534, 65534)
        original_netns = os.readlink('/proc/self/ns/net')
        command = ['/usr/bin/timeout', '--signal=TERM', '--kill-after=5s', '1200s',
                   '/usr/bin/unshare', '--mount', '--net', '--pid', '--fork',
                   '--mount-proc', '--kill-child=KILL', str(python), '-I', '-B',
                   str(control / 'namespace_entry.py'), str(root), original_netns]
        result = subprocess.run(command, env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin'},
                                stdin=subprocess.DEVNULL, close_fds=True, timeout=1210)
        print(json.dumps({'qualification_process_exit': result.returncode,
                          'production_mutation': False, 'live_collection': False}), flush=True)
        return result.returncode

if __name__ == '__main__':
    raise SystemExit(main())
