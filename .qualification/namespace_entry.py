"""Runs as namespace init, then drops all privileges before project test imports."""
import os
from pathlib import Path
import subprocess
import sys

root = Path(sys.argv[1])
original = sys.argv[2]
if os.geteuid() != 0 or os.readlink('/proc/self/ns/net') == original:
    raise SystemExit('new_namespace_required')
# Do not add any veth, external device, route, DNS, or uplink.
subprocess.run(['/usr/sbin/ip', 'link', 'set', 'lo', 'up'], check=True,
               env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin'}, close_fds=True)
python = str(Path(sys.executable).resolve())
work = str(root / 'work')
env = {'PATH': str(Path(python).parent) + ':/usr/bin:/bin', 'HOME': work,
       'TMPDIR': work, 'PYTHONDONTWRITEBYTECODE': '1', 'LANG': 'C.UTF-8'}
os.chdir(work)
os.execve('/usr/bin/setpriv', ['/usr/bin/setpriv', '--reuid=65534', '--regid=65534',
    '--clear-groups', '--bounding-set=-all', '--inh-caps=-all', '--ambient-caps=-all',
    '--no-new-privs', python, '-I', '-B', str(root / 'control' / 'guest.py'),
    str(root), original], env)
