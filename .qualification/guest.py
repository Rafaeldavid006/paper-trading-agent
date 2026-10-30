"""No-uplink namespace controller; never receives credentials or production state."""
import collections
import ctypes
import fcntl
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import resource
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import sysconfig
import time

root = Path(sys.argv[1])
original_netns = sys.argv[2]
work = root / 'work'
control = root / 'control'

def preflight():
    if (sys.version_info[:3] != (3, 14, 8) or sys.platform != 'linux' or platform.machine() != 'x86_64'
            or sysconfig.get_config_var('Py_GIL_DISABLED') or os.geteuid() != 65534 or os.getegid() != 65534):
        raise RuntimeError('wrong_runtime_or_privileges')
    status = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
    if status['NoNewPrivs'].strip() != '1' or any(int(status[k].strip(), 16) for k in ('CapEff', 'CapPrm', 'CapBnd', 'CapAmb')):
        raise RuntimeError('capabilities_not_dropped')
    if os.getgroups() or os.readlink('/proc/self/ns/net') == original_netns:
        raise RuntimeError('isolation_missing')
    if {name for _, name in socket.if_nameindex()} != {'lo'}:
        raise RuntimeError('external_interface_present')
    if len(Path('/proc/net/route').read_text().splitlines()) != 1:
        raise RuntimeError('ipv4_route_present')
    for line in Path('/proc/net/ipv6_route').read_text().splitlines():
        if line.split()[-1] != 'lo':
            raise RuntimeError('external_ipv6_route_present')
    child = subprocess.run([sys.executable, '-I', '-c', 'import os; print(os.readlink("/proc/self/ns/net"))'],
                           env={}, close_fds=True, capture_output=True, text=True, timeout=5, check=True)
    if child.stdout.strip() != os.readlink('/proc/self/ns/net'):
        raise RuntimeError('child_network_namespace_changed')
    if Path('/var/data').exists():
        raise RuntimeError('unexpected_production_mount')
    # Local stdlib functionality, not merely successful imports.
    ssl.create_default_context()
    ctypes.CDLL(None)
    with sqlite3.connect(':memory:') as db:
        if db.execute('SELECT 1').fetchone() != (1,):
            raise RuntimeError('sqlite_unavailable')
    with (work / 'lock-probe').open('w') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(handle, fcntl.LOCK_UN)
    required = dict(line.strip().split('==') for line in (control / 'requirements.txt').read_text().splitlines() if line.strip())
    installed = {name: importlib.metadata.version(name) for name in required}
    if installed != required:
        raise RuntimeError('dependency_version_changed')
    return {'python': platform.python_version(), 'implementation': platform.python_implementation(),
            'python_build': platform.python_build(), 'machine': platform.machine(),
            'python_abi': sysconfig.get_config_var('SOABI'), 'gil_disabled': False,
            'libc': platform.libc_ver(), 'openssl': ssl.OPENSSL_VERSION,
            'sqlite': sqlite3.sqlite_version, 'dependencies': installed,
            'uid': os.geteuid(), 'no_new_privileges': True, 'capabilities': 0,
            'network_interfaces': ['lo'], 'external_routes': 0, 'child_same_network_namespace': True}

def verify_sources():
    expected = json.loads((control / 'source_hashes.json').read_text())
    for label, entries in expected.items():
        path = root / label
        actual = {p.relative_to(path).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in path.rglob('*') if p.is_file()}
        if actual != entries:
            raise RuntimeError('source_changed')
    return {label: hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
            for label, entries in expected.items()}

def run(label, args, seconds, cwd):
    log = work / (label + '.log')
    timed_out = False
    with log.open('wb') as output:
        child = subprocess.Popen(args, cwd=cwd, env=dict(os.environ), stdin=subprocess.DEVNULL,
                                 stdout=output, stderr=subprocess.STDOUT, close_fds=True, start_new_session=True)
        try:
            code = child.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(child.pid, signal.SIGKILL)
            code = child.wait(timeout=5)
        finally:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    body = log.read_bytes()
    return {'exit_code': code, 'timeout': timed_out, 'output_bytes': len(body),
            'output_sha256': hashlib.sha256(body).hexdigest()}

def read_summary(path):
    if path.is_symlink():
        return {'complete': False, 'successful': False, 'summary_state': 'nonregular_refused'}
    try:
        body = path.read_bytes()
        value = json.loads(body)
        if not isinstance(value, dict):
            raise ValueError('object_required')
        return value
    except (OSError, ValueError):
        return {'complete': False, 'successful': False, 'summary_state': 'unreadable_or_incomplete'}

def main():
    info = preflight()
    for limit, value in ((resource.RLIMIT_CORE, 0), (resource.RLIMIT_NOFILE, 128),
                         (resource.RLIMIT_NPROC, 128), (resource.RLIMIT_FSIZE, 40 * 1024 * 1024),
                         (resource.RLIMIT_AS, 2 * 1024**3), (resource.RLIMIT_CPU, 900)):
        resource.setrlimit(limit, (value, value))
    report = {'runtime': info, 'resource_limits': {'file_size_bytes': 40 * 1024 * 1024, 'file_descriptors': 128, 'processes': 128, 'address_space_bytes': 2 * 1024**3, 'cpu_seconds': 900},
              'before_source_hashes': verify_sources(),
              'synthetic_only': True, 'source_requests': 0, 'production_mutation': False,
              'scope': 'risk_based_inactive_collector_lifecycle_and_inspection_web_only',
              'not_qualified': ['render_native_binary_identity', 'production_storage', 'live_collection',
                                'resident_alpaca_runtime', 'trading', 'full_service_regression_suite'],
              'full_discovery_evidence': 'https://github.com/Rafaeldavid006/paper-trading-agent/actions/runs/41012536360',
              'runs': {}}
    python = sys.executable
    expected_hashes = json.loads((control / 'source_hashes.json').read_text())
    for label in ('baseline', 'candidate'):
        expected_modules = sum('/' not in name and name.startswith('test_') and name.endswith('.py') for name in expected_hashes[label])
        summary = work / (label + '.json')
        state = run(label, [python, '-I', '-B', str(control / 'suite.py'), str(root / label), str(summary), str(expected_modules), 'historical'], 90, work)
        if summary.is_file():
            state['suite'] = read_summary(summary)
        report['runs'][label] = state
    summary = work / 'critical.json'
    expected_modules = sum('/' not in name and name.startswith('test_') and name.endswith('.py') for name in expected_hashes['candidate'])
    state = run('critical', [python, '-I', '-B', str(control / 'suite.py'), str(root / 'candidate'), str(summary), str(expected_modules), 'critical'], 180, work)
    if summary.is_file():
        state['suite'] = read_summary(summary)
    report['runs']['critical'] = state
    report['runs']['collector_gate'] = run('collector_gate', [python, '-I', '-B',
        str(root / 'candidate' / 'production_collector' / 'build_gate.py')], 195, work)
    gate_output = (work / 'collector_gate.log').read_text(errors='replace')
    decoder = json.JSONDecoder()
    for index, char in enumerate(gate_output):
        if char != '{':
            continue
        try:
            value, _ = decoder.raw_decode(gate_output[index:])
        except ValueError:
            continue
        if isinstance(value, dict) and 'tests_run' in value and 'unraisable_errors' in value:
            keys = ('tests_run', 'failures', 'errors', 'skipped', 'unraisable_errors', 'live_http_requests', 'synthetic_only')
            report['runs']['collector_gate']['package_summary'] = {key: value[key] for key in keys if key in value}
    for label in ('baseline', 'candidate'):
        summary = work / (label + '_web.json')
        state = run(label + '_web', [python, '-I', '-B', str(control / 'smoke.py'), str(root / label), str(summary)], 30, work)
        if summary.is_file():
            state['smoke'] = read_summary(summary)
        report['runs'][label + '_web'] = state
    report['after_source_hashes'] = verify_sources()
    suites = [report['runs'][label].get('suite', {}) for label in ('baseline', 'candidate')]
    failures = [{item['test'] for kind in ('failures', 'errors') for item in suite.get(kind, [])} for suite in suites]
    comparison_available = all(suite.get('complete') is True for suite in suites)
    report['baseline_candidate_comparison'] = {'comparison_available': comparison_available,
        'shared_failing_tests': sorted(failures[0] & failures[1]) if comparison_available else None,
        'candidate_only_failing_tests': sorted(failures[1] - failures[0]) if comparison_available else None,
        'baseline_only_failing_tests': sorted(failures[0] - failures[1]) if comparison_available else None,
        'interpretation': ('Targeted historical fixtures only; overrides are explicit in each suite. Full unmodified discovery remains separately failed and is not qualified.' if comparison_available else 'INCONCLUSIVE: at least one root suite did not produce a complete report.')}
    remaining = [int(p.name) for p in Path('/proc').iterdir() if p.name.isdigit() and int(p.name) != os.getpid()]
    report['remaining_processes'] = len(remaining)
    for pid in remaining:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    while True:
        try:
            waited, _ = os.waitpid(-1, os.WNOHANG)
            if waited == 0:
                break
        except ChildProcessError:
            break
    package = report['runs']['collector_gate'].get('package_summary', {})
    report['passed'] = (all(run['exit_code'] == 0 for run in report['runs'].values()) and not remaining
        and all(suite.get('complete') is True and suite.get('successful') is True for suite in suites)
        and report['runs']['critical'].get('suite', {}).get('complete') is True
        and report['runs']['critical'].get('suite', {}).get('successful') is True
        and all(report['runs'][label + '_web'].get('smoke', {}).get('passed') is True for label in ('baseline', 'candidate'))
        and package.get('tests_run') == 95 and package.get('skipped') == 0
        and package.get('synthetic_only') is True and package.get('live_http_requests') == 0
        and package.get('failures') == 0 and package.get('errors') == 0
        and package.get('unraisable_errors') == [])
    print(json.dumps(report, sort_keys=True, indent=2), flush=True)
    return 0 if report['passed'] else 1

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({'qualification_refused': type(exc).__name__, 'no_unrestricted_fallback': True}), flush=True)
        raise SystemExit(2)
