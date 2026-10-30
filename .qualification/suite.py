"""Unmodified unittest discovery; report even runner errors without private details."""
import gc
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys
import traceback
import unittest
import warnings

root, report_path = map(Path, sys.argv[1:3])
expected_modules = int(sys.argv[3]) if len(sys.argv) > 3 else None
summary = {'complete': False, 'successful': False, 'collected': 0, 'tests_run': 0,
           'root_module_count': len(list(root.glob('test_*.py')))}
unraisable = []
warning_categories = {}
warning_sites = {}
result = None
# Open before any application imports. Never emit raw exception messages/source.
report_handle = report_path.open('w')

def save():
    report_handle.seek(0)
    json.dump(summary, report_handle, sort_keys=True)
    report_handle.truncate()
    report_handle.flush()

def record_warning(message, category, filename, lineno, file=None, line=None):
    name = category.__name__
    warning_categories[name] = warning_categories.get(name, 0) + 1
    site = Path(filename).name + ':' + str(lineno) + ':' + name
    warning_sites[site] = warning_sites.get(site, 0) + 1

def fd_count():
    try:
        return len(os.listdir('/proc/self/fd'))
    except OSError:
        return -1

def issue(test, detail):
    normalized = detail.replace(str(root), '<SOURCE>')
    normalized = re.sub(r'/[^\s\"\']*/offline-runtime-[^/\s]+/work/[^\s\"\']*', '<FIXTURE>', normalized)
    known = ('AssertionError', 'PermissionError', 'FileNotFoundError', 'ImportError',
             'ModuleNotFoundError', 'ValueError', 'TypeError', 'RuntimeError',
             'TimeoutExpired', 'OperationalError', 'AttributeError', 'KeyError', 'RecursionError', 'OSError', 'NameError')
    exception_type = next((name for name in known if re.search(r'^(?:[A-Za-z_][\w]*\.)*' + name + r':', detail, re.M)), 'other')
    markers = {'successor_code_changed': 'historical_code_pin', 'expired': 'expiry_marker',
               'outside_window': 'time_window_marker', 'Permission denied': 'permission_marker',
               'Read-only file system': 'readonly_marker', 'No module named': 'missing_module_marker',
               'No such file or directory': 'missing_file_marker', 'Too many open files': 'fd_limit_marker',
               'unable to open database file': 'sqlite_open_marker', 'database is locked': 'sqlite_locked_marker',
               'attempt to write a readonly database': 'readonly_sqlite_marker'}
    return {'test': test.id(), 'exception_type': exception_type,
            'diagnostic_categories': sorted({label for text, label in markers.items() if text in detail}),
            'detail_sha256': hashlib.sha256(normalized.encode()).hexdigest()}

class ProgressResult(unittest.TextTestResult):
    def startTest(self, test):
        super().startTest(test)
        summary.update(current_test=test.id(), tests_run=self.testsRun, phase='running')
        summary['fd_peak'] = max(summary.get('fd_peak', 0), fd_count())
        save()
    def stopTest(self, test):
        super().stopTest(test)
        # Account for finalizer warnings while releasing fixture-only cyclic resources.
        # Unlike record=True, this hook stores no WarningMessage/source object.
        gc.collect()
        summary['fd_after_latest_test_gc'] = fd_count()

try:
    save()
    sys.path.insert(0, str(root))
    os.chdir(root)
    sys.unraisablehook = lambda event: unraisable.append(type(event.exc_value).__name__)
    summary['fd_before_discovery'] = fd_count()
    with warnings.catch_warnings(record=False):
        warnings.simplefilter('always')
        warnings.showwarning = record_warning
        summary['phase'] = 'discovering'; save()
        suite = unittest.defaultTestLoader.discover(str(root), pattern='test_*.py', top_level_dir=str(root))
        summary['collected'] = suite.countTestCases(); save()
        result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0, resultclass=ProgressResult).run(suite)
        gc.collect()
    summary.update(tests_run=result.testsRun,
        failures=[issue(t, d) for t, d in result.failures],
        errors=[issue(t, d) for t, d in result.errors],
        skipped=[{'test': t.id(), 'reason_sha256': hashlib.sha256(reason.encode()).hexdigest()} for t, reason in result.skipped],
        expected_failures=[issue(t, d) for t, d in result.expectedFailures],
        unexpected_successes=[t.id() for t in result.unexpectedSuccesses], complete=True, phase='complete')
    summary['successful'] = (summary['collected'] > 0 and result.testsRun == summary['collected'] and
        (expected_modules is None or summary['root_module_count'] == expected_modules) and result.wasSuccessful())
except BaseException as exc:
    summary['runner_exception_type'] = type(exc).__name__
    if isinstance(exc, OSError):
        summary['runner_errno'] = exc.errno
    summary['runner_frames'] = [{'file': Path(f.filename).name, 'line': f.lineno, 'function': f.name}
                                for f in traceback.extract_tb(exc.__traceback__)[-8:]]
    summary['successful'] = False
finally:
    summary['warnings_by_category'] = warning_categories
    summary['warning_sites'] = warning_sites
    summary['fd_final'] = fd_count()
    summary['unraisable_types'] = unraisable
    summary['successful'] = summary['successful'] and not unraisable and not warning_categories.get('ResourceWarning')
    save(); report_handle.close()
raise SystemExit(0 if summary['successful'] else 1)
