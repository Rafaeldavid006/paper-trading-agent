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
caught = []
result = None
# Open before any application imports. Never emit raw exception messages/source.
report_handle = report_path.open('w')

def save():
    report_handle.seek(0)
    json.dump(summary, report_handle, sort_keys=True)
    report_handle.truncate()
    report_handle.flush()

def issue(test, detail):
    normalized = detail.replace(str(root), '<SOURCE>')
    normalized = re.sub(r'/[^\s\"\']*/offline-runtime-[^/\s]+/work/[^\s\"\']*', '<FIXTURE>', normalized)
    known = ('AssertionError', 'PermissionError', 'FileNotFoundError', 'ImportError',
             'ModuleNotFoundError', 'ValueError', 'TypeError', 'RuntimeError',
             'TimeoutExpired', 'OperationalError', 'AttributeError', 'KeyError', 'RecursionError')
    exception_type = next((name for name in known if re.search(r'^' + name + r':', detail, re.M)), 'other')
    markers = {'successor_code_changed': 'historical_code_pin', 'expired': 'expiry_marker',
               'outside_window': 'time_window_marker', 'Permission denied': 'permission_marker',
               'Read-only file system': 'readonly_marker', 'No module named': 'missing_module_marker',
               'No such file or directory': 'missing_file_marker', 'Too many open files': 'fd_limit_marker'}
    return {'test': test.id(), 'exception_type': exception_type,
            'diagnostic_categories': sorted({label for text, label in markers.items() if text in detail}),
            'detail_sha256': hashlib.sha256(normalized.encode()).hexdigest()}

class ProgressResult(unittest.TextTestResult):
    def startTest(self, test):
        super().startTest(test)
        summary.update(current_test=test.id(), tests_run=self.testsRun, phase='running')
        save()

try:
    save()
    sys.path.insert(0, str(root))
    os.chdir(root)
    sys.unraisablehook = lambda event: unraisable.append(type(event.exc_value).__name__)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
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
    categories = {}
    for warning in caught:
        key = warning.category.__name__; categories[key] = categories.get(key, 0) + 1
    summary['warnings_by_category'] = categories
    summary['unraisable_types'] = unraisable
    summary['successful'] = summary['successful'] and not unraisable and not categories.get('ResourceWarning')
    save(); report_handle.close()
raise SystemExit(0 if summary['successful'] else 1)
