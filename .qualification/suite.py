"""Unmodified unittest discovery; public summary omits raw exception/provider text."""
import gc
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys
import unittest
import warnings

root, report_path = map(Path, sys.argv[1:3])
expected_modules = int(sys.argv[3]) if len(sys.argv) > 3 else None
sys.path.insert(0, str(root))
os.chdir(root)
unraisable = []
sys.unraisablehook = lambda event: unraisable.append(type(event.exc_value).__name__)
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    suite = unittest.defaultTestLoader.discover(str(root), pattern='test_*.py', top_level_dir=str(root))
    collected = suite.countTestCases()
    result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(suite)
    gc.collect()

def issue(test, detail):
    normalized = detail.replace(str(root), '<SOURCE>')
    normalized = re.sub(r'/[^\s\"\']*/offline-runtime-[^/\s]+/work/[^\s\"\']*', '<FIXTURE>', normalized)
    # Fixed labels only: never export raw exception messages, object reprs or paths.
    known = ('AssertionError', 'PermissionError', 'FileNotFoundError', 'ImportError',
             'ModuleNotFoundError', 'ValueError', 'TypeError', 'RuntimeError',
             'TimeoutExpired', 'OperationalError', 'AttributeError', 'KeyError')
    exception_type = next((name for name in known if re.search(r'^' + name + r':', detail, re.M)), 'other')
    markers = {'successor_code_changed': 'historical_code_pin',
               'expired': 'expiry_marker', 'outside_window': 'time_window_marker',
               'Permission denied': 'permission_marker', 'Read-only file system': 'readonly_marker',
               'No module named': 'missing_module_marker', 'No such file or directory': 'missing_file_marker'}
    return {'test': test.id(), 'exception_type': exception_type,
            'diagnostic_categories': sorted({label for text, label in markers.items() if text in detail}),
            'detail_sha256': hashlib.sha256(normalized.encode()).hexdigest()}

categories = {}
for warning in caught:
    key = warning.category.__name__
    categories[key] = categories.get(key, 0) + 1
summary = {'collected': collected, 'tests_run': result.testsRun,
    'failures': [issue(t, d) for t, d in result.failures],
    'errors': [issue(t, d) for t, d in result.errors],
    'skipped': [{'test': t.id(), 'reason_sha256': hashlib.sha256(reason.encode()).hexdigest()}
                for t, reason in result.skipped],
    'expected_failures': [issue(t, d) for t, d in result.expectedFailures],
    'unexpected_successes': [t.id() for t in result.unexpectedSuccesses],
    'warnings_by_category': categories, 'unraisable_types': unraisable,
    'root_module_count': len(list(root.glob('test_*.py'))),
    'successful': (collected > 0 and result.testsRun == collected and
        (expected_modules is None or len(list(root.glob('test_*.py'))) == expected_modules) and
        result.wasSuccessful() and not unraisable and not categories.get('ResourceWarning'))}
report_path.write_text(json.dumps(summary, sort_keys=True))
raise SystemExit(0 if summary['successful'] else 1)
