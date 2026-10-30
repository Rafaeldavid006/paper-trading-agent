import contextlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import collector_storage_survey as survey
import collector_supervisor as supervisor

MOUNTINFO = b'10 1 8:1 / /var/data rw,relatime - ext4 /dev/example rw\n'


class SurveyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fd = os.open(self.root, survey.DIR_FLAGS)
        self.addCleanup(os.close, self.fd)

    def test_empty_02775_mount_reproduces_gate_without_creation(self):
        os.chmod(self.root, 0o2775)
        before = list(self.root.iterdir())
        report = survey.survey_fd(self.fd, MOUNTINFO)
        self.assertTrue(report['survey_complete'])
        self.assertEqual(report['mount_mode'], 0o2775)
        self.assertTrue(report['mount_group_writable'])
        self.assertTrue(report['mount_setgid'])
        self.assertEqual(report['mount_group_matches_egid'], self.root.stat().st_gid == os.getegid())
        self.assertEqual(report['mount_group_in_supplementary'], self.root.stat().st_gid in os.getgroups())
        self.assertFalse(report['mount_exclusive_write_gate'])
        for name in ('root', 'started', 'complete'):
            self.assertFalse(report[name + '_present'])
        self.assertFalse(report['persisted_identity_checked'])
        self.assertEqual(list(self.root.iterdir()), before)

    def test_private_mount_is_classified_not_enrolled(self):
        os.chmod(self.root, 0o700)
        report = survey.survey_fd(self.fd, MOUNTINFO)
        self.assertTrue(report['mount_exclusive_write_gate'])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_partial_marker_is_metadata_only_and_preserved(self):
        marker = self.root / survey.STARTED
        marker.write_bytes(b'not-json-private-sentinel')
        os.chmod(marker, 0o600)
        before = marker.stat()
        report = survey.survey_fd(self.fd, MOUNTINFO)
        self.assertTrue(report['started_present'])
        self.assertEqual(report['started_kind'], 'regular')
        self.assertFalse(report['complete_present'])
        self.assertNotIn('private-sentinel', json.dumps(report))
        self.assertEqual(marker.read_bytes(), b'not-json-private-sentinel')
        self.assertEqual((marker.stat().st_ino, marker.stat().st_mtime_ns), (before.st_ino, before.st_mtime_ns))

    def test_root_symlink_is_not_followed(self):
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'manifest.json').write_text('private-sentinel')
        (self.root / survey.ROOT_NAME).symlink_to(outside, target_is_directory=True)
        with patch.object(survey.os, 'open', side_effect=AssertionError('no root open')):
            report = survey.survey_fd(self.fd, MOUNTINFO)
        self.assertEqual(report['root_kind'], 'symlink')
        self.assertIsNone(report['manifest_present'])

    def test_fixed_root_children_are_metadata_only(self):
        root = self.root / survey.ROOT_NAME
        root.mkdir(mode=0o700)
        for name in ('manifest.json', 'ready.json', 'ledger.sqlite', 'evidence.sqlite', 'owner.lock'):
            (root / name).write_bytes(b'private-content-never-parsed')
            os.chmod(root / name, 0o600)
        real_open = os.open
        seen = []
        def capture(path, flags, **kwargs):
            seen.append((path, flags))
            self.assertEqual(path, survey.ROOT_NAME)
            self.assertEqual(flags, survey.DIR_FLAGS)
            return real_open(path, flags, **kwargs)
        with patch.object(survey.os, 'open', side_effect=capture):
            report = survey.survey_fd(self.fd, MOUNTINFO)
        self.assertEqual(len(seen), 1)
        self.assertTrue(report['databases_same_device'])
        self.assertTrue(report['fixed_entries_stable'])
        self.assertFalse(report['persisted_identity_checked'])
        self.assertNotIn('private-content', json.dumps(report))

    def test_symlink_marker_classified_without_open(self):
        (self.root / survey.COMPLETE).symlink_to('/does-not-exist')
        report = survey.survey_fd(self.fd, MOUNTINFO)
        self.assertTrue(report['complete_present'])
        self.assertEqual(report['complete_kind'], 'symlink')

    def test_replaced_marker_reports_uncertain(self):
        marker = self.root / survey.STARTED
        marker.write_text('old')
        real = survey.metadata
        counts = {}
        def racing(fd, name):
            counts[name] = counts.get(name, 0) + 1
            if name == survey.STARTED and counts[name] == 2:
                marker.unlink()
                marker.write_text('replacement')
            return real(fd, name)
        with patch.object(survey, 'metadata', side_effect=racing):
            report = survey.survey_fd(self.fd, MOUNTINFO)
        self.assertFalse(report['survey_complete'])
        self.assertFalse(report['fixed_entries_stable'])
        self.assertEqual(report['survey_stage'], 'metadata_changed')

    def test_mountinfo_bounds_and_platform_classification(self):
        for raw in (b'x' * (survey.MOUNTINFO_LIMIT + 1), b'x' * 4097, b'\n' * 4097):
            with self.assertRaises(survey.Refused):
                survey.mount_description(raw)
        self.assertFalse(survey.mount_description(MOUNTINFO * 2)['mount_boundary_unique'])
        self.assertFalse(survey.mount_description(MOUNTINFO.replace(b'ext4', b'overlay'))['mount_persistent_type'])
        self.assertFalse(survey.mount_description(MOUNTINFO.replace(b'rw,relatime', b'ro,relatime'))['mount_rw'])

    def test_fixed_anchor_nofollow_chain(self):
        with patch.object(survey.os, 'open', side_effect=[10, 11, 12]) as op:
            self.assertEqual(survey.open_fixed_mount(), [10, 11, 12])
        self.assertEqual(op.call_args_list[0].args, ('/', survey.DIR_FLAGS))
        self.assertEqual(op.call_args_list[1].args, ('var', survey.DIR_FLAGS))
        self.assertEqual(op.call_args_list[1].kwargs, {'dir_fd': 10})
        self.assertEqual(op.call_args_list[2].args, ('data', survey.DIR_FLAGS))
        self.assertEqual(op.call_args_list[2].kwargs, {'dir_fd': 11})

    def test_fixed_anchor_failure_closes_prior_descriptors(self):
        with patch.object(survey.os, 'open', side_effect=[10, 11, OSError('private-sentinel')]), patch.object(survey.os, 'close') as close:
            with self.assertRaises(OSError):
                survey.open_fixed_mount()
        self.assertEqual([x.args[0] for x in close.call_args_list], [11, 10])

    def test_failure_vocabulary_does_not_leak_error_text(self):
        with patch.object(survey, 'LAST_STAGE', 'root_open'):
            known = survey.failure_payload(survey.Refused('root_identity_changed'))
            unknown = survey.failure_payload(survey.Refused('private-secret-sentinel'))
            operating = survey.failure_payload(OSError(13, 'private-secret-sentinel'))
        self.assertEqual(known['survey_reason'], 'root_identity_changed')
        self.assertEqual(known['survey_stage'], 'root_open')
        self.assertEqual(unknown['survey_reason'], 'unexpected_refusal')
        self.assertEqual(survey.failure_payload(survey.Refused({'private': 'sentinel'}))['survey_reason'], 'unexpected_refusal')
        self.assertEqual(operating['survey_reason'], 'os_error')
        self.assertEqual(operating['survey_errno'], 13)
        self.assertNotIn('private-secret-sentinel', json.dumps([known, unknown, operating]))

    def test_short_or_failed_atomic_output_refuses_success(self):
        for effect in (lambda fd, raw: len(raw) - 1, lambda fd, raw: (_ for _ in ()).throw(OSError(32, 'private-sentinel'))):
            with patch.object(survey.os, 'set_blocking'), patch.object(survey.os, 'write', side_effect=effect):
                self.assertEqual(survey.emit({'survey_complete': True}, 0), 70)

    def test_supervisor_only_launches_survey_with_empty_environment(self):
        child = Mock()
        child.stdout.fileno.return_value = 10
        with patch.object(supervisor.subprocess, 'Popen', return_value=child) as spawn, patch.object(supervisor.os, 'set_blocking'):
            s = supervisor.CollectorSupervisor()
            s.start()
            s.start()
        args = spawn.call_args.args[0]
        self.assertEqual(Path(args[3]).name, 'collector_storage_survey.py')
        self.assertNotIn('production_collector', args[3])
        self.assertEqual(args[1:3], ['-I', '-S'])
        self.assertEqual(spawn.call_args.kwargs['env'], {})
        self.assertEqual(spawn.call_count, 1)
        self.assertEqual(supervisor.HEARTBEAT_TIMEOUT, 15.0)

    def test_survey_scalar_fields_survive_sanitizer_and_fifo_bound(self):
        report = survey.survey_fd(self.fd, MOUNTINFO)
        report.update(order_permission='none', state='diagnostic_only', active_collection=False, live_http_requests=0)
        sanitized = supervisor.sanitized_status(report)
        self.assertEqual(set(report), set(sanitized))
        s = supervisor.CollectorSupervisor()
        s._last = sanitized
        self.assertLess(len(json.dumps(s.status(), separators=(',', ':')).encode()) + 1, 4096)
        self.assertNotEqual(report['state'], 'enrollment_complete')

    def test_readonly_guard_blocks_mutation_network_and_enumeration(self):
        module_root = str(Path(survey.__file__).resolve().parent)
        target = str(self.root / 'must-not-exist')
        script = '''import os,sys,socket,subprocess
sys.path.insert(0, ROOT)
import collector_storage_survey as s
s.install_readonly_guard()
checks=[lambda:os.open(TARGET,os.O_CREAT|os.O_WRONLY,0o600),lambda:socket.socket(),lambda:subprocess.Popen(['false']),lambda:os.listdir('/')]
count=0
for check in checks:
 try: check()
 except s.Refused: count+=1
print(count)
'''.replace('ROOT', repr(module_root)).replace('TARGET', repr(target))
        result = subprocess.run([sys.executable, '-B', '-I', '-S', '-c', script], env={}, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), b'4')
        self.assertFalse(Path(target).exists())

    def test_actual_main_limits_guard_and_emit_on_synthetic_anchor(self):
        virtual = self.root / 'synthetic_anchor'
        mount = virtual / 'var' / 'data'
        mount.mkdir(parents=True)
        os.chmod(mount, 0o2775)
        module_root = str(Path(survey.__file__).resolve().parent)
        script = '''import os,sys
sys.path.insert(0, MODULE_ROOT)
import collector_storage_survey as s
def fixture_anchor():
 fds=[os.open(VIRTUAL,s.DIR_FLAGS)]
 fds.append(os.open('var',s.DIR_FLAGS,dir_fd=fds[-1]))
 fds.append(os.open('data',s.DIR_FLAGS,dir_fd=fds[-1]))
 return fds
s.open_fixed_mount=fixture_anchor
s.read_mountinfo=lambda:MOUNTINFO
sys.argv=['collector_storage_survey.py',str(os.getppid())]
raise SystemExit(s.emit(s.main(),0))
'''.replace('MODULE_ROOT', repr(module_root)).replace('VIRTUAL', repr(str(virtual))).replace('MOUNTINFO', repr(MOUNTINFO))
        result = subprocess.run([sys.executable, '-B', '-I', '-S', '-c', script], env={}, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload['state'], 'diagnostic_only')
        self.assertEqual(payload['mount_mode'], 0o2775)
        self.assertTrue(payload['survey_complete'])
        self.assertFalse(payload['root_present'])
        self.assertEqual(payload['live_http_requests'], 0)
        self.assertEqual(list(mount.iterdir()), [])

    def test_real_supervisor_survey_terminal_frame_and_no_respawn(self):
        root = self.root / 'fake_runtime'
        root.mkdir()
        payload = {'mode':'separate_collector_storage_survey','state':'diagnostic_only','reason':'readonly_storage_metadata',
                   'order_permission':'none','active_collection':False,'live_http_requests':0,'survey_complete':True,
                   'mount_group_writable':True,'root_present':False,'started_present':False,'complete_present':False,
                   'persisted_identity_checked':False}
        (root / 'collector_storage_survey.py').write_text('print(' + repr(json.dumps(payload)) + ', flush=True)\n')
        s = supervisor.CollectorSupervisor()
        self.addCleanup(s.close)
        with patch.object(supervisor, '__file__', str(root / 'collector_supervisor.py')):
            s.start()
        deadline = time.monotonic() + 5
        while not s.reaped and time.monotonic() < deadline:
            s.poll()
            time.sleep(.01)
        self.assertTrue(s.reaped)
        self.assertEqual(s.exit_code, 0)
        self.assertEqual(s.status()['owner_status'], payload)
        child = s.child
        s.start()
        self.assertIs(s.child, child)
        self.assertFalse(s.status()['active_collection'])
        self.assertEqual(s.reason, 'owner_exited')


if __name__ == '__main__':
    unittest.main(verbosity=2)
