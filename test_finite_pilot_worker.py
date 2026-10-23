"""Disposable cohorts only; sockets/DNS forbidden before all project imports."""
from contextlib import ExitStack
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

with ExitStack() as _imports:
    for _name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
        _imports.enter_context(patch(_name, side_effect=AssertionError('NETWORK FORBIDDEN')))
    import finite_pilot_contract as contract
    import finite_pilot_worker as worker
    from finite_pilot_anchor import AnchorJournal
    from finite_pilot_transport import FinitePilotTransport, RecoveryReadOnlyTransport
    from native_paper_execution import NativePaperHTTP
    from primary_earnings_source import NvidiaReadOnlyHTTP
    from runtime_state_bundle import GENERATION, RuntimeStateBundle
    from test_readonly_runtime_worker import RuntimeWorkerTests
    from test_primary_earnings_source import feed_bytes, item_xml
    from test_native_observation_worker import FP
    from test_finite_pilot_anchor import initialize_synthetic_foundation
    from alpaca_schema import timestamp


class EnvironmentTrap:
    def __getattribute__(self, name):
        raise AssertionError('NO ENVIRONMENT ACCESS: ' + name)


class DisabledBoundaryTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            self.enterContext(patch(name, side_effect=AssertionError('NETWORK FORBIDDEN')))

    def test_none_manifest_returns_before_any_state_secret_clock_or_factory(self):
        self.assertIsNone(contract.APPROVED_PILOT)
        self.assertIsNone(contract.APPROVED_RECOVERY)
        trap = Mock(side_effect=AssertionError('NO I/O'))
        with patch.object(worker, 'RuntimeStateBundle', trap), patch('sqlite3.connect', trap), \
             patch('builtins.open', trap), patch.object(worker.os, 'environ', EnvironmentTrap()):
            for phase in worker.PHASES:
                result = worker.run_phase(phase, environ=EnvironmentTrap(), now_ns=trap,
                    paths=EnvironmentTrap(), broker_factory=trap, source_factory=trap)
                self.assertEqual(result['phase'], 'disabled')
                self.assertEqual(result['order_permission'], 'none')
        trap.assert_not_called()

    def test_boolean_and_json_receipts_never_create_authority(self):
        for forged in (True, {'approved': True}, {'pilot_id': 'x'}, object()):
            with self.subTest(forged=type(forged)), patch.object(contract, 'APPROVED_PILOT', forged), \
                 patch.object(worker, 'RuntimeStateBundle') as bundle:
                with self.assertRaises(contract.PilotRefused):
                    worker.run_phase('tick')
                bundle.assert_not_called()

    def test_cli_accepts_only_one_fixed_phase_and_redacts_failures(self):
        for args in ([], ['--execute'], ['--tick', '--enabled'], ['--tick', '/tmp/approval.json'], ['tick']):
            with patch('builtins.print') as emit:
                self.assertEqual(worker.main(args), 2)
                self.assertEqual(json.loads(emit.call_args.args[0])['phase'], 'invalid')
        with patch.object(worker, 'run_phase', side_effect=RuntimeError('private-key-private-account')), \
             patch('builtins.print') as emit:
            self.assertEqual(worker.main(['--tick']), 1)
        self.assertNotIn('private', emit.call_args.args[0])
        self.assertEqual(json.loads(emit.call_args.args[0])['phase'], 'manual_handoff')

    def test_public_summary_refuses_unreviewed_content(self):
        for kwargs in ({'account_id': 'secret'}, {'events': True}, {'events': 'secret'}, {'outcome': 'private'}):
            with self.assertRaises(contract.PilotRefused):
                worker.public('tick', 'bounded_tick_finished', **kwargs)


class FiniteRuntimeTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            self.enterContext(patch(name, side_effect=AssertionError('NETWORK FORBIDDEN')))
        self.h = RuntimeWorkerTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)
        self.h.now = self.h.now.replace(minute=31)
        self.h.initialize()  # Existing read-only cohort, not pilot enrollment.
        self.bundle = RuntimeStateBundle(FP, paths=self.h.paths, test_only=True)
        founding = initialize_synthetic_foundation(self.bundle, self.ns)
        self.manifest = contract.PilotManifest('SYNTHETIC-NOT-AN-APPROVAL',
            self.ns() - 30 * contract.NS, self.ns() + 1800 * contract.NS, GENERATION,
            hashlib.sha256(Path(self.h.paths['seal']).read_bytes()).hexdigest(), ('NVDA',),
            10, 8, 120, 'a' * 64, 'SYNTHETIC-NOT-A-GRANT',
            **founding)
        self.enterContext(patch.object(contract, 'APPROVED_PILOT', self.manifest))
        self.kw = dict(self.h.kw, monotonic_ns=lambda: 100_000_000_000)
        self.assertEqual(self.stage('initialize')['phase'], 'initialize')

    def ns(self):
        return timestamp(self.h.now.isoformat())

    def stage(self, phase):
        return worker.run_phase(phase, **self.kw)

    def ready(self):
        self.h.http.feed = feed_bytes(item_xml(date='Tue, 20 Oct 2026 13:31:00 GMT'))
        self.stage('source')
        for _ in range(15):
            self.h.now += timedelta(seconds=20)
            self.stage('source')

    def test_actual_source_bytes_drive_bounded_native_tick_once(self):
        self.ready()
        report = self.stage('tick')
        self.assertEqual(report['outcome'], 'complete', report)
        self.assertEqual(report['order_permission'], 'finite_paper_scope')
        posts = [c for c in self.h.wire.calls if c[0] == 'POST']
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0][2]['symbol'], 'NVDA')
        self.assertEqual(posts[0][2]['qty'], '1')
        self.assertEqual(posts[0][2]['time_in_force'], 'day')
        self.assertFalse(posts[0][2]['extended_hours'])
        self.assertEqual(len([c for c in self.h.wire.calls if c[1].startswith('/v2/calendar?')]), 1)
        self.assertEqual([c[0] for c in self.h.wire.calls], ['GET'] * 11 + ['POST'])
        with sqlite3.connect(self.h.paths['lifecycle']) as db:
            self.assertEqual(db.execute('SELECT state FROM native_dispatch').fetchall(), [('observed',)])

    def test_source_never_reads_credentials_or_calls_broker(self):
        self.kw['environ'] = EnvironmentTrap()
        report = self.stage('source')
        self.assertEqual(report['source_requests'], 1)
        self.assertEqual(self.h.wire.calls, [])

    def test_unknown_source_failure_is_counted_and_halted_across_restart(self):
        self.h.http.get = Mock(side_effect=TimeoutError('private-source-detail'))
        with self.assertRaises(Exception):
            self.stage('source')
        self.assertEqual(self.h.http.get.call_count, 1)
        with self.assertRaises(Exception):
            self.stage('source')
        self.assertEqual(self.h.http.get.call_count, 1)
        self.assertEqual(self.h.wire.calls, [])
        with sqlite3.connect(self.h.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT sources,dirty FROM finite_pilot_control').fetchone(), (1, 1))

    def test_existing_halt_rejected_before_keys_or_provider(self):
        with sqlite3.connect(self.h.paths['runner']) as db:
            db.execute("UPDATE runner_control SET dirty=1,attention='readonly_runtime_stage_failed',stage='started'")
        self.kw['environ'] = EnvironmentTrap()
        for phase in ('source', 'tick'):
            with self.assertRaises(Exception):
                self.stage(phase)
        self.assertEqual(self.h.wire.calls, [])
        self.assertEqual(self.h.http.calls, [])

    def test_missing_enrolled_file_not_recreated_before_keys(self):
        Path(self.h.paths['runner']).unlink()
        self.kw['environ'] = EnvironmentTrap()
        for phase in ('source', 'tick', 'initialize'):
            with self.assertRaises(Exception):
                self.stage(phase)
            self.assertFalse(Path(self.h.paths['runner']).exists())

    def test_uncertain_post_consumes_attempt_and_restart_never_replays(self):
        self.ready()
        self.h.wire.fail_post = True
        self.assertEqual(self.stage('tick')['phase'], 'manual_handoff')
        self.assertEqual(len([c for c in self.h.wire.calls if c[0] == 'POST']), 1)
        with self.assertRaises(Exception):
            self.stage('tick')
        self.assertEqual(len([c for c in self.h.wire.calls if c[0] == 'POST']), 1)
        with sqlite3.connect(self.h.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT writes,dirty FROM finite_pilot_control').fetchone(), (1, 1))
        with sqlite3.connect(self.h.paths['lifecycle']) as db:
            self.assertEqual(db.execute('SELECT state FROM native_dispatch').fetchall(), [('uncertain',)])

    def test_expired_scope_stops_before_new_keys_or_provider(self):
        self.h.now += timedelta(seconds=1800)
        self.kw['environ'] = EnvironmentTrap()
        for phase in ('source', 'tick'):
            with self.assertRaises(Exception):
                self.stage(phase)
        self.assertEqual(self.h.wire.calls, [])
        self.assertEqual(self.h.http.calls, [])

    def test_persisted_stop_during_asset_get_blocks_post(self):
        self.ready()
        def stop(method, path, body):
            if path.startswith('/v2/assets/'):
                with sqlite3.connect(self.h.paths['runner']) as db:
                    db.execute("UPDATE runner_control SET stopped=1,stage='stopped'")
        self.h.wire.on_call = stop
        self.assertEqual(self.stage('tick')['phase'], 'manual_handoff')
        self.assertEqual([c for c in self.h.wire.calls if c[0] == 'POST'], [])

    def test_final_gate_is_after_native_request_preparation(self):
        self.ready()
        self.kw['broker_factory'] = lambda *a, **k: canned_native(self.h.wire)
        import native_paper_execution
        original = native_paper_execution.request.Request
        def prepare(*args, **kwargs):
            req = original(*args, **kwargs)
            if req.get_method() == 'POST':
                self.h.now += timedelta(seconds=1800)
            return req
        with patch.object(native_paper_execution.request, 'Request', side_effect=prepare):
            self.assertEqual(self.stage('tick')['phase'], 'manual_handoff')
        self.assertEqual([c for c in self.h.wire.calls if c[0] == 'POST'], [])
        with sqlite3.connect(self.h.paths['lifecycle']) as db:
            self.assertEqual(db.execute('SELECT state FROM native_dispatch').fetchall(), [('uncertain',)])

    def test_real_native_http_with_canned_opener_uses_final_gate(self):
        self.ready()
        self.kw['broker_factory'] = lambda *a, **k: canned_native(self.h.wire)
        report = self.stage('tick')
        self.assertEqual(report['outcome'], 'complete', report)
        self.assertEqual(len([c for c in self.h.wire.calls if c[0] == 'POST']), 1)

    def test_budget_wait_before_anchor_begin_is_clean_and_not_a_tick(self):
        from native_account_guard import SharedAccountGuard
        guard = SharedAccountGuard(self.h.paths['legacy'], self.h.paths['lifecycle'], FP,
            enabled=True, state_database=self.h.paths['guard'], test_only=True)
        with guard.hold(now_ns=self.ns(), read_only=True):
            guard.reserve_request(now_ns=self.ns())
        before = {role: Path(path).read_bytes() for role, path in self.h.paths.items()}
        result = self.stage('tick')
        self.assertEqual(result['phase'], 'tick', result)
        self.assertEqual(result['outcome'], 'waiting_request_budget', result)
        self.assertEqual(self.h.wire.calls, [])
        self.assertEqual(before, {role: Path(path).read_bytes() for role, path in self.h.paths.items()})
        self.assertEqual(self.stage('initialize')['phase'], 'initialize')

    def test_budget_wait_after_anchor_begin_is_handoff_not_retry_success(self):
        from native_account_guard import SharedAccountGuard, AccountGuardRefused
        original = SharedAccountGuard.admit_cycle
        held = []
        def late_refusal(guard, **kwargs):
            held.append((id(guard), guard._held))
            if len(held) == 2:
                raise AccountGuardRefused('cycle_budget_wait')
            return original(guard, **kwargs)
        with patch.object(SharedAccountGuard, 'admit_cycle', late_refusal):
            result = self.stage('tick')
        self.assertEqual(held, [(held[0][0], True)] * 2)
        self.assertEqual(result['phase'], 'manual_handoff')
        self.assertEqual(result['error_code'], 'tick_completion_unproven')
        self.assertEqual(self.h.wire.calls, [])
        with self.assertRaises(Exception):
            self.stage('initialize')
        with self.assertRaises(Exception):
            self.stage('tick')

    def test_actual_post_holds_all_original_stop_locks_and_anchor(self):
        self.ready()
        locks = []
        def contender(method, path, body):
            if method == 'POST':
                for role in ('runner', 'decision', 'lifecycle', 'anchor'):
                    with sqlite3.connect(self.h.paths[role], timeout=0) as db:
                        with self.assertRaises(sqlite3.OperationalError):
                            db.execute('BEGIN IMMEDIATE')
                    locks.append(role)
        self.h.wire.on_call = contender
        self.assertEqual(self.stage('tick')['outcome'], 'complete')
        self.assertEqual(locks, ['runner', 'decision', 'lifecycle', 'anchor'])

    def test_source_expiry_during_request_preparation_counts_but_never_opens(self):
        import primary_earnings_source
        raw = NvidiaReadOnlyHTTP(enabled=True)
        opener = Mock()
        opener.open.side_effect = AssertionError('EXPIRED SOURCE MUST NOT OPEN')
        raw._opener = opener
        self.kw['source_factory'] = lambda **kwargs: raw
        original = primary_earnings_source.request.Request
        def prepare(*args, **kwargs):
            req = original(*args, **kwargs)
            self.h.now += timedelta(seconds=1800)
            return req
        with patch.object(primary_earnings_source.request, 'Request', side_effect=prepare):
            with self.assertRaises(Exception):
                self.stage('source')
        opener.open.assert_not_called()
        with sqlite3.connect(self.h.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT sources,dirty FROM finite_pilot_control').fetchone(), (1, 1))

    def capped_runtime(self, **limits):
        h = RuntimeWorkerTests()
        h.setUp()
        self.addCleanup(h.doCleanups)
        h.now = h.now.replace(minute=31)
        with patch.object(contract, 'APPROVED_PILOT', None):
            h.initialize()
        founding = initialize_synthetic_foundation(RuntimeStateBundle(FP, paths=h.paths, test_only=True),
                                                   lambda: timestamp(h.now.isoformat()))
        manifest = replace(self.manifest,
            bundle_seal_sha256=hashlib.sha256(Path(h.paths['seal']).read_bytes()).hexdigest(),
            **founding, **limits)
        self.enterContext(patch.object(contract, 'APPROVED_PILOT', manifest))
        kwargs = dict(h.kw, monotonic_ns=lambda: 100_000_000_000)
        run = lambda phase: worker.run_phase(phase, **kwargs)
        self.assertEqual(run('initialize')['phase'], 'initialize')
        return h, run, kwargs

    def test_last_source_request_finishes_then_hands_off_without_extra_io(self):
        h, run, kwargs = self.capped_runtime(max_source_requests=1)
        report = run('source')
        self.assertEqual(report['phase'], 'manual_handoff')
        self.assertEqual(report['order_permission'], 'revoked')
        with sqlite3.connect(h.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT sources,dirty FROM finite_pilot_control').fetchone(), (1, 0))
        kwargs['environ'] = EnvironmentTrap()
        for phase in ('source', 'tick'):
            with self.assertRaises(Exception):
                run(phase)
        self.assertEqual(len(h.http.calls), 1)
        self.assertEqual(h.wire.calls, [])

    def test_last_tick_finishes_then_hands_off_without_extra_io(self):
        h, run, kwargs = self.capped_runtime(max_ticks=1)
        self.assertEqual(run('tick')['phase'], 'manual_handoff')
        with sqlite3.connect(h.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT ticks,dirty FROM finite_pilot_control').fetchone(), (1, 0))
        calls = len(h.wire.calls)
        kwargs['environ'] = EnvironmentTrap()
        with self.assertRaises(Exception):
            run('tick')
        self.assertEqual(len(h.wire.calls), calls)

    def test_last_write_finishes_then_hands_off_without_second_native_claim(self):
        h, run, kwargs = self.capped_runtime(max_write_attempts=1)
        h.http.feed = feed_bytes(item_xml(date='Tue, 20 Oct 2026 13:31:00 GMT'))
        run('source')
        for _ in range(15):
            h.now += timedelta(seconds=20)
            run('source')
        self.assertEqual(run('tick')['phase'], 'manual_handoff')
        with sqlite3.connect(h.paths['anchor']) as db:
            self.assertEqual(db.execute('SELECT writes,dirty FROM finite_pilot_control').fetchone(), (1, 0))
        with sqlite3.connect(h.paths['lifecycle']) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM intents').fetchone(), (1,))
        calls = len(h.wire.calls)
        kwargs['environ'] = EnvironmentTrap()
        with self.assertRaises(Exception):
            run('tick')
        self.assertEqual(len(h.wire.calls), calls)
        with sqlite3.connect(h.paths['lifecycle']) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM intents').fetchone(), (1,))

    def test_source_expiry_during_last_control_read_never_opens(self):
        import primary_earnings_source
        raw = NvidiaReadOnlyHTTP(enabled=True)
        opener = Mock()
        raw._opener = opener
        self.kw['source_factory'] = lambda **kwargs: raw
        prepared = []
        original_request = primary_earnings_source.request.Request
        original_controls = AnchorJournal._controls
        def prepare(*args, **kwargs):
            req = original_request(*args, **kwargs)
            prepared.append(True)
            return req
        def controls(journal, *args, **kwargs):
            result = original_controls(journal, *args, **kwargs)
            if prepared:
                self.h.now += timedelta(seconds=1800)
            return result
        with patch.object(primary_earnings_source.request, 'Request', side_effect=prepare), \
             patch.object(AnchorJournal, '_controls', controls):
            with self.assertRaises(Exception):
                self.stage('source')
        opener.open.assert_not_called()

    def test_factory_or_path_override_is_not_a_production_api(self):
        with self.assertRaises(contract.PilotRefused):
            worker.run_phase('tick', paths=self.h.paths)
        with self.assertRaises(contract.PilotRefused):
            worker.run_phase('tick', broker_factory=lambda *a: self.h.wire)


class RecoveryRuntimeTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            self.enterContext(patch(name, side_effect=AssertionError('NETWORK FORBIDDEN')))
        self.h = RuntimeWorkerTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)
        self.h.initialize()
        self.bundle = RuntimeStateBundle(FP, paths=self.h.paths, test_only=True)
        with sqlite3.connect(self.h.paths['runner']) as db:
            db.execute("UPDATE runner_control SET heartbeat=?,dirty=1,attention='readonly_runtime_stage_failed',stage='started'",
                       (self.h.now.timestamp(),))
            self.original = db.execute('SELECT * FROM runner_control').fetchone()
        journal = AnchorJournal(self.bundle, lambda: timestamp(self.h.now.isoformat()))
        ns = timestamp(self.h.now.isoformat())
        founding = initialize_synthetic_foundation(self.bundle, ns)
        self.manifest = contract.RecoveryManifest(contract.INCIDENT_ID, ns - contract.NS,
            ns + 60 * contract.NS, GENERATION,
            hashlib.sha256(Path(self.h.paths['seal']).read_bytes()).hexdigest(), contract.digest(self.original),
            journal.protected_state_digest(), 'a' * 64, 'b' * 64, 'c' * 64, 'd' * 64,
            'SYNTHETIC-NOT-A-RECOVERY-APPROVAL',
            **founding)
        self.enterContext(patch.object(contract, 'APPROVED_RECOVERY', self.manifest))
        self.enterContext(patch.object(worker.os, 'environ', self.h.env))
        transport = RecoveryReadOnlyTransport
        def synthetic(key, secret, **kwargs):
            return transport(key, secret, test_only=True,
                             transport_factory=lambda *a, **k: self.h.wire, **kwargs)
        self.enterContext(patch.object(worker, 'RecoveryReadOnlyTransport', side_effect=synthetic))

    def recover(self):
        return worker.run_phase('recover', **self.h.kw)

    def current_runner(self):
        with sqlite3.connect(self.h.paths['runner']) as db:
            return db.execute('SELECT * FROM runner_control').fetchone()

    def test_actual_get_observer_retains_halt_until_exact_cas(self):
        from decimal_lifecycle import Lifecycle
        seen = []
        def check_halt(*args):
            seen.append(self.current_runner())
            self.assertEqual(seen[-1], self.original)
        self.h.wire.on_call = check_halt
        before = {role: Path(self.h.paths[role]).read_bytes()
                  for role in ('source', 'lifecycle', 'decision')}
        with patch.object(Lifecycle, 'observe', side_effect=AssertionError('NO OBSERVE BEFORE CAS')):
            report = self.recover()
            self.assertEqual(report['phase'], 'recover')
            self.assertEqual(report['order_permission'], 'none')
        self.assertTrue(seen)
        self.assertTrue(all(method == 'GET' for method, _, _ in self.h.wire.calls))
        self.assertLessEqual(len(self.h.wire.calls), 12)
        self.assertEqual(self.current_runner()[2:], (0, 0, None, 'recovered_readonly'))
        self.assertEqual(before, {role: Path(self.h.paths[role]).read_bytes() for role in before})
        calls = len(self.h.wire.calls)
        self.h.now += timedelta(seconds=120)
        self.assertEqual(self.recover()['phase'], 'recover')
        self.assertEqual(len(self.h.wire.calls), calls)  # Applied receipt only, no replay.

    def test_unknown_position_remains_halted_and_attempt_consumed(self):
        from test_native_observation_worker import F
        self.h.wire.positions = [dict(F['positions'][0], symbol='NVDA')]
        with self.assertRaises(Exception):
            self.recover()
        self.assertEqual(self.current_runner(), self.original)
        count = len(self.h.wire.calls)
        with self.assertRaises(Exception):
            self.recover()
        self.assertEqual(len(self.h.wire.calls), count)
        self.assertTrue(all(m == 'GET' for m, _, _ in self.h.wire.calls))

    def test_unknown_order_remains_halted(self):
        import copy
        order = copy.deepcopy(self.h.wire.legacy[0])
        order.update(id='unknown-native-id', client_order_id='external-native-id', symbol='NVDA')
        self.h.wire.native.append(order)
        with self.assertRaises(Exception):
            self.recover()
        self.assertEqual(self.current_runner(), self.original)
        self.assertTrue(all(m == 'GET' for m, _, _ in self.h.wire.calls))

    def test_malformed_observation_never_clears_halt_and_never_retries(self):
        self.h.wire.legacy[0]['submitted_at'] = None
        with self.assertRaises(Exception):
            self.recover()
        self.assertEqual(self.current_runner(), self.original)
        calls = len(self.h.wire.calls)
        with self.assertRaises(Exception):
            self.recover()
        self.assertEqual(len(self.h.wire.calls), calls)

    def test_recovery_transport_rejects_every_write_before_backend(self):
        # Actual typed manifest is not permission for any recovery POST/DELETE.
        transport = RecoveryReadOnlyTransport('fixture', 'fixture', manifest=self.manifest,
            now_ns=lambda: timestamp(self.h.now.isoformat()), started_ns=timestamp(self.h.now.isoformat()),
            test_only=True, transport_factory=lambda *a, **k: self.h.wire)
        for method in ('POST', 'DELETE', 'PATCH'):
            with self.assertRaises(contract.PilotRefused):
                transport.call(method, '/v2/orders')
        self.assertEqual(self.h.wire.calls, [])


class Response:
    def __init__(self, url, status, data):
        self.url, self.status = url, status
        self.body = json.dumps(data).encode() if data is not None else b''
        self.headers = {'Content-Encoding': 'identity'}
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def geturl(self):
        return self.url
    def read(self, size):
        return self.body[:size]


class CannedOpener:
    def __init__(self, wire):
        self.wire = wire
    def open(self, req, **kwargs):
        split = urlsplit(req.full_url)
        path = split.path + ('?' + split.query if split.query else '')
        body = None if req.data is None else json.loads(req.data)
        status, data = self.wire.call(req.get_method(), path, body)
        if status >= 400:
            from urllib.error import HTTPError
            raise HTTPError(req.full_url, status, 'synthetic', {}, None)
        return Response(req.full_url, status, data)


def canned_native(wire):
    native = NativePaperHTTP('fixture-key', 'fixture-secret', enabled=True)
    native._opener = CannedOpener(wire)
    native._reader._opener = CannedOpener(wire)
    return native


if __name__ == '__main__':
    unittest.main()
