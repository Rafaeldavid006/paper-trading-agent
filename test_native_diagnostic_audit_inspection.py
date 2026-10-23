"""Disposable synthetic states only. No sockets, credentials or provider calls."""
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import Mock, patch

import test_readonly_runtime_worker as base
from alpaca_schema import timestamp
import native_schema_diagnostic as diagnostic
import native_diagnostic_audit_inspection as inspect
from native_diagnostic_audit_supervisor import DiagnosticAuditSupervisor


class AuditInspectionTests(unittest.TestCase):
    setUp = base.RuntimeWorkerTests.setUp

    def consumed(self):
        base.run_phase('initialize', **self.kw)
        original = self.wire.call
        def stale(method, path, body=None):
            status, data = original(method, path, body)
            if '/quotes/latest?' in path:
                for value in data['quotes'].values():
                    value['t'] = (self.now - timedelta(seconds=11)).isoformat()
            return status, data
        self.wire.call = stale
        with self.assertRaises(Exception):
            base.run_phase('broker', **self.kw)
        self.now += timedelta(seconds=65)
        def fail_positions(method, path, body=None):
            if path == '/v2/positions':
                self.wire.calls.append((method, path, body))
                return 503, None
            return original(method, path, body)
        self.wire.call = fail_positions
        result = diagnostic.run_once(enabled=True, paths=self.paths, test_only=True,
            environ=self.env, now_ns=self.kw['now_ns'], transport_factory=lambda *a, **k: self.wire)
        self.assertEqual((result['reason'], result['requests'], result['prior_requests'],
                          result['prior_orders']), ('collection_refused', 4, 6, 2))
        return result

    def inspect(self):
        return inspect.run_once(enabled=True, paths=self.paths, test_only=True)

    def state(self):
        return {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.iterdir() if p.is_file()}

    def test_exact_correlated_audit_no_calls_or_state_writes(self):
        self.consumed()
        before, calls = self.state(), list(self.wire.calls)
        with (patch('native_observation_collector.NativeObservationCollector.collect', side_effect=AssertionError('no collector')),
             patch('native_paper_execution.NativePaperHTTP.__init__', side_effect=AssertionError('no credentials'))):
            result = self.inspect()
        self.assertEqual(result['audit_code'], 'provider_http_error')
        self.assertEqual(result['prior_owned_count'], 0)
        self.assertEqual(result['reason'], 'existing_audit_observed')
        self.assertEqual(result['provider_requests'], 0)
        self.assertFalse(result['state_mutated'])
        self.assertEqual(before, self.state())
        self.assertEqual(calls, self.wire.calls)
        self.assertNotIn(self.paths['anchor'], json.dumps(result))
        self.assertNotIn(self.wire.account['id'], json.dumps(result))

    def test_repeat_only_repeats_nonmutating_local_read(self):
        self.consumed(); before = self.state()
        self.assertEqual(self.inspect(), self.inspect())
        self.assertEqual(before, self.state())

    def test_disabled_touches_nothing(self):
        with (patch.object(inspect, '_frozen_code', side_effect=AssertionError('no FS')),
             patch.object(inspect, 'RuntimeStateBundle', side_effect=AssertionError('no DB'))):
            self.assertEqual(inspect.run_once()['reason'], 'inspection_disabled')

    def test_missing_failure_audit_refuses_instead_of_guessing(self):
        self.consumed()
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute('DELETE FROM native_read_audit WHERE rowid=(SELECT MAX(rowid) FROM native_read_audit)')
        before = self.state()
        with self.assertRaises(inspect.InspectionRefused): self.inspect()
        self.assertEqual(before, self.state())

    def test_duplicate_post_claim_audit_refuses(self):
        self.consumed()
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute('INSERT INTO native_read_audit SELECT * FROM native_read_audit ORDER BY rowid DESC LIMIT 1')
        with self.assertRaises(inspect.InspectionRefused): self.inspect()

    def test_unknown_private_audit_text_is_not_emitted(self):
        self.consumed()
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute('UPDATE native_read_audit SET status=? WHERE rowid=(SELECT MAX(rowid) FROM native_read_audit)',
                       ('private secret account body',))
        result = self.inspect()
        self.assertEqual(result['audit_code'], 'unclassified')
        self.assertNotIn('private', json.dumps(result))

    def test_mismatched_count_digest_window_or_prior_refuses(self):
        self.consumed()
        for update in ('attempts=5', 'owned_count=1', 'order_count=1', "digest='private'", 'at_ns=at_ns+16000000000'):
            with self.subTest(update=update):
                path = Path(self.paths['guard']); raw = path.read_bytes()
                with sqlite3.connect(path) as db:
                    db.execute('UPDATE native_read_audit SET ' + update + ' WHERE rowid=(SELECT MAX(rowid) FROM native_read_audit)')
                with self.assertRaises(inspect.InspectionRefused): self.inspect()
                path.write_bytes(raw)

    def test_claim_wrong_generation_hash_status_or_count_refuses(self):
        self.consumed()
        for update in ("generation='other'", "code_hash='other'", "status='claimed'", 'requests=5'):
            with self.subTest(update=update):
                path = Path(self.paths['anchor']); raw = path.read_bytes()
                with sqlite3.connect(path) as db: db.execute('UPDATE native_schema_diagnostic_claim SET ' + update)
                with self.assertRaises(inspect.InspectionRefused): self.inspect()
                path.write_bytes(raw)

    def test_clean_runner_and_missing_inventory_refuse(self):
        self.consumed()
        with sqlite3.connect(self.paths['runner']) as db: db.execute('UPDATE runner_control SET dirty=0')
        with self.assertRaises(inspect.InspectionRefused): self.inspect()
        Path(self.paths['guard']).unlink()
        with self.assertRaises(Exception): self.inspect()
        self.assertFalse(Path(self.paths['guard']).exists())

    def test_production_rejects_path_override(self):
        with self.assertRaises(inspect.InspectionRefused): inspect.run_once(enabled=True, paths=self.paths)

    def test_main_redacts_errors_and_rejects_alternate_commands(self):
        with (patch.object(inspect, 'run_once', side_effect=ValueError('private path secret')),
             patch('builtins.print') as emit):
            self.assertEqual(inspect.main(['--inspect-existing-audit']), 1)
        self.assertNotIn('private', emit.call_args.args[0])
        self.assertNotIn('secret', emit.call_args.args[0])
        with patch.object(inspect, 'run_once') as run, patch('builtins.print'):
            self.assertEqual(inspect.main(['--once']), 1)
            run.assert_not_called()


class AuditSupervisorTests(unittest.TestCase):
    def test_disabled_does_not_spawn_and_enabled_starts_once_with_watchdog(self):
        child = Mock(); child.poll.return_value = None; child.wait.return_value = -9
        spawn = Mock(return_value=child); timer = Mock()
        DiagnosticAuditSupervisor(popen=spawn).start(); spawn.assert_not_called()
        supervisor = DiagnosticAuditSupervisor(enabled=True, popen=spawn,
            timer_factory=lambda seconds, callback: timer, monotonic=lambda: 1.0, emit=Mock())
        supervisor.start(); supervisor.start()
        self.assertEqual(spawn.call_count, 1)
        self.assertEqual(Path(spawn.call_args.args[0][1]).name, 'native_restart_preflight.py')
        self.assertEqual(spawn.call_args.args[0][2], '--inspect-restart-preflight')
        self.assertFalse(spawn.call_args.kwargs['shell'])
        supervisor._expire(); child.kill.assert_called_once()
        supervisor.start(); self.assertEqual(spawn.call_count, 1)

    def test_startup_launches_only_new_local_inspector_not_old_workers(self):
        import serve
        self.assertIs(serve.INSPECT_EXISTING_AUDIT_ONLY, True)
        web = Mock(); web.poll.return_value = None
        stop = Mock(); stop.wait.side_effect = [False, True]; stop.is_set.return_value = False
        with (patch.object(serve, 'validate_environment', return_value=('/tmp/events.db','8000')),
              patch.dict(serve.os.environ, {'ENABLE_AUTO_RESEARCH':'true'}),
              patch.object(serve.subprocess, 'Popen', return_value=web) as spawn,
              patch.object(serve.threading, 'Event', return_value=stop), patch.object(serve.signal, 'signal'),
              patch.object(serve, 'NativeProbeSupervisor') as probe,
              patch.object(serve, 'ReadonlyRuntimeSupervisor') as runtime,
              patch.object(serve, 'SchemaDiagnosticSupervisor') as diagnostic,
              patch.object(serve, 'DiagnosticAuditSupervisor') as audit):
            self.assertEqual(serve.main(), 0)
            self.assertEqual(spawn.call_count, 1)
            self.assertEqual(spawn.call_args.args[0][1:3], ['-m', 'gunicorn'])
            for old in (probe, runtime, diagnostic):
                old.assert_called_once_with(enabled=False)
                old.return_value.start.assert_not_called()
            audit.assert_called_once_with(enabled=True)
            audit.return_value.start.assert_called_once()
            audit.return_value.close.assert_called_once()

    def test_inspection_startup_does_not_read_unused_provider_credentials(self):
        import serve
        class Env(dict):
            def get(self, key, default=None):
                if key in ('ALPACA_API_KEY', 'ALPACA_SECRET_KEY', 'OPENAI_API_KEY', 'OPENAI_MODEL'):
                    raise AssertionError('unused provider credential read')
                return super().get(key, default)
        env = Env(DASHBOARD_PASSWORD='x'*20, FLASK_SECRET_KEY='x'*32,
                  EVENT_DB='/tmp/events.db', ENABLE_AUTO_RESEARCH='true')
        self.assertEqual(serve.validate_environment(env), ('/tmp/events.db','8000'))


if __name__ == '__main__': unittest.main()

class InspectionWebEntryTests(unittest.TestCase):
    def test_actual_gunicorn_import_and_health_never_probe_or_change_existing_state(self):
        import os, sys, tempfile, socket
        from gunicorn.util import import_app
        import paper_readonly
        from webapp import create_app
        with tempfile.TemporaryDirectory() as root:
            db = str(Path(root)/'events.sqlite')
            with patch.dict(os.environ, {'EVENT_DB':db, 'FLASK_SECRET_KEY':'x'*32,
                                          'DASHBOARD_PASSWORD':'x'*20}, clear=True):
                create_app()  # Synthetic existing deployed UI schema, before snapshot.
                before = Path(db).read_bytes()
                real_get = os.environ.get
                def no_provider(key, default=None):
                    if key in ('ALPACA_API_KEY','ALPACA_SECRET_KEY','OPENAI_API_KEY','OPENAI_MODEL'):
                        raise AssertionError('provider credential read')
                    return real_get(key,default)
                sys.modules.pop('inspection_wsgi', None)
                with (patch.object(os.environ, 'get', side_effect=no_provider),
                      patch.object(socket, 'socket', side_effect=AssertionError('network')),
                      patch.object(socket, 'create_connection', side_effect=AssertionError('network')),
                      patch.object(socket, 'getaddrinfo', side_effect=AssertionError('DNS')),
                      patch.object(paper_readonly, 'build_readonly_connection', side_effect=AssertionError('legacy probe')),
                      patch.object(paper_readonly, 'ReadOnlyPaperConnection', side_effect=AssertionError('legacy state'))):
                    app = import_app('inspection_wsgi:app')
                    client = app.test_client()
                    response = client.get('/health')
                    self.assertEqual(response.status_code,200)
                    self.assertEqual(response.json['mode'],'inspection_only')
                    self.assertIs(response.json['account_verified'],False)
                    self.assertIs(response.json['background_research_paused'],True)
                    self.assertEqual(client.head('/health').status_code,200)
                    self.assertEqual(client.head('/').status_code,401)
                self.assertEqual(before,Path(db).read_bytes())
                self.assertEqual([p.name for p in Path(root).iterdir()],['events.sqlite'])
                sys.modules.pop('inspection_wsgi', None)

    def test_actual_web_import_missing_ui_schema_refuses_without_mutation(self):
        import os, sys, tempfile
        from gunicorn.util import import_app
        with tempfile.TemporaryDirectory() as root:
            db = str(Path(root)/'events.sqlite')
            with sqlite3.connect(db) as c: c.execute('CREATE TABLE unrelated(a)')
            before = Path(db).read_bytes()
            with patch.dict(os.environ, {'EVENT_DB':db,'FLASK_SECRET_KEY':'x'*32},clear=True):
                sys.modules.pop('inspection_wsgi',None)
                with self.assertRaises(Exception): import_app('inspection_wsgi:app')
                self.assertEqual(before,Path(db).read_bytes())
                self.assertEqual([p.name for p in Path(root).iterdir()],['events.sqlite'])
                sys.modules.pop('inspection_wsgi',None)
