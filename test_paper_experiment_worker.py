"""Offline worker supervision and authenticated status/stop tests."""
import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch
from event_core import Store
from paper_experiment_worker import run
from webapp import create_app
import test_paper_experiment as fixtures


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.db=self.tmp.name+'/paper.sqlite'
        with sqlite3.connect(self.db) as c:c.execute('CREATE TABLE paper_readonly(identity_first_seen REAL)')
        self.output=[]
        self.emit=lambda text,**kwargs:self.output.append(json.loads(text))
        self.env={'ALPACA_API_KEY':'fixture-key','ALPACA_SECRET_KEY':'fixture-secret'}

    def test_worker_waits_then_terminal_once_without_sensitive_logs(self):
        factory=Mock();broker=Mock();quotes=Mock();sleep=Mock()
        factory.return_value.tick.side_effect=[dict(phase='waiting',reason='waiting_regular_session',next_open='2026-10-19T13:30:00+00:00'),dict(phase='completed',reason='roundtrip_filled',next_open=None)]
        run(database=self.db,environ=self.env,factory=factory,broker_factory=broker,quote_factory=quotes,sleep=sleep,emit=self.emit)
        sleep.assert_called_once_with(60)
        self.assertEqual(2,factory.return_value.tick.call_count)
        self.assertEqual('completed',self.output[-1]['phase'])
        self.assertNotIn('fixture',json.dumps(self.output))
        for row in self.output:self.assertEqual({'mode','phase','reason','next_open'},set(row))

    def test_missing_keys_never_construct_transport(self):
        broker=Mock()
        run(database=self.db,environ={},broker_factory=broker,emit=self.emit)
        broker.assert_not_called()
        self.assertEqual('credentials_missing',self.output[-1]['reason'])

    def test_state_exception_is_sanitized_and_not_retried(self):
        factory=Mock();factory.return_value.tick.side_effect=ValueError('fixture-secret')
        run(database=self.db,environ=self.env,factory=factory,broker_factory=Mock(),quote_factory=Mock(),emit=self.emit)
        self.assertEqual(1,factory.return_value.tick.call_count)
        self.assertEqual('state_unavailable',self.output[-1]['reason'])
        self.assertNotIn('fixture-secret',json.dumps(self.output))

    def test_initialization_failure_does_not_retry_provider(self):
        factory=Mock(side_effect=ValueError('fixture-secret'))
        run(database=self.db,environ=self.env,factory=factory,broker_factory=Mock(),quote_factory=Mock(),emit=self.emit)
        self.assertEqual(1,factory.call_count)
        self.assertEqual('private_state_unavailable',self.output[-1]['reason'])

    def test_authenticated_status_navigation_and_stop_do_not_write_broker(self):
        fixture=fixtures.OneShotExperimentTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        with patch.dict(os.environ,{'DASHBOARD_PASSWORD':'fixture-password-at-least-20','FLASK_SECRET_KEY':'fixture-signing-secret-at-least-32-characters'}):
            app=create_app(Store(self.tmp.name+'/events.sqlite'),paper_adapter=fixture.connection)
            client=app.test_client();base='https://localhost'
            self.assertEqual(302,client.get('/paper',base_url=base).status_code)
            self.assertEqual(401,client.post('/paper/stop',base_url=base).status_code)
            client.get('/login',base_url=base)
            with client.session_transaction(base_url=base) as session:csrf=session['csrf']
            client.post('/login',base_url=base,data={'csrf':csrf,'password':'fixture-password-at-least-20'})
            with client.session_transaction(base_url=base) as session:csrf=session['csrf']
            for _ in range(3):
                page=client.get('/paper',base_url=base).get_data(as_text=True)
                self.assertIn('ניסוי חד־פעמי שאושר בדמו',page)
                self.assertIn('action="/paper/stop"',page)
                self.assertNotIn('action="/paper/enable"',page)
            self.assertEqual([],fixture.transport.calls)
            self.assertEqual(403,client.post('/paper/stop',base_url=base,data={'csrf':'wrong'}).status_code)
            for _ in range(2):self.assertEqual(303,client.post('/paper/stop',base_url=base,data={'csrf':csrf}).status_code)
            self.assertEqual('stopped',fixture.make().tick()['phase'])
            self.assertEqual([],fixture.calls('POST')+fixture.calls('DELETE'))


if __name__=='__main__':unittest.main()
