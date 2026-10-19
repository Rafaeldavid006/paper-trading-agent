"""Offline renewal integration, status-only navigation and supervision checks."""
import json
import os
import sqlite3
import unittest
from unittest.mock import Mock, patch
from decimal import Decimal
from event_core import Store
from webapp import create_app
from paper_renewal import RenewalExperiment
from paper_renewal_worker import run
from serve import record_experiment_failure
import test_paper_experiment as fixtures

class RenewalControlsTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.OneShotExperimentTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        original=self.f.quotes.latest_quote;count=0
        def moved():
            nonlocal count
            count+=1;q=original()
            if count>1:q.ask=Decimal('250')
            return q
        with patch.object(self.f.quotes,'latest_quote',side_effect=moved):self.f.experiment.tick()
        self.renewal=RenewalExperiment(self.f.database,self.f.transport,self.f.quotes,now=lambda:self.f.now)
        self.f.transport.calls.clear()

    def test_renewal_worker_emits_separate_mode_and_exits_terminal_without_repeat(self):
        factory=Mock();factory.return_value.tick.return_value=dict(phase='completed',reason='roundtrip_filled',next_open=None)
        logs=[]
        run(database=self.f.database,environ={'ALPACA_API_KEY':'fixture-key','ALPACA_SECRET_KEY':'fixture-secret'},factory=factory,broker_factory=Mock(),quote_factory=Mock(),emit=lambda text,**kw:logs.append(json.loads(text)))
        self.assertEqual(1,factory.return_value.tick.call_count)
        self.assertEqual('paper_renewal',logs[0]['mode'])
        self.assertNotIn('fixture',json.dumps(logs))

    def test_supervisor_marks_new_generation_preserves_original_and_all_intents(self):
        with self.f.experiment.db() as c:
            old=tuple(c.execute('SELECT * FROM paper_experiment').fetchone())
            intents=[tuple(r) for r in c.execute('SELECT * FROM intents')]
        logs=[]
        record_experiment_failure(-9,database=self.f.database,emit=lambda text,**kw:logs.append(json.loads(text)))
        self.assertEqual('manual_attention',self.renewal.status()['phase'])
        self.assertEqual('worker_failed',self.renewal.status()['reason'])
        with self.f.experiment.db() as c:
            self.assertEqual(old,tuple(c.execute('SELECT * FROM paper_experiment').fetchone()))
            self.assertEqual(intents,[tuple(r) for r in c.execute('SELECT * FROM intents')])
        self.assertTrue(logs[0]['attention_recorded'])
        self.assertEqual([],self.f.transport.calls)

    def test_authenticated_navigation_only_reads_local_status_and_stop_latches(self):
        with patch.dict(os.environ,{'DASHBOARD_PASSWORD':'fixture-password-at-least-20','FLASK_SECRET_KEY':'fixture-signing-secret-at-least-32-characters'}):
            app=create_app(Store(self.f.tmp.name+'/events.sqlite'),paper_adapter=self.f.connection)
            client=app.test_client();base='https://localhost'
            self.assertEqual(302,client.get('/paper',base_url=base).status_code)
            client.get('/login',base_url=base)
            with client.session_transaction(base_url=base) as session:csrf=session['csrf']
            client.post('/login',base_url=base,data={'csrf':csrf,'password':'fixture-password-at-least-20'})
            with client.session_transaction(base_url=base) as session:csrf=session['csrf']
            for _ in range(3):
                page=client.get('/paper',base_url=base).get_data(as_text=True)
                self.assertIn('ניסיון נוסף יחיד שאושר',page)
                self.assertIn('תיעוד הניסיון המקורי',page)
                self.assertNotIn('action="/paper/enable"',page)
            self.assertEqual([],self.f.transport.calls)
            self.assertEqual(403,client.post('/paper/stop',base_url=base,data={'csrf':'wrong'}).status_code)
            for _ in range(2):self.assertEqual(303,client.post('/paper/stop',base_url=base,data={'csrf':csrf}).status_code)
            self.assertEqual('stopped',self.renewal.tick()['phase'])
            self.assertEqual([],self.f.calls('POST')+self.f.calls('DELETE'))

if __name__=='__main__':unittest.main()
