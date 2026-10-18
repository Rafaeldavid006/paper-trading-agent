"""Control/UI tests: only injected MockPaper, no broker credentials or network."""
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from event_core import Store
from paper_adapter import PaperAdapter, Intent, SafetyError
from test_paper_adapter import MockPaper
from webapp import create_app


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = datetime(2026, 10, 19, 15, tzinfo=timezone.utc)
        self.transport = MockPaper(lambda: self.now)
        self.transport.account['multiplier'] = '4'
        self.adapter = self.make()

    def tearDown(self): self.tmp.cleanup()

    def make(self, **kwargs):
        args = dict(enabled=True, allowlist=['AAPL'], expected_account_id='fixture-account', now=lambda:self.now)
        args.update(kwargs)
        return PaperAdapter(self.tmp.name+'/paper.sqlite',self.transport,**args)

    def activate(self):
        status=self.adapter.check_connection()
        return self.adapter.enable(revision=status['revision'])

    def test_persistent_permission_initially_off_even_with_capability(self):
        self.assertFalse(self.adapter.control_status()['enabled'])
        with self.assertRaises(SafetyError): self.adapter.submit(Intent('one','AAPL','buy',1,'100'))
        self.assertFalse(any(c[0] == 'POST' for c in self.transport.calls))

    def test_margin_capability_allowed_but_no_borrowing(self):
        for multiplier in ('1','2','4'):
            self.transport.account['multiplier']=multiplier
            self.assertTrue(self.adapter.check_connection()['check_ok'])
        for field,value in [('cash','-1'),('equity','999'),('short_market_value','1')]:
            saved=dict(self.transport.account)
            self.transport.account['long_market_value']='1000'
            self.transport.account[field]=value
            self.assertFalse(self.adapter.check_connection()['check_ok'])
            self.transport.account=saved

    def test_buying_power_does_not_replace_cash(self):
        self.activate()
        self.transport.account.update(cash='99',buying_power='100000')
        with self.assertRaises(SafetyError): self.adapter.submit(Intent('one','AAPL','buy',1,'100'))
        self.assertFalse(any(c[0] == 'POST' for c in self.transport.calls))

    def test_enable_requires_recent_successful_check(self):
        with self.assertRaises(SafetyError): self.adapter.enable(revision=0)
        state=self.adapter.check_connection()
        self.now += timedelta(seconds=31)
        with self.assertRaises(SafetyError): self.adapter.enable(revision=state['revision'])
        self.assertFalse(self.adapter.control_status()['enabled'])

    def test_enable_rechecks_broker_and_failed_check_revokes(self):
        state=self.adapter.check_connection()
        self.transport.account['cash']='-1'
        with self.assertRaises(SafetyError): self.adapter.enable(revision=state['revision'])
        self.assertFalse(self.adapter.control_status()['enabled'])
        self.transport.account['cash']='20000'
        self.activate()
        self.transport.account['id']='changed'
        self.assertFalse(self.adapter.check_connection()['enabled'])
        self.assertFalse(self.adapter.control_status()['check_ok'])

    def test_stop_restart_and_explicit_resume(self):
        self.activate()
        self.adapter.stop()
        restarted=self.make()
        self.assertTrue(restarted.control_status()['stopped'])
        state=restarted.check_connection()
        self.assertTrue(state['stopped'])
        with self.assertRaises(SafetyError): restarted.enable(revision=state['revision'])
        self.assertTrue(restarted.enable(revision=state['revision'],resume=True)['enabled'])
        self.assertTrue(self.make().control_status()['enabled'])
        self.assertFalse(self.make(enabled=False).control_status()['enabled'])

    def test_stale_form_cannot_undo_newer_stop(self):
        state=self.adapter.check_connection()
        self.adapter.stop()
        with self.assertRaises(SafetyError): self.adapter.enable(revision=state['revision'],resume=True)
        self.assertTrue(self.adapter.control_status()['stopped'])

    def test_repeated_enable_does_not_repeat_operation(self):
        state=self.adapter.check_connection()
        self.adapter.enable(revision=state['revision'])
        calls=len(self.transport.calls)
        with self.assertRaises(SafetyError): self.adapter.enable(revision=state['revision'])
        self.assertEqual(calls,len(self.transport.calls))

    def test_config_change_cannot_inherit_permission(self):
        self.activate()
        different=self.make(allowlist=['MSFT'])
        self.assertFalse(different.control_status()['enabled'])
        self.assertFalse(different.check_connection()['enabled'])
        self.assertFalse(self.adapter.control_status()['enabled'])

    def test_no_enable_with_open_or_uncertain_orders(self):
        self.activate()
        self.adapter.submit(Intent('one','AAPL','buy',1,'100'))
        self.adapter.stop()
        self.assertFalse(self.adapter.check_connection()['check_ok'])
        self.transport.hidden=True
        self.assertFalse(self.adapter.check_connection()['check_ok'])
        state=self.adapter.control_status()
        self.assertEqual(1,state['unresolved'])
        with self.assertRaises(SafetyError): self.adapter.enable(revision=state['revision'],resume=True)

    def test_connection_probe_is_get_only_and_status_has_no_secrets(self):
        self.adapter.check_connection()
        status=self.adapter.control_status()
        self.assertTrue(all(c[0]=='GET' for c in self.transport.calls))
        self.assertNotIn('fixture-account',json.dumps(status))
        self.assertNotIn('binding',status)

    def test_observed_daily_loss_latches_even_with_open_orders(self):
        self.activate()
        self.adapter.submit(Intent('one','AAPL','buy',1,'100'))
        self.transport.account['equity']='19000'
        self.assertFalse(self.adapter.check_connection()['check_ok'])
        self.transport.account['equity']='20000'
        order=next(iter(self.transport.orders.values()))
        order['status']='canceled'
        self.assertFalse(self.adapter.check_connection()['check_ok'])
        state=self.adapter.control_status()
        with self.assertRaises(SafetyError): self.adapter.enable(revision=state['revision'])

    def test_interrupted_check_persistently_disables_before_provider_io(self):
        self.activate()
        with patch.object(self.transport,'call',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt): self.adapter.check_connection()
        state=self.make().control_status()
        self.assertFalse(state['enabled'])
        self.assertFalse(state['check_ok'])

    def test_interrupted_enable_cannot_leave_previous_permission(self):
        self.activate()
        state=self.adapter.control_status()
        with patch.object(self.transport,'call',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt): self.adapter.enable(revision=state['revision'])
        self.assertFalse(self.make().control_status()['enabled'])
        self.assertFalse(self.make().control_status()['check_ok'])

    def test_check_pauses_execution_and_success_never_restores_it(self):
        self.activate()
        self.assertFalse(self.adapter.check_connection()['enabled'])
        state=self.adapter.control_status()
        self.assertTrue(state['check_ok'])
        self.adapter.stop()
        self.assertFalse(self.adapter.check_connection()['enabled'])
        self.assertTrue(self.adapter.control_status()['stopped'])

    def test_superseded_check_cannot_overwrite_newer_control_action(self):
        import sqlite3
        original_connect=sqlite3.connect
        pending=[True]
        adapter=self.adapter
        class CommitHook(sqlite3.Connection):
            def commit(connection):
                super().commit()
                if pending[0]:
                    pending[0]=False
                    newer=adapter.check_connection()
                    adapter.enable(revision=newer['revision'])
        def connect(*args,**kwargs):
            return original_connect(*args,**kwargs,factory=CommitHook)
        with patch('paper_adapter.sqlite3.connect',side_effect=connect):
            with self.assertRaises(SafetyError): adapter.check_connection()
        self.assertTrue(adapter.control_status()['enabled'])
        # The newer explicit enable wins; the older probe never writes stale results.

    def test_stale_provider_clock_and_daily_stop(self):
        original=self.transport.call
        def stale(method,path,body=None):
            if path=='/v2/clock':return 200,dict(timestamp=(self.now-timedelta(seconds=11)).isoformat(),is_open=True)
            return original(method,path,body)
        self.transport.call=stale
        self.assertFalse(self.adapter.check_connection()['check_ok'])
        self.transport.call=original
        self.transport.account['equity']='19000'
        self.assertFalse(self.adapter.check_connection()['check_ok'])
        self.transport.account['equity']='20000'
        self.assertFalse(self.adapter.check_connection()['check_ok'])


class PaperUITests(unittest.TestCase):
    make = ControlTests.make
    # Exercise only the UI-specific methods below; adapter cases live above.
    def setUp(self):
        ControlTests.setUp(self)
        self.env=patch.dict(os.environ,{'DASHBOARD_PASSWORD':'fixture-password-at-least-20','FLASK_SECRET_KEY':'fixture-signing-secret-at-least-32-characters'})
        self.env.start()
        self.store=Store(self.tmp.name+'/events.sqlite',lambda:self.now)
        self.app=create_app(self.store,paper_adapter=self.adapter)
        self.app.testing=True
        self.client=self.app.test_client()

    def tearDown(self):
        self.env.stop()
        ControlTests.tearDown(self)

    def get(self,path='/paper'):
        return self.client.get(path,base_url='https://localhost')

    def csrf(self):
        with self.client.session_transaction(base_url='https://localhost') as s:return s.get('csrf','')

    def post(self,path,data=None):
        return self.client.post(path,data={'csrf':self.csrf(),**(data or {})},base_url='https://localhost')

    def login(self):
        self.get('/login')
        self.post('/login',{'password':'fixture-password-at-least-20'})

    def test_ui_auth_csrf_all_controls(self):
        self.assertEqual(302,self.get().status_code)
        for path in ('/paper/check','/paper/enable','/paper/stop'):
            self.assertEqual(401,self.post(path).status_code)
        self.login()
        for path in ('/paper/check','/paper/enable','/paper/stop'):
            self.assertEqual(403,self.post(path,{'csrf':'bad'}).status_code)
        self.assertEqual([],self.transport.calls)

    def test_ui_get_navigation_refresh_and_back_do_not_call_broker(self):
        self.login()
        for path in ('/paper','/','/paper','/paper'):
            self.assertEqual(200,self.get(path).status_code)
        for path in ('/paper/check','/paper/enable','/paper/stop'):
            self.assertEqual(405,self.get(path).status_code)
        self.assertEqual([],self.transport.calls)
        self.assertFalse(self.adapter.control_status()['enabled'])

    def test_ui_enable_stop_refresh_repeated_posts(self):
        self.login()
        self.assertEqual(303,self.post('/paper/check').status_code)
        revision=self.adapter.control_status()['revision']
        self.assertEqual(400,self.post('/paper/enable',{'revision':revision}).status_code)
        self.assertEqual(303,self.post('/paper/enable',{'revision':revision,'confirm':'yes'}).status_code)
        self.assertTrue(self.adapter.control_status()['enabled'])
        self.post('/paper/enable',{'revision':revision,'confirm':'yes'})
        self.post('/paper/stop');self.post('/paper/stop')
        self.get();self.get()
        self.assertTrue(self.adapter.control_status()['stopped'])
        self.post('/paper/enable',{'revision':revision,'confirm':'yes','resume':'yes'})
        self.assertFalse(self.adapter.control_status()['enabled'])
        self.assertTrue(all(c[0]=='GET' for c in self.transport.calls))
        self.assertIn('אינה סוגרת פוזיציות',self.get().get_data(as_text=True))

    def test_ui_expired_session_and_old_form(self):
        self.login()
        self.post('/paper/check')
        revision=self.adapter.control_status()['revision']
        self.now += timedelta(hours=5)
        self.assertEqual(401,self.post('/paper/enable',{'revision':revision,'confirm':'yes'}).status_code)
        self.assertFalse(self.adapter.control_status()['enabled'])
        self.login()
        self.post('/paper/enable',{'revision':revision,'confirm':'yes'})
        self.assertFalse(self.adapter.control_status()['enabled'])

    def test_ui_unconfigured_has_no_secret_fields(self):
        app=create_app(self.store)
        with app.test_client() as client:
            response=client.get('/paper',base_url='https://localhost',headers={'Authorization':'Basic '+__import__('base64').b64encode(b'user:fixture-password-at-least-20').decode()})
        text=response.get_data(as_text=True)
        self.assertEqual(200,response.status_code)
        self.assertIn('החיבור טרם הוגדר',text)
        self.assertNotIn('name="secret"',text)
        self.assertNotIn('name="key"',text)
        self.assertNotIn('fixture-account',text)

    def test_ui_exception_text_never_rendered_and_no_order_routes(self):
        self.login()
        with patch.object(self.adapter,'preflight',side_effect=RuntimeError('sensitive-fixture-value')):
            self.post('/paper/check')
        self.assertNotIn('sensitive-fixture-value',self.get().get_data(as_text=True))
        self.assertFalse(self.adapter.control_status()['enabled'])
        for path in ('/paper/orders','/paper/submit','/paper/cancel'):
            self.assertEqual(404,self.post(path).status_code)

    def test_ui_stop_failure_not_reported_as_success(self):
        self.login()
        with patch.object(self.adapter,'stop',side_effect=OSError('fixture')):
            self.post('/paper/stop')
        text=self.get().get_data(as_text=True)
        self.assertIn('לא ניתן לאשר שהעצירה נשמרה',text)
        self.assertNotIn('העצירה נשמרה. פקודות חדשות חסומות',text)

    def test_ui_security_headers_and_session_cookie(self):
        self.login()
        response=self.get()
        self.assertEqual('no-store',response.headers['Cache-Control'])
        self.assertEqual('DENY',response.headers['X-Frame-Options'])
        self.assertIn("form-action 'self'",response.headers['Content-Security-Policy'])
        self.assertTrue(self.app.config['SESSION_COOKIE_SECURE'])
        self.assertTrue(self.app.config['SESSION_COOKIE_HTTPONLY'])


if __name__=='__main__':unittest.main()
