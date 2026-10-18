import os
import tempfile
import unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from event_core import Store
from webapp import create_app

PASSWORD='fixture-password-at-least-20-characters'


class LoginTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.now=datetime(2026,10,18,tzinfo=timezone.utc)
        self.store=Store(self.tmp.name+'/db',lambda:self.now)
        self.env=patch.dict(os.environ,{'DASHBOARD_PASSWORD':PASSWORD,'FLASK_SECRET_KEY':'fixture-secret-signing-key-at-least-32'})
        self.env.start();self.app=create_app(self.store);self.client=self.app.test_client()

    def tearDown(self):self.env.stop();self.tmp.cleanup()

    def get(self,path='/login'):
        return self.client.get(path,base_url='https://localhost')

    def post(self,path,data):
        return self.client.post(path,data=data,base_url='https://localhost')

    def csrf(self):
        with self.client.session_transaction(base_url='https://localhost') as s:return s['csrf']

    def sign_in(self):
        self.get();return self.post('/login',{'csrf':self.csrf(),'password':PASSWORD})

    def test_login_is_public_but_dashboard_is_protected(self):
        response=self.get('/')
        self.assertEqual(response.status_code,302)
        self.assertEqual(response.headers['Location'],'/login')
        page=self.get()
        self.assertEqual(page.status_code,200)
        self.assertIn(b'type="password"',page.data)
        self.assertNotIn('WWW-Authenticate',page.headers)
        self.assertEqual(self.get('/health').status_code,200)

    def test_login_requires_csrf_and_does_not_echo_password(self):
        self.assertEqual(self.post('/login',{'password':PASSWORD}).status_code,403)
        self.get()
        wrong=self.post('/login',{'csrf':self.csrf(),'password':'wrong-secret'})
        self.assertEqual(wrong.status_code,401)
        self.assertNotIn(b'wrong-secret',wrong.data)
        self.assertEqual(self.get('/').status_code,302)

    def test_success_rotates_csrf_and_sets_secure_cookie(self):
        self.get();old=self.csrf()
        response=self.post('/login',{'csrf':old,'password':PASSWORD})
        self.assertEqual(response.status_code,302)
        cookie=response.headers['Set-Cookie']
        for protection in ('Secure','HttpOnly','SameSite=Strict'):self.assertIn(protection,cookie)
        self.assertNotEqual(old,self.csrf())
        self.assertEqual(self.get('/').status_code,200)
        self.assertEqual(self.post('/collect',{'csrf':old}).status_code,403)

    def test_session_expires_without_sliding_extension(self):
        self.sign_in();self.now+=timedelta(hours=3)
        self.assertEqual(self.get('/').status_code,200)
        self.now+=timedelta(hours=1,seconds=1)
        self.assertEqual(self.get('/').status_code,302)

    def test_password_rotation_revokes_existing_session(self):
        self.sign_in()
        with patch.dict(os.environ,{'DASHBOARD_PASSWORD':'new-fixture-password-long-enough'}):
            self.assertEqual(self.get('/').status_code,302)

    def test_logout_revokes_copied_session_token(self):
        self.sign_in()
        with self.client.session_transaction(base_url='https://localhost') as s:
            copied=dict(s)
        self.assertEqual(self.post('/logout',{'csrf':self.csrf()}).status_code,302)
        with self.client.session_transaction(base_url='https://localhost') as s:s.update(copied)
        self.assertEqual(self.get('/').status_code,302)

    def test_persistent_global_failure_limit_and_recovery(self):
        self.get();token=self.csrf()
        for _ in range(20):
            self.assertEqual(self.post('/login',{'csrf':token,'password':'wrong'}).status_code,401)
        # A new app/client cannot reset the failure counter.
        self.client=create_app(self.store).test_client();self.get()
        blocked=self.post('/login',{'csrf':self.csrf(),'password':PASSWORD})
        self.assertEqual(blocked.status_code,429)
        self.assertEqual(blocked.headers['Retry-After'],'900')
        self.now+=timedelta(minutes=16)
        self.assertEqual(self.post('/login',{'csrf':self.csrf(),'password':PASSWORD}).status_code,302)

    def test_credentials_not_persisted_in_session_or_attempt_database(self):
        self.sign_in()
        with self.client.session_transaction(base_url='https://localhost') as s:
            self.assertNotIn(PASSWORD,str(dict(s)))
            token=s['login_token']
        with self.store.db() as c:
            saved=str([tuple(r) for r in c.execute('SELECT * FROM dashboard_sessions')])
            self.assertNotIn(token,saved)
            self.assertNotIn(PASSWORD,saved)
            attempts=str([tuple(r) for r in c.execute('SELECT * FROM dashboard_login_attempts')])
            self.assertNotIn(PASSWORD,attempts)

    def test_login_has_no_open_redirect(self):
        self.get()
        response=self.post('/login?next=https://example.org',{'csrf':self.csrf(),'password':PASSWORD})
        self.assertEqual(response.headers['Location'],'/')

    def test_modified_cookie_cannot_authenticate(self):
        self.sign_in()
        cookie=self.client.get_cookie('session',domain='localhost')
        self.client.set_cookie('session',cookie.value+'tampered',domain='localhost')
        self.assertEqual(self.get('/').status_code,302)
