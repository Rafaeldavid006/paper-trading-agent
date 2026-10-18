import base64
import os
import tempfile
import unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from event_core import Store
from webapp import create_app


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.now=datetime(2026,10,17,20,tzinfo=timezone.utc)
        self.store=Store(self.tmp.name+'/db',lambda:self.now)
        self.env=patch.dict(os.environ,{'DASHBOARD_PASSWORD':'test-password-at-least-20-chars',
            'FLASK_SECRET_KEY':'test-session-key','ENABLE_AI_RESEARCH':'false','AI_MAX_CALLS_PER_DAY':'1'})
        self.env.start()
        self.calls=[]
        def collector(store,symbols):self.calls.append(symbols);return 0
        self.app=create_app(self.store,collector=collector)
        self.client=self.app.test_client()
        encoded=base64.b64encode(b'user:test-password-at-least-20-chars').decode()
        self.auth={'Authorization':'Basic '+encoded}
        self.version=self.store.ingest(source='fixture',source_id='1',symbol='AAA',
            published_at=self.now,payload={'headline':'<script>bad()</script>'},url='https://example.org/event')[0]

    def tearDown(self):self.env.stop();self.tmp.cleanup()

    def token(self):
        self.client.get('/',headers=self.auth,base_url='https://localhost')
        with self.client.session_transaction(base_url='https://localhost') as s:return s['csrf']

    def post(self,path,data):
        return self.client.post(path,data=data,headers=self.auth,base_url='https://localhost')

    def test_login_required(self):
        self.assertEqual(self.client.get('/').status_code,401)

    def test_dashboard_escapes_untrusted_news(self):
        r=self.client.get('/',headers=self.auth,base_url='https://localhost')
        self.assertEqual(r.status_code,200)
        self.assertIn(b'&lt;script&gt;',r.data)
        self.assertNotIn(b'<script>bad()',r.data)
        self.assertIn("default-src 'none'",r.headers['Content-Security-Policy'])

    def test_csrf_required_and_unicode_rejected(self):
        self.token()
        for token in ('','wrong','עברית'):
            self.assertEqual(self.post('/collect',{'csrf':token}).status_code,403)
        self.assertEqual(self.calls,[])

    def test_collection_requires_explicit_post(self):
        self.client.get('/',headers=self.auth)
        self.assertEqual(self.calls,[])
        self.assertEqual(self.post('/collect',{'csrf':self.token()}).status_code,302)
        self.assertEqual(len(self.calls),1)

    def test_prepare_and_show_without_ai(self):
        r=self.post('/prepare/'+self.version,{'csrf':self.token(),'event_types':'earnings'})
        self.assertEqual(r.status_code,302)
        shown=self.client.get(r.location,headers=self.auth,base_url='https://localhost')
        self.assertEqual(shown.status_code,200)
        self.assertIn('אין עדיין קשרים'.encode(),shown.data)

    def test_ai_disabled(self):
        self.assertEqual(self.post('/review/unknown',{'csrf':self.token()}).status_code,403)

    def test_discovery_page_and_disabled_post(self):
        r=self.client.get('/discovery/'+self.version,headers=self.auth,base_url='https://localhost')
        self.assertEqual(r.status_code,200)
        self.assertIn('החיפוש טרם הופעל'.encode(),r.data)
        self.assertEqual(self.post('/discovery/'+self.version,{'csrf':self.token()}).status_code,403)

    def test_discovery_post_displays_clickable_sources(self):
        from test_discovery import response
        with patch.dict(os.environ,{'ENABLE_WEB_RESEARCH':'true','OPENAI_API_KEY':'fake','OPENAI_MODEL':'test'}):
            app=create_app(self.store,discovery_transport=lambda _:response())
            client=app.test_client()
            client.get('/',headers=self.auth,base_url='https://localhost')
            with client.session_transaction(base_url='https://localhost') as s:token=s['csrf']
            result=client.post('/discovery/'+self.version,data={'csrf':token},headers=self.auth,base_url='https://localhost')
            self.assertEqual(result.status_code,302)
            page=client.get(result.location,headers=self.auth,base_url='https://localhost')
            self.assertEqual(page.status_code,200)
            self.assertIn(b'href="https://example.org/report"',page.data)

    def test_evidence_form_creates_new_packet(self):
        run=self.app.extensions['research_pipeline'].prepare(self.version)
        self.now+=timedelta(seconds=1)
        r=self.post('/evidence/'+run['id'],dict(csrf=self.token(),target='BBB',relation='sells_to',
            source_url='https://example.org/evidence',title='Fixture',published_at='2026-10-17T18:00:00Z',
            excerpt='Synthetic claim used in local test only.'))
        self.assertEqual(r.status_code,302)
        new=self.app.extensions['research_pipeline'].get(r.location.rsplit('/',1)[1])
        self.assertEqual(new['packet']['graph']['branches'][0]['target'],'BBB')
        self.assertFalse(new['packet']['graph']['event_verified'])

    def test_daily_limit_shared_across_runs_and_restarts(self):
        pipe=self.app.extensions['research_pipeline']
        eid=pipe.graph.evidence(url='https://example.org/e',title='Fixture',published_at=self.now,excerpt='fixture')
        pipe.graph.edge(origin='AAA',target='BBB',relation='sells_to',evidence_id=eid)
        first=pipe.prepare(self.version)
        def fail(_):raise RuntimeError('provider failure')
        self.assertEqual(pipe.review(first['id'],analyst=fail,max_daily_calls=1)['state'],'failed')
        self.now+=timedelta(seconds=1)
        second=pipe.prepare(self.version)
        from pipeline import Pipeline
        with self.assertRaisesRegex(ValueError,'daily AI'):
            Pipeline(self.store).review(second['id'],analyst=fail,max_daily_calls=1)
        self.assertEqual(pipe.get(second['id'])['state'],'prepared')
