import copy
import os
import tempfile
import unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from discovery import Discovery,extract_report,safe_url,failure_code
from event_core import Store
from research_worker import cycle
from pipeline import Pipeline


def response():
    return {'status':'completed','usage':{'total_tokens':100},'output':[
        {'type':'web_search_call','status':'completed','action':{'type':'search',
            'sources':[{'url':'https://example.org/report','title':'Fixture source'}]}},
        {'type':'message','content':[{'type':'output_text','text':'Synthetic report for testing only.',
            'annotations':[{'type':'url_citation','url':'https://example.org/report','title':'Fixture source'}]}]}]}


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.now=datetime(2026,10,18,tzinfo=timezone.utc)
        self.s=Store(self.tmp.name+'/db',lambda:self.now);self.d=Discovery(self.s)
        self.env=patch.dict(os.environ,{'OPENAI_API_KEY':'fake','OPENAI_MODEL':'test',
            'ENABLE_AUTO_RESEARCH':'true','ENABLE_WEB_RESEARCH':'true','WATCHLIST':'AAA','AI_MAX_CALLS_PER_DAY':'5'})
        self.env.start();self.v=self.ingest()

    def tearDown(self):self.env.stop();self.tmp.cleanup()

    def ingest(self):return self.s.ingest(source='fixture',source_id='1',symbol='AAA',
        published_at=self.now,updated_at=self.now,url='https://example.org/event',payload={'headline':'test'})[0]

    def test_failure_codes_do_not_expose_provider_secrets(self):
        from urllib.error import HTTPError, URLError
        self.assertEqual(failure_code(HTTPError('https://secret',429,'secret',{},None)), 'provider_http_429')
        self.assertEqual(failure_code(URLError('secret')), 'provider_connection_failed')
        self.assertEqual(failure_code(TimeoutError('secret')), 'provider_timeout')
        self.assertEqual(failure_code(ValueError('citation absent from searched sources')), 'citation_inventory_mismatch')
        self.assertEqual(failure_code(RuntimeError('secret')), 'unexpected_research_error')

    def test_real_tool_inventory_required(self):
        r=response();r['output']=r['output'][1:]
        with self.assertRaises(ValueError):extract_report(r)

    def test_uncited_report_rejected(self):
        r=response();r['output'][1]['content'][0]['annotations']=[]
        with self.assertRaises(ValueError):extract_report(r)

    def test_fabricated_citation_rejected(self):
        r=response();r['output'][1]['content'][0]['annotations'][0]['url']='https://invented.org/'
        with self.assertRaises(ValueError):extract_report(r)

    def test_incomplete_response_rejected(self):
        r=response();r['status']='incomplete'
        with self.assertRaises(ValueError):extract_report(r)

    def test_non_https_and_credentials_rejected(self):
        for url in ['javascript:alert(1)','http://example.org','https://user:secret@example.org','https://[']:
            self.assertFalse(safe_url(url))

    def test_research_saved_once_across_restart(self):
        calls=[]
        def transport(p):calls.append(p);return response()
        first=self.d.run(self.v,transport=transport)
        second=Discovery(self.s).run(self.v,transport=transport)
        self.assertEqual(len(calls),1);self.assertEqual(first,second)
        self.assertEqual(first['report']['order_permission'],'none')
        self.assertFalse(first['report']['event_verified'])
        self.assertEqual(calls[0]['tool_choice'],'required')
        self.assertEqual(calls[0]['max_tool_calls'],4)

    def test_failure_redacted_and_not_retried(self):
        calls=[]
        def fail(_):calls.append(1);raise RuntimeError('secret provider error')
        result=self.d.run(self.v,transport=fail);self.d.run(self.v,transport=fail)
        self.assertEqual(calls,[1]);self.assertEqual(result['state'],'failed')
        self.assertNotIn('secret',result['error'])

    def test_new_revision_supersedes_report(self):
        self.d.run(self.v,transport=lambda _:response())
        self.now+=timedelta(seconds=1);self.ingest()
        self.assertTrue(self.d.get(self.v)['superseded'])
        with self.assertRaises(ValueError):self.d.run(self.v,transport=lambda _:response())

    def test_missing_key_no_attempt_consumed(self):
        with patch.dict(os.environ,{'OPENAI_API_KEY':''}):
            with self.assertRaises(ValueError):self.d.run(self.v)
        with self.s.db() as c:self.assertEqual(c.execute('SELECT COUNT(*) FROM ai_attempts').fetchone()[0],0)

    def test_budget_shared_with_graph_pipeline(self):
        self.d.run(self.v,transport=lambda _:response(),max_daily_calls=1)
        p=Pipeline(self.s)
        eid=p.graph.evidence(url='https://example.org/edge',title='fixture',published_at=self.now,excerpt='fixture')
        p.graph.edge(origin='AAA',target='BBB',relation='sells_to',evidence_id=eid)
        run=p.prepare(self.v)
        with self.assertRaisesRegex(ValueError,'daily AI'):p.review(run['id'],max_daily_calls=1)

    def test_worker_collects_then_researches_without_graph(self):
        def collect(s,_):s.heartbeat('alpaca-news','ok');return 0
        result=cycle(self.s,collector=collect,transport=lambda _:response())
        self.assertEqual(result['status'],'completed')
        again=cycle(self.s,collector=collect,transport=lambda _:self.fail('repeat'))
        self.assertEqual(again['status'],'no_recent_pending_event')

    def test_worker_blocks_incomplete_coverage(self):
        def collect(s,_):s.heartbeat('alpaca-news','coverage_incomplete');return 0
        self.assertEqual(cycle(self.s,collector=collect,transport=lambda _:self.fail('called'))['status'],'blocked_source_coverage')

    def test_worker_skips_old_event(self):
        self.now+=timedelta(hours=1)
        def collect(s,_):s.heartbeat('alpaca-news','ok');return 0
        self.assertEqual(cycle(self.s,collector=collect)['status'],'no_recent_pending_event')

    def test_worker_disabled_by_default(self):
        with patch.dict(os.environ,{'ENABLE_AUTO_RESEARCH':'false'}):
            with self.assertRaises(ValueError):cycle(self.s,collector=lambda *_:self.fail('called'))

    def test_supervisor_requires_persistent_store_for_automatic_mode(self):
        from serve import validate_environment
        env=dict(os.environ,FLASK_SECRET_KEY='x'*32,DASHBOARD_PASSWORD='x'*20,
                 EVENT_DB=self.tmp.name+'/db',PERSISTENT_STORAGE_CONFIRMED='false',
                 ALPACA_API_KEY='fake',ALPACA_SECRET_KEY='fake')
        with self.assertRaisesRegex(ValueError,'persistent'):validate_environment(env)
        env['PERSISTENT_STORAGE_CONFIRMED']='true'
        self.assertEqual(validate_environment(env)[0],self.tmp.name+'/db')
