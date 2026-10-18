import json
import os
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from event_core import Store
from discovery import Discovery
from research_worker import cycle
from triage import screen, record_screenings


def event(headline):
    return {'payload':json.dumps({'headline':headline})}


class ScreeningTests(unittest.TestCase):
    def test_opinion_does_not_consume_automatic_research(self):
        result=screen(event("Kevin O'Leary Says This Is What Sets Elon Musk Apart"))
        self.assertFalse(result['auto_eligible'])
        self.assertEqual(result['status'],'commentary')

    def test_concrete_announcement_keeps_priority_even_with_says(self):
        result=screen(event('Company says it agrees to acquire its competitor'))
        self.assertTrue(result['auto_eligible'])
        self.assertIn('merger',result['event_types'])
        self.assertFalse(result['event_verified'])

    def test_conditional_language_lowers_priority(self):
        definite=screen(event('Company announces investment in chip factory'))
        conditional=screen(event('Company reportedly announces investment in chip factory'))
        self.assertLess(conditional['priority'],definite['priority'])

    def test_unknown_and_non_english_remain_available_for_manual_review(self):
        for headline in ['Unclassified development', 'החברה הודיעה על חוזה חדש']:
            self.assertEqual(screen(event(headline))['status'],'unclassified')

    def test_no_unsafe_payload_failure(self):
        for payload in ['null','[]','bad',json.dumps({'headline':[]} )]:
            self.assertFalse(screen({'payload':payload})['auto_eligible'])


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.now=datetime(2026,10,18,tzinfo=timezone.utc)
        self.store=Store(self.tmp.name+'/db',lambda:self.now)
        self.env=patch.dict(os.environ,{'OPENAI_API_KEY':'fake','OPENAI_MODEL':'test',
            'ENABLE_AUTO_RESEARCH':'true','ENABLE_WEB_RESEARCH':'true','WATCHLIST':'AAA,BBB','AI_MAX_CALLS_PER_DAY':'5'})
        self.env.start()

    def tearDown(self):
        self.env.stop();self.tmp.cleanup()

    def add(self,id,headline,symbol='AAA',url=None,age=0):
        return self.store.ingest(source='fixture',source_id=id,symbol=symbol,
            published_at=self.now-timedelta(seconds=age),updated_at=self.now,
            url=url or 'https://example.org/'+id,payload={'headline':headline})[0]

    def collect(self,store,_):store.heartbeat('alpaca-news','ok')

    def response(self,_):
        return {'status':'completed','output':[
            {'type':'web_search_call','status':'completed','action':{}},
            {'type':'message','content':[{'type':'output_text','text':'Fixture report only',
                'annotations':[{'type':'url_citation','url':'https://example.org/report'}]}]}]}

    def test_older_material_event_outranks_new_opinion(self):
        expected=self.add('1','Fixture agrees to acquire a competitor',age=60)
        self.add('2','Analyst says stock could soar')
        result=cycle(self.store,price_transport=lambda *args:[],collector=self.collect,transport=self.response)
        self.assertEqual(result['event_version'],expected)
        with self.store.db() as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM event_screenings').fetchone()[0],2)

    def test_duplicate_article_across_symbols_uses_one_request(self):
        for symbol in ['AAA','BBB']:
            self.add('shared:'+symbol,'Fixture reports quarterly earnings',symbol,'https://example.org/shared')
        calls=[]
        def transport(payload):calls.append(1);return self.response(payload)
        cycle(self.store,price_transport=lambda *args:[],collector=self.collect,transport=transport)
        self.assertEqual(cycle(self.store,price_transport=lambda *args:[],collector=self.collect,transport=transport)['status'],'no_recent_pending_event')
        self.assertEqual(calls,[1])

    def test_skipped_event_is_audited_without_spend(self):
        self.add('1','Opinion: stocks to watch')
        result=cycle(self.store,price_transport=lambda *args:[],collector=self.collect,transport=lambda _:self.fail('paid call'))
        self.assertEqual(result['status'],'no_priority_candidate')
        with self.store.db() as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM ai_attempts').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM event_screenings').fetchone()[0],1)

    def test_old_event_never_reclassified_as_fresh(self):
        self.add('1','Fixture reports quarterly earnings',age=901)
        self.assertEqual(cycle(self.store,price_transport=lambda *args:[],collector=self.collect,transport=lambda _:self.fail('paid call'))['status'],'no_recent_pending_event')
