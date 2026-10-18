import copy,os,tempfile,unittest,json
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from event_core import Store
from relationships import Relationships
from ai_research import validate_review,analyze
from demo_relationships import build_demo

class GraphTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.t=datetime(2026,10,1,tzinfo=timezone.utc)
        self.s=Store(self.tmp.name+'/db',lambda:self.t);self.g=Relationships(self.s)
        self.e=self.g.evidence(url='https://example.org/source',title='Fixture',published_at=self.t-timedelta(days=1),excerpt='A buys from B. Synthetic test fixture, not a real market fact.')
        self.g.edge(origin='A',target='B',relation='buys_from',evidence_id=self.e,status='reported')
    def tearDown(self):self.tmp.cleanup()
    def graph(self):return self.g.investigate(entity='A',event_text='Unverified hypothetical event')
    def review(self):return {'summary_he':'test','scenarios':[{'target':'B','conditional_effect':'conditional','evidence_ids':[self.e],'missing_evidence':['materiality'],'strongest_counterargument':'not incremental','invalidation_condition':'no contract'}],'research_tasks':['verify event'],'status':'research_only'}
    def test_paths_never_authorize_orders(self):
        r=self.graph();self.assertEqual(r['order_permission'],'none');self.assertFalse(r['event_verified']);self.assertEqual(r['branches'][0]['target'],'B')
    def test_cycle_no_infinite_paths(self):
        self.g.edge(origin='B',target='A',relation='buys_from',evidence_id=self.e)
        self.assertEqual(len(self.graph()['branches']),1)
    def test_post_cutoff_relationship_hidden(self):
        before=self.t;self.t+=timedelta(days=1)
        self.g.edge(origin='B',target='C',relation='buys_from',evidence_id=self.e)
        r=self.g.investigate(entity='A',event_text='test',as_of=before)
        self.assertNotIn('C',[x['target'] for x in r['branches']])
    def test_hypothesis_not_reported(self):
        self.g.edge(origin='B',target='C',relation='buys_from',evidence_id=self.e)
        r=self.graph();self.assertEqual(next(x for x in r['branches'] if x['target']=='C')['status'],'unverified_relationship_hypothesis')
    def test_evidence_required(self):
        with self.assertRaises(ValueError):self.g.edge(origin='A',target='D',relation='buys_from',evidence_id='invented')
    def test_synthetic_excluded(self):
        e=self.g.evidence(url='https://example.org/demo',title='demo',published_at=self.t,excerpt='fiction',synthetic=True)
        self.g.edge(origin='A',target='Z',relation='buys_from',evidence_id=e)
        self.assertNotIn('Z',[x['target'] for x in self.graph()['branches']])
    def test_hallucinated_citation_rejected(self):
        r=self.review();r['scenarios'][0]['evidence_ids']=['fabricated']
        with self.assertRaises(ValueError):validate_review(r,self.graph())
    def test_unrecognized_target_rejected(self):
        r=self.review();r['scenarios'][0]['target']='invented'
        with self.assertRaises(ValueError):validate_review(r,self.graph())
    def test_order_authorization_rejected(self):
        r=self.review();r['status']='buy'
        with self.assertRaises(ValueError):validate_review(r,self.graph())
    def test_missing_evidence_disclosure_required(self):
        r=self.review();r['scenarios'][0]['missing_evidence']=[]
        with self.assertRaises(ValueError):validate_review(r,self.graph())
    def test_ai_mock_response(self):
        response={'status':'completed','output':[{'content':[{'type':'output_text','text':json.dumps(self.review())}]}]}
        with patch.dict(os.environ,{'OPENAI_API_KEY':'fake','OPENAI_MODEL':'test'}):self.assertEqual(analyze(self.graph(),transport=lambda _:response)['status'],'research_only')
    def test_demo_no_live_analysis(self):
        with self.assertRaises(ValueError):analyze(build_demo())
    def test_expired_relationship_hidden(self):
        self.g.edge(origin='B',target='C',relation='buys_from',evidence_id=self.e,valid_until=self.t+timedelta(hours=1))
        self.t+=timedelta(hours=2);self.assertNotIn('C',[x['target'] for x in self.graph()['branches']])
