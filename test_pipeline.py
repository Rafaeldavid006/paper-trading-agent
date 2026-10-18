import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from event_core import Store
from pipeline import Pipeline


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = datetime(2026,10,1,tzinfo=timezone.utc)
        self.store = Store(self.tmp.name+'/events.db',lambda:self.now)
        self.pipe = Pipeline(self.store)
        self.version = self.ingest('initial')
        self.evidence = self.pipe.graph.evidence(url='https://example.org/fixture',title='Test fixture',
            published_at=self.now,excerpt='Fictional A sells to B, used only by unit tests.')
        self.pipe.graph.edge(origin='A',target='B',relation='sells_to',evidence_id=self.evidence)

    def tearDown(self):
        self.tmp.cleanup()

    def ingest(self, title):
        return self.store.ingest(source='fixture',source_id='1',symbol='A',published_at=self.now,
            updated_at=self.now,payload={'headline':title},url='https://example.org/event')[0]

    def result(self, graph):
        return dict(summary_he='test',scenarios=[dict(target='B',conditional_effect='conditional',
            evidence_ids=[self.evidence],missing_evidence=['materiality'],
            strongest_counterargument='offsetting costs',invalidation_condition='no exposure')],
            research_tasks=['verify event'],status='research_only')

    def test_end_to_end_persisted_review_never_authorizes_order(self):
        run=self.pipe.prepare(self.version,event_types=['earnings','financing'])
        result=self.pipe.review(run['id'],analyst=self.result)
        self.assertEqual(result['state'],'reviewed')
        self.assertEqual(Pipeline(self.store).get(run['id'])['review'],result['review'])
        self.assertEqual(result['order_permission'],'none')
        self.assertFalse(result['packet']['graph']['event_verified'])

    def test_same_run_calls_analyst_only_once(self):
        run=self.pipe.prepare(self.version);calls=[]
        def analyst(g):
            calls.append(1)
            return self.result(g)
        self.pipe.review(run['id'],analyst=analyst)
        self.pipe.review(run['id'],analyst=analyst)
        self.assertEqual(calls,[1])

    def test_failure_is_redacted_and_not_retried(self):
        run=self.pipe.prepare(self.version);calls=[]
        def fail(g):
            calls.append(1)
            raise RuntimeError('sensitive provider response')
        result=self.pipe.review(run['id'],analyst=fail)
        self.pipe.review(run['id'],analyst=fail)
        self.assertEqual(calls,[1])
        self.assertEqual(result['state'],'failed')
        self.assertNotIn('sensitive',result['error'])

    def test_invalid_model_citation_fails_closed(self):
        run=self.pipe.prepare(self.version)
        def wrong(g):
            result=self.result(g);result['scenarios'][0]['evidence_ids']=['invented']
            return result
        self.assertEqual(self.pipe.review(run['id'],analyst=wrong)['state'],'failed')

    def test_new_revision_marks_saved_review_superseded(self):
        run=self.pipe.prepare(self.version)
        self.pipe.review(run['id'],analyst=self.result)
        self.now+=timedelta(seconds=1);self.ingest('correction')
        self.assertTrue(self.pipe.get(run['id'])['superseded'])
        with self.assertRaises(ValueError):self.pipe.review(run['id'],analyst=self.result)
        with self.assertRaises(ValueError):self.pipe.prepare(self.version)

    def test_no_edges_no_paid_call(self):
        with self.store.db() as c:c.execute('DELETE FROM research_edges')
        run=self.pipe.prepare(self.version)
        with self.assertRaises(ValueError):self.pipe.review(run['id'],analyst=lambda _:self.fail('called'))
