import tempfile,unittest
from datetime import datetime,timezone,timedelta
from dataclasses import replace
from event_core import Store,Quote,iso

class EventTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.t=datetime(2026,10,19,12,tzinfo=timezone.utc)
        self.s=Store(self.tmp.name+'/db',lambda:self.t);self.pub=self.t+timedelta(minutes=1)
        self.expected=dict(symbol='ABC',metric='revenue',period='2026Q3',unit='USD',value=100,published_at=self.t-timedelta(minutes=1),source='consensus',url='https://example.org/estimate')
        self.s.expectation(**self.expected);self.t=self.pub+timedelta(seconds=1)
        self.event=dict(source='IR',source_id='1',symbol='ABC',published_at=self.pub,payload={'metric':'revenue','period':'2026Q3','unit':'USD','actual':110},url='https://example.org/release',verified_primary=True)
        self.version,_=self.s.ingest(**self.event);self.t+=timedelta(seconds=1)
        self.q=Quote('ABC',100.9,101,self.t-timedelta(seconds=.5),self.t-timedelta(seconds=.4),True,'SIP-test')
        self.pre=Quote('ABC',99.9,100,self.pub-timedelta(seconds=1),self.pub-timedelta(seconds=.5),True,'SIP-test')
    def tearDown(self):self.tmp.cleanup()
    def decide(self,**kw):
        args=dict(quote=self.q,pre_event_quote=self.pre,market_open=True,halted=False);args.update(kw)
        return self.s.evaluate(self.version,**args)
    def test_research_candidate(self):self.assertEqual(self.decide()['status'],'research_candidate')
    def test_duplicate_ingestion(self):
        v,new=self.s.ingest(**self.event);self.assertEqual(v,self.version);self.assertFalse(new)
    def test_no_repeat(self):
        self.decide();self.assertIn('candidate_already_recorded',self.decide()['reasons'])
    def test_post_event_expectation_not_used(self):
        self.s.expectation(**dict(self.expected,value=1000));self.assertEqual(self.decide()['evidence']['expectation']['value'],100)
    def test_backfill_not_used(self):
        with self.s.db() as c:c.execute('DELETE FROM expectations')
        self.s.expectation(**self.expected);self.assertIn('no_pre_event_expectation',self.decide()['reasons'])
    def test_unit_mismatch(self):
        self.event['payload']['unit']='EUR';self.event['updated_at']=self.t;self.version,_=self.s.ingest(**self.event)
        self.assertIn('no_pre_event_expectation',self.decide()['reasons'])
    def test_revision_invalidates(self):
        decision=self.decide();self.t+=timedelta(seconds=1);self.event['payload']['actual']=90;self.event['updated_at']=self.t
        new,_=self.s.ingest(**self.event)
        with self.s.db() as c:self.assertEqual(c.execute('SELECT new_event_version FROM invalidations WHERE decision_id=?',(decision['decision_id'],)).fetchone()[0],new)
        self.assertIn('superseded_version',self.decide()['reasons'])
    def test_late_old_revision(self):
        self.t+=timedelta(seconds=1);self.event['updated_at']=self.t;self.event['payload']['actual']=115;new,_=self.s.ingest(**self.event)
        self.t+=timedelta(seconds=1);self.event['updated_at']=self.pub;self.event['payload']['actual']=105;self.s.ingest(**self.event)
        self.version=new;self.assertNotIn('superseded_version',self.decide()['reasons'])
    def test_stale_event(self):
        self.t+=timedelta(minutes=5);self.assertIn('event_too_old',self.decide()['reasons'])
    def test_stale_quote(self):self.assertIn('current_quote_stale',self.decide(quote=replace(self.q,exchange_at=self.t-timedelta(seconds=5)))['reasons'])
    def test_partial_feed(self):self.assertIn('current_feed_not_consolidated',self.decide(quote=replace(self.q,consolidated=False))['reasons'])
    def test_already_moved(self):self.assertIn('price_already_moved',self.decide(quote=replace(self.q,bid=109.9,ask=110))['reasons'])
    def test_halt_unknown(self):self.assertIn('halt_status_unknown_or_halted',self.decide(halted=None)['reasons'])
    def test_wrong_symbol(self):self.assertIn('current_symbol_mismatch',self.decide(quote=replace(self.q,symbol='XYZ'))['reasons'])
    def test_timezone(self):
        with self.assertRaises(ValueError):iso(datetime(2026,1,1))
    def test_bad_metric(self):
        self.event['payload']['actual']=True;self.event['updated_at']=self.t;self.version,_=self.s.ingest(**self.event)
        self.assertIn('structured_metric_missing_or_invalid',self.decide()['reasons'])
    def test_heartbeat(self):
        self.s.heartbeat('test','ok');stamp=iso(self.t);self.t+=timedelta(minutes=1);self.s.heartbeat('test','error')
        with self.s.db() as c:self.assertEqual(c.execute('SELECT last_success FROM health').fetchone()[0],stamp)
