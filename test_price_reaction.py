import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from unittest.mock import patch
from event_core import Store, iso
from price_reaction import summarize, Reactions


AT = datetime(2026,10,19,14,0,30,tzinfo=timezone.utc)


def bar(offset, price):
    return {'t':iso(AT.replace(second=0)+timedelta(minutes=offset)), 'c':price}


class CalculationTests(unittest.TestCase):
    def test_event_minute_is_not_the_baseline(self):
        result=summarize(AT,AT+timedelta(minutes=10),[bar(-1,100),bar(0,500),bar(4,110)])
        self.assertEqual(result['baseline']['close'],100)
        self.assertAlmostEqual(result['horizons'][0]['change_pct'],10)

    def test_incomplete_future_bar_is_excluded(self):
        result=summarize(AT,AT+timedelta(minutes=4),[bar(-1,100),bar(4,900)])
        self.assertIsNone(result['latest'])
        self.assertEqual(result['horizons'][0]['status'],'pending')

    def test_weekend_gap_does_not_reuse_old_price(self):
        result=summarize(AT,AT+timedelta(minutes=70),[bar(-20,100),bar(5,110)])
        self.assertEqual(result['status'],'no_baseline')
        self.assertTrue(all(h['change_pct'] is None for h in result['horizons']))

    def test_missing_horizon_not_interpolated(self):
        result=summarize(AT,AT+timedelta(minutes=70),[bar(-1,100),bar(1,110)])
        self.assertEqual(result['horizons'][1]['status'],'missing_data')

    def test_nan_and_nonpositive_rejected(self):
        for price in [float('nan'),float('inf'),0,-1,True]:
            with self.assertRaises(ValueError):summarize(AT,AT,[bar(-1,price)])

    def test_conflicting_and_unaligned_bars_rejected(self):
        with self.assertRaises(ValueError):summarize(AT,AT,[bar(-1,100),bar(-1,101)])
        with self.assertRaises(ValueError):summarize(AT,AT,[{'t':iso(AT),'c':100}])


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.now=AT+timedelta(minutes=20)
        self.store=Store(self.tmp.name+'/db',lambda:self.now);self.r=Reactions(self.store)
        self.version=self.store.ingest(source='fixture',source_id='1',symbol='AAA',published_at=AT,
            updated_at=AT,url='https://example.org',payload={})[0]

    def tearDown(self):self.tmp.cleanup()

    def test_raw_observation_saved_and_refresh_throttled(self):
        calls=[]
        def transport(*args):calls.append(args);return [bar(-1,100),bar(4,105),bar(14,110)]
        with patch.dict(os.environ,{'PRICE_DATA_FEED':'iex'}):
            result=self.r.measure(self.version,transport)
            self.r.measure(self.version,lambda *_:self.fail('repeat'))
        self.assertEqual(len(calls),1)
        self.assertFalse(result['report']['executable_quote'])
        self.assertEqual(result['report']['order_permission'],'none')
        self.assertEqual(result['report']['feed'],'iex')
        with self.store.db() as c:
            self.assertEqual(len(json.loads(c.execute('SELECT raw_bars FROM price_reactions').fetchone()[0])),3)

    def test_provider_error_is_safe_and_never_falls_back(self):
        calls=[]
        def fail(*args):calls.append(args);raise HTTPError('secret',403,'secret',{},None)
        with patch.dict(os.environ,{'PRICE_DATA_FEED':'sip'}):
            result=self.r.measure(self.version,fail)
        self.assertEqual(result['report']['status'],'provider_http_403')
        self.assertEqual(len(calls),1)
        self.assertNotIn('secret',json.dumps(result))

    def test_follow_up_stops_after_ninety_minutes(self):
        from discovery import Discovery
        Discovery(self.store)
        with self.store.db() as c:
            c.execute('INSERT INTO discovery_runs VALUES (?,?,?,?,?,?)',('run',self.version,iso(self.now),'completed',None,None))
        self.assertIsNotNone(self.r.follow_up(lambda *_:[]))
        self.now=AT+timedelta(minutes=91)
        self.assertIsNone(self.r.follow_up(lambda *_:self.fail('late request')))


class RouteTests(unittest.TestCase):
    def test_price_measurement_requires_login_and_csrf_and_renders(self):
        import base64
        from webapp import create_app
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ,{
                'DASHBOARD_PASSWORD':'test-password-at-least-20-chars','PRICE_DATA_FEED':'iex'}):
            store=Store(directory+'/db',lambda:AT+timedelta(minutes=20))
            version=store.ingest(source='fixture',source_id='1',symbol='AAA',published_at=AT,
                url='https://example.org',payload={'headline':'Fixture event'})[0]
            calls=[]
            def transport(*args):calls.append(args);return [bar(-1,100),bar(4,105)]
            client=create_app(store,price_transport=transport).test_client()
            auth={'Authorization':'Basic '+base64.b64encode(b'user:test-password-at-least-20-chars').decode()}
            path='/reaction/'+version
            self.assertEqual(client.post(path,base_url='https://localhost').status_code,401)
            self.assertEqual(client.post(path,headers=auth,base_url='https://localhost').status_code,403)
            client.get('/',headers=auth,base_url='https://localhost')
            with client.session_transaction(base_url='https://localhost') as session:token=session['csrf']
            response=client.post(path,data={'csrf':token},headers=auth,base_url='https://localhost')
            self.assertEqual(response.status_code,302)
            page=client.get('/',headers=auth,base_url='https://localhost')
            self.assertEqual(page.status_code,200)
            self.assertIn(b'+5.00',page.data)
            self.assertEqual(len(calls),1)
