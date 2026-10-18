import json
import os
import pathlib
import tempfile
import unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from event_core import Store
from paper_adapter import SafetyError
from paper_readonly import ReadOnlyPaperHTTP, ReadOnlyPaperConnection, build_readonly_connection
from test_paper_adapter import MockPaper
from webapp import create_app


class ReadOnlyTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.now=datetime(2026,10,19,15,tzinfo=timezone.utc)
        self.transport=MockPaper(lambda:self.now)
        self.transport.account['account_number']='TEST12345678'
        self.connection=ReadOnlyPaperConnection(self.tmp.name+'/ro.sqlite',self.transport,now=lambda:self.now)

    def tearDown(self):self.tmp.cleanup()

    def test_private_database_and_short_identity_masking(self):
        import stat
        self.assertEqual(0o600,stat.S_IMODE(os.stat(self.connection.database).st_mode))
        self.transport.account['account_number']='ABCD'
        result=self.connection.check_connection()
        self.assertEqual('…****',result['masked_identity'])
        self.assertNotIn('ABCD',json.dumps(result))
        link=pathlib.Path(self.tmp.name)/'linked.sqlite'
        link.symlink_to(self.connection.database)
        with self.assertRaises(OSError):ReadOnlyPaperConnection(link,self.transport)

    def test_transport_rejects_every_write_before_underlying_call(self):
        with patch('paper_readonly.PaperHTTP') as http:
            transport=ReadOnlyPaperHTTP('fixture-key','fixture-secret')
            for method in ('POST','PUT','DELETE','PATCH','HEAD'):
                with self.assertRaises(SafetyError):transport.call(method,'/v2/account')
            with self.assertRaises(SafetyError):transport.call('GET','/v2/account',{})
            with self.assertRaises(SafetyError):transport.call('GET','https://api.alpaca.markets/v2/account')
            with self.assertRaises(SafetyError):transport.call('GET','/v2/orders/anything')
            http.return_value.call.assert_not_called()
            transport.call('GET','/v2/account')
            http.return_value.call.assert_called_once_with('GET','/v2/account')

    def test_discovery_never_verifies_identity_or_enables(self):
        result=self.connection.check_connection()
        self.assertTrue(result['check_ok'])
        self.assertEqual('…5678',result['masked_identity'])
        self.assertFalse(result['identity_verified'])
        self.assertFalse(result['capable'] or result['enabled'])
        with self.assertRaises(SafetyError):self.connection.enable(revision=0,resume=True)
        with self.assertRaises(SafetyError):self.connection.stop(cancel_open=True)
        self.assertFalse(hasattr(self.connection,'submit'))
        self.assertTrue(all(c[0]=='GET' for c in self.transport.calls))
        serialized=json.dumps(result)
        self.assertNotIn('fixture-account',serialized)
        self.assertNotIn('TEST12345678',serialized)
        with self.connection.db() as c:
            row=dict(c.execute('SELECT * FROM paper_readonly').fetchone())
        self.assertNotIn('fixture-account',json.dumps(row))
        self.assertNotIn('TEST12345678',json.dumps(row))
        self.assertEqual(64,len(row['fingerprint']))

    def test_identity_change_blocks_and_retains_original_fingerprint(self):
        self.connection.check_connection()
        with self.connection.db() as c:before=c.execute('SELECT fingerprint FROM paper_readonly').fetchone()[0]
        self.transport.account['id']='another-account'
        result=self.connection.check_connection()
        self.assertFalse(result['check_ok'])
        self.assertEqual('identity_changed',result['reason'])
        self.assertEqual({},result['summary'])
        with self.connection.db() as c:self.assertEqual(before,c.execute('SELECT fingerprint FROM paper_readonly').fetchone()[0])

    def test_failure_clears_snapshot_and_no_raw_exception(self):
        self.connection.check_connection()
        with patch.object(self.transport,'call',side_effect=RuntimeError('secret-sensitive-fixture')):
            result=self.connection.check_connection()
        self.assertEqual('check_failed',result['reason'])
        self.assertFalse(result['last_check_ok'])
        self.assertEqual({},result['summary'])
        self.assertNotIn('secret-sensitive-fixture',json.dumps(result))

    def test_interruption_clears_previous_success(self):
        self.connection.check_connection()
        with patch.object(self.transport,'call',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):self.connection.check_connection()
        self.assertFalse(self.connection.control_status()['last_check_ok'])
        self.assertEqual({},self.connection.control_status()['summary'])

    def test_auth_failure_is_sanitized_and_no_retry(self):
        with patch.object(self.transport,'call',return_value=(401,{'message':'private'})) as call:
            result=self.connection.check_connection()
        self.assertEqual('authentication_failed',result['reason'])
        self.assertEqual(1,call.call_count)

    def test_read_positions_and_open_orders_only_whitelisted_fields(self):
        self.transport.positions=[dict(symbol='AAPL',qty='2',market_value='250',side='long',private='hidden')]
        self.transport.orders={'one':dict(symbol='AAPL',qty='2',filled_qty='1',status='partially_filled',side='buy',client_order_id='private-order-id')}
        result=self.connection.check_connection()
        self.assertTrue(result['check_ok'])
        self.assertEqual(1,result['summary']['positions'])
        self.assertEqual(1,result['summary']['open_orders'])
        self.assertNotIn('private-order-id',json.dumps(result))
        self.assertNotIn('hidden',json.dumps(result))

    def test_stale_and_malformed_data_fail_closed(self):
        self.transport.account['equity']='NaN'
        self.assertFalse(self.connection.check_connection()['check_ok'])
        self.transport.account['equity']='20000'
        original=self.transport.call
        def stale(method,path,body=None):
            if path=='/v2/clock':return 200,dict(timestamp=(self.now-timedelta(seconds=11)).isoformat(),is_open=True)
            return original(method,path,body)
        self.transport.call=stale
        self.assertEqual('stale_response',self.connection.check_connection()['reason'])

    def test_status_refresh_is_local_and_marks_old_snapshot(self):
        self.connection.check_connection()
        self.transport.calls.clear()
        self.now+=timedelta(seconds=31)
        result=self.connection.control_status()
        self.assertFalse(result['fresh'] or result['check_ok'])
        self.assertTrue(result['last_check_ok'])
        self.assertEqual([],self.transport.calls)

    def test_factory_uses_only_injected_existing_keys_and_sanitized_log(self):
        output=[]
        with patch('paper_readonly.ReadOnlyPaperConnection',return_value=self.connection):
            factory=unittest.mock.Mock(return_value=self.transport)
            result=build_readonly_connection(environ={'ALPACA_API_KEY':'fixture-key','ALPACA_SECRET_KEY':'fixture-secret'},
                database=self.tmp.name+'/factory.sqlite',transport_factory=factory,emit=output.append)
        factory.assert_called_once_with('fixture-key','fixture-secret')
        self.assertIs(self.connection,result)
        self.assertEqual([{'mode':'paper_readonly','result':'connected_identity_unverified'}],[json.loads(x) for x in output])
        for text in ('fixture-key','fixture-secret','fixture-account','20000','5678'):
            self.assertNotIn(text,''.join(output))

    def test_missing_config_and_factory_failures_never_reveal_values(self):
        output=[];factory=unittest.mock.Mock(side_effect=RuntimeError('secret-value'))
        self.assertIsNone(build_readonly_connection(environ={},database=self.tmp.name+'/ro.sqlite',transport_factory=factory,emit=output.append))
        factory.assert_not_called()
        self.assertIsNone(build_readonly_connection(environ={'ALPACA_API_KEY':'x','ALPACA_SECRET_KEY':'y'},database=self.tmp.name+'/ro.sqlite',transport_factory=factory,emit=output.append))
        self.assertNotIn('secret-value',''.join(output))
        self.assertIn('initialization_failed',output[-1])

    def test_wsgi_only_wires_readonly_factory(self):
        import runpy
        sentinel=object()
        with patch('paper_readonly.build_readonly_connection',return_value=sentinel) as factory,patch('webapp.create_app') as create:
            runpy.run_path(str(pathlib.Path(__file__).with_name('wsgi.py')))
        factory.assert_called_once_with()
        create.assert_called_once_with(paper_adapter=sentinel)

    def test_authenticated_ui_readonly_no_enable_or_order_controls(self):
        self.connection.check_connection()
        with patch.dict(os.environ,{'DASHBOARD_PASSWORD':'fixture-password-at-least-20','FLASK_SECRET_KEY':'fixture-signing-secret-at-least-32-characters'}):
            app=create_app(Store(self.tmp.name+'/events.sqlite'),paper_adapter=self.connection)
            client=app.test_client();base='https://localhost'
            self.assertEqual(302,client.get('/paper',base_url=base).status_code)
            self.assertEqual(401,client.post('/paper/check',base_url=base).status_code)
            client.get('/login',base_url=base)
            with client.session_transaction(base_url=base) as session:csrf=session['csrf']
            client.post('/login',base_url=base,data={'csrf':csrf,'password':'fixture-password-at-least-20'})
            self.transport.calls.clear()
            page=client.get('/paper',base_url=base).get_data(as_text=True)
            self.assertIn('חיבור Paper לקריאה בלבד',page)
            self.assertIn('זהות החשבון עדיין ממתינה',page)
            self.assertNotIn('action="/paper/enable"',page)
            self.assertNotIn('fixture-account',page)
            self.assertEqual([],self.transport.calls)
            self.assertEqual(403,client.post('/paper/check',base_url=base,data={'csrf':'wrong'}).status_code)
            with client.session_transaction(base_url=base) as session:csrf=session['csrf']
            client.post('/paper/enable',base_url=base,data={'csrf':csrf,'confirm':'yes','revision':0})
            self.assertFalse(self.connection.control_status()['enabled'])
            self.assertEqual([],self.transport.calls)
            self.assertEqual(303,client.post('/paper/check',base_url=base,data={'csrf':csrf}).status_code)
            self.assertTrue(all(c[0]=='GET' for c in self.transport.calls))


if __name__=='__main__':unittest.main()
