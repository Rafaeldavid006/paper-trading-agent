"""Strict inspection WSGI with real Flask routes and disposable local state."""
import base64
from contextlib import ExitStack
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from event_core import Store
from webapp import create_app
import finite_pilot_contract as contract
import test_finite_startup_integration as finite_fixture

PASSWORD='fixture-password-at-least-20-characters'


class InspectionRouteTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.path=self.root/'events.sqlite'
        self.enterContext(patch.dict(os.environ,dict(EVENT_DB=str(self.path),
            DASHBOARD_PASSWORD=PASSWORD,FLASK_SECRET_KEY='fixture-signing-secret-at-least-32',
            ENABLE_AUTO_RESEARCH='true',ENABLE_WEB_RESEARCH='true',ENABLE_AI_RESEARCH='true'),clear=True))
        # Create only the disposable pre-existing fixture before the snapshot.
        source=Store(self.path)
        source_app=create_app(source)
        self.version=source.ingest(source='fixture',source_id='1',symbol='NVDA',
            published_at=datetime(2026,10,20,13,30,tzinfo=timezone.utc),
            payload={'headline':'Synthetic fixture'},url='https://example.org/fixture')[0]
        self.run=source_app.extensions['research_pipeline'].prepare(self.version)['id']
        source_client=source_app.test_client();source_client.get('/login',base_url='https://localhost')
        with source_client.session_transaction(base_url='https://localhost') as session:
            csrf=session['csrf']
        self.assertEqual(source_client.post('/login',data=dict(csrf=csrf,password=PASSWORD),
            base_url='https://localhost').status_code,302)
        self.cookie=source_client.get_cookie('session').value
        self.before=self.path.read_bytes()
        self.files=sorted(p.name for p in self.root.iterdir())
        for name in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
            self.enterContext(patch(name,side_effect=AssertionError('NO NETWORK')))
        sys.modules.pop('inspection_wsgi',None)
        self.module=importlib.import_module('inspection_wsgi')
        self.addCleanup(lambda:sys.modules.pop('inspection_wsgi',None))
        self.app=self.module.app;self.client=self.app.test_client()
        self.auth={'Authorization':'Basic '+base64.b64encode(('user:'+PASSWORD).encode()).decode()}
        self.provider_traps=[]
        for name in ('webapp.tick','discovery.Discovery.run','pipeline.Pipeline.review',
                     'price_reaction.Reactions.check_connection','price_reaction.Reactions.measure'):
            self.provider_traps.append(self.enterContext(patch(name,side_effect=AssertionError('NO PROVIDER'))))

    def assert_unchanged(self):
        self.assertEqual(self.path.read_bytes(),self.before)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()),self.files)
        for trap in self.provider_traps:trap.assert_not_called()

    def test_existing_read_pages_and_health_keep_state_and_cookies_unchanged(self):
        paths=('/', '/health','/paper','/history','/discovery/'+self.version,'/research/'+self.run)
        for method in ('GET','HEAD'):
            for path in paths:
                with self.subTest(method=method,path=path):
                    response=self.client.open(path,method=method,headers=self.auth,base_url='https://localhost')
                    self.assertEqual(response.status_code,200)
                    self.assertNotIn('Set-Cookie',response.headers)
        self.assert_unchanged()

    def test_existing_session_authentication_works_without_refresh_or_revoke(self):
        self.client.set_cookie('session',self.cookie,domain='localhost',secure=True)
        for path in ('/','/paper','/history'):
            response=self.client.get(path,base_url='https://localhost')
            self.assertEqual(response.status_code,200);self.assertNotIn('Set-Cookie',response.headers)
        self.assertEqual(self.client.post('/logout',base_url='https://localhost').status_code,405)
        self.assertEqual(self.client.get('/',base_url='https://localhost').status_code,200)
        self.assert_unchanged()

    def test_no_new_login_and_no_authentication_or_internal_mutation_bypass(self):
        paths=('/', '/health','/login','/logout','/collect','/market-data-check',
               '/paper/check','/paper/enable','/paper/stop','/reaction/'+self.version,
               '/prepare/'+self.version,'/discovery/'+self.version,'/evidence/'+self.run,'/review/'+self.run)
        self.client.set_cookie('session',self.cookie,domain='localhost',secure=True)
        for method in ('POST','PUT','PATCH','DELETE','TRACE'):
            for path in paths:
                with self.subTest(method=method,path=path):
                    response=self.client.open(path,method=method,
                        headers=dict(self.auth,**{'X-Internal-Request':'true','X-HTTP-Method-Override':'GET'}),
                        data=dict(password=PASSWORD,confirm='yes',csrf='fixture'),base_url='https://localhost')
                    self.assertEqual(response.status_code,405)
                    self.assertEqual(response.json['reason'],'inspection_route_readonly')
                    self.assertNotIn('Set-Cookie',response.headers)
        self.assertEqual(self.client.get('/login').status_code,405)
        self.assertEqual(self.client.options('/collect',headers=self.auth).status_code,405)
        self.assert_unchanged()

    def test_future_routes_and_direct_mutating_handler_calls_are_denied(self):
        for endpoint,path in (('collect','/collect'),('login','/login'),('discover','/discovery/'+self.version)):
            with self.app.test_request_context(path,method='POST',headers=self.auth):
                args={'version':self.version} if endpoint=='discover' else {}
                self.assertEqual(self.app.view_functions[endpoint](**args)[1],405)
        unknown=Mock(side_effect=AssertionError('UNREVIEWED ROUTE'))
        self.app.add_url_rule('/new-unreviewed-read','new_unreviewed_read',lambda:unknown(),methods=['GET'])
        self.assertEqual(self.client.get('/new-unreviewed-read',headers=self.auth).status_code,405)
        unknown.assert_not_called();self.assert_unchanged()

    def test_store_remains_readonly_even_if_internal_startup_flag_is_changed(self):
        self.module._store._startup=False
        with self.module._store.db() as db:
            self.assertEqual(db.execute('PRAGMA query_only').fetchone()[0],1)
            with self.assertRaises(sqlite3.OperationalError):
                db.execute("UPDATE dashboard_sessions SET expires_at='2099-01-01T00:00:00+00:00'")
        self.assert_unchanged()

    def test_health_distinguishes_web_routes_from_compiled_pilot_authority(self):
        pilot,recovery,proof=finite_fixture.StartupSelectionTests.grants(self)
        for grants,expected in (({},False),({'APPROVED_RECOVERY':recovery},False),
                                ({'APPROVED_INSPECTION':proof},False),({'APPROVED_PILOT':pilot},True)):
            with self.subTest(grants=tuple(grants)),ExitStack() as stack:
                for name in ('APPROVED_PILOT','APPROVED_RECOVERY','APPROVED_INSPECTION'):
                    stack.enter_context(patch.object(contract,name,grants.get(name)))
                with patch.object(self.module._store,'db',side_effect=AssertionError('NO HEALTH DB')):
                    value=self.client.get('/health').json
                self.assertEqual(value['order_permission'],'none')
                self.assertEqual(value['order_permission_scope'],'web_routes')
                self.assertIs(value['finite_pilot_approval_compiled'],expected)
                self.assertIs(value['execution_ready'],False);self.assertIs(value['account_verified'],False)
        self.assert_unchanged()


if __name__=='__main__':unittest.main()
