"""No sockets or actual subprocesses; complete staged observation contract."""
import copy
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock,patch

from event_core import Store
from alpaca_schema import timestamp
from runtime_state_bundle import FIXED_PATHS
from readonly_runtime_worker import run_phase,RuntimeReadOnlyHTTP,main,public
from test_native_observation_worker import create_source,FP
from test_native_paper_execution import Wire
from test_primary_earnings_source import FakeHTTP,feed_bytes,item_xml


class RuntimeWorkerTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket','socket.create_connection','socket.getaddrinfo'):
            self.enterContext(patch(name,side_effect=AssertionError('no sockets or DNS')))
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.paths={k:str(self.root/Path(p).name) for k,p in FIXED_PATHS.items()}
        self.paths['legacy_lock']=self.paths['legacy']+'.experiment.lock'
        create_source(self.paths['legacy'])
        with sqlite3.connect(self.paths['legacy']) as db:db.execute('CREATE TABLE loss_days(day TEXT PRIMARY KEY,stopped INTEGER)')
        Path(self.paths['legacy_lock']).touch(mode=0o600)
        historical=datetime(2026,10,19,12,tzinfo=timezone.utc)
        Store(self.paths['anchor'],clock=lambda:historical).ingest(
            source='fixture',source_id='old-event',symbol='NVDA',
            published_at=historical,payload={'fixture':True},url='https://issuer.example/fixture')
        self.now=datetime(2026,10,20,13,36,1,tzinfo=timezone.utc)
        self.http=FakeHTTP();self.http.feed=feed_bytes('')
        self.wire=Wire(lambda:self.now)
        self.env={'ALPACA_API_KEY':'fixture-key','ALPACA_SECRET_KEY':'fixture-secret'}
        self.kw=dict(paths=self.paths,test_only=True,environ=self.env,now_ns=lambda:timestamp(self.now.isoformat()),
            broker_factory=lambda *a,**k:self.wire,source_factory=lambda **k:self.http)
    def stage(self,phase):
        return run_phase(phase,**self.kw)
    def initialize(self):return self.stage('initialize')
    def test_initializer_enrolls_once_without_network_or_secrets(self):
        before=Path(self.paths['legacy']).read_bytes()
        self.kw['environ']=Mock();self.kw['environ'].get.side_effect=AssertionError('no keys during init')
        self.assertEqual(self.initialize()['reason'],'initialized_readonly')
        self.assertEqual(self.initialize()['reason'],'initialized_readonly')
        self.assertEqual(self.wire.calls,[]);self.assertEqual(self.http.calls,[])
        self.assertEqual(before,Path(self.paths['legacy']).read_bytes())
    def test_source_and_broker_connected_readonly_observation(self):
        self.initialize()
        self.assertEqual(self.stage('source')['reason'],'no_earnings_in_feed')
        result=self.stage('broker');self.assertEqual(result['reason'],'observed_readonly')
        self.assertTrue(result['market_inputs_valid']);self.assertTrue(result['legacy_reconciled'])
        self.assertTrue(result['source_healthy']);self.assertEqual(result['order_permission'],'none')
        self.assertFalse(result['execution_ready']);self.assertLessEqual(len(self.wire.calls),12)
        self.assertTrue(all(c[0]=='GET' for c in self.wire.calls))
        with sqlite3.connect(self.paths['lifecycle']) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM intents').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_dispatch').fetchone()[0],0)
        self.assertNotIn(self.wire.account['id'].encode(),Path(self.paths['lifecycle']).read_bytes())
    def test_closed_market_is_expected_not_failure(self):
        self.initialize();original=self.wire.call
        def closed(method,path,body=None):
            status,data=original(method,path,body)
            if path=='/v2/clock':data['is_open']=False
            return status,data
        self.wire.call=closed
        result=self.stage('broker');self.assertEqual(result['reason'],'closed_session')
        self.assertFalse(result['market_inputs_valid']);self.assertFalse(result['legacy_reconciled'])
        self.assertEqual(len(self.wire.calls),3)
        with sqlite3.connect(self.paths['runner']) as db:self.assertEqual(db.execute('SELECT dirty,attention FROM runner_control').fetchone(),(0,None))
    def test_opening_minutes_wait_for_regular_bars_without_latching(self):
        self.now=self.now.replace(hour=13,minute=31)
        self.initialize();self.stage('source')
        result=self.stage('broker');self.assertEqual(result['reason'],'waiting_regular_bar_window')
        self.assertFalse(result['market_inputs_valid'])
        self.assertFalse(any('/bars?' in p for _,p,_ in self.wire.calls))
        with sqlite3.connect(self.paths['runner']) as db:self.assertEqual(db.execute('SELECT dirty,attention FROM runner_control').fetchone(),(0,None))
    def test_missing_state_never_recreated_on_initialize_or_stage(self):
        self.initialize();Path(self.paths['runner']).unlink()
        for phase in ('initialize','source','broker'):
            with self.subTest(phase=phase),self.assertRaises(Exception):self.stage(phase)
            self.assertFalse(Path(self.paths['runner']).exists())
        self.assertEqual(self.http.calls,[]);self.assertEqual(self.wire.calls,[])
    def test_dirty_state_refuses_restart_without_network(self):
        self.initialize()
        with sqlite3.connect(self.paths['runner']) as db:db.execute('UPDATE runner_control SET dirty=1')
        for phase in ('initialize','source','broker'):
            with self.assertRaises(Exception):self.stage(phase)
        self.assertEqual(self.wire.calls,[]);self.assertEqual(self.http.calls,[])
    def test_source_interruption_remains_latched(self):
        self.initialize();self.http.get=Mock(side_effect=KeyboardInterrupt('private-provider-data'))
        with self.assertRaises(KeyboardInterrupt):self.stage('source')
        with self.assertRaises(Exception):self.initialize()
        self.assertEqual(self.http.get.call_count,1)
        with sqlite3.connect(self.paths['runner']) as db:self.assertEqual(db.execute('SELECT dirty,attention FROM runner_control').fetchone(),(1,'readonly_runtime_stage_failed'))
    def test_stage_source_failure_latches_and_is_not_retried(self):
        self.initialize();self.http.fail=True
        with self.assertRaises(Exception):self.stage('source')
        with self.assertRaises(Exception):self.stage('source')
        self.assertEqual(len(self.http.calls),1)
    def test_source_bytes_to_signal_preview_never_claims_order(self):
        self.now=datetime(2026,10,20,13,31,1,tzinfo=timezone.utc)
        self.http.feed=feed_bytes(item_xml(date='Tue, 20 Oct 2026 13:31:00 GMT'))
        self.initialize();self.stage('source')
        for _ in range(15):self.now+=timedelta(seconds=20);self.stage('source')
        result=self.stage('broker');self.assertEqual(result['preview_enter'],1)
        with sqlite3.connect(self.paths['lifecycle']) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM intents').fetchone()[0],0)
        self.assertTrue(all(c[0]=='GET' for c in self.wire.calls))
        # Re-observation outside the immutable decision window does not rewrite it.
        for _ in range(4):self.now+=timedelta(seconds=20);self.stage('source')
        result=self.stage('broker');self.assertEqual(result['preview_enter'],0)
    def test_stopped_legacy_cannot_create_a_submission(self):
        self.initialize()
        with sqlite3.connect(self.paths['legacy']) as db:db.execute('UPDATE safety SET stopped=1')
        result=self.stage('broker')
        self.assertEqual(result['order_permission'],'none');self.assertTrue(all(c[0]=='GET' for c in self.wire.calls))
    def test_no_runtime_path_or_phase_override(self):
        with self.assertRaises(Exception):run_phase('broker',paths=self.paths)
        for phase in ('execute','submit','provision','cancel','--broker'):
            with self.assertRaises(Exception):run_phase(phase,**self.kw)
    def test_entrypoints_redact_all_failures(self):
        with patch('readonly_runtime_worker.run_phase',side_effect=ValueError('private-account-secret')), \
             patch('builtins.print') as emit:
            self.assertEqual(main(['--broker']),1)
        raw=emit.call_args.args[0];self.assertNotIn('private',raw);self.assertEqual(json.loads(raw)['order_permission'],'none')
    def test_public_summary_rejects_sensitive_fields_and_values(self):
        with self.assertRaises(Exception):public('broker','ok',account_id='secret')
        with self.assertRaises(Exception):public('broker','ok',source_events='secret')


class ReadOnlyTransportTests(unittest.TestCase):
    def test_writes_rejected_even_when_enabled(self):
        with patch('readonly_runtime_worker.NativePaperHTTP') as factory:
            h=RuntimeReadOnlyHTTP('fixture','fixture',enabled=True)
            for method in ('POST','DELETE','PATCH','PUT','get','GET'):
                with self.assertRaises(Exception):h.call(method,'/v2/orders',{'forbidden':'body'})
            factory.return_value.call.assert_not_called()
            for method in ('POST','DELETE','PATCH','PUT'):
                with self.assertRaises(Exception):h.call(method,'/v2/orders')
            factory.return_value.call.assert_not_called()
            h.call('GET','/v2/account');factory.return_value.call.assert_called_once_with('GET','/v2/account')
    def test_default_read_transport_disabled(self):
        with patch('readonly_runtime_worker.NativePaperHTTP') as factory:
            with self.assertRaises(Exception):RuntimeReadOnlyHTTP('fixture','fixture').call('GET','/v2/account')
            factory.return_value.call.assert_not_called()


class StartupHookTests(unittest.TestCase):
    def test_existing_service_starts_only_separate_readonly_supervisor(self):
        import serve
        web=Mock();web.poll.return_value=None
        worker=Mock();worker.poll.return_value=0
        stop=Mock();stop.wait.side_effect=[False,True];stop.is_set.return_value=False
        with patch.object(serve,'validate_environment',return_value=('/tmp/events.db','8000')), \
             patch.dict(serve.os.environ,{'ENABLE_AUTO_RESEARCH':'false'}), \
             patch.object(serve.subprocess,'Popen',side_effect=[web,worker]) as popen, \
             patch.object(serve.threading,'Event',return_value=stop), \
             patch.object(serve.signal,'signal'), \
             patch.object(serve,'NativeProbeSupervisor') as probe, \
             patch.object(serve,'ReadonlyRuntimeSupervisor') as runtime:
            self.assertEqual(serve.main(),0)
        probe.assert_called_once_with(enabled=True)
        runtime.assert_called_once_with(enabled=True)
        runtime.return_value.start.assert_called_once()
        runtime.return_value.poll.assert_called_once()
        runtime.return_value.close.assert_called_once()
        self.assertEqual(popen.call_count,2)


if __name__=='__main__':unittest.main()
