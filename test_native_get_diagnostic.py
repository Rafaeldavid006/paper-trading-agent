"""Disposable migrated cohort, real guard/quotas/parser/HTTP JSON, no sockets."""
from datetime import timedelta
import io
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import Mock,patch
from urllib import error
from urllib.parse import urlsplit

import native_get_diagnostic as diagnostic
import native_get_diagnostic_trace as trace_module
import native_observation_collector as collector
import native_restart_migration as migration
import native_schema_diagnostic as old
from native_account_guard import SharedAccountGuard
from native_postmigration_supervisors import GetDiagnosticSupervisor,ExistingAuditSupervisor,verify_completed_storage
from runtime_state_bundle import RuntimeStateBundle
from test_native_observation_worker import FP
import test_readonly_runtime_worker as runtime_fixtures
from readonly_runtime_worker import run_phase as diagnostic_run_phase


class Response:
    def __init__(self,url,data):
        self.url=url;self.data=json.dumps(data).encode();self.headers={};self.status=200
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def geturl(self):return self.url
    def read(self,size):return self.data[:size]


class Opener:
    def __init__(self,wire):self.wire=wire;self.requests=[];self.mutate=lambda response,path:None
    def open(self,request,timeout):
        self.requests.append(request)
        split=urlsplit(request.full_url);path=split.path+('?' + split.query if split.query else '')
        status,data=self.wire.call(request.get_method(),path)
        if status!=200:raise error.HTTPError(request.full_url,status,'PRIVATE',{},io.BytesIO(b'PRIVATE'))
        response=Response(request.full_url,data);self.mutate(response,path);return response


class DiagnosticTests(unittest.TestCase):
    setUp=runtime_fixtures.RuntimeWorkerTests.setUp
    def setup_migrated(self):
        diagnostic_run_phase('initialize',**self.kw)
        now=self.kw['now_ns']();claim_at=now-95*collector.NS
        with sqlite3.connect(self.paths['runner']) as db:
            db.execute("UPDATE runner_control SET dirty=1,attention='readonly_runtime_stage_failed'")
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute("INSERT INTO native_read_audit VALUES(?,'native_schema_refused',6,0,2,NULL)",(now-100*collector.NS,))
            db.execute("INSERT INTO native_read_audit VALUES(?,'provider_read_failed',4,0,0,NULL)",(now-90*collector.NS,))
        with sqlite3.connect(self.paths['anchor']) as db:
            db.execute(old._SQL)
            db.execute("INSERT INTO native_schema_diagnostic_claim VALUES(1,?,?,?,'collection_refused','none','none',4)",
                       (old.GENERATION,old.code_hash(),claim_at))
        backup=self.root/'backup';backup.mkdir(mode=0o700)
        plan=migration.prepare(paths=self.paths,test_only=True)
        migration.apply(enabled=True,plan=plan,approved_digest=plan.digest,quiescence_confirmed=True,
                        backup_directory=str(backup),paths=self.paths,test_only=True)
        self.opener=Opener(self.wire)
        self.enterContext(patch.object(collector.request,'build_opener',return_value=self.opener))
        self.args=dict(enabled=True,window=(now-collector.NS,now+60*collector.NS),paths=self.paths,
                       test_only=True,environ=self.env,now_ns=self.kw['now_ns'])
    def run_sample(self):return diagnostic.run_once(**self.args)
    def protected(self):
        result={role:Path(self.paths[role]).read_bytes() for role in
                ('legacy','legacy_lock','lifecycle','runner','source','decision','seal','manifest')}
        with sqlite3.connect(self.paths['guard']) as db:
            result['old_audit']=db.execute('SELECT * FROM native_read_audit').fetchall()
        with sqlite3.connect(self.paths['anchor']) as db:
            result['old_claim']=db.execute('SELECT * FROM native_schema_diagnostic_claim').fetchall()
        return result
    def receipt(self):
        with sqlite3.connect(self.paths['anchor']) as db:
            return db.execute('SELECT status,audit_json,result_json FROM native_get_diagnostic_claim').fetchone()
    def test_success_real_migrated_guard_quota_http_json_parser_preserves_old_state(self):
        self.setup_migrated();before=self.protected()
        with patch.object(collector.os,'fchmod',side_effect=AssertionError('no chmod')):
            result=self.run_sample()
        self.assertEqual(result['reason'],'sample_observed',result)
        self.assertEqual((result['requests'],result['confirmed_collector_reservations'],result['confirmed_account_reservations'],result['open_attempts']),(8,8,8,8))
        self.assertEqual(self.protected(),before)
        self.assertEqual(self.receipt()[0],'recorded')
        self.assertEqual(json.loads(self.receipt()[1])['status'],'observed_readonly')
        self.assertEqual(diagnostic.historic.run_once(enabled=True,paths=self.paths,test_only=True)['audit_code'],'provider_read_failed')
        self.assertTrue(all(r.get_method()=='GET' and r.data is None for r in self.opener.requests))
        with sqlite3.connect(self.paths['guard']) as db:
            for table in ('native_read_attempts','account_guard_attempts'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone(),(8,))
    def test_fourth_attempt_account_quota_refusal_only_three_http_starts(self):
        self.setup_migrated();now=self.kw['now_ns']()
        bundle=RuntimeStateBundle(FP,paths=self.paths,test_only=True)
        guard=SharedAccountGuard(self.paths['legacy'],self.paths['lifecycle'],FP,enabled=True,
            state_database=self.paths['guard'],test_only=True,runtime_bundle=bundle)
        with guard.hold(now_ns=now,read_only=True):
            for _ in range(9):guard.reserve_request(now)
        result=self.run_sample()
        self.assertEqual((result['collector_code'],result['layer'],result['detail_code']),
            ('provider_read_failed','account_quota','shared_request_budget_exhausted'))
        self.assertEqual((result['endpoint_role'],result['request_ordinal'],result['open_attempts']),('positions',4,3))
        self.assertEqual((result['confirmed_collector_reservations'],result['confirmed_account_reservations']),(4,3))
    def test_collector_quota_is_not_refilled_or_bypassed(self):
        self.setup_migrated();now=self.kw['now_ns']()
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute('UPDATE native_read_budget SET high_ns=?',(now,))
            db.executemany('INSERT INTO native_read_attempts VALUES(?)',[(now,)]*12)
        result=self.run_sample()
        self.assertEqual((result['layer'],result['detail_code'],result['open_attempts']),
            ('collector_quota','shared_request_budget_exhausted',0))
    def test_http_status_transport_timeout_json_and_schema_are_distinct(self):
        cases=(('http','http','http_status'),('timeout','transport_read','timeout'),
               ('json','json','invalid_json'),('schema','schema','order_timestamp_order'))
        for fault,layer,detail in cases:
            with self.subTest(fault=fault):
                fixture=DiagnosticTests();fixture.setUp()
                try:
                    fixture.setup_migrated()
                    def mutate(response,path):
                        if path!='/v2/positions':return
                        if fault=='timeout':response.read=Mock(side_effect=TimeoutError('PRIVATE SECRET'))
                        if fault=='json':response.data=b'{PRIVATE SECRET'
                    fixture.opener.mutate=mutate
                    if fault=='http':fixture.wire.call=Mock(return_value=(403,None))
                    if fault=='schema':fixture.wire.legacy[0]['created_at']='2026-10-19T17:14:16.000001Z'
                    result=fixture.run_sample()
                    self.assertEqual((result['layer'],result['detail_code']),(layer,detail),result)
                    self.assertNotIn('PRIVATE',json.dumps(result)+str(fixture.receipt()))
                    if fault=='http':self.assertEqual(result['http_status'],403)
                    if fault=='schema':self.assertEqual(result['parser'],'parse_order')
                finally:fixture.doCleanups()
    def test_restart_cancel_and_setup_failure_never_replay(self):
        self.setup_migrated();self.args['transport_factory']=Mock(side_effect=KeyboardInterrupt('PRIVATE'))
        with self.assertRaises(KeyboardInterrupt):self.run_sample()
        self.assertEqual(self.receipt(),('claimed',None,None))
        self.assertEqual(self.run_sample()['reason'],'diagnostic_already_claimed')
        self.args['transport_factory'].assert_called_once();self.assertEqual(self.wire.calls,[])
    def test_real_process_crash_after_four_reservations_is_consumed_on_restart(self):
        self.setup_migrated();before=self.protected()
        def crash_child():
            original=self.opener.open
            def crash(request,timeout):
                if urlsplit(request.full_url).path=='/v2/positions':os._exit(23)
                return original(request,timeout)
            self.opener.open=crash
            self.run_sample()
        child=multiprocessing.get_context('fork').Process(target=crash_child)
        child.start();child.join(5)
        if child.is_alive():child.kill();child.join();self.fail('crash fixture exceeded bound')
        self.assertEqual(child.exitcode,23)
        self.assertEqual(self.receipt(),('claimed',None,None))
        self.assertEqual(self.run_sample()['reason'],'diagnostic_already_claimed')
        self.assertEqual(self.opener.requests,[]);self.assertEqual(self.protected(),before)
        with sqlite3.connect(self.paths['guard']) as db:
            for table in ('native_read_attempts','account_guard_attempts'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone(),(4,))
    def test_closed_provider_clock_consumes_claim_after_two_gets(self):
        self.setup_migrated()
        def closed(response,path):
            if path=='/v2/clock':
                value=json.loads(response.data);value['is_open']=False;response.data=json.dumps(value).encode()
        self.opener.mutate=closed;result=self.run_sample()
        self.assertEqual((result['collector_code'],result['open_attempts']),('closed_session',2))
        self.assertEqual(self.run_sample()['reason'],'diagnostic_already_claimed')
    def test_expired_before_claim_does_not_read_secrets_or_touch_storage(self):
        self.setup_migrated();before={p:Path(p).read_bytes() for p in self.paths.values()}
        self.now+=timedelta(minutes=2);env=Mock();env.get.side_effect=AssertionError('no secrets');self.args['environ']=env
        self.assertEqual(self.run_sample()['reason'],'outside_approved_window')
        self.assertEqual({p:Path(p).read_bytes() for p in self.paths.values()},before);env.get.assert_not_called()
    def test_window_end_at_claim_transaction_never_creates_claim_or_quota(self):
        self.setup_migrated();before={p:Path(p).read_bytes() for p in self.paths.values()}
        now=self.kw['now_ns']();end=self.args['window'][1]
        self.args['now_ns']=Mock(side_effect=[now,now,now,end])
        self.assertEqual(self.run_sample()['reason'],'outside_approved_window')
        self.assertEqual({p:Path(p).read_bytes() for p in self.paths.values()},before)
        self.assertEqual(self.opener.requests,[])
    def test_window_expiry_after_claim_stops_without_repeat(self):
        self.setup_migrated()
        self.wire.on_call=lambda *args:setattr(self,'now',self.now+timedelta(minutes=2))
        result=self.run_sample()
        self.assertEqual(result['open_attempts'],1,result)
        self.assertEqual(self.run_sample()['reason'],'outside_approved_window')
        self.now-=timedelta(minutes=2)
        self.assertEqual(self.run_sample()['reason'],'diagnostic_already_claimed')
    def test_disabled_and_invalid_production_overrides_are_no_io(self):
        with patch.object(diagnostic,'load_owned_ledger',side_effect=AssertionError('no storage')):
            self.assertEqual(diagnostic.run_once()['reason'],'diagnostic_disabled')
            with self.assertRaises(ValueError):diagnostic.run_once(enabled=True)
    def test_missing_state_never_recreated(self):
        self.setup_migrated();Path(self.paths['guard']).unlink()
        with self.assertRaises(Exception):self.run_sample()
        self.assertFalse(Path(self.paths['guard']).exists());self.assertEqual(self.opener.requests,[])
    def test_scoped_observers_restored_after_cancellation(self):
        self.setup_migrated();original=(collector.json,collector.SharedRequestBudget,collector.parse_account)
        self.wire.on_call=Mock(side_effect=KeyboardInterrupt('PRIVATE'))
        with self.assertRaises(KeyboardInterrupt):self.run_sample()
        self.assertEqual((collector.json,collector.SharedRequestBudget,collector.parse_account),original)
    def test_all_eight_database_sidecars_refused_without_claim_or_recovery(self):
        self.setup_migrated();before=self.protected()
        for role in migration.DB_ROLES:
            for suffix in ('-journal','-wal','-shm'):
                path=Path(self.paths[role]+suffix);path.write_bytes(b'not-a-journal');path.chmod(0o600)
                try:
                    with self.subTest(role=role,suffix=suffix),self.assertRaises(Exception):self.run_sample()
                    self.assertEqual(path.read_bytes(),b'not-a-journal')
                finally:path.unlink()
        self.assertEqual(self.protected(),before);self.assertEqual(self.opener.requests,[])
    def test_receipt_audit_failure_is_not_success_or_a_retry(self):
        self.setup_migrated();original=diagnostic.persist
        def failed(bundle,receipt,column,value):
            if column=='audit_json':raise OSError('PRIVATE')
            return original(bundle,receipt,column,value)
        with patch.object(diagnostic,'persist',side_effect=failed):result=self.run_sample()
        self.assertEqual(result['reason'],'diagnostic_refused');self.assertTrue(result['audit_failed'])
        self.assertEqual(self.run_sample()['reason'],'diagnostic_already_claimed')


class TransportTests(unittest.TestCase):
    def test_context_manager_returned_response_is_not_used_as_manager(self):
        trace=trace_module.Trace();response=Mock();response.status=200
        manager=Mock();manager.__enter__=Mock(return_value=response);manager.__exit__=Mock(return_value=False)
        with trace_module._Response(manager,trace) as wrapped:
            self.assertEqual(wrapped.status,200)
        manager.__exit__.assert_called_once();self.assertEqual(trace.status,200)
    def test_enter_close_and_decoder_details_are_bounded(self):
        for stage in ('enter','close'):
            trace=trace_module.Trace();manager=Mock();manager.__enter__=Mock(return_value=manager)
            manager.__exit__=Mock(return_value=False)
            getattr(manager,'__enter__' if stage=='enter' else '__exit__').side_effect=TimeoutError('PRIVATE')
            with self.assertRaises(TimeoutError):
                with trace_module._Response(manager,trace):pass
            self.assertEqual(trace.result()['layer'],'transport_'+stage)
            self.assertNotIn('PRIVATE',json.dumps(trace.result()))
        for raw,code in ((b'{','invalid_json'),(b'"\xff"','invalid_text_encoding'),(b'{"a":1,"a":2}','invalid_json_value')):
            trace=trace_module.Trace()
            with self.assertRaises(Exception):trace_module._JSON(json,trace).loads(raw,object_pairs_hook=collector._unique_object)
            self.assertEqual(trace.result()['detail_code'],code)
    def test_remaining_deadline_is_used_at_actual_open(self):
        trace=trace_module.Trace();trace.remaining=lambda:.1
        opener=Mock();opener.open.return_value=Mock()
        trace_module._Opener(opener,trace).open('fixed-request',timeout=2.)
        self.assertEqual(opener.open.call_args.kwargs['timeout'],.1)
        self.assertEqual(trace.open_attempts,1)
    def test_window_crossed_while_calculating_timeout_does_not_open(self):
        clock={'allowed':True};trace=trace_module.Trace(lambda:clock['allowed'])
        def remaining():clock['allowed']=False;return .1
        trace.remaining=remaining;opener=Mock()
        with self.assertRaises(collector.CollectionRefused):
            trace_module._Opener(opener,trace).open('fixed-request',timeout=2.)
        opener.open.assert_not_called();self.assertEqual(trace.open_attempts,0)
    def test_unknown_guard_string_cannot_escape(self):
        guard=Mock();guard.check.side_effect=ValueError('PRIVATE account key')
        trace=trace_module.Trace();wire=trace_module.ObservedWire(Mock(),guard,trace,lambda:1)
        with self.assertRaises(ValueError):wire.call('GET','/v2/account')
        self.assertEqual(trace.result()['detail_code'],'guard_refused')
        self.assertNotIn('PRIVATE',json.dumps(trace.result()))


class StartupTests(unittest.TestCase):
    def test_disabled_supervisor_never_launches(self):
        spawn=Mock();child=GetDiagnosticSupervisor(popen=spawn)
        child.start();child.start();child.close();spawn.assert_not_called();self.assertTrue(child.reaped)
    def test_fixed_get_child_deadline_cancel_and_no_repeat(self):
        child=Mock();child.poll.return_value=None;child.wait.return_value=-9
        timer=Mock();spawn=Mock(return_value=child)
        supervisor=GetDiagnosticSupervisor(enabled=True,popen=spawn,timer_factory=lambda _,cb:timer,emit=Mock())
        supervisor.start();supervisor.start();self.assertEqual(spawn.call_count,1)
        self.assertEqual(Path(spawn.call_args.args[0][1]).name,'native_get_diagnostic.py')
        self.assertFalse(spawn.call_args.kwargs['shell'])
        supervisor._expire();child.kill.assert_called_once();supervisor.start();self.assertEqual(spawn.call_count,1)
        self.assertFalse(supervisor.reaped);child.poll.return_value=-9;self.assertTrue(supervisor.reaped)
        supervisor.close()
    def test_audit_supervisor_does_not_launch_preflight_or_apply(self):
        child=Mock();child.poll.return_value=0;spawn=Mock(return_value=child)
        supervisor=ExistingAuditSupervisor(enabled=True,popen=spawn,timer_factory=lambda _,cb:Mock(),emit=Mock())
        supervisor.start();supervisor.close()
        self.assertEqual(Path(spawn.call_args.args[0][1]).name,'native_diagnostic_audit_inspection.py')
    def test_completion_gate_is_readonly_and_rejects_active_migration_flags(self):
        import native_postmigration_supervisors as module
        stop=Mock();stop.is_set.return_value=False
        with patch.object(module.migration_gate,'_completed_attempt',return_value=True) as verify:
            self.assertTrue(verify_completed_storage(inspection_only=True,stop=stop));verify.assert_called_once()
        with patch.object(module.migration_gate,'ENABLE_APPROVED_MIGRATION',True),patch('builtins.print'),\
             patch.object(module.migration_gate,'_completed_attempt') as verify:
            self.assertFalse(verify_completed_storage(inspection_only=True,stop=stop));verify.assert_not_called()
    def test_cancellation_during_completion_check_does_not_launch_web(self):
        import native_postmigration_supervisors as module
        stop=Mock();stop.is_set.side_effect=[False,True]
        with patch.object(module.migration_gate,'_completed_attempt',return_value=True):
            self.assertFalse(verify_completed_storage(inspection_only=True,stop=stop))
    def test_full_inspection_startup_waits_for_diagnostic_reap_then_only_audit(self):
        import serve
        web=Mock();web.poll.return_value=None;web.wait.return_value=-9
        stop=Mock();stop.wait.side_effect=[False,False,True];stop.is_set.return_value=False
        diagnostic_child=Mock();diagnostic_child.reaped=False
        def poll():diagnostic_child.reaped=True
        diagnostic_child.poll.side_effect=poll
        with patch.object(serve,'validate_environment',return_value=('/tmp/events.db','8000')),\
             patch.object(serve,'run_startup_gate',return_value=True),\
             patch.object(serve.threading,'Event',return_value=stop),patch.object(serve.signal,'signal'),\
             patch.object(serve.subprocess,'Popen',return_value=web) as spawn,\
             patch.object(serve,'GetDiagnosticSupervisor',return_value=diagnostic_child),\
             patch.object(serve,'DiagnosticAuditSupervisor') as audit,\
             patch.object(serve,'NativeProbeSupervisor') as probe,\
             patch.object(serve,'ReadonlyRuntimeSupervisor') as runtime,\
             patch.object(serve,'SchemaDiagnosticSupervisor') as old_diagnostic,\
             patch.dict(serve.os.environ,{'ENABLE_AUTO_RESEARCH':'true'}):
            self.assertEqual(serve.main(),0)
        spawn.assert_called_once();self.assertEqual(spawn.call_args.args[0][-1],'inspection_wsgi:app')
        diagnostic_child.start.assert_called_once();diagnostic_child.close.assert_called_once()
        audit.return_value.start.assert_called_once()
        for worker in (probe,runtime,old_diagnostic):
            worker.assert_called_once_with(enabled=False);worker.return_value.start.assert_not_called()
    def test_failed_completion_starts_no_child(self):
        import serve
        stop=Mock();stop.is_set.return_value=False
        with patch.object(serve,'validate_environment',return_value=('/tmp/events.db','8000')),\
             patch.object(serve,'run_startup_gate',return_value=False),\
             patch.object(serve.threading,'Event',return_value=stop),patch.object(serve.signal,'signal'),\
             patch.object(serve.subprocess,'Popen') as spawn,\
             patch.object(serve,'GetDiagnosticSupervisor') as diagnostic_child,\
             patch.object(serve,'DiagnosticAuditSupervisor') as audit:
            self.assertEqual(serve.main(),1)
        spawn.assert_not_called();diagnostic_child.return_value.start.assert_not_called();audit.return_value.start.assert_not_called()


if __name__=='__main__':unittest.main()
