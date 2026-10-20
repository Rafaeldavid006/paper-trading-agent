"""Real parser control path, synthetic responses only; all sockets/DNS prohibited."""
from datetime import timedelta
import copy
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch,Mock

import test_readonly_runtime_worker as base
import native_observation_collector as collector
from alpaca_schema import SchemaError
from native_schema_diagnostic import run_once,main,instrument_parsers,PARSERS
from native_schema_diagnostic_supervisor import SchemaDiagnosticSupervisor


class DiagnosticTests(unittest.TestCase):
    setUp=base.RuntimeWorkerTests.setUp
    def setup_halt(self,mutator=None):
        base.run_phase('initialize',**self.kw)
        if mutator:mutator()
        else:self.wire.legacy[0]['created_at']='2026-10-19T17:14:16.000001Z'
        with self.assertRaises(Exception):base.run_phase('broker',**self.kw)
        self.now+=timedelta(seconds=65)
        self.kw['transport_factory']=self.kw.pop('broker_factory')
        self.kw.pop('source_factory')
    def diagnose(self):return run_once(enabled=True,**self.kw)
    def preserved(self):return {k:Path(self.paths[k]).read_bytes() for k in ('legacy','lifecycle','source','runner','decision')}
    def test_exact_order_failure_localizes_without_reset_or_orders(self):
        self.setup_halt();before=self.preserved();calls=len(self.wire.calls)
        result=self.diagnose()
        self.assertEqual(result['reason'],'schema_reproduced',result)
        self.assertEqual(result['parser'],'parse_order');self.assertEqual(result['schema_code'],'order_timestamp_order')
        self.assertEqual(result['prior_requests'],5);self.assertEqual(result['requests'],5)
        self.assertEqual(self.preserved(),before);self.assertTrue(all(c[0]=='GET' for c in self.wire.calls[calls:]))
        self.assertFalse(result['execution_ready']);self.assertEqual(result['order_permission'],'none')
        with sqlite3.connect(self.paths['guard']) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM account_guard_attempts').fetchone(),(5,))
    def test_quote_staleness_is_not_misidentified_as_order_failure(self):
        self.setup_halt();self.wire.legacy[0]['created_at']=self.wire.legacy[0]['submitted_at']
        old=self.wire.call
        def stale(method,path,body=None):
            status,data=old(method,path,body)
            if '/quotes/latest?' in path:
                for q in data['quotes'].values():q['t']=(self.now-timedelta(seconds=11)).isoformat()
            return status,data
        self.wire.call=stale;result=self.diagnose()
        self.assertEqual((result['parser'],result['schema_code']),('parse_latest_quotes','stale_or_future'))
    def test_nonreproduction_does_not_claim_historical_cause_or_readiness(self):
        self.setup_halt();self.wire.legacy[0]['created_at']=self.wire.legacy[0]['submitted_at']
        result=self.diagnose();self.assertEqual(result['reason'],'schema_not_reproduced')
        self.assertEqual(result['parser'],'none');self.assertFalse(result['execution_ready'])
    def test_claim_is_consumed_once_even_when_result_changes_audit(self):
        self.setup_halt();self.diagnose();calls=len(self.wire.calls)
        self.assertEqual(self.diagnose()['reason'],'diagnostic_already_claimed');self.assertEqual(len(self.wire.calls),calls)
    def test_timeout_after_claim_is_not_replayed(self):
        self.setup_halt();self.kw['transport_factory']=Mock(side_effect=KeyboardInterrupt('private'))
        with self.assertRaises(KeyboardInterrupt):self.diagnose()
        self.assertEqual(self.diagnose()['reason'],'diagnostic_already_claimed')
        self.kw['transport_factory'].assert_called_once()
    def test_precondition_refuses_clean_or_wrong_halt_without_key_read(self):
        base.run_phase('initialize',**self.kw)
        kwargs=dict(paths=self.paths,test_only=True,environ=Mock(),now_ns=self.kw['now_ns'])
        kwargs['environ'].get.side_effect=AssertionError('no keys')
        with self.assertRaises(Exception):run_once(enabled=True,**kwargs)
        kwargs['environ'].get.assert_not_called()
    def test_missing_inventory_is_never_recreated(self):
        self.setup_halt();Path(self.paths['runner']).unlink();calls=len(self.wire.calls)
        with self.assertRaises(Exception):self.diagnose()
        self.assertFalse(Path(self.paths['runner']).exists());self.assertEqual(len(self.wire.calls),calls)
    def test_disabled_does_not_touch_environment_or_storage(self):
        env=Mock();env.get.side_effect=AssertionError('secret')
        self.assertEqual(run_once(environ=env)['reason'],'diagnostic_disabled');env.get.assert_not_called()
    def test_capture_unknown_exception_string_is_redacted_and_restored(self):
        original={name:getattr(collector,name) for name in PARSERS}
        with patch.object(collector,'parse_order',side_effect=SchemaError('private account data')):
            with instrument_parsers() as captured:
                with self.assertRaises(SchemaError):collector.parse_order({})
            self.assertEqual(captured,{'parser':'parse_order','schema_code':'unknown_schema'})
        self.assertTrue(all(getattr(collector,name) is fn for name,fn in original.items()))
    def test_sanitized_main_never_prints_exception(self):
        with patch('native_schema_diagnostic.run_once',side_effect=ValueError('private secret')),patch('builtins.print') as emit:
            self.assertEqual(main(['--once']),1)
        self.assertNotIn('private',emit.call_args.args[0]);self.assertNotIn('secret',emit.call_args.args[0])
    def test_startup_diagnostic_waits_for_original_halt_and_reap(self):
        import serve
        for halted,stage,expected in ((False,None,0),(True,'initialize',0),(True,None,1)):
            with self.subTest(halted=halted,stage=stage):
                web=Mock();web.poll.return_value=None
                worker=Mock();worker.poll.return_value=0
                stop=Mock();stop.wait.side_effect=[False,True];stop.is_set.return_value=False
                with patch.object(serve,'validate_environment',return_value=('/tmp/events.db','8000')), \
                     patch.dict(serve.os.environ,{'ENABLE_AUTO_RESEARCH':'false'}), \
                     patch.object(serve.subprocess,'Popen',side_effect=[web,worker]), \
                     patch.object(serve.threading,'Event',return_value=stop),patch.object(serve.signal,'signal'), \
                     patch.object(serve,'NativeProbeSupervisor'),patch.object(serve,'ReadonlyRuntimeSupervisor') as runtime, \
                     patch.object(serve,'SchemaDiagnosticSupervisor') as diagnostic:
                    runtime.return_value.halted=halted;runtime.return_value.active_stage=stage
                    self.assertEqual(serve.main(),0)
                self.assertEqual(diagnostic.return_value.start.call_count,expected)
                diagnostic.return_value.close.assert_called_once()
    def test_supervisor_fixed_command_watchdog_and_no_retry(self):
        child=Mock();child.poll.return_value=None;child.wait.return_value=-9
        spawn=Mock(return_value=child);timer=Mock();messages=[]
        runtime=SchemaDiagnosticSupervisor(enabled=True,popen=spawn,timer_factory=lambda _,callback:timer,
            monotonic=lambda:1.0,emit=lambda value,**kwargs:messages.append(json.loads(value)))
        runtime.start();runtime.start();self.assertEqual(spawn.call_count,1)
        self.assertEqual(Path(spawn.call_args.args[0][1]).name,'native_schema_diagnostic.py')
        self.assertEqual(spawn.call_args.args[0][-1],'--once');self.assertFalse(spawn.call_args.kwargs['shell'])
        runtime._expire();child.kill.assert_called_once();runtime.start();self.assertEqual(spawn.call_count,1)
        self.assertEqual(messages[-1]['mode'],'native_schema_diagnostic_supervisor')


if __name__=='__main__':unittest.main()
