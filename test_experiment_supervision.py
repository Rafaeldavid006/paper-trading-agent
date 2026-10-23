"""No-process/no-network regression for one-shot child failure supervision."""
import unittest
import sqlite3
import tempfile
import json
from pathlib import Path
from unittest.mock import patch
import serve

class Child:
    def __init__(self,status=None):self.status=status;self.polls=0
    def poll(self):self.polls+=1;return self.status
    def terminate(self):self.status=-15
    def kill(self):self.status=-9
    def wait(self,timeout=None):return self.status

class Stop:
    def __init__(self,ticks=5):self.ticks=ticks
    def wait(self,_):self.ticks-=1;return self.ticks<=0
    def set(self):self.ticks=0
    def is_set(self):return self.ticks<=0

class SupervisionTests(unittest.TestCase):
    """Current readonly service never starts the historical renewal worker."""
    def setUp(self):
        self.enterContext(patch.object(serve, 'run_startup_gate', return_value=True))
        self.probe = self.enterContext(patch.object(serve, 'NativeProbeSupervisor'))
        self.readonly = self.enterContext(patch.object(serve, 'ReadonlyRuntimeSupervisor'))
        self.enterContext(patch.object(serve, 'validate_environment', return_value=('/tmp/mock-events.sqlite','8000')))
        self.enterContext(patch.object(serve.signal, 'signal'))
        self.failure=self.enterContext(patch.object(serve,'record_experiment_failure'))

    def exercise(self, web, stop_ticks=5):
        with patch.object(serve.subprocess,'Popen',return_value=web) as popen, \
             patch.object(serve.threading,'Event',return_value=Stop(stop_ticks)):
            result=serve.main()
        self.failure.assert_not_called();self.probe.assert_not_called();self.readonly.assert_not_called()
        popen.assert_called_once();self.assertEqual(popen.call_args.args[0][-1],'inspection_wsgi:app')
        return result

    def test_healthy_web_lives_until_shutdown_without_legacy_worker(self):
        web=Child();self.assertEqual(self.exercise(web),0);self.assertEqual(web.status,-15)

    def test_web_death_returns_failure_without_restart(self):
        for status in (0,1,-9):
            with self.subTest(status=status):self.assertEqual(self.exercise(Child(status)),1)

    def test_shutdown_reap_is_bounded_even_if_kill_is_uncertain(self):
        import subprocess
        from unittest.mock import Mock
        web=Mock();web.poll.return_value=None
        web.wait.side_effect=subprocess.TimeoutExpired('FIXED',1)
        with patch('builtins.print') as emit:
            self.assertEqual(self.exercise(web,1),0)
        web.terminate.assert_called_once();web.kill.assert_called_once()
        self.assertEqual([c.kwargs['timeout'] for c in web.wait.call_args_list],[10,1])
        self.assertEqual(json.loads(emit.call_args.args[0])['reason'],'web_reap_pending')

class FailureRecordingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/'state.sqlite');self.messages=[]
        with sqlite3.connect(self.path) as c:
            c.execute('CREATE TABLE paper_experiment(id INTEGER,phase TEXT,reason TEXT,updated REAL,buy_id TEXT,sell_id TEXT)')
            c.execute("INSERT INTO paper_experiment VALUES(1,'buy_working','reconciling',1,'private-buy-id',NULL)")
            c.execute('CREATE TABLE intents(client_id TEXT,state TEXT,filled TEXT)')
            c.execute("INSERT INTO intents VALUES('private-buy-id','uncertain','0.4')")

    def report(self,path=None):
        serve.record_experiment_failure(-9,database=path or self.path,emit=lambda text,**kwargs:self.messages.append(json.loads(text)))

    def test_failure_marks_attention_without_modifying_order_evidence(self):
        self.report()
        with sqlite3.connect(self.path) as c:
            self.assertEqual(('manual_attention','worker_failed','private-buy-id',None),c.execute('SELECT phase,reason,buy_id,sell_id FROM paper_experiment').fetchone())
            self.assertEqual([('private-buy-id','uncertain','0.4')],c.execute('SELECT * FROM intents').fetchall())
        self.assertTrue(self.messages[0]['attention_recorded'])
        self.assertNotIn('private-buy-id',json.dumps(self.messages))

    def test_all_terminal_outcomes_preserved(self):
        for phase in ('completed','no_fill','blocked','manual_attention','stopped'):
            with sqlite3.connect(self.path) as c:c.execute('UPDATE paper_experiment SET phase=?,reason=?',(phase,'original'))
            self.report()
            with sqlite3.connect(self.path) as c:self.assertEqual((phase,'original'),c.execute('SELECT phase,reason FROM paper_experiment').fetchone())
            self.assertFalse(self.messages[-1]['attention_recorded'])

    def test_missing_or_broken_storage_logs_without_creating_reset(self):
        absent=str(Path(self.tmp.name)/'absent.sqlite')
        self.report(absent)
        self.assertFalse(Path(absent).exists())
        self.assertEqual({'mode':'paper_one_shot_supervisor','reason':'worker_failed','exit_code':-9,'attention_recorded':False},self.messages[-1])
        with patch.object(serve.sqlite3,'connect',side_effect=RuntimeError('private-sensitive-detail')):self.report()
        self.assertNotIn('private-sensitive-detail',json.dumps(self.messages))

    def test_symlink_not_followed(self):
        link=Path(self.tmp.name)/'link.sqlite';link.symlink_to(self.path)
        self.report(str(link))
        with sqlite3.connect(self.path) as c:self.assertEqual('buy_working',c.execute('SELECT phase FROM paper_experiment').fetchone()[0])

if __name__=='__main__':unittest.main()
