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
    def setUp(self):
        self.probe = self.enterContext(patch.object(serve, 'NativeProbeSupervisor'))

    def exercise(self,status,stop_ticks=5):
        web=Child();worker=Child(status)
        with patch.object(serve,'validate_environment',return_value=('/tmp/mock-events.sqlite','8000')),patch.dict(serve.os.environ,{'ENABLE_AUTO_RESEARCH':'false'}),patch.object(serve.subprocess,'Popen',side_effect=[web,worker]) as popen,patch.object(serve.threading,'Event',return_value=Stop(stop_ticks)),patch.object(serve.signal,'signal'),patch.object(serve,'record_experiment_failure',create=True) as failure:
            result=serve.main()
        return result,worker,failure,popen

    def test_killed_worker_records_failure_once_without_respawn_or_web_failure(self):
        result,worker,failure,popen=self.exercise(-9)
        self.assertEqual(0,result)
        failure.assert_called_once_with(-9)
        self.assertEqual(2,popen.call_count)
        self.assertGreater(worker.polls,1)

    def test_positive_failure_exit_is_distinguished_from_success(self):
        _,_,failure,popen=self.exercise(2)
        failure.assert_called_once_with(2)
        self.assertEqual(2,popen.call_count)

    def test_normal_terminal_exit_never_reports_failure_or_respawns(self):
        _,_,failure,popen=self.exercise(0)
        failure.assert_not_called();self.assertEqual(2,popen.call_count)

    def test_signal_after_wait_before_report_suppresses_shutdown_alert(self):
        web=Child();worker=Child(-9);stop=Stop()
        original=worker.poll
        def interrupted_poll():
            stop.set()
            return original()
        with patch.object(serve,'validate_environment',return_value=('/tmp/mock-events.sqlite','8000')),patch.dict(serve.os.environ,{'ENABLE_AUTO_RESEARCH':'false'}),patch.object(serve.subprocess,'Popen',side_effect=[web,worker]),patch.object(serve.threading,'Event',return_value=stop),patch.object(serve.signal,'signal'),patch.object(worker,'poll',side_effect=interrupted_poll),patch.object(serve,'record_experiment_failure') as failure:
            self.assertEqual(0,serve.main())
        failure.assert_not_called()

    def test_intentional_supervisor_shutdown_does_not_report_termination(self):
        _,worker,failure,popen=self.exercise(None,1)
        failure.assert_not_called();self.assertEqual(-15,worker.status)
        self.assertEqual(2,popen.call_count)

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
