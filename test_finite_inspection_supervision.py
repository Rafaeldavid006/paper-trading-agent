"""Fixed proof child supervision with synthetic grants only; no subprocesses."""
from dataclasses import replace
from unittest.mock import Mock,patch
import unittest
import finite_pilot_contract as contract
from finite_pilot_supervisor import FinitePilotSupervisor

class Child:
    def __init__(self):self.code=None;self.kills=0
    def poll(self):return self.code
    def kill(self):self.kills+=1
    def wait(self,timeout=None):return self.code

class Timer:
    def __init__(self,interval,callback):self.interval=interval;self.callback=callback;self.daemon=False
    def start(self):pass
    def cancel(self):pass

class InspectionSupervisionTests(unittest.TestCase):
    def make(self,*,now=100,inspection=True,pilot=None):
        scope=contract.InspectionManifest(contract.INSPECTION_GENERATION,100*contract.NS,200*contract.NS,
            'a'*64,'b'*64,'SYNTHETIC-NOT-AUTHORITY') if inspection else None
        self.enterContext(patch.object(contract,'APPROVED_INSPECTION',scope))
        self.enterContext(patch.object(contract,'APPROVED_PILOT',pilot))
        self.enterContext(patch.object(contract,'APPROVED_RECOVERY',None))
        children=[];timers=[]
        def popen(*args,**kwargs):c=Child();children.append(c);return c
        def timer(*args):t=Timer(*args);timers.append(t);return t
        factory=Mock(side_effect=popen)
        supervisor=FinitePilotSupervisor(enabled=True,popen=factory,monotonic=lambda:0,
            wall_time_ns=lambda:now*contract.NS,timer_factory=timer,emit=Mock())
        self.addCleanup(supervisor.close)
        return supervisor,factory,children,timers,scope

    def test_none_does_not_launch_or_inspect(self):
        s,p,_,_,_=self.make(inspection=False);s.start();s.poll();s.close();p.assert_not_called()

    def test_exact_fixed_child_ten_second_deadline_and_no_followup(self):
        s,p,children,timers,_=self.make();s.start()
        self.assertEqual(p.call_args.args[0][-1],'--initialize-proof-state')
        self.assertTrue(p.call_args.args[0][-2].endswith('/finite_pilot_inspection.py'))
        self.assertEqual(timers[0].interval,10);self.assertTrue(timers[0].daemon)
        children[0].code=0;s.poll();self.assertTrue(s.completed)
        for _ in range(3):s.start();s.poll()
        self.assertEqual(p.call_count,1)

    def test_failed_or_uncertain_inspection_never_retries(self):
        for code in (1,-9,True):
            with self.subTest(code=code):
                s,p,children,_,_=self.make();s.start();children[0].code=code;s.poll();s.start();s.poll()
                self.assertTrue(s.halted);self.assertFalse(s.completed);self.assertEqual(p.call_count,1)

    def test_independent_timer_kills_without_main_poll(self):
        s,p,children,timers,_=self.make();s.start();timers[0].callback()
        self.assertEqual(children[0].kills,1);self.assertTrue(s.halted);s.poll();self.assertEqual(p.call_count,1)

    def test_future_and_expired_scope_never_launch(self):
        for now in (99,200):
            with self.subTest(now=now):
                s,p,_,_,_=self.make(now=now);s.start();s.poll();p.assert_not_called()

    def test_replaced_scope_and_mixed_scope_fail_closed(self):
        s,p,children,_,scope=self.make();s.start();contract.APPROVED_INSPECTION=replace(scope);s.poll()
        self.assertTrue(s.halted);self.assertEqual(children[0].kills,1)
        s,p,_,_,_=self.make(pilot=object());s.start();p.assert_not_called();self.assertTrue(s.halted)

if __name__=='__main__':unittest.main()
