import copy
import hashlib
import json
from pathlib import Path
from decimal import Decimal as D
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal_lifecycle import Lifecycle, Proposal, LifecycleError
from alpaca_schema import PAPER_ORIGIN, timestamp, SchemaError

FIXTURE=json.loads((Path(__file__).parent/'fixtures'/'alpaca-native.json').read_text())
FP=hashlib.sha256(FIXTURE['account']['id'].encode()).hexdigest()
BASE=timestamp('2026-10-19T13:31:01Z')

class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.path=self.tmp.name+'/ledger.db'
        self.l=Lifecycle(self.path,FP)
        self.observe()
    def tearDown(self): self.tmp.cleanup()
    def observe(self, orders=None, quantity=None, offset=0, equity='100000', **kwargs):
        f=copy.deepcopy(FIXTURE); t=f'2026-10-19T13:31:{1+offset:02d}Z'
        f['clock']['timestamp']=t; f['account']['equity']=equity
        f['quote']['quote']['t']=t
        pos=[] if quantity is None else [dict(f['positions'][0],qty=quantity,qty_available=quantity)]
        args=dict(origin=PAPER_ORIGIN,response_account_fingerprint=FP,observed_ns=BASE+offset*10**9,now_ns=BASE+offset*10**9,collection_complete=True,quotes=f['quote'],quote_symbols=('AAPL',))
        args.update(kwargs)
        return self.l.observe(f['account'],f['clock'],pos,orders or [],**args)
    def proposal(self, side='buy', qty='1', cid='demo-entry', key=None, price='200.2'):
        return Proposal(key or cid,cid,'AAPL',side,D(qty),D(price))
    def order(self,cid='demo-entry',side='buy',qty='1',fill='0.125000001',status='canceled',offset=1,price='200.2'):
        r=copy.deepcopy(FIXTURE['orders'][0]); t=f'2026-10-19T13:31:{1+offset:02d}Z'
        r.update(id=cid+'-provider',client_order_id=cid,side=side,qty=qty,filled_qty=fill,status=status,limit_price=price,filled_avg_price=price if D(fill)>0 else None,created_at='2026-10-19T13:31:01Z',submitted_at='2026-10-19T13:31:01Z',updated_at=t,canceled_at=t if status=='canceled' else None,filled_at=t if status=='filled' else None)
        return r
    def test_claim_is_nonexecutable_and_durable(self):
        r=self.l.claim(self.proposal(),now_ns=BASE)
        self.assertFalse(r.executable); self.assertEqual(r.state,'uncertain')
        self.assertEqual(len(Lifecycle(self.path,FP).status()['intents']),1)
    def test_duplicate(self):
        self.l.claim(self.proposal(),now_ns=BASE)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(),now_ns=BASE)
    def test_changed_cid_same_logic(self):
        self.l.claim(self.proposal(key='episode'),now_ns=BASE)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(cid='other',key='episode'),now_ns=BASE)
    def test_missing_order_blocks_restart(self):
        self.l.claim(self.proposal(),now_ns=BASE); self.observe(offset=1)
        self.l=Lifecycle(self.path,FP)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(cid='new'),now_ns=BASE+10**9)
    def test_partial_entry_exact_exit(self):
        self.l.claim(self.proposal(),now_ns=BASE)
        self.observe([self.order()],quantity='0.125000001',offset=1)
        self.l.claim(self.proposal('sell','0.125000001','exit'),now_ns=BASE+10**9)
        self.assertEqual(self.l.status()['intents'][1]['quantity'],'0.125000001')
    def test_partial_exit_remainder(self):
        self.l.claim(self.proposal(),now_ns=BASE)
        entry=self.order(); self.observe([entry],quantity='0.125000001',offset=1)
        self.l.claim(self.proposal('sell','0.125000001','exit'),now_ns=BASE+10**9)
        ex=self.order('exit','sell','0.125000001','0.025000001',offset=2)
        ex['submitted_at']='2026-10-19T13:31:02Z'
        self.observe([entry,ex],quantity='0.100000000',offset=2)
        self.l.claim(self.proposal('sell','0.100000000','exit2'),now_ns=BASE+2*10**9)
    def test_wrong_remainder(self):
        self.l.claim(self.proposal(),now_ns=BASE); self.observe([self.order()],quantity='0.125000001',offset=1)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal('sell','0.12','exit'),now_ns=BASE+10**9)
    def test_pending_cancel(self):
        self.l.claim(self.proposal(),now_ns=BASE); self.observe([self.order(status='pending_cancel')],quantity='0.125000001',offset=1)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal('sell','0.125000001','exit'),now_ns=BASE+10**9)
    def test_replace_latches(self):
        self.l.claim(self.proposal(),now_ns=BASE); r=self.order(); r['replaced_by']='other'
        with self.assertRaises(LifecycleError): self.observe([r],offset=1)
        self.assertEqual(self.l.status()['attention'],1)
    def test_fill_regression_latches(self):
        self.l.claim(self.proposal(),now_ns=BASE); self.observe([self.order(status='partially_filled')],quantity='0.125000001',offset=1)
        with self.assertRaises(LifecycleError): self.observe([self.order(fill='0.1',status='partially_filled',offset=2)],quantity='0.1',offset=2)
        self.assertEqual(self.l.status()['attention'],1)
    def test_wrong_binding_latches(self):
        with self.assertRaises(LifecycleError): self.observe(offset=1,response_account_fingerprint='f'*64)
        self.assertEqual(self.l.status()['attention'],1)
    def test_constructor_wrong_binding(self):
        with self.assertRaises(LifecycleError): Lifecycle(self.path,'f'*64)
    def test_daily_loss_blocks_entry(self):
        self.observe(offset=1,equity='99000')
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(),now_ns=BASE+10**9)
    def test_daily_recovery_stays_latched(self):
        self.observe(offset=1,equity='99000'); self.observe(offset=2)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(),now_ns=BASE+2*10**9)
    def test_loss_allows_reduction(self):
        self.l.claim(self.proposal(),now_ns=BASE); self.observe([self.order()],quantity='0.125000001',offset=1,equity='98000')
        self.l.claim(self.proposal('sell','0.125000001','exit'),now_ns=BASE+10**9)
    def test_stale(self):
        with self.assertRaises(SchemaError): self.l.claim(self.proposal(),now_ns=BASE+11*10**9)
    def test_far_limit(self):
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(price='1000'),now_ns=BASE)
    def test_stopped_reduction_also_blocked(self):
        self.l.stop()
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(),now_ns=BASE)
    def test_preexisting_claim_rejected(self):
        self.observe([self.order()],quantity='0.125000001',offset=1)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(),now_ns=BASE+10**9)
    def test_order_predates_claim(self):
        self.l.claim(self.proposal(),now_ns=BASE); r=self.order(); r['submitted_at']='2026-10-19T13:30:59Z'; r['created_at']=r['submitted_at']
        with self.assertRaises(LifecycleError): self.observe([r],offset=1)
    def test_cancel_claim_is_once_and_uncertain(self):
        self.l.claim(self.proposal(),now_ns=BASE)
        self.observe([self.order(status='partially_filled')],quantity='0.125000001',offset=1)
        r=self.l.claim_cancel('demo-entry',now_ns=BASE+10**9)
        self.assertFalse(r.executable)
        with self.assertRaises(LifecycleError): Lifecycle(self.path,FP).claim_cancel('demo-entry',now_ns=BASE+10**9)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal('sell','0.125000001','exit'),now_ns=BASE+10**9)
        self.observe([self.order(offset=2)],quantity='0.125000001',offset=2)
        self.l.claim(self.proposal('sell','0.125000001','exit'),now_ns=BASE+2*10**9)
    def test_unowned_cancel_rejected(self):
        with self.assertRaises(LifecycleError): self.l.claim_cancel('other',now_ns=BASE)
    def test_bad_price_tick(self):
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(price='200.20001'),now_ns=BASE)
    def test_provider_client_collision_other_symbol(self):
        r=self.order(); r['symbol']='MSFT'
        self.observe([r],offset=1)
        with self.assertRaises(LifecycleError): self.l.claim(self.proposal(),now_ns=BASE+10**9)
    def test_concurrent_claim(self):
        def one(_):
            try: return Lifecycle(self.path,FP).claim(self.proposal(),now_ns=BASE).executable
            except LifecycleError: return 'blocked'
        with ThreadPoolExecutor(max_workers=4) as ex: values=list(ex.map(one,range(4)))
        self.assertEqual(values.count(False),1); self.assertEqual(values.count('blocked'),3)

if __name__=='__main__': unittest.main()
