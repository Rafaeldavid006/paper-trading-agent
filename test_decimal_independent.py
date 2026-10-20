import copy, hashlib, json, tempfile, unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D
from pathlib import Path
from decimal_lifecycle import Lifecycle, Proposal, LifecycleError
from alpaca_schema import timestamp, PAPER_ORIGIN, SchemaError
F=json.loads(Path(__file__).with_name('native-fixture.json').read_text())
FP=hashlib.sha256(F['account']['id'].encode()).hexdigest()
BASE=timestamp('2026-10-19T13:30:00Z')
def ts(sec): return f'2026-10-19T13:30:{sec:02d}Z'
def order(cid='entry',side='buy',qty='1',fill='0',status='new',sec=2,price='200.2'):
    o=copy.deepcopy(F['orders'][0]); o.update(id='broker-'+cid,client_order_id=cid,side=side,qty=qty,filled_qty=fill,filled_avg_price='200.2' if D(fill)>0 else None,limit_price=price,status=status,created_at=ts(1),submitted_at=ts(1),updated_at=ts(sec),canceled_at=ts(sec) if status=='canceled' else None,filled_at=ts(sec) if status=='filled' else None); return o
class Review(unittest.TestCase):
 def setUp(self):
    self.tmp=tempfile.TemporaryDirectory(); self.path=str(Path(self.tmp.name)/'ledger.sqlite'); self.l=Lifecycle(self.path,FP); self.obs()
 def tearDown(self): self.tmp.cleanup()
 def obs(self,sec=0,orders=None,qty=None,loss=False,**kw):
    a=copy.deepcopy(F['account']); a['cash']='100000'; a['equity']='98000' if loss else '100000'
    c=copy.deepcopy(F['clock']); c['timestamp']=ts(sec)
    p=[] if qty is None else [dict(F['positions'][0],qty=qty,qty_available=qty,market_value=str(D(qty)*D('200.2')))]
    q=copy.deepcopy(F['quote']); q['quote']['t']=ts(sec)
    args=dict(origin=PAPER_ORIGIN,response_account_fingerprint=FP,observed_ns=BASE+sec*10**9,now_ns=BASE+sec*10**9,collection_complete=True,quotes=q,quote_symbols=('AAPL',)); args.update(kw)
    return self.l.observe(a,c,p,orders or [],**args)
 def claim(self,cid='entry',side='buy',qty='1',sec=0,price='200.2',logical=None):
    return self.l.claim(Proposal(logical or cid,cid,'AAPL',side,D(qty),D(price)),now_ns=BASE+sec*10**9)
 def test_uncertain_restart(self):
    self.claim(); self.l=Lifecycle(self.path,FP)
    with self.assertRaises((LifecycleError,SchemaError)): self.claim('other')
    self.assertEqual(len(self.l.status()['intents']),1)
 def test_concurrent_once(self):
    def run(_):
      try:return Lifecycle(self.path,FP).claim(Proposal('entry','entry','AAPL','buy',D('1'),D('200.2')),now_ns=BASE)
      except (LifecycleError,SchemaError):return None
    with ThreadPoolExecutor(max_workers=8) as x: out=list(x.map(run,range(8)))
    self.assertEqual(sum(v is not None for v in out),1)
 def test_cancel_pending_then_exact_exit(self):
    self.claim(); self.obs(2,[order(fill='0.125000001',status='pending_cancel')],'0.125000001')
    with self.assertRaises((LifecycleError,SchemaError)):self.claim('exit','sell','.125000001',2)
    self.obs(3,[order(fill='0.125000001',status='canceled',sec=3)],'0.125000001')
    self.assertFalse(self.claim('exit','sell','0.125000001',3).executable)
 def test_entry_loss_allows_owned_exit(self):
    self.claim(); self.obs(2,[order(fill='1',status='filled')],'1',loss=True)
    self.assertFalse(self.claim('exit','sell','1',2).executable)
 def test_wrong_residual_blocked(self):
    self.claim(); self.obs(2,[order(fill='0.125000001',status='canceled')],'0.125000001')
    with self.assertRaises((LifecycleError,SchemaError)): self.claim('exit','sell','0.125',2)
 def test_partial_exit_then_residual(self):
    self.claim(); e=order(fill='1',status='filled'); self.obs(2,[e],'1'); self.claim('exit','sell','1',2)
    x=order('exit','sell',fill='0.333333333',status='canceled',sec=4);x['submitted_at']=x['created_at']=ts(3)
    self.obs(4,[e,x],'0.666666667'); self.assertFalse(self.claim('exit2','sell','0.666666667',4).executable)
 def test_stale_claim(self):
    with self.assertRaises((LifecycleError,SchemaError)):self.claim(sec=11)
 def test_wrong_account_latches(self):
    with self.assertRaises(LifecycleError):self.obs(1,response_account_fingerprint='0'*64)
    self.assertEqual(self.l.status()['attention'],1)
 def test_old_order_cannot_be_adopted(self):
    self.claim(sec=2); o=order(); self.obs_error(3,[o])
 def obs_error(self,sec,orders):
    with self.assertRaises(LifecycleError):self.obs(sec,orders)
    self.assertEqual(self.l.status()['attention'],1)
 def test_mutated_identity_latches(self):
    self.claim();o=order();self.obs(2,[o]);o['qty']='2';o['updated_at']=ts(3);self.obs_error(3,[o])
 def test_stop_survives_restart(self):
    self.l.stop();self.l=Lifecycle(self.path,FP)
    with self.assertRaises((LifecycleError,SchemaError)):self.claim()
 def test_future_quote(self):
    q=copy.deepcopy(F['quote']);q['quote']['t']=ts(2);self.obs(1,quotes=q)
    with self.assertRaises((LifecycleError,SchemaError)):self.claim(sec=1)
 def test_far_limit(self):
    with self.assertRaises((LifecycleError,SchemaError)):self.claim(price='999')
 def test_subpenny_limit_rejected(self):
    with self.assertRaises((LifecycleError,SchemaError)):self.claim(price='200.20001')
 def test_preexisting_other_symbol_client_id(self):
    o=order(fill='1',status='filled');o['symbol']='MSFT';self.obs(2,[o])
    with self.assertRaises((LifecycleError,SchemaError)):self.claim(sec=2)
 def test_terminal_resurrection_latches(self):
    self.claim();o=order(fill='0.125000001',status='canceled');self.obs(2,[o],'0.125000001')
    o['status']='partially_filled';o['canceled_at']=None;o['updated_at']=ts(3);self.obs_error(3,[o])
 def test_pending_exit_reserves(self):
    self.claim();e=order(fill='1',status='filled');self.obs(2,[e],'1');self.claim('exit','sell','1',2)
    x=order('exit','sell',fill='0.125000001',status='pending_cancel',sec=4);x['created_at']=x['submitted_at']=ts(3)
    self.obs(4,[e,x],'0.874999999')
    with self.assertRaises((LifecycleError,SchemaError)):self.claim('exit2','sell','0.874999999',4)
 def test_missing_owned_order_blocks(self):
    self.claim();self.obs(2)
    with self.assertRaises((LifecycleError,SchemaError)):self.claim('other',sec=2)
 def test_manual_quantity_not_adopted(self):
    self.claim();self.obs(2,[order(fill='1',status='filled')],'2')
    with self.assertRaises((LifecycleError,SchemaError)):self.claim('exit','sell','2',2)
 def test_account_rebind_rejected(self):
    with self.assertRaises(LifecycleError):Lifecycle(self.path,'0'*64)
 def test_loss_latch_recovery_not_reenable(self):
    self.obs(1,loss=True);self.obs(2)
    with self.assertRaises((LifecycleError,SchemaError)):self.claim(sec=2)
 def test_external_position_blocks_entry(self):
    self.obs(1,qty='1')
    with self.assertRaises((LifecycleError,SchemaError)):self.claim(sec=1)
 def test_cancel_uncertain_restart(self):
    self.claim();self.obs(2,[order()]);r=self.l.claim_cancel('entry',now_ns=BASE+2*10**9);self.assertFalse(r.executable)
    self.l=Lifecycle(self.path,FP)
    with self.assertRaises((LifecycleError,SchemaError)):self.l.claim_cancel('entry',now_ns=BASE+2*10**9)
    with self.assertRaises((LifecycleError,SchemaError)):self.claim('other',sec=2)
 def test_cancel_fill_race(self):
    self.claim();self.obs(2,[order()]);self.l.claim_cancel('entry',now_ns=BASE+2*10**9)
    self.obs(3,[order(fill='1',status='filled',sec=3)],'1');self.assertFalse(self.claim('exit','sell','1',3).executable)
 def test_cancel_external_rejected(self):
    self.obs(2,[order('external')])
    with self.assertRaises((LifecycleError,SchemaError)):self.l.claim_cancel('external',now_ns=BASE+2*10**9)
 def test_cancel_after_stop_rejected(self):
    self.claim();self.obs(2,[order()]);self.l.stop()
    with self.assertRaises((LifecycleError,SchemaError)):self.l.claim_cancel('entry',now_ns=BASE+2*10**9)
if __name__=='__main__':unittest.main(verbosity=2)
