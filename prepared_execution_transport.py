"""Final-opener PAPER fence and durable shared/inherited finite request accounting."""
from decimal import Decimal
import copy,json,re
import prepared_execution_contract as c
from native_paper_execution import NativePaperHTTP,NativePaperDispatcher
from native_observation_collector import PAPER_ORIGIN,DATA_ORIGIN,ORDER_PATH,_route
from finite_pilot_transport import _FinalOpener,_read
from alpaca_schema import parse_order
class Transport:
    paper_origin=PAPER_ORIGIN;data_origin=DATA_ORIGIN;feed='iex'
    def __init__(self,key,secret,*,grant,journal,test_only=False,transport_factory=None):
        c.approved(grant);c.require(type(test_only) is bool and (test_only or transport_factory is None),'execution_factory_override')
        self.grant,self.j=grant,journal;self.pending=None;self.opened=False;self.coordinator=None;self.scan=[];self.request_count=0
        self.wire=(transport_factory or NativePaperHTTP)(key,secret,enabled=True)
        self.native=type(self.wire) is NativePaperHTTP;c.require(test_only or self.native,'execution_native_transport_required')
        if self.native:
            self.wire._opener=_FinalOpener(self.wire._opener,self._open);self.wire._reader._opener=_FinalOpener(self.wire._reader._opener,self._open)
    def bind(self,coordinator):
        from prepared_execution_worker import Coordinator
        c.require(type(coordinator) is Coordinator and self.coordinator is None and type(coordinator.dispatch) is NativePaperDispatcher and coordinator.dispatch.wire.transport is self and coordinator.j is self.j,'execution_binding_invalid')
        self.coordinator=coordinator
    def native_attempt(self,method,path,body):
        co=self.coordinator;c.require(co is not None,'execution_dispatcher_unbound');d=co.dispatch
        d.guard.assert_write_allowed();d._gate();self.j.active(write=True)
        if method=='POST':
            NativePaperHTTP.validate_payload(body)
            c.require(path=='/v2/orders' and body['symbol']=='NVDA' and body['client_order_id']==c.cid(self.grant,body['side']),'execution_payload_scope')
            cid,kind,target=body['client_order_id'],'submit',body['client_order_id']
        else:
            raw=d.observation.arguments();matches=[o for o in raw['orders'] if '/v2/orders/'+o['id']==path]
            c.require(method=='DELETE' and body is None and len(matches)==1,'execution_cancel_owned')
            cid,kind,target=matches[0]['client_order_id'],'cancel',path
            c.require(cid in (c.cid(self.grant,'buy'),c.cid(self.grant,'sell')),'execution_cancel_owned')
        with _read(d.lifecycle.path) as db:
            c.require(db.execute('SELECT stopped,attention FROM control').fetchall()==[(0,0)],'execution_existing_attention')
            c.require(db.execute('SELECT state FROM native_dispatch WHERE kind=? AND cid=?',(kind,cid)).fetchall()==[('uncertain',)],'execution_committed_uncertainty')
            rows=db.execute('SELECT symbol,side,quantity,price,response FROM intents WHERE cid=?',(cid,)).fetchall()
            c.require(len(rows)==1 and rows[0][0]=='NVDA','execution_owned_claim')
            if method=='POST':
                symbol,side,qty,price,response=rows[0]
                c.require(response is None and (symbol,side,Decimal(qty),Decimal(price))==(body['symbol'],body['side'],Decimal(body['qty']),Decimal(body['limit_price'])),'execution_claim_payload_changed')
            else:c.require(db.execute('SELECT COUNT(*) FROM cancels WHERE cid=?',(cid,)).fetchone()==(1,),'execution_cancel_claim')
        co.final_write_check(method,path,body);return target
    def _at_wire(self,invoke):
        c.require(self.pending is not None and not self.opened,'execution_single_open');self.opened=True
        method,path,body=self.pending;self.j.check();st=self.j.state()
        if st['applied']:self.j.active()
        else:c.window(self.grant,self.j.now(),self.j.last,admitting=True)
        target=path if method=='GET' else self.native_attempt(method,path,body)
        self.j.reserve(method,target);self.request_count+=1
        self.j.check()
        if method=='GET':return invoke()
        # Committed reservation remains even if final controls or network fail.
        with self.j.tx():
            self.j.active(write=True);self.native_attempt(method,path,body)
            self.j.checkpoint('execution_before_write_wire');return invoke()
    def _open(self,opener,req,*args,**kwargs):
        c.require(self.pending is not None,'execution_request_scope');method,path,body=self.pending
        origin=DATA_ORIGIN if path.startswith('/v2/stocks/') else PAPER_ORIGIN
        payload=None if body is None else json.dumps(body,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        c.require(req.get_method()==method and req.full_url==origin+path and req.data==payload,'execution_prepared_request_changed')
        return self._at_wire(lambda:opener.open(req,*args,**kwargs))
    def call(self,method,path,body=None):
        c.require(self.pending is None and method in ('GET','POST','DELETE'),'execution_method_or_reentry')
        if method=='GET':
            c.require(body is None,'execution_get_body')
            if path!='/v2/assets/NVDA':_route(path)
            c.require(not path.startswith('/v2/stocks/') or path=='/v2/stocks/quotes/latest?symbols=NVDA&feed=iex','execution_quote_only')
        elif method=='POST':c.require(path=='/v2/orders','execution_post_route');NativePaperHTTP.validate_payload(body)
        else:c.require(body is None and type(path) is str and re.fullmatch('/v2/orders/[A-Za-z0-9_-]{1,128}',path),'execution_delete_route')
        self.pending=(method,path,copy.deepcopy(body));self.opened=False
        try:
            result=self.wire.call(method,path,body) if self.native else self._at_wire(lambda:self.wire.call(method,path,body))
            if not self.j.state()['applied'] and path.startswith(ORDER_PATH) and result[0]==200:
                c.require(type(result[1]) is list and len(self.scan)+len(result[1])<500,'execution_history_bound');self.scan.extend(result[1])
            return result
        finally:self.pending=None
    def legacy_terminal_digests(self,ids):
        from native_account_guard import _order_digest
        rows=[(r['client_order_id'],_order_digest(parse_order(r),r)) for r in self.scan if r.get('client_order_id') in ids]
        c.require(len(rows)==len(ids) and {r[0] for r in rows}==set(ids),'execution_legacy_missing');return tuple(sorted(rows))
