"""Admission GETs only, with durable final-wire reservations; no redirects/retries."""
import prepared_successor_contract as c
from native_observation_collector import PAPER_ORIGIN,DATA_ORIGIN,ORDER_PATH,_route
from native_paper_execution import NativePaperHTTP
from finite_pilot_transport import _FinalOpener
from alpaca_schema import parse_order
class Transport:
    paper_origin=PAPER_ORIGIN;data_origin=DATA_ORIGIN;feed='iex'
    def __init__(self,key,secret,*,grant,journal,now_ns,started_ns,controls,test_only=False,transport_factory=None):
        c.approved(grant);c.require(type(test_only) is bool and (test_only or transport_factory is None),'successor_factory_override')
        self.grant,self.j,self.now,self.started,self.controls=grant,journal,now_ns,started_ns,controls
        self.request_count=0;self.pending=None;self.opened=False;self.scan=[];self.last=started_ns
        self.wire=(transport_factory or NativePaperHTTP)(key,secret,enabled=True)
        self.native=type(self.wire) is NativePaperHTTP;c.require(test_only or self.native,'successor_native_transport_required')
        if self.native:
            self.wire._opener=_FinalOpener(self.wire._opener,self._open)
            self.wire._reader._opener=_FinalOpener(self.wire._reader._opener,self._open)
    def window(self):
        self.last=c.window(self.grant,self.now(),self.last)
        c.require(0<=self.last-self.started<=10*c.NS,'successor_collection_expired')
    def at_wire(self,invoke):
        self.window();self.controls();c.require(self.pending is not None and not self.opened,'successor_single_open')
        self.j.reserve(self.pending);self.request_count+=1;self.opened=True
        self.window();self.controls();return invoke()
    def _open(self,opener,req,*args,**kwargs):
        c.require(self.pending is not None and req.get_method()=='GET' and req.data is None and req.full_url==_route(self.pending)+self.pending,'successor_get_only')
        return self.at_wire(lambda:opener.open(req,*args,**kwargs))
    def call(self,method,path,body=None):
        c.require(method=='GET' and body is None and self.pending is None,'successor_get_only');_route(path)
        c.require(not path.startswith('/v2/stocks/') or path=='/v2/stocks/quotes/latest?symbols=NVDA&feed=iex','successor_quote_scope')
        self.window();c.require(self.request_count<self.grant.max_gets,'successor_get_cap');self.pending=path;self.opened=False
        try:
            result=self.wire.call('GET',path) if self.native else self.at_wire(lambda:self.wire.call('GET',path))
            if path.startswith(ORDER_PATH) and result[0]==200:
                c.require(type(result[1]) is list and len(self.scan)+len(result[1])<500,'successor_order_bound');self.scan.extend(result[1])
            return result
        finally:self.pending=None
    def legacy_terminal_digests(self,ids):
        from native_account_guard import _order_digest
        rows=[(r['client_order_id'],_order_digest(parse_order(r),r)) for r in self.scan if r.get('client_order_id') in ids]
        c.require(len(rows)==len(ids) and {r[0] for r in rows}==set(ids),'successor_legacy_missing');return tuple(sorted(rows))
