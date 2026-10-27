"""GET-only at API AND final opener; no dispatcher or order action capability."""
import finite_observation_contract as c
from native_paper_execution import NativePaperHTTP
from native_observation_collector import PAPER_ORIGIN,DATA_ORIGIN,_route
from finite_pilot_transport import _FinalOpener
class ObservationTransport:
    paper_origin=PAPER_ORIGIN;data_origin=DATA_ORIGIN;feed='iex'
    def __init__(self,key,secret,*,grant,now_ns,started_ns,reserve,test_only=False,transport_factory=None):
        c.approved(grant)
        c.require(type(test_only) is bool and (test_only or transport_factory is None),'factory_override_forbidden')
        self.grant,self.now_ns,self.started_ns,self.reserve=grant,now_ns,started_ns,reserve
        self.request_count=0;self._pending=None;self._opened=False;self._last=started_ns
        self.__transport=(transport_factory or NativePaperHTTP)(key,secret,enabled=True)
        native=type(self.__transport) is NativePaperHTTP
        c.require(test_only or native,'reviewed_transport_required')
        if native:
            self.__transport._opener=_FinalOpener(self.__transport._opener,self._open)
            self.__transport._reader._opener=_FinalOpener(self.__transport._reader._opener,self._open)
    def _window(self):
        now=c.window(self.grant,self.now_ns(),self._last)
        c.require(0<=now-self.started_ns<=10*c.NS,'collection_expired');self._last=now
    def _open(self,opener,req,*args,**kwargs):
        c.require(self._pending is not None and not self._opened and req.get_method()=='GET' and req.data is None and req.full_url==_route(self._pending)+self._pending,'get_only')
        self._window();self._opened=True
        return opener.open(req,*args,**kwargs)
    def call(self,method,path,body=None):
        c.require(method=='GET' and body is None and self._pending is None,'get_only')
        _route(path)
        c.require(not path.startswith('/v2/stocks/') or path=='/v2/stocks/quotes/latest?symbols=NVDA&feed=iex','quote_scope')
        self._window()
        c.require(self.request_count<self.grant.max_gets_per_cycle,'cycle_get_cap')
        self.reserve() # Commit observation quota before HTTP, no refunds.
        self.request_count+=1;self._pending=path;self._opened=False
        try:return self.__transport.call('GET',path)
        finally:self._pending=None
