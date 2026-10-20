from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
import unittest
import types

_cases=[]
class _Check:
    @staticmethod
    def raises(kind,match=None):
        return unittest.TestCase().assertRaisesRegex(kind,match) if match else unittest.TestCase().assertRaises(kind)
    class mark:
        @staticmethod
        def parametrize(name,values):
            def wrap(fn):
                for i,value in enumerate(values):
                    _cases.append((fn.__name__+str(i),lambda fn=fn,value=value:fn(value)))
                return None
            return wrap
checks=_Check
from shadow_market_inputs import *

NOW=datetime(2026,10,19,14,0,10,tzinfo=timezone.utc)
START=NOW.replace(minute=58,second=0,hour=13)
END=NOW.replace(second=0)

def bars():return {'symbol':'AAPL','bars':[{'t':(START+timedelta(minutes=i)).isoformat(),'c':100+i} for i in range(2)],'next_page_token':None}
def parse(p):return parse_bars(p,'AAPL',start=START,end=END,received_at=NOW,now=NOW)
def quote():return {'symbol':'AAPL','quote':{'bp':100,'ap':100.1,'t':NOW.isoformat()}}

def test_native_bar_receipt_not_end():
    b=parse(bars());assert b[-1].available_at==NOW and b[-1].end==END

@checks.mark.parametrize('mutation',[lambda p:p.update(symbol='SPY'),lambda p:p.update(next_page_token='next'),lambda p:p.update(bars=[]),lambda p:p['bars'].reverse(),lambda p:p['bars'][0].update(c=True),lambda p:p['bars'][0].update(c='NaN'),lambda p:p['bars'][1].update(t=START.isoformat())])
def test_bad_bars(mutation):
    p=bars();mutation(p)
    with checks.raises(SafetyError):parse(p)

def test_future_receipt():
    with checks.raises(SafetyError):parse_bars(bars(),'AAPL',start=START,end=END,received_at=NOW+timedelta(seconds=1),now=NOW)

def test_quote_valid():assert parse_quote(quote(),'AAPL',received_at=NOW,now=NOW).ask=='100.1'

@checks.mark.parametrize('change',[{'bp':0},{'ap':99},{'ap':101},{'t':(NOW-timedelta(seconds=11)).isoformat()},{'t':'2026-10-19T14:00:10.000000001Z'}])
def test_quote_reject(change):
    p=quote();p['quote'].update(change)
    with checks.raises(SafetyError):parse_quote(p,'AAPL',received_at=NOW,now=NOW)

def clock():return dict(timestamp=NOW.isoformat(),is_open=True,next_open='2026-10-20T13:30:00Z',next_close='2026-10-19T20:00:00Z')
def calendar():return [dict(date='2026-10-19',open='09:30',close='16:00')]
def test_session():assert parse_session(clock(),calendar(),received_at=NOW,now=NOW).open.hour==13
@checks.mark.parametrize('change',[{'is_open':False},{'is_open':1},{'next_close':'2026-10-19T21:00:00Z'},{'timestamp':'2026-10-19T13:59:00Z'}])
def test_clock_reject(change):
    c=clock();c.update(change)
    with checks.raises(SafetyError):parse_session(c,calendar(),received_at=NOW,now=NOW)

def test_disabled_no_calls():
    http=ShadowMarketHTTP('fixture-key','fixture-secret');http._opener=Mock()
    with checks.raises(SafetyError,match='disabled'):http.clock()
    http._opener.open.assert_not_called()

def test_http_budget_and_fixed_get():
    http=ShadowMarketHTTP('fixture-key','fixture-secret',enabled=True,monotonic=lambda:0)
    response=Mock();response.geturl.return_value=PAPER+'/v2/clock';response.status=200;response.read.return_value=b'{}'
    http._opener=Mock();http._opener.open.return_value.__enter__=Mock(return_value=response);http._opener.open.return_value.__exit__=Mock(return_value=False)
    for _ in range(12):assert http.clock()=={}
    assert http._opener.open.call_args[0][0].get_method()=='GET'
    with checks.raises(SafetyError,match='budget'):http.clock()
    assert http._opener.open.call_count==12

def test_no_arbitrary_routes():
    http=ShadowMarketHTTP('fixture-key','fixture-secret',enabled=True)
    with checks.raises(SafetyError,match='invalid_route'):http._get(PAPER,'/v2/orders')

@checks.mark.parametrize('symbol',['../orders','TSLA','AAPL?feed=sip'])
def test_symbol_allowlist(symbol):
    with checks.raises(SafetyError):ShadowMarketHTTP('x','y').quote(symbol)

def test_bar_request_excludes_partial_and_sip():
    http=ShadowMarketHTTP('x','y');http._get=Mock(return_value={})
    http.bars('AAPL',START,END)
    host,path=http._get.call_args[0]
    assert host==DATA and 'feed=iex' in path and '13%3A59%3A59.999999' in path


class MarketInputTests(unittest.TestCase):
    pass
for _name,_fn in list(globals().items()):
    if _name.startswith("test_") and callable(_fn):
        setattr(MarketInputTests,_name,lambda self,fn=_fn:fn())
for _name,_fn in _cases:
    setattr(MarketInputTests,_name,lambda self,fn=_fn:fn())
if __name__ == "__main__":unittest.main()
