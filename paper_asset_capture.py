"""Disabled, fixed-window public-asset capture; no trading or source authority."""
import base64
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import stat
import sys
import time
from urllib import request, error
import uuid

CAPTURE_ENABLED = True
START_NS = 1793545200000000000
END_NS = 1793545800000000000
SYMBOLS = ('MSFT', 'JPM', 'XOM')
ORIGIN = 'https://paper-api.alpaca.markets'
MARKER = '/var/data/paper-asset-capture-20261101-v1.jsonl'
MAX_SECONDS = 20
MAX_BODY = 8192
MAX_EXPORT = 49152
CHUNK = 2048  # base64 + closed framing remains below 4096 bytes
CAPTURE_ID = 'paper-assets-20261101-msft-jpm-xom-v1'
NATIVE_PINS = {'runtime_state_bundle': 'e29c26dade85df66d88a7b9ea66040337932ffd8eea8d7db0bf702b480f852b6', 'native_account_guard': '2281a8c753582a08c5d832537fbe022ad088a99790d7d206f9c259292e2ff7d5', 'native_existing_budget': '828d531e391a5599d34c12fc129582bd81f11fce774c7144acf24c419104b6f6', 'native_observation_collector': '6d428d86afdc1b925096094756dde01d143875e8fe68744dde2a31031f2b5112', 'readonly_runtime_worker': 'e77998462cb2dff696bb83985a52f3581a191e1938ca4449c66ec8db4840294c'}
STOP = False
CODES = frozenset(('disabled','completed','refused','window','clock','stop',
 'native','budget','marker','transport','http_status','body','privacy','export','cleanup'))

class Refused(ValueError):
    def __init__(self, code):
        self.code = code if code in CODES else 'refused'
        super().__init__(self.code)

def require(ok, code='refused'):
    if not ok: raise Refused(code)

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()

def sha(raw): return hashlib.sha256(raw).hexdigest()

def unique(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'body'); value[key] = item
    return value

def parse(raw):
    def invalid(_): raise Refused('body')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=unique, parse_constant=invalid)

def stop(*_):
    global STOP
    STOP = True

class Clock:
    def __init__(self):
        self.wall = self.last = time.time_ns(); self.mono = time.monotonic_ns()
        self.deadline = min(END_NS, self.wall + MAX_SECONDS * 10**9)
        self.check()
    def check(self):
        now, mono = time.time_ns(), time.monotonic_ns()
        require(not STOP, 'stop')
        require(START_NS <= now < self.deadline and now >= self.last, 'window')
        require(mono >= self.mono and abs((now-self.wall)-(mono-self.mono)) <= 10**9, 'clock')
        require(mono-self.mono < MAX_SECONDS*10**9, 'window')
        self.last = now
        return now

def _stamp(info): return (info.st_dev, info.st_ino, info.st_mode, info.st_uid)

class Marker:
    """Existing or partial file consumes the invocation. No repair or reopening."""
    def __init__(self, path=MARKER):
        self.path = str(path); self.fd = None; self.parent = None; self.size = 0; self.attempt_count = 0
        p = Path(path)
        require(p.is_absolute() and '..' not in p.parts and str(p) == str(path), 'marker')
        for part in reversed(p.parents):
            require(stat.S_ISDIR(part.stat(follow_symlinks=False).st_mode), 'marker')
        self.parent = os.open(str(p.parent), os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        self.parent_stamp = _stamp(os.fstat(self.parent))
        try:
            self.fd = os.open(p.name, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600, dir_fd=self.parent)
            info = os.fstat(self.fd); self.stamp = _stamp(info)
            require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode)==0o600 and info.st_uid==os.geteuid() and info.st_nlink==1, 'marker')
            self.append({'event':'started','capture_id':CAPTURE_ID,'starts_ns':START_NS,'expires_ns':END_NS,'symbols':list(SYMBOLS)})
            os.fsync(self.parent)
        except BaseException:
            self.close(); raise
    def check(self):
        require(self.fd is not None and self.parent is not None, 'marker')
        require(_stamp(os.stat(Path(self.path).parent,follow_symlinks=False))==self.parent_stamp, 'marker')
        info=os.stat(Path(self.path).name,dir_fd=self.parent,follow_symlinks=False)
        require(_stamp(info)==self.stamp and info.st_nlink==1 and _stamp(os.fstat(self.fd))==self.stamp, 'marker')
    def append(self, value):
        self.check(); raw=canonical(value)+b'\n'
        require(self.size+len(raw)<=131072, 'marker')
        at=0
        while at<len(raw):
            n=os.write(self.fd,raw[at:]); require(n>0,'marker'); at+=n
        os.fsync(self.fd); self.size+=len(raw); self.check()
        if value.get('event')=='attempt': self.attempt_count=value['attempt']
    def close(self):
        failure=None
        for name in ('fd','parent'):
            fd=getattr(self,name,None)
            if fd is not None:
                try: os.close(fd); setattr(self,name,None)
                except BaseException as exc: failure=exc
        if failure is not None: raise Refused('cleanup') from failure

class Secrets:
    def __init__(self, key, secret):
        require(all(type(x) is str and 8<=len(x)<=512 and all(33<=ord(c)<=126 for c in x) for x in (key,secret)), 'privacy')
        self.values=(key,secret)
        self.needles=tuple(x.encode() for v in self.values for x in (v,base64.b64encode(v.encode()).decode()))
    def check(self, raw, value):
        require(type(raw) is bytes and all(n not in raw for n in self.needles), 'privacy')
        def walk(x):
            if type(x) is str: require(all(n.decode() not in x for n in self.needles),'privacy')
            elif type(x) is dict:
                for k,v in x.items(): walk(k); walk(v)
            elif type(x) is list:
                for v in x: walk(v)
        walk(value)

BOOLS={'tradable','marginable','shortable','easy_to_borrow','fractionable'}
NUMBERS={'maintenance_margin_requirement'}
DECIMAL_FIELDS={'min_order_size','min_trade_increment','price_increment'}
ATTRIBUTES=('ptp_no_exception','ptp_with_exception','ipo','has_options','options_late_close','fractional_eh_enabled','overnight_tradable','overnight_halted')
ALLOWED={'id','class','exchange','symbol','name','status','attributes','borrow_status','cusip','margin_requirement_long','margin_requirement_short'}|BOOLS|NUMBERS|DECIMAL_FIELDS

def public_asset(value, symbol):
    """Whole-body predicate, NOT a filtered projection or eligibility decision."""
    require(type(value) is dict and set(value)<=ALLOWED and {'id','class','exchange','symbol','status','tradable'}<=set(value),'privacy')
    require(type(value['id']) is str and str(uuid.UUID(value['id']))==value['id'],'privacy')
    require(value['symbol']==symbol and value['class']=='us_equity' and value['exchange'] in ('NYSE','NASDAQ','ARCA','AMEX','BATS','NYSEARCA','OTC','TXSE') and value['status'] in ('active','inactive'),'privacy')
    for k,v in value.items():
        if k in BOOLS: require(type(v) is bool or (k=='easy_to_borrow' and v is None),'privacy')
        elif k in NUMBERS: require(v is None or (type(v) in (int,float) and math.isfinite(v) and 0<=v<=1000000),'privacy')
        elif k in DECIMAL_FIELDS: require(v is None or (type(v) is str and re.fullmatch(r'[0-9]{1,7}(?:\.[0-9]{1,18})?',v) is not None and float(v)<=1000000),'privacy')
        elif k=='name': require(v is None or (type(v) is str and re.fullmatch(r"[A-Za-z0-9 .,&'()/+\-]{1,160}",v) is not None),'privacy')
        elif k=='attributes': require(v is None or (type(v) is list and len(v)<=8 and all(type(x) is str and x in ATTRIBUTES for x in v) and len(set(v))==len(v)),'privacy')
        elif k=='borrow_status': require(v is None or v in ('easy_to_borrow','hard_to_borrow'),'privacy')
        elif k=='cusip': require(v is None or (type(v) is str and re.fullmatch(r'[0-9A-Z*@#]{9}',v) is not None),'privacy')
        elif k in ('margin_requirement_long','margin_requirement_short'): require(v is None or (type(v) is str and re.fullmatch(r'[0-9]{1,3}(?:\.[0-9]{1,6})?',v) is not None and float(v)<=100),'privacy')
    return value['id']

class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None

class HTTP:
    def __init__(self,secrets):
        require(type(secrets) is Secrets,'privacy'); self.secrets=secrets
        self.opener=request.build_opener(request.ProxyHandler({}),NoRedirect())
    def get(self,symbol,marker,clock,index):
        require(symbol in SYMBOLS and type(marker) is Marker and type(clock) is Clock,'transport')
        url=ORIGIN+'/v2/assets/'+symbol
        headers={'APCA-API-KEY-ID':self.secrets.values[0],'APCA-API-SECRET-KEY':self.secrets.values[1],'Accept':'application/json','Accept-Encoding':'identity'}
        start=clock.check(); response=None
        try:
            try: response=self.opener.open(request.Request(url,headers=headers,method='GET'),timeout=2)
            except error.HTTPError as exc: response=exc
            status=response.status if hasattr(response,'status') else response.code
            require(type(status) is int and 100<=status<=599,'transport')
            marker.append({'event':'http_status','attempt':index+1,'status':status,'at_ns':time.time_ns()})
            require(status==200,'http_status')
            require(response.geturl()==url and response.headers.get('Content-Encoding','identity') in ('identity',''),'body')
            length=response.headers.get('Content-Length')
            require(length is None or (re.fullmatch(r'[0-9]{1,9}',length) and int(length)<=MAX_BODY),'body')
            raw=response.read(MAX_BODY+1); received=clock.check()
            require(type(raw) is bytes and len(raw)<=MAX_BODY and (length is None or len(raw)==int(length)),'body')
            value=parse(raw)
            try: self.secrets.check(raw,value)
            except Refused:
                marker.append({'event':'body_veto','attempt':index+1,'bytes':len(raw),'sha256':sha(raw)})
                raise
            # Retain only whole-predicate-valid public assets; unknown bodies are discarded.
            public_asset(value,symbol)
            receipt={'request':{'method':'GET','origin':ORIGIN,'path':'/v2/assets/'+symbol,'query':{}},'status':status,'request_started_ns':start,'received_ns':received,'body_sha256':sha(raw)}
            result={**receipt,'body_base64':base64.b64encode(raw).decode(),'evidence_mode':'imported_capture','synthetic':False,'upstream_receipt_sha256':sha(canonical(receipt))}
            self.secrets.check(canonical(result),result)
            marker.append({'event':'response','response':result})
            return result,value['id']
        finally:
            if response is not None: response.close()

def native_bundle():
    import runtime_state_bundle as inventory
    from paper_account_diagnostic import exclusive_authorities
    exclusive_authorities()
    import importlib
    for name,digest in NATIVE_PINS.items():
        module=importlib.import_module(name)
        require(sha(Path(module.__file__).read_bytes())==digest,'native')
    path=inventory.FIXED_PATHS['seal']; identity=inventory._private(path)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd); require((info.st_dev,info.st_ino)==identity and info.st_size<=4096,'native'); raw=os.read(fd,4097)
    finally: os.close(fd)
    require(len(raw)<=4096 and inventory._private(path)==identity,'native')
    bundle=inventory.RuntimeStateBundle(inventory._load(raw)['fingerprint']);bundle.verify()
    require(bundle._pinned['seal']==identity,'native')
    return bundle

def charge(bundle,marker,index,clock):
    from readonly_runtime_worker import serialized
    from native_account_guard import SharedAccountGuard, MAX_REQUESTS, WINDOW_NS
    from native_existing_budget import ExistingStateRequestBudget
    from native_observation_collector import MAX_GETS, WINDOW_NS as READ_WINDOW
    from paper_account_diagnostic import exclusive_authorities
    require(MAX_REQUESTS==MAX_GETS==12 and WINDOW_NS==READ_WINDOW==60*10**9,'budget')
    with serialized(bundle):
        guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,enabled=True,state_database=bundle.paths['guard'],runtime_bundle=bundle)
        with guard.hold(now_ns=clock.check(),read_only=True):
            exclusive_authorities()
            native=_charge_held(guard,bundle,marker,index,clock)
    # Deliberately outside BOTH native fences before transport/body I/O.
    return native

def _charge_held(guard,bundle,marker,index,clock):
    from native_existing_budget import ExistingStateRequestBudget
    guard.check()
    require(type(index) is int and 0<=index<len(SYMBOLS),'budget')
    require(guard.request_budget_remaining(now_ns=clock.check())>=len(SYMBOLS)-index+2,'budget')
    require(bundle._restart is not None,'native')
    refs=dict(bundle._restart.references)
    require(set(refs)==set(bundle.paths) and all(type(v) in (list,tuple) and len(v)==2 and all(type(n) is int and n>=0 for n in v) for v in refs.values()),'native')
    native={'account_binding':bundle.binding,'cohort':bundle._restart.cohort,'contract':bundle._restart.contract,'preimage':bundle._restart.preimage,'references':{k:list(v) for k,v in refs.items()},'paths':dict(bundle.paths)}
    require(all(type(native[k]) is str and re.fullmatch('[0-9a-f]{64}',native[k]) for k in ('account_binding','cohort','contract','preimage')),'native')
    marker.append({'event':'attempt','attempt':index+1,'symbol':SYMBOLS[index],'at_ns':clock.check()})
    ExistingStateRequestBudget(bundle.paths['guard'],guard).reserve(clock.check())
    guard.reserve_request(clock.check());guard.check();bundle.verify();clock.check()
    return native

def validate_public_envelope(value):
    """Closed public asset export only; native/account metadata is never allowed."""
    require(type(value) is dict and set(value)=={'capture_id','capture'} and value['capture_id']==CAPTURE_ID,'export')
    data=value['capture']
    require(type(data) is dict and set(data)=={'schema','requested_symbols','responses'} and data['schema']=='alpaca-asset-responses-raw-capture-v1' and data['requested_symbols']==list(SYMBOLS),'export')
    rows=data['responses'];require(type(rows) is list and len(rows)==3,'export');ids=set()
    fields={'request','status','request_started_ns','received_ns','body_sha256','body_base64','evidence_mode','synthetic','upstream_receipt_sha256'}
    for symbol,row in zip(SYMBOLS,rows):
        require(type(row) is dict and set(row)==fields and type(row['request']) is dict and type(row['request'].get('query')) is dict and row['request']=={'method':'GET','origin':ORIGIN,'path':'/v2/assets/'+symbol,'query':{}},'export')
        require(type(row['status']) is int and row['status']==200 and row['evidence_mode']=='imported_capture' and row['synthetic'] is False,'export')
        require(type(row['request_started_ns']) is int and type(row['received_ns']) is int and START_NS<=row['request_started_ns']<=row['received_ns']<END_NS,'export')
        encoded=row['body_base64'];require(type(encoded) is str and len(encoded)<=4*((MAX_BODY+2)//3),'export')
        raw=base64.b64decode(encoded,validate=True)
        require(len(raw)<=MAX_BODY and base64.b64encode(raw).decode()==encoded and sha(raw)==row['body_sha256'],'export')
        identity=public_asset(parse(raw),symbol);require(identity not in ids,'export');ids.add(identity)
        receipt={k:row[k] for k in ('request','status','request_started_ns','received_ns','body_sha256')}
        require(sha(canonical(receipt))==row['upstream_receipt_sha256'],'export')
    return value

def export_frames(value,secrets):
    require(type(secrets) is Secrets,'privacy')
    validate_public_envelope(value)
    # The envelope's base64 may hide a literal/escaped secret through alignment.
    # Recheck every original body, not merely its encoded representation.
    for row in value['capture']['responses']:
        original=base64.b64decode(row['body_base64'],validate=True)
        secrets.check(original,parse(original))
    raw=canonical(value);secrets.check(raw,value)
    encoded=base64.b64encode(raw).decode();require(len(encoded)<=MAX_EXPORT,'export')
    digest=sha(raw);parts=[encoded[i:i+CHUNK] for i in range(0,len(encoded),CHUNK)]
    return [{'mode':'paper_asset_capture','kind':'chunk','capture_id':CAPTURE_ID,'seq':i,'total':len(parts),'sha256':digest,'chunk_sha256':sha(part.encode()),'payload':part} for i,part in enumerate(parts)]

def status(code,attempts=0):
    require(code in CODES and ((attempts is None and code in ('refused','cleanup')) or (type(attempts) is int and 0<=attempts<=3)),'refused')
    return {'mode':'paper_asset_capture','kind':'status','reason':code,'attempts':attempts,'order_permission':'none','automatic_retry':False}

def run():
    if CAPTURE_ENABLED is not True: return status('disabled')
    marker=None;attempts=0;stage='window'
    try:
        clock=Clock();stage='native';bundle=native_bundle()
        stage='marker';marker=Marker()
        responses=[];ids=set();native=None;secrets=None;wire=None
        for i,symbol in enumerate(SYMBOLS):
            stage='budget';clock.check();observed=charge(bundle,marker,i,clock);attempts=i+1
            if native is None: native=observed
            require(native==observed,'native')
            stage='privacy'
            if secrets is None:
                secrets=Secrets(os.environ.get('ALPACA_API_KEY'),os.environ.get('ALPACA_SECRET_KEY'));wire=HTTP(secrets)
                # Native/account linkage is internal only: never retained or exported.
                secrets.check(canonical(native),native)
            require(tuple(os.environ.get(k) for k in ('ALPACA_API_KEY','ALPACA_SECRET_KEY'))==secrets.values,'privacy')
            stage='transport';clock.check();response,identity=wire.get(symbol,marker,clock,i)
            require(identity not in ids,'body');ids.add(identity);responses.append(response)
        stage='export'
        envelope={'capture_id':CAPTURE_ID,'capture':{'schema':'alpaca-asset-responses-raw-capture-v1','requested_symbols':list(SYMBOLS),'responses':responses}}
        frames=export_frames(envelope,secrets);clock.check()
        marker.append({'event':'completed','attempts':attempts,'export_sha256':sha(canonical(envelope))})
        for frame in frames: print(canonical(frame).decode(),flush=True)
        return status('completed',attempts)
    except BaseException as exc:
        code=exc.code if type(exc) is Refused else stage
        if marker is not None:
            marker.append({'event':'terminal','reason':code,'attempts':marker.attempt_count})
        return status(code,marker.attempt_count if marker is not None else 0)
    finally:
        if marker is not None: marker.close()

def guards(expected_parent):
    require(type(expected_parent) is int and expected_parent>1 and os.getppid()==expected_parent,'stop')
    parent=expected_parent
    libc=ctypes.CDLL(None,use_errno=True);require(libc.prctl(1,signal.SIGKILL,0,0,0)==0,'stop')
    require(os.getppid()==parent,'stop')
    for kind,limit in ((resource.RLIMIT_CORE,0),(resource.RLIMIT_AS,128*1024*1024),(resource.RLIMIT_CPU,8)):
        _,hard=resource.getrlimit(kind);cap=limit if hard==resource.RLIM_INFINITY else min(limit,hard);resource.setrlimit(kind,(cap,cap))
    def no_children(event,args):
        if event in ('os.fork','os.forkpty','os.posix_spawn','os.exec','os.system','subprocess.Popen'):raise Refused('stop')
    sys.addaudithook(no_children)
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    signal.signal(signal.SIGALRM,signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_REAL,min(MAX_SECONDS,max(0.001,(END_NS-time.time_ns())/10**9)))

def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if len(args)!=3 or args[:2]!=['--capture-three-assets','--parent-pid'] or not re.fullmatch(r'[1-9][0-9]{0,9}',args[2]):return 2
    expected_parent=int(args[2])
    if CAPTURE_ENABLED is not True:
        print(canonical(status('disabled')).decode(),flush=True);return 0
    try:
        require(START_NS<=time.time_ns()<END_NS,'window');guards(expected_parent);result=run()
        print(canonical(result).decode(),flush=True);return 0 if result['reason']=='completed' else 1
    except BaseException as exc:
        code='cleanup' if type(exc) is Refused and exc.code=='cleanup' else 'refused'
        print(canonical(status(code,None)).decode(),flush=True);return 1
    finally: signal.setitimer(signal.ITIMER_REAL,0)

if __name__=='__main__':raise SystemExit(main())
