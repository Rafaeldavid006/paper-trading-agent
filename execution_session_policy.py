"""Exact-root IEX 08:00–09:30 Eastern smoke policy; never a rewritten clock.

Private stored context is descriptive evidence, never execution authority.
"""
from dataclasses import dataclass,asdict
from datetime import datetime,date,time,timezone
from zoneinfo import ZoneInfo
import re,threading
NS=10**9
ET=ZoneInfo('America/New_York')
_cache_root=None
_cache_policy=None
_lock=threading.RLock()
def require(ok,code):
    if not ok:
        from alpaca_schema import SchemaError
        raise SchemaError(code)
@dataclass(frozen=True)
class ExecutionSessionPolicy:
    mode:str
    root_digest:str

def policy_for_root():
    import finite_root_contract as rc
    from finite_pilot_contract import digest
    root=rc.require_root(rc.APPROVED_ROOT)
    smoke=root.smoke
    require(smoke is not None,'execution_smoke_scope_required')
    mode=getattr(smoke,'session_mode','regular')
    if mode=='regular':return None
    require(mode=='iex_premarket','execution_session_mode')
    global _cache_root,_cache_policy
    with _lock:
        if _cache_root is not root:
            _cache_root=root;_cache_policy=ExecutionSessionPolicy(mode,digest(root.document()))
        return _cache_policy

def require_policy(policy):
    require(type(policy) is ExecutionSessionPolicy and policy is policy_for_root(),'execution_policy_not_authorized')
    return policy

def _stamp(day,hm):
    return int(datetime.combine(day,time.fromisoformat(hm),ET).timestamp())*NS

def accounting_day_start(at_ns):
    require(type(at_ns) is int and at_ns>0,'session_clock_invalid')
    day=datetime.fromtimestamp(at_ns/NS,ET).date()
    return _stamp(day,'00:00')

@dataclass(frozen=True)
class SessionContext:
    mode:str
    trade_date:str
    core_open_ns:int
    core_close_ns:int
    execution_open_ns:int
    execution_close_ns:int
    accounting_open_ns:int
    clock_ns:int
    root_digest:str
    root_starts_ns:int
    root_expires_ns:int
    def document(self):return asdict(self)

def stored_context(document,clock):
    """Validate descriptive private projection without granting execution."""
    require(type(document) is dict and set(document)==set(SessionContext.__dataclass_fields__),'session_context_shape')
    ctx=SessionContext(**document)
    require(ctx.mode=='iex_premarket' and type(ctx.trade_date) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}',ctx.trade_date),'session_context_mode')
    day=date.fromisoformat(ctx.trade_date)
    require(day.weekday()<5,'session_calendar_weekend')
    require(all(type(getattr(ctx,k)) is int and 0<getattr(ctx,k)<2**63 for k in ('core_open_ns','core_close_ns','execution_open_ns','execution_close_ns','accounting_open_ns','clock_ns','root_starts_ns','root_expires_ns')),'session_context_time')
    require(type(ctx.root_digest) is str and re.fullmatch('[a-f0-9]{64}',ctx.root_digest),'session_context_root')
    require(ctx.core_open_ns==_stamp(day,'09:30') and ctx.execution_open_ns==_stamp(day,'08:00') and ctx.execution_close_ns==ctx.core_open_ns and ctx.accounting_open_ns==_stamp(day,'00:00'),'session_context_calendar')
    require(ctx.core_open_ns<ctx.core_close_ns<=_stamp(day,'16:00'),'session_context_close')
    require(ctx.execution_open_ns<=ctx.root_starts_ns<ctx.root_expires_ns<=ctx.execution_close_ns,'root_crosses_execution_session')
    require(clock.is_open is False and clock.timestamp_ns==ctx.clock_ns and clock.next_open_ns==ctx.core_open_ns and clock.next_close_ns==ctx.core_close_ns,'session_context_clock_mismatch')
    require(ctx.execution_open_ns<=ctx.clock_ns<ctx.execution_close_ns and datetime.fromtimestamp(ctx.clock_ns/NS,ET).date()==day,'session_context_clock_outside')
    return ctx

def validate_context(document,clock,now_ns,policy):
    require_policy(policy)
    import finite_root_contract as rc
    root=rc.APPROVED_ROOT
    ctx=stored_context(document,clock)
    require(ctx.root_digest==policy.root_digest and (ctx.root_starts_ns,ctx.root_expires_ns)==(root.starts_ns,root.expires_ns),'session_context_authority_changed')
    from alpaca_schema import fresh
    fresh(ctx.clock_ns,now_ns)
    require(type(now_ns) is int and ctx.root_starts_ns<=now_ns<ctx.root_expires_ns and ctx.execution_open_ns<=now_ns<ctx.execution_close_ns,'execution_session_closed')
    return ctx

def context_from_calendar(clock,calendar,*,received_ns,now_ns,policy):
    require_policy(policy)
    require(type(calendar) is list and len(calendar)==1 and type(calendar[0]) is dict,'session_calendar_missing')
    require(type(received_ns) is int and clock.timestamp_ns<=received_ns<=now_ns,'session_calendar_clock')
    day=datetime.fromtimestamp(now_ns/NS,ET).date();row=calendar[0]
    require(row.get('date')==day.isoformat() and row.get('open')=='09:30' and type(row.get('close')) is str and re.fullmatch(r'\d{2}:\d{2}',row['close']),'session_calendar_mismatch')
    import finite_root_contract as rc
    root=rc.APPROVED_ROOT
    ctx=SessionContext('iex_premarket',day.isoformat(),_stamp(day,'09:30'),_stamp(day,row['close']),_stamp(day,'08:00'),_stamp(day,'09:30'),_stamp(day,'00:00'),clock.timestamp_ns,policy.root_digest,root.starts_ns,root.expires_ns)
    return validate_context(ctx.document(),clock,now_ns,policy)

def validate_execution(snapshot,now_ns,policy):
    require_policy(policy)
    require(type(snapshot.execution_session) is SessionContext,'premarket_context_required')
    return validate_context(snapshot.execution_session.document(),snapshot.clock,now_ns,policy)
