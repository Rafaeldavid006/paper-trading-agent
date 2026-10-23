"""One separately approved, bounded GET-only sample. Not recovery or trading.

Disabled source gate and unset UTC window are intentional. A claim is durable
BEFORE credentials or any GET, including a provider clock check. A crash, closed
provider session, partial sample or expiry after claim consumes this generation.
"""
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import sys
import time

import native_diagnostic_audit_inspection as historic
import native_observation_collector as collector
from native_account_guard import SharedAccountGuard
from native_get_diagnostic_trace import Trace,ObservedCollector,ObservedWire,observe
from native_observation_worker import load_owned_ledger
from readonly_runtime_worker import serialized
from runtime_state_bundle import RuntimeStateBundle,FIXED_PATHS,_database,_SIDECARS

ENABLE_APPROVED_GET_DIAGNOSTIC=True
APPROVED_START_NS=1792762560000000000
APPROVED_END_NS=1792763160000000000
GENERATION='native-get-diagnostic-20261023-g1'
SQL=('CREATE TABLE native_get_diagnostic_claim(id INTEGER PRIMARY KEY CHECK(id=1),'
     'generation TEXT NOT NULL,code_hash TEXT NOT NULL,claimed_ns INTEGER NOT NULL,'
     'status TEXT NOT NULL,audit_json TEXT,result_json TEXT)')


def require(ok):
    if not ok:raise ValueError('diagnostic_refused')


def report(reason,trace=None,*,requests=0,parser='none',schema_code='none',collector_code='none'):
    require(reason in ('diagnostic_disabled','outside_approved_window','diagnostic_already_claimed',
        'diagnostic_refused','sample_observed','sample_refused','invalid_arguments'))
    require(parser in (*historic.frozen.PARSERS,'none','unclassified'))
    require(schema_code in historic.frozen.SCHEMA_CODES|{'none','unknown_schema'})
    require(collector_code in collector._CODES|{'none'})
    require(type(requests) is int and 0<=requests<=12)
    value=dict(mode='native_get_diagnostic',reason=reason,requests=requests,
        parser=parser,schema_code=schema_code,collector_code=collector_code,
        order_permission='none',execution_ready=False,runtime_reset_performed=False,
        automatic_retry=False,historical_cause_proven=False,**(trace or Trace()).result())
    require(len(json.dumps(value,separators=(',',':')))<2048)
    return value


def code_hash():
    historic._frozen_code()
    here=Path(__file__).parent
    return hashlib.sha256(b''.join((here/name).read_bytes() for name in
        ('native_get_diagnostic.py','native_get_diagnostic_trace.py'))).hexdigest()


def table(db):
    row=db.execute("SELECT type,sql FROM sqlite_master WHERE name='native_get_diagnostic_claim'").fetchone()
    if row is None:return False
    require(row==('table',SQL))
    require(not db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='native_get_diagnostic_claim'").fetchone())
    # Any row consumes the generation, including an interrupted result.
    require(db.execute('SELECT id,generation FROM native_get_diagnostic_claim').fetchall()==[(1,GENERATION)])
    return True


def no_sidecars(bundle):
    # No journal recovery is part of this diagnostic authority. Active owned
    # transaction journals exist only inside their original bounded DB contexts.
    for role in ('anchor','legacy','lifecycle','guard','runner','source','decision','manifest'):
        for suffix in _SIDECARS:
            require(not os.path.lexists(bundle.paths[role]+suffix))


def claim(bundle,now_ns,digest,window):
    with _database(bundle.paths['anchor'],bundle._pinned['anchor'],write=True,public_read=True) as db:
        require(not table(db))
        now=now_ns()
        if not (type(now) is int and window[0]<=now<window[1]):return None
        db.execute(SQL)
        db.execute('INSERT INTO native_get_diagnostic_claim VALUES(1,?,?,?,\'claimed\',NULL,NULL)',
                   (GENERATION,digest,now))
    return digest,now


def persist(bundle,receipt,column,value):
    require(column in ('audit_json','result_json'))
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
    require(len(raw)<2048)
    with _database(bundle.paths['anchor'],bundle._pinned['anchor'],write=True,public_read=True) as db:
        require(table(db))
        require(db.execute('UPDATE native_get_diagnostic_claim SET '+column+'=?'+
            (",status='recorded'" if column=='result_json' else '')+
            " WHERE id=1 AND generation=? AND code_hash=? AND claimed_ns=? AND status='claimed'",
            (raw,GENERATION,*receipt)).rowcount==1)


def audit_writer(bundle,receipt):
    def audit(*,at_ns,status,attempts,owned_count,order_count,digest=None,rowid=None):
        require(type(at_ns) is int and 0<at_ns<2**63 and status in collector._CODES)
        require(all(type(n) is int and 0<=n<=limit for n,limit in
                    ((attempts,12),(owned_count,4),(order_count,499))))
        require(rowid is None or type(rowid) is int and rowid==1)
        require(digest is None or type(digest) is str and len(digest)==64
                and all(c in '0123456789abcdef' for c in digest))
        persist(bundle,receipt,'audit_json',dict(at_ns=at_ns,status=status,attempts=attempts,
            owned_count=owned_count,order_count=order_count,digest=digest))
        return 1
    return audit


def run_once(*,test_only=False,paths=None,environ=None,now_ns=time.time_ns,
             transport_factory=None,enabled=None,window=None):
    require(type(test_only) is bool and (test_only or (paths is None and environ is None
            and now_ns is time.time_ns and transport_factory is None and enabled is None and window is None)))
    active=ENABLE_APPROVED_GET_DIAGNOSTIC if enabled is None else enabled
    require(type(active) is bool)
    if not active:return report('diagnostic_disabled')
    start,end=(APPROVED_START_NS,APPROVED_END_NS) if window is None else window
    require(type(start) is int and type(end) is int and 0<start<end<2**63
            and end-start<=600*collector.NS)
    def allowed():
        now=now_ns()
        return type(now) is int and start<=now<end
    if not allowed():return report('outside_approved_window')
    digest=code_hash();resolved=FIXED_PATHS if paths is None else paths
    binding=load_owned_ledger(resolved['legacy']).account_fingerprint
    bundle=RuntimeStateBundle(binding,paths=paths,test_only=test_only)
    trace=Trace(allowed);attempt=None;captured={}
    with serialized(bundle):
        require(bundle.guard_identities()[2])  # Completed durable restart identity required.
        with _database(bundle.paths['anchor'],bundle._pinned['anchor'],public_read=True) as db:
            if table(db):return report('diagnostic_already_claimed')
        # Full original evidence check, with no new historical audit row added.
        with ExitStack() as stack:
            dbs={r:stack.enter_context(_database(bundle.paths[r],bundle._pinned[r],
                      public_read=r=='anchor')) for r in ('anchor','guard','runner','lifecycle')}
            prior=historic._read_evidence(**dbs)
            require(prior['audit_code']=='provider_read_failed')
        no_sidecars(bundle)
        guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],binding,
            enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
        with guard.hold(now_ns=now_ns(),read_only=True):
            if not allowed():return report('outside_approved_window')
            receipt=claim(bundle,now_ns,digest,(start,end))
            if receipt is None:return report('outside_approved_window')
            # No raw transport/env data reaches result, including on setup failure.
            try:
                env=os.environ if environ is None else environ
                key,secret=env.get('ALPACA_API_KEY'),env.get('ALPACA_SECRET_KEY')
                require(bool(key) and bool(secret))
                transport=(transport_factory or collector.NativeReadOnlyHTTP)(key,secret,enabled=True)
                wire=ObservedWire(transport,guard,trace,now_ns)
                def view():
                    guard.check();return collector.OwnedLedgerView(binding,guard.legacy_revision,())
                attempt=ObservedCollector(trace=trace,database=bundle.paths['guard'],transport=wire,
                    expected_fingerprint=binding,ledger_view=view,symbols=('NVDA',),enabled=True,now_ns=now_ns)
                with observe(bundle,guard,trace,transport,audit_writer(bundle,receipt)) as captured:
                    try:
                        attempt.collect()
                        result=report('sample_observed',trace,requests=attempt._requests,collector_code='observed_readonly')
                    except collector.CollectionRefused as exc:
                        if captured:trace.fail('schema',captured['schema_code'])
                        else:trace.fail('collector',exc.code)
                        result=report('sample_refused',trace,requests=attempt._requests,collector_code=exc.code,**captured)
                guard.check();bundle.verify()
                if trace.audit_failed:result=report('diagnostic_refused',trace,requests=attempt._requests)
            except Exception:
                trace.fail('setup_or_integrity','diagnostic_refused')
                result=report('diagnostic_refused',trace,requests=0 if attempt is None else attempt._requests)
            # Recorded collector evidence is not an independent success authority:
            # subsequent guard/cohort cleanup must also pass before emitting result.
            persist(bundle,receipt,'result_json',result)
        bundle.verify()
    return result


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if args!=['--once']:result=report('invalid_arguments')
    else:
        try:result=run_once()
        except Exception:result=report('diagnostic_refused')
    print(json.dumps(result,sort_keys=True),flush=True)
    return 0 if result['reason'] in ('diagnostic_disabled','outside_approved_window',
        'diagnostic_already_claimed','sample_observed','sample_refused') else 1


if __name__=='__main__':raise SystemExit(main())
