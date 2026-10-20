"""One fixed GET-only diagnostic after the first-open schema halt. No recovery.

The original runtime/probe/lifecycle latches are never cleared or advanced.
Parser wrappers exist only in this dedicated child, capture fixed enums, and
re-raise unchanged. A current sample may fail to reproduce the earlier failure;
that outcome does not diagnose the earlier sample or authorize another attempt.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

import native_observation_collector as collector_module
from alpaca_schema import SchemaError
from native_account_guard import SharedAccountGuard
from native_observation_collector import NativeObservationCollector,OwnedLedgerView,CollectionRefused
from native_observation_worker import load_owned_ledger
from native_paper_execution import CoordinatedWire
from readonly_runtime_worker import RuntimeReadOnlyHTTP,serialized
from runtime_state_bundle import RuntimeStateBundle,FIXED_PATHS,_database

GENERATION='native-schema-diagnostic-20261020-133037-g1'
COLLECTOR_SHA='fd74ef54f25d4865e5875708d9f08de05af0831747613b58460bdefb0f077936'
SCHEMA_SHA='400ddf80247c37ba2c59a34b91da0c910a6631515feb66061cd591fc7c1faa64'
PARSERS=('parse_account','parse_clock','parse_order','parse_positions','parse_snapshot',
         'parse_latest_quotes','reconcile_order','fresh','timestamp')
SCHEMA_CODES=frozenset(('account_binding_mismatch','account_blocked_or_unknown','cancel_state_regression','collection_completeness_unknown','conflicting_same_timestamp','decimal_precision','duplicate_intent','duplicate_order','duplicate_position','empty_quote_size','external_symbol_order','fill_beyond_limit','filled_status_inconsistent','future_order','intent_account_mismatch','intent_order_mismatch','invalid_decimal_range','invalid_decimal_string','invalid_fill_quantity','invalid_identifier','invalid_limit_price','invalid_market_open','invalid_next_session','invalid_object','invalid_owned_intent','invalid_position_quantity','invalid_positions','invalid_quote_number','invalid_quote_spread','invalid_symbols','invalid_timestamp','missing_fill_price','missing_filled_timestamp','missing_owned_intents','order_event_timestamp_order','order_identity_mutated','order_regression','order_timestamp_order','orders_truncated_or_invalid','owned_order_missing','owned_order_still_working','ownership_or_available_mismatch','paper_origin_required','partial_status_inconsistent','quantity_precision','quote_universe_mismatch','response_context_mismatch','stale_or_future','terminal_order_changed','terminal_status_inconsistent','unknown_feed','unknown_or_unsupported_order_status','unsupported_account','unsupported_order_asset','unsupported_order_policy','unsupported_order_structure','unsupported_position','working_full_fill_inconsistent','zero_fill_status_inconsistent'))
_SQL=('CREATE TABLE native_schema_diagnostic_claim(id INTEGER PRIMARY KEY CHECK(id=1),'
      'generation TEXT NOT NULL,code_hash TEXT NOT NULL,claimed_ns INTEGER NOT NULL,'
      'status TEXT NOT NULL,parser TEXT,schema_code TEXT,requests INTEGER)')


def require(ok):
    if not ok:raise ValueError('diagnostic_refused')


def report(reason,*,parser='none',schema_code='none',requests=None,prior_requests=None,prior_orders=None):
    require(reason in ('diagnostic_disabled','diagnostic_already_claimed','diagnostic_refused',
        'schema_reproduced','schema_not_reproduced','collection_refused','invalid_arguments'))
    require(parser in (*PARSERS,'none','unclassified') and schema_code in SCHEMA_CODES|{'none','unknown_schema'})
    require(all(n is None or (type(n) is int and 0<=n<=limit)
                for n,limit in ((requests,12),(prior_requests,12),(prior_orders,499))))
    return dict(mode='native_schema_diagnostic',reason=reason,parser=parser,schema_code=schema_code,
        requests=requests,prior_requests=prior_requests,prior_orders=prior_orders,
        order_permission='none',execution_ready=False,runtime_reset_performed=False)


@contextmanager
def instrument_parsers():
    """First failure wins; strings outside literal code whitelist never escape."""
    captured={};original={name:getattr(collector_module,name) for name in PARSERS}
    def wrap(name,fn):
        def observed(*args,**kwargs):
            try:return fn(*args,**kwargs)
            except SchemaError as exc:
                if not captured:
                    code=exc.args[0] if len(exc.args)==1 and type(exc.args[0]) is str else None
                    captured.update(parser=name,schema_code=code if code in SCHEMA_CODES else 'unknown_schema')
                raise
        return observed
    try:
        for name,fn in original.items():setattr(collector_module,name,wrap(name,fn))
        yield captured
    finally:
        for name,fn in original.items():setattr(collector_module,name,fn)


def code_hash():
    here=Path(__file__)
    require(hashlib.sha256(here.with_name('native_observation_collector.py').read_bytes()).hexdigest()==COLLECTOR_SHA)
    require(hashlib.sha256(here.with_name('alpaca_schema.py').read_bytes()).hexdigest()==SCHEMA_SHA)
    return hashlib.sha256(here.read_bytes()).hexdigest()


def _table(db):
    row=db.execute("SELECT type,sql FROM sqlite_master WHERE name='native_schema_diagnostic_claim'").fetchone()
    if row is None:return False
    require(row==('table',_SQL))
    require(not db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='native_schema_diagnostic_claim'").fetchone())
    return True


def claim(bundle,now_ns,digest):
    with _database(bundle.paths['anchor'],bundle._pinned['anchor'],write=True,public_read=True) as db:
        if _table(db):
            rows=db.execute('SELECT id,generation,code_hash FROM native_schema_diagnostic_claim').fetchall()
            require(len(rows)==1 and rows[0][:2]==(1,GENERATION))
            return False
        db.execute(_SQL)
        db.execute('INSERT INTO native_schema_diagnostic_claim VALUES(1,?,?,?,\'claimed\',NULL,NULL,NULL)',(GENERATION,digest,now_ns))
    return True


def finish(bundle,result):
    with _database(bundle.paths['anchor'],bundle._pinned['anchor'],write=True,public_read=True) as db:
        require(_table(db))
        require(db.execute("UPDATE native_schema_diagnostic_claim SET status=?,parser=?,schema_code=?,requests=? WHERE id=1 AND generation=? AND status='claimed'",
            (result['reason'],result['parser'],result['schema_code'],result['requests'],GENERATION)).rowcount==1)


def prior_halt(bundle):
    with sqlite3.connect(Path(bundle.paths['runner']).as_uri()+'?mode=ro',uri=True) as db:
        row=db.execute('SELECT dirty,attention FROM runner_control WHERE id=1').fetchone()
        require(row==(1,'readonly_runtime_stage_failed'))
    with sqlite3.connect(Path(bundle.paths['lifecycle']).as_uri()+'?mode=ro',uri=True) as db:
        require(db.execute('SELECT COUNT(*) FROM intents').fetchone()==(0,))
        require(db.execute('SELECT COUNT(*) FROM native_dispatch').fetchone()==(0,))
    with sqlite3.connect(Path(bundle.paths['guard']).as_uri()+'?mode=ro',uri=True) as db:
        audit=db.execute('SELECT status,attempts,order_count FROM native_read_audit ORDER BY rowid DESC LIMIT 1').fetchone()
        require(audit is not None and audit[0]=='native_schema_refused')
        require(type(audit[1]) is int and 0<=audit[1]<=12 and type(audit[2]) is int and 0<=audit[2]<500)
    return audit[1:]


def run_once(*,enabled=False,paths=None,test_only=False,environ=None,now_ns=time.time_ns,
             transport_factory=RuntimeReadOnlyHTTP):
    require(type(enabled) is bool and type(test_only) is bool and (paths is None or test_only))
    if not enabled:return report('diagnostic_disabled',requests=0)
    digest=code_hash();resolved=FIXED_PATHS if paths is None else paths
    binding=load_owned_ledger(resolved['legacy']).account_fingerprint
    bundle=RuntimeStateBundle(binding,paths=paths,test_only=test_only)
    with serialized(bundle):
        # Check consumed claim BEFORE prior audit: diagnostic itself adds a budget
        # audit, but can never authorize repeating its fixed generation.
        with _database(bundle.paths['anchor'],bundle._pinned['anchor'],public_read=True) as db:
            if _table(db):
                rows=db.execute('SELECT id,generation FROM native_schema_diagnostic_claim').fetchall()
                require(rows==[(1,GENERATION)])
                return report('diagnostic_already_claimed',requests=0)
        prior_requests,prior_orders=prior_halt(bundle)
        require(claim(bundle,now_ns(),digest))  # Durable before key access/transport.
        result=report('diagnostic_refused',prior_requests=prior_requests,prior_orders=prior_orders)
        collector=None
        try:
            env=os.environ if environ is None else environ
            key,secret=env.get('ALPACA_API_KEY'),env.get('ALPACA_SECRET_KEY')
            require(bool(key) and bool(secret))
            guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],binding,
                enabled=True,state_database=bundle.paths['guard'],test_only=test_only)
            wire=CoordinatedWire(transport_factory(key,secret,enabled=True),guard,now_ns)
            with guard.hold(now_ns=now_ns(),read_only=True):
                def view():
                    guard.check()
                    return OwnedLedgerView(binding,guard.legacy_revision,())
                collector=NativeObservationCollector(database=bundle.paths['guard'],transport=wire,
                    expected_fingerprint=binding,ledger_view=view,symbols=('NVDA',),enabled=True,now_ns=now_ns)
                with instrument_parsers() as captured:
                    try:
                        collector.collect()
                        result=report('schema_not_reproduced',requests=collector._requests,
                            prior_requests=prior_requests,prior_orders=prior_orders)
                    except CollectionRefused:
                        result=report('schema_reproduced' if captured else 'collection_refused',
                            **captured,requests=collector._requests,
                            prior_requests=prior_requests,prior_orders=prior_orders)
            bundle.verify()
        except Exception:
            # Integrity or transport setup failure must not retain a success label.
            attempted=0 if collector is None else getattr(collector,'_requests',None)
            attempted=attempted if type(attempted) is int and 0<=attempted<=12 else None
            result=report('diagnostic_refused',requests=attempted,prior_requests=prior_requests,prior_orders=prior_orders)
        finish(bundle,result)
        return result


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if args!=['--once']:
        print(json.dumps(report('invalid_arguments',requests=0)),flush=True);return 2
    try:result=run_once(enabled=True)
    except Exception:result=report('diagnostic_refused')
    print(json.dumps(result,sort_keys=True),flush=True)
    return 0 if result['reason'] in ('schema_reproduced','schema_not_reproduced','diagnostic_already_claimed') else 1


if __name__=='__main__':raise SystemExit(main())
