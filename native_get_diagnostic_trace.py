"""Scoped safe observations around the frozen collector, never a quota bypass.

Only used in the separate one-shot child. Original collector/HTTP/parser source
and both original quota algorithms stay unchanged. No raw exception or response
is stored. 'open_attempts' counts urllib invocations, not provider receipt.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import sqlite3
import ssl
import time
from urllib import error

import native_observation_collector as collector
from native_schema_diagnostic import instrument_parsers
from runtime_state_bundle import _SIDECARS

GUARD_CODES=frozenset(('shared_request_budget_exhausted','request_clock_regression',
    'guard_state_corrupt','guard_storage_unavailable','guard_database_replaced',
    'account_guard_check_failed','account_guard_unavailable','legacy_revision_changed',
    'legacy_writer_busy','legacy_worker_busy','restart_identity_physical_changed',
    'restart_identity_context_required','restart_identity_context_invalid',
    'unsafe_database_file','database_replaced','native_control_invalid',
    'native_binding_mismatch','guard_binding_mismatch','account_guard_not_held'))


def safe_code(exc,allowed,fallback):
    value=exc.args[0] if len(exc.args)==1 and type(exc.args[0]) is str else None
    return value if value in allowed else fallback


def transport_type(exc):
    # Never inspect/format exception messages, URLs or response bodies.
    if isinstance(exc,error.URLError) and isinstance(exc.reason,BaseException):
        exc=exc.reason
    for cls,name in ((TimeoutError,'timeout'),(ssl.SSLError,'tls'),
                     (socket.gaierror,'dns'),(ConnectionError,'connection'),
                     (OSError,'os_error')):
        if isinstance(exc,cls):return name
    return 'other_transport'


def role(path):
    collector._route(path)  # Validate first; never report the path itself.
    if path=='/v2/account':return 'account'
    if path=='/v2/clock':return 'clock'
    if path=='/v2/positions':return 'positions'
    if path.startswith('/v2/calendar?'):return 'calendar'
    if path.startswith('/v2/orders?'):return 'orders_page'
    if path.startswith('/v2/orders:'):return 'owned_order'
    return 'iex_quotes'


class Trace:
    def __init__(self,allowed=lambda:True):
        self.allowed=allowed;self.remaining=lambda:2.0
        self.confirmed_collector_reservations=0;self.confirmed_account_reservations=0
        self.quota_outcome_uncertain=False
        self.endpoint='none';self.ordinal=0;self.status=None
        self.open_attempts=0;self.failure={};self.audit_failed=False
    def check_window(self):
        if not self.allowed():
            self.fail('approval','approval_window_expired')
            raise collector.CollectionRefused('collection_deadline_exceeded')
    def begin(self,path,ordinal):
        self.endpoint=role(path);self.ordinal=ordinal;self.status=None
    def fail(self,layer,code):
        if not self.failure:
            self.failure=dict(layer=layer,detail_code=code,endpoint_role=self.endpoint,
                request_ordinal=self.ordinal,http_status=self.status)
    def result(self):
        return dict(self.failure or dict(layer='none',detail_code='none',endpoint_role='none',
                    request_ordinal=0,http_status=None),open_attempts=self.open_attempts,
                    audit_failed=self.audit_failed,confirmed_collector_reservations=self.confirmed_collector_reservations,
                    confirmed_account_reservations=self.confirmed_account_reservations,
                    quota_outcome_uncertain=self.quota_outcome_uncertain)


class _Response:
    def __init__(self,response,trace):self.manager=response;self.response=response;self.trace=trace
    def __enter__(self):
        try:
            self.response=self.manager.__enter__()
            return self
        except Exception as exc:
            self.trace.fail('transport_enter',transport_type(exc));raise
    @property
    def status(self):
        try:
            value=self.response.status
            self.trace.status=value if type(value) is int and 100<=value<=599 else None
            return value
        except Exception as exc:
            self.trace.fail('transport_headers',transport_type(exc));raise
    def __exit__(self,*args):
        try:return self.manager.__exit__(*args)
        except Exception as exc:
            self.trace.fail('transport_close',transport_type(exc));raise
    def __getattr__(self,name):return getattr(self.response,name)
    def read(self,*args):
        try:return self.response.read(*args)
        except Exception as exc:
            self.trace.fail('transport_read',transport_type(exc));raise


class _Opener:
    def __init__(self,opener,trace):self.opener=opener;self.trace=trace
    def open(self,*args,**kwargs):
        kwargs['timeout']=min(kwargs['timeout'],self.trace.remaining())
        self.trace.check_window()
        self.trace.open_attempts+=1
        try:return _Response(self.opener.open(*args,**kwargs),self.trace)
        except error.HTTPError as exc:
            self.trace.status=exc.code if type(exc.code) is int and 100<=exc.code<=599 else None
            self.trace.fail('http','http_status');raise
        except Exception as exc:
            self.trace.fail('transport_open',transport_type(exc));raise


class _JSON:
    """Replace only the collector's module reference, not global json.loads."""
    def __init__(self,original,trace):self.original=original;self.trace=trace
    def __getattr__(self,name):return getattr(self.original,name)
    def loads(self,*args,**kwargs):
        try:return self.original.loads(*args,**kwargs)
        except Exception as exc:
            code=('invalid_json' if isinstance(exc,json.JSONDecodeError) else
                  'invalid_text_encoding' if isinstance(exc,UnicodeDecodeError) else
                  'invalid_json_value' if isinstance(exc,(ValueError,collector.SafetyError)) else 'decode_failed')
            self.trace.fail('json',code);raise


class ObservedWire:
    paper_origin=collector.PAPER_ORIGIN;data_origin=collector.DATA_ORIGIN;feed='iex'
    def __init__(self,transport,guard,trace,now_ns):
        self.transport=transport;self.guard=guard;self.trace=trace;self.now_ns=now_ns
    def call(self,method,path,body=None):
        collector.require(method=='GET' and body is None,'forbidden_method')
        self.trace.check_window()
        for layer,fn in (('guard',self.guard.check),
                         ('account_quota',lambda:self.guard.reserve_request(self.now_ns()))):
            try:
                fn()
                if layer=='account_quota':self.trace.confirmed_account_reservations+=1
            except Exception as exc:
                code=safe_code(exc,GUARD_CODES,'guard_refused')
                if layer=='account_quota' and code not in ('shared_request_budget_exhausted','request_clock_regression'):
                    self.trace.quota_outcome_uncertain=True
                self.trace.fail(layer,code);raise
        try:
            remaining=self.trace.remaining()
            if isinstance(self.transport,collector.NativeReadOnlyHTTP):self.transport.set_timeout(remaining)
            status,data=self.transport.call(method,path,body)
            self.trace.status=status if type(status) is int and 100<=status<=599 else None
            if status!=200:self.trace.fail('http','http_status')
            return status,data
        except collector.CollectionRefused as exc:
            self.trace.fail('transport_policy',exc.code);raise
        except Exception as exc:
            self.trace.fail('transport_open',transport_type(exc));raise


class ObservedCollector(collector.NativeObservationCollector):
    def __init__(self,*,trace,**kwargs):
        self.trace=trace;super().__init__(**kwargs);self.trace.remaining=self._remaining
    def _remaining(self):
        self._check_time()
        seconds=(collector.MAX_COLLECTION_NS-(self._last_mono-self._start_mono))/collector.NS
        collector.require(seconds>0,'collection_deadline_exceeded')
        return min(2.0,seconds)
    def _get(self,path):
        self.trace.begin(path,self._requests+1)
        return super()._get(path)


class ExistingBudget(collector.SharedRequestBudget):
    """Use the SAME quota algorithm and a separate explicit diagnostic audit, but never create/chmod a database.

    This adapter is intentionally valid only with the verified migrated cohort
    and the held account guard. It is not a standalone alternate authority.
    """
    def __init__(self,database,bundle,guard,trace,audit):
        collector.require(str(database)==bundle.paths['guard'],'invalid_database_file')
        collector.require(guard._runtime_bundle is bundle and guard._held and guard._read_only,
                          'budget_storage_unavailable')
        self.database=str(database);self.bundle=bundle;self.guard=guard;self.trace=trace;self._audit=audit
        self.guard.check();self.bundle.verify()
    @contextmanager
    def db(self):
        db=None
        try:
            self.guard.check();self.bundle.verify()
            collector.require(not any(os.path.lexists(self.database+s) for s in _SIDECARS),
                              'budget_storage_unavailable')
            db=sqlite3.connect(Path(self.database).as_uri()+'?mode=rw',uri=True,timeout=.05)
            db.execute('PRAGMA trusted_schema=OFF');db.execute('PRAGMA temp_store=MEMORY')
            db.execute('PRAGMA synchronous=FULL');db.execute('PRAGMA cache_spill=OFF')
            deadline=time.monotonic()+2
            db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            with db:
                self.guard.check();self.bundle.verify()
                yield db
                self.guard.check();self.bundle.verify()
        except collector.CollectionRefused:raise
        except Exception:
            raise collector.CollectionRefused('budget_storage_unavailable') from None
        finally:
            if db is not None:db.close()
            self.guard.check();self.bundle.verify()
    def reserve(self,now_ns):
        try:
            result=super().reserve(now_ns);self.trace.confirmed_collector_reservations+=1;return result
        except Exception as exc:
            code=safe_code(exc,collector._CODES,'budget_storage_unavailable')
            if code not in ('shared_request_budget_exhausted','budget_clock_regression'):
                self.trace.quota_outcome_uncertain=True
            self.trace.fail('collector_quota',code);raise
    def audit(self,**kwargs):
        # Dedicated durable receipt, not the immutable historical audit stream.
        try:return self._audit(**kwargs)
        except Exception:
            self.trace.audit_failed=True;raise


@contextmanager
def observe(bundle,guard,trace,transport,audit):
    """Dedicated child only; restore every temporary reference even on cancel."""
    original_budget=collector.SharedRequestBudget;original_json=collector.json
    original_opener=getattr(transport,'_opener',None)
    try:
        collector.SharedRequestBudget=lambda database:ExistingBudget(database,bundle,guard,trace,audit)
        collector.json=_JSON(original_json,trace)
        if original_opener is not None:transport._opener=_Opener(original_opener,trace)
        with instrument_parsers() as captured:
            yield captured
    finally:
        if original_opener is not None:transport._opener=original_opener
        collector.SharedRequestBudget=original_budget;collector.json=original_json
