"""One owned source child; no process restart or execution activation."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

KEY = 'LIVE_SOURCE_STREAM_JSON'
# Public non-secret finite grant is frozen only after specific launch approval.
# None keeps this code-only candidate disabled; no Render env changes required.
APPROVED_SOURCE_STREAM = {'version': 1, 'starts_ns': 1793478130000000000, 'expires_ns': 1793479930000000000, 'symbols': ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'MU', 'SPY'], 'news': '*', 'max_bytes': 67108864, 'max_attempts': 4}
TOP = {'mode','state','heartbeat_ns','expires_ns','persisted_bytes','byte_limit','journal_seq',
       'journal_sha256','iex_symbol_count','news_scope','sources','execution_ready','order_permission',
       'coverage_complete','broker_requests','automatic_owner_retry','realtime_entitlement_verified',
       'process_counters_only','clock_verified','reason'}
SOURCE = {'state','records','frames','fresh_records','one_sided','no_sides','two_sided',
          'fresh_two_sided','last_receipt_ns','newest_provider_ns','duplicate_news',
          'attempts_reserved','current_fresh_symbol_count','newest_provider_age_seconds','newest_update_ns','fresh_updates'}


def safe(value):
    if type(value) is not dict or set(value)-TOP or value.get('order_permission') != 'none':
        raise ValueError('invalid_source_status')
    for key,item in value.items():
        if key == 'sources':
            if type(item) is not dict or set(item) != {'iex','news'}: raise ValueError()
            for source in item.values():
                if type(source) is not dict or set(source)-SOURCE: raise ValueError()
                for scalar in source.values(): scalar_safe(scalar)
        else: scalar_safe(item)
    if value.get('execution_ready') is not False or value.get('broker_requests') != 0: raise ValueError()
    return value


def scalar_safe(value):
    if value is None or type(value) is bool: return
    if type(value) is int and 0 <= value < 2**63: return
    if type(value) is str and re.fullmatch(r'[A-Za-z0-9_\-]{1,128}',value): return
    raise ValueError('invalid_source_status')


class SourceSupervisor:
    def __init__(self):
        self.child, self.last, self.buffer = None,None,bytearray()
        self.started, self.reaped = False,True
        self.stop_at, self.heartbeat = None,None
        self.reason = 'disabled'

    def start(self):
        if self.started or APPROVED_SOURCE_STREAM is None: return
        self.started = True
        try:
            from live_source_protocol import config
            if os.environ.get(KEY): raise ValueError('mixed_source_authority')
            raw = json.dumps(APPROVED_SOURCE_STREAM,sort_keys=True,separators=(',',':'))
            config(raw)
            env = {k:os.environ[k] for k in ('ALPACA_API_KEY','ALPACA_SECRET_KEY','EVENT_DB') if k in os.environ}
            env[KEY] = raw
            self.reaped = False
            self.child = subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve().with_name(
                'live_source_owner.py')),'--source-only',str(os.getpid())],
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
                env=env,shell=False,close_fds=True,bufsize=0)
            os.set_blocking(self.child.stdout.fileno(),False)
            self.heartbeat, self.reason = time.monotonic(),'starting'
        except Exception:
            self.reason = 'start_uncertain'; self.begin_stop(); self.emit()

    def read(self):
        try: raw = os.read(self.child.stdout.fileno(),16385)
        except BlockingIOError: return
        self.buffer.extend(raw)
        if len(self.buffer) > 16384: raise ValueError()
        while b'\n' in self.buffer:
            line,_,tail = self.buffer.partition(b'\n'); self.buffer = bytearray(tail)
            if len(line) > 8192: raise ValueError()
            self.last = safe(json.loads(line))
            self.heartbeat = time.monotonic()
            self.reason = 'owner_reporting'
            self.emit()

    def poll(self):
        if self.child is None or self.reaped: return
        try:
            self.read()
            code = self.child.poll()
            if code is not None:
                self.read(); self.reaped = True
                self.reason = 'owner_finished' if code == 0 and not self.buffer else 'owner_refused'
                self.child.stdout.close(); self.emit(); return
            if self.stop_at is None and time.monotonic()-self.heartbeat > 45:
                self.reason = 'heartbeat_stale'; self.begin_stop()
            if self.stop_at is not None and time.monotonic()-self.stop_at > 5:
                self.child.kill()
        except Exception:
            self.reason = 'owner_status_uncertain'; self.begin_stop()

    def emit(self):
        print(json.dumps(dict(mode='live_source_supervisor',reason=self.reason,
            child_reaped=self.reaped,owner_status=self.last,
            owner_status_current=not self.reaped and self.heartbeat is not None and time.monotonic()-self.heartbeat <= 45,
            automatic_retry=False,
            order_permission='none')),flush=True)

    def begin_stop(self):
        if self.child is None or self.reaped or self.stop_at is not None: return
        self.stop_at = time.monotonic()
        try: self.child.terminate()
        except ProcessLookupError: pass

    def close(self):
        self.begin_stop()
        deadline = time.monotonic()+6
        while self.child is not None and not self.reaped and time.monotonic() < deadline:
            self.poll(); time.sleep(.05)
        if self.child is not None and not self.reaped:
            try:
                self.child.kill(); self.child.wait(timeout=1); self.reaped = True
            except Exception: self.reason = 'reap_uncertain'
        self.emit()
