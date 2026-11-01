"""Source-only child supervisor; recurring authority is disabled until approved."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from live_source_supervisor import scalar_safe
from resident_source_policy import KEY, policy
from private_research_bridge import KEY as RESEARCH_KEY, PUBLIC_FIELDS

# Separate bounded private replay; staged off until specifically authorized.
APPROVED_PRIVATE_REPLAY = True

# Requires a reviewed recurring-service policy, not the expired trial's consent.
APPROVED_RESIDENT_SOURCE = {'version': 2, 'authority_id': 'resident-source-20261031-v1', 'starts_ns': 1793480820000000000, 'symbols': ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'MU', 'SPY'], 'news': '*', 'attempts_per_day': 8, 'attempts_per_hour': 4, 'boots_per_day': 8, 'retained_bytes': 100663296, 'retained_seconds': 1209600, 'predecessor': {'seq': 17, 'bytes': 5634, 'sha256': '0abb98e12cf41bf36ccfdd6a19f338524edfe2a9ec74fbf381afc5e9734e116e'}}
TOP = {'mode','state','heartbeat_ns','persisted_bytes','byte_limit','journal_seq','journal_sha256',
    'retained_after_seq','retention_anchor_sha256','purged_records','purged_bytes','retention_seconds',
    'predecessor_sha256','boots_today','boots_total','iex_symbol_count','news_scope','sources',
    'execution_ready','order_permission','coverage_complete','broker_requests','realtime_entitlement_verified',
    'process_counters_only','clock_verified','market_session_verified','continuous_authority','reason'}
TOP |= PUBLIC_FIELDS
SOURCE = {'state','records','frames','fresh_records','one_sided','no_sides','two_sided','fresh_two_sided',
    'last_receipt_ns','newest_provider_ns','newest_update_ns','fresh_updates','duplicate_news',
    'attempts_reserved','attempts_today','retry_ns','terminal_reason','frames_total','records_total',
    'gaps_total','current_fresh_symbol_count','newest_provider_age_seconds'}


def safe(value):
    if type(value) is not dict or set(value)-TOP or value.get('order_permission') != 'none' or \
        value.get('execution_ready') is not False or value.get('broker_requests') != 0: raise ValueError()
    for key,item in value.items():
        if key == 'sources':
            if type(item) is not dict or set(item) != {'iex','news'}: raise ValueError()
            for source in item.values():
                if type(source) is not dict or set(source)-SOURCE: raise ValueError()
                for scalar in source.values(): scalar_safe(scalar)
        else: scalar_safe(item)
    return value


class SourceSupervisor:
    def __init__(self):
        self.child,self.last,self.buffer = None,None,bytearray()
        self.started,self.reaped,self.closing = False,True,False
        self.stop_at,self.heartbeat,self.retry_at = None,None,None
        self.watchdog_restart,self.restarts = False,0
        self.reason = 'disabled'

    def start(self):
        if self.started or APPROVED_RESIDENT_SOURCE is None: return
        self.started = True
        self._spawn()

    def _spawn(self):
        try:
            if any(os.environ.get(k) for k in (KEY,'LIVE_SOURCE_STREAM_JSON',RESEARCH_KEY)): raise ValueError()
            raw = json.dumps(APPROVED_RESIDENT_SOURCE,sort_keys=True,separators=(',',':'))
            policy(raw)
            env = {k:os.environ[k] for k in ('ALPACA_API_KEY','ALPACA_SECRET_KEY','EVENT_DB') if k in os.environ}
            env[KEY] = raw
            if APPROVED_PRIVATE_REPLAY is True: env[RESEARCH_KEY] = '1'
            self.buffer,self.last,self.stop_at,self.watchdog_restart = bytearray(),None,None,False
            self.child = subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve().with_name(
                'resident_source_owner.py')),'--source-only',str(os.getpid())],stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=env,shell=False,close_fds=True,bufsize=0)
            self.reaped = False
            os.set_blocking(self.child.stdout.fileno(),False)
            self.heartbeat,self.reason = time.monotonic(),'starting'
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
            self.last = safe(json.loads(line)); self.heartbeat = time.monotonic()
            self.reason = 'owner_reporting'; self.emit()

    def poll(self):
        if self.retry_at is not None and not self.closing and time.monotonic() >= self.retry_at:
            self.retry_at = None; self._spawn()
        if self.child is None or self.reaped: return
        try:
            self.read(); code = self.child.poll()
            if code is not None:
                self.read(); self.reaped = True
                self.reason = 'owner_finished' if code == 0 and not self.buffer else 'owner_refused'
                self.child.stdout.close()
                if not self.closing and (code == -9 and self.stop_at is None or self.watchdog_restart) and self.restarts < 4:
                    self.restarts += 1; self.retry_at = time.monotonic()+60*self.restarts
                    self.reason = 'bounded_restart_wait'
                self.emit(); return
            if self.stop_at is None and time.monotonic()-self.heartbeat > 45:
                self.reason,self.watchdog_restart = 'heartbeat_stale',True
                self.begin_stop()
            if self.stop_at is not None and time.monotonic()-self.stop_at > 5: self.child.kill()
        except Exception:
            self.reason = 'owner_status_uncertain'; self.watchdog_restart = False; self.begin_stop()

    def emit(self):
        print(json.dumps(dict(mode='resident_source_supervisor',reason=self.reason,child_reaped=self.reaped,
            owner_status=self.last,owner_status_current=not self.reaped and self.heartbeat is not None and
            time.monotonic()-self.heartbeat <= 45,restarts_this_parent=self.restarts,
            retry_pending=self.retry_at is not None,order_permission='none')),flush=True)

    def begin_stop(self):
        if self.child is None or self.reaped or self.stop_at is not None: return
        self.stop_at = time.monotonic()
        try: self.child.terminate()
        except ProcessLookupError: pass

    def close(self):
        self.closing,self.retry_at = True,None
        self.begin_stop(); deadline = time.monotonic()+6
        while self.child is not None and not self.reaped and time.monotonic() < deadline:
            self.poll(); time.sleep(.05)
        if self.child is not None and not self.reaped:
            try: self.child.kill(); self.child.wait(timeout=1); self.reaped = True
            except Exception: self.reason = 'reap_uncertain'
        self.emit()
