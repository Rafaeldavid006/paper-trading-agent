"""Real source-only WebSocket owner. Finite durable scope; no broker/order URLs."""
import asyncio
import json
import logging
import os
from pathlib import Path
import signal
import sys
import time

from live_source_protocol import (KEY, NS, MAX_MESSAGE, URLS, Refused, require,
    config, control, subscription, data_summary)
from live_source_store import Journal


def reserve_shared_request():
    """Conservative shared HTTP-upgrade reservation; never initialize/reset state."""
    from paper_account_diagnostic import exclusive_authorities
    import runtime_state_bundle as inventory
    from readonly_runtime_worker import serialized
    from native_account_guard import SharedAccountGuard
    from native_existing_budget import ExistingStateRequestBudget
    exclusive_authorities()
    path = inventory.FIXED_PATHS['seal']
    identity = inventory._private(path)
    fd = os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require((info.st_dev,info.st_ino) == identity and info.st_size <= 4096)
        raw = os.read(fd,4097)
    finally: os.close(fd)
    require(len(raw) <= 4096 and inventory._private(path) == identity)
    bundle = inventory.RuntimeStateBundle(inventory._load(raw)['fingerprint'])
    bundle.verify()
    require(bundle._pinned['seal'] == identity)
    with serialized(bundle):
        guard = SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,
            enabled=True,state_database=bundle.paths['guard'],runtime_bundle=bundle)
        with guard.hold(now_ns=time.time_ns(),read_only=True):
            # A single upgrade, not the all-12-slots cycle used by broker scans.
            ExistingStateRequestBudget(bundle.paths['guard'],guard).reserve(time.time_ns())
            guard.reserve_request(time.time_ns())
            bundle.verify()


def connector():
    from websockets.asyncio.client import connect
    class FixedConnect(connect):
        def process_redirect(self, exc):
            return Refused('handshake_refused')
        def __aiter__(self):
            raise Refused('automatic_retry_forbidden')
    silent = logging.Logger('private_source_transport',level=logging.CRITICAL+1)
    silent.disabled, silent.propagate = True, False
    def open_source(source):
        require(source in URLS)
        return FixedConnect(URLS[source],proxy=None,compression=None,
            additional_headers={'Content-Type':'application/json'},logger=silent,
            open_timeout=8,ping_interval=20,ping_timeout=10,close_timeout=2,
            max_size=MAX_MESSAGE,max_queue=8,write_limit=32768)
    return open_source


def safe_code(exc):
    code = getattr(exc,'code','source_failure')
    if type(code) is not str: code = 'source_failure'
    allowed = {'source_invariant','message_bound','duplicate_key','invalid_number','control_shape',
        'control_order','subscription_shape','subscription_mismatch','unexpected_subscription',
        'provider_error','data_shape','unexpected_channel_or_symbol','price_type','price_bound',
        'crossed_positive_quote','size_shape','news_shape','news_time','news_symbols','news_text_shape',
        'provider_time','storage_budget','journal_unavailable','journal_identity','journal_changed',
        'journal_clock','attempt_budget','grant_completed','handshake_refused','source_failure','stop_requested',
        'journal_parent','journal_sidecar','journal_bound','incomplete_journal','journal_shape',
        'journal_chain','journal_hash','journal_event','journal_data','journal_write','journal_path',
        'invalid_grant','disk_headroom','enrollment_missing','enrollment_write','enrolled_journal_missing',
        'enrollment_identity','enrollment_grant','enrollment_changed','transport_policy_refused',
        'shared_request_budget_exhausted','budget_clock_regression','request_clock_regression',
        'budget_state_corrupt','budget_storage_unavailable','legacy_worker_busy','legacy_writer_busy'}
    allowed |= {'provider_'+str(n) for n in (400,401,402,403,404,405,406,407,409,410,500)}
    return code if code in allowed else 'source_failure'


class Owner:
    def __init__(self, grant, raw_grant, journal, parent, *, open_source=None, reserve=reserve_shared_request):
        self.grant, self.raw_grant, self.journal, self.parent = grant,raw_grant,journal,parent
        self.open_source, self.reserve = open_source or connector(), reserve
        self.stop = asyncio.Event()
        self.started_wall, self.started_mono = time.time_ns(),time.monotonic_ns()
        self.reason = 'deadline'
        self.states = {s:dict(state='not_started',records=0,frames=0,fresh_records=0,
            one_sided=0,no_sides=0,two_sided=0,fresh_two_sided=0,last_receipt_ns=None,
            newest_provider_ns=None,newest_update_ns=None,fresh_updates=0,quote_times={},duplicate_news=0,news_seen={}) for s in URLS}

    def check(self):
        require(not self.stop.is_set(), 'stop_requested')
        now = time.time_ns()
        require(self.grant['starts_ns'] <= now < self.grant['expires_ns'], 'grant_completed')
        require(now >= self.journal.high_ns and abs((now-self.started_wall)-
                (time.monotonic_ns()-self.started_mono)) <= 5*NS, 'journal_clock')
        require(os.environ.get(KEY) == self.raw_grant, 'source_invariant')
        require(os.getppid() == self.parent, 'source_invariant')
        return now

    def append(self, event, source=None, **values):
        row = dict(event=event,at_ns=time.time_ns(),**values)
        if source is not None: row['source'] = source
        self.journal.append(row)

    async def receive(self, ws, timeout):
        raw = await asyncio.wait_for(ws.recv(),timeout)
        require(type(raw) in (bytes,str), 'message_bound')
        raw = raw.encode('utf-8') if type(raw) is str else raw
        require(len(raw) <= MAX_MESSAGE, 'message_bound')
        return raw,time.time_ns()

    async def handshake(self, ws, source, credentials):
        for phase in ('connected','authenticated','subscription'):
            self.check()
            raw,at = await self.receive(ws,8)
            # Preserve returned evidence even if protocol validation fails.
            self.journal.body(source,raw,at,{'kind':'control','phase':phase})
            control(raw,phase,source,self.grant)
            self.append('control',source,phase=phase)
            self.states[source]['state'] = phase
            if phase == 'connected':
                self.check()
                await ws.send(json.dumps({'action':'auth','key':credentials[0],'secret':credentials[1]}))
            elif phase == 'authenticated':
                self.check()
                await ws.send(json.dumps(subscription(source,self.grant)))

    async def source(self, source, credentials):
        from websockets.exceptions import ConnectionClosed
        state = self.states[source]
        if source in self.journal.terminal:
            state['state'] = 'previously_terminal'
            return
        while not self.stop.is_set():
            try:
                self.check()
                if self.journal.attempts[source] >= self.grant['max_attempts']:
                    self.append('source_terminal',source,reason='attempt_budget')
                    state['state'] = 'attempt_budget'
                    return
                self.append('attempt',source,attempt=self.journal.attempts[source]+1)
                self.reserve()  # Two durable budgets precede every actual upgrade.
                self.check()
                state['state'] = 'connecting'
                async with self.open_source(source) as ws:
                    await self.handshake(ws,source,credentials)
                    state['state'] = 'subscribed_quiet'
                    while not self.stop.is_set():
                        self.check()
                        try: raw,at = await self.receive(ws,1)
                        except TimeoutError: continue
                        try:
                            summary = data_summary(raw,source,set(self.grant['symbols']),at)
                        except Exception as exc:
                            self.journal.body(source,raw,at,{'kind':'invalid','reason':safe_code(exc)})
                            raise
                        self.journal.body(source,raw,at,summary)
                        state['state'] = 'receiving'
                        state['frames'] += 1
                        state['records'] += summary['records']
                        state['fresh_records'] += summary['fresh_count']
                        for field in ('one_sided','no_sides','two_sided','fresh_two_sided'):
                            state[field] += summary[field]
                        state['last_receipt_ns'] = at
                        state['newest_provider_ns'] = max(summary['newest_ns'],state['newest_provider_ns'] or 0)
                        for symbol,stamp in summary['quote_times'].items():
                            state['quote_times'][symbol] = max(stamp,state['quote_times'].get(symbol,0))
                        state['fresh_updates'] += summary['fresh_updates_count']
                        if summary['newest_update_ns']:
                            state['newest_update_ns'] = max(summary['newest_update_ns'],state['newest_update_ns'] or 0)
                        for key in summary['news_keys']:
                            state['duplicate_news'] += int(key in state['news_seen'])
                            state['news_seen'][key] = None
                            if len(state['news_seen']) > 4096:
                                del state['news_seen'][next(iter(state['news_seen']))]
            except asyncio.CancelledError:
                state['state'] = 'stopped'
                raise
            except (ConnectionClosed,TimeoutError) as exc:
                if isinstance(exc,ConnectionClosed):
                    closed = exc.rcvd or exc.sent
                    if closed is not None and closed.code not in (1000,1001,1006,1011,1012,1013,1014):
                        state['state'] = 'transport_policy_refused'
                        try: self.append('source_terminal',source,reason='transport_policy_refused')
                        except Exception:
                            self.reason = 'storage_failure'; self.stop.set()
                        return
                # No reset of persistent attempts; no replay/backfill on reconnect.
                state['state'] = 'transport_gap'
                try: self.append('gap',source,reason='transport_gap')
                except Exception:
                    self.reason = 'storage_failure'; self.stop.set(); return
                delay = (5,15,30,30)[min(self.journal.attempts[source]-1,3)]
                try: await asyncio.wait_for(self.stop.wait(),delay)
                except TimeoutError: pass
            except Exception as exc:
                code = safe_code(exc)
                state['state'] = code
                if code == 'stop_requested': return
                try: self.append('source_terminal',source,reason=code)
                except Exception:
                    self.reason = 'storage_failure'; self.stop.set()
                return

    def status(self):
        now = time.time_ns()
        sources = {}
        for source,state in self.states.items():
            public = {k:v for k,v in state.items() if k not in ('quote_times','news_seen')}
            public['attempts_reserved'] = self.journal.attempts[source]
            public['current_fresh_symbol_count'] = sum(0 <= now-t <= 10*NS for t in state['quote_times'].values())
            public['newest_provider_age_seconds'] = ((now-state['newest_provider_ns'])//NS
                if state['newest_provider_ns'] else None)
            sources[source] = public
        return dict(mode='live_source_owner',state='stopping' if self.stop.is_set() else 'running',
            heartbeat_ns=now,expires_ns=self.grant['expires_ns'],persisted_bytes=self.journal.size+self.journal.enrolled_bytes,
            byte_limit=self.grant['max_bytes'],journal_seq=self.journal.seq,journal_sha256=self.journal.head,
            iex_symbol_count=len(self.grant['symbols']),news_scope='all',sources=sources,
            execution_ready=False,order_permission='none',coverage_complete=False,
            broker_requests=0,automatic_owner_retry=False,realtime_entitlement_verified=False,
            process_counters_only=True,clock_verified=False,reason=self.reason if self.stop.is_set() else None)

    def emit(self):
        raw = json.dumps(self.status(),sort_keys=True,separators=(',',':'))
        require(len(raw) <= 8192)
        print(raw,flush=True)

    async def watch(self, tasks):
        next_emit = 0
        try:
            while not self.stop.is_set() and not all(t.done() for t in tasks):
                if any(t.done() and not t.cancelled() and t.exception() is not None for t in tasks):
                    self.reason = 'source_failure'; self.stop.set(); break
                try: self.check()
                except Exception as exc:
                    self.reason = safe_code(exc); self.stop.set(); break
                if time.monotonic() >= next_emit:
                    self.emit(); next_emit = time.monotonic()+15
                await asyncio.sleep(.25)
        finally:
            for task in tasks:
                if not task.done(): task.cancel()

    async def run(self, credentials):
        self.check()
        require(not self.journal.completed, 'grant_completed')
        self.append('boot',previous_attempts=dict(self.journal.attempts))
        for source in URLS:
            if self.journal.attempts[source]: self.append('gap',source,reason='process_restart_gap')
        tasks = [asyncio.create_task(self.source(s,credentials)) for s in URLS]
        await self.watch(tasks)
        outcomes = await asyncio.gather(*tasks,return_exceptions=True)
        if self.reason == 'deadline':
            if any(isinstance(v,Exception) for v in outcomes): self.reason = 'source_failure'
            elif all(s['state'] not in ('receiving','subscribed_quiet','stopped') for s in self.states.values()): self.reason = 'sources_terminal'
        self.stop.set()
        if time.time_ns() >= self.grant['expires_ns'] and not self.journal.completed:
            self.append('completed',reason='deadline')
        elif not self.journal.failed:
            for source in URLS:
                if self.journal.attempts[source] and source not in self.journal.terminal:
                    self.append('gap',source,reason='owner_stopped')
        self.emit()


def main():
    if len(sys.argv) != 3 or sys.argv[1] != '--source-only' or not sys.argv[2].isdigit(): return 2
    journal = None
    try:
        import resource
        for kind,limit in ((resource.RLIMIT_AS,192*1024*1024),(resource.RLIMIT_CPU,300),(resource.RLIMIT_CORE,0)):
            _,hard = resource.getrlimit(kind)
            cap = limit if hard == resource.RLIM_INFINITY else min(limit,hard)
            resource.setrlimit(kind,(cap,cap))
        raw = os.environ.get(KEY)
        grant = config(raw)
        if grant is None:
            print('{"mode":"live_source_owner","state":"disabled","order_permission":"none","broker_requests":0}',flush=True)
            return 0
        require(grant['starts_ns'] <= time.time_ns() < grant['expires_ns'],'grant_completed')
        credentials = os.environ.get('ALPACA_API_KEY'),os.environ.get('ALPACA_SECRET_KEY')
        require(all(type(v) is str and 0 < len(v) <= 512 and all(33 <= ord(c) <= 126 for c in v) for v in credentials))
        journal = Journal(grant)
        async def start():
            owner = Owner(grant,raw,journal,int(sys.argv[2]))
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM,signal.SIGINT):
                loop.add_signal_handler(sig,lambda: (setattr(owner,'reason','stop_signal'),owner.stop.set()))
            await owner.run(credentials)
        asyncio.run(start())
        return 0
    except Exception as exc:
        print(json.dumps(dict(mode='live_source_owner',state='refused',reason=safe_code(exc),
            order_permission='none',broker_requests=0,execution_ready=False)),flush=True)
        return 1
    finally:
        if journal: journal.close()


if __name__ == '__main__': raise SystemExit(main())
