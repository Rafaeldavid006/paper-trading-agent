"""Recurring real-source collector. Never a broker, order or paid-feed owner."""
import asyncio
import errno
import json
import logging
import os
import signal
import socket
import sys
import time

from live_source_protocol import NS, URLS, MAX_MESSAGE, Refused, require, data_summary
from live_source_owner import Owner as TrialOwner, safe_code as trial_safe_code
from resident_source_policy import KEY, policy
from resident_source_store import Store


def safe_code(exc):
    code = getattr(exc,'code',None)
    allowed = {'resident_policy','resident_path','resident_parent','resident_predecessor',
        'resident_identity','resident_sidecar','resident_enrollment_missing',
        'resident_enrolled_state_missing','resident_policy_changed','resident_write',
        'resident_enrollment_changed','resident_unavailable','resident_page_size',
        'resident_integrity','resident_state','resident_schema','resident_chain','resident_clock',
        'resident_record_bound','resident_state_bound','resident_boot_budget','resident_attempt_budget','resident_snapshot',
        'predecessor_enrollment','predecessor_size','predecessor_shape','predecessor_chain',
        'predecessor_changed','predecessor_incomplete'}
    return code if code in allowed else trial_safe_code(exc)


def transient_failure(exc):
    """Finite allowlist only; TLS, authentication, entitlement and syntax refuse."""
    from websockets.exceptions import ConnectionClosed, InvalidStatus
    if isinstance(exc,ConnectionClosed):
        closed = exc.rcvd or exc.sent
        return closed is None or closed.code in (1000,1001,1006,1011,1012,1013,1014)
    if isinstance(exc,InvalidStatus):
        return exc.response.status_code in (429,500,502,503,504)
    if isinstance(exc,socket.gaierror): return exc.errno == socket.EAI_AGAIN
    if isinstance(exc,(TimeoutError,ConnectionError)): return True
    if type(exc) is OSError: return exc.errno in (errno.ENETUNREACH,errno.EHOSTUNREACH,errno.ETIMEDOUT)
    return safe_code(exc) in ('provider_500','shared_request_budget_exhausted','legacy_worker_busy','legacy_writer_busy')


def connector():
    from websockets.asyncio.client import connect
    from websockets.exceptions import InvalidStatus
    class FixedConnect(connect):
        def process_redirect(self, exc):
            # Never return a new URL. Preserve nonredirect failures so the fixed
            # bounded retry classifier can distinguish overload from refusal.
            if isinstance(exc,InvalidStatus) and exc.response.status_code in (301,302,303,307,308):
                return Refused('handshake_refused')
            return exc
        def __aiter__(self): raise Refused('automatic_retry_forbidden')
    silent = logging.Logger('private_resident_transport',level=logging.CRITICAL+1)
    silent.disabled,silent.propagate = True,False
    def open_source(source):
        require(source in URLS)
        return FixedConnect(URLS[source],proxy=None,compression=None,
            additional_headers={'Content-Type':'application/json'},logger=silent,
            open_timeout=8,ping_interval=20,ping_timeout=10,close_timeout=2,
            max_size=MAX_MESSAGE,max_queue=8,write_limit=32768)
    return open_source


class Owner(TrialOwner):
    def __init__(self, *args, open_source=None, **kwargs):
        super().__init__(*args,open_source=open_source or connector(),**kwargs)

    def check(self):
        require(not self.stop.is_set(), 'stop_requested')
        now = time.time_ns()
        require(self.grant['starts_ns'] <= now and now >= self.journal.state['high_ns'] and
            abs((now-self.started_wall)-(time.monotonic_ns()-self.started_mono)) <= 5*NS, 'resident_clock')
        require(os.environ.get(KEY) == self.raw_grant and os.getppid() == self.parent, 'source_invariant')
        return now

    def append(self, event, source=None, **values):
        # Trial handshake only calls this for fixed validated control phases.
        require(event == 'control' and source in URLS, 'source_invariant')
        self.journal._write(event,time.time_ns(),source,**values)

    def accumulate(self, source, summary, at):
        state = self.states[source]
        state['state'] = 'receiving'
        state['frames'] += 1; state['records'] += summary['records']
        state['fresh_records'] += summary['fresh_count']
        for field in ('one_sided','no_sides','two_sided','fresh_two_sided'): state[field] += summary[field]
        state['last_receipt_ns'] = at
        state['newest_provider_ns'] = max(summary['newest_ns'],state['newest_provider_ns'] or 0)
        for symbol,stamp in summary['quote_times'].items(): state['quote_times'][symbol] = max(stamp,state['quote_times'].get(symbol,0))
        state['fresh_updates'] += summary['fresh_updates_count']
        if summary['newest_update_ns']: state['newest_update_ns'] = max(summary['newest_update_ns'],state['newest_update_ns'] or 0)
        for key in summary['news_keys']:
            state['duplicate_news'] += int(key in state['news_seen'])
            state['news_seen'][key] = None
            if len(state['news_seen']) > 4096: del state['news_seen'][next(iter(state['news_seen']))]

    async def source(self, source, credentials):
        from websockets.exceptions import ConnectionClosed
        state = self.states[source]
        while not self.stop.is_set():
            try:
                at = self.check()
                due = self.journal.due(source,at)
                if due is None:
                    state['state'] = 'previously_terminal'; return
                if due > at:
                    state['state'] = 'retry_wait'
                    try: await asyncio.wait_for(self.stop.wait(),min(1,(due-at)/NS))
                    except TimeoutError: pass
                    continue
                self.journal.attempt(source,at)
                self.reserve()  # Existing two shared native quotas, before any upgrade.
                self.check()
                state['state'] = 'connecting'
                async with self.open_source(source) as ws:
                    await self.handshake(ws,source,credentials)
                    self.check()
                    self.journal.connected(source,time.time_ns())
                    state['state'] = 'subscribed_quiet'
                    while not self.stop.is_set():
                        self.check()
                        try: raw,at = await self.receive(ws,1)
                        except TimeoutError: continue
                        try: summary = data_summary(raw,source,set(self.grant['symbols']),at)
                        except Exception as exc:
                            self.journal.body(source,raw,at,{'kind':'invalid','reason':safe_code(exc)})
                            raise
                        self.journal.body(source,raw,at,summary)
                        self.accumulate(source,summary,at)
            except asyncio.CancelledError:
                state['state'] = 'stopped'; raise
            except Exception as exc:
                code = safe_code(exc)
                if code == 'stop_requested': return
                transient = transient_failure(exc)
                if isinstance(exc,ConnectionClosed):
                    code = 'transport_gap' if transient else 'transport_policy_refused'
                elif transient and code == 'source_failure': code = 'transport_gap'
                state['state'] = code
                try: self.journal.gap(source,time.time_ns(),code,terminal=not transient)
                except Exception:
                    self.reason = 'storage_failure'; self.stop.set(); return
                if not transient: return

    def status(self):
        now = time.time_ns(); durable = self.journal.state; sources = {}
        for source,state in self.states.items():
            item = durable['sources'][source]
            public = {k:v for k,v in state.items() if k not in ('quote_times','news_seen')}
            public.update(attempts_reserved=item['attempts_total'],attempts_today=item['attempts_today'],
                retry_ns=item['retry_ns'],terminal_reason=item['terminal'],frames_total=item['frames_total'],
                records_total=item['records_total'],gaps_total=item['gaps_total'],
                current_fresh_symbol_count=sum(0 <= now-t <= 10*NS for t in state['quote_times'].values()),
                newest_provider_age_seconds=(now-state['newest_provider_ns'])//NS if state['newest_provider_ns'] else None)
            sources[source] = public
        return dict(mode='resident_source_owner',state='stopping' if self.stop.is_set() else 'running',
            heartbeat_ns=now,persisted_bytes=durable['retained_bytes'],byte_limit=self.grant['retained_bytes'],
            journal_seq=durable['seq'],journal_sha256=durable['head'],retained_after_seq=durable['anchor_seq'],
            retention_anchor_sha256=durable['anchor_hash'],purged_records=durable['purged_records'],
            purged_bytes=durable['purged_bytes'],retention_seconds=self.grant['retained_seconds'],
            predecessor_sha256=self.grant['predecessor']['sha256'],boots_today=durable['boots_today'],
            boots_total=durable['boots_total'],iex_symbol_count=len(self.grant['symbols']),news_scope='all',sources=sources,
            execution_ready=False,order_permission='none',coverage_complete=False,broker_requests=0,
            realtime_entitlement_verified=False,process_counters_only=True,clock_verified=False,
            market_session_verified=False,continuous_authority=True,reason=self.reason if self.stop.is_set() else None)

    async def watch(self, tasks):
        next_emit,next_pulse = 0,0
        try:
            while not self.stop.is_set() and not all(t.done() for t in tasks):
                if any(t.done() and not t.cancelled() and t.exception() is not None for t in tasks):
                    self.reason = 'source_failure'; self.stop.set(); break
                try:
                    now = self.check()
                    if time.monotonic() >= next_pulse:
                        self.journal.pulse(now); next_pulse = time.monotonic()+60
                    if time.monotonic() >= next_emit:
                        self.emit(); next_emit = time.monotonic()+15
                except Exception as exc:
                    self.reason = safe_code(exc); self.stop.set(); break
                await asyncio.sleep(.25)
        finally:
            for task in tasks:
                if not task.done(): task.cancel()

    async def run(self, credentials):
        self.journal.boot(self.check())
        self.reason = 'sources_terminal'
        tasks = [asyncio.create_task(self.source(s,credentials)) for s in URLS]
        await self.watch(tasks)
        await asyncio.gather(*tasks,return_exceptions=True)
        self.stop.set()
        if not self.journal.failed: self.journal.stopped(time.time_ns(),self.reason)
        self.emit()


def main():
    if len(sys.argv) != 3 or sys.argv[1] != '--source-only' or not sys.argv[2].isdigit(): return 2
    journal = None
    try:
        import resource
        for kind,limit in ((resource.RLIMIT_AS,192*1024*1024),(resource.RLIMIT_CORE,0)):
            _,hard = resource.getrlimit(kind)
            cap = limit if hard == resource.RLIM_INFINITY else min(limit,hard)
            resource.setrlimit(kind,(cap,cap))
        raw = os.environ.get(KEY); grant = policy(raw)
        if grant is None:
            print('{"mode":"resident_source_owner","state":"disabled","order_permission":"none","broker_requests":0,"execution_ready":false}',flush=True)
            return 0
        require(grant['starts_ns'] <= time.time_ns(), 'resident_clock')
        credentials = os.environ.get('ALPACA_API_KEY'),os.environ.get('ALPACA_SECRET_KEY')
        require(all(type(v) is str and 0 < len(v) <= 512 and all(33 <= ord(c) <= 126 for c in v) for v in credentials))
        journal = Store(grant)
        async def start():
            owner = Owner(grant,raw,journal,int(sys.argv[2]))
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM,signal.SIGINT):
                loop.add_signal_handler(sig,lambda: (setattr(owner,'reason','stop_signal'),owner.stop.set()))
            await owner.run(credentials)
        asyncio.run(start())
        return 0
    except Exception as exc:
        print(json.dumps(dict(mode='resident_source_owner',state='refused',reason=safe_code(exc),
            order_permission='none',broker_requests=0,execution_ready=False)),flush=True)
        return 1
    finally:
        if journal: journal.close()


if __name__ == '__main__': raise SystemExit(main())
