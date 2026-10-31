"""Disabled private one-shot PAPER account inspection; never trading authority.

No credentials or provider bodies leave this process. Broker operations are GET
only; local one-shot and shared quota reservations are durable writes. Old
admission/execution/root journals, claims, controls and sources are not reset.
"""
from contextlib import ExitStack
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import signal
import time
from urllib import request, error

KEY = 'PAPER_ACCOUNT_CHECK_ONCE_JSON'
PAPER = 'https://paper-api.alpaca.markets'
ORDERS = '/v2/orders?status=open&limit=500&nested=false'
ROUTES = {p: PAPER for p in ('/v2/account', '/v2/clock', '/v2/positions', ORDERS)}
SEQUENCE = ('/v2/account', '/v2/clock', '/v2/positions', ORDERS,
            '/v2/positions', ORDERS)
# Retain the prior candidate's consumed-once identity; narrowing scope is not a reset.
MARKER = '/var/data/paper-account-check-once-v1.jsonl'
MAX_GETS = 6
MAX_BYTES = 524288
MAX_SECONDS = 20
NS = 10**9


class Refused(ValueError):
    pass


class ProviderRefused(Refused):
    def __init__(self, stage, status):
        self.stage, self.status = stage, status
        super().__init__('provider_refused')


def require(ok):
    if not ok:
        raise Refused('account_check_refused')


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def invalid(_):
    raise Refused('account_check_refused')


def config(raw):
    if raw is None:
        return None
    require(type(raw) is str and 0 < len(raw) <= 256)
    value = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)
    require(type(value) is dict and set(value) == {'starts_ns', 'expires_ns'})
    start, end = value['starts_ns'], value['expires_ns']
    require(type(start) is int and type(end) is int and 0 < start < end <= 2**63-1
            and end-start <= 300*NS)
    return start, end


def text(value):
    require(type(value) is str and re.fullmatch(r'[A-Za-z0-9_.:/-]{1,128}', value))
    return value


def number(value):
    require(type(value) in (str, int, Decimal) and type(value) is not bool)
    value = Decimal(value)
    require(value.is_finite() and abs(value) <= Decimal('1e18'))
    return value


def timestamp(value):
    from alpaca_schema import timestamp as decode_timestamp
    return decode_timestamp(value)


def book(rows, *, orders=False):
    require(type(rows) is list and len(rows) <= 500)
    signatures = []
    for row in rows:
        require(type(row) is dict)
        identity = text(row.get('id') if orders else row.get('asset_id'))
        symbol, asset_class = text(row.get('symbol')), text(row.get('asset_class'))
        qty = number(row.get('qty'))
        side = text(row.get('side'))
        if orders:
            require(qty >= 0)
            signature = (identity, symbol, asset_class, str(qty), side,
                         text(row.get('status')), str(number(row.get('filled_qty'))),
                         timestamp(row.get('updated_at')))
        else:
            signature = (identity, symbol, asset_class, str(qty), side)
        signatures.append(signature)
    require(len({x[0] for x in signatures}) == len(signatures))
    return tuple(sorted(signatures))


class Once:
    """Any existing marker consumes the attempt; never repair/delete/refund it."""
    def __init__(self, path):
        self.path = str(path)
        self.fd = None
        self.count = 0
        from runtime_state_bundle import _parents
        _parents(self.path)
        require(not any(os.path.lexists(self.path+s) for s in ('-journal', '-wal', '-shm')))
        self.fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(self.fd)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_mode & 0o077 == 0)
            self.identity = info.st_dev, info.st_ino
            self.append({'event': 'started', 'version': 1, 'max_gets': MAX_GETS})
            parent = os.open(str(Path(self.path).parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
        except BaseException:
            self.close()
            raise

    def append(self, value):
        info = os.stat(self.path, follow_symlinks=False)
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                (info.st_dev, info.st_ino) == self.identity)
        raw = (json.dumps(value, sort_keys=True, separators=(',', ':'))+'\n').encode()
        require(os.write(self.fd, raw) == len(raw))
        os.fsync(self.fd)

    def reserve(self, index):
        require(type(index) is int and index == self.count and index < MAX_GETS)
        self.count += 1
        self.append({'event': 'reserved', 'attempt': self.count})

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class HTTP:
    """Fixed origin/path, no proxies, redirects, retries or writable operation."""
    def __init__(self, key, secret):
        from paper_adapter import NoRedirect
        require(all(type(x) is str and 0 < len(x) <= 512 and
                    all(33 <= ord(c) <= 126 for c in x) for x in (key, secret)))
        self.__headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret,
                          'Accept': 'application/json', 'Accept-Encoding': 'identity'}
        self.__opener = request.build_opener(request.ProxyHandler({}), NoRedirect())

    def call(self, method, path, body=None):
        require(method == 'GET' and body is None and path in ROUTES)
        url = ROUTES[path]+path
        try:
            with self.__opener.open(request.Request(url, headers=self.__headers, method='GET'), timeout=2) as response:
                require(response.geturl() == url and response.status == 200)
                require(response.headers.get('Content-Encoding', 'identity') in ('identity', ''))
                length = response.headers.get('Content-Length')
                require(length is None or (re.fullmatch(r'\d{1,9}', length) and int(length) <= MAX_BYTES))
                raw = response.read(MAX_BYTES+1)
                require(len(raw) <= MAX_BYTES and (length is None or len(raw) == int(length)))
                return 200, json.loads(raw, parse_float=Decimal, object_pairs_hook=unique, parse_constant=invalid)
        except error.HTTPError as exc:
            try:
                return exc.code, None
            finally:
                exc.close()
        except Exception:
            raise Refused('account_check_refused') from None


def collect(wire, reserve, binding, now_ns, monotonic, bounds, check):
    started, mono = now_ns(), monotonic()
    last = started
    require(bounds[0] <= started < bounds[1])
    values, results = {}, []
    for index, path in enumerate(SEQUENCE):
        at = now_ns()
        require(last <= at < bounds[1] and 0 <= monotonic()-mono < MAX_SECONDS)
        last = at
        check()
        reserve(index)
        check()
        before_wire = now_ns()
        require(last <= before_wire < bounds[1] and 0 <= monotonic()-mono < MAX_SECONDS)
        last = before_wire
        status, raw = wire.call('GET', path)
        require(type(status) is int and 100 <= status <= 599)
        received = now_ns()
        require(last <= received < bounds[1] and 0 <= monotonic()-mono < MAX_SECONDS)
        last = received
        results.append({'stage': index+1, 'http_status': status, 'observed_ns': received})
        # Account identity must match retained enrollment before any other read.
        if index == 0:
            if status != 200:
                raise ProviderRefused(index+1, status)
            require(type(raw) is dict and
                    hashlib.sha256(text(raw.get('id')).encode()).hexdigest() == binding)
        if status != 200:
            raise ProviderRefused(index+1, status)
        values[index] = raw
    account, clock = values[0], values[1]
    require(type(clock) is dict and type(clock.get('is_open')) is bool)
    clock_age = last-timestamp(clock.get('timestamp'))
    require(0 <= clock_age <= MAX_SECONDS*NS)
    positions1, positions2 = book(values[2]), book(values[4])
    orders1, orders2 = book(values[3], orders=True), book(values[5], orders=True)
    flags = {}
    for key in ('trading_blocked', 'account_blocked', 'trade_suspended_by_user'):
        require(type(account.get(key)) is bool)
        flags[key] = account[key]
    # Only literal categories/booleans/counts leave the process, never identifiers.
    check()
    return dict(mode='paper_account_diagnostic', status='observed', started_ns=started,
        observed_ns=last, provider_requests=len(results), request_results=results,
        paper_account_binding_matches=True, account_active=account.get('status') == 'ACTIVE',
        account_currency_usd=account.get('currency') == 'USD', **flags,
        market_open=clock['is_open'], position_count=len(positions2),
        open_order_count=len(orders2), open_orders_complete=len(orders1) < 500 and len(orders2) < 500,
        sampled_book_stable=positions1 == positions2 and orders1 == orders2,
        atomic_snapshot=False, execution_reconciliation=False, execution_ready=False,
        account_book_scope='all_returned_positions_and_open_orders',
        market_data_verified=False, universe_eligibility_verified=False,
        source_coverage_verified=False, strategy_ready=False,
        order_permission='none', automatic_retry=False, local_accounting_writes=True)


def exclusive_authorities():
    import finite_pilot_contract as p
    import finite_recovery_successor_contract as s
    import finite_observation_contract as o
    import finite_root_contract as r
    import prepared_successor_activation as a
    import prepared_execution_activation as e
    require(all(x is None for x in (p.APPROVED_PILOT, p.APPROVED_RECOVERY, p.APPROVED_INSPECTION,
                s.APPROVED_SUCCESSOR, o.APPROVED_OBSERVATION, r.APPROVED_ROOT,
                a.APPROVED_ADMISSION, e.APPROVED_EXECUTION)))


def run():
    raw_config = os.environ.get(KEY)
    bounds = config(raw_config)
    if bounds is None:
        return dict(mode='paper_account_diagnostic', status='disabled', provider_requests=0,
                    order_permission='none', automatic_retry=False)
    require(bounds[0] <= time.time_ns() < bounds[1])
    exclusive_authorities()
    import runtime_state_bundle as inventory
    from readonly_runtime_worker import serialized
    from native_account_guard import SharedAccountGuard
    from native_existing_budget import ExistingStateRequestBudget
    seal_path = inventory.FIXED_PATHS['seal']
    identity = inventory._private(seal_path)
    fd = os.open(seal_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require((info.st_dev, info.st_ino) == identity and info.st_size <= 4096)
        raw = os.read(fd, 4097)
    finally:
        os.close(fd)
    require(len(raw) <= 4096 and inventory._private(seal_path) == identity)
    seal = inventory._load(raw)
    bundle = inventory.RuntimeStateBundle(seal['fingerprint'])
    bundle.verify()
    require(bundle._pinned['seal'] == identity)
    with serialized(bundle):
        guard = SharedAccountGuard(bundle.paths['legacy'], bundle.paths['lifecycle'], bundle.binding,
                    enabled=True, state_database=bundle.paths['guard'], runtime_bundle=bundle)
        with guard.hold(now_ns=time.time_ns(), read_only=True):
            guard.admit_cycle(now_ns=time.time_ns())
            budget = ExistingStateRequestBudget(bundle.paths['guard'], guard)
            key, secret = os.environ.get('ALPACA_API_KEY'), os.environ.get('ALPACA_SECRET_KEY')
            wire = HTTP(key, secret)
            attempt = Once(MARKER)
            try:
                def check():
                    require(os.environ.get(KEY) == raw_config and
                        os.environ.get('ALPACA_API_KEY') == key and os.environ.get('ALPACA_SECRET_KEY') == secret)
                    exclusive_authorities()
                    guard.check()
                def reserve(index):
                    attempt.reserve(index)
                    budget.reserve(time.time_ns())
                    guard.reserve_request(time.time_ns())
                result = collect(wire, reserve, bundle.binding, time.time_ns, time.monotonic, bounds, check)
                attempt.append({'event': 'completed', 'provider_requests': attempt.count})
                bundle.verify()
                return result
            finally:
                attempt.close()


def main(argv=None):
    if (sys.argv[1:] if argv is None else argv) != ['--check-paper-once']:
        return 2
    try:
        import resource
        for kind, limit in ((resource.RLIMIT_AS, 128*1024*1024), (resource.RLIMIT_CPU, 8), (resource.RLIMIT_CORE, 0)):
            _, hard = resource.getrlimit(kind)
            cap = limit if hard == resource.RLIM_INFINITY else min(limit, hard)
            resource.setrlimit(kind, (cap, cap))
        signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
        signal.setitimer(signal.ITIMER_REAL, MAX_SECONDS)
        result = run()
        raw = json.dumps(result, sort_keys=True, separators=(',', ':'))
        require(len(raw) <= 8192)
        print(raw, flush=True)
        return 0
    except ProviderRefused as exc:
        print(json.dumps(dict(mode='paper_account_diagnostic', status='provider_refused',
              stage=exc.stage, http_status=exc.status, reserved_attempts=exc.stage, order_permission='none',
              automatic_retry=False, partial_attempt_possible=True)), flush=True)
        return 1
    except Exception:
        print('{"mode":"paper_account_diagnostic","status":"refused","order_permission":"none","automatic_retry":false,"partial_attempt_possible":true}', flush=True)
        return 1
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == '__main__':
    raise SystemExit(main())
