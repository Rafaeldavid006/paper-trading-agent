"""Optional two-GET source acquisition. No orders, AI, subscription or retry.

Provider market/news bytes stay in a new private create-once receipt. Public
output contains only fixed labels, counts, HTTP codes and source/receipt times.
This is not a stream, complete market coverage or execution authorization.
"""
import base64
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys
import time
from urllib import request, error
from urllib.parse import urlencode

KEY = 'MARKET_SOURCE_CHECK_ONCE_JSON'
MARKER = '/var/data/market-source-check-once-v1.jsonl'
ORIGIN = 'https://data.alpaca.markets'
SYMBOLS = ('AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'MU', 'SPY')
MAX_GETS = 2
MAX_BYTES = 2 * 1024 * 1024
MAX_SECONDS = 20
NS = 10**9


class Refused(ValueError):
    pass


class RequestFailed(Refused):
    def __init__(self, source, attempts):
        self.source, self.attempts = source, attempts
        super().__init__('source_request_failed')


def require(ok):
    if not ok:
        raise Refused('source_check_refused')


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def invalid(_):
    raise Refused('source_check_refused')


def decode(raw):
    return json.loads(raw.decode('utf-8'), parse_float=Decimal,
                      object_pairs_hook=unique, parse_constant=invalid)


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


def routes(started_ns):
    require(type(started_ns) is int and 0 < started_ns < 2**63)
    start = datetime.fromtimestamp(started_ns // NS, timezone.utc)-timedelta(hours=1)
    return (
        ('alpaca_news', '/v1beta1/news?'+urlencode(dict(symbols=','.join(SYMBOLS),
            start=start.isoformat(), limit='50', sort='asc', include_content='false'))),
        ('iex_quotes', '/v2/stocks/quotes/latest?symbols='+','.join(SYMBOLS)+'&feed=iex'),
    )


class Once:
    """Any existing marker consumes this attempt, including an interrupted one."""
    def __init__(self, path, started_ns):
        from runtime_state_bundle import _parents
        self.path, self.fd, self.count = str(path), None, 0
        _parents(self.path)
        require(not any(os.path.lexists(self.path+s) for s in ('-journal', '-wal', '-shm')))
        self.fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(self.fd)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                    info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600)
            self.identity = info.st_dev, info.st_ino
            self.append(dict(event='started', version=1, started_ns=started_ns, max_gets=MAX_GETS))
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
        require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600 and
                info.st_uid == os.geteuid() and info.st_nlink == 1 and
                (info.st_dev, info.st_ino) == self.identity)
        raw = (json.dumps(value, sort_keys=True, separators=(',', ':'))+'\n').encode()
        require(len(raw) < 3*1024*1024)
        while raw:
            count = os.write(self.fd, raw)
            require(count > 0)
            raw = raw[count:]
        os.fsync(self.fd)
        again = os.stat(self.path, follow_symlinks=False)
        require((again.st_dev, again.st_ino) == self.identity)

    def reserve(self, index):
        require(type(index) is int and index == self.count and index < MAX_GETS)
        self.count += 1
        self.append(dict(event='reserved', attempt=self.count))

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class HTTP:
    """Exact two prebuilt GET URLs, no redirects/proxies/retries/error bodies."""
    def __init__(self, key, secret, planned_routes):
        from paper_adapter import NoRedirect
        require(all(type(x) is str and 0 < len(x) <= 512 and
                    all(33 <= ord(c) <= 126 for c in x) for x in (key, secret)))
        require(type(planned_routes) is tuple and len(planned_routes) == MAX_GETS)
        self.allowed = dict(planned_routes)
        self.__headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret,
                          'Accept': 'application/json', 'Accept-Encoding': 'identity'}
        self.__opener = request.build_opener(request.ProxyHandler({}), NoRedirect())

    def get(self, source, path):
        require(source in ('alpaca_news', 'iex_quotes') and self.allowed.get(source) == path)
        require(path.startswith('/v1beta1/news?') if source == 'alpaca_news' else
                path == '/v2/stocks/quotes/latest?symbols='+','.join(SYMBOLS)+'&feed=iex')
        url = ORIGIN+path
        try:
            with self.__opener.open(request.Request(url, headers=self.__headers, method='GET'), timeout=3) as response:
                require(response.geturl() == url and response.status == 200)
                require(response.headers.get('Content-Encoding', 'identity') in ('identity', ''))
                length = response.headers.get('Content-Length')
                require(length is None or (re.fullmatch(r'\d{1,9}', length) and int(length) <= MAX_BYTES))
                raw = response.read(MAX_BYTES+1)
                require(len(raw) <= MAX_BYTES and (length is None or len(raw) == int(length)))
                return 200, raw
        except error.HTTPError as exc:
            try:
                return exc.code, None
            finally:
                exc.close()
        except Exception:
            raise Refused('source_check_refused') from None


def source_summary(source, raw, received_ns):
    from alpaca_schema import timestamp
    data = decode(raw)
    require(type(data) is dict)
    if source == 'alpaca_news':
        rows, token = data.get('news'), data.get('next_page_token')
        require(type(rows) is list and len(rows) <= 50 and
                (token is None or type(token) is str and 0 < len(token) <= 8192))
        published, updated = [], []
        for row in rows:
            require(type(row) is dict and type(row.get('id')) is int and row['id'] >= 0)
            p, u = timestamp(row.get('created_at')), timestamp(row.get('updated_at'))
            require(0 < p <= u <= received_ns)
            published.append(p)
            updated.append(u)
        return dict(item_count=len(rows), pagination_present=bool(token),
            response_pagination_exhausted=not bool(token),
            newest_published_ns=max(published) if published else None,
            newest_updated_ns=max(updated) if updated else None,
            newest_publication_age_seconds=(received_ns-max(published))//NS if published else None,
            recent_article_observed=bool(published) and received_ns-max(published) <= 120*NS,
            realtime_entitlement_verified=False, streaming=False,
            result='received' if rows else 'received_empty')
    require(source == 'iex_quotes' and data.get('feed', 'iex') == 'iex')
    quotes = data.get('quotes')
    require(type(quotes) is dict and set(quotes).issubset(SYMBOLS))
    stamps = []
    for symbol, row in quotes.items():
        require(type(row) is dict)
        stamp = timestamp(row.get('t'))
        bid, ask = row.get('bp'), row.get('ap')
        require(type(bid) in (int, Decimal) and type(ask) in (int, Decimal))
        require(Decimal(bid).is_finite() and Decimal(ask).is_finite() and
                0 <= bid <= ask <= Decimal('1e12') and 0 < stamp <= received_ns)
        stamps.append(stamp)
    fresh_count = sum(received_ns-stamp <= 10*NS for stamp in stamps)
    return dict(feed='iex', requested_symbol_count=len(SYMBOLS), returned_symbol_count=len(stamps),
        missing_symbol_count=len(SYMBOLS)-len(stamps), fresh_within_10s_count=fresh_count,
        newest_quote_ns=max(stamps) if stamps else None,
        oldest_quote_ns=min(stamps) if stamps else None,
        newest_quote_age_seconds=(received_ns-max(stamps))//NS if stamps else None,
        streaming=False, tradability_verified=False,
        result=('received_empty' if not stamps else 'received_fresh' if fresh_count == len(SYMBOLS)
                else 'received_stale_or_partial'))


def collect(wire, planned_routes, reserve, append, now_ns, monotonic, bounds, check):
    started, mono = now_ns(), monotonic()
    require(bounds[0] <= started < bounds[1] and len(planned_routes) == MAX_GETS)
    last, results = started, []
    for index, (source, path) in enumerate(planned_routes):
        at = now_ns()
        require(last <= at < bounds[1] and 0 <= monotonic()-mono < MAX_SECONDS)
        check()
        reserve(index)
        check()
        before = now_ns()
        require(at <= before < bounds[1] and 0 <= monotonic()-mono < MAX_SECONDS)
        try:
            status, raw = wire.get(source, path)
        except Exception:
            append(dict(event='request_failed', source=source, request_path=path,
                        request_started_ns=before, attempt=index+1))
            raise RequestFailed(source, index+1) from None
        received = now_ns()
        require(before <= received < bounds[1] and 0 <= monotonic()-mono < MAX_SECONDS)
        last = received
        require(type(status) is int and 100 <= status <= 599)
        summary = dict(source=source, http_status=status, observed_ns=received)
        receipt = dict(event='receipt', source=source, request_path=path,
                       request_started_ns=before, received_ns=received, http_status=status)
        if status == 200:
            require(type(raw) is bytes and len(raw) <= MAX_BYTES)
            receipt.update(body_sha256=hashlib.sha256(raw).hexdigest(),
                           body_base64=base64.b64encode(raw).decode('ascii'))
            # Preserve actual evidence even if its provider schema is unusable.
            append(receipt)
            try:
                summary.update(source_summary(source, raw, received))
            except Exception:
                summary.update(result='invalid_source_payload', realtime_entitlement_verified=False)
        else:
            require(raw is None)
            append(receipt)
            summary.update(result='provider_refused', realtime_entitlement_verified=False)
        check()
        results.append(summary)
    append(dict(event='completed', request_attempts=len(results), observed_ns=last))
    outcome = 'sources_received' if all(r['result'].startswith('received') for r in results) else 'source_unavailable'
    return dict(mode='market_source_check', status=outcome, started_ns=started,
        observed_ns=last, request_attempts=len(results), sources=results,
        private_source_receipts_retained=True, live_http_count_exact=False,
        source_coverage_verified=False, account_binding_verified=False,
        streaming=False, execution_ready=False, order_permission='none',
        automatic_retry=False, local_accounting_writes=True)


def run():
    raw_config = os.environ.get(KEY)
    bounds = config(raw_config)
    if bounds is None:
        return dict(mode='market_source_check', status='disabled', request_attempts=0,
                    order_permission='none', automatic_retry=False)
    require(bounds[0] <= time.time_ns() < bounds[1])
    from paper_account_diagnostic import exclusive_authorities
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
            planned_routes = routes(time.time_ns())
            wire = HTTP(key, secret, planned_routes)
            attempt = Once(MARKER, time.time_ns())
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
                result = collect(wire, planned_routes, reserve, attempt.append,
                                 time.time_ns, time.monotonic, bounds, check)
                bundle.verify()
                return result
            finally:
                attempt.close()


def main(argv=None):
    if (sys.argv[1:] if argv is None else argv) != ['--collect-source-once']:
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
    except RequestFailed as exc:
        print(json.dumps(dict(mode='market_source_check', status='source_request_failed',
              source=exc.source, reserved_attempts=exc.attempts, order_permission='none',
              automatic_retry=False, partial_attempt_possible=True)), flush=True)
        return 1
    except Exception:
        print('{"mode":"market_source_check","status":"refused","order_permission":"none","automatic_retry":false,"partial_attempt_possible":true}', flush=True)
        return 1
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == '__main__':
    raise SystemExit(main())
