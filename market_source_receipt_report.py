"""Read the completed private two-GET receipt; zero network, writes or keys.

Outputs fixed shape/rejection labels and timestamps only. One-sided quote
classification is descriptive, not executable-price acceptance or a new signal.
"""
import base64
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys

PATH = '/var/data/market-source-check-once-v1.jsonl'
SYMBOLS = ('AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'MU', 'SPY')
MAX_FILE = 6*1024*1024
MAX_BODY = 2*1024*1024


def require(ok):
    if not ok:
        raise ValueError('receipt_report_refused')


def unique(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value)
        value[key] = item
    return value


def invalid(_):
    raise ValueError('receipt_report_refused')


def decode(raw):
    return json.loads(raw.decode('utf-8'), parse_float=Decimal,
                      object_pairs_hook=unique, parse_constant=invalid)


def file_bytes(path=PATH, *, test_only=False):
    require(test_only is True or path == PATH)
    for parent in reversed(Path(path).parents):
        require(stat.S_ISDIR(parent.stat(follow_symlinks=False).st_mode))
    require(not any(os.path.lexists(path+s) for s in ('-journal','-wal','-shm')))
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and stat.S_IMODE(before.st_mode) == 0o600
                and before.st_uid == os.geteuid() and before.st_nlink == 1
                and 0 < before.st_size <= MAX_FILE)
        parts, size = [], 0
        while True:
            piece = os.read(fd, min(65536, MAX_FILE+1-size))
            if not piece:
                break
            parts.append(piece);size += len(piece)
            require(size <= MAX_FILE)
        raw = b''.join(parts)
        after, named = os.fstat(fd), os.stat(path, follow_symlinks=False)
        stamp = lambda s: (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_mode,s.st_nlink)
        require(len(raw) == before.st_size and stamp(before) == stamp(after) == stamp(named))
        return raw
    finally:
        os.close(fd)


def receipts(raw):
    lines = raw.splitlines()
    require(len(lines) == 6)
    rows = [decode(line) for line in lines]
    require(all(type(row) is dict for row in rows))
    require([row.get('event') for row in rows] ==
            ['started','reserved','receipt','reserved','receipt','completed'])
    require(rows[0].get('version') == 1 and rows[0].get('max_gets') == 2 and
            rows[1].get('attempt') == 1 and rows[3].get('attempt') == 2 and
            rows[5].get('request_attempts') == 2)
    previous = rows[0].get('started_ns')
    require(type(previous) is int and previous > 0)
    output = []
    for source, row in zip(('alpaca_news','iex_quotes'), (rows[2],rows[4])):
        require(row.get('source') == source and row.get('http_status') == 200)
        before, received = row.get('request_started_ns'), row.get('received_ns')
        require(type(before) is int and type(received) is int and previous <= before <= received < 2**63)
        previous = received
        path = row.get('request_path')
        require(type(path) is str and len(path) <= 2048)
        require(path.startswith('/v1beta1/news?') if source == 'alpaca_news' else
                path == '/v2/stocks/quotes/latest?symbols='+','.join(SYMBOLS)+'&feed=iex')
        digest, encoded = row.get('body_sha256'), row.get('body_base64')
        require(type(digest) is str and re.fullmatch('[a-f0-9]{64}',digest) and
                type(encoded) is str and len(encoded) <= (MAX_BODY+2)//3*4)
        body = base64.b64decode(encoded,validate=True)
        require(len(body) <= MAX_BODY and hashlib.sha256(body).hexdigest() == digest)
        output.append((source,body,received,digest))
    require(rows[5].get('observed_ns') == previous)
    return output


def quote_shape(body, received):
    from alpaca_schema import timestamp
    try:
        data = decode(body)
    except Exception:
        return dict(result='invalid_json')
    if type(data) is not dict:
        return dict(result='envelope_not_object')
    if data.get('feed','iex') != 'iex':
        return dict(result='unexpected_feed')
    quotes = data.get('quotes')
    if type(quotes) is not dict:
        return dict(result='quotes_not_object')
    unknown = len(set(quotes)-set(SYMBOLS))
    result = dict(result='shape_observed', returned_symbol_count=len(quotes),
                  missing_symbol_count=len(set(SYMBOLS)-set(quotes)),
                  unexpected_symbol_count=unknown, symbols=[])
    for symbol in SYMBOLS:
        if symbol not in quotes:
            continue
        row, reasons = quotes[symbol], []
        shape = dict(symbol=symbol)
        if type(row) is not dict:
            shape['original_validator_reasons'] = ['quote_not_object']
            result['symbols'].append(shape)
            continue
        try:
            at = timestamp(row.get('t'))
            shape['provider_timestamp_ns'] = at
            shape['age_at_receipt_seconds'] = (received-at)//10**9
            if not 0 < at <= received:
                reasons.append('timestamp_nonpositive_or_future')
        except Exception:
            reasons.append('timestamp_invalid')
        bid, ask = row.get('bp'), row.get('ap')
        if type(bid) not in (int,Decimal) or type(ask) not in (int,Decimal):
            reasons.append('price_type_invalid')
        elif not Decimal(bid).is_finite() or not Decimal(ask).is_finite():
            reasons.append('price_nonfinite')
        else:
            shape.update(bid_zero=bid == 0, ask_zero=ask == 0,
                         bid_greater_than_ask=bid > ask)
            if bid < 0 or ask < 0:
                reasons.append('negative_price')
            if bid > ask:
                reasons.append('bid_greater_than_ask')
            if ask > Decimal('1e12'):
                reasons.append('ask_above_bound')
            shape['one_sided_zero_price'] = (bid == 0) != (ask == 0)
        shape['original_validator_reasons'] = reasons
        result['symbols'].append(shape)
    result['original_validator_passes'] = unknown == 0 and all(
        not row['original_validator_reasons'] for row in result['symbols'])
    return result


def report(raw):
    output = dict(mode='market_source_receipt_report',status='observed_saved_receipts',
        provider_requests=0,credentials_read=False,state_writes=0,order_permission='none',
        execution_ready=False,source_coverage_verified=False,realtime_entitlement_verified=False,
        receipt_sha256=hashlib.sha256(raw).hexdigest(),sources=[])
    for source, body, received, digest in receipts(raw):
        item = dict(source=source,http_status=200,received_ns=received,body_sha256=digest,body_bytes=len(body))
        if source == 'iex_quotes':
            item['shape'] = quote_shape(body,received)
        else:
            data = decode(body)
            require(type(data) is dict and type(data.get('news')) is list)
            item['item_count'] = len(data['news'])
        output['sources'].append(item)
    return output


def main(argv=None):
    if (sys.argv[1:] if argv is None else argv) != ['--read-existing-receipt']:
        return 2
    try:
        import resource
        for kind,limit in ((resource.RLIMIT_AS,128*1024*1024),(resource.RLIMIT_CPU,4),(resource.RLIMIT_CORE,0)):
            _,hard=resource.getrlimit(kind)
            cap=limit if hard == resource.RLIM_INFINITY else min(limit,hard)
            resource.setrlimit(kind,(cap,cap))
        signal.signal(signal.SIGALRM,lambda *_:os._exit(124))
        signal.setitimer(signal.ITIMER_REAL,8)
        raw = json.dumps(report(file_bytes()),sort_keys=True,separators=(',',':'))
        require(len(raw) <= 8192)
        print(raw,flush=True)
        return 0
    except Exception:
        print('{"mode":"market_source_receipt_report","status":"refused","provider_requests":0,"credentials_read":false,"state_writes":0,"order_permission":"none"}',flush=True)
        return 1
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)


if __name__ == '__main__':
    raise SystemExit(main())
