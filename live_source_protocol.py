"""Fixed real-source protocol and pure input validation. Never execution authority."""
from decimal import Decimal
import hashlib
import json
import re

KEY = 'LIVE_SOURCE_STREAM_JSON'
NS = 10**9
MAX_MESSAGE = 1024*1024
MAX_STORE = 64*1024*1024
URLS = {'iex': 'wss://stream.data.alpaca.markets/v2/iex',
        'news': 'wss://stream.data.alpaca.markets/v1beta1/news'}


class Refused(ValueError):
    def __init__(self, code='source_invariant'):
        self.code = code
        super().__init__('source_refused')


def require(ok, code='source_invariant'):
    if not ok: raise Refused(code)


def unique(pairs):
    out = {}
    for key, value in pairs:
        require(key not in out, 'duplicate_key')
        out[key] = value
    return out


def invalid(_): raise Refused('invalid_number')


def decode(raw):
    require(type(raw) is bytes and len(raw) <= MAX_MESSAGE, 'message_bound')
    return json.loads(raw.decode('utf-8'), parse_float=Decimal,
                      object_pairs_hook=unique, parse_constant=invalid)


def config(raw):
    if raw is None: return None
    require(type(raw) is str and 0 < len(raw) <= 2048, 'invalid_grant')
    v = decode(raw.encode())
    require(type(v) is dict and set(v) == {'version','starts_ns','expires_ns','symbols','news','max_bytes','max_attempts'}, 'invalid_grant')
    require(type(v['version']) is int and v['version'] == 1, 'invalid_grant')
    start, end = v['starts_ns'], v['expires_ns']
    require(type(start) is int and type(end) is int and 0 < start < end < 2**63 and end-start <= 72*3600*NS, 'invalid_grant')
    symbols = v['symbols']
    require(type(symbols) is list and 1 <= len(symbols) <= 30 and
            all(type(s) is str and re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,11}',s) for s in symbols) and
            len(set(symbols)) == len(symbols), 'invalid_grant')
    require(v['news'] == '*' and type(v['max_bytes']) is int and
            65536 <= v['max_bytes'] <= MAX_STORE and type(v['max_attempts']) is int and
            1 <= v['max_attempts'] <= 4, 'invalid_grant')
    return v


def subscription(source, grant):
    return {'action':'subscribe', 'quotes':grant['symbols']} if source == 'iex' else {'action':'subscribe','news':['*']}


def control(raw, expected, source, grant):
    rows = decode(raw)
    require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict, 'control_shape')
    row = rows[0]
    if row.get('T') == 'error':
        code = row.get('code')
        require(type(code) is int and code in (400,401,402,403,404,405,406,407,409,410,500), 'provider_error')
        raise Refused('provider_'+str(code))
    if expected in ('connected','authenticated'):
        require(row.get('T') == 'success' and row.get('msg') == expected, 'control_order')
    else:
        require(expected == 'subscription' and row.get('T') == 'subscription', 'subscription_shape')
        name, wanted = ('quotes',grant['symbols']) if source == 'iex' else ('news',['*'])
        require(type(row.get(name)) is list and len(row[name]) == len(wanted) and set(row[name]) == set(wanted), 'subscription_mismatch')
        require(all(k in ('T',name) or type(v) is list and not v for k,v in row.items()), 'unexpected_subscription')
    return expected


def data_summary(raw, source, symbols, received_ns):
    from alpaca_schema import timestamp
    rows = decode(raw)
    require(type(rows) is list and 0 < len(rows) <= 8192, 'data_shape')
    stamps, one, zero, two, fresh_two = [], 0, 0, 0, 0
    news_keys, quote_times, updates = [], {}, []
    for row in rows:
        require(type(row) is dict, 'data_shape')
        if row.get('T') == 'error':
            control(raw, 'subscription', source, {'symbols':list(symbols)})
        if source == 'iex':
            require(row.get('T') == 'q' and row.get('S') in symbols, 'unexpected_channel_or_symbol')
            at = timestamp(row.get('t'))
            bid, ask = row.get('bp'), row.get('ap')
            require(type(bid) in (int,Decimal) and type(ask) in (int,Decimal), 'price_type')
            require(all(Decimal(v).is_finite() and 0 <= v <= Decimal('1e12') for v in (bid,ask)), 'price_bound')
            require(bid == 0 or ask == 0 or bid <= ask, 'crossed_positive_quote')
            require(all(type(row.get(k)) is int and 0 <= row[k] <= 10**12 for k in ('bs','as')), 'size_shape')
            if bid and ask:
                two += 1
                fresh_two += int(0 <= received_ns-at <= 10*NS)
            elif bid == ask == 0: zero += 1
            else: one += 1
            quote_times[row['S']] = max(at,quote_times.get(row['S'],0))
        else:
            require(row.get('T') == 'n' and type(row.get('id')) is int and row['id'] >= 0, 'news_shape')
            at, updated = timestamp(row.get('created_at')), timestamp(row.get('updated_at'))
            require(0 < at <= updated <= received_ns, 'news_time')
            require(type(row.get('symbols')) is list and len(row['symbols']) <= 1024 and
                    all(type(s) is str and len(s) <= 64 for s in row['symbols']), 'news_symbols')
            for field in ('headline','summary','content','url','source','author'):
                require(field not in row or type(row[field]) is str, 'news_text_shape')
            digest = hashlib.sha256(json.dumps(row,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
            news_keys.append((row['id'], updated, digest))
            updates.append(updated)
        require(0 < at <= received_ns, 'provider_time')
        stamps.append(at)
    return dict(records=len(rows), newest_ns=max(stamps), oldest_ns=min(stamps),
        fresh_count=sum(received_ns-at <= (10 if source == 'iex' else 120)*NS for at in stamps),
        one_sided=one,no_sides=zero,two_sided=two,fresh_two_sided=fresh_two,
        quote_times=quote_times,news_keys=news_keys,
        newest_update_ns=max(updates) if updates else None,
        fresh_updates_count=sum(received_ns-at <= 120*NS for at in updates))
