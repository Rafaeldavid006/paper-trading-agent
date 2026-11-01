"""Explicit recurring source-only policy; no implicit renewal of finite grants."""
import re
from live_source_protocol import NS, decode, require

KEY = 'RESIDENT_SOURCE_POLICY_JSON'
DAY = 86400 * NS
PREDECESSOR = {'seq':17, 'bytes':5634,
    'sha256':'0abb98e12cf41bf36ccfdd6a19f338524edfe2a9ec74fbf381afc5e9734e116e'}
TRIAL_GRANT = {'version':1,'starts_ns':1793478130000000000,'expires_ns':1793479930000000000,
    'symbols':['AAPL','MSFT','NVDA','AMZN','META','GOOGL','MU','SPY'], 'news':'*',
    'max_bytes':67108864,'max_attempts':4}


def policy(raw):
    if raw is None: return None
    require(type(raw) is str and 0 < len(raw) <= 4096, 'resident_policy')
    value = decode(raw.encode())
    require(type(value) is dict and set(value) == {'version','authority_id','starts_ns',
        'symbols','news','attempts_per_day','attempts_per_hour','boots_per_day',
        'retained_bytes','retained_seconds','predecessor'}, 'resident_policy')
    require(value['version'] == 2 and type(value['version']) is int, 'resident_policy')
    require(type(value['authority_id']) is str and
        re.fullmatch(r'[a-z0-9_-]{8,80}',value['authority_id']), 'resident_policy')
    require(type(value['starts_ns']) is int and 0 < value['starts_ns'] < 2**63, 'resident_policy')
    symbols = value['symbols']
    require(type(symbols) is list and 1 <= len(symbols) <= 30 and len(set(symbols)) == len(symbols)
        and all(type(s) is str and re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,11}',s) for s in symbols), 'resident_policy')
    require(value['news'] == '*' and value['predecessor'] == PREDECESSOR, 'resident_policy')
    for key,lo,hi in (('attempts_per_day',1,8),('attempts_per_hour',1,4),('boots_per_day',1,8),
        ('retained_bytes',8*1024*1024,96*1024*1024),('retained_seconds',86400,14*86400)):
        require(type(value[key]) is int and lo <= value[key] <= hi, 'resident_policy')
    require(value['attempts_per_hour'] <= value['attempts_per_day'], 'resident_policy')
    return value
