"""Isolated, read-only AAPL/IEX quote transport for the bounded Paper test.

No environment reads, credential discovery, broker writes, caching or retries.
IEX is one exchange, not a consolidated NBBO. Constructing the transport makes
no requests; credentials must be supplied by the separately authorized caller.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, Underflow, localcontext
import json
import re
from urllib import error, request

from paper_adapter import NoRedirect, SafetyError


DATA_HOST = 'https://data.alpaca.markets'
QUOTE_PATH = '/v2/stocks/AAPL/quotes/latest?feed=iex'
MAX_QUOTE_AGE_SECONDS = 10
MAX_SPREAD_FRACTION = Decimal('0.01')
MAX_RESPONSE_BYTES = 65_536
_TIMESTAMP = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
                        r'(?:\.(?P<fraction>\d{1,9}))?'
                        r'(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)')


@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: Decimal
    ask: Decimal
    timestamp: datetime


def _price(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise SafetyError('Invalid quote price')
    try:
        number = Decimal(str(value))
        # Reject pathological numeric payloads before doing monetary arithmetic.
        if not number.is_finite() or not 0 < number <= Decimal('1e12'):
            raise SafetyError('Invalid quote price')
        return number
    except (DecimalException, ValueError):
        raise SafetyError('Invalid quote price') from None


def parse_quote(payload, *, now=None):
    """Validate a latest-quote response and return only the required safe fields.

    Prices are Decimal. Quotes must be 0..10 seconds old at validation time,
    uncrossed, and no wider than 1% of bid. Naive/future timestamps fail closed.
    The supplied clock is an aware datetime, not an epoch or local-time guess.
    """
    if not isinstance(payload, dict) or payload.get('symbol') != 'AAPL':
        raise SafetyError('Invalid quote symbol')
    quote = payload.get('quote')
    if not isinstance(quote, dict):
        raise SafetyError('Invalid quote response')
    bid, ask = _price(quote.get('bp')), _price(quote.get('ap'))
    try:
        # Compare against an exact upper bound, not a rounded subtraction. A
        # price just beyond 1% must not pass through Decimal context rounding.
        with localcontext() as context:
            context.prec = max(28, len(bid.as_tuple().digits) + 4)
            context.traps[Underflow] = True
            if ask < bid or ask > bid * (1 + MAX_SPREAD_FRACTION):
                raise SafetyError('Quote spread is crossed or too wide')
    except DecimalException:
        raise SafetyError('Invalid quote price') from None
    stamp = quote.get('t')
    match = _TIMESTAMP.fullmatch(stamp) if isinstance(stamp, str) else None
    if match is None:
        raise SafetyError('Invalid quote timestamp')
    try:
        timestamp = datetime.fromisoformat(stamp.replace('Z', '+00:00')).astimezone(timezone.utc)
        current = datetime.now(timezone.utc) if now is None else now
        if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() is None:
            raise SafetyError('Invalid quote validation clock')
        delta = current - timestamp
        age_microseconds = ((delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds)
        # datetime truncates Alpaca's nanoseconds; retain that remainder for the
        # strict freshness/future checks. Returned timestamp is conservative.
        fraction = (match.group('fraction') or '').ljust(9, '0')
        age_nanoseconds = age_microseconds * 1000 - int(fraction[6:])
    except (ValueError, OverflowError, TypeError):
        raise SafetyError('Invalid quote timestamp') from None
    if not 0 <= age_nanoseconds <= MAX_QUOTE_AGE_SECONDS * 1_000_000_000:
        raise SafetyError('Quote is stale or from the future')
    return Quote(symbol='AAPL', bid=bid, ask=ask, timestamp=timestamp)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SafetyError('Duplicate quote response field')
        result[key] = value
    return result


def _invalid_constant(value):
    raise SafetyError('Invalid quote response number')


class IEXQuoteHTTP:
    """Fixed-host GET-only transport; no proxy, redirect or feed fallback.

    call() follows PaperHTTP's (status, payload) interface but accepts only the
    single literal quote route. latest_quote() additionally validates all fields.
    Neither errors nor returned values include credentials or provider messages.
    """
    def __init__(self, key, secret, base_url=DATA_HOST, *, now=None):
        if base_url != DATA_HOST:
            raise SafetyError('Only the exact market-data host is allowed')
        if any(not isinstance(value, str) or not value or any(ord(c) < 33 or ord(c) > 126 for c in value)
               for value in (key, secret)):
            raise SafetyError('Injected market-data credentials required')
        self._headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret,
                         'Accept': 'application/json'}
        self._opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
        self.now = (lambda: datetime.now(timezone.utc)) if now is None else now

    def call(self, method, path, body=None):
        if method != 'GET' or path != QUOTE_PATH or body is not None:
            raise SafetyError('Only the fixed AAPL IEX quote GET is allowed')
        try:
            req = request.Request(DATA_HOST + QUOTE_PATH, headers=self._headers, method='GET')
            with self._opener.open(req, timeout=10) as response:
                if response.geturl() != DATA_HOST + QUOTE_PATH:
                    raise SafetyError('Quote response URL mismatch')
                if response.status != 200:
                    return response.status, None
                data = response.read(MAX_RESPONSE_BYTES + 1)
                if len(data) > MAX_RESPONSE_BYTES:
                    raise SafetyError('Oversized quote response')
                payload = json.loads(data, parse_float=Decimal, parse_constant=_invalid_constant,
                                     object_pairs_hook=_unique_object)
                return 200, payload
        except error.HTTPError as exc:
            code = exc.code
            try:
                exc.close()
            except Exception:
                pass
            return code, None  # Never return/read raw provider error bodies.
        except Exception:
            raise SafetyError('Quote request failed') from None

    def latest_quote(self):
        status, payload = self.call('GET', QUOTE_PATH)
        if status != 200:
            raise SafetyError('Quote read failed')
        try:
            current = self.now()
        except Exception:
            raise SafetyError('Invalid quote validation clock') from None
        if not isinstance(current, datetime):
            raise SafetyError('Invalid quote validation clock')
        return parse_quote(payload, now=current)
