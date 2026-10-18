"""All network calls are mocked. No real credentials or external requests."""
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
import json
import unittest
from unittest.mock import Mock, patch
from urllib import error, request

from paper_adapter import NoRedirect, SafetyError
from paper_quotes import (DATA_HOST, QUOTE_PATH, MAX_RESPONSE_BYTES, IEXQuoteHTTP,
                          Quote, parse_quote)


NOW = datetime(2026, 10, 19, 15, 0, 0, tzinfo=timezone.utc)


def sample(**changes):
    quote = {'t': NOW.isoformat(), 'bp': '200.00', 'ap': '200.02',
             'bx': 'V', 'ax': 'V', 'bs': 1, 'as': 1}
    quote.update(changes)
    return {'symbol': 'AAPL', 'quote': quote}


class QuoteParsingTests(unittest.TestCase):
    def test_returns_immutable_decimal_fields_and_utc_timestamp(self):
        result = parse_quote(sample(t='2026-10-19T11:00:00-04:00'), now=NOW)
        self.assertEqual(Quote('AAPL', Decimal('200.00'), Decimal('200.02'), NOW), result)
        self.assertIs(type(result.bid), Decimal)
        self.assertIs(result.timestamp.tzinfo, timezone.utc)
        with self.assertRaises(FrozenInstanceError):
            result.bid = Decimal(1)

    def test_nanosecond_timestamp_is_supported(self):
        quote = parse_quote(sample(t='2026-10-19T14:59:59.123456789Z'), now=NOW)
        self.assertEqual(123456, quote.timestamp.microsecond)

    def test_nanosecond_precision_does_not_accept_future_or_stale_quote(self):
        for stamp in ('2026-10-19T15:00:00.000000001Z',
                      '2026-10-19T14:59:49.999999999Z'):
            with self.subTest(stamp=stamp), self.assertRaises(SafetyError):
                parse_quote(sample(t=stamp), now=NOW)
        parse_quote(sample(t='2026-10-19T14:59:50.000000001Z'), now=NOW)

    def test_freshness_boundaries(self):
        for age in (0, 1, 9.999999, 10):
            with self.subTest(age=age):
                parse_quote(sample(t=(NOW-timedelta(seconds=age)).isoformat()), now=NOW)
        for age in (-1, -0.000001, 10.000001, 11, 86400):
            with self.subTest(age=age), self.assertRaises(SafetyError):
                parse_quote(sample(t=(NOW-timedelta(seconds=age)).isoformat()), now=NOW)

    def test_one_percent_of_bid_boundary_and_locked_quote(self):
        parse_quote(sample(bp='100', ap='101'), now=NOW)
        parse_quote(sample(bp='100', ap='100'), now=NOW)
        for ask in ('99.99', '101.000001', '101.00000000000000000000000000001', '102'):
            with self.subTest(ask=ask), self.assertRaises(SafetyError):
                parse_quote(sample(bp='100', ap=ask), now=NOW)

    def test_nonpositive_nonfinite_and_malformed_prices_fail_closed(self):
        values = (None, True, False, '', 'broken', '-1', '0', 'NaN', 'sNaN',
                  'Infinity', '-Infinity', float('nan'), [], {}, '1e999999999')
        for field in ('bp', 'ap'):
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaises(SafetyError):
                    parse_quote(sample(**{field: value}), now=NOW)

    def test_missing_wrong_symbol_or_quote_fails_closed(self):
        for payload in (None, [], {}, {'symbol': 'MSFT', 'quote': sample()['quote']},
                        {'symbol': 'aapl', 'quote': sample()['quote']},
                        {'symbol': 'AAPL'}, {'symbol': 'AAPL', 'quote': []}):
            with self.subTest(payload=payload), self.assertRaises(SafetyError):
                parse_quote(payload, now=NOW)
        for field in ('bp', 'ap', 't'):
            payload = sample()
            del payload['quote'][field]
            with self.subTest(field=field), self.assertRaises(SafetyError):
                parse_quote(payload, now=NOW)

    def test_invalid_and_naive_timestamps_fail_closed(self):
        for stamp in (None, 123, '', '2026-10-19T15:00:00', '2026-10-19',
                      '2026-99-19T15:00:00Z', '2026-10-19 15:00:00+00:00',
                      '2026-10-19T15:00:00+00:99', '2026-10-19T15:00:00+24:00',
                      '2026-10-19T15:00:00.1234567890Z'):
            with self.subTest(stamp=stamp), self.assertRaises(SafetyError):
                parse_quote(sample(t=stamp), now=NOW)
        for current in (NOW.replace(tzinfo=None), 0, '2026-10-19T15:00:00Z'):
            with self.subTest(current=current), self.assertRaises(SafetyError):
                parse_quote(sample(), now=current)


class QuoteTransportTests(unittest.TestCase):
    def setUp(self):
        self.builder = patch('paper_quotes.request.build_opener')
        self.build_opener = self.builder.start()
        self.addCleanup(self.builder.stop)
        self.opener = self.build_opener.return_value
        self.transport = IEXQuoteHTTP('fixture-key', 'fixture-secret', now=lambda: NOW)
        self.response = self.opener.open.return_value.__enter__.return_value
        self.response.status = 200
        self.response.geturl.return_value = DATA_HOST + QUOTE_PATH
        self.response.read.return_value = json.dumps(sample()).encode()

    def test_constructor_never_connects_and_disables_proxies_and_redirects(self):
        self.opener.open.assert_not_called()
        handlers = self.build_opener.call_args.args
        self.assertEqual(2, len(handlers))
        self.assertIsInstance(handlers[0], request.ProxyHandler)
        self.assertEqual({}, handlers[0].proxies)
        self.assertIsInstance(handlers[1], NoRedirect)
        with self.assertRaises(SafetyError):
            handlers[1].redirect_request(None, None, 302, None, None, 'https://example.invalid')

    def test_fixed_get_url_headers_no_body_and_decimal_result(self):
        result = self.transport.latest_quote()
        req = self.opener.open.call_args.args[0]
        self.assertEqual(DATA_HOST + QUOTE_PATH, req.full_url)
        self.assertEqual('GET', req.get_method())
        self.assertIsNone(req.data)
        self.assertEqual('fixture-key', req.get_header('Apca-api-key-id'))
        self.assertEqual('fixture-secret', req.get_header('Apca-api-secret-key'))
        self.assertEqual({'timeout': 10}, self.opener.open.call_args.kwargs)
        self.response.read.assert_called_once_with(MAX_RESPONSE_BYTES + 1)
        self.assertEqual(Decimal('200.02'), result.ask)

    def test_json_numeric_precision_is_not_converted_through_float(self):
        self.response.read.return_value = (b'{"symbol":"AAPL","quote":{"bp":200.00000000000001,'
                                          b'"ap":200.00000000000002,"t":"2026-10-19T15:00:00Z"}}')
        quote = self.transport.latest_quote()
        self.assertEqual(Decimal('200.00000000000001'), quote.bid)
        self.assertEqual(Decimal('200.00000000000002'), quote.ask)

    def test_all_alternate_operations_fail_before_network(self):
        for method in ('POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'get'):
            with self.subTest(method=method), self.assertRaises(SafetyError):
                self.transport.call(method, QUOTE_PATH)
        for path in (QUOTE_PATH.replace('AAPL', 'MSFT'), QUOTE_PATH.replace('iex', 'sip'),
                     QUOTE_PATH.replace('?feed=iex', ''), QUOTE_PATH+'&currency=USD',
                     DATA_HOST+QUOTE_PATH, '/v2/account', '/v2/orders', '//example.invalid'):
            with self.subTest(path=path), self.assertRaises(SafetyError):
                self.transport.call('GET', path)
        for body in ({}, '', b'', False):
            with self.subTest(body=body), self.assertRaises(SafetyError):
                self.transport.call('GET', QUOTE_PATH, body)
        self.opener.open.assert_not_called()

    def test_alternate_hosts_and_invalid_credentials_rejected_before_construction(self):
        self.build_opener.reset_mock()
        for host in (DATA_HOST+'/', DATA_HOST+':443', 'http://data.alpaca.markets',
                     'https://api.alpaca.markets', 'https://paper-api.alpaca.markets',
                     DATA_HOST+'.example.invalid'):
            with self.subTest(host=host), self.assertRaises(SafetyError):
                IEXQuoteHTTP('fixture-key', 'fixture-secret', base_url=host)
        for bad in (None, '', 123, True, 'line\nbreak', 'line\rbreak', 'x\x00y', 'has space'):
            with self.subTest(bad=bad), self.assertRaises(SafetyError):
                IEXQuoteHTTP(bad, 'fixture-secret')
            with self.subTest(bad_secret=bad), self.assertRaises(SafetyError):
                IEXQuoteHTTP('fixture-key', bad)
        self.build_opener.assert_not_called()

    def test_response_url_mismatch_fails_before_read(self):
        self.response.geturl.return_value = 'https://example.invalid/'
        with self.assertRaises(SafetyError):
            self.transport.latest_quote()
        self.response.read.assert_not_called()

    def test_http_failure_never_reads_or_discloses_response_or_retries(self):
        raw = Mock(wraps=io.BytesIO(b'fixture-secret: private provider body'))
        self.opener.open.side_effect = error.HTTPError(DATA_HOST+QUOTE_PATH, 401,
                                                      'fixture-key', {}, raw)
        with self.assertRaises(SafetyError) as caught:
            self.transport.latest_quote()
        self.assertEqual('Quote read failed', str(caught.exception))
        raw.read.assert_not_called()
        raw.close.assert_called_once_with()
        self.assertEqual(1, self.opener.open.call_count)

    def test_unexpected_status_never_reads_response(self):
        for status in (204, 301, 302, 403, 429, 500):
            self.response.status = status
            with self.subTest(status=status), self.assertRaises(SafetyError):
                self.transport.latest_quote()
        self.response.read.assert_not_called()

    def test_transport_failures_are_sanitized_without_retry(self):
        for failure in (error.URLError('fixture-secret'), TimeoutError('fixture-key'),
                        RuntimeError('private provider details')):
            self.opener.open.reset_mock(side_effect=True)
            self.opener.open.side_effect = failure
            with self.subTest(failure=type(failure)), self.assertRaises(SafetyError) as caught:
                self.transport.latest_quote()
            self.assertEqual('Quote request failed', str(caught.exception))
            self.assertEqual(1, self.opener.open.call_count)

    def test_invalid_oversized_duplicate_and_nonfinite_json_fails_closed(self):
        payloads = (b'', b'not json', b'\xff', b' '* (MAX_RESPONSE_BYTES+1),
                    b'{"symbol":"MSFT","symbol":"AAPL"}',
                    b'{"symbol":"AAPL","quote":{"bp":NaN}}',
                    b'{"symbol":"AAPL","quote":{"ap":Infinity}}')
        for payload in payloads:
            self.response.read.return_value = payload
            with self.subTest(payload=payload[:80]), self.assertRaises(SafetyError):
                self.transport.latest_quote()

    def test_freshness_is_checked_after_receiving_the_response(self):
        current = [NOW]
        self.transport.now = lambda: current[0]
        def read(_):
            current[0] += timedelta(seconds=11)
            return json.dumps(sample()).encode()
        self.response.read.side_effect = read
        with self.assertRaises(SafetyError):
            self.transport.latest_quote()

    def test_invalid_clock_failure_never_leaks_details(self):
        self.transport.now = Mock(side_effect=RuntimeError('fixture-secret'))
        with self.assertRaises(SafetyError) as caught:
            self.transport.latest_quote()
        self.assertEqual('Invalid quote validation clock', str(caught.exception))
        for value in (None, 0, '2026-10-19T15:00:00Z', NOW.replace(tzinfo=None)):
            self.transport.now = lambda: value
            with self.subTest(value=value), self.assertRaises(SafetyError):
                self.transport.latest_quote()


if __name__ == '__main__':
    unittest.main()
