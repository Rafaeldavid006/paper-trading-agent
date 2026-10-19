"""Offline-only transport tests. All credentials are synthetic fixtures."""
import io
import json
import os
import unittest
from unittest.mock import Mock, patch
from urllib import error, request
import monitor

class Response(io.BytesIO):
    status = 200
    def __init__(self, body, url=None, chunk_size=None):
        super().__init__(body)
        self.url = url
        self.chunk_size = chunk_size
        self.read_sizes = []
    def geturl(self): return self.url
    def read(self, size=-1):
        self.read_sizes.append(size)
        assert size > 0, 'Unbounded read forbidden'
        return super().read(min(size, self.chunk_size) if self.chunk_size else size)

class NewsTransportTests(unittest.TestCase):
    def setUp(self):
        self.params = dict(symbols='AAPL', start='2026-01-01T00:00:00+00:00', limit=50,
                           sort='asc', include_content='false')
    def call(self): return monitor._news_page(self.params, 'fixture-key', 'fixture-secret')
    def body(self, body, **kwargs):
        response = Response(body, **kwargs)
        opener = Mock()
        def open_response(req, timeout):
            self.assertEqual(timeout, 10)
            self.assertEqual(req.host, 'data.alpaca.markets')
            self.assertEqual(req.type, 'https')
            self.assertEqual(req.get_method(), 'GET')
            if response.url is None: response.url = req.full_url
            return response
        opener.open.side_effect = open_response
        return response, patch.object(monitor.urllib.request, 'build_opener', return_value=opener)
    def test_success_and_proxy_disabled(self):
        response, context = self.body(b'{"news": []}')
        with context as build:
            self.assertEqual(self.call(), {'news': []})
            proxy, redirect = build.call_args.args
            self.assertEqual(proxy.proxies, {})
            self.assertIsInstance(redirect, monitor._NoNewsRedirect)
        self.assertTrue(response.closed)
    def test_streaming_without_content_length(self):
        response, context = self.body(b'{"news": []}', chunk_size=2)
        with context: self.assertEqual(self.call(), {'news': []})
        self.assertGreater(len(response.read_sizes), 2)
    def test_oversized_stream(self):
        response, context = self.body(b' '*129, chunk_size=7)
        with patch.object(monitor, 'MAX_NEWS_BYTES', 128), context:
            with self.assertRaises(monitor.NewsSourceError): self.call()
        self.assertTrue(response.closed)
        self.assertLessEqual(max(response.read_sizes), 129)
    def test_exact_size_boundary(self):
        body=b'{"news": []}'
        _, context=self.body(body)
        with patch.object(monitor, 'MAX_NEWS_BYTES', len(body)), context:
            self.assertEqual(self.call(), {'news': []})
    def test_invalid_payloads(self):
        for body in [b'not json fixture-secret', b'\xff', b'[]', b'{}', b'{"error":"fixture-secret"}', b'{"news":null}',
                     b'{"news":NaN}', b'{"next_page_token":123}', b'{"news": Infinity}']:
            with self.subTest(body=body):
                _, context=self.body(body)
                with context, self.assertRaisesRegex(monitor.NewsSourceError, '^News source request failed$'):
                    self.call()
    def test_status_and_url_mismatch(self):
        for status, url in [(302,None),(401,None),(429,None),(500,None),(200,'http://data.alpaca.markets/v1beta1/news'),(200,'https://evil.invalid/')]:
            response, context=self.body(b'fixture-secret', url=url)
            response.status=status
            with context, self.assertRaises(monitor.NewsSourceError): self.call()
            self.assertEqual(response.read_sizes, [])
    def test_errors_sanitized_and_http_body_closed(self):
        body=io.BytesIO(b'fixture-secret')
        http=error.HTTPError('https://evil.invalid/fixture-key', 401, 'fixture-secret', {}, body)
        for exc in [http, TimeoutError('fixture-secret'), error.URLError('fixture-secret')]:
            with patch.object(monitor.urllib.request, 'build_opener') as build:
                build.return_value.open.side_effect=exc
                with self.assertRaisesRegex(monitor.NewsSourceError, '^News source request failed$'):
                    self.call()
        self.assertTrue(body.closed)
    def test_redirects_are_not_followed(self):
        # Exercise real urllib redirect processing using an entirely fake HTTPS handler.
        for status in [301,302,303,307,308]:
            for target in ['https://evil.invalid/news','http://data.alpaca.markets/v1beta1/news','https://data.alpaca.markets/other']:
                with self.subTest(status=status, target=target):
                    seen=[]
                    class FixtureHTTPS(request.HTTPSHandler):
                        def https_open(self, req):
                            seen.append(req)
                            response=error.addinfourl(io.BytesIO(b'fixture-secret'), {'Location':target}, req.full_url, status)
                            response.msg='redirect'
                            return response
                    opener=request.build_opener(request.ProxyHandler({}), monitor._NoNewsRedirect(), FixtureHTTPS())
                    with patch.object(monitor.urllib.request, 'build_opener', return_value=opener):
                        with self.assertRaises(monitor.NewsSourceError): self.call()
                    self.assertEqual(len(seen), 1)
    def test_invalid_query_never_opens(self):
        for params in [dict(self.params, symbols='AAPL&host=evil'), dict(self.params, page_token=123),
                       dict(self.params, endpoint='https://evil.invalid'), dict(self.params, limit=100),
                       dict(self.params, start='2026-01-01')]:
            with patch.object(monitor.urllib.request, 'build_opener') as build:
                with self.assertRaises(monitor.NewsSourceError):
                    monitor._news_page(params,'fixture-key','fixture-secret')
                build.assert_not_called()
    def test_pagination_and_ingestion_contract(self):
        item=dict(id=1,symbols=['AAPL','MSFT'],created_at='2026-01-01T00:00:00Z',url='https://example.test/news')
        store=Mock(); store.ingest.return_value=('id',True)
        with patch.dict(os.environ, {'ALPACA_API_KEY':'fixture-key','ALPACA_SECRET_KEY':'fixture-secret'}), patch.object(monitor,'_news_page',side_effect=[{'news':[item],'next_page_token':'a&b'},{'news':[]}]) as fetch:
            self.assertEqual(monitor.tick(store,'AAPL'),1)
            self.assertEqual(fetch.call_count,2)
        store.ingest.assert_called_once()
        store.heartbeat.assert_called_once_with('alpaca-news','ok')
    def test_main_records_sanitized_source_health(self):
        store=Mock()
        with patch('sys.argv',['monitor.py','--db','fixture.db','--once']), patch.object(monitor,'Store',return_value=store), patch.object(monitor.signal,'signal'), patch.object(monitor,'tick',side_effect=monitor.NewsSourceError('fixture-secret')), patch('sys.stdout',new_callable=io.StringIO) as output:
            monitor.main()
        self.assertEqual(output.getvalue().strip(), '{"status":"source_error_no_signal"}')
        store.heartbeat.assert_called_once_with('alpaca-news','error')

    def test_page_limit_preserved(self):
        store=Mock()
        with patch.dict(os.environ, {'ALPACA_API_KEY':'fixture-key','ALPACA_SECRET_KEY':'fixture-secret'}), patch.object(monitor,'_news_page',return_value={'news':[],'next_page_token':'more'}) as fetch:
            self.assertEqual(monitor.tick(store,'AAPL'),0)
            self.assertEqual(fetch.call_count,10)
        store.heartbeat.assert_called_once_with('alpaca-news','coverage_incomplete')

if __name__ == '__main__': unittest.main()
