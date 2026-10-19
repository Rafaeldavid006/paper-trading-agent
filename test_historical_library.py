import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import historical_library as library
from event_core import Store
from webapp import create_app


class HistoricalLibraryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {'DASHBOARD_PASSWORD': 'history-test-password-over-20',
                                         'FLASK_SECRET_KEY': 'history-test'})
        self.env.start()
        self.store = Store(self.tmp.name + '/db')
        self.broker = Mock()
        self.app = create_app(self.store, paper_adapter=self.broker,
                              analyst=Mock(side_effect=AssertionError('model called')),
                              collector=Mock(side_effect=AssertionError('collector called')))
        self.client = self.app.test_client()
        self.auth = {'Authorization': 'Basic ' + base64.b64encode(
            b'user:history-test-password-over-20').decode()}

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def get(self, suffix=''):
        return self.client.get('/history' + suffix, headers=self.auth, base_url='https://localhost')

    def test_authentication_required(self):
        self.assertEqual(self.client.get('/history').status_code, 302)
        self.assertEqual(self.client.get('/history', headers={'Authorization': 'Basic dTpiYWQ='}).status_code, 401)

    def test_auth_not_configured(self):
        with patch.dict(os.environ, {'DASHBOARD_PASSWORD': ''}):
            self.assertEqual(self.get().status_code, 503)

    def test_all_three_cases_and_precision(self):
        response = self.get()
        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        for item in ('2007-01-09', '2008-09-16', '2022-03-01', 'day', 'minute',
                     '21:00', 'EDT (UTC-04:00)', '2008-09-17T01:00:00Z'):
            self.assertIn(item, text)
        catalog, pilot = library.load_snapshot()
        for case in pilot['cases']:
            self.assertIsNone(case['event']['time'])
            self.assertIn(case['case_id'], text)
            for source in case['sources']:
                self.assertIn(source['official_url'], text)

    def test_conspicuous_limitations(self):
        text = self.get().get_data(as_text=True)
        for item in ('3 אירועים לדוגמה בלבד', 'PIT eligible: false', 'not_ingested',
                     'אין נתוני תשואה', 'אין אותות מסחר', 'חסר / לא נאסף', 'null',
                     'UCDP', 'GDELT', 'לא מקור אמת', 'FRED / ALFRED', 'IMF',
                     'rights_review_blocked', 'rights_and_access_review_blocked'):
            self.assertIn(item, text)

    def test_all_expected_source_categories(self):
        text = self.get().get_data(as_text=True)
        for source_id in library.SOURCE_IDS:
            self.assertIn(source_id, text)

    def test_case_search_and_domain_filter(self):
        text = self.get('?q=Cingular').get_data(as_text=True)
        self.assertIn('iphone_announcement_2007', text)
        self.assertNotIn('aig_facility_authorization_2008', text)
        response = self.client.get('/history', query_string={'domain': 'אנרגיה'}, headers=self.auth)
        text = response.get_data(as_text=True)
        self.assertIn('iea_emergency_stock_agreement_2022', text)
        self.assertNotIn('iphone_announcement_2007', text)
        self.assertIn('world_bank', text)  # Source catalog intentionally remains unfiltered.

    def test_empty_search_results(self):
        self.assertIn('לא נמצאו דוגמאות', self.get('?q=nonexistent-xyz').get_data(as_text=True))

    def test_query_escaping(self):
        response = self.client.get('/history', query_string={'q': '"><script>alert(1)</script>'}, headers=self.auth)
        self.assertNotIn(b'<script>', response.data)
        self.assertIn(b'&lt;script&gt;', response.data)

    def test_query_limits_and_duplicate_rejection(self):
        for suffix in ('?q='+'x'*121, '?domain=unknown', '?file=/etc/passwd',
                       '?q=a&q=b', '?domain=&domain=', '?q='+'%41'*1000):
            with self.subTest(suffix=suffix[:40]):
                self.assertEqual(self.get(suffix).status_code, 400)

    def test_no_mutating_history_endpoints(self):
        for method in ('post', 'put', 'patch', 'delete'):
            self.assertEqual(getattr(self.client, method)('/history', headers=self.auth).status_code, 405)

    def test_no_network_broker_or_database_mutation(self):
        before = Path(self.tmp.name + '/db').read_bytes()
        with patch('socket.socket', side_effect=AssertionError('network called')):
            self.assertEqual(self.get().status_code, 200)
        self.assertEqual(self.broker.mock_calls, [])
        self.assertEqual(before, Path(self.tmp.name + '/db').read_bytes())

    def test_absent_corrupt_oversized_snapshots_fail_safe(self):
        directory = Path(self.tmp.name) / 'snapshots'
        directory.mkdir()
        for value in (None, b'{bad JSON', b'x' * (library.MAX_BYTES + 1)):
            with self.subTest(value=value and len(value)):
                if value is not None:
                    (directory/'global-history-catalog.json').write_bytes(value)
                with patch.object(library, 'DATA_DIR', directory):
                    response = self.get()
                    self.assertEqual(response.status_code, 503)
                    self.assertNotIn(b'Apple Reinvents', response.data)
                    self.assertEqual(self.client.get('/health').status_code, 200)

    def test_modified_snapshot_rejected(self):
        with patch.object(library, '_read', return_value=b'{}'):
            self.assertRaises(library.SnapshotUnavailable, library.load_snapshot)

    def test_exact_snapshot_hashes_and_nulls(self):
        manifest = json.loads((library.DATA_DIR/'snapshot-manifest.json').read_text())
        self.assertEqual(manifest['files'], library.SNAPSHOT_DIGESTS)
        catalog, pilot = library.load_snapshot()
        self.assertEqual(len(catalog['sources']), 8)
        self.assertEqual(len(pilot['cases']), 3)
        for case in pilot['cases']:
            self.assertIsNone(case['outcomes'])
            self.assertIsNone(case['market_data'])
            self.assertIs(case['eligibility']['point_in_time_backtest_eligible'], False)

    def test_unsafe_urls_rejected(self):
        for url in ('javascript:alert(1)', 'http://www.sec.gov', 'https://u:p@www.sec.gov',
                    'https://www.sec.gov:22', 'https://www.sec.gov/\nfoo'):
            with self.subTest(url=url):
                self.assertRaises(library.SnapshotUnavailable, library._validate_tree, {'official_url': url})

    def test_security_headers_and_no_remote_assets(self):
        response = self.get()
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertIn("default-src 'none'", response.headers['Content-Security-Policy'])
        self.assertNotIn(b'<script', response.data)
        self.assertNotIn(b'<iframe', response.data)
        self.assertNotIn(b'<img', response.data)

    def test_dashboard_navigation(self):
        response = self.client.get('/', headers=self.auth)
        self.assertIn(b'href="/history"', response.data)

if __name__ == '__main__':
    unittest.main()
