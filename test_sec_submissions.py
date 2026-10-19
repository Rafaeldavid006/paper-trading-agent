"""Synthetic fixture tests only. No requests to SEC or broker endpoints."""
import copy
from datetime import datetime, timezone, timedelta
from pathlib import Path
import io
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import sec_submissions as sec

NOW = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)
CIK = '0000123456'
FIXTURE = json.loads((Path(__file__).parent / 'fixtures/sec_submissions.json').read_text())


class Response(io.BytesIO):
    status = 200


class Harness(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / 'sec.sqlite')
        self.payload = copy.deepcopy(FIXTURE)
        self.calls, self.waits, self.ticks = [], [], 0
        self.responses = []

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.ticks += seconds

    def open(self, request, timeout):
        self.calls.append((request, timeout))
        result = self.responses.pop(0) if self.responses else Response(json.dumps(self.payload).encode())
        if isinstance(result, Exception):
            raise result
        return result

    def client(self, **kwargs):
        return sec.SecClient(self.path, 'Research Test test@example.invalid',
            opener=self.open, clock=lambda: NOW, monotonic=lambda: self.ticks,
            sleep=self.sleep, **kwargs)

    def ingestor(self, **kwargs):
        return sec.SecIngestor(self.path, self.client(), [CIK], clock=lambda: NOW, **kwargs)

    def snapshot(self, payload=None, **kwargs):
        return sec.Snapshot(CIK, self.payload if payload is None else payload,
                            kwargs.get('fetched_at', NOW.isoformat()), False)


class ParserTests(Harness):
    def test_official_urls_and_amendment(self):
        self.payload['filings']['recent']['form'][0] = '8-K/A'
        rows, older = sec.parse_submissions(self.payload, CIK)
        self.assertTrue(older)
        self.assertTrue(rows[0]['amendment'])
        self.assertEqual(rows[0]['document_url'], 'https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/test-filing.htm')

    def test_offset_normalization_and_no_precision_fabrication(self):
        self.assertEqual(sec.acceptance_time('2026-10-16T16:05:10-04:00'), '2026-10-16T20:05:10.000000+00:00')
        for value in (None, '', '2026-10-16', '2026-10-16T16:05:10', 42, '2026-02-30T00:00:00Z'):
            with self.subTest(value=value):
                self.assertIsNone(sec.acceptance_time(value))

    def test_cik_validation(self):
        self.assertEqual(sec.cik_number(123456), CIK)
        for value in (True, 0, '0', '-1', '../1', '12345678901', 1.5, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                sec.cik_number(value)

    def test_identity_mismatch_missing_recent_not_empty_success(self):
        for payload in ({}, [], {'cik': CIK}, dict(self.payload, cik=999)):
            with self.subTest(payload=payload), self.assertRaises(sec.SourceError):
                sec.parse_submissions(payload, CIK)

    def test_missing_required_column_and_parallel_array_truncation(self):
        for column in ('form', 'acceptanceDateTime', 'items'):
            p = copy.deepcopy(self.payload)
            p['filings']['recent'][column] = []
            with self.subTest(column=column), self.assertRaises(sec.SourceError):
                sec.parse_submissions(p, CIK)
        del self.payload['filings']['recent']['form']
        with self.assertRaises(sec.SourceError):
            sec.parse_submissions(self.payload, CIK)

    def test_missing_acceptance_column_allowed_without_fabrication(self):
        del self.payload['filings']['recent']['acceptanceDateTime']
        rows, _ = sec.parse_submissions(self.payload, CIK)
        self.assertIsNone(rows[0]['accepted_at'])


    def test_safe_relative_primary_document_subpath(self):
        self.payload['filings']['recent']['primaryDocument'][0] = 'xslF345X05/ownership.xml'
        rows, _ = sec.parse_submissions(self.payload, CIK)
        self.assertTrue(rows[0]['document_url'].endswith('/xslF345X05/ownership.xml'))
        for value in ('/absolute.xml', 'x/../secret', './file', 'x//file', 'x\\file', 'a\nfile'):
            self.payload['filings']['recent']['primaryDocument'][0] = value
            with self.subTest(value=value), self.assertRaises(sec.SourceError):
                sec.parse_submissions(self.payload, CIK)

    def test_invalid_filename_and_date(self):
        for field, value in [('primaryDocument', '../secrets'), ('primaryDocument', ''),
                             ('filingDate', '2026-02-30'), ('filingDate', '20261016'),
                             ('accessionNumber', '123'), ('form', ''), ('form', 42)]:
            p = copy.deepcopy(self.payload)
            p['filings']['recent'][field][0] = value
            with self.subTest(field=field, value=value), self.assertRaises(sec.SourceError):
                sec.parse_submissions(p, CIK)


    def test_conflicting_duplicate_accession_rejected(self):
        for values in self.payload['filings']['recent'].values():
            values.append(values[0])
        self.payload['filings']['recent']['form'][1] = '10-Q'
        with self.assertRaises(sec.SourceError):
            sec.parse_submissions(self.payload, CIK)

    def test_legitimate_empty_snapshot(self):
        self.payload['filings']['recent'] = {k: [] for k in self.payload['filings']['recent']}
        self.assertEqual(sec.parse_submissions(self.payload, CIK), ([], True))

    def test_row_limit_not_silent_truncation(self):
        with patch.object(sec, 'MAX_ROWS', 0), self.assertRaises(sec.SourceError):
            sec.parse_submissions(self.payload, CIK)


class ClientTests(Harness):
    def test_fetch_official_endpoint_headers_cache_persists_restart(self):
        first = self.client().fetch(CIK)
        second = self.client().fetch(CIK)
        self.assertFalse(first.cache_hit)
        self.assertTrue(second.cache_hit)
        self.assertEqual(first.fetched_at, second.fetched_at)
        self.assertEqual(len(self.calls), 1)
        req, timeout = self.calls[0]
        self.assertEqual(req.full_url, 'https://data.sec.gov/submissions/CIK0000123456.json')
        self.assertEqual(req.get_header('User-agent'), 'Research Test test@example.invalid')
        self.assertEqual(req.get_header('Accept-encoding'), 'identity')
        self.assertEqual(timeout, 20)

    def test_cache_expiration_and_rate_limit(self):
        client = self.client(cache_seconds=0)
        client.fetch(CIK)
        client.fetch(CIK)
        self.assertEqual(len(self.calls), 2)
        self.assertGreaterEqual(sum(self.waits), 1)

    def test_future_cache_entry_not_used(self):
        client = self.client()
        client.fetch(CIK)
        with sqlite3.connect(self.path) as db:
            db.execute('UPDATE sec_cache SET fetched_at=?', ((NOW + timedelta(days=1)).isoformat(),))
        self.assertFalse(client.fetch(CIK).cache_hit)


    def test_invalid_cache_timestamp_is_refetched(self):
        client = self.client()
        client.fetch(CIK)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE sec_cache SET fetched_at='not-a-date'")
        self.assertFalse(client.fetch(CIK).cache_hit)

    def test_corrupt_payload_cache_is_refetched(self):
        client = self.client()
        client.fetch(CIK)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE sec_cache SET payload='{}'")
        self.assertFalse(client.fetch(CIK).cache_hit)

    def test_rate_429_retry_after_seconds(self):
        self.responses = [HTTPError(sec.source_url(CIK), 429, 'slow', {'Retry-After': '7'}, None)]
        self.client().fetch(CIK)
        self.assertEqual(len(self.calls), 2)
        self.assertGreaterEqual(sum(self.waits), 7)

    def test_retry_after_http_date(self):
        client = self.client()
        self.assertEqual(client._retry_delay('Sun, 18 Oct 2026 12:00:09 GMT', 0), 9)
        self.assertEqual(client._retry_delay('invalid', 1), 2)
        for value in ('121', 'inf'):
            with self.assertRaises(sec.SourceError):
                client._retry_delay(value, 0)

    def test_retry_503_and_network_failure_then_success(self):
        self.responses = [HTTPError(sec.source_url(CIK), 503, 'down', {}, None), URLError('offline')]
        self.client().fetch(CIK)
        self.assertEqual(len(self.calls), 3)
        self.assertGreaterEqual(sum(self.waits), 3)

    def test_exhausted_retries_no_cache(self):
        self.responses = [URLError('offline')] * 3
        with self.assertRaises(sec.SourceError):
            self.client().fetch(CIK)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_cache').fetchone()[0], 0)

    def test_forbidden_is_not_retried(self):
        self.responses = [HTTPError(sec.source_url(CIK), 403, 'forbidden', {}, None)]
        with self.assertRaises(sec.SourceError):
            self.client().fetch(CIK)
        self.assertEqual(len(self.calls), 1)

    def test_invalid_json_truncation_nan_rejected(self):
        client = self.client()
        for raw in (b'{', b'<html>unavailable</html>', b'null', b'{"cik":NaN}', b'\xff'):
            self.responses = [Response(raw)]
            with self.subTest(raw=raw), self.assertRaises(sec.SourceError):
                client.fetch(CIK)

    def test_byte_limit(self):
        with patch.object(sec, 'MAX_BYTES', 4), self.assertRaises(sec.SourceError):
            self.client().fetch(CIK)

    def test_redirects_refused(self):
        with self.assertRaises(sec.SourceError):
            sec.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://evil.invalid/')

    def test_bad_configuration(self):
        for ua in ('', 'bot', 'x x@example.invalid\nBad: x', 'x@example.invalid'):
            with self.subTest(ua=ua), self.assertRaises(ValueError):
                sec.SecClient(self.path, ua)
        for kwargs in ({'attempts': 0}, {'attempts': True}, {'attempts': 6}, {'cache_seconds': -1}, {'timeout': 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.client(**kwargs)


class IngestorTests(Harness):
    def test_durable_dedup_restarted_ingestor_and_project(self):
        ing = self.ingestor()
        one = ing.record(self.snapshot())
        two = self.ingestor().record(self.snapshot())
        self.assertEqual(one['new_versions'], 1)
        self.assertEqual(two['new_versions'], 0)
        self.assertEqual(one['versions'], two['versions'])
        eid, created = ing.project(one['versions'][0], 'TEST')
        self.assertTrue(created)
        self.assertEqual(self.ingestor().project(one['versions'][0], 'TEST'), (eid, False))
        with ing.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_event_map').fetchone()[0], 1)
            event = db.execute('SELECT * FROM events').fetchone()
            self.assertEqual(event['source_id'], CIK + ':0000123456-26-000001:TEST')
            self.assertNotIn('actual', json.loads(event['payload']))


    def test_multiple_share_classes_do_not_supersede_each_other(self):
        ing = self.ingestor()
        vid = ing.record(self.snapshot())['versions'][0]
        first = ing.project(vid, 'TEST.A')[0]
        second = ing.project(vid, 'TEST.B')[0]
        self.assertNotEqual(first, second)
        self.assertNotIn('superseded_version', ing.store.evaluate(first)['reasons'])

    def test_absence_from_later_snapshot_does_not_delete_records(self):
        ing = self.ingestor()
        ing.record(self.snapshot())
        self.payload['filings']['recent'] = {k: [] for k in self.payload['filings']['recent']}
        self.assertEqual(ing.record(self.snapshot())['new_versions'], 0)
        with ing.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_records').fetchone()[0], 1)

    def test_projection_crash_mapping_replay_is_idempotent(self):
        ing = self.ingestor()
        vid = ing.record(self.snapshot())['versions'][0]
        event = ing.project(vid, 'TEST')[0]
        with ing.store.db() as db:
            db.execute('DELETE FROM sec_event_map')  # Simulate crash between event and map commits.
        self.assertEqual(ing.project(vid, 'TEST'), (event, False))
        with ing.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_event_map').fetchone()[0], 1)

    def test_correction_preserved_and_event_version_updated(self):
        ing = self.ingestor()
        old = ing.record(self.snapshot())['versions'][0]
        first = ing.project(old, 'TEST')[0]
        self.payload['filings']['recent']['items'][0] = '2.02,9.01,8.01'
        new = ing.record(self.snapshot())['versions'][0]
        second = ing.project(new, 'TEST')[0]
        self.assertNotEqual(old, new)
        self.assertNotEqual(first, second)
        with ing.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_records').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 2)


    def test_correction_order_is_observation_not_projection_order(self):
        ing = self.ingestor()
        original = ing.record(self.snapshot())['versions'][0]
        self.payload['filings']['recent']['items'][0] = 'CORRECTED'
        corrected = ing.record(self.snapshot())['versions'][0]
        current_event = ing.project(corrected, 'TEST')[0]
        with self.assertRaisesRegex(sec.SourceError, 'stale'):
            ing.project(original, 'TEST')
        self.assertNotIn('superseded_version', ing.store.evaluate(current_event)['reasons'])

    def test_previously_projected_old_revision_cannot_resurrect(self):
        ing = self.ingestor()
        original = ing.record(self.snapshot())['versions'][0]
        ing.project(original, 'TEST')
        self.payload['filings']['recent']['items'][0] = 'CORRECTED'
        corrected = ing.record(self.snapshot())['versions'][0]
        current_event = ing.project(corrected, 'TEST')[0]
        with self.assertRaisesRegex(sec.SourceError, 'stale'):
            ing.project(original, 'TEST')
        self.assertNotIn('superseded_version', ing.store.evaluate(current_event)['reasons'])


    def test_regressed_source_acceptance_does_not_reverse_correction_order(self):
        ing = self.ingestor()
        original = ing.record(self.snapshot())['versions'][0]
        old_event = ing.project(original, 'TEST')[0]
        self.payload['filings']['recent']['acceptanceDateTime'][0] = '2026-10-15T16:05:10Z'
        corrected = ing.record(self.snapshot())['versions'][0]
        current_event = ing.project(corrected, 'TEST')[0]
        self.assertNotIn('superseded_version', ing.store.evaluate(current_event)['reasons'])
        self.assertIn('superseded_version', ing.store.evaluate(old_event)['reasons'])
        with ing.store.db() as db:
            event = db.execute('SELECT * FROM events WHERE version_id=?', (current_event,)).fetchone()
            self.assertEqual(event['updated_at'], sec.iso(NOW))
            self.assertEqual(json.loads(event['payload'])['updated_at_semantics'],
                             'local_metadata_observation_not_sec_correction_time')

    def test_observation_clock_regression_refuses_projection(self):
        ing = self.ingestor()
        ing.record(self.snapshot())
        ing.store.clock = lambda: NOW - timedelta(seconds=1)
        self.payload['filings']['recent']['items'][0] = 'CORRECTED'
        corrected = ing.record(self.snapshot(fetched_at=(NOW - timedelta(seconds=2)).isoformat()))['versions'][0]
        with self.assertRaisesRegex(sec.SourceError, 'clock regressed'):
            ing.project(corrected, 'TEST')

    def test_amendment_new_accession_not_guessed_parent(self):
        ing = self.ingestor()
        old = ing.record(self.snapshot())['versions'][0]
        ing.project(old, 'TEST')
        self.payload['filings']['recent']['accessionNumber'][0] = '0000123456-26-000002'
        self.payload['filings']['recent']['form'][0] = '8-K/A'
        new = ing.record(self.snapshot())['versions'][0]
        event = ing.project(new, 'TEST')[0]
        with ing.store.db() as db:
            payload = json.loads(db.execute('SELECT payload FROM events WHERE version_id=?', (event,)).fetchone()[0])
            self.assertTrue(payload['amendment'])
            self.assertIsNone(payload['amends_accession'])
            self.assertEqual(db.execute('SELECT COUNT(DISTINCT source_id) FROM events').fetchone()[0], 2)

    def test_no_fabricated_event_timestamp(self):
        ing = self.ingestor()
        for value in ('', '2026-10-16', '2026-10-16T16:00:00'):
            self.payload['filings']['recent']['acceptanceDateTime'][0] = value
            vid = ing.record(self.snapshot())['versions'][0]
            with self.assertRaises(sec.SourceError):
                ing.project(vid, 'TEST')
        with ing.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_records').fetchone()[0], 3)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 0)

    def test_future_acceptance_cannot_project(self):
        ing = self.ingestor()
        self.payload['filings']['recent']['acceptanceDateTime'][0] = '2027-01-01T00:00:00Z'
        vid = ing.record(self.snapshot())['versions'][0]
        with self.assertRaises(sec.SourceError):
            ing.project(vid, 'TEST')

    def test_future_snapshot_cannot_record(self):
        with self.assertRaises(sec.SourceError):
            self.ingestor().record(self.snapshot(fetched_at='2027-01-01T00:00:00Z'))

    def test_opt_in_limits_and_projection_symbol(self):
        for issuers in ([], [CIK] * 26, '123456'):
            with self.assertRaises(ValueError):
                sec.SecIngestor(self.path, self.client(), issuers)
        ing = self.ingestor()
        with self.assertRaises(ValueError):
            ing.record(sec.Snapshot('0000000001', self.payload, NOW.isoformat(), False))
        with self.assertRaises(ValueError):
            ing.project('missing', 'TEST')
        vid = ing.record(self.snapshot())['versions'][0]
        with self.assertRaises(ValueError):
            ing.project(vid, '')

    def test_poll_no_projection_and_error_not_empty_success(self):
        ing = self.ingestor()
        results = ing.poll_once()
        self.assertEqual(results[0]['coverage'], 'recent_only')
        self.assertTrue(results[0]['older_files_available'])
        with ing.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT status FROM health').fetchone()[0], 'ok')
        ing.client = self.client(cache_seconds=0)
        self.responses = [HTTPError(sec.source_url(CIK), 403, 'no', {}, None)]
        results = ing.poll_once()
        self.assertEqual(results[0]['status'], 'error')
        with ing.store.db() as db:
            health = db.execute('SELECT * FROM health').fetchone()
            self.assertEqual(health['status'], 'error')
            self.assertIsNotNone(health['last_success'])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_records').fetchone()[0], 1)

    def test_malformed_batch_is_atomic(self):
        ing = self.ingestor()
        for key, values in self.payload['filings']['recent'].items():
            values.append(values[0])
        self.payload['filings']['recent']['accessionNumber'][1] = 'bad'
        with self.assertRaises(sec.SourceError):
            ing.record(self.snapshot())
        with ing.store.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_records').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sec_snapshots').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
