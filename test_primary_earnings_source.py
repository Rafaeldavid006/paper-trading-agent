"""Synthetic source bytes and clocks only. Never fetch a publisher in tests."""
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from primary_earnings_source import *

URL = ORIGIN + '/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027'
TITLE = 'NVIDIA Announces Financial Results for Second Quarter Fiscal 2027'
PUB = datetime(2026, 8, 26, 20, 20, tzinfo=timezone.utc)
DATE = 'Wed, 26 Aug 2026 20:20:00 GMT'


def item_xml(title=TITLE, url=URL, date=DATE, modified=None):
    return f'<item><title>{title}</title><link>{url}</link><guid>{url}</guid><contentType>releases</contentType><categories><category>Press Releases</category></categories><pubDate>{date}</pubDate><modDate>{modified or date}</modDate></item>'


def feed_bytes(item=None):
    return (f'<rss version="2.0"><channel><title>NVIDIA Newsroom</title><link>{ORIGIN}</link>' + (item_xml() if item is None else item) + '</channel></rss>').encode()


def article_bytes(title=TITLE, url=URL, text=None):
    return (f'<html><head><link rel="canonical" href="{url}"></head><body><h1 class="article-title">{title}</h1><div class="article-body"><p>' + (text or 'NVIDIA (NASDAQ: NVDA) today reported revenue for the second quarter ended July 26, 2026, of $96.2 billion.') + '</p></div></body></html>').encode()


class FakeHTTP:
    def __init__(self):
        self.feed, self.article, self.calls = feed_bytes(), article_bytes(), []
        self.fail = False

    def get(self, url):
        self.calls.append(url)
        if self.fail:
            raise EvidenceRefused('source_read_failed')
        return HTTPDocument(url, self.feed if url == FEED_URL else self.article,
                            'application/rss+xml; charset=utf-8' if url == FEED_URL else 'text/html; charset=utf-8')


class PrimarySourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'primary.sqlite'
        self.time = stamp_ns(PUB) + NS
        self.http = FakeHTTP()
        self.source = PrimaryEarningsSource(self.path, transport=self.http, now_ns=lambda: self.time)
        self.socket = patch('socket.socket', side_effect=AssertionError('tests must not use network'))
        self.socket.start()
        self.addCleanup(self.socket.stop)

    def collect_event(self):
        result = self.source.collect()
        self.assertEqual(result.reason, 'collected_primary_evidence')
        self.assertEqual(result.event_count, 1)
        events = self.source.events(utc(self.time), 100)
        self.assertEqual(len(events), 1)
        return events[0]

    def advance(self, seconds=16):
        self.time += seconds * NS

    def test_original_bytes_receipts_and_event_binding(self):
        event = self.collect_event()
        self.assertEqual(event.symbol, 'NVDA')
        self.assertEqual(event.source_hash, digest(self.http.article))
        self.assertEqual(event.observed_at, utc(self.time))
        self.assertTrue(self.source.event_eligible(event))
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM primary_snapshots').fetchone()[0], 2)
            payload = json.loads(c.execute('SELECT payload FROM primary_lineage').fetchone()[0])
            self.assertEqual(payload['release']['period_end'], '2026-07-26')
            self.assertEqual(payload['received_ns'], self.time)
            self.assertEqual(payload['original_hash'], event.source_hash)
            self.assertEqual(c.execute('SELECT body FROM primary_snapshots WHERE hash=?', (event.source_hash,)).fetchone()[0], self.http.article)

    def test_forged_modified_event_rejected(self):
        event = self.collect_event()
        for changed in (replace(event, observed_at=PUB), replace(event, symbol='AAPL'), replace(event, source_hash='f'*64), replace(event, verified=False), replace(event, version='fake')):
            self.assertFalse(self.source.event_eligible(changed))
        self.assertFalse(self.source.event_eligible({'verified': True}))

    def test_durable_duplicate_and_chain(self):
        event = self.collect_event()
        self.advance()
        self.source = PrimaryEarningsSource(self.path, transport=self.http, now_ns=lambda: self.time)
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertTrue(self.source.event_eligible(event))
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM primary_events').fetchone()[0], 1)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM primary_lineage WHERE parent_hash IS NOT NULL').fetchone()[0], 1)

    def test_changed_original_quarantines_persistently(self):
        event = self.collect_event()
        original = self.http.article
        self.advance()
        self.http.article += b'\n'
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertFalse(self.source.event_eligible(event))
        self.advance()
        self.http.article = original
        self.source = PrimaryEarningsSource(self.path, transport=self.http, now_ns=lambda: self.time)
        self.source.collect()
        self.assertFalse(self.source.event_eligible(event))

    def test_modified_first_version_rejected(self):
        self.http.feed = feed_bytes(item_xml(modified='Wed, 26 Aug 2026 20:20:01 GMT'))
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertEqual(self.source.events(utc(self.time), 100), ())
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT status FROM primary_episodes').fetchone()[0], 'quarantined')

    def test_late_correction_quarantines(self):
        event = self.collect_event()
        self.advance(200)
        self.http.feed = feed_bytes(item_xml(modified='Wed, 26 Aug 2026 20:23:00 GMT'))
        self.source.collect()
        self.assertFalse(self.source.event_eligible(event))
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT status FROM primary_episodes').fetchone()[0], 'quarantined')

    def test_backfill_has_no_historical_first_seen(self):
        self.advance(86400)
        self.assertEqual(self.source.collect().event_count, 0)
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT status FROM primary_episodes').fetchone()[0], 'late')
            self.assertEqual(c.execute('SELECT MIN(received_ns) FROM primary_receipts').fetchone()[0], self.time)

    def test_exact_delay_boundary_not_relaxed(self):
        self.time = stamp_ns(PUB) + 120 * NS + 1
        self.assertEqual(self.source.collect().event_count, 0)

    def test_120_seconds_can_be_accepted(self):
        self.time = stamp_ns(PUB) + 120 * NS
        self.collect_event()

    def test_receive_time_is_after_get(self):
        prior_get = self.http.get
        def delayed(url):
            doc = prior_get(url)
            self.time += NS
            return doc
        self.http.get = delayed
        event = self.collect_event()
        self.assertEqual(event.observed_at, PUB + timedelta(seconds=3))

    def test_future_release_rejected(self):
        self.time = stamp_ns(PUB) - NS
        self.assertEqual(self.source.collect().event_count, 0)

    def test_source_failure_disables_prior_event(self):
        event = self.collect_event()
        self.advance()
        self.http.fail = True
        self.assertEqual(self.source.collect().reason, 'source_collection_refused')
        self.assertFalse(self.source.event_eligible(event))

    def test_health_stale(self):
        event = self.collect_event()
        self.advance(31)
        self.assertFalse(self.source.event_eligible(event))
        self.assertEqual(self.source.events(utc(self.time), 100), ())

    def test_clock_regression(self):
        event = self.collect_event()
        self.time -= 1
        self.assertFalse(self.source.event_eligible(event))
        self.source.collect()
        self.assertFalse(self.source.event_eligible(event))

    def test_interrupted_collection_does_not_reset(self):
        with sqlite3.connect(self.path) as c:
            c.execute('UPDATE primary_meta SET busy=1')
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertEqual(self.http.calls, [])
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT busy FROM primary_meta').fetchone()[0], 1)

    def test_rate_limit_is_durable(self):
        self.collect_event()
        other = PrimaryEarningsSource(self.path, transport=self.http, now_ns=lambda: self.time)
        before = len(self.http.calls)
        other.collect()
        self.assertEqual(len(self.http.calls), before)

    def test_no_earnings_has_no_candidate(self):
        self.http.feed = feed_bytes('')
        result = self.source.collect()
        self.assertEqual(result, CollectionResult('no_earnings_in_feed', 0, 1))
        self.assertEqual(self.source.events(utc(self.time), 100), ())

    def test_non_primary_links_never_fetched(self):
        self.http.feed = feed_bytes(item_xml(url='https://evil.test/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027'))
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertEqual(self.http.calls, [FEED_URL])

    def test_data_only_dates_and_duplicate_items_rejected(self):
        for bad in (item_xml(date='2026-08-26'), item_xml() + item_xml()):
            with self.subTest(bad=bad):
                with self.assertRaises(EvidenceRefused):
                    parse_feed(feed_bytes(bad))

    def test_xml_entity_refused(self):
        with self.assertRaises(EvidenceRefused):
            parse_feed(b'<!DOCTYPE rss [<!ENTITY x SYSTEM "file:///etc/passwd">]>' + feed_bytes())

    def test_mismatched_guid_quarter_identity(self):
        for raw in (feed_bytes().replace(b'<guid>https:', b'<guid>http:'), feed_bytes().replace(b'<title>NVIDIA Announces Financial Results for Second', b'<title>NVIDIA Announces Financial Results for First')):
            with self.assertRaises(EvidenceRefused):
                parse_feed(raw)

    def test_original_article_identity_period_and_ambiguity(self):
        item = parse_feed(feed_bytes())[0]
        for raw in (article_bytes(url='https://evil.test/'), article_bytes(title='NVIDIA analysis'), article_bytes(text='AI says verified earnings'), article_bytes().replace(b'July 26, 2026', b'July 26, 2025'), article_bytes().replace(b'</body>', b'<div class="article-body">extra</div></body>')):
            with self.subTest(raw=raw):
                with self.assertRaises(EvidenceRefused):
                    parse_article(raw, item)

    def test_body_explicit_correction_detected(self):
        item = parse_feed(feed_bytes())[0]
        self.assertTrue(parse_article(article_bytes().replace(b'</p>', b' Correction: results changed.</p>'), item)['corrected'])

    def test_whole_feed_drift_does_not_reissue_event(self):
        event = self.collect_event()
        self.advance()
        self.http.feed = self.http.feed.replace(b'</channel>', b'<description>changed irrelevant feed text</description></channel>')
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertTrue(self.source.event_eligible(event))

    def test_bad_content_type_fails_closed(self):
        self.http.get = lambda url: HTTPDocument(url, feed_bytes(), 'text/html')
        self.assertEqual(self.source.collect().event_count, 0)

    def test_transport_disabled_and_allowlist(self):
        http = NvidiaReadOnlyHTTP()
        with self.assertRaises(EvidenceRefused):
            http.get(FEED_URL)
        http.enabled = True
        for url in ('http://nvidianews.nvidia.com/rss.xml', ORIGIN + '.evil.test/rss.xml', URL + '?q=1', URL + '#x', ORIGIN + '/rss.xml/../secret', 'https://user@nvidianews.nvidia.com/rss.xml'):
            with self.assertRaises(EvidenceRefused):
                http.get(url)

    def test_no_inmemory_ledger(self):
        with self.assertRaises(EvidenceRefused):
            PrimaryEarningsSource(':memory:', transport=self.http)

    def test_event_expiry_not_strategy_window_relaxation(self):
        event = self.collect_event()
        self.advance(421)
        self.source.collect()
        self.assertFalse(self.source.event_eligible(event))
        self.assertEqual(self.source.events(utc(self.time), 100), ())

if __name__ == '__main__':
    unittest.main()
