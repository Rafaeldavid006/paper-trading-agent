"""Independent offline adversarial review. Never contacts an external service."""
from dataclasses import replace
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import format_datetime
from pathlib import Path
from http.client import IncompleteRead
import sqlite3
import socket
import tempfile
import unittest
from unittest.mock import Mock, patch

from primary_earnings_source import (
    EvidenceRefused, FEED_URL, HTTPDocument, NS, ORIGIN,
    NvidiaReadOnlyHTTP, PrimaryEarningsSource, parse_article, parse_feed,
    release_url, stamp_ns, utc,
)


PUB = datetime(2026, 8, 26, 20, 20, tzinfo=timezone.utc)
URL = ORIGIN + '/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027'
TITLE = 'NVIDIA Announces Financial Results for Second Quarter Fiscal 2027'


def feed(*, modified=None, published=PUB, title=TITLE, url=URL):
    return (
        '<rss version="2.0"><channel><title>NVIDIA Newsroom</title>'
        f'<link>{ORIGIN}</link><item><title>{title}</title><link>{url}</link>'
        f'<guid>{url}</guid><contentType>releases</contentType>'
        '<categories><category>Press Releases</category></categories>'
        f'<pubDate>{format_datetime(published, usegmt=True)}</pubDate>'
        f'<modDate>{format_datetime(modified or published, usegmt=True)}</modDate>'
        '</item></channel></rss>'
    ).encode()


def article(*, body=None):
    body = body or ('<p>NVIDIA (NASDAQ: NVDA) today reported revenue for the '
                    'second quarter ended July 26, 2026.</p>')
    return (
        f'<html><head><link rel="canonical" href="{URL}"></head><body>'
        f'<h1 class="article-title">{TITLE}</h1>'
        f'<div class="article-body">{body}</div></body></html>'
    ).encode()


class Clock:
    def __init__(self, seconds=100):
        self.ns = stamp_ns(PUB) + seconds * NS

    def __call__(self):
        return self.ns

    def advance(self, seconds):
        self.ns += seconds * NS


class ScriptClock:
    """Supply staged times, then hold the final time for later integrity reads."""
    def __init__(self, seconds):
        self.values = [stamp_ns(PUB) + value * NS for value in seconds]
        self.index = 0

    def __call__(self):
        result = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        return result


class Transport:
    def __init__(self, feed_raw=None, article_raw=None):
        self.feed_raw = feed_raw or feed()
        self.article_raw = article_raw or article()
        self.requests = []
        self.error = None
        self.callback = None

    def get(self, url):
        self.requests.append(url)
        if self.callback:
            self.callback(url)
        if self.error:
            raise self.error
        return HTTPDocument(
            url,
            self.feed_raw if url == FEED_URL else self.article_raw,
            'application/rss+xml' if url == FEED_URL else 'text/html',
        )


class NoNetwork(unittest.TestCase):
    def setUp(self):
        prohibited = patch.object(socket, 'socket',
                                  side_effect=AssertionError('Independent review forbids network'))
        prohibited.start()
        self.addCleanup(prohibited.stop)


class IndependentPrimaryReview(NoNetwork):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / 'primary.db')
        self.clock = Clock()
        self.transport = Transport()
        self.source = self.reopen()

    def reopen(self, **kwargs):
        return PrimaryEarningsSource(
            self.db, transport=kwargs.get('transport', self.transport),
            now_ns=kwargs.get('now_ns', self.clock),
        )

    def rows(self, table):
        with sqlite3.connect(self.db) as connection:
            return connection.execute('SELECT * FROM ' + table).fetchall()

    def events(self, source=None):
        return (source or self.source).events(utc(self.clock.ns), 100)

    def accept(self):
        result = self.source.collect()
        self.assertEqual(result.reason, 'collected_primary_evidence')
        self.assertEqual(result.event_count, 1)
        event, = self.events()
        return event

    def test_original_bytes_receipts_and_lineage_survive_restart(self):
        event = self.accept()
        self.assertEqual(event.observed_at, utc(self.clock.ns))
        self.assertNotEqual(event.observed_at, event.published_at)
        self.assertEqual(len(self.rows('primary_snapshots')), 2)
        self.assertEqual(len(self.rows('primary_receipts')), 2)
        self.assertEqual(len(self.rows('primary_lineage')), 1)
        self.assertEqual(self.events(self.reopen()), (event,))
        self.assertTrue(self.reopen().event_eligible(event))

    def test_historical_first_reception_never_backdates(self):
        self.clock.advance(86400 * 50)
        result = self.source.collect()
        self.assertEqual(result.event_count, 0)
        self.assertEqual(self.events(), ())
        self.assertEqual(len(self.rows('primary_receipts')), 2)
        self.assertEqual(self.rows('primary_episodes')[0][2], 'late')
        self.clock.advance(15)
        self.assertEqual(self.reopen().collect().event_count, 0)
        self.assertEqual(self.rows('primary_episodes')[0][2], 'late')

    def test_exact_120_seconds_is_inclusive(self):
        self.clock = Clock(120)
        self.source = self.reopen()
        self.accept()

    def test_one_nanosecond_over_120_is_late(self):
        self.clock = Clock(120)
        self.clock.ns += 1
        self.source = self.reopen()
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertEqual(self.events(), ())
        self.assertEqual(self.rows('primary_episodes')[0][2], 'late')

    def test_observation_rounds_up_not_into_acceptance_window(self):
        ns = stamp_ns(PUB) + 119 * NS + 999_999_999
        self.assertGreaterEqual(stamp_ns(utc(ns)), ns)

    def test_every_event_field_is_bound(self):
        event = self.accept()
        modifications = {
            'event_id': 'wrong', 'episode_id': 'wrong', 'version': 'wrong',
            'symbol': 'AAPL', 'published_at': PUB - timedelta(seconds=1),
            'observed_at': PUB, 'source_url': FEED_URL,
            'source_hash': '0' * 64, 'verified': False, 'corrected': True,
            'timestamp_precision': 'day', 'event_type': 'news',
        }
        for field, value in modifications.items():
            with self.subTest(field=field):
                self.assertFalse(self.source.event_eligible(replace(event, **{field: value})))

    def test_unchanged_recollection_adds_lineage_not_an_event(self):
        event = self.accept()
        self.clock.advance(15)
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertEqual(self.events(), (event,))
        self.assertEqual(len(self.rows('primary_events')), 1)
        self.assertEqual(len(self.rows('primary_lineage')), 2)
        self.assertEqual(len(self.rows('primary_receipts')), 4)
        self.assertEqual(len(self.rows('primary_snapshots')), 2)

    def test_corrected_episode_and_reversion_stay_quarantined(self):
        event = self.accept()
        self.clock.advance(15)
        self.transport.feed_raw = feed(modified=PUB + timedelta(seconds=1))
        self.assertEqual(self.source.collect().event_count, 0)
        self.assertFalse(self.source.event_eligible(event))
        self.assertEqual(self.rows('primary_episodes')[0][2], 'quarantined')
        self.clock.advance(15)
        self.transport.feed_raw = feed()
        reopened = self.reopen()
        self.assertEqual(reopened.collect().event_count, 0)
        self.assertFalse(reopened.event_eligible(event))
        self.assertEqual(self.events(reopened), ())

    def test_late_correction_permanently_revokes_accepted_event(self):
        event = self.accept()
        self.clock.advance(100)
        self.transport.article_raw = article(body=(
            '<p>Correction. NVIDIA (NASDAQ: NVDA) today reported revenue for the '
            'second quarter ended July 26, 2026.</p>'
        ))
        self.source.collect()
        self.assertEqual(self.rows('primary_episodes')[0][2], 'quarantined')
        self.assertFalse(self.reopen().event_eligible(event))

    def test_original_byte_drift_quarantines_even_without_correction_label(self):
        event = self.accept()
        self.clock.advance(15)
        self.transport.article_raw += b'\n'
        self.source.collect()
        self.assertFalse(self.source.event_eligible(event))
        self.assertEqual(self.rows('primary_episodes')[0][2], 'quarantined')

    def test_recognized_feed_correction_survives_article_fetch_failure(self):
        event = self.accept()
        self.clock.advance(15)
        self.transport.feed_raw = feed(modified=PUB + timedelta(seconds=1))
        def unavailable_article(url):
            if url == URL:
                raise EvidenceRefused('source_read_failed')
        self.transport.callback = unavailable_article
        self.assertEqual(self.source.collect().reason, 'source_collection_refused')
        self.assertFalse(self.source.event_eligible(event))
        self.clock.advance(15)
        self.transport.feed_raw = feed()
        self.transport.callback = None
        reopened = self.reopen()
        reopened.collect()
        self.assertFalse(reopened.event_eligible(event))
        self.assertEqual(self.rows('primary_episodes')[0][2], 'quarantined')

    def test_first_seen_correction_cannot_be_replaced_by_clean_initial_event(self):
        self.transport.feed_raw = feed(modified=PUB + timedelta(seconds=1))
        def unavailable_article(url):
            if url == URL:
                raise EvidenceRefused('source_read_failed')
        self.transport.callback = unavailable_article
        self.assertEqual(self.source.collect().reason, 'source_collection_refused')
        self.clock.advance(15)  # Still inside initial admission's 120-second window.
        self.transport.feed_raw = feed()
        self.transport.callback = None
        reopened = self.reopen()
        self.assertEqual(reopened.collect().event_count, 0)
        self.assertEqual(self.events(reopened), ())

    def test_original_drift_quarantine_survives_later_batch_parse_failure(self):
        event = self.accept()
        self.clock.advance(15)
        other_feed = feed(
            title=TITLE.replace('Second', 'First'),
            url=URL.replace('second-quarter', 'first-quarter'),
        )
        other_item = b'<item>' + other_feed.split(b'<item>', 1)[1].split(b'</item>', 1)[0] + b'</item>'
        self.transport.feed_raw = feed().replace(b'</channel>', other_item + b'</channel>')
        self.transport.article_raw = article() + b'\n'
        # First original parses as a changed version. The second article has
        # wrong canonical/title and causes the full collection to fail.
        self.assertEqual(self.source.collect().reason, 'source_collection_refused')
        self.clock.advance(15)
        self.transport.feed_raw = feed()
        self.transport.article_raw = article()
        reopened = self.reopen()
        reopened.collect()
        self.assertFalse(reopened.event_eligible(event))
        self.assertEqual(self.rows('primary_episodes')[0][2], 'quarantined')

    def test_first_body_correction_survives_later_batch_parse_failure(self):
        other_feed = feed(
            title=TITLE.replace('Second', 'First'),
            url=URL.replace('second-quarter', 'first-quarter'),
        )
        other_item = b'<item>' + other_feed.split(b'<item>', 1)[1].split(b'</item>', 1)[0] + b'</item>'
        self.transport.feed_raw = feed().replace(b'</channel>', other_item + b'</channel>')
        self.transport.article_raw = article().replace(b'</p>', b' Correction: values changed.</p>')
        self.assertEqual(self.source.collect().reason, 'source_collection_refused')
        self.clock.advance(15)
        self.transport.feed_raw = feed()
        self.transport.article_raw = article()
        reopened = self.reopen()
        self.assertEqual(reopened.collect().event_count, 0)
        self.assertEqual(self.events(reopened), ())

    def test_stale_collection_prevents_all_delivery(self):
        event = self.accept()
        self.clock.advance(30)
        self.assertTrue(self.source.event_eligible(event))
        self.clock.ns += 1
        self.assertFalse(self.source.event_eligible(event))
        self.assertEqual(self.events(), ())

    def test_failed_collection_revokes_health_across_instances(self):
        event = self.accept()
        self.clock.advance(15)
        self.transport.error = EvidenceRefused('source_read_failed')
        self.assertEqual(self.source.collect().reason, 'source_collection_refused')
        self.assertFalse(self.reopen().event_eligible(event))
        self.assertEqual(self.events(self.reopen()), ())

    def test_crash_marker_cannot_be_silently_cleared(self):
        self.transport.error = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.source.collect()
        self.assertEqual(self.rows('primary_meta')[0][-1], 1)
        self.transport.error = None
        self.clock.advance(15)
        reopened = self.reopen()
        self.assertEqual(reopened.collect().reason, 'source_collection_refused')
        self.assertEqual(self.rows('primary_meta')[0][-1], 1)
        self.assertEqual(self.events(reopened), ())

    def test_nested_concurrent_collector_cannot_acquire_busy_marker(self):
        concurrent = self.reopen()
        outcomes = []
        def competing_read(url):
            if url == FEED_URL:
                outcomes.append(concurrent.collect())
                self.assertEqual(self.rows('primary_meta')[0][-1], 1)
                self.assertEqual(self.events(concurrent), ())
        self.transport.callback = competing_read
        self.accept()
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0].reason, 'source_collection_refused')
        self.assertEqual(outcomes[0].request_count, 0)
        self.assertEqual(len(self.rows('primary_events')), 1)

    def test_future_publication_fails_closed(self):
        self.clock = Clock(-1)
        self.source = self.reopen()
        self.assertEqual(self.source.collect().reason, 'source_collection_refused')
        self.assertEqual(self.events(), ())

    def test_clock_regression_after_availability_fails_closed(self):
        # Final availability cannot precede the completed original response.
        self.source = self.reopen(now_ns=ScriptClock((50, 50, 60, 55)))
        result = self.source.collect()
        self.assertEqual(result.reason, 'source_collection_refused')
        self.clock = Clock(55)
        self.assertEqual(self.events(self.reopen()), ())

    def test_full_batch_durable_availability_after_120_is_not_accepted(self):
        # Evidence is inaccessible to consumers until the batch succeeds.
        self.source = self.reopen(now_ns=ScriptClock((110, 110, 119, 121)))
        self.source.collect()
        self.clock = Clock(121)
        self.assertEqual(self.events(self.reopen()), ())

    def test_final_write_lock_delay_counts_toward_availability(self):
        self.source = self.reopen(now_ns=ScriptClock((110, 110, 119, 119, 121)))
        self.source.collect()
        self.clock = Clock(121)
        self.assertEqual(self.events(self.reopen()), ())

    def test_invalid_limits_and_clock_do_not_serve_events(self):
        self.accept()
        for limit in (0, 101, True, 1.0, '1'):
            self.assertEqual(self.source.events(utc(self.clock.ns), limit), ())
        self.assertEqual(self.source.events(PUB.replace(tzinfo=None), 1), ())
        self.assertEqual(self.source.events(utc(self.clock.ns + 2 * NS), 1), ())

    def runner_with_native_readiness(self):
        # Real source + real runner/readiness; execution is explicitly mocked.
        from active_demo_pipeline import Config, DemoPipeline
        from decimal_lifecycle import Lifecycle, SessionRiskPolicy
        from demo_tick_runner import DemoTickRunner, RunnerConfig, RunnerLedger
        from native_decimal_port import NativeDecimalExecutionPort, NativeFixtureObservations
        root = Path(self.tmp.name)
        lifecycle = Lifecycle(root / 'native.db', '1' * 64, allowlist=('NVDA',),
                              session_risk_policy=SessionRiskPolicy(3, Decimal('100')))
        evaluator = DemoPipeline(root / 'decisions.db',
                                 Config(enabled=True, allowlist=('NVDA',)))
        port = NativeDecimalExecutionPort(
            evaluator=evaluator, lifecycle=lifecycle,
            source=NativeFixtureObservations({}),
        )
        mocks = ExitStack()
        self.addCleanup(mocks.close)
        for method, value in (('reconcile', None), ('expire_entries', None),
                              ('session', True), ('candidates', []), ('blocked', False),
                              ('evaluate', {'action': 'enter', 'id': 'synthetic'}),
                              ('submit', None)):
            mocks.enter_context(patch.object(port, method, return_value=value))
        market = Mock()
        ledger = RunnerLedger(root / 'runner.db')
        runner = DemoTickRunner(
            eventsource=self.source, marketsource=market, executionport=port,
            ledger=ledger, clock=lambda: utc(self.clock.ns),
            config=RunnerConfig(enabled=True, max_tick_gap_seconds=3600),
        )
        return runner, ledger, port, market

    def test_native_runner_defers_early_source_without_consuming_event(self):
        self.accept()
        runner, ledger, port, market = self.runner_with_native_readiness()
        self.assertEqual(runner.tick()['phase'], 'complete')
        with ledger.db() as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM runner_events').fetchone()[0], 0)
        market.inputs.assert_not_called()
        self.clock.ns = stamp_ns(PUB) + 300 * NS
        self.source.collect()
        self.assertEqual(runner.tick()['phase'], 'complete')
        port.evaluate.assert_called_once()
        port.submit.assert_called_once()
        self.assertEqual(runner.tick()['phase'], 'complete')
        port.submit.assert_called_once()

    def test_native_runner_rechecks_correction_after_market_inputs(self):
        event = self.accept()
        runner, ledger, port, market = self.runner_with_native_readiness()
        self.clock.ns = stamp_ns(PUB) + 300 * NS
        self.source.collect()
        def corrected_during_inputs(*args):
            self.clock.advance(15)
            self.transport.feed_raw = feed(modified=PUB + timedelta(seconds=5))
            self.source.collect()
            return object()
        market.inputs.side_effect = corrected_during_inputs
        self.assertEqual(runner.tick()['phase'], 'complete')
        market.inputs.assert_called_once()
        port.evaluate.assert_not_called()
        port.submit.assert_not_called()
        self.assertFalse(self.source.event_eligible(event))


class IndependentParserAndTransportReview(NoNetwork):
    def test_release_url_rejects_origin_path_and_redirect_injection(self):
        bad_urls = (
            URL + '?redirect=https://evil.example', URL + '#fragment',
            URL.replace('https://', 'http://'),
            URL.replace('nvidianews.nvidia.com', 'nvidianews.nvidia.com.evil.example'),
            URL.replace('nvidianews.nvidia.com', 'user@nvidianews.nvidia.com'),
            URL.replace('nvidianews.nvidia.com', '127.0.0.1'),
            URL.replace('nvidianews.nvidia.com', 'nvidianews.nvidia.com:443'),
            URL + '/../rss.xml', URL + '/', URL + '%2f',
        )
        for url in bad_urls:
            with self.subTest(url=url), self.assertRaises(EvidenceRefused):
                release_url(url)

    def test_disabled_transport_cannot_open_network(self):
        transport = NvidiaReadOnlyHTTP()
        with patch.object(transport._opener, 'open') as opened:
            with self.assertRaisesRegex(EvidenceRefused, 'transport_disabled'):
                transport.get(FEED_URL)
            opened.assert_not_called()

    def test_pinned_transport_rejects_redirect_before_reading_body(self):
        transport = NvidiaReadOnlyHTTP(enabled=True)
        class Response:
            status = 200
            headers = {}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def geturl(self): return 'https://evil.example'
            def read(self, count): raise AssertionError('Redirect body was read')
        with patch.object(transport._opener, 'open', return_value=Response()):
            with self.assertRaisesRegex(EvidenceRefused, 'redirect_refused'):
                transport.get(FEED_URL)

    def test_truncated_chunked_body_is_sanitized_source_failure(self):
        transport = NvidiaReadOnlyHTTP(enabled=True)
        class Response:
            status = 200
            headers = {}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def geturl(self): return FEED_URL
            def read(self, count): raise IncompleteRead(b'private remote detail')
        with patch.object(transport._opener, 'open', return_value=Response()):
            with self.assertRaisesRegex(EvidenceRefused, 'source_read_failed'):
                transport.get(FEED_URL)

    def test_feed_external_entity_and_doctype_rejected(self):
        for declaration in (
            b'<!DOCTYPE rss [<!ENTITY x "boom">]>',
            b'<!DOCTYPE rss SYSTEM "file:///etc/passwd">',
        ):
            with self.subTest(declaration=declaration), self.assertRaises(EvidenceRefused):
                parse_feed(declaration + feed())

    def test_utf16_cannot_bypass_xml_declaration_guard(self):
        xml = ('<?xml version="1.0" encoding="UTF-16"?>'
               '<!DOCTYPE rss [<!ENTITY issuer "NVIDIA Newsroom">]>'
               + feed().decode().replace('NVIDIA Newsroom', '&issuer;'))
        with self.assertRaises(EvidenceRefused):
            parse_feed(xml.encode('utf-16'))

    def test_feed_modified_before_published_is_invalid(self):
        with self.assertRaises(EvidenceRefused):
            parse_feed(feed(modified=PUB - timedelta(seconds=1)))

    def test_duplicate_canonical_title_or_body_fails_closed(self):
        item, = parse_feed(feed())
        for addition in (
            f'<link rel="canonical" href="{URL}">',
            f'<h1 class="article-title">{TITLE}</h1>',
            '<div class="article-body">duplicate</div>',
        ):
            with self.subTest(addition=addition), self.assertRaises(EvidenceRefused):
                parse_article(article() + addition.encode(), item)

    def test_inert_template_is_not_a_reported_article(self):
        item, = parse_feed(feed())
        with self.assertRaises(EvidenceRefused):
            parse_article(b'<template>' + article() + b'</template>', item)

    def test_mismatched_nested_tags_are_not_valid_article_structure(self):
        item, = parse_feed(feed())
        malformed = article().replace(b'</p>', b'</span>')
        with self.assertRaises(EvidenceRefused):
            parse_article(malformed, item)


if __name__ == '__main__':
    unittest.main()
