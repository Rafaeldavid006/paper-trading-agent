from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from direct_adapter import CaptureEvidence, DirectSourceAdapter, Rejected, FAQ, ECB, parse
from upstream.scheduler.bounded_scheduler import Scheduler, Source, Policy, Response
from upstream.storage.durable_store import EvidenceStore

START = 1793181600  # 2026-10-28 10:00:00 UTC, explicitly synthetic clocks.


def ts(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="microseconds")


def html(title="Microsoft Investor FAQ - synthetic A"):
    return ("<!doctype html><html><head><title>" + title + "</title></head><body>synthetic fixture</body></html>").encode()


def xml(day="2026-10-28", rate="1.1645", extra=""):
    return (f'''<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
<gesmes:subject>Reference rates - synthetic</gesmes:subject>
<Cube><Cube time="{day}"><Cube currency="USD" rate="{rate}"/>{extra}</Cube></Cube>
</gesmes:Envelope>''').encode()


class Clock:
    def __init__(self, value=START + 2):
        self.value = value

    def __call__(self):
        answer = ts(self.value)
        self.value += 1
        return answer


class Harness:
    def __init__(self, profile="faq_snapshot", enabled=True):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.url = FAQ if profile == "faq_snapshot" else ECB
        self.profile = profile
        self.policy = Policy("synthetic_direct_test", START, START + 100, 5, 5 * 2048)
        self.source = Source("source", self.url, 10, 5, 5 * 2048, 2048, approved=True)
        self.scheduler = Scheduler(self.path / "scheduler.sqlite", self.policy, (self.source,), enabled=True)
        self.clock = Clock()
        self.store = EvidenceStore(self.path / "evidence.sqlite", clock=self.clock)
        self.adapter = DirectSourceAdapter(self.path / "adapter.sqlite", self.scheduler, self.store,
                                           {"source": profile}, offline_test_enabled=enabled, clock=self.clock)

    def receive(self, body=None, offset=0, status=200, url=None, reaped=True):
        body = body if body is not None else html() if self.profile == "faq_snapshot" else xml()
        request = self.scheduler.claim(START + offset)
        assert request is not None
        self.scheduler.finish(request, Response(url or self.url, status, body, START + offset + 1, reaped))
        evidence = CaptureEvidence(request.run_id, request.configuration_hash, request.attempt_id,
                                   request.source_id, request.url, request.url,
                                   ts(START + offset), ts(START + offset + 1), 50.0 + offset, 51.0 + offset,
                                   sha256(body).hexdigest(), len(body), status,
                                   "http_entity_bytes_identity", "synthetic_transport_test", True, 0, True, True, "received",
                                   (("content-type", "text/html; charset=utf-8" if self.profile == "faq_snapshot" else "application/xml"),
                                    ("content-length", str(len(body))), ("etag", '"untrusted-cache-validator"')))
        self.clock.value = max(self.clock.value, START + offset + 2)
        return request.attempt_id, evidence

    def close(self):
        self.adapter.close()
        self.scheduler.close()
        self.temp.cleanup()


class DirectTests(unittest.TestCase):
    def harness(self, *args, **kwargs):
        result = Harness(*args, **kwargs)
        self.addCleanup(result.close)
        return result

    def test_disabled_default_has_no_ingest(self):
        h = self.harness(enabled=False)
        aid, capture = h.receive()
        with self.assertRaisesRegex(Rejected, "adapter_disabled"):
            h.adapter.ingest(aid, capture)
        self.assertEqual(h.store.verify()["observations"], 0)
        self.assertEqual(h.adapter.db.execute("SELECT COUNT(*) FROM mappings").fetchone()[0], 0)

    def test_fresh_bodies_create_distinct_versions_same_event(self):
        h = self.harness()
        first, evidence = h.receive(html("Fresh source document version A"))
        h.adapter.ingest(first, evidence)
        second, evidence2 = h.receive(html("Fresh source document version B"), offset=10)
        h.adapter.ingest(second, evidence2)
        items = h.adapter.events_as_locally_observed(ts(START + 50))
        self.assertEqual(len(items), 2)
        self.assertEqual(len({x["event"]["event_id"] for x in items}), 1)
        self.assertEqual(len({x["event"]["version_id"] for x in items}), 2)
        for item in items:
            self.assertFalse(item["eligible_for_backtest"])
            self.assertFalse(item["original_verified"])
            self.assertFalse(item["clock_verified"])
            self.assertIsNone(item["historical_available_at"])
            self.assertEqual(item["order_permission"], "none")
            self.assertEqual(item["event"]["parsed"]["event_type"], "reference_document_snapshot")
            self.assertIsNone(item["event"]["parsed"]["publisher_timestamp"])
            self.assertTrue(item["observations"][0]["synthetic"])
        self.assertEqual(h.store.verify()["observations"], 2)

    def test_duplicate_bytes_retain_new_observation(self):
        h = self.harness()
        aid, capture = h.receive()
        one = h.adapter.ingest(aid, capture)
        repeated = h.adapter.ingest(aid, capture)
        self.assertTrue(repeated["duplicate_call"])
        self.assertEqual(one["observation_seq"], repeated["observation_seq"])
        aid2, capture2 = h.receive(offset=10)
        h.adapter.ingest(aid2, capture2)
        self.assertEqual(h.store.verify()["versions"], 1)
        self.assertEqual(h.store.verify()["observations"], 2)
        self.assertEqual(len(h.adapter.events_as_locally_observed(ts(START + 50))[0]["observations"]), 2)

    def test_no_lookahead_before_confirmation(self):
        h = self.harness()
        aid, capture = h.receive()
        h.adapter.ingest(aid, capture)
        row = h.adapter.db.execute("SELECT * FROM mappings").fetchone()
        self.assertEqual(h.adapter.events_as_locally_observed(ts(START - 1)), [])
        self.assertEqual(h.adapter.events_as_locally_observed(capture.received_at), [])
        self.assertEqual(h.adapter.events_as_locally_observed(row["normalized_at"]), [])
        boundary = datetime.fromisoformat(row["confirmed_by"]).timestamp()
        self.assertEqual(h.adapter.events_as_locally_observed(ts(boundary - .000001)), [])
        self.assertEqual(len(h.adapter.events_as_locally_observed(row["confirmed_by"])), 1)

    def test_denials_redirects_bounds_and_unreaped_never_normalize(self):
        cases = (({"status": 401}, "failed"), ({"status": 403}, "failed"), ({"status": 429}, "failed"),
                 ({"status": 503}, "failed"), ({"url": FAQ + "/other"}, "failed"),
                 ({"body": b"x" * 2049}, "failed"), ({"body": b""}, "failed"), ({"reaped": False}, "failed"))
        for args, state in cases:
            with self.subTest(args=args):
                h = self.harness()
                aid, evidence = h.receive(**args)
                self.assertEqual(h.scheduler.db.execute("SELECT state FROM attempts").fetchone()[0], state)
                with self.assertRaisesRegex(Rejected, "accepted_response_required"):
                    h.adapter.ingest(aid, evidence)
                self.assertEqual(h.store.verify()["observations"], 0)
                self.assertIsNone(h.scheduler.claim(START + 10))

    def test_provenance_rejections_are_consumed_and_halt_adapter(self):
        cases = ({"representation": "web_tool_extracted_text"},
                 {"evidence_class": "saved_fixture_replay"}, {"final_url": FAQ + "/x"},
                 {"run_id": "other"}, {"configuration_hash": "0" * 64},
                 {"body_sha256": "0" * 64}, {"body_bytes": 1}, {"http_status": 403},
                 {"attempt_id": True}, {"body_complete": False}, {"tls_verified": False},
                 {"owned_transport_reaped": False}, {"redirect_count": 1}, {"outcome": "uncertain"},
                 {"requested_at": ts(START - 1)}, {"received_at": ts(START + 2)},
                 {"receive_monotonic": 99.0}, {"receive_monotonic": 49.0})
        for patch in cases:
            with self.subTest(patch=patch):
                h = self.harness()
                aid, evidence = h.receive()
                with self.assertRaises((Rejected, ValueError)):
                    h.adapter.ingest(aid, replace(evidence, **patch))
                row = h.adapter.db.execute("SELECT state FROM mappings").fetchone()
                self.assertEqual(row[0], "rejected")
                self.assertEqual(h.adapter.db.execute("SELECT halted FROM adapter_metadata").fetchone()[0], 1)
                self.assertEqual(h.store.verify()["observations"], 0)
                with self.assertRaisesRegex(Rejected, "no_retry"):
                    h.adapter.ingest(aid, replace(evidence, **patch))

    def test_headers_are_bounded_and_not_promoted_to_publication(self):
        h = self.harness()
        aid, capture = h.receive()
        capture = replace(capture, headers=capture.headers + (("date", "Wed, 28 Oct 1998 10:00:00 GMT"),
                                                               ("last-modified", "Wed, 28 Oct 1998 10:00:00 GMT")))
        h.adapter.ingest(aid, capture)
        item = h.adapter.events_as_locally_observed(ts(START + 50))[0]
        self.assertIsNone(item["event"]["parsed"]["publisher_timestamp"])
        self.assertEqual(h.adapter.events_as_locally_observed("1998-10-28T11:00:00Z"), [])
        self.assertIsNone(item["source_available_at"])

    def test_content_encoding_length_media_charset_reject(self):
        for headers in ((('content-type', 'text/plain'),),
                        (("content-type", "text/html"), ("content-encoding", "gzip")),
                        (("content-type", "text/html"), ("content-length", "99")),
                        (("content-type", "text/html; charset=iso-8859-1"),)):
            with self.subTest(headers=headers):
                h = self.harness()
                aid, capture = h.receive()
                with self.assertRaises(Rejected):
                    h.adapter.ingest(aid, replace(capture, headers=headers))
                self.assertEqual(h.store.verify()["observations"], 0)

    def test_secret_header_refused_before_capture_persistence(self):
        h = self.harness()
        aid, capture = h.receive()
        with self.assertRaisesRegex(Rejected, "capture_contract_rejected"):
            h.adapter.ingest(aid, replace(capture, headers=(("authorization", "secret-sentinel"),)))
        self.assertNotIn(b"secret-sentinel", h.adapter.path.read_bytes())
        self.assertNotIn("secret-sentinel", h.adapter.db.execute("SELECT capture_json FROM mappings").fetchone()[0])
        self.assertEqual(h.adapter.db.execute("SELECT state FROM mappings").fetchone()[0], "rejected")
        with self.assertRaisesRegex(Rejected, "no_retry"):
            h.adapter.ingest(aid, capture)

    def test_malformed_capture_metadata_consumes_attempt(self):
        for patch in ({"headers": (("content-type", "a\r\nb"),)},
                      {"headers": (("content-type", "x" * 1025),)},
                      {"headers": (("content-type", "text/html"), ("content-type", "text/html"))},
                      {"receive_monotonic": float("nan")}, {"receive_monotonic": float("inf")},
                      {"run_id": "x" * 2049}):
            with self.subTest(patch=str(patch)[:60]):
                h = self.harness()
                aid, capture = h.receive()
                with self.assertRaisesRegex(Rejected, "capture_contract_rejected"):
                    h.adapter.ingest(aid, replace(capture, **patch))
                self.assertEqual(h.adapter.db.execute("SELECT state FROM mappings").fetchone()[0], "rejected")
                self.assertEqual(h.store.verify()["observations"], 0)
                with self.assertRaisesRegex(Rejected, "no_retry"):
                    h.adapter.ingest(aid, capture)

    def test_invalid_html_and_utf8_reject(self):
        for raw in (b"<title>No html</title>", b"<html><title>Unclosed", html("x" * 1025),
                    b"<html><title>A</title><title>B</title></html>", b"\xff", b"\x00"):
            with self.subTest(raw=raw[:20]):
                h = self.harness()
                aid, capture = h.receive(raw)
                with self.assertRaises((Rejected, ValueError)):
                    h.adapter.ingest(aid, capture)
                self.assertEqual(h.store.verify()["observations"], 0)

    def test_ecb_fresh_reference_not_publication_time(self):
        h = self.harness("ecb_daily_reference")
        aid, capture = h.receive(xml(extra='<Cube currency="JPY" rate="152.05"/>'))
        h.adapter.ingest(aid, capture)
        items = h.adapter.events_as_locally_observed(ts(START + 50))
        self.assertEqual(len(items), 2)
        for item in items:
            self.assertIsNone(item["event"]["parsed"]["publisher_claim"])
            self.assertIsNone(item["event"]["parsed"]["publisher_timestamp"])
            self.assertEqual(item["event"]["parsed"]["event_type"], "fx_daily_reference")
            self.assertIn("2026-10-28", item["event"]["parsed"]["logical_key"])

    def test_ecb_invalid_payloads_whole_batch_atomic(self):
        cases = [xml(day="2026-10-29"), xml(day="invalid"), xml(rate="NaN"), xml(rate="1e999999"),
                 xml(rate="0"), xml(extra='<Cube currency="USD" rate="2"/>'),
                 xml(extra='<Cube currency="EUR" rate="1"/>'), xml(extra='<Cube currency="GBP" rate="-1"/>'),
                 xml().replace(b"<gesmes:Envelope", b'<!DOCTYPE x [<!ENTITY a "xxx">]><gesmes:Envelope'),
                 b"<notxml>", xml().replace(b"<Cube><Cube time", b"<Cube><Cube time").replace(b"</Cube></Cube>", b"</Cube><Cube time='2026-10-27'/></Cube>"),
                 xml(extra=''.join(f'<Cube currency="A{chr(65 + i // 26)}{chr(65 + i % 26)}" rate="1"/>' for i in range(100)))]
        for raw in cases:
            with self.subTest(raw=raw[:100]):
                h = self.harness("ecb_daily_reference")
                # Scheduler itself rejects the >2KiB case; adapter accepts no partial events.
                aid, capture = h.receive(raw)
                with self.assertRaises((Rejected, ValueError)):
                    h.adapter.ingest(aid, capture)
                self.assertEqual(h.store.verify()["versions"], 0)

    def test_storage_commit_uncertain_never_retried_or_visible(self):
        h = self.harness()
        aid, capture = h.receive()
        def fail():
            raise RuntimeError("synthetic crash")
        with self.assertRaises(RuntimeError):
            h.adapter.ingest(aid, capture, _after_store=fail)
        self.assertEqual(h.store.verify()["observations"], 1)
        self.assertEqual(h.adapter.db.execute("SELECT state FROM mappings").fetchone()[0], "claimed")
        self.assertEqual(h.adapter.events_as_locally_observed(ts(START + 50)), [])
        with self.assertRaisesRegex(Rejected, "no_retry"):
            h.adapter.ingest(aid, capture)
        with self.assertRaisesRegex(Rejected, "mapping_not_complete"):
            h.adapter.confirm_mapping(aid)

    def test_pending_confirmation_explicit_later_no_backdating(self):
        h = self.harness()
        aid, capture = h.receive()
        def fail():
            raise RuntimeError("synthetic failure")
        with self.assertRaises(RuntimeError):
            h.adapter.ingest(aid, capture, _before_confirm=fail)
        self.assertEqual(h.adapter.ingest(aid, capture)["confirmation"], "pending")
        self.assertEqual(h.adapter.events_as_locally_observed(ts(START + 20)), [])
        h.clock.value = START + 30
        h.adapter.confirm_mapping(aid)
        self.assertEqual(h.adapter.events_as_locally_observed(ts(START + 20)), [])
        self.assertEqual(len(h.adapter.events_as_locally_observed(ts(START + 31))), 1)
        self.assertEqual(h.store.verify()["observations"], 1)

    def test_backward_normalization_and_confirmation_clocks(self):
        for boundary in ("normalization", "confirmation"):
            with self.subTest(boundary=boundary):
                h = self.harness()
                aid, capture = h.receive()
                h.adapter.clock = (lambda: ts(START)) if boundary == "normalization" else (lambda: ts(START + 2))
                with self.assertRaises(Rejected):
                    h.adapter.ingest(aid, capture)
                self.assertEqual(h.adapter.events_as_locally_observed(ts(START + 50)), [])

    def test_restart_identity_and_idempotency(self):
        h = self.harness()
        aid, capture = h.receive()
        one = h.adapter.ingest(aid, capture)
        h.adapter.close()
        h.adapter = DirectSourceAdapter(h.path / "adapter.sqlite", h.scheduler, h.store,
                                       {"source": h.profile}, offline_test_enabled=True, clock=h.clock)
        self.assertEqual(h.adapter.ingest(aid, capture)["observation_seq"], one["observation_seq"])
        self.assertEqual(h.store.verify()["observations"], 1)
        wrong_store = EvidenceStore(h.path / "wrong.sqlite", clock=h.clock)
        with self.assertRaisesRegex(ValueError, "adapter_identity_mismatch"):
            DirectSourceAdapter(h.path / "adapter.sqlite", h.scheduler, wrong_store, {"source": h.profile})

    def test_run_config_and_source_runtime_tamper_reject(self):
        h = self.harness()
        aid, capture = h.receive()
        h.scheduler.sources["source"] = replace(h.source, approved=False)
        with self.assertRaisesRegex(Rejected, "scheduler_runtime_configuration_changed"):
            h.adapter.ingest(aid, capture)

    def test_stored_scheduler_bytes_tamper_reject(self):
        h = self.harness()
        aid, capture = h.receive()
        h.scheduler.db.execute("UPDATE attempts SET body=? WHERE id=?", (b"tampered", aid))
        with self.assertRaisesRegex(Rejected, "scheduler_body_hash"):
            h.adapter.ingest(aid, capture)

    def test_changed_capture_same_attempt_rejects(self):
        h = self.harness()
        aid, capture = h.receive()
        h.adapter.ingest(aid, capture)
        with self.assertRaisesRegex(Rejected, "attempt_evidence_conflict"):
            h.adapter.ingest(aid, replace(capture, headers=capture.headers + (("age", "10"),)))

    def test_corrupt_store_lineage_query_rejects(self):
        h = self.harness()
        aid, capture = h.receive()
        h.adapter.ingest(aid, capture)
        h.adapter.db.execute("UPDATE mappings SET store_record_hash=?", ("0" * 64,))
        with self.assertRaisesRegex(Rejected, "store_mapping_lineage"):
            h.adapter.events_as_locally_observed(ts(START + 50))

    def test_symlink_unrelated_and_shared_database_refused(self):
        h = self.harness()
        alias = h.path / "alias.sqlite"
        alias.symlink_to(h.path / "adapter.sqlite")
        with self.assertRaisesRegex(ValueError, "adapter_path"):
            DirectSourceAdapter(alias, h.scheduler, h.store, {"source": h.profile})
        with self.assertRaisesRegex(ValueError, "separate_database_required"):
            DirectSourceAdapter(h.scheduler.path, h.scheduler, h.store, {"source": h.profile})
        unrelated = h.path / "unrelated.sqlite"
        db = sqlite3.connect(unrelated)
        try:
            db.execute("CREATE TABLE unrelated(x)")
        finally:
            db.close()
        before = unrelated.read_bytes()
        with self.assertRaisesRegex(ValueError, "not_adapter_database"):
            DirectSourceAdapter(unrelated, h.scheduler, h.store, {"source": h.profile})
        self.assertEqual(before, unrelated.read_bytes())


if __name__ == "__main__":
    unittest.main()
