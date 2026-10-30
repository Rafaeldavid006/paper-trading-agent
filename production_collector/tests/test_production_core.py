"""Production policy/provenance unit tests. All transport is mocked: zero HTTP.

These tests deliberately patch the compiled release validator; their fake live
receipts are test artifacts, never evidence of an actual retrieval.
"""
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from recurring import Lease, LeaseCollector, LIMIT
import owned_transport
import source_worker
from transport_contract import decode


@dataclass(frozen=True)
class TestRelease:
    root: str
    collector_id: str
    lease: dict
    renewals: tuple = ()
    release_id: str = "initial"


class ProductionCoreTests(unittest.TestCase):
    def make(self):
        import production_release
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = str(Path(temp.name) / 'live')
        now = int(time.time())
        lease = Lease('production_fixture', now, now + 7 * 86400, 86400,
                      7, 7 * LIMIT, 'test:compiled_release', synthetic_only=False)
        release = TestRelease(root, 'live_fixture', asdict(lease))
        def require(value):
            if value is not release:
                raise ValueError('compiled_release_required')
        mock = patch.object(production_release, 'require_release', require)
        mock.start(); self.addCleanup(mock.stop)
        for name, fn in (
            ('approved_leases', lambda value: (value.lease,) + value.renewals),
            ('require_collection_release', lambda value: value),
            ('require_enrollment_release', lambda value: value),
            ('release_manifest_hash', lambda value: __import__('hashlib').sha256(value.release_id.encode()).hexdigest()),
            ('stable_identity', lambda value: {'root': value.root, 'collector_id': value.collector_id})):
            mock = patch.object(production_release, name, fn, create=True)
            mock.start(); self.addCleanup(mock.stop)
        c = LeaseCollector(root, 'live_fixture', create=True, live_release=release)
        self.addCleanup(c.close)
        c.add_lease(lease)
        return c, lease, release

    def test_closed_default_release_prevents_transport(self):
        import production_release
        with patch.object(production_release, 'APPROVED_RELEASE', None):
            with self.assertRaises(ValueError):
                source_worker.request_once()
        with self.assertRaises(ValueError):
            owned_transport.collect('live', stop=threading.Event(), deadline_mono=time.monotonic() + 1)

    def test_live_lease_bounds_and_provenance_hash(self):
        lease = Lease('one', 0, 7 * 86400, 86400, 7, 7 * LIMIT, 'test:one', synthetic_only=False)
        self.assertNotEqual(lease.policy_hash, replace(lease, synthetic_only=True).policy_hash)
        for changes in ({'cadence': 3600}, {'request_limit': 8}, {'byte_limit': 8 * LIMIT}, {'synthetic_only': 1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(lease, **changes)

    def test_live_mode_rejects_fixture_injection_and_uncompiled_lease(self):
        c, lease, _ = self.make()
        for kwargs in ({}, {'mode': 'ok'}, {'mode': 'live', '_popen': lambda: None}, {'mode': 'live', '_fault': lambda: None}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                c.tick(**kwargs)
        with self.assertRaises(ValueError):
            c.add_lease(replace(lease, synthetic_only=True))
        with self.assertRaises(ValueError):
            c.add_lease(replace(lease, lease_id='not_compiled'))
        self.assertEqual(c.status()['attempts'], 0)

    def test_mocked_live_receipt_uses_live_provenance_not_clock_claim(self):
        c, _, _ = self.make()
        # Construct fixture bytes in memory. No worker, opener or socket invoked.
        body = b'<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref"><Cube><Cube time="2026-10-28"><Cube currency="USD" rate="1.10"/></Cube></Cube></gesmes:Envelope>'
        from transport_contract import URL
        meta = {"url": URL, "status": 200, "headers": {"content-type": "application/xml"}, "body_complete": True, "tls_verified": True}
        def response(*args, **kwargs):
            return owned_transport.Result(meta, body, c.wall(), c.mono(), True, 'received', 123, 0, len(body))
        with patch('recurring.collect', response):
            status = c.tick(mode='live')
        self.assertEqual(status['action'], 'confirmed')
        self.assertFalse(status['synthetic_only'])
        self.assertIsNone(status['live_http_requests'])
        self.assertEqual(status['live_http_attempts_reserved'], 1)
        self.assertEqual(status['reference_data_date'], '2026-10-28')
        self.assertFalse(status['clock_verified'])
        self.assertEqual(status['lifetime_requests_remaining'], 127)
        observations = c.observations('2099-01-01T00:00:00Z')
        self.assertTrue(observations)
        self.assertFalse(observations[0]['synthetic'])
        self.assertFalse(observations[0]['original_verified'])
        self.assertFalse(observations[0]['eligible_for_backtest'])
        capture = c._payloads(1)['CAPTURED']['capture']
        self.assertEqual(capture['evidence_class'], 'direct_http_caller_attestation')
        from direct_adapter import CaptureEvidence
        capture['headers'] = tuple(tuple(x) for x in capture['headers'])
        forged = replace(CaptureEvidence(**capture), evidence_class='synthetic_transport_test')
        attempt = dict(c.db.execute('SELECT * FROM attempts WHERE id=1').fetchone())
        with self.assertRaisesRegex(ValueError, 'capture_provenance'):
            c._receipt(attempt, forged, body)
        self.assertEqual(c.tick(mode='live')['action'], 'no_claim')

    def test_mutated_runtime_provenance_refused(self):
        c, _, _ = self.make()
        c.synthetic_only = True
        with self.assertRaises(ValueError):
            c.status()

    @staticmethod
    def response_for(c):
        from transport_contract import URL
        body = b'<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref"><Cube><Cube time="2026-10-28"><Cube currency="USD" rate="1.10"/></Cube></Cube></gesmes:Envelope>'
        meta = {'url': URL, 'status': 200, 'headers': {'content-type': 'application/xml'}, 'body_complete': True, 'tls_verified': True}
        return owned_transport.Result(meta, body, c.wall(), c.mono(), True, 'received', 123, 0, len(body))

    def test_live_renewal_reopens_same_ledger_and_retains_old_policies(self):
        import production_release
        c, lease, release = self.make()
        root = c.root.path
        identities = {p.name: p.stat().st_ino for p in root.glob('*.sqlite')}
        with patch('recurring.collect', lambda *a, **k: self.response_for(c)):
            self.assertEqual(c.tick(mode='live')['attempt_id'], 1)
        c.close()
        second = replace(lease, lease_id='renewal', start=lease.end, end=lease.end + 7 * 86400)
        renewed = replace(release, renewals=(asdict(second),), release_id='new_metadata')
        anchor = time.monotonic()
        wall = lambda: datetime.fromtimestamp(second.start + .1 + time.monotonic() - anchor, timezone.utc).isoformat(timespec='microseconds')
        def require(value):
            if value is not renewed:
                raise ValueError('compiled_release_required')
        with patch.object(production_release, 'require_release', require):
            c = LeaseCollector(root, release.collector_id, live_release=renewed, wall=wall)
            self.addCleanup(c.close)
            self.assertEqual(c.status()['attempts'], 1)
            self.assertEqual(c.add_lease(lease), 'already_recorded')
            self.assertEqual(c.add_lease(second), 'recorded')
            self.assertEqual(c.add_lease(second), 'already_recorded')
            with patch('recurring.collect', lambda *a, **k: self.response_for(c)):
                self.assertEqual(c.tick(mode='live')['attempt_id'], 2)
            self.assertEqual(c.tick(mode='live')['action'], 'no_claim')
            self.assertEqual(c.status()['reserved_bytes'], 2 * LIMIT)
            import json
            grants = [json.loads(row[0]) for row in c.db.execute("SELECT payload FROM events WHERE phase='LEASE_ADDED' ORDER BY seq")]
            self.assertEqual([g['release_id'] for g in grants], ['initial', 'new_metadata'])
            self.assertNotEqual(grants[0]['approval_manifest_sha256'], grants[1]['approval_manifest_sha256'])
            self.assertEqual(identities, {p.name: p.stat().st_ino for p in root.glob('*.sqlite')})
            self.assertEqual(len(c.observations('2099-01-01T00:00:00Z')[0]['observations']), 2)
            c.close()
        # Publication that removes or mutates historical authority cannot reopen
        # the old ledger, even when its stable profile identity is unchanged.
        for invalid in (replace(renewed, lease=asdict(second), renewals=()),
                        replace(renewed, lease=asdict(replace(lease, approval_ref='test:mutated')))):
            with self.subTest(invalid=invalid), patch.object(production_release, 'require_release', lambda v: v):
                with self.assertRaisesRegex(ValueError, 'compiled_lease_required'):
                    LeaseCollector(root, release.collector_id, live_release=invalid, wall=wall)

    def test_live_renewal_overlap_refused_without_clearing_fence(self):
        import production_release
        c, lease, release = self.make()
        overlapping = replace(lease, lease_id='overlap')
        changed = replace(release, renewals=(asdict(overlapping),))
        c.live_release = changed
        with patch.object(production_release, 'require_release', lambda v: v):
            with self.assertRaisesRegex(ValueError, 'overlapping_lease'):
                c.add_lease(overlapping)
            c._write(lambda: c._set_gate(fence='test_uncertainty'))
            next_lease = replace(lease, lease_id='next', start=lease.end, end=lease.end + 86400)
            c.live_release = replace(release, renewals=(asdict(next_lease),))
            with self.assertRaisesRegex(ValueError, 'renewal_cannot_clear_gate'):
                c.add_lease(next_lease)
            self.assertEqual(c.status()['state'], 'fenced')

    def test_live_late_slot_grace_no_old_slot_catchup_or_duplicate(self):
        c, lease, _ = self.make()
        anchor = time.monotonic()
        offset = [3600]
        c.wall = lambda: datetime.fromtimestamp(lease.start + offset[0] + time.monotonic() - anchor, timezone.utc).isoformat(timespec='microseconds')
        c.store.clock = c.wall
        with patch('recurring.collect', lambda *a, **k: self.response_for(c)):
            self.assertEqual(c.tick(mode='live')['attempt_id'], 1)
        self.assertEqual(c.tick(mode='live')['action'], 'no_claim')
        # Beyond six hours in the next slot: no backfilled prior-slot request.
        offset[0] = 86400 + 21601
        self.assertEqual(c.tick(mode='live')['action'], 'no_claim')
        self.assertEqual(c.status()['attempts'], 1)
        offset[0] = 2 * 86400 + 1800
        with patch('recurring.collect', lambda *a, **k: self.response_for(c)):
            self.assertEqual(c.tick(mode='live')['attempt_id'], 2)
        slots = [r[0] for r in c.db.execute('SELECT slot FROM attempts ORDER BY id')]
        self.assertEqual(slots, [lease.start, lease.start + 2 * 86400])

    def test_enrollment_phase_cannot_claim_or_report_active(self):
        import production_release
        c, _, _ = self.make()
        def refused(value):
            raise ValueError('collection_phase_required')
        with patch.object(production_release, 'require_collection_release', refused):
            self.assertEqual(c.status()['state'], 'enrollment_only')
            self.assertFalse(c.status()['active_collection'])
            with self.assertRaisesRegex(ValueError, 'collection_phase_required'):
                c.tick(mode='live')
            self.assertEqual(c.status()['attempts'], 0)

    def test_live_manifest_is_append_only_prefix_not_membership(self):
        import production_release
        c, lease, release = self.make()
        earlier = replace(lease, lease_id='inserted_before_history',
                          start=lease.start - 7 * 86400, end=lease.start)
        c.live_release = replace(release, lease=asdict(earlier), renewals=(asdict(lease),))
        with patch.object(production_release, 'require_release', lambda v: v):
            with self.assertRaisesRegex(ValueError, 'compiled_lease_required'):
                c.verify()
            with self.assertRaisesRegex(ValueError, 'compiled_lease_required'):
                c.add_lease(earlier)
            self.assertEqual(c.db.execute('SELECT COUNT(*) FROM leases').fetchone()[0], 1)
            second = replace(lease, lease_id='second', start=lease.end, end=lease.end + 86400)
            third = replace(second, lease_id='third', start=second.end, end=second.end + 86400)
            c.live_release = replace(release, renewals=(asdict(second), asdict(third)))
            with self.assertRaisesRegex(ValueError, 'compiled_lease_required'):
                c.add_lease(third)
            self.assertEqual(c.add_lease(lease), 'already_recorded')
            self.assertEqual(c.add_lease(second), 'recorded')
            self.assertEqual(c.add_lease(third), 'recorded')
            self.assertEqual(c.db.execute('SELECT COUNT(*) FROM leases').fetchone()[0], 3)
