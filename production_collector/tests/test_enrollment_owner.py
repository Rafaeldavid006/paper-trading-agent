"""Offline bootstrap/owner regressions; no live source request or real mount claim."""
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import collector_owner as owner
import enrollment
import production_release as policy
from recurring import LeaseCollector


class EnrollmentOwnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.parent = Path(self.temp.name)
        self.root = self.parent / "state"
        self.start = int(datetime(2026, 11, 1, 18, tzinfo=timezone.utc).timestamp())
        self.release = policy.Release("release_test", str(self.root), "research-collector-ecb-v1",
            "enrollment_test", self.start - 100,
            dict(lease_id="lease_test", start=self.start, end=self.start + 7*86400,
                 cadence=86400, request_limit=7, byte_limit=7*65536,
                 approval_ref="user:approved_test", allow_transient_next_slot=False,
                 synthetic_only=False))
        self.patches = [patch.object(policy, "ROOT", str(self.root)),
                        patch.object(policy, "APPROVED_RELEASE", self.release),
                        patch.object(policy, "ENROLLMENT_APPROVED", True),
                        patch("enrollment.time.time", return_value=self.start-200)]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.temp.cleanup()

    def enroll(self, **kw):
        return enrollment.enroll_once(self.release, _test_mount=True, **kw)

    def test_release_is_compiled_finite_daily_and_ordinary_immutable(self):
        self.assertIs(policy.require_release(self.release), self.release)
        with self.assertRaises(TypeError):
            self.release.lease["request_limit"] = 100
        self.assertEqual(asdict(self.release)["lease"]["request_limit"], 7)
        with self.assertRaises(ValueError):
            policy.require_release(replace(self.release, release_id="different"))
        invalid = replace(self.release, lease={**self.release.lease, "cadence":1})
        with patch.object(policy, "APPROVED_RELEASE", invalid), self.assertRaises(ValueError):
            policy.require_release(invalid)

    def test_closed_enrollment_does_not_create(self):
        with patch.object(policy, "ENROLLMENT_APPROVED", False), self.assertRaises(ValueError):
            self.enroll()
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_reopen_missing_refuses_without_creation(self):
        with self.assertRaises((ValueError, OSError)):
            enrollment.verify_enrollment(self.release, _test_mount=True)
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_interrupted_marker_prevents_second_attempt(self):
        def crash(where):
            if where == "after_started":
                raise RuntimeError("synthetic_crash")
        with self.assertRaises(RuntimeError):
            self.enroll(_fault=crash)
        self.assertFalse(self.root.exists())
        with self.assertRaisesRegex(ValueError,"existing_or_partial"):
            self.enroll()
        with self.assertRaises((ValueError,OSError)):
            enrollment.verify_enrollment(self.release,_test_mount=True)

    def test_complete_enrollment_close_reopen_and_deleted_root_refusal(self):
        self.assertEqual(self.enroll()["source_requests"],0)
        enrollment.verify_enrollment(self.release,_test_mount=True)
        collector=LeaseCollector(self.root,self.release.collector_id,live_release=self.release)
        try:
            status=collector.status()
            self.assertEqual(status["attempts"],0)
            self.assertFalse(status["synthetic_only"])
            self.assertEqual(collector.db.execute("SELECT COUNT(*) FROM leases").fetchone()[0],1)
        finally:collector.close()
        shutil.rmtree(self.root)
        with self.assertRaises((ValueError,OSError)):
            enrollment.verify_enrollment(self.release,_test_mount=True)
        with self.assertRaisesRegex(ValueError,"existing_or_partial"):
            self.enroll()

    def test_root_ready_without_complete_marker_never_resumes(self):
        def crash(where):
            if where == "after_root":raise RuntimeError("synthetic_crash")
        with self.assertRaises(RuntimeError):self.enroll(_fault=crash)
        self.assertTrue((self.root/"ready.json").is_file())
        with self.assertRaises((ValueError,OSError)):
            enrollment.verify_enrollment(self.release,_test_mount=True)
        with self.assertRaises(ValueError):self.enroll()

    def test_changed_complete_or_symlink_marker_refuses(self):
        self.enroll()
        _,started,complete=enrollment._paths(self.release)
        data=json.loads(complete.read_text());data["root_identity"]=[0,0]
        complete.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,"root_missing_or_changed"):
            enrollment.verify_enrollment(self.release,_test_mount=True)
        complete.unlink();complete.symlink_to(started)
        with self.assertRaises(ValueError):
            enrollment.verify_enrollment(self.release,_test_mount=True)

    def test_real_mount_gate_rejects_temporary_directory(self):
        with self.assertRaisesRegex(ValueError,"fixed_mount"):
            enrollment.qualify_mount(self.root)
        self.assertEqual(list(self.parent.iterdir()),[])

    def test_wrong_python_refuses_before_storage_or_network(self):
        with patch.object(policy,"REQUIRED_PYTHON",(0,0,0)), self.assertRaisesRegex(ValueError,"exact_python"):
            owner.run(self.release,threading.Event())
        self.assertEqual(list(self.parent.iterdir()),[])

    def test_status_full_pipe_never_blocks_or_leaks_caller_permission(self):
        read,write=os.pipe()
        try:
            os.set_blocking(write,False)
            while True:
                try:os.write(write,b'x'*4096)
                except BlockingIOError:break
            before=time.monotonic()
            self.assertFalse(owner.emit_status({"state":"idle"},fd=write))
            self.assertLess(time.monotonic()-before,.2)
            os.set_blocking(read,False)
            while True:
                try:
                    if not os.read(read,65536):break
                except BlockingIOError:break
            self.assertTrue(owner.emit_status({"state":"idle","order_permission":"bogus"},fd=write))
            payload=json.loads(os.read(read,4096))
            self.assertEqual(payload["order_permission"],"none")
            self.assertIn("heartbeat_utc",payload)
        finally:os.close(read);os.close(write)

class TwoPhaseAndManifestTests(unittest.TestCase):
    # Reuse fixtures without rerunning/inflating the original test cases.
    setUp = EnrollmentOwnerTests.setUp
    tearDown = EnrollmentOwnerTests.tearDown
    enroll = EnrollmentOwnerTests.enroll
    def collection_release(self, **changes):
        _, _, complete = enrollment._paths(self.release)
        from filesystem import digest
        return replace(self.release, phase="collect",
                       enrollment_receipt_sha256=digest(json.loads(complete.read_text())), **changes)

    def test_enroll_then_collect_pin_and_same_identity_after_renewal(self):
        self.enroll()
        collect = self.collection_release(release_id="collect_deploy")
        with patch.object(policy,"APPROVED_RELEASE",collect),patch.object(policy,"ENROLLMENT_APPROVED",False):
            self.assertIs(policy.require_collection_release(collect),collect)
            enrollment.verify_enrollment(collect,_test_mount=True)
            c=LeaseCollector(self.root,collect.collector_id,live_release=collect)
            c.close()
            next_lease={**collect.lease,"lease_id":"lease_two","start":collect.lease["end"],"end":collect.lease["end"]+7*86400}
            renewed=replace(collect,renewals=(next_lease,),release_id="renewal_deploy")
        with patch.object(policy,"APPROVED_RELEASE",renewed),patch.object(policy,"ENROLLMENT_APPROVED",False):
            enrollment.verify_enrollment(renewed,_test_mount=True)
            c=LeaseCollector(self.root,renewed.collector_id,live_release=renewed)
            try:
                from recurring import Lease
                c.add_lease(Lease(**next_lease))
                self.assertEqual(c.db.execute("SELECT COUNT(*) FROM leases").fetchone()[0],2)
                self.assertEqual(c.status()["attempts"],0)
            finally:c.close()

    def test_active_phase_total_loss_never_recreates_controls(self):
        self.enroll();collect=self.collection_release()
        root,started,complete=enrollment._paths(self.release)
        shutil.rmtree(root);started.unlink();complete.unlink()
        with patch.object(policy,"APPROVED_RELEASE",collect),patch.object(policy,"ENROLLMENT_APPROVED",False):
            with self.assertRaisesRegex(ValueError,"never_bootstraps"):
                enrollment.ensure_enrolled(collect,_test_mount=True)
        self.assertEqual(list(self.parent.iterdir()),[])

    def test_collection_constructor_cannot_bootstrap(self):
        collect=replace(self.release,phase="collect",enrollment_receipt_sha256="0"*64)
        with patch.object(policy,"APPROVED_RELEASE",collect),patch.object(policy,"ENROLLMENT_APPROVED",False):
            with self.assertRaisesRegex(ValueError,"enrollment_phase_required"):
                LeaseCollector(self.root,collect.collector_id,create=True,live_release=collect)
        self.assertEqual(list(self.parent.iterdir()),[])

    def test_wrong_pinned_receipt_and_enrollment_collection_gate(self):
        self.enroll()
        with self.assertRaisesRegex(ValueError,"collection_phase"):
            policy.require_collection_release(self.release)
        bad=replace(self.release,phase="collect",enrollment_receipt_sha256="0"*64)
        with patch.object(policy,"APPROVED_RELEASE",bad),patch.object(policy,"ENROLLMENT_APPROVED",False):
            with self.assertRaisesRegex(ValueError,"receipt_pin"):
                enrollment.verify_enrollment(bad,_test_mount=True)

    def test_json_schema_snapshot_change_and_malformed_input(self):
        path=self.parent/"approved_release.json"
        doc={"schema":1,"enrollment_approved":True,"release":asdict(self.release)}
        path.write_text(json.dumps(doc))
        with patch.object(policy,"MANIFEST_PATH",path):
            loaded,approved,hashvalue=policy.load_manifest()
            self.assertTrue(approved)
            with patch.object(policy,"APPROVED_RELEASE",loaded),patch.object(policy,"_MANIFEST_SHA256",hashvalue):
                policy.require_release(loaded)
                path.write_text(json.dumps({**doc,"enrollment_approved":False}))
                with self.assertRaisesRegex(ValueError,"changed_requires_restart"):
                    policy.require_release(loaded)
            for raw in ('{', '{"schema":1,"schema":1}',json.dumps({**doc,"extra":"refuse"})):
                path.write_text(raw)
                with self.assertRaises((ValueError,TypeError)):
                    policy.load_manifest()
            path.unlink();path.symlink_to(self.parent/"not_a_real_file")
            with self.assertRaises(ValueError):policy.load_manifest()

    def test_cached_owner_heartbeat_does_not_revalidate_every_30_seconds(self):
        # Fully fake storage/transport, deterministic clock and stop. Only the
        # scheduling/heartbeat policy is under test; not live source evidence.
        self.enroll();collect=self.collection_release()
        now=[float(self.start+10)]
        class Stop:
            waits=0
            def is_set(inner):return inner.waits>=5
            def wait(inner,delay):
                inner.waits+=1;now[0]+=delay;return inner.is_set()
        class FakeDB:
            def execute(inner,*a):return [(collect.lease["lease_id"],)]
        class FakeCollector:
            db=FakeDB();ticks=0;closed=False
            def __init__(inner,*a,**k):pass
            def tick(inner,**k):
                inner.ticks+=1
                return {"state":"waiting_next_slot","active_collection":True,
                        "next_due_at":datetime.fromtimestamp(self.start+86400,timezone.utc).isoformat(),
                        "lease_expires_at":datetime.fromtimestamp(self.start+7*86400,timezone.utc).isoformat(),
                        "order_permission":"none"}
            def close(inner):inner.closed=True
        fake=FakeCollector();frames=[]
        clock=lambda:datetime.fromtimestamp(now[0],timezone.utc).isoformat()
        with patch.object(policy,"APPROVED_RELEASE",collect),patch.object(policy,"ENROLLMENT_APPROVED",False),\
             patch.object(policy,"REQUIRED_PYTHON",sys.version_info[:3]),\
             patch("enrollment.verify_enrollment",return_value={}),\
             patch("recurring.LeaseCollector",return_value=fake),\
             patch("owned_transport.wall_now",clock),patch("collector_owner.utc_now",clock),\
             patch("collector_owner.time.monotonic",lambda:now[0]),\
             patch("collector_owner.emit_status",lambda p:frames.append(p)):
            self.assertEqual(owner.run(collect,Stop()),0)
        self.assertEqual(fake.ticks,1)
        self.assertTrue(fake.closed)
        self.assertFalse(frames[0]["status_cached"])
        self.assertTrue(frames[1]["status_cached"])
        self.assertEqual(frames[0]["integrity_verified_at"],frames[1]["integrity_verified_at"])
        self.assertEqual(frames[-1]["state"],"stopped")
