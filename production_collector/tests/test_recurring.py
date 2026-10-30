from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from recurring import Lease, LeaseCollector, LIMIT


class Clock:
    def __init__(self):
        self.start = int(time.time())
        self.offset = .1
        self.anchor = time.monotonic()
    def __call__(self):
        return datetime.fromtimestamp(self.start + self.offset + time.monotonic() - self.anchor, timezone.utc).isoformat(timespec="microseconds")
    def jump(self, offset):
        self.offset = offset + .1
        self.anchor = time.monotonic()


class RecurringTests(unittest.TestCase):
    def make(self, **kwargs):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        path = Path(temp.name) / "collector"
        clock = Clock()
        c = LeaseCollector(path, "test_collector", create=True, offline_test_enabled=True, wall=clock, **kwargs)
        self.addCleanup(c.close)
        return c, path, clock
    def lease(self, clock, **kwargs):
        return Lease(**{**dict(lease_id="first", start=clock.start, end=clock.start + 240,
                              cadence=60, request_limit=3, byte_limit=3 * LIMIT,
                              approval_ref="synthetic:test"), **kwargs})
    def reopen(self, path, clock, **kwargs):
        c = LeaseCollector(path, "test_collector", offline_test_enabled=True, wall=clock, **kwargs)
        self.addCleanup(c.close)
        return c

    def test_disabled_default_and_live_mode_make_no_requests(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "no"
            with self.assertRaisesRegex(ValueError, "disabled"):
                LeaseCollector(p, "closed", create=True)
            self.assertFalse(p.exists())
        c, _, clock = self.make()
        c.add_lease(self.lease(clock))
        with self.assertRaisesRegex(ValueError, "offline_mode"):
            c.tick(mode="live")
        self.assertEqual(c.status()["attempts"], 0)

    def test_recurrence_versions_restart_and_quota_same_databases(self):
        c, p, clock = self.make()
        lease = self.lease(clock)
        c.add_lease(lease)
        ids = {}
        for name in ("ledger.sqlite", "evidence.sqlite"):
            ids[name] = (p / name).stat().st_ino
        self.assertEqual(c.tick()["attempt_id"], 1)
        self.assertEqual(c.tick()["action"], "no_claim")
        clock.jump(60)
        self.assertEqual(c.tick()["attempt_id"], 2)
        c.close(); c = self.reopen(p, clock)
        clock.jump(120)
        self.assertEqual(c.tick(mode="revised")["attempt_id"], 3)
        clock.jump(180)
        self.assertEqual(c.tick()["state"], "quota_exhausted")
        self.assertEqual(c.status()["reserved_bytes"], 3 * LIMIT)
        self.assertEqual(c.store.verify()["observations"], 3)
        self.assertEqual(c.store.verify()["versions"], 2)
        rows = c.observations("2099-01-01T00:00:00Z")
        self.assertEqual(sum(len(r["observations"]) for r in rows), 3)
        for name, ino in ids.items():
            self.assertEqual((p / name).stat().st_ino, ino)

    def test_immutable_nonoverlapping_renewal_preserves_global_ids_and_history(self):
        c, _, clock = self.make()
        first = self.lease(clock, end=clock.start + 120)
        self.assertEqual(c.add_lease(first), "recorded")
        self.assertEqual(c.add_lease(first), "already_recorded")
        with self.assertRaisesRegex(ValueError, "immutable_lease_conflict"):
            c.add_lease(replace(first, request_limit=4))
        with self.assertRaisesRegex(ValueError, "overlapping"):
            c.add_lease(replace(first, lease_id="overlap"))
        self.assertEqual(c.tick()["attempt_id"], 1)
        clock.jump(60); self.assertEqual(c.tick()["attempt_id"], 2)
        clock.jump(120); self.assertEqual(c.tick()["state"], "expired")
        second = replace(first, lease_id="second", start=clock.start + 120, end=clock.start + 240)
        c.add_lease(second)
        self.assertEqual(c.tick(mode="revised")["attempt_id"], 3)
        self.assertEqual(c.status()["attempts"], 3)
        rows = c.observations("2099-01-01T00:00:00Z")
        self.assertEqual({o["collector"]["lease_id"] for r in rows for o in r["observations"]}, {"first", "second"})

    def test_all_crash_boundaries_fence_every_future_claim_and_renewal(self):
        for point in ("after_claim", "after_spawn_intent", "after_capture", "before_store", "after_store", "after_complete"):
            with self.subTest(point=point):
                c, p, clock = self.make(); first = self.lease(clock); c.add_lease(first)
                def crash(stage):
                    if stage == point:
                        raise SystemExit("synthetic boundary")
                with self.assertRaises(SystemExit):
                    c.tick(_fault=crash)
                c.close(); c = self.reopen(p, clock)
                clock.jump(60)
                result = c.tick(_popen=lambda *a, **k: self.fail("spawn after unresolved outcome"))
                self.assertEqual(result["state"], "fenced")
                self.assertEqual(result["attempts"], 1)
                self.assertEqual(c.observations("2099-01-01T00:00:00Z"), [])
                with self.assertRaisesRegex(ValueError, "cannot_clear_gate"):
                    c.add_lease(replace(first, lease_id="second", start=first.end, end=first.end + 120))

    def test_global_fence_is_rechecked_after_claim_and_before_spawn(self):
        for point in ("after_claim", "after_spawn_intent"):
            with self.subTest(point=point):
                c, _, clock = self.make(); c.add_lease(self.lease(clock))
                def revoke(stage):
                    if stage == point:
                        c.revoke()
                result = c.tick(_fault=revoke, _popen=lambda *a, **k: self.fail("spawn after revocation"))
                self.assertEqual(result["state"], "operator_revoked")
                self.assertEqual(result["attempts"], 1)

    def test_earlier_confirmed_observations_survive_later_uncertain_store(self):
        c, _, clock = self.make(); c.add_lease(self.lease(clock))
        c.tick(); clock.jump(60)
        def crash(stage):
            if stage == "after_store":
                raise OSError("secret exception string")
        result = c.tick(mode="revised", _fault=crash)
        self.assertEqual(result["state"], "fenced")
        self.assertEqual(c.store.verify()["observations"], 2)
        visible = c.observations("2099-01-01T00:00:00Z")
        self.assertEqual(len(visible), 1)
        self.assertEqual(len(visible[0]["observations"]), 1)
        self.assertNotIn("secret exception", " ".join(r[0] for r in c.db.execute("SELECT payload FROM events")))
        clock.jump(120)
        self.assertEqual(c.tick()["attempts"], 2)

    def test_429_pauses_without_hiding_history_or_renewal_reset(self):
        c, p, clock = self.make(); first = self.lease(clock); c.add_lease(first)
        c.tick(); clock.jump(60)
        result = c.tick(mode="rate_limited")
        self.assertEqual(result["state"], "paused_source")
        c.close(); c = self.reopen(p, clock)
        self.assertEqual(len(c.observations("2099-01-01T00:00:00Z")), 1)
        with self.assertRaisesRegex(ValueError, "cannot_clear_gate"):
            c.add_lease(replace(first, lease_id="new", start=first.end, end=first.end + 60))
        clock.jump(120); self.assertEqual(c.tick()["attempts"], 2)

    def test_local_shutdown_idle_and_inflight_does_not_revoke_lease(self):
        stop = threading.Event()
        c, p, clock = self.make(stop=stop); c.add_lease(self.lease(clock))
        stop.set(); self.assertEqual(c.tick()["action"], "local_shutdown")
        self.assertEqual(c.status()["attempts"], 0)
        stop.clear()
        def spawn(*a, **k):
            child = subprocess.Popen(*a, **k); stop.set(); return child
        result = c.tick(mode="hang", _popen=spawn)
        self.assertEqual(result["action"], "cancelled")
        self.assertIsNone(result["reason"])
        c.close(); c = self.reopen(p, clock)
        self.assertEqual(c.tick()["action"], "no_claim")
        clock.jump(60)
        self.assertEqual(c.tick()["action"], "confirmed")
        self.assertEqual(c.status()["attempts"], 2)

    def test_timeout_later_slot_only_if_explicitly_permitted(self):
        for allowed in (False, True):
            with self.subTest(allowed=allowed):
                c, _, clock = self.make()
                c.add_lease(self.lease(clock, allow_transient_next_slot=allowed))
                result = c.tick(mode="hang", timeout=.1)
                self.assertEqual(result["attempts"], 1)
                self.assertEqual(c.tick()["attempts"], 1)
                clock.jump(60)
                result = c.tick()
                self.assertEqual(result["attempts"], 2 if allowed else 1)
                self.assertEqual(result["state"], "waiting_next_slot" if allowed else "paused_source")

    def test_5xx_and_byte_budget_independent_of_request_cap(self):
        c, _, clock = self.make()
        c.add_lease(self.lease(clock, byte_limit=2 * LIMIT, allow_transient_next_slot=True))
        self.assertEqual(c.tick(mode="server_error")["action"], "failed")
        self.assertEqual(c.tick()["attempts"], 1)
        clock.jump(60)
        self.assertEqual(c.tick()["action"], "confirmed")
        clock.jump(120)
        result = c.tick()
        self.assertEqual(result["state"], "quota_exhausted")
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["requests_remaining"], 1)
        self.assertEqual(result["body_budget_remaining"], 0)

    def test_confirmation_cutoff_not_receipt_or_normalization_time(self):
        c, _, clock = self.make(); c.add_lease(self.lease(clock)); c.tick()
        p = c._payloads(1)
        confirmed = p["CONFIRMED"]["confirmed_at"]
        earlier = (datetime.fromisoformat(confirmed) - timedelta(microseconds=1)).isoformat()
        self.assertEqual(c.observations(p["CAPTURED"]["capture"]["received_at"]), [])
        self.assertEqual(c.observations(p["NORMALIZING"]["normalized_at"]), [])
        self.assertEqual(c.observations(earlier), [])
        self.assertEqual(len(c.observations(confirmed)), 1)

    def test_no_catchup_and_expiry_with_remaining_quota(self):
        c, _, clock = self.make(); c.add_lease(self.lease(clock, end=clock.start + 180))
        clock.jump(10); self.assertEqual(c.tick()["attempts"], 0)
        self.assertEqual(c.db.execute("SELECT COUNT(*) FROM missed").fetchone()[0], 1)
        clock.jump(60); self.assertEqual(c.tick()["attempts"], 1)
        clock.jump(180); self.assertEqual(c.tick()["state"], "expired")
        self.assertEqual(c.status()["attempts"], 1)

    def test_unknown_spawn_and_bad_fixtures_never_normalize(self):
        c, _, clock = self.make(); c.add_lease(self.lease(clock))
        def unknown(*a, **k):
            raise OSError("not persisted")
        self.assertEqual(c.tick(_popen=unknown)["state"], "fenced")
        for mode in ("denied", "redirect", "empty", "oversize", "malformed", "overflow", "compressed", "bad_length", "invalid_html", "secret_header"):
            with self.subTest(mode=mode):
                c, _, clock = self.make(); c.add_lease(self.lease(clock))
                result = c.tick(mode=mode)
                self.assertIn(result["state"], ("paused_source", "fenced"))
                self.assertEqual(c.store.verify()["observations"], 0)

    def test_root_identity_and_concurrent_owner(self):
        c, p, clock = self.make()
        with self.assertRaises(BlockingIOError):
            self.reopen(p, clock)
        c.close()
        (p / "ledger.sqlite").unlink()
        with self.assertRaises(FileNotFoundError):
            self.reopen(p, clock)
        self.assertFalse((p / "ledger.sqlite").exists())

    def test_low_space_plus_failed_gate_write_still_fences_restart(self):
        c, p, clock = self.make(); first = self.lease(clock); c.add_lease(first)
        with patch.object(c.root, "capacity", side_effect=ValueError("storage_reserve")):
            with patch.object(c, "_set_gate", side_effect=OSError("simulated disk cannot append fence")):
                with self.assertRaises(OSError):
                    c.tick()
        self.assertTrue(c._admission_pending())
        c.close(); c = self.reopen(p, clock)
        self.assertEqual(c.tick(_popen=lambda *a, **k: self.fail("resumed uncertain admission"))["state"], "fenced")
        with self.assertRaisesRegex(ValueError, "cannot_clear_gate"):
            c.add_lease(replace(first, lease_id="new", start=first.end, end=first.end + 100))

    def test_idle_loop_does_not_grow_ledger(self):
        c, _, clock = self.make(); c.add_lease(self.lease(clock)); c.tick()
        rows = c.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        for _ in range(10):
            self.assertEqual(c.tick()["action"], "no_claim")
        self.assertEqual(c.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], rows)

    def test_clock_regression_on_already_consumed_or_outside_slot_still_fences(self):
        c, p, clock = self.make(); c.add_lease(self.lease(clock)); c.tick()
        clock.jump(-1)
        self.assertEqual(c.tick()["state"], "fenced")
        c.close(); clock.jump(60); c = self.reopen(p, clock)
        self.assertEqual(c.tick(_popen=lambda *a, **k: self.fail("clock fence lost"))["attempts"], 1)

    def test_reusable_loop_issues_successive_slots_then_idles_without_reset(self):
        # Run in this owning thread; another thread only requests local shutdown.
        c, _, clock = self.make(); c.add_lease(self.lease(clock, cadence=1, request_limit=2, byte_limit=2 * LIMIT))
        timer = threading.Timer(1.7, c.stop_event.set)
        timer.start()
        try:
            result = c.serve(max_wait=.05)
        finally:
            timer.join()
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["confirmed_attempts"], 2)
        self.assertEqual(result["state"], "quota_exhausted")


if __name__ == "__main__":
    unittest.main()
