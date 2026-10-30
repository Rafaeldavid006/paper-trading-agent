"""Recurring synthetic captures, restart, renewal, expiry and source pause.

No network; same executable and two database files throughout. Deliberately
accelerated synthetic clocks/slots are not source publication/availability time.
"""
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import time
from recurring import LeaseCollector, Lease, LIMIT


class Clock:
    def __init__(self):
        self.start = int(time.time())
        self.jump(0)
    def jump(self, offset):
        self.offset, self.anchor = offset + .1, time.monotonic()
    def __call__(self):
        return datetime.fromtimestamp(self.start + self.offset + time.monotonic() - self.anchor, timezone.utc).isoformat(timespec="microseconds")


def main():
    clock = Clock()
    with tempfile.TemporaryDirectory() as t:
        root = Path(t) / "stable_collector"
        c = LeaseCollector(root, "recurring_demo", create=True, offline_test_enabled=True, wall=clock)
        lease = Lease("synthetic_lease_a", clock.start, clock.start + 180, 60, 3, 3 * LIMIT, "synthetic:approval_a")
        c.add_lease(lease)
        inodes = {p.name: p.stat().st_ino for p in root.glob("*.sqlite")}
        actions = [c.tick()]
        clock.jump(60); actions.append(c.tick())
        c.close()
        c = LeaseCollector(root, "recurring_demo", offline_test_enabled=True, wall=clock)
        clock.jump(120); actions.append(c.tick(mode="revised"))
        clock.jump(180); expired = c.tick()
        renewal = replace(lease, lease_id="synthetic_lease_b", start=clock.start + 180,
                          end=clock.start + 300, request_limit=2, byte_limit=2 * LIMIT,
                          approval_ref="synthetic:approval_b")
        c.add_lease(renewal)
        actions.append(c.tick(mode="revised"))
        clock.jump(240); denied = c.tick(mode="rate_limited")
        visible = c.observations("2099-01-01T00:00:00Z")
        final = c.status()
        report = {"synthetic_only": True, "live_http_requests": 0,
                  "same_database_files": inodes == {p.name: p.stat().st_ino for p in root.glob("*.sqlite")},
                  "successful_attempt_ids": [a["attempt_id"] for a in actions],
                  "before_renewal_state": expired["state"], "later_denial_state": denied["state"],
                  "confirmed_observations_still_visible": sum(len(r["observations"]) for r in visible),
                  "distinct_versions": len(visible), "final": final}
        c.close()
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
