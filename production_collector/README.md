# Bounded separate ECB collector

Inactive by default: no `approved_release.json` is shipped. The owner exits
without storage or source access when the manifest is absent.

The collector runs as a separate no-secret process on the existing Linux service,
using only `/var/data/research-collector-ecb-v1` and its two sibling enrollment
markers. It does not execute trading orders or invoke model APIs. Process/state
separation is not independent infrastructure, resource reservation, or a security
boundary against a malicious administrator.

This prospective inactive payload restores fixed owner supervision but ships no
activation manifest. Its owner therefore exits disabled before storage or source
access. The read-only survey remains available for separate review; it is not
automatically run and is never an enrollment fallback. Supplying enrollment or
collect deployment grants remains a separately reviewed action.

## Storage contract

Fresh enrollment uses storage schema 2 and a versioned evidence-store adapter;
there is no automatic migration, reset, repair or adoption of prior storage.
The fixed Render mount profile accepts only the reviewed non-owned `02775`,
matching-runtime-group, read-write ext4 mount. This relies on cooperating code
within one service, not protection from hostile or concurrent manual writers.
Render restricts a disk to one service instance, but authorized SSH sessions can
still write inside it. The shared mount is never chmodded or chowned. Collector
directories remain runtime-owned `0700` or inherited `02700`; regular files are
private, single-link and nofollow checked.

An immutable enrollment receipt binds the original manifest, code/profile,
nonce and file cohort. A fresh process can accept only a coherent device-number
remap preserving the original inodes, metadata and markers across the complete
cohort. Original records are retained, not rewritten to match the new device.
During a process lifetime, full current physical identities are pinned and any
observed replacement refuses without repinning. Reopens use explicit non-creating
SQLite modes; only approved fresh enrollment can create database files. The
evidence adapter owns its new schema and cohort table while preserving the
vendored record APIs and checks.

Descriptor checks do not turn pathname-based SQLite/WAL access into an atomic
hostile-filesystem sandbox. A coherent whole-cohort restore can replay prior
quota state; detecting that requires an external monotonic authority not provided
here. Such restoration requires separate reconciliation, never automatic resume.

## Qualification

Run the package suite from this directory with `python run_checks.py`.
The fail-closed build gate is `python production_collector/build_gate.py` from
the repository root. It requires exact Python 3.14.8 and runs the reviewed offline
package tests in a temporary copy without release data. Audit hooks are not an
OS-enforced network sandbox. Full service dependency/regression/lifecycle checks
are separate required qualification; a package pass alone is not activation.
Root-level `test_collector_supervisor.py` covers the actual lifecycle integration,
including a bounded final pipe drain preserving terminal enrollment receipts.

## Reviewed two-phase deployment

1. Supply reviewed bounded `phase="enroll"` data with enrollment approval and a
   finite window. This qualifies the existing mount, creates isolated storage,
   closes/reopens and verifies it, emits a completion receipt hash, then exits
   without source access. Partial state is preserved and blocks automatic retry.
2. After reviewing that receipt and runtime evidence, change only deployment data
   to `phase="collect"`, set enrollment approval false, and pin the exact receipt.
   Collection reopens existing state only; it never bootstraps or resets counters.

Keep all package Python files byte-identical across phases and renewals. Approved
renewals append finite leases while preserving the exact historical prefix and
same ledger, receipt, quotas, and evidence. There is no automatic renewal.

The fixed source is ECB daily reference XML over verified HTTPS, without redirects
or proxies. An initial lease is at most seven days/seven daily attempts at 18:00
UTC, with a six-hour current-slot eligibility window, no immediate retry and no
old-slot catch-up. Each attempt reserves 64 KiB response budget before launch.
Lifetime bounds are 128 attempts, 8 MiB reserved bodies and 32 MiB root storage;
response caps are not billing or network-wire caps. Failed/uncertain captures
pause or fence instead of replaying.

Actual runtime acceptance requires the correct commit/runtime, readable sanitized
status/receipt logs, verified cohort continuity across redeploy/restart, first
confirmed budgeted capture, next daily capture in the same ledger, and restart
without quota reset or replay. Green web health alone proves none of these.

## Data interpretation

Source: ECB statistics. Preserve original raw responses and metadata; normalized
views are derived. Reference dates are not verified publication times. Local
capture timestamps are not historical availability proof. Records remain
ineligible for backtests and carry `order_permission="none"`. Weekend captures
may confirm unchanged reference data. ECB reference rates are informational, not
executable transaction quotes.
