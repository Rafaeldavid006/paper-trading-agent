# Bounded separate ECB collector

Inactive by default: no `approved_release.json` is shipped. The owner exits
without storage or source access when the manifest is absent.

The collector runs as a separate no-secret process on the existing Linux service,
using only `/var/data/research-collector-ecb-v1` and its two sibling enrollment
markers. It does not execute trading orders or invoke model APIs. Process/state
separation is not independent infrastructure, resource reservation, or a security
boundary against a malicious administrator.

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
status/receipt logs, stable persistent identities across redeploy/restart, first
confirmed budgeted capture, next daily capture in the same ledger, and restart
without quota reset or replay. Green web health alone proves none of these.

## Data interpretation

Source: ECB statistics. Preserve original raw responses and metadata; normalized
views are derived. Reference dates are not verified publication times. Local
capture timestamps are not historical availability proof. Records remain
ineligible for backtests and carry `order_permission="none"`. Weekend captures
may confirm unchanged reference data. ECB reference rates are informational, not
executable transaction quotes.
