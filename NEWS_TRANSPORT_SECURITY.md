# News polling transport hardening (local proposal only)

## Baseline and scope

This isolated worktree was initialized from the tracked HEAD tree of
`overnight-integration`, whose commit 153b6efd65d482ada4e3d39278bfd2e353fee52e
is labeled as the verified exact source snapshot of deployed revision
9c12e0497df2f9a1137f9638889b58d9262e5891. The original remote commit object is
not available in that local repository; provenance relies on the previously
verified snapshot. Only monitor.py, its new offline tests, and this note change.
No publication, deployment, broker/provider requests, or real credentials were
used. Live source and unrelated modules remain untouched.

## Finding and risk conditions

The previous monitor used default urllib redirects and unbounded json.load.
An offline fixture demonstrated APCA credential headers retained on a generated
cross-host redirect request. Exploitation requires a redirect response from the
requested endpoint or equivalent influence over that response; this is NOT
proof of an actual redirect, credential disclosure, or account compromise.
Unbounded response reads also allowed a sufficiently large response to consume
excess memory. No live endpoint was tested.

## Changes

- Fix news requests to the existing exact HTTPS news endpoint, with encoded,
  validated query values. No new destination, route, or provider.
- Disable environment proxies and all followed redirects, including same-host
  and HTTPS-to-HTTP redirects. This intentionally fails closed if the provider
  changes its route or the environment requires a proxy.
- Keep the existing ten-second urllib timeout, 50-item limit, ten-page cap,
  pagination, symbol filtering, ingest contract and source-health reporting.
- Stream at most 2 MiB plus one overflow sentinel byte. Reject oversized bodies
  regardless of Content-Length. Decode UTF-8 explicitly; reject malformed JSON,
  non-finite constants, missing/non-list news fields, and invalid page tokens.
- Require exact final response URL and HTTP 200. Do not read provider error
  bodies. Close HTTP errors and responses; emit only a generic transport error.
  The main loop retains its sanitized source_error_no_signal/error heartbeat.

A legitimate response over 2 MiB now fails instead of ingesting unbounded data.
The timeout is urllib's existing socket-operation timeout, not a new total
wall-clock deadline. Existing pages already ingested before a later page error
remain ingested; the source is not marked healthy after the failing page.
This is transport/schema-envelope hardening, not full news-item validation.

## Offline validation

PYTHONPATH=/tmp/paper-adapter-deps python -m unittest discover -v

261 tests passed (249 baseline and 12 new transport test methods). Redirect
coverage includes 301, 302, 303, 307 and 308 to cross-host HTTPS, same-host HTTP
and same-host alternate paths using real urllib redirect processing with a
fixture transport only. Additional cases cover missing Content-Length,
partial/chunked reads, size boundary/overflow, invalid UTF-8/JSON/schema,
status/URL mismatch, timeouts, sanitized errors, proxy disabling, query
validation, pagination, ingestion and main-loop error heartbeat.

python -m py_compile monitor.py test_monitor_transport.py

Changed Python files compile successfully. Whole-tree compileall is NOT clean:
pre-existing app.py has an unterminated triple-quoted string at lines 185–198.
Its blob 4df9595a5d69f1cd18938a7e7cb247b7ac6153ff matches the baseline exactly;
this unrelated legacy-file defect was not modified.

Independent offline reviewer checked transport behavior and identified missing
news-envelope validation, which was tightened with regression cases. No actual
provider compatibility or production runtime claim is made. User approval is
required before publishing, merging or deploying this proposal.
