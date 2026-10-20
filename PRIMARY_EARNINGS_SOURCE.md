# Original earnings evidence: NVDA-only local boundary

Status: implemented locally, not deployed or wired to startup. No credentials,
signing keys, paid APIs, model calls, broker calls, or orders. This is a source
integrity adapter, not strategy validation or a claim of trading readiness.

## Deliberately bounded scope

- Issuer: NVIDIA, NVDA, CIK 1045810. No other issuer is accepted.
- Discovery and publisher timestamp: the original NVIDIA Newsroom RSS at
  https://nvidianews.nvidia.com/rss.xml . Only exact earnings-release titles,
  matching exact canonical release URLs, matching GUID, releases content type,
  and the Press Releases category qualify.
- Original release: the linked HTML at the same pinned HTTPS origin. The URL
  grammar permits only NVIDIA financial-results quarterly release routes.
  The parser requires one canonical URL, one matching article title, and one
  original article body identifying NVIDIA (NASDAQ: NVDA), the reporting quarter
  and an unambiguous fiscal-period end date. Fiscal episode comes from the
  quarter/year, not from a caller-supplied event ID.
- The RSS publisher pubDate must explicitly include seconds and GMT. Its weekday
  must agree with the date. HTML publication dates, HTTP Date, HTTP Last-Modified,
  SEC submissions metadata, search-result dates and model analysis are not used
  as replacement publication timestamps.

The original Apple release investigated during source selection had date-only
publication metadata. It was not promoted by inventing a time. NVIDIA original
HTML also displays a date; the matching original RSS item is mandatory for its
seconds-precision publication claim. This adapter will abstain if that item is
absent or malformed.

## Contract and exact data lineage

PrimaryEarningsSource(database, transport=..., now_ns=...) supplies the existing
runner protocol:

- collect(): explicit, bounded collection; returns CollectionResult with stable
  reason, event_count, request_count. No scheduler is started.
- events(now, limit): returns existing active_demo_pipeline.Event objects only
  while the last whole collection is healthy and recent.
- event_eligible(event): validates every Event field against its durable exact
  record, checks current source health and the permanent episode quarantine.

NvidiaReadOnlyHTTP(enabled=False) is the real transport, disabled by default.
Explicitly enabling it permits GET only for the pinned RSS/release URLs. It
follows no redirect, requires HTTPS/TLS validation, bounds response length to
1 MiB, checks content type and encoding, and refuses partial content. Standard
trusted host proxy settings are honored; no proxy/credential configuration is
created. The source performs at most five GETs per collection, processes at most
100 feed items/four earnings releases, enforces a durable minimum 15 seconds
between collections, and refuses batches lasting over 20 seconds. Its four-second
socket timeout is an inactivity timeout, not a complete DNS/request watchdog.
Deployment still needs an external process deadline.

SQLite retains:

1. Exact raw RSS and original HTML bytes in content-addressed snapshot rows.
2. Each successful complete response's URL, raw hash and local receipt time in
   integer nanoseconds. This sample is taken after get() returns, not from a
   publisher header or caller-provided timestamp.
3. Parser version, source item extraction, fiscal episode and period end,
   original/feed hashes, receipt IDs, final availability sample, and parent
   lineage hash. Whole-feed changes unrelated to the selected item do not create
   a new earnings event; the unchanged original/selected-item identity dedups.
4. Exact Event payload, event ID, episode ID, original HTML source_hash, and
   provenance-derived version.

Availability is sampled after raw receipts are durably retained, parsing is
complete, and the final SQLite write lock is acquired. The lineage/event/health
records commit atomically after that sample. It is a local observation clock,
not a publisher attestation or a trusted timestamp service. Observed time rounds
up to the Event's microsecond representation, never backwards. Every sampled
clock must be monotonic; persistent last-clock checks cover reconstruction.

There is no HMAC and no new secret. Unlike trusted_event_boundary.py's fixture
receipt, provenance here begins with fetched original source bytes. The trust
root is the trusted HTTPS transport, parser, collector clock, application-owned
database and operating system. This does not constrain malicious code with
process/database access or independently prove the publisher's claimed time.
Injected transport/clock objects are explicitly trusted test/application code.

## Fail-closed rules

- First availability over 120 seconds after publication records late evidence,
  not an eligible event. Historical fetches cannot manufacture historical
  first-seen data. Returning old bytes later does not reopen a late episode.
- A modified timestamp after publication, an explicit correction/restatement,
  changed original HTML bytes, changed extracted period/publication metadata,
  or a changed identity for an existing fiscal episode permanently quarantines
  that episode. Recognized revocations are committed as soon as their original
  RSS/HTML receipt is available, even if later fetching/parsing or another batch
  item fails. A first-seen corrected RSS item creates a permanent tombstone even
  if the HTML cannot be fetched. Reverting to old bytes does not clear it. A modification before
  publication is invalid rather than silently treated as an original version.
- This is intentionally conservative: CMS edits, page chrome changes and
  non-substantive corrections can cause abstention. No heuristic distinguishes
  economically material from harmless edits, and no gate is relaxed to produce
  a trade. Initial publication with modDate later than pubDate also abstains.
- XML DTD/entities, non-UTF-8/NUL payloads, duplicate relevant feed fields/items,
  ambiguous HTML identities or captured tag structure, and earnings embedded
  in scripts/styles/templates are rejected.
- Failed or incomplete batches invalidate source health. A previously accepted
  active release disappearing from the RSS feed also blocks delivery. Failed
  parsing of an unidentifiable change cannot establish a correction lineage;
  source health remains unavailable until a complete valid collection succeeds.
- Health expires after 30 seconds; collection in progress disables delivery.
  An interrupted collection leaves a durable busy marker and cannot silently
  restart. Events older than seven minutes are not delivered. This is an outer
  evidence horizon, NOT a relaxed strategy entry window.

## Runner integration and remaining deployment path

The NativeDecimalExecutionPort event_ready guard must remain before the runner's
permanent claim, so a release observed immediately is not consumed/abstained
before its five-minute bar endpoint. The legacy PipelineExecutionPort lacks this
readiness hook and is not the continuous source integration target. The existing
five-minute rule, its one-minute decision window, regular-session restrictions,
quotes, bars, account/reconciliation, reservations and exits remain untouched.

Next steps, not performed by this change:

1. Independently review and publish the exact source files, then provision a
   separate durable source ledger and detect missing/replaced/corrupt state.
   Existing demo/probe ledgers and one-shot probe latches must not be reset.
2. Add an explicitly disabled-by-default supervised source polling hook, using
   this real transport and the existing storage. No new source API key is needed.
   Serialize collect(), input gathering, source eligibility rechecks and decision
   processing, with an external timeout and verified stop behavior. A correction
   arriving between independent database checks remains a race without such
   serialized orchestration; there is no cross-ledger atomic order transaction.
3. Run observation-only in the deployed environment and verify real current
   RSS/HTML acquisition, clock/storage health and provenance. Wait for an actual
   fresh matching release; no archived release can substitute for this test.
4. Connect the source to the reviewed native Decimal runner only after all other
   deployment gates pass. Real native market inputs/regular-session observations,
   original reserved-order state, guarded execution/exit and lifecycle readiness
   remain separate requirements. A source event is never itself order permission.

No startup module, Render setting, external endpoint, execution module, existing
trusted-boundary fixture, or strategy threshold was changed for this component.

## Verification and public sample

Run the two primary-source test modules plus the existing runner suites with
network calls prohibited. Synthetic accepted source fixtures are clearly labeled
and separate from real public evidence. The independent suite checks corrections
at the market-input boundary and preservation of the five-minute readiness gate.

The separate public sample report records actual retrieval time, byte hash,
request outcome and zero event/order counts. Its current RSS observation has no
matching earnings release; archived Q2 FY2027 HTML is only a raw source sample,
not evidence of its original availability and not an accepted earnings event.
Failed public retrieval attempts are retained separately, not presented as
successful transport validation. Unit-test success does not establish that a
fresh real earnings event has traversed this boundary in a deployed environment.
