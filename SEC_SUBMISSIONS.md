# SEC submissions metadata adapter (opt-in, research only)

This module is an additive, standard-library-only research data adapter. It does
not import trading modules, start a scheduler, discover a stock universe, fetch
quotes, extract earnings surprises, select investments, or submit orders. It is
not connected to application startup. SEC filings are evidence, not proof of a
predictive or profitable trading strategy.

## Use

Supply a real descriptive application/organization and contact email for the
SEC User-Agent. The adapter transmits that header to SEC. Do not use the fixture
email for live traffic. Choose the CIK list explicitly (1–25 entries):

```python
from sec_submissions import SecClient, SecIngestor

path = 'research-sec.sqlite'
client = SecClient(path, user_agent='Your Organization research contact@your-domain.com')
ingestor = SecIngestor(path, client, ['0000320193'])
results = ingestor.poll_once()  # One bounded read-only request per uncached CIK.
```

Inspect each result's `status`, not just an empty `versions` list. Network,
identity, malformed-array and oversized-response errors never mean “no filings.”
A failed issuer does not prevent attempting the other opted-in issuers. Source
health is stored under `sec:<CIK>` and preserves the previous successful time.
No source is polled until this method is called.

`poll_once()` stores metadata only. `project(version_id, symbol)` optionally
projects a **current** metadata revision into the existing `event_core.Store`.
The caller must independently verify the security symbol; issuer tickers are
not automatically mapped. Distinct share classes have distinct event source IDs.
Projection is not a trading signal. Its payload contains no `actual`, `metric`,
`period`, or `unit` required by the existing event evaluation model.

## Time and provenance contract

- `filingDate` remains the original date string. No midnight is invented.
- Original `acceptanceDateTime` is retained. `accepted_at` is normalized only for
  a valid timestamp with explicit UTC `Z` or numeric offset. Naive, missing,
  date-only and invalid acceptance values remain stored with `accepted_at=NULL`.
- Acceptance is **not verified public dissemination time**. The optional
  projection uses this timestamp in the legacy publication slot, explicitly
  tagging `timestamp_semantics=sec_acceptance_not_verified_public_dissemination`.
  Do not use that slot alone as a historical point-in-time availability boundary.
  Records without a valid offset-aware acceptance time cannot be projected.
- `fetched_at` is successful HTTP retrieval completion. Cache hits preserve it;
  a cache hit does not claim a new network observation. `observed_at` and each
  record's `first_seen_at` are local persistence observations. Historical
  availability must never be inferred earlier than these observed times merely
  because a filing has an old acceptance/filing date.
- `sec_snapshots` retains the full parsed JSON, official source URL, CIK,
  adapter version, retrieval and observation time. Canonical hashes identify
  snapshots and row revisions; they are not signatures or independently
  authenticated SEC document checksums.
- `sec_snapshot_records` links every snapshot to its row versions. `sec_records`
  stores accession, content hash, raw metadata, first observation, accepted time,
  document URL and amendment flag. Source document bodies are not downloaded.
- A changed row for the same CIK/accession creates an immutable new revision.
  Repeated identical observations do not overwrite the original. Conflicting
  duplicate accessions within one snapshot fail validation rather than guess
  which is current. Observation insertion order is the local revision order
  even when the wall clock is equal; it is not a claimed SEC correction time.
- Only the latest observed revision can be projected. Older projections,
  including previously projected versions, are explicitly rejected as stale.
  Same-clock, out-of-order projection cannot resurrect an old revision.
  The event `updated_at` slot is the honest local `first_seen_at` observation,
  tagged `updated_at_semantics=local_metadata_observation_not_sec_correction_time`,
  not an invented SEC correction time. Regressed acceptance times therefore do
  not reverse revision order. A regressed local observation clock blocks projection.
- `/A` filings are separate accessions. `amends_accession` is deliberately null:
  this snapshot does not establish an unambiguous parent amendment relation.
- A filing disappearing from the bounded recent snapshot is not a withdrawal
  and never deletes previously captured data.

## Bounds and fair access

Default cache TTL is 300 seconds, maximum 24 hours. A client serializes network
requests with at least one second between attempts, including retries. Three
attempts default, five maximum. HTTP 429/500/502/503/504 and connection failures
can retry with exponential backoff. `Retry-After` seconds or an HTTP date are
honored; a server delay over 120 seconds fails the run rather than retrying early.
403 and redirects are not retried. Redirects are refused, not followed to an
unverified origin. No API key is needed.

Requests have a 20-second timeout, an 8 MiB decompressed/identity-body cap and
10,000-row cap. Oversized data and inconsistent parallel arrays fail as a whole:
there is no silent truncation. Required columns include accession, form, filing
date and primary document relative path. Safe subdirectories are retained;
absolute paths, traversal, empty segments, backslashes and control characters
are rejected. Strict validation may reject an issuer whose source
schema differs; this is reported, not silently repaired. UTF-8/JSON/schema errors
are not retried. Invalid cached payloads and timestamps trigger a fresh request;
no stale cache is substituted after a failed request.

**Operational limit:** use one shared client and one ingestion/projection writer
for a given database. Do not run multiple adapter processes or concurrently
record/project with independent ingestors. The per-client limiter does not
coordinate other applications, hosts or processes, and revision projection is
not a cross-process transaction. Aggregate SEC traffic must be coordinated by
the operator. SQLite has normal transactional durability, but archive growth,
backup, retention and recovery policy are the operator's responsibility. There
is no automatic deletion, retention job or schema migration system.

## Coverage and known limitations

The official recent snapshot is not complete history. Results always report
`coverage=recent_only`, and `older_files_available` indicates historical-file
metadata. Historical files are not recursively fetched; long outages and initial
backfill can miss observations. No polling schedule, archive catch-up, deletion
interpretation, document-text parsing, XBRL calculations, ticker-master history,
amendment-parent inference, event classification or price model is included.
SEC lag, upstream corrections, local clock error and network outages remain
possible. This is not a low-latency feed or a complete historical backtest source.
The source row's corrected metadata may be detected without an independently
known correction-publication timestamp.

## Validation and source basis

All adapter tests use synthetic fixtures and injected network responses. No live
SEC requests were issued with a fabricated contact identity, and no broker calls
are made. Run `python -m unittest test_sec_submissions -v`; run the whole checkout
with `python -m unittest discover -v` (the source materialization places tests in
the repository root rather than the README's historical `tests` directory).

Official documentation read on 2026-10-18:

- [EDGAR API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces):
  unauthenticated JSON submissions endpoint, 10-digit CIKs, recent compact arrays
  and extra historical files. APIs update as submissions disseminate, but this
  does not certify an individual acceptance field as dissemination time.
- [SEC Developer Resources](https://www.sec.gov/about/developer-resources):
  fair-access guideline of at most 10 requests/second **per user across machines**,
  efficient downloads and classified automated clients. This adapter targets a
  more conservative one request/second per shared client.

The fixture is invented and is not a real filing or investment recommendation.
