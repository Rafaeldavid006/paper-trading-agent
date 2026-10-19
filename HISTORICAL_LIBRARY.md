# Local historical research library

## Scope and status

This additive, authenticated `/history` page is a **local-only candidate**, not a deployed feature. It provides three curated, source-linked examples and a catalog of eight source families plus two review-blocked sources. It is not comprehensive world history, an ingested corpus, a market-return dataset, a causal study, a trading signal, or a point-in-time-valid backtest.

The page has Hebrew navigation, filtering and safety labels. Evidence details remain in the original source-catalog language to preserve exact facts, temporal qualifiers, access restrictions and rights caveats. The GET form searches the three samples by case text/entity/name and filters their domain. Source cards remain unfiltered and say so. Clear-filter navigation and an empty-results message are provided.

## Provenance

The two data snapshots are byte-identical copies of the reviewed inputs:

- `history_data/global-history-catalog.json`: 8 source families; FRED/ALFRED and IMF review blocks; zero ingested records.
- `history_data/historical-case-samples.json`: iPhone announcement (2007-01-09), AIG facility authorization (2008-09-16), and IEA stock-release agreement (2022-03-01).
- `history_data/snapshot-manifest.json`: SHA-256 file hashes and baseline metadata, for inspection only. The actual acceptance hashes are pinned in code, not loaded from this manifest.

Baseline source snapshot: remote commit `9c12e0497df2f9a1137f9638889b58d9262e5891`, represented locally by `153b6ef`. No unmerged PR4, SEC connector, replay, ledger or other worker changes were imported.

All original case fields are available under the evidence disclosure: event dates and day precision remain separate from publication metadata. AIG's publisher-stated 21:00 EDT release is represented as 2008-09-17T01:00:00Z, not assigned to the Board authorization itself. Current source pages are not authenticated historical snapshots. All three cases retain `point_in_time_backtest_eligible: false`, `outcomes: null` and `market_data: null`.

## Safety boundary

- Uses the existing app's password/session authentication and security headers. No new authentication system.
- GET/HEAD only at `/history`; no edits, uploads, API keys, user-chosen file paths or activation controls.
- Reads only two fixed, bounded local JSON files. Each is limited to 256 KiB and checked against a code-pinned SHA-256 before parsing; recursive shape and official HTTPS URL validation follow. Any read/parse/integrity failure produces a generic library-only 503, not partial invented data.
- Query limits: 2,048 raw bytes, 120 characters of search, 40 of domain; unknown/duplicate arguments and unknown domains rejected. No dynamic file or URL construction from request input.
- Autoescaped Jinja values and URLs, no `safe` markup. No JS, remote assets, model calls, collection, market accounts, paid APIs or network requests on page view. Clicking an explicitly displayed official source link navigates to that external site; it does not authorize ingestion.
- FRED/ALFRED and IMF remain rights/access-review blocked. No fetch or workaround. UCDP's token requirement and GDELT's discovery-not-ground-truth limitation are prominent. Full original rights/access statements remain visible in source disclosures.
- No outcome substitutions: missing is displayed as missing/not collected, never a zero return or no-effect claim.
- To update data, review new bytes and restrictions, update code hashes and manifest together, and rerun tests. Never remove hash checks to make a changed snapshot load.

## Verification

Run with installed project dependencies (`requirements.txt`):

    python -m unittest discover -q

In the isolated task workspace, dependencies already present at `/tmp/paper-adapter-deps` were used through PYTHONPATH. Baseline: **249 tests passed**. Candidate: **266 tests passed**, including 17 new tests for authentication/configuration, exact dates and precision, source categories and caveats, search/domain/empty results, escaping, argument bounds, mutating-method rejection, unavailable/corrupt/oversized/changed files, hashes/nulls, unsafe URL rejection, security headers, navigation, and no socket/broker/database mutation on GET.

HTML was rendered through Flask's test client. **Visual browser layout was not verified**; no claim of browser screenshot QA. No remote publish, push, merge, deploy, account operation or live integration was performed.

## Patch integration

New: module, history template, three data files, test module, this document.
Modified: `webapp.py` (one import and authenticated GET route), `templates/research.html` (one navigation link). No existing API or data-model behavior changes. Review any conflict in these shared files rather than replacing a later version wholesale. Run the complete receiving branch test suite after integration.
