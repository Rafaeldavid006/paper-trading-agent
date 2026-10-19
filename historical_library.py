"""Bounded, read-only historical evidence snapshot. No integrations or network IO."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

DATA_DIR = Path(__file__).resolve().parent / 'history_data'
MAX_BYTES = 256 * 1024
CASE_LABELS = {
    'iphone_announcement_2007': ('הכרזת iPhone', 'טכנולוגיה'),
    'aig_facility_authorization_2008': ('אישור מסגרת האשראי ל־AIG', 'משבר פיננסי'),
    'iea_emergency_stock_agreement_2022': ('הסכם שחרור מלאי הנפט של IEA', 'אנרגיה'),
}
SOURCE_IDS = {'us_original_macro', 'world_bank', 'bis', 'ucdp', 'gdelt',
              'sec_company', 'federal_register', 'eia'}

class SnapshotUnavailable(ValueError):
    """Snapshot unavailable, invalid, or changed without review."""


def _read(name):
    # Filenames are code-owned constants; no request input reaches the filesystem.
    with (DATA_DIR / name).open('rb') as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise SnapshotUnavailable('snapshot too large')
    return raw


def _validate_tree(value, depth=0):
    if depth > 14:
        raise SnapshotUnavailable('snapshot too deep')
    if isinstance(value, dict):
        for key, item in value.items():
            if key in ('official_url', 'official_urls'):
                for url in item if isinstance(item, list) else [item]:
                    if not isinstance(url, str) or any(c.isspace() for c in url):
                        raise SnapshotUnavailable('invalid source URL')
                    parsed = urlsplit(url)
                    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username
                            or parsed.password or parsed.port not in (None, 443)):
                        raise SnapshotUnavailable('unsafe source URL')
            _validate_tree(item, depth + 1)
    elif isinstance(value, list):
        if len(value) > 100:
            raise SnapshotUnavailable('too many records')
        for item in value:
            _validate_tree(item, depth + 1)
    elif isinstance(value, str) and len(value) > 12000:
        raise SnapshotUnavailable('text too long')


def load_snapshot():
    """Return reviewed snapshots only; a failed library must not break other pages."""
    try:
        documents = []
        for filename, digest in SNAPSHOT_DIGESTS.items():
            raw = _read(filename)
            if hashlib.sha256(raw).hexdigest() != digest:
                raise SnapshotUnavailable('snapshot differs from reviewed data')
            data = json.loads(raw)
            _validate_tree(data)
            documents.append(data)
        catalog, pilot = documents
        if ({s['id'] for s in catalog['sources']} != SOURCE_IDS or
                {c['case_id'] for c in pilot['cases']} != set(CASE_LABELS) or
                catalog['total_ingested_records'] != 0 or
                {s['id'] for s in catalog['review_blocks']} != {'fred_alfred', 'imf'}):
            raise SnapshotUnavailable('invalid catalog')
        for case in pilot['cases']:
            if (case['eligibility']['point_in_time_backtest_eligible'] is not False or
                    case['outcomes'] is not None or case['market_data'] is not None):
                raise SnapshotUnavailable('unsupported outcomes or eligibility')
        return catalog, pilot
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as exc:
        raise SnapshotUnavailable('Historical snapshot unavailable') from exc


def select_cases(pilot, query, domain):
    return [case for case in pilot['cases']
            if (not domain or CASE_LABELS[case['case_id']][1] == domain)
            and (not query or query.casefold() in
                 (json.dumps(case, ensure_ascii=False) + ' '.join(CASE_LABELS[case['case_id']])).casefold())]

# Code-pinned reviewed bytes: changes require another review.
SNAPSHOT_DIGESTS = {'global-history-catalog.json': 'b020b21b194e7af9e1fa383c1909d523fc90ea7251fcc2e2449ebf9291f2df6f', 'historical-case-samples.json': 'b2782eb737a565cd94e734b86f6e0b395b0f796993cea693de2dc7ec2ee72c93'}
