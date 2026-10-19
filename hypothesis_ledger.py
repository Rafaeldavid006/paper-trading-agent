"""Append-only, fixture-only hypothesis/outcome audit. No network or orders.

Times are supplied by event_core.Store.clock; use a trusted clock in any future
integration. Prospective means recorded before this ledger's explicit window,
NOT before the news event, and never establishes an edge or causal effect.
"""
import json
import re
from datetime import timedelta
from event_core import utc, iso, finite

SCHEMA_VERSION = 1
HORIZONS = (5, 15, 60)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def _text(value):
    return isinstance(value, str) and bool(value.strip())


class HypothesisLedger:
    """Uses Store.db, but never changes events, decisions or price_reactions.

    known_targets maps symbols to stable entity IDs. It is a caller-supplied,
    copied registry, not automatic entity resolution or evidence verification.
    """
    def __init__(self, store, known_targets):
        self.store = store
        self.targets = dict(known_targets)
        if not all(re.fullmatch(r'[A-Z][A-Z0-9.-]{0,11}', k) and _text(v)
                   for k, v in self.targets.items()):
            raise ValueError('invalid target registry')
        with store.db() as c:
            c.executescript('''
                CREATE TABLE IF NOT EXISTS hypothesis_entries(
                    id INTEGER PRIMARY KEY, entry_key TEXT UNIQUE NOT NULL,
                    root_id INTEGER, previous_id INTEGER UNIQUE,
                    recorded_at TEXT NOT NULL, request TEXT NOT NULL,
                    record TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS hypothesis_outcomes(
                    id INTEGER PRIMARY KEY, entry_key TEXT UNIQUE NOT NULL,
                    hypothesis_id INTEGER NOT NULL, recorded_at TEXT NOT NULL,
                    request TEXT NOT NULL, record TEXT NOT NULL);
            ''')
            for table in ('hypothesis_entries', 'hypothesis_outcomes'):
                for action in ('UPDATE', 'DELETE'):
                    c.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                        BEFORE {action} ON {table} BEGIN
                        SELECT RAISE(ABORT, 'append-only ledger'); END''')

    def _check_clock(self, c, now):
        latest = c.execute("SELECT MAX(recorded_at) FROM (SELECT recorded_at FROM hypothesis_entries UNION ALL SELECT recorded_at FROM hypothesis_outcomes)").fetchone()[0]
        if latest and now < utc(latest):
            raise ValueError('ledger clock moved backwards')

    def _superseded(self, c, version, now):
        return bool(c.execute('''SELECT 1 FROM events newer JOIN events old
            ON newer.source=old.source AND newer.source_id=old.source_id
            WHERE old.version_id=? AND newer.version_id!=old.version_id
            AND newer.received_at<=? AND (newer.updated_at>old.updated_at OR
            (newer.updated_at=old.updated_at AND newer.rowid>old.rowid))''',
            (version, iso(now))).fetchone())

    def _existing(self, c, table, key, request):
        old = c.execute(f'SELECT id,request,record FROM {table} WHERE entry_key=?', (key,)).fetchone()
        if old:
            if old['request'] != request:
                raise ValueError('idempotency key reused with different content')
            return dict(json.loads(old['record']), id=old['id'])

    def record(self, *, key, event_version, symbol, entity_id, relation,
               evidence, direction, horizon_minutes, window_start, benchmark_symbol,
               knowledge_cutoff, invalidation, strategy_version,
               disposition='hypothesis', reason='', previous_id=None):
        """Append a hypothesis, abstention, rejection or explicit revision.

        Syntactically invalid input raises without insertion; semantically unsafe
        hypotheses are retained as rejected. Retrying identical keys is idempotent.
        Citation fields: reference, excerpt, published_at, known_at. Evidence is
        unverified caller input. Reference may identify a relationship record.
        """
        now = utc(self.store.clock())
        start, cutoff = utc(window_start), utc(knowledge_cutoff)
        if not _text(key) or not _text(strategy_version) or not _text(invalidation):
            raise ValueError('key, strategy version and invalidation required')
        if disposition not in ('hypothesis', 'abstained', 'rejected'):
            raise ValueError('invalid disposition')
        if direction not in ('up', 'down', 'flat') or relation not in ('direct', 'indirect'):
            raise ValueError('invalid direction or relationship tag')
        if type(horizon_minutes) is not int or horizon_minutes not in HORIZONS:
            raise ValueError('horizon must be 5, 15 or 60 minutes')
        if disposition != 'hypothesis' and not _text(reason):
            raise ValueError('abstention/rejection reason required')
        if not isinstance(evidence, list):
            raise ValueError('evidence must be a list')
        citations = []
        for item in evidence:
            if not isinstance(item, dict) or not all(_text(item.get(k)) for k in ('reference', 'excerpt')):
                raise ValueError('citation reference and excerpt required')
            citations.append(dict(reference=item['reference'], excerpt=item['excerpt'],
                                  published_at=iso(item['published_at']), known_at=iso(item['known_at'])))
        request = _json(dict(event_version=event_version, symbol=symbol, entity_id=entity_id,
            relation=relation, evidence=citations, direction=direction, horizon_minutes=horizon_minutes,
            benchmark_symbol=benchmark_symbol,
            window_start=iso(start), knowledge_cutoff=iso(cutoff), invalidation=invalidation,
            strategy_version=strategy_version, disposition=disposition, reason=reason, previous_id=previous_id))
        with self.store.db() as c:
            c.execute('BEGIN IMMEDIATE')
            old = self._existing(c, 'hypothesis_entries', key, request)
            if old:
                return old
            self._check_clock(c, now)
            issues = []
            event = c.execute('SELECT rowid,* FROM events WHERE version_id=?', (event_version,)).fetchone()
            if not event:
                issues.append('unknown_event_version')
            else:
                if any(utc(event[k]) > cutoff for k in ('published_at', 'updated_at', 'received_at')):
                    issues.append('event_not_known_at_cutoff')
                if start < utc(event['published_at']):
                    issues.append('window_before_event')
                newer = c.execute('''SELECT 1 FROM events WHERE source=? AND source_id=?
                    AND version_id!=? AND received_at<=? AND
                    (updated_at>? OR (updated_at=? AND rowid>?))''',
                    (event['source'], event['source_id'], event_version, iso(now),
                     event['updated_at'], event['updated_at'], event['rowid'])).fetchone()
                if newer:
                    issues.append('superseded_event_version')
            if self.targets.get(symbol) != entity_id or symbol not in self.targets:
                issues.append('unknown_target')
            if benchmark_symbol not in self.targets or benchmark_symbol == symbol:
                issues.append('invalid_benchmark')
            if cutoff > now:
                issues.append('future_knowledge_cutoff')
            if not citations:
                issues.append('missing_evidence')
            for item in citations:
                if utc(item['published_at']) > utc(item['known_at']) or utc(item['known_at']) > cutoff:
                    issues.append('evidence_not_known_at_cutoff')
            previous = None
            if previous_id is not None:
                previous = c.execute('SELECT * FROM hypothesis_entries WHERE id=?', (previous_id,)).fetchone()
                if not previous:
                    raise ValueError('unknown previous hypothesis')
                if c.execute('SELECT 1 FROM hypothesis_entries WHERE previous_id=?', (previous_id,)).fetchone():
                    raise ValueError('only the latest revision may be revised')
                prior = json.loads(previous['request'])
                if any(prior[k] != json.loads(request)[k] for k in ('symbol', 'entity_id', 'window_start', 'horizon_minutes', 'benchmark_symbol')):
                    raise ValueError('revision must retain target and observation window')
            record = dict(json.loads(request), schema_version=SCHEMA_VERSION,
                recorded_at=iso(now), timing='prospective' if now <= start else 'retrospective',
                status='rejected' if issues else disposition, issues=sorted(set(issues)),
                evidence_verified=False, order_permission='none', causal_effect_proven=False)
            root = (previous['root_id'] or previous['id']) if previous else None
            cursor = c.execute('''INSERT INTO hypothesis_entries
                (entry_key,root_id,previous_id,recorded_at,request,record) VALUES (?,?,?,?,?,?)''',
                (key, root, previous_id, iso(now), request, _json(record)))
            return dict(record, id=cursor.lastrowid)

    def observe(self, *, key, hypothesis_id, benchmark_symbol, samples,
                coverage_complete, source, adjustment='raw', max_staleness_seconds=0):
        """Append fixture observations. Never fetches or overwrites old captures.

        samples maps symbol -> {'baseline': point, 'terminal': point}; a point
        contains price, at, known_at. Exact-window comparison is required even
        if a stale endpoint is within the allowed tolerance. Missing samples
        must be represented by absent/None entries, never invented prices.
        """
        now = utc(self.store.clock())
        if not _text(key) or not _text(source) or adjustment not in ('raw', 'split', 'all'):
            raise ValueError('key, source and valid adjustment required')
        if type(coverage_complete) is not bool or type(max_staleness_seconds) is not int or not 0 <= max_staleness_seconds <= 120:
            raise ValueError('explicit coverage flag and bounded staleness required')
        if not isinstance(samples, dict):
            raise ValueError('samples must be a mapping')
        # JSON snapshot protects against caller mutation and excludes NaN/Infinity.
        request = _json(dict(hypothesis_id=hypothesis_id, benchmark_symbol=benchmark_symbol,
            samples=samples, coverage_complete=coverage_complete, source=source,
            adjustment=adjustment, max_staleness_seconds=max_staleness_seconds))
        with self.store.db() as c:
            c.execute('BEGIN IMMEDIATE')
            old = self._existing(c, 'hypothesis_outcomes', key, request)
            if old:
                return old
            self._check_clock(c, now)
            row = c.execute('SELECT record FROM hypothesis_entries WHERE id=?', (hypothesis_id,)).fetchone()
            if not row:
                raise ValueError('unknown hypothesis')
            h = json.loads(row['record'])
            superseded = self._superseded(c, h['event_version'], now)
            start = utc(h['window_start'])
            end = start + timedelta(minutes=h['horizon_minutes'])
            legs = {}
            for role, symbol in (('target', h['symbol']), ('benchmark', benchmark_symbol)):
                issues, prices, stamps, offsets = [], [], [], []
                if symbol not in self.targets:
                    issues.append('unknown_target')
                data = samples.get(symbol) or {}
                for name, expected in (('baseline', start), ('terminal', end)):
                    point = data.get(name) if isinstance(data, dict) else None
                    if not point:
                        issues.append('missing_' + name)
                        continue
                    try:
                        price = finite(point['price'])
                        at, known = utc(point['at']), utc(point['known_at'])
                        if price <= 0:
                            raise ValueError('nonpositive')
                        if at > known or known > now or at > now:
                            issues.append('future_or_invalid_timestamp')
                        if at > expected:
                            issues.append('endpoint_after_window')
                        if (expected - at).total_seconds() > max_staleness_seconds:
                            issues.append('stale_' + name)
                        offsets.append((expected - at).total_seconds())
                        prices.append(price)
                        stamps.append(iso(at))
                    except (ValueError, TypeError, KeyError, OverflowError):
                        issues.append('invalid_' + name)
                if now < end:
                    issues.append('window_incomplete')
                if not coverage_complete:
                    issues.append('coverage_incomplete')
                change = None
                if not issues and len(prices) == 2:
                    try:
                        change = finite(100 * (prices[1] / prices[0] - 1))
                    except (ValueError, OverflowError):
                        issues.append('invalid_return')
                legs[role] = dict(symbol=symbol, issues=sorted(set(issues)),
                    endpoints=stamps, endpoint_lag_seconds=offsets, change_pct=change,
                    within_tolerance_but_stale=any(lag > 0 for lag in offsets),
                    status='observed' if not issues else 'unavailable')
            exact = [iso(start), iso(end)]
            comparable = (not legs['target']['issues'] and not legs['benchmark']['issues']
                and legs['target']['endpoints'] == legs['benchmark']['endpoints'] == exact
                and benchmark_symbol == h['benchmark_symbol'] and benchmark_symbol != h['symbol'])
            excess = None
            if comparable:
                try:
                    excess = finite(legs['target']['change_pct'] - legs['benchmark']['change_pct'])
                except ValueError:
                    comparable = False
            report = dict(schema_version=SCHEMA_VERSION, hypothesis_id=hypothesis_id,
                recorded_at=iso(now), source=source, adjustment=adjustment,
                fixture_only=True, event_superseded=superseded, legs=legs, same_window_comparable=comparable,
                comparison_status='comparable' if comparable else 'not_comparable',
                benchmark_matches_registration=benchmark_symbol == h['benchmark_symbol'],
                excess_change_percentage_points=excess if comparable else None,
                direction_match=None, order_permission='none', causal_effect_proven=False)
            # Descriptive matching, not profit/win rate. Never score retrospective,
            # rejected or abstained rows, nor unmatched benchmark windows.
            if comparable and not superseded and h['status'] == 'hypothesis' and h['timing'] == 'prospective':
                change = legs['target']['change_pct']
                report['direction_match'] = {'up': change > 0, 'down': change < 0, 'flat': change == 0}[h['direction']]
            cursor = c.execute('''INSERT INTO hypothesis_outcomes
                (entry_key,hypothesis_id,recorded_at,request,record) VALUES (?,?,?,?,?)''',
                (key, hypothesis_id, iso(now), request, _json(report)))
            return dict(report, id=cursor.lastrowid)

    def audit(self):
        """Complete append history plus latest-per-root coverage; no winner filter.

        Latest revision/outcome is a declared presentation rule, not best result.
        Original immutable entries remain visible, including all rejections.
        """
        with self.store.db() as c:
            entries = [dict(json.loads(r['record']), id=r['id'], root_id=r['root_id'] or r['id'])
                       for r in c.execute('SELECT * FROM hypothesis_entries ORDER BY id')]
            outcomes = [dict(json.loads(r['record']), id=r['id'], input_snapshot=json.loads(r['request']))
                        for r in c.execute('SELECT * FROM hypothesis_outcomes ORDER BY id')]
            superseded = {e['id'] for e in entries if self._superseded(c, e['event_version'], utc(self.store.clock()))}
        latest = {e['root_id']: e for e in entries}
        observations = {o['hypothesis_id']: o for o in outcomes}
        counts = dict(roots=len(latest), revisions=len(entries)-len(latest), entries=len(entries),
                      hypothesis=0, abstained=0, rejected=0, prospective=0, retrospective=0,
                      comparable=0, unobserved=0, not_comparable=0, direction_matches=0,
                      direction_mismatches=0, unscored=0, superseded_event=0)
        for entry in latest.values():
            counts[entry['status']] += 1
            counts[entry['timing']] += 1
            is_superseded = entry['id'] in superseded
            counts['superseded_event'] += int(is_superseded)
            outcome = observations.get(entry['id'])
            if not outcome:
                counts['unobserved'] += 1
                counts['unscored'] += 1
            else:
                counts['comparable' if outcome['same_window_comparable'] else 'not_comparable'] += 1
                match = None if is_superseded else outcome['direction_match']
                counts['unscored' if match is None else 'direction_matches' if match else 'direction_mismatches'] += 1
        return dict(schema_version=SCHEMA_VERSION, entries=entries, outcomes=outcomes, counts=counts,
                    selection_rule='latest_revision_and_latest_observation_by_insertion_id',
                    causal_effect_proven=False, order_permission='none', fixture_only=True)
