"""Fail-closed inventory gate for the separately reviewed, PAPER-only runtime.

Ordinary stages call verify BEFORE any auto-creating constructor. They never
enroll, repair, chmod, reset, or create missing state. A separately bounded
initialize_once stage may enroll only this reviewed generation in an EXISTING
application anchor. An interrupted attempt remains a blocked cohort. The legacy
ledger and its existing experiment lock are never written here.

This is local continuity protection, not authenticated anti-rollback storage.
Deletion of the cohort cannot authorize enrollment while its independent
application marker remains. A missing app database or missing pre-generation
history refuses, even if other app code recreates an empty schema. Restoring the
entire app/legacy store to a consistent pre-enrollment backup cannot be
distinguished from first installation locally. Whole-volume rollback, deliberate coherent in-place
rewrites, hostile same-user races, and manual clients require an independent
external anchor/fence; neither inode pins nor SQLite header stamps prove those
properties. The runtime must separately hold its process flock and check latches.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from types import MappingProxyType

import lifecycle_storage
from native_observation_worker import load_owned_ledger
from primary_earnings_source import PARSER


GENERATION = hashlib.sha256(b'native-readonly-runtime-20261020-g1').hexdigest()
VERSION = 1
DB_ROLES = ('legacy', 'source', 'lifecycle', 'guard', 'decision', 'runner')
NEW_ROLES = DB_ROLES[1:]
FIXED_PATHS = MappingProxyType({
    # Fixture/example slot ONLY. Production obtains the actual existing path
    # from EVENT_DB, without a default or any environment mutation.
    'anchor': '/var/data/event-db-test-placeholder.sqlite',
    'legacy': '/var/data/paper-readonly.sqlite',
    'legacy_lock': '/var/data/paper-readonly.sqlite.experiment.lock',
    'source': '/var/data/continuous-primary-source.sqlite',
    'lifecycle': '/var/data/continuous-native-lifecycle.sqlite',
    'guard': '/var/data/native-account-guard.sqlite',
    'decision': '/var/data/continuous-demo-decisions.sqlite',
    'runner': '/var/data/continuous-demo-runner.sqlite',
    'seal': '/var/data/continuous-demo-generation.json',
    'manifest': '/var/data/continuous-demo-state.sqlite',
})
MAX_NS = 2**63 - 1
_SIDECARS = ('-journal', '-wal', '-shm')
_COLUMNS = {
    'source': {
        'primary_meta': ('id', 'binding', 'last_clock', 'success_ns', 'busy'),
        'primary_snapshots': ('hash', 'body'),
        'primary_receipts': ('id', 'source_url', 'snapshot_hash', 'received_ns'),
        'primary_lineage': ('hash', 'episode', 'parent_hash', 'payload'),
        'primary_episodes': ('episode', 'latest_hash', 'status'),
        'primary_events': ('event_id', 'episode', 'lineage_hash', 'payload'),
    },
    'lifecycle': {
        'lifecycle_storage_binding': ('id', 'version'),
        'control': ('id', 'binding', 'universe', 'stopped', 'attention', 'snapshot', 'observed'),
        'intents': ('logical_key', 'cid', 'symbol', 'side', 'quantity', 'price', 'claimed', 'response'),
        'cancels': ('cid', 'claimed'),
        'loss_days': ('day',),
        'session_risk_binding': ('id', 'policy', 'last_clock_ns'),
        'risk_sessions': ('session_close_ns', 'baseline_equity', 'loss_latched', 'entry_claims',
                          'first_observed_ns', 'last_observed_ns'),
        'native_dispatch': ('kind', 'cid', 'attempted_ns', 'state'),
        'native_strategy_sessions': ('close_ns', 'open_ns'),
    },
    'guard': {
        'account_guard_binding': ('id', 'version', 'fingerprint', 'legacy_path',
                                 'legacy_device', 'legacy_inode', 'lifecycle_path',
                                 'lifecycle_device', 'lifecycle_inode', 'legacy_revision', 'high_ns'),
        'account_guard_attempts': ('at_ns',),
        'legacy_absence_receipts': ('cid', 'observed_ns', 'digest'),
        'legacy_native_terminals': ('cid', 'native_digest'),
        'native_read_budget': ('id', 'version', 'high_ns'),
        'native_read_attempts': ('at_ns',),
        'native_read_audit': ('at_ns', 'status', 'attempts', 'owned_count', 'order_count', 'digest'),
    },
    'decision': {
        'demo_control': ('id', 'stopped'),
        'demo_decisions': ('id', 'episode', 'symbol', 'created', 'expires', 'binding',
                           'action', 'reason', 'payload'),
        'demo_intents': ('key', 'decision_id', 'side', 'symbol', 'qty', 'limit_price',
                        'created', 'state', 'filled', 'average_price', 'first_fill_at'),
        'demo_loss': ('day',),
        'demo_cancels': ('key', 'created', 'outcome'),
        'demo_attention': ('key', 'reason'),
    },
    'runner': {
        'runner_control': ('id', 'heartbeat', 'dirty', 'stopped', 'attention', 'stage'),
        'runner_events': ('event_id', 'version', 'symbol', 'payload'),
    },
}
_LEGACY_TABLES = frozenset(('paper_readonly', 'safety', 'paper_experiment',
                           'paper_renewal', 'intents', 'renewal_intents', 'loss_days'))
_ANCHOR_COLUMNS = {
    'events': ('version_id', 'source', 'source_id', 'symbol', 'published_at', 'updated_at',
               'received_at', 'url', 'verified_primary', 'content_hash', 'payload'),
    'expectations': ('id', 'symbol', 'metric', 'period', 'unit', 'value',
                     'published_at', 'recorded_at', 'source', 'url'),
    'decisions': ('id', 'event_version', 'decided_at', 'status', 'reasons', 'evidence'),
    'invalidations': ('id', 'decision_id', 'new_event_version', 'recorded_at'),
    'health': ('source', 'last_attempt', 'last_success', 'status'),
}
_ANCHOR_TABLE = 'native_runtime_enrollment'
_HISTORY_CUTOFF = datetime(2026, 10, 20, 7, 2, 56, tzinfo=timezone.utc)
_ANCHOR_SQL = (
    'CREATE TABLE native_runtime_enrollment(id INTEGER PRIMARY KEY CHECK(id=1),'
    'version INTEGER NOT NULL,generation TEXT NOT NULL,binding_hash TEXT NOT NULL,'
    'anchor_device INTEGER NOT NULL,anchor_inode INTEGER NOT NULL,'
    'history_version TEXT NOT NULL,history_digest TEXT NOT NULL,'
    'status TEXT NOT NULL,seal_device INTEGER,seal_inode INTEGER,'
    'manifest_device INTEGER,manifest_inode INTEGER,manifest_digest TEXT)')


class RuntimeStateRefused(ValueError):
    """Only fixed local codes; never provider, account, or arbitrary DB text."""


def _require(ok, code):
    if not ok:
        raise RuntimeStateRefused(code)


def _canonical(path):
    _require(isinstance(path, (str, Path)), 'invalid_bundle_path')
    value = str(path)
    p = Path(value)
    _require(p.is_absolute() and '..' not in p.parts and str(p) == value,
             'canonical_bundle_path_required')
    return value


def _parents(path):
    for parent in reversed(Path(path).parents):
        info = parent.stat(follow_symlinks=False)
        _require(stat.S_ISDIR(info.st_mode), 'unsafe_bundle_file')


def _private(path, *, public_read=False):
    """Read-only no-follow inspection, including journal/WAL/SHM identities."""
    _parents(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        mode = stat.S_IMODE(info.st_mode)
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                 and (mode in (0o600, 0o640, 0o644) if public_read else mode == 0o600)
                 and info.st_nlink == 1,
                 'unsafe_bundle_file')
        current = os.stat(path, follow_symlinks=False)
        _require((current.st_dev, current.st_ino) == (info.st_dev, info.st_ino),
                 'bundle_file_replaced')
        for suffix in _SIDECARS:
            try:
                side = os.stat(path + suffix, follow_symlinks=False)
            except FileNotFoundError:
                continue
            side_mode = stat.S_IMODE(side.st_mode)
            _require(stat.S_ISREG(side.st_mode) and side.st_uid == info.st_uid
                     and (side_mode in (0o600, 0o640, 0o644) if public_read else side_mode == 0o600)
                     and side.st_nlink == 1,
                     'unsafe_bundle_file')
        return (info.st_dev, info.st_ino)
    finally:
        os.close(fd)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, 'bundle_state_corrupt')
        result[key] = value
    return result


def _load(raw):
    return json.loads(raw, object_pairs_hook=_pairs,
                      parse_constant=lambda _: _require(False, 'bundle_state_corrupt'))


def _digest(value):
    return hashlib.sha256(_json(value)).hexdigest()


def _stamp(role):
    # Role-specific 62-bit header marker. Not a secret/MAC or anti-rollback proof.
    digest = hashlib.sha256((GENERATION + ':' + role).encode()).digest()
    return tuple((int.from_bytes(digest[i:i + 4], 'big') & 0x7fffffff) or 1
                 for i in (0, 4))


def _ns(value, *, nullable=False):
    return (nullable and value is None) or (type(value) is int and 0 <= value <= MAX_NS)


def _bool(value):
    return type(value) is int and value in (0, 1)


@contextmanager
def _database(path, identity, *, write=False, public_read=False):
    _require(_private(path, public_read=public_read) == identity, 'bundle_file_replaced')
    if not write:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            head = os.read(fd, 100)
        finally:
            os.close(fd)
        # The reviewed cohort uses rollback journals, not WAL. No read-only
        # startup can create/repair a WAL shared-memory sidecar.
        _require(len(head) == 100 and head[:16] == b'SQLite format 3\0'
                 and head[18:20] == b'\1\1', 'bundle_database_corrupt')
    db = None
    try:
        db = sqlite3.connect(Path(path).as_uri() + ('?mode=rw' if write else '?mode=ro'),
                             uri=True, timeout=0.05, isolation_level=None)
        db.execute('PRAGMA trusted_schema=OFF')
        db.execute('PRAGMA temp_store=MEMORY')
        db.execute('PRAGMA synchronous=FULL')
        if not write:
            db.execute('PRAGMA query_only=ON')
        deadline = time.monotonic() + 2
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        _require(_private(path, public_read=public_read) == identity, 'bundle_file_replaced')
        db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        yield db
        _require(_private(path, public_read=public_read) == identity, 'bundle_file_replaced')
        if write:
            db.commit()
    finally:
        if db is not None:
            if db.in_transaction:
                db.rollback()
            db.close()
        _require(_private(path, public_read=public_read) == identity, 'bundle_file_replaced')


def _singleton(db, table):
    rows = db.execute('SELECT * FROM ' + table).fetchmany(2)
    _require(len(rows) == 1 and rows[0][0] == 1, 'bundle_state_uninitialized')
    return rows[0]


def _schema(db, role):
    _require(db.execute('PRAGMA quick_check(1)').fetchall() == [('ok',)],
             'bundle_database_corrupt')
    rows = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master "
                      "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()
    _require(all(r[0] in ('table', 'index') for r in rows), 'bundle_schema_invalid')
    names = {r[1] for r in rows if r[0] == 'table'}
    if role == 'legacy':
        _require(_LEGACY_TABLES <= names, 'bundle_schema_incomplete')
    else:
        _require(names == set(_COLUMNS[role]), 'bundle_schema_incomplete')
        indexes = {r[1] for r in rows if r[0] == 'index'}
        _require(indexes == ({'demo_one_side'} if role == 'decision' else set()),
                 'bundle_schema_invalid')
        for table, expected in _COLUMNS[role].items():
            columns = db.execute('PRAGMA table_xinfo(' + table + ')').fetchall()
            _require(tuple(r[1] for r in columns) == expected
                     and all(r[-1] == 0 for r in columns), 'bundle_schema_incomplete')
    return _digest(rows)


class RuntimeStateBundle:
    def __init__(self, expected_account_fingerprint, *, paths=None, test_only=False):
        _require(type(test_only) is bool, 'invalid_test_mode')
        _require(type(expected_account_fingerprint) is str
                 and re.fullmatch('[0-9a-f]{64}', expected_account_fingerprint),
                 'invalid_account_binding')
        if not test_only:
            _require(paths is None, 'fixed_bundle_paths_required')
            anchor = os.environ.get('EVENT_DB')
            _require(type(anchor) is str and bool(anchor), 'existing_event_db_configuration_required')
            anchor = _canonical(anchor)
            _require(Path('/var/data') in Path(anchor).parents,
                     'existing_event_db_persistent_path_required')
            supplied = dict(FIXED_PATHS)
            supplied['anchor'] = anchor
        else:
            supplied = dict(FIXED_PATHS) if paths is None else dict(paths)
        _require(set(supplied) == set(FIXED_PATHS), 'complete_inventory_required')
        supplied = {key: _canonical(value) for key, value in supplied.items()}
        _require(test_only or all(supplied[k] == value for k, value in FIXED_PATHS.items()
                                 if k != 'anchor'), 'fixed_bundle_paths_required')
        _require(len(set(supplied.values())) == len(supplied), 'separate_bundle_files_required')
        _require(supplied['legacy_lock'] == supplied['legacy'] + '.experiment.lock',
                 'legacy_lock_path_mismatch')
        # A database name must not alias another file's SQLite sidecar.
        _require(not any(path + suffix in supplied.values() for path in supplied.values()
                         for suffix in _SIDECARS), 'separate_bundle_files_required')
        self.paths = MappingProxyType(supplied)
        self.binding = expected_account_fingerprint
        self._pinned = None
        self._restart = None
        self._restart_contract = None
        self._enrolling = False
        if not test_only:
            try:
                self._identity('anchor')
            except RuntimeStateRefused:
                raise
            except OSError:
                raise RuntimeStateRefused('bundle_anchor_unavailable') from None

    def _identity(self, role):
        return _private(self.paths[role], public_read=role == 'anchor')

    def _reference_identity(self, role):
        return self._restart.references[role] if self._restart is not None else self._identity(role)

    def guard_identities(self):
        """Fresh full-cohort capability; never a caller-supplied ignore-device flag."""
        self.verify()
        return (dict(self._pinned), dict(self._restart.references) if self._restart is not None
                else dict(self._pinned), self._restart is not None)

    def _anchor_binding(self):
        return _digest([GENERATION, self.binding, dict(self.paths)])

    def _anchor_schema(self, db, *, enrolled=True):
        _require(db.execute('PRAGMA quick_check(1)').fetchall() == [('ok',)],
                 'bundle_anchor_corrupt')
        records = []
        for table, columns in _ANCHOR_COLUMNS.items():
            rows = db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master '
                              'WHERE name=?', (table,)).fetchall()
            _require(len(rows) == 1 and rows[0][0] == 'table', 'bundle_anchor_schema_invalid')
            actual = db.execute('PRAGMA table_xinfo(' + table + ')').fetchall()
            _require(tuple(r[1] for r in actual) == columns and all(r[-1] == 0 for r in actual),
                     'bundle_anchor_schema_invalid')
            _require(not db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' "
                                    'AND tbl_name=?', (table,)).fetchone(),
                     'bundle_anchor_schema_invalid')
            records.extend(rows)
        if enrolled:
            row = db.execute('SELECT sql FROM sqlite_master WHERE type=\'table\' AND name=?',
                             (_ANCHOR_TABLE,)).fetchone()
            _require(row == (_ANCHOR_SQL,), 'bundle_anchor_schema_invalid')
            _require(not db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' "
                                    'AND tbl_name=?', (_ANCHOR_TABLE,)).fetchone(),
                     'bundle_anchor_schema_invalid')
            records.append(row)
        return _digest(records)

    def _anchor_row(self, db, *, migration_reference=None):
        self._anchor_schema(db)
        row = _singleton(db, _ANCHOR_TABLE)
        _require(row[:4] == (1, VERSION, GENERATION, self._anchor_binding()),
                 'bundle_anchor_binding_mismatch')
        _require(row[4:6] == (self._reference_identity('anchor') if migration_reference is None else migration_reference), 'bundle_anchor_replaced')
        _require(type(row[6]) is str and re.fullmatch('[0-9a-f]{64}', row[6])
                 and type(row[7]) is str and re.fullmatch('[0-9a-f]{64}', row[7]),
                 'bundle_anchor_corrupt')
        history = db.execute('SELECT * FROM events WHERE version_id=?', (row[6],)).fetchmany(2)
        _require(len(history) == 1 and _digest(history[0]) == row[7]
                 and self._historical(history[0]), 'bundle_anchor_history_changed')
        _require(row[8] in ('provisioning', 'completed'), 'bundle_anchor_corrupt')
        return row

    @staticmethod
    def _historical(row):
        try:
            received = datetime.fromisoformat(row[6])
            return (type(row[0]) is str and re.fullmatch('[0-9a-f]{64}', row[0]) is not None
                    and received.utcoffset() is not None
                    and datetime(1970, 1, 1, tzinfo=timezone.utc) < received < _HISTORY_CUTOFF)
        except (TypeError, ValueError, OverflowError):
            return False

    def _choose_history(self, db):
        # Bounded oldest-row inspection, without ever returning event content to
        # a caller or emitting it in an error. Newly fetched rows cannot qualify.
        cursor = db.execute('SELECT * FROM events ORDER BY rowid LIMIT 10000')
        for row in cursor:
            if self._historical(row):
                return row[0], _digest(row)
        raise RuntimeStateRefused('bundle_existing_history_required')

    def _content(self, db, role, identities):
        if role == 'legacy':
            account = _singleton(db, 'paper_readonly')
            _require(db.execute('SELECT fingerprint FROM paper_readonly').fetchall()
                     == [(self.binding,)], 'bundle_account_mismatch')
            for table in ('safety', 'paper_experiment', 'paper_renewal'):
                _singleton(db, table)
            _require(account[0] == 1, 'bundle_state_corrupt')
        elif role == 'source':
            row = _singleton(db, 'primary_meta')
            _require(row[1] == PARSER and _ns(row[2], nullable=True)
                     and _ns(row[3], nullable=True) and _bool(row[4]),
                     'bundle_source_state_corrupt')
        elif role == 'lifecycle':
            _require(_singleton(db, 'lifecycle_storage_binding') == (1, lifecycle_storage.VERSION),
                     'bundle_lifecycle_state_corrupt')
            row = _singleton(db, 'control')
            _require(row[1] == self.binding, 'bundle_account_mismatch')
            universe = _load(row[2])
            _require(type(universe) is list and universe and
                     all(type(s) is str and re.fullmatch('[A-Z][A-Z0-9.\\-]{0,11}', s) for s in universe)
                     and sorted(set(universe)) == universe
                     and _bool(row[3]) and _bool(row[4]), 'bundle_lifecycle_state_corrupt')
            if row[5] is None:
                _require(row[6] is None, 'bundle_lifecycle_state_corrupt')
            else:
                snapshot = lifecycle_storage.decode_snapshot(
                    lifecycle_storage.loads(row[5]), binding=self.binding)
                _require(snapshot.observed_ns == row[6], 'bundle_lifecycle_state_corrupt')
            policy = _singleton(db, 'session_risk_binding')
            value = _load(policy[1])
            _require(type(value) is dict and set(value) == {'max_entries', 'max_loss'}
                     and type(value['max_entries']) is int and 0 < value['max_entries'] <= 4
                     and 0 < lifecycle_storage.number(value['max_loss']) <= 100
                     and _ns(policy[2], nullable=True), 'bundle_risk_state_corrupt')
            sessions = db.execute('SELECT * FROM risk_sessions ORDER BY session_close_ns').fetchall()
            for session in sessions:
                close, baseline, loss, count, first, last = session
                _require(_ns(close) and close > 0 and lifecycle_storage.number(baseline) > 0
                         and _bool(loss) and type(count) is int and 0 <= count <= value['max_entries']
                         and _ns(first) and _ns(last) and 0 < first <= last and first < close,
                         'bundle_risk_state_corrupt')
            intents = db.execute('SELECT * FROM intents').fetchall()
            buys = []
            ids = set()
            for logical, cid, symbol, side, quantity, price, claimed, response in intents:
                _require(type(logical) is str and logical and type(cid) is str and cid
                         and symbol in universe and side in ('buy', 'sell')
                         and lifecycle_storage.number(quantity, quantity=True) > 0
                         and lifecycle_storage.number(price) > 0 and _ns(claimed) and claimed > 0,
                         'bundle_intent_state_corrupt')
                ids.add(cid)
                if side == 'buy':
                    buys.append(claimed)
                if response is not None:
                    order = lifecycle_storage.decode_order(lifecycle_storage.loads(response))
                    _require(order.client_order_id == cid and order.symbol == symbol and order.side == side,
                             'bundle_intent_state_corrupt')
            _require(sum(s[3] for s in sessions) == len(buys)
                     and all(sum(s[4] <= at < s[0] for s in sessions) == 1 for at in buys),
                     'bundle_risk_state_corrupt')
            if row[5] is None:
                _require(not sessions and not intents and policy[2] is None,
                         'bundle_lifecycle_state_corrupt')
            else:
                _require(policy[2] == snapshot.clock.timestamp_ns, 'bundle_risk_state_corrupt')
                if snapshot.clock.is_open or getattr(snapshot,'execution_session',None) is not None:
                    _require(sessions and sessions[-1][0] == snapshot.clock.next_close_ns,
                             'bundle_risk_state_corrupt')
            cancel_ids = set()
            for cid, claimed in db.execute('SELECT * FROM cancels'):
                _require(cid in ids and _ns(claimed) and claimed > 0, 'bundle_intent_state_corrupt')
                cancel_ids.add(cid)
            for kind, cid, attempted, status in db.execute('SELECT * FROM native_dispatch'):
                _require(kind in ('submit', 'cancel') and cid in ids
                         and (kind != 'cancel' or cid in cancel_ids)
                         and _ns(attempted) and attempted > 0 and status in ('uncertain', 'observed'),
                         'bundle_dispatch_state_corrupt')
            for close, opening in db.execute('SELECT * FROM native_strategy_sessions'):
                _require(_ns(close) and _ns(opening) and 0 < opening < close,
                         'bundle_session_state_corrupt')
        elif role == 'guard':
            row = _singleton(db, 'account_guard_binding')
            expected = (1, 1, self.binding, self.paths['legacy'], *identities['legacy'],
                        self.paths['lifecycle'], *identities['lifecycle'])
            _require(row[:9] == expected, 'bundle_guard_binding_mismatch')
            _require(type(row[9]) is str and re.fullmatch('[0-9]{1,78}', row[9])
                     and _ns(row[10]), 'bundle_guard_state_corrupt')
            budget = _singleton(db, 'native_read_budget')
            _require(budget[:2] == (1, 1) and _ns(budget[2]), 'bundle_budget_state_corrupt')
            for table, high in (('account_guard_attempts', row[10]),
                                ('native_read_attempts', budget[2])):
                values = db.execute('SELECT at_ns FROM ' + table).fetchmany(13)
                _require(len(values) <= 12 and all(_ns(r[0]) and 0 < r[0] <= high for r in values),
                         'bundle_budget_state_corrupt')
        elif role == 'decision':
            _require(_bool(_singleton(db, 'demo_control')[1]), 'bundle_decision_state_corrupt')
        elif role == 'runner':
            row = _singleton(db, 'runner_control')
            _require((row[1] is None or (type(row[1]) in (int, float)
                     and math.isfinite(row[1]) and row[1] > 0))
                     and _bool(row[2]) and _bool(row[3])
                     and (row[4] is None or type(row[4]) is str)
                     and type(row[5]) is str, 'bundle_runner_state_corrupt')

    def _inspect(self, identities, *, stamped, references=None):
        references = identities if references is None else references
        result = []
        for role in DB_ROLES:
            with _database(self.paths[role], identities[role]) as db:
                schema = _schema(db, role)
                self._content(db, role, references)
                if role != 'legacy' and stamped:
                    actual = (db.execute('PRAGMA application_id').fetchone()[0],
                              db.execute('PRAGMA user_version').fetchone()[0])
                    _require(actual == (
                        __import__('native_restart_identity').stamp(self._restart.cohort, role)
                        if self._restart is not None else _stamp(role)), 'bundle_generation_mismatch')
                result.append((role, self.paths[role], *references[role], schema))
        # Validate the complete immutable legacy account/intent shape without
        # constructing an execution adapter or writing any legacy field.
        view = load_owned_ledger(self.paths['legacy'])
        _require(view.account_fingerprint == self.binding, 'bundle_account_mismatch')
        result.append(('legacy_lock', self.paths['legacy_lock'],
                       *references['legacy_lock'], None))
        with _database(self.paths['anchor'], identities['anchor'], public_read=True) as db:
            anchor_schema = self._anchor_schema(db)
        result.append(('anchor', self.paths['anchor'], *references['anchor'], anchor_schema))
        return sorted(result)

    def _read_seal(self, identity):
        _require(_private(self.paths['seal']) == identity, 'bundle_file_replaced')
        fd = os.open(self.paths['seal'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            _require((info.st_dev, info.st_ino) == identity, 'bundle_file_replaced')
            raw = os.read(fd, 4097)
            _require(len(raw) <= 4096, 'bundle_state_corrupt')
        finally:
            os.close(fd)
        value = _load(raw)
        expected_keys = {'format', 'version', 'generation', 'fingerprint', 'initialized',
                         'manifest_identity', 'manifest_digest'}
        if self._restart is not None:
            expected_keys.add('restart_identity')
        _require(type(value) is dict and set(value) == expected_keys, 'bundle_state_corrupt')
        _require(value['format'] == 'native_runtime_state_seal' and
                 type(value['version']) is int and value['version'] == VERSION and
                 value['generation'] == GENERATION and value['fingerprint'] == self.binding,
                 'bundle_generation_or_account_mismatch')
        _require(value['initialized'] is True, 'bundle_provision_incomplete')
        return value

    def verify(self):
        """No writes or repairs. Call before constructors and before every tick."""
        try:
            identities = {role: self._identity(role) for role in self.paths}
            if self._pinned is not None:
                _require(identities == self._pinned, 'bundle_file_replaced')
            if not self._enrolling:
                import native_restart_identity
                adopted = native_restart_identity.load(self, identities)
                if self._restart_contract is not None:
                    _require(adopted is not None and adopted.contract == self._restart_contract,
                             'bundle_restart_identity_changed')
                self._restart = adopted
                if adopted is not None:
                    self._restart_contract = adopted.contract
            references = dict(self._restart.references) if self._restart is not None else identities
            seal = self._read_seal(identities['seal'])
            with _database(self.paths['anchor'], identities['anchor'], public_read=True) as db:
                anchor = self._anchor_row(db)
            if not self._enrolling:
                _require(anchor[8:] == ('completed', *references['seal'],
                         *references['manifest'], seal['manifest_digest']),
                         'bundle_anchor_incomplete_or_changed')
            else:
                _require(anchor[8:] == ('provisioning', None, None, None, None, None),
                         'bundle_anchor_incomplete_or_changed')
            _require(seal['manifest_identity'] == list(references['manifest']),
                     'bundle_manifest_replaced')
            with _database(self.paths['manifest'], identities['manifest']) as db:
                _require(db.execute('PRAGMA quick_check(1)').fetchall() == [('ok',)],
                         'bundle_database_corrupt')
                names = db.execute("SELECT type,name FROM sqlite_master "
                                   "WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
                _require(names == [('table', 'bundle_binding'), ('table', 'bundle_files')],
                         'bundle_manifest_corrupt')
                binding = _singleton(db, 'bundle_binding')
                _require(binding == (1, VERSION, GENERATION, self.binding, 1, *references['seal']),
                         'bundle_manifest_binding_mismatch')
                records = db.execute('SELECT role,path,device,inode,schema_hash '
                                     'FROM bundle_files ORDER BY role').fetchall()
                _require(seal['manifest_digest'] == _digest([binding, records]),
                         'bundle_manifest_corrupt')
            if self._restart is not None:
                metadata = self._restart.metadata
                _require(metadata['manifest_digest'] == seal['manifest_digest']
                         and metadata['history'] == list(anchor[6:8])
                         and metadata['schemas'] == {r[0]: r[4] for r in records},
                         'bundle_restart_metadata_changed')
            expected = self._inspect(identities, stamped=True, references=references)
            _require(records == expected, 'bundle_inventory_or_schema_changed')
            _require({role: self._identity(role) for role in self.paths} == identities,
                     'bundle_file_replaced')
            self._pinned = identities
            return True
        except RuntimeStateRefused:
            raise
        except FileNotFoundError:
            raise RuntimeStateRefused('bundle_file_missing') from None
        except Exception:
            raise RuntimeStateRefused('bundle_state_unavailable') from None

    @staticmethod
    def _create(path):
        _parents(path)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            _private(path)
        except BaseException:
            os.close(fd)
            raise
        return fd

    @staticmethod
    def _write(fd, raw):
        os.lseek(fd, 0, os.SEEK_SET)
        remaining = memoryview(raw)
        while remaining:
            written = os.write(fd, remaining)
            _require(written > 0, 'bundle_provision_failed')
            remaining = remaining[written:]
        os.ftruncate(fd, len(raw))
        os.fsync(fd)

    def _sync_dirs(self):
        for directory in {str(Path(path).parent) for path in self.paths.values()}:
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def initialize_once(self, *, initialize):
        """Claim the existing application anchor before any new cohort writes.

        Only the fixed reviewed generation is enrolled. The pre-existing app
        schema, legacy account binding, and legacy experiment lock are required.
        Missing/empty/partial enrollment rows never mean first installation.
        """
        _require(callable(initialize), 'invalid_bundle_initializer')
        try:
            anchor_id = self._identity('anchor')
            self._identity('legacy_lock')
            _require(load_owned_ledger(self.paths['legacy']).account_fingerprint == self.binding,
                     'bundle_account_mismatch')
            # Validate the existing header/schema READ-ONLY before a reserved
            # writer lock: corrupt or zero-byte app files must not be initialized.
            with _database(self.paths['anchor'], anchor_id, public_read=True) as db:
                self._anchor_schema(db, enrolled=False)
                existing = db.execute('SELECT name FROM sqlite_master WHERE name IN (?,?)',
                                      (_ANCHOR_TABLE, 'native_restart_identity')).fetchall()
                if (_ANCHOR_TABLE,) in existing:
                    self._anchor_schema(db)
                    _require(_singleton(db, _ANCHOR_TABLE)[8] != 'provisioning',
                             'bundle_anchor_provision_incomplete')
            # A previously enrolled or partially migrated cohort is verification
            # only. Load its restart identity before any old physical-reference
            # comparison, and never open a writable enrollment path on restart.
            if existing:
                return self.verify()
            already = False
            with _database(self.paths['anchor'], anchor_id, write=True, public_read=True) as db:
                self._anchor_schema(db, enrolled=False)
                existing = db.execute('SELECT type FROM sqlite_master WHERE name=?',
                                      (_ANCHOR_TABLE,)).fetchall()
                if existing:
                    row = self._anchor_row(db)
                    _require(row[8] == 'completed', 'bundle_anchor_provision_incomplete')
                    already = True
                else:
                    history_version, history_digest = self._choose_history(db)
                    for role in (*NEW_ROLES, 'seal', 'manifest'):
                        for path in (self.paths[role], *(self.paths[role] + s for s in _SIDECARS)):
                            _require(not os.path.lexists(path),
                                     'bundle_already_or_partially_provisioned')
                    db.execute(_ANCHOR_SQL)
                    db.execute('INSERT INTO native_runtime_enrollment VALUES(1,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,NULL)',
                               (VERSION, GENERATION, self._anchor_binding(), *anchor_id,
                                history_version, history_digest, 'provisioning'))
            if already:
                return self.verify()
            self._sync_dirs()
            self._enrolling = True
            try:
                self.provision(generation=GENERATION, initialize=initialize, reviewed=True)
            finally:
                self._enrolling = False
            _require(self._identity('anchor') == anchor_id, 'bundle_file_replaced')
            seal = self._read_seal(self._identity('seal'))
            with _database(self.paths['anchor'], anchor_id, write=True, public_read=True) as db:
                row = self._anchor_row(db)
                _require(row[8:] == ('provisioning', None, None, None, None, None),
                         'bundle_anchor_incomplete_or_changed')
                db.execute("UPDATE native_runtime_enrollment SET status='completed',"
                           'seal_device=?,seal_inode=?,manifest_device=?,manifest_inode=?,manifest_digest=? WHERE id=1',
                           (*self._identity('seal'), *self._identity('manifest'), seal['manifest_digest']))
            return self.verify()
        except RuntimeStateRefused:
            raise
        except FileNotFoundError:
            raise RuntimeStateRefused('bundle_file_missing') from None
        except Exception:
            raise RuntimeStateRefused('bundle_provision_failed') from None

    def provision(self, *, generation, initialize, reviewed=False):
        """Explicit operator enrollment, NEVER a missing-file fallback.

        initialize(paths) is a trusted local, no-network callback. It receives
        precreated owner-only source/lifecycle/decision/runner files; guard is
        absent so SharedAccountGuard can initialize its own schema. It must add
        the collector budget, native dispatch and strategy-session tables too.
        Any existing new-cohort file/sidecar refuses before callback invocation.
        No cleanup after failure: operator review is required, not a fresh reset.
        """
        _require(reviewed is True and generation == GENERATION and callable(initialize),
                 'reviewed_provision_required')
        _require(self._enrolling is True, 'existing_anchor_enrollment_required')
        seal_fd = None
        try:
            identities = {role: self._identity(role) for role in ('legacy', 'legacy_lock', 'anchor')}
            _require(load_owned_ledger(self.paths['legacy']).account_fingerprint == self.binding,
                     'bundle_account_mismatch')
            for role in (*NEW_ROLES, 'seal', 'manifest'):
                _parents(self.paths[role])
                for path in (self.paths[role], *(self.paths[role] + s for s in _SIDECARS)):
                    _require(not os.path.lexists(path), 'bundle_already_or_partially_provisioned')
            # Durable marker comes FIRST. Every subsequent failure is terminal
            # for automatic initialization, even if no database was completed.
            seal_fd = self._create(self.paths['seal'])
            seal = dict(format='native_runtime_state_seal', version=VERSION,
                        generation=GENERATION, fingerprint=self.binding, initialized=False,
                        manifest_identity=None, manifest_digest=None)
            self._write(seal_fd, _json(seal))
            self._sync_dirs()
            identities['seal'] = _private(self.paths['seal'])
            for role in ('manifest', 'source', 'lifecycle', 'decision', 'runner'):
                fd = self._create(self.paths[role])
                os.close(fd)
                identities[role] = _private(self.paths[role])
            initialize(self.paths)
            for role, identity in identities.items():
                _require(self._identity(role) == identity, 'bundle_file_replaced')
            identities['guard'] = _private(self.paths['guard'])
            records = self._inspect(identities, stamped=False)
            for role in NEW_ROLES:
                with _database(self.paths[role], identities[role], write=True) as db:
                    # Do not silently adopt an already stamped/restored cohort.
                    _require(db.execute('PRAGMA application_id').fetchone() == (0,)
                             and db.execute('PRAGMA user_version').fetchone() == (0,),
                             'bundle_existing_generation')
                    app, version = _stamp(role)
                    db.execute('PRAGMA application_id=' + str(app))
                    db.execute('PRAGMA user_version=' + str(version))
            binding = (1, VERSION, GENERATION, self.binding, 1, *identities['seal'])
            with _database(self.paths['manifest'], identities['manifest'], write=True) as db:
                db.execute('CREATE TABLE bundle_binding(id INTEGER PRIMARY KEY CHECK(id=1), '
                           'version INTEGER NOT NULL,generation TEXT NOT NULL,fingerprint TEXT NOT NULL,'
                           'initialized INTEGER NOT NULL CHECK(initialized=1),'
                           'seal_device INTEGER NOT NULL,seal_inode INTEGER NOT NULL)')
                db.execute('CREATE TABLE bundle_files(role TEXT PRIMARY KEY,path TEXT UNIQUE NOT NULL,'
                           'device INTEGER NOT NULL,inode INTEGER NOT NULL,schema_hash TEXT)')
                db.execute('INSERT INTO bundle_binding VALUES(?,?,?,?,?,?,?)', binding)
                db.executemany('INSERT INTO bundle_files VALUES(?,?,?,?,?)', records)
            for role in (*NEW_ROLES, 'manifest'):
                fd = os.open(self.paths[role], os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            self._sync_dirs()
            _require(_private(self.paths['seal']) == identities['seal'], 'bundle_file_replaced')
            seal.update(initialized=True, manifest_identity=list(identities['manifest']),
                        manifest_digest=_digest([binding, records]))
            self._write(seal_fd, _json(seal))
            self._sync_dirs()
            self._pinned = identities
            return self.verify()
        except RuntimeStateRefused:
            raise
        except FileNotFoundError:
            raise RuntimeStateRefused('bundle_file_missing') from None
        except Exception:
            raise RuntimeStateRefused('bundle_provision_failed') from None
        finally:
            if seal_fd is not None:
                os.close(seal_fd)
