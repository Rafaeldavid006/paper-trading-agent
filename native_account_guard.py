"""Disabled PAPER-path coordination; no transport, credentials, or startup wiring.

The source ledger is NEVER written. Its existing experiment flock plus a SQLite
reserved writer lock serialize this path with legacy workers and committed stop
writes. A stop waiting for that lock takes effect AFTER this bounded hold, not
retroactively. Callers must also serialize native claim/dispatch with native stop.
Manual/API clients outside these locks are not fenced: this is not account-wide
atomicity, and a complete authenticated account snapshot remains mandatory.

Production uses one fixed guard file. Alternate state paths are explicit offline
test injection only, never an environment/request override. All new-path transport
attempts (including failures, POST and DELETE) must reserve before the call. The
frozen one-shot probe's separate budget and durable claim are never reset or used.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from zoneinfo import ZoneInfo

from alpaca_schema import SchemaError, TERMINAL, fresh, parse_order, timestamp
from native_observation_worker import load_owned_ledger, ProbeRefused

FIXED_GUARD_PATH = '/var/data/native-account-guard.sqlite'
MAX_REQUESTS = 12
WINDOW_NS = 60_000_000_000
MAX_NS = 2**63 - 1


class AccountGuardRefused(ValueError):
    """Only local error codes are surfaced, never provider or SQLite text."""


def _require(ok, code):
    if not ok:
        raise AccountGuardRefused(code)


def _ns(value):
    _require(type(value) is int and 0 < value <= MAX_NS, 'invalid_local_clock')
    return value


def _canonical(path):
    p = Path(path)
    _require(p.is_absolute() and '..' not in p.parts and str(p) == str(path), 'canonical_path_required')
    for item in (*reversed(p.parents), p):
        _require(not item.is_symlink(), 'unsafe_database_file')
    return str(p)


def _private(path, *, create=False):
    """Open without following final links; validate parents, sidecars and inode."""
    path = _canonical(path)
    _require(Path(path).parent.is_dir(), 'unsafe_database_file')
    created = False
    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
    if create:
        try:
            fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
            created = True
        except FileExistsError:
            fd = os.open(path, flags)
    else:
        fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        _require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
                 and info.st_uid == os.geteuid() and info.st_nlink == 1, 'unsafe_database_file')
        current = os.stat(path, follow_symlinks=False)
        _require((current.st_dev, current.st_ino) == (info.st_dev, info.st_ino), 'database_replaced')
        for suffix in ('-journal', '-wal', '-shm'):
            p = Path(path + suffix)
            _require(not p.is_symlink(), 'unsafe_database_file')
            if p.exists():
                side = p.stat(follow_symlinks=False)
                _require(stat.S_ISREG(side.st_mode) and side.st_uid == info.st_uid
                         and not stat.S_IMODE(side.st_mode) & 0o077 and side.st_nlink == 1,
                         'unsafe_database_file')
        return (info.st_dev, info.st_ino), created
    finally:
        os.close(fd)


def _order_digest(order, raw):
    # Hash only validated fields, never arbitrary provider extras or private raw
    # account IDs. Timestamps are actual native values, not legacy inventions.
    amount = lambda n: None if n is None else str(n.normalize())
    # parse_order validates all seven supported provider timestamps, but its
    # Order projection retains only submitted/updated. Preserve the others in
    # this immutable digest too, normalized to exact ns (not textual RFC3339),
    # so a later valid-looking creation/event rewrite cannot pass unnoticed.
    native_times = [None if raw.get(field) is None else timestamp(raw[field])
                    for field in ('created_at', 'submitted_at', 'updated_at',
                                  'filled_at', 'canceled_at', 'expired_at', 'failed_at')]
    return _digest([order.id, order.client_order_id, order.symbol, order.side,
                    amount(order.quantity), amount(order.filled), amount(order.average_price),
                    order.status, amount(order.limit_price), native_times])


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class LegacyAbsenceReceipt:
    client_order_id: str = field(repr=False)
    account_fingerprint: str = field(repr=False)
    legacy_revision: int = field(repr=False)
    observed_ns: int
    evidence_digest: str = field(repr=False)
    reason: str = 'authenticated_404_original_quote_moved_reserved'


@dataclass(frozen=True)
class LegacyReconciliation:
    absent_client_order_ids: tuple = field(repr=False)
    submitted_client_order_ids: tuple = field(repr=False)
    absence_receipts: tuple = field(repr=False)
    observed_ns: int
    legacy_flat: bool = True
    account_atomic: bool = False


class SharedAccountGuard:
    def __init__(self, legacy_database, lifecycle_database, expected_account_fingerprint,
                 *, enabled=False, state_database=FIXED_GUARD_PATH, test_only=False):
        _require(type(enabled) is bool and type(test_only) is bool, 'invalid_enable')
        _require(type(expected_account_fingerprint) is str
                 and re.fullmatch('[0-9a-f]{64}', expected_account_fingerprint), 'invalid_binding')
        self.legacy_database = _canonical(legacy_database)
        self.lifecycle_database = _canonical(lifecycle_database)
        self.state_database = _canonical(state_database)
        _require(test_only or self.state_database == FIXED_GUARD_PATH, 'fixed_guard_path_required')
        _require(len({self.legacy_database, self.lifecycle_database, self.state_database}) == 3,
                 'separate_databases_required')
        self.binding = expected_account_fingerprint
        self.enabled = enabled
        self._source = None
        self._held = False
        self._source_identity = self._lifecycle_identity = self._state_identity = None
        self._view = None
        self._rows = None
        self._last_reconciliation = None
        self._mutex = threading.Lock()
        self._owner = None
        self._read_only = False
        self._execution_revision = None

    def _inside(self):
        _require(self.enabled is True, 'account_guard_disabled')
        _require(self._held and self._source is not None
                 and self._owner == threading.get_ident(), 'account_guard_not_held')

    @property
    def legacy_client_ids(self):
        self._inside()
        return self._view.client_ids

    @property
    def reserved_client_ids(self):
        self._inside()
        return tuple(sorted(cid for cid, (row, _) in self._rows.items() if row['state'] == 'reserved'))

    @property
    def submitted_client_ids(self):
        self._inside()
        return tuple(sorted(set(self.legacy_client_ids) - set(self.reserved_client_ids)))

    @property
    def legacy_revision(self):
        self._inside()
        return self._execution_revision

    def _read_source(self):
        view = load_owned_ledger(self.legacy_database)
        _require(view.account_fingerprint == self.binding, 'account_binding_mismatch')
        ro = self._source.execute('SELECT stopped FROM paper_readonly WHERE id=1').fetchone()
        safety = self._source.execute('SELECT stopped FROM safety WHERE id=1').fetchone()
        old = self._source.execute('SELECT * FROM paper_experiment WHERE id=1').fetchone()
        renewal = self._source.execute('SELECT * FROM paper_renewal WHERE id=1').fetchone()
        _require(self._read_only or (ro['stopped'] == 0 and safety['stopped'] == 0
                 and old['phase'] != 'stopped' and renewal['phase'] != 'stopped'), 'legacy_stopped')
        _require(renewal['phase'] in ('completed', 'no_fill'), 'legacy_not_terminal_flat')
        _require(old['phase'] == 'manual_attention' and old['reason'] == 'quote_moved'
                 and old['cancel_buy'] == 0 and old['cancel_sell'] == 0
                 and old['cancel_at'] is None, 'legacy_absence_not_eligible')
        rows = {}
        for table in ('intents', 'renewal_intents'):
            for row in self._source.execute('SELECT * FROM ' + table + ' ORDER BY client_id'):
                r = dict(row)
                _require(r['client_id'] not in rows, 'legacy_duplicate_id')
                rows[r['client_id']] = (r, json.loads(r['payload']))
        _require(set(rows) == set(view.client_ids), 'legacy_registry_mismatch')
        for cid, (row, payload) in rows.items():
            if row['state'] == 'reserved':
                _require(cid == old['buy_id'] and row['broker_id'] is None
                         and Decimal(row['filled']) == 0 and payload['side'] == 'buy',
                         'legacy_absence_not_eligible')
            else:
                _require(row['state'] in TERMINAL, 'legacy_order_not_terminal')
        # A missing loss ledger is not equivalent to proof of no historical stop.
        loss_table = self._source.execute("SELECT type FROM sqlite_master WHERE name='loss_days'").fetchone()
        _require(loss_table is not None and tuple(loss_table) == ('table',), 'legacy_risk_state_missing')
        losses = [dict(r) for r in self._source.execute('SELECT day,stopped FROM loss_days ORDER BY day')]
        _require(len(losses) <= 10000 and all(type(r['day']) is str
                 and re.fullmatch(r'\d{4}-\d{2}-\d{2}', r['day'])
                 and type(r['stopped']) is int and r['stopped'] in (0, 1) for r in losses),
                 'legacy_risk_state_invalid')
        return view, rows

    def _native_control(self):
        with sqlite3.connect(Path(self.lifecycle_database).as_uri() + '?mode=ro', uri=True,
                             timeout=0.05) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA trusted_schema=OFF')
            rows = db.execute('SELECT id,binding,stopped,attention FROM control').fetchall()
            _require(len(rows) == 1 and rows[0][:2] == (1, self.binding), 'native_binding_mismatch')
            _require(all(type(v) is int and v in (0, 1) for v in rows[0][2:]), 'native_control_invalid')
            _require(self._read_only or rows[0][2:] == (0, 0), 'native_stopped_or_attention')

    def _stable_execution_revision(self):
        # Probe-only refreshes are not ownership/risk resets. Account identity
        # plus every original/renewal control and intent field is pinned; stops
        # are read freshly, not normalized away from execution checks.
        account = dict(self._source.execute(
            'SELECT fingerprint,identity_first_seen FROM paper_readonly WHERE id=1').fetchone())
        tables = {'account_binding': account}
        for table in ('paper_experiment', 'paper_renewal', 'intents', 'renewal_intents'):
            order = 'client_id' if table.endswith('intents') else 'id'
            tables[table] = [dict(r) for r in self._source.execute(
                'SELECT * FROM ' + table + ' ORDER BY ' + order)]
        return int(_digest(tables), 16)

    def _pin(self):
        return (1, 1, self.binding, self.legacy_database, *self._source_identity,
                self.lifecycle_database, *self._lifecycle_identity, str(self.legacy_revision))

    @contextmanager
    def _state(self, *, initialize=False):
        db = None
        try:
            identity, created = _private(self.state_database, create=initialize)
            if self._state_identity is not None:
                _require(identity == self._state_identity, 'guard_database_replaced')
            self._state_identity = identity
            db = sqlite3.connect(Path(self.state_database).as_uri() + '?mode=rw', uri=True,
                                 timeout=0.05, isolation_level=None)
            db.execute('PRAGMA trusted_schema=OFF')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            if created:
                db.execute('CREATE TABLE account_guard_binding (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, fingerprint TEXT NOT NULL, legacy_path TEXT NOT NULL, legacy_device INTEGER NOT NULL, legacy_inode INTEGER NOT NULL, lifecycle_path TEXT NOT NULL, lifecycle_device INTEGER NOT NULL, lifecycle_inode INTEGER NOT NULL, legacy_revision TEXT NOT NULL, high_ns INTEGER NOT NULL)')
                db.execute('CREATE TABLE account_guard_attempts (at_ns INTEGER NOT NULL)')
                db.execute('CREATE TABLE legacy_absence_receipts (cid TEXT NOT NULL, observed_ns INTEGER NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(cid,observed_ns))')
                db.execute('CREATE TABLE legacy_native_terminals (cid TEXT PRIMARY KEY, native_digest TEXT NOT NULL)')
                db.execute('INSERT INTO account_guard_binding VALUES(?,?,?,?,?,?,?,?,?,?,0)', self._pin())
            names = db.execute("SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
            required_names = {('table', 'account_guard_attempts'), ('table', 'account_guard_binding'),
                              ('table', 'legacy_absence_receipts'), ('table', 'legacy_native_terminals')}
            collector_names = {('table', 'native_read_budget'), ('table', 'native_read_attempts'),
                               ('table', 'native_read_audit')}
            _require(set(names) in (required_names, required_names | collector_names), 'guard_state_corrupt')
            binding = db.execute('SELECT * FROM account_guard_binding').fetchall()
            _require(len(binding) == 1 and binding[0][:-1] == self._pin(), 'guard_binding_mismatch')
            _require(type(binding[0][-1]) is int and 0 <= binding[0][-1] <= MAX_NS, 'guard_state_corrupt')
            yield db
            db.commit()
        except AccountGuardRefused:
            raise
        except Exception:
            raise AccountGuardRefused('guard_storage_unavailable') from None
        finally:
            if db is not None:
                if db.in_transaction:
                    db.rollback()
                db.close()

    @contextmanager
    def hold(self, *, now_ns, read_only=False):
        _require(self.enabled is True, 'account_guard_disabled')
        _ns(now_ns)
        _require(type(read_only) is bool, 'invalid_read_only_mode')
        _require(self._mutex.acquire(blocking=False), 'account_guard_already_held')
        self._owner = threading.get_ident()
        self._read_only = read_only
        fd = None
        body_started = False
        try:
            self._source_identity, _ = _private(self.legacy_database)
            self._lifecycle_identity, _ = _private(self.lifecycle_database)
            lock = self.legacy_database + '.experiment.lock'
            identity, _ = _private(lock)
            fd = os.open(lock, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
            info = os.fstat(fd)
            _require((info.st_dev, info.st_ino) == identity, 'database_replaced')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise AccountGuardRefused('legacy_worker_busy') from None
            self._source = sqlite3.connect(Path(self.legacy_database).as_uri() + '?mode=rw',
                                           uri=True, timeout=0, isolation_level=None)
            self._source.row_factory = sqlite3.Row
            self._source.execute('PRAGMA trusted_schema=OFF')
            try:
                self._source.execute('BEGIN IMMEDIATE')
            except sqlite3.OperationalError:
                raise AccountGuardRefused('legacy_writer_busy') from None
            # Query-only must be set AFTER BEGIN IMMEDIATE; setting it first
            # prevents SQLite taking the reserved writer lock at all.
            self._source.execute('PRAGMA query_only=ON')
            self._held = True
            self._view, self._rows = self._read_source()
            self._execution_revision = self._stable_execution_revision()
            self.check()
            with self._state(initialize=True):
                pass
            self._last_reconciliation = None
            body_started = True
            yield self
        except (AccountGuardRefused, GeneratorExit):
            raise
        except ProbeRefused:
            if body_started:
                raise
            raise AccountGuardRefused('legacy_ledger_invalid') from None
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
            if body_started:
                raise
            raise AccountGuardRefused('account_guard_unavailable') from None
        finally:
            self._held = False
            self._last_reconciliation = None
            if self._source is not None:
                self._source.rollback()
                self._source.close()
                self._source = None
            if fd is not None:
                os.close(fd)
            self._owner = None
            self._read_only = False
            self._mutex.release()

    def check(self):
        """Fresh local binding, terminal-state and stop check, only inside hold."""
        self._inside()
        try:
            _require(_private(self.legacy_database)[0] == self._source_identity, 'legacy_database_replaced')
            _require(_private(self.lifecycle_database)[0] == self._lifecycle_identity, 'native_database_replaced')
            current, _ = self._read_source()
            _require(current == self._view, 'legacy_revision_changed')
            self._native_control()
        except AccountGuardRefused:
            raise
        except Exception:
            raise AccountGuardRefused('account_guard_check_failed') from None
        return True

    def assert_write_allowed(self):
        """Called before every POST/DELETE, including attention reconciliation."""
        self._inside()
        _require(not self._read_only, 'read_only_guard_write_forbidden')
        return self.check()

    def reserve_request(self, now_ns):
        """Commit before attempting any new-path call. Never refund failures."""
        self.check()
        now_ns = _ns(now_ns)
        with self._state() as db:
            high = db.execute('SELECT high_ns FROM account_guard_binding').fetchone()[0]
            _require(now_ns >= high, 'request_clock_regression')
            times = [r[0] for r in db.execute('SELECT at_ns FROM account_guard_attempts')]
            _require(len(times) <= MAX_REQUESTS and all(type(t) is int and 0 < t <= high for t in times),
                     'guard_state_corrupt')
            db.execute('DELETE FROM account_guard_attempts WHERE at_ns <= ?', (now_ns - WINDOW_NS,))
            count = db.execute('SELECT COUNT(*) FROM account_guard_attempts').fetchone()[0]
            _require(count < MAX_REQUESTS, 'shared_request_budget_exhausted')
            db.execute('INSERT INTO account_guard_attempts VALUES(?)', (now_ns,))
            db.execute('UPDATE account_guard_binding SET high_ns=? WHERE id=1', (now_ns,))
        return count + 1

    def reconcile_legacy(self, native_orders, missing_ids, *, now_ns, observed_ns):
        """Validate native broker evidence without rewriting legacy history.

        Trusted adapter contract: missing_ids are CURRENT authenticated 404s for
        these exact IDs on this bound PAPER account, not scan absence. Raw orders
        come from the same complete authenticated collection. This method cannot
        itself authenticate a caller-supplied dict, detect external-client races,
        or attest no manual holdings. The adapter must separately verify the full
        account book. Never use this receipt to manufacture previous Orders.
        """
        self.check()
        _ns(now_ns); _ns(observed_ns)
        try:
            fresh(observed_ns, now_ns)
            _require(type(native_orders) in (list, tuple) and len(native_orders) <= 500,
                     'invalid_legacy_order_evidence')
            _require(type(missing_ids) in (tuple, list, set, frozenset), 'invalid_absence_evidence')
            _require(all(type(cid) is str for cid in missing_ids) and len(set(missing_ids)) == len(missing_ids),
                     'invalid_absence_evidence')
            missing = set(missing_ids)
            _require(missing == set(self.reserved_client_ids), 'legacy_missing_id_mismatch')
            parsed = {}
            for raw in native_orders:
                order = parse_order(raw)
                _require(order.client_order_id not in parsed, 'duplicate_native_order')
                _require(order.updated_ns <= observed_ns, 'future_legacy_order')
                parsed[order.client_order_id] = (order, raw)
            _require(not missing.intersection(parsed), 'legacy_absence_conflict')
            net = Decimal(0)
            terminal_digests = {}
            receipts = []
            for cid, (row, payload) in self._rows.items():
                if cid in missing:
                    _require(row['state'] == 'reserved' and row['broker_id'] is None
                             and Decimal(row['filled']) == 0, 'legacy_absence_not_eligible')
                    digest = _digest([self.binding, str(self.legacy_revision), row, observed_ns,
                                      'authenticated_404_original_quote_moved_reserved'])
                    receipts.append(LegacyAbsenceReceipt(cid, self.binding, self.legacy_revision,
                                                         observed_ns, digest))
                    continue
                _require(cid in parsed, 'legacy_submitted_order_missing')
                order, raw = parsed[cid]
                _require(order.status in TERMINAL and (order.id, order.symbol, order.side,
                         order.quantity, order.limit_price, order.status, order.filled) ==
                         (row['broker_id'], payload['symbol'], payload['side'], Decimal(payload['qty']),
                          Decimal(payload['limit_price']), row['state'], Decimal(row['filled'])),
                         'legacy_order_mismatch')
                # Decimal(str(...)) preserves the stored timestamp rather than
                # fabricating an Order timestamp for an old non-native ledger.
                claimed_ns = Decimal(str(row['created'])) * Decimal(1_000_000_000)
                _require(Decimal(order.submitted_ns) >= claimed_ns, 'legacy_order_predates_claim')
                net += order.filled if order.side == 'buy' else -order.filled
                terminal_digests[cid] = _order_digest(order, raw)
            _require(net == 0, 'legacy_not_flat')
            with self._state() as db:
                for cid, digest in terminal_digests.items():
                    prior = db.execute('SELECT native_digest FROM legacy_native_terminals WHERE cid=?', (cid,)).fetchone()
                    _require(prior is None or prior[0] == digest, 'legacy_terminal_changed')
                    if prior is None:
                        db.execute('INSERT INTO legacy_native_terminals VALUES(?,?)', (cid, digest))
                for receipt in receipts:
                    prior = db.execute('SELECT digest FROM legacy_absence_receipts WHERE cid=? AND observed_ns=?',
                                       (receipt.client_order_id, observed_ns)).fetchone()
                    _require(prior is None or prior[0] == receipt.evidence_digest, 'absence_receipt_conflict')
                    if prior is None:
                        db.execute('INSERT INTO legacy_absence_receipts VALUES(?,?,?)',
                                   (receipt.client_order_id, observed_ns, receipt.evidence_digest))
            result = LegacyReconciliation(tuple(sorted(missing)), self.submitted_client_ids,
                                          tuple(receipts), observed_ns)
            self._last_reconciliation = result
            return result
        except AccountGuardRefused:
            raise
        except (SchemaError, ValueError, TypeError, KeyError):
            raise AccountGuardRefused('invalid_legacy_order_evidence') from None

    def legacy_buy_claims(self, *, session_open_ns, session_close_ns):
        """Count all durable legacy buy reservations, even never-submitted ones."""
        self.check()
        _require(_ns(session_open_ns) < _ns(session_close_ns), 'invalid_session_window')
        return sum(1 for row, payload in self._rows.values() if payload['side'] == 'buy'
                   and Decimal(session_open_ns) <= Decimal(str(row['created'])) * Decimal(1_000_000_000)
                   < Decimal(session_close_ns))

    def legacy_entry_gate(self, *, now_ns):
        """Entry-only legacy exchange-date loss stop; never clears a latch."""
        self.check()
        day = datetime.fromtimestamp(_ns(now_ns) // 1_000_000_000, tz=timezone.utc).astimezone(
            ZoneInfo('America/New_York')).date().isoformat()
        row = self._source.execute('SELECT stopped FROM loss_days WHERE day=?', (day,)).fetchone()
        _require(row is None or row['stopped'] == 0, 'legacy_loss_entry_gate')
        return True
