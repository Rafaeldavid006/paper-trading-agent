"""Production Paper account reads only. No execution adapter or broker write path.

Existing credentials are consumed server-side, never returned, persisted or logged.
Identity discovery is NOT owner verification and cannot authorize trading.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from paper_adapter import PaperHTTP, SafetyError


class ReadFailure(SafetyError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__('Paper read-only check failed')


class ReadOnlyPaperHTTP:
    """Independent GET-only boundary around the fixed-host/no-redirect transport."""
    PATHS = frozenset(('/v2/account', '/v2/positions', '/v2/clock',
                       '/v2/orders?status=open&limit=500&nested=false'))

    def __init__(self, key, secret):
        self.__http = PaperHTTP(key, secret)

    def call(self, method, path, body=None):
        if method != 'GET' or path not in self.PATHS or body is not None:
            raise SafetyError('Read-only transport forbids this operation')
        return self.__http.call('GET', path)


def numeric(value):
    if isinstance(value, bool):
        raise ReadFailure('invalid_response')
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ReadFailure('invalid_response') from None
    if not number.is_finite() or abs(number) > Decimal('1e15'):
        raise ReadFailure('invalid_response')
    return str(number)


def label(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,60}', value):
        raise ReadFailure('invalid_response')
    return value


class ReadOnlyPaperConnection:
    def __init__(self, database, transport, *, now=None):
        self.database, self.transport = str(database), transport
        self.now = now or (lambda: datetime.now(timezone.utc))
        # Snapshot contains private financial data. Never follow a database symlink.
        descriptor = os.open(self.database, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise SafetyError('Read-only state must be a regular private file')
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        with self.db() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS paper_readonly (
                id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL DEFAULT 0,
                fingerprint TEXT, masked_identity TEXT, checked_at REAL,
                ok INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT 'not_checked',
                snapshot TEXT NOT NULL DEFAULT '{}', stopped INTEGER NOT NULL DEFAULT 0)''')
            c.execute('INSERT OR IGNORE INTO paper_readonly(id) VALUES(1)')
            if 'identity_first_seen' not in {r[1] for r in c.execute('PRAGMA table_info(paper_readonly)')}:
                c.execute('ALTER TABLE paper_readonly ADD COLUMN identity_first_seen REAL')
                c.execute('UPDATE paper_readonly SET identity_first_seen=checked_at WHERE fingerprint IS NOT NULL')

    @contextmanager
    def db(self):
        connection = sqlite3.connect(self.database, timeout=15)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _get(self, path):
        status, data = self.transport.call('GET', path)
        if status in (401, 403):
            raise ReadFailure('authentication_failed')
        if status != 200:
            raise ReadFailure('provider_unavailable')
        return data

    def _snapshot(self):
        account = self._get('/v2/account')
        identity = label(account['id'])
        fingerprint = hashlib.sha256(identity.encode()).hexdigest()
        # Never persist the full account ID or number; mask before any display/storage.
        display_identity = label(account.get('account_number') or identity)
        masked = '…' + (display_identity[-4:] if len(display_identity) > 4 else '*' * len(display_identity))
        positions = self._get('/v2/positions')
        orders = self._get('/v2/orders?status=open&limit=500&nested=false')
        clock = self._get('/v2/clock')
        if not isinstance(positions, list) or not isinstance(orders, list) or len(positions) > 500 or len(orders) >= 500:
            raise ReadFailure('incomplete_snapshot')
        timestamp = datetime.fromisoformat(clock['timestamp'].replace('Z', '+00:00'))
        if timestamp.tzinfo is None or not 0 <= (self.now()-timestamp).total_seconds() <= 10:
            raise ReadFailure('stale_response')
        result = dict(cash=numeric(account['cash']), equity=numeric(account['equity']),
                      currency=label(account['currency']), account_status=label(account['status']),
                      market_open=clock.get('is_open') is True, positions=len(positions), open_orders=len(orders),
                      holdings=[dict(symbol=label(p['symbol']), qty=numeric(p['qty']),
                                     value=numeric(p['market_value']), side=label(p['side'])) for p in positions],
                      orders=[dict(symbol=label(o['symbol']), side=label(o['side']), qty=numeric(o['qty']),
                                   filled=numeric(o['filled_qty']), status=label(o['status'])) for o in orders])
        return fingerprint, masked, result

    def check_connection(self):
        # Commit invalidation first: interruptions cannot leave a stale success.
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            c.execute("UPDATE paper_readonly SET ok=0,reason='checking',snapshot='{}',revision=revision+1 WHERE id=1")
            revision = c.execute('SELECT revision FROM paper_readonly WHERE id=1').fetchone()[0]
        reason, fingerprint, masked, snapshot = 'check_failed', None, None, {}
        try:
            fingerprint, masked, snapshot = self._snapshot()
            reason = 'connected_identity_unverified'
        except ReadFailure as exc:
            reason = exc.reason
        except Exception:
            pass  # Do not emit raw exception/request/provider text.
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            previous = c.execute('SELECT * FROM paper_readonly WHERE id=1').fetchone()
            if previous['revision'] != revision:
                raise SafetyError('Read-only check superseded')
            if fingerprint and previous['fingerprint'] and previous['fingerprint'] != fingerprint:
                reason, snapshot = 'identity_changed', {}
            success = reason == 'connected_identity_unverified'
            c.execute('''UPDATE paper_readonly SET ok=?,reason=?,checked_at=?,snapshot=?,
                fingerprint=COALESCE(fingerprint,?),masked_identity=COALESCE(masked_identity,?),identity_first_seen=COALESCE(identity_first_seen,?) WHERE id=1''',
                      (int(success), reason, self.now().timestamp(), json.dumps(snapshot if success else {}),
                       fingerprint if success else None, masked if success else None, self.now().timestamp() if success else None))
        return self.control_status()

    def control_status(self):
        with self.db() as c:
            row = dict(c.execute('SELECT * FROM paper_readonly WHERE id=1').fetchone())
        experiment=None
        renewal=None
        with self.db() as c:
            if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_experiment'").fetchone():
                exp=c.execute('SELECT phase,reason,next_open FROM paper_experiment WHERE id=1').fetchone()
                if exp:experiment=dict(exp)
            if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_renewal'").fetchone():
                exp=c.execute('SELECT phase,reason,next_open FROM paper_renewal WHERE id=1').fetchone()
                if exp:renewal=dict(exp)
        stamp = row['checked_at']
        age = None if stamp is None else self.now().timestamp()-stamp
        fresh = age is not None and 0 <= age <= 30
        return dict(renewal=renewal, experiment=experiment, readonly=True, capable=False, enabled=False, stopped=bool(row['stopped']),
                    revision=row['revision'], checked_at=stamp,
                    checked_at_iso=None if stamp is None else datetime.fromtimestamp(stamp,timezone.utc).isoformat(),
                    check_ok=bool(row['ok'] and fresh), last_check_ok=bool(row['ok']), fresh=fresh,
                    reason=row['reason'], summary=json.loads(row['snapshot']),
                    masked_identity=row['masked_identity'], identity_verified=False, unresolved=0)

    def enable(self, **kwargs):
        raise SafetyError('This production connection is read-only; execution is unavailable')

    def stop(self, *, cancel_open=False):
        if cancel_open:
            raise SafetyError('Read-only connection cannot cancel broker orders')
        with self.db() as c:
            c.execute('UPDATE paper_readonly SET stopped=1,revision=revision+1 WHERE id=1')
        return []


def build_readonly_connection(*, environ=None, database='/var/data/paper-readonly.sqlite', transport_factory=ReadOnlyPaperHTTP, emit=print):
    """Called once at worker startup. Only log fixed aggregate result codes.

    This never reads secrets back through a connector or returns them to a client.
    No startup retry or recurring probe is scheduled.
    """
    env = os.environ if environ is None else environ
    key, secret = env.get('ALPACA_API_KEY'), env.get('ALPACA_SECRET_KEY')
    if not key or not secret:
        emit(json.dumps({'mode':'paper_readonly','result':'credentials_missing'}))
        return None
    if not Path(database).is_absolute() or not Path(database).parent.is_dir():
        emit(json.dumps({'mode':'paper_readonly','result':'storage_unavailable'}))
        return None
    try:
        connection = ReadOnlyPaperConnection(database, transport_factory(key,secret))
        status = connection.check_connection()
        emit(json.dumps({'mode':'paper_readonly','result':status['reason']}))
        return connection
    except Exception:
        emit(json.dumps({'mode':'paper_readonly','result':'initialization_failed'}))
        return None
