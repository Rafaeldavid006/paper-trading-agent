"""One fixed, opt-in, GET-only native probe. Never imported as a startup action.

Production CLI accepts only --once; paths, generation and account binding cannot
be supplied by a request or environment override. A durable claim is committed
before constructing the transport. Every claimed attempt, including an uncertain
or killed attempt, remains consumed across process restarts. The supervisor must
supply the external 15-second process watchdog. No sleep, retry or order worker.

The legacy ledger lacks complete native Order timestamps, so it cannot supply
invented previous_orders. Legacy payload/fill reconciliation and execution
readiness are explicitly false in every report, even observed_readonly. All original and renewal IDs, including the original
untransmitted reservation, are retained. Their complete durable rows contribute
to a private deterministic revision; strict owned_order_missing is unchanged.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time

from native_observation_collector import (
    CollectionRefused, NativeObservation, NativeObservationCollector,
    NativeReadOnlyHTTP, OwnedLedgerView, _CODES as COLLECTOR_CODES)

SOURCE_DATABASE = '/var/data/paper-readonly.sqlite'
STATE_DATABASE = '/var/data/native-observation.sqlite'
RELEASE_PROBE_ID = 'native-readonly-probe-20261020-g1'
REVIEWED_COLLECTOR_SHA256 = 'fd74ef54f25d4865e5875708d9f08de05af0831747613b58460bdefb0f077936'
ORIGINAL_ID = 'approved-paper-aapl-roundtrip-20261018'
ORIGINAL_APPROVAL = 'owner-approved-one-share-paper-roundtrip-20261018'
ORIGINAL_CUTOFF = datetime(2026, 10, 18, 20, 8, 5, tzinfo=timezone.utc).timestamp()
RENEWAL_ID = 'approved-paper-aapl-renewal-20261019-171346'
RENEWAL_APPROVAL = 'owner-approved-one-renewal-20261019-171346'
RENEWAL_CUTOFF = datetime(2026, 10, 19, 17, 13, 46, tzinfo=timezone.utc).timestamp()
_TABLES = ('paper_readonly', 'safety', 'paper_experiment', 'paper_renewal',
           'intents', 'renewal_intents')
_TERMINAL = frozenset(('filled', 'canceled', 'expired', 'rejected'))
_INTENT_STATES = _TERMINAL | frozenset(('reserved', 'new', 'accepted', 'pending_new',
    'partially_filled', 'pending_cancel', 'accepted_for_bidding'))
_PHASES = frozenset(('waiting', 'buy_reserved', 'buy_working', 'sell_reserved',
    'sell_working', 'completed', 'no_fill', 'blocked', 'manual_attention', 'stopped'))
_STATUS_CODES = COLLECTOR_CODES | frozenset(('probe_disabled', 'invalid_arguments',
    'credentials_missing', 'invalid_binding', 'inconsistent_ledger',
    'source_unavailable', 'state_unavailable', 'probe_state_corrupt',
    'probe_already_claimed', 'probe_failed', 'code_binding_invalid', 'claimed'))


class ProbeRefused(Exception):
    def __init__(self, code):
        self.code = code if type(code) is str and code in _STATUS_CODES else 'probe_failed'
        super().__init__(self.code)


def _require(ok, code='inconsistent_ledger'):
    if not ok:
        raise ProbeRefused(code)


def _client_id(generation, side):
    return 'pa-' + hashlib.sha256((generation + ':' + side).encode()).hexdigest()[:40]


def _finite_time(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def _amount(value):
    _require(type(value) is str and re.fullmatch(r'\d+(?:\.\d{1,9})?', value)
             and len(value) <= 40)
    return Decimal(value)


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _private_regular(path, *, create=False):
    """Reject symlink components and nonprivate/nonregular/foreign-owned files."""
    p = Path(path)
    _require(p.is_absolute() and '..' not in p.parts, 'state_unavailable' if create else 'source_unavailable')
    for parent in reversed(p.parents):
        _require(not parent.is_symlink() and parent.is_dir(), 'state_unavailable' if create else 'source_unavailable')
    flags = os.O_RDWR | os.O_CREAT if create else os.O_RDONLY
    fd = os.open(p, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                 and stat.S_IMODE(info.st_mode) & 0o077 == 0
                 and info.st_nlink == 1, 'state_unavailable' if create else 'source_unavailable')
        _require(p.stat(follow_symlinks=False).st_ino == info.st_ino,
                 'state_unavailable' if create else 'source_unavailable')
        # SQLite journal/WAL sidecars must not redirect writes or reads either.
        for suffix in ('-journal', '-wal', '-shm'):
            sidecar = Path(str(p) + suffix)
            _require(not sidecar.is_symlink(), 'state_unavailable' if create else 'source_unavailable')
            if sidecar.exists():
                side = sidecar.stat(follow_symlinks=False)
                _require(stat.S_ISREG(side.st_mode) and side.st_uid == info.st_uid
                         and stat.S_IMODE(side.st_mode) & 0o077 == 0 and side.st_nlink == 1,
                         'state_unavailable' if create else 'source_unavailable')
        return info
    finally:
        os.close(fd)


@contextmanager
def _source_connection(database):
    c = None
    try:
        info = _private_regular(database)
        c = sqlite3.connect(Path(database).as_uri() + '?mode=ro', uri=True, timeout=0.05)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA query_only=ON')
        c.execute('PRAGMA trusted_schema=OFF')
        after = _private_regular(database)
        _require((info.st_dev, info.st_ino) == (after.st_dev, after.st_ino), 'source_unavailable')
        c.execute('BEGIN')
        yield c
    except ProbeRefused:
        raise
    except Exception:
        raise ProbeRefused('source_unavailable') from None
    finally:
        if c is not None:
            c.close()


def _intent_rows(rows, record, generation, cutoff):
    """Validate the complete durable registry, not an active-only/provider list."""
    _require(len(rows) <= 2)
    by_id = {}
    for row in rows:
        _require(type(row['client_id']) is str and row['client_id'] not in by_id)
        _require(type(row['payload']) is str and len(row['payload']) <= 4096)
        payload = json.loads(row['payload'], object_pairs_hook=_no_duplicate_keys)
        _require(type(payload) is dict and set(payload) == {
            'client_order_id', 'symbol', 'side', 'qty', 'type', 'limit_price',
            'time_in_force', 'extended_hours'})
        side = payload['side']
        _require(side in ('buy', 'sell') and row['client_id'] == _client_id(generation, side)
                 and payload['client_order_id'] == row['client_id']
                 and record[side + '_id'] == row['client_id']
                 and payload['symbol'] == 'AAPL' and payload['type'] == 'limit'
                 and payload['time_in_force'] == 'day' and payload['extended_hours'] is False)
        qty, filled, price = _amount(payload['qty']), _amount(row['filled']), _amount(payload['limit_price'])
        _require(0 < qty <= 1 and (side != 'buy' or qty == 1)
                 and 0 <= filled <= qty and 1 <= price <= 1000
                 and _finite_time(row['created']) and cutoff <= row['created'])
        _require(row['state'] in _INTENT_STATES)
        if row['state'] == 'reserved':
            _require(row['broker_id'] is None and filled == 0)
        else:
            _require(type(row['broker_id']) is str and re.fullmatch(r'[A-Za-z0-9\-]{1,128}', row['broker_id']))
        if row['state'] == 'filled':
            _require(filled == qty)
        if row['state'] in ('new', 'accepted', 'pending_new', 'rejected', 'accepted_for_bidding'):
            _require(filled == 0)
        if row['state'] == 'partially_filled':
            _require(0 < filled < qty)
        by_id[row['client_id']] = (row, payload)
    for side in ('buy', 'sell'):
        cid = record[side + '_id']
        _require(cid is None or (cid == _client_id(generation, side) and cid in by_id))
    if record['sell_id'] is not None:
        _require(record['buy_id'] in by_id)
        buy, _ = by_id[record['buy_id']]
        _, sell_payload = by_id[record['sell_id']]
        _require(buy['state'] in _TERMINAL and _amount(buy['filled']) > 0
                 and _amount(sell_payload['qty']) == _amount(buy['filled']))
    return by_id


def load_owned_ledger(database=SOURCE_DATABASE):
    """One read-only transaction over all binding, control and intent rows.

    The revision includes complete rows (including prior states and payloads),
    sorted deterministically. It is private, never emitted or persisted. No
    provider order scan can add an ID to this registry.
    """
    try:
        with _source_connection(database) as c:
            tables = {}
            for table in _TABLES:
                _require(c.execute("SELECT type FROM sqlite_master WHERE name=?", (table,)).fetchone()[0] == 'table')
                order = 'client_id' if table.endswith('intents') else 'id'
                rows = c.execute('SELECT * FROM ' + table + ' ORDER BY ' + order).fetchmany(5)
                _require(len(rows) <= (2 if table.endswith('intents') else 1))
                tables[table] = [dict(row) for row in rows]
            for table in _TABLES[:4]:
                _require(len(tables[table]) == 1 and tables[table][0]['id'] == 1, 'invalid_binding')
            account, safety, old, renewal = (tables[t][0] for t in _TABLES[:4])
            fp = account['fingerprint']
            _require(type(fp) is str and re.fullmatch('[0-9a-f]{64}', fp), 'invalid_binding')
            _require(_finite_time(account['identity_first_seen'])
                     and account['identity_first_seen'] <= ORIGINAL_CUTOFF
                     and account['identity_first_seen'] <= RENEWAL_CUTOFF, 'invalid_binding')
            _require(type(account['stopped']) is int and account['stopped'] in (0, 1)
                     and type(safety['stopped']) is int and safety['stopped'] in (0, 1))
            for record, generation, approval, cutoff in (
                (old, ORIGINAL_ID, ORIGINAL_APPROVAL, ORIGINAL_CUTOFF),
                (renewal, RENEWAL_ID, RENEWAL_APPROVAL, RENEWAL_CUTOFF)):
                _require(record['experiment'] == generation and record['approval'] == approval
                         and record['fingerprint'] == fp, 'invalid_binding')
                _require(record['phase'] in _PHASES and _finite_time(record['updated'])
                         and record['updated'] >= cutoff
                         and all(type(record[k]) is int and record[k] in (0, 1)
                                 for k in ('cancel_buy', 'cancel_sell')))
            _require(renewal['phase'] in ('completed', 'no_fill', 'blocked', 'manual_attention', 'stopped'))
            # Match the preserved original eligibility gate, without constructing
            # or importing either execution adapter/worker.
            _require(old['phase'] == 'manual_attention' and old['reason'] == 'quote_moved'
                     and old['buy_id'] == _client_id(ORIGINAL_ID, 'buy') and old['sell_id'] is None)
            prior = _intent_rows(tables['intents'], old, ORIGINAL_ID, ORIGINAL_CUTOFF)
            _require(len(prior) == 1 and next(iter(prior.values()))[0]['state'] == 'reserved')
            recent = _intent_rows(tables['renewal_intents'], renewal, RENEWAL_ID, RENEWAL_CUTOFF)
            if renewal['phase'] == 'completed':
                _require(renewal['buy_id'] in recent and renewal['sell_id'] in recent)
                buy, sell = recent[renewal['buy_id']][0], recent[renewal['sell_id']][0]
                _require(buy['state'] in _TERMINAL and sell['state'] in _TERMINAL
                         and _amount(buy['filled']) == _amount(sell['filled']) > 0)
            if renewal['phase'] == 'no_fill':
                _require(renewal['buy_id'] in recent and renewal['sell_id'] is None)
                buy = recent[renewal['buy_id']][0]
                _require(buy['state'] in _TERMINAL and _amount(buy['filled']) == 0)
            ids = tuple(sorted((*prior, *recent)))
            _require(len(ids) == len(set(ids)))
            serialized = json.dumps(tables, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
            revision = int.from_bytes(hashlib.sha256(serialized).digest(), 'big')
            return OwnedLedgerView(fp, revision, ids)
    except ProbeRefused:
        raise
    except Exception:
        raise ProbeRefused('inconsistent_ledger') from None


@contextmanager
def _state_connection(database):
    c = None
    try:
        info = _private_regular(database, create=True)
        c = sqlite3.connect(Path(database).as_uri() + '?mode=rw', uri=True, timeout=0.05)
        c.execute('PRAGMA trusted_schema=OFF')
        c.execute('PRAGMA synchronous=FULL')
        after = _private_regular(database, create=True)
        _require((info.st_dev, info.st_ino) == (after.st_dev, after.st_ino), 'state_unavailable')
        with c:
            yield c
    except ProbeRefused:
        raise
    except Exception:
        raise ProbeRefused('state_unavailable') from None
    finally:
        if c is not None:
            c.close()


def _claim(database, at_ns, code_hash):
    with _state_connection(database) as c:
        c.execute('BEGIN IMMEDIATE')
        existing = c.execute("SELECT type FROM sqlite_master WHERE name='native_probe_claim'").fetchone()
        _require(existing is None or existing == ('table',), 'probe_state_corrupt')
        if existing is None:
            # A missing latch alongside earlier quota/audit or other state is
            # corruption, not permission to replay this fixed release.
            _require(not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").fetchone(),
                     'probe_state_corrupt')
        c.execute('''CREATE TABLE IF NOT EXISTS native_probe_claim (
            id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
            probe_id TEXT NOT NULL, claimed_ns INTEGER NOT NULL,
            code_sha256 TEXT NOT NULL, status TEXT NOT NULL,
            finished_ns INTEGER, identity_match INTEGER,
            requests INTEGER, owned_count INTEGER, order_count INTEGER)''')
        rows = c.execute('SELECT id,version,probe_id FROM native_probe_claim').fetchall()
        if rows:
            _require(rows == [(1, 1, RELEASE_PROBE_ID)], 'probe_state_corrupt')
            return False
        _require(existing is None, 'probe_state_corrupt')
        c.execute('INSERT INTO native_probe_claim(id,version,probe_id,claimed_ns,code_sha256,status) VALUES(1,1,?,?,?,?)',
                  (RELEASE_PROBE_ID, at_ns, code_hash, 'claimed'))
    return True  # The transaction has committed before any transport construction.


def _finish(database, result):
    with _state_connection(database) as c:
        c.execute('BEGIN IMMEDIATE')
        n = c.execute('''UPDATE native_probe_claim SET status=?,finished_ns=?,identity_match=?,
            requests=?,owned_count=?,order_count=? WHERE id=1 AND version=1 AND probe_id=? AND status='claimed' ''',
            (result['status'], result['at_ns'], 1 if result.get('identity_match') is True else None,
             result.get('requests'), result.get('owned_count'), result.get('order_count'), RELEASE_PROBE_ID)).rowcount
        _require(n == 1, 'probe_state_corrupt')


def _code_hash():
    worker = Path(__file__).read_bytes()
    collector = Path(__file__).with_name('native_observation_collector.py').read_bytes()
    _require(hashlib.sha256(collector).hexdigest() == REVIEWED_COLLECTOR_SHA256, 'code_binding_invalid')
    return hashlib.sha256(worker + b'\0' + collector).hexdigest()


def _timestamp(now_ns):
    value = now_ns()
    _require(type(value) is int and 0 < value <= 2**63 - 1, 'invalid_local_clock')
    return value


def run_once(*, enabled=False, environ=None, source_database=SOURCE_DATABASE,
             state_database=STATE_DATABASE, transport_factory=NativeReadOnlyHTTP,
             collector_factory=NativeObservationCollector, now_ns=time.time_ns,
             monotonic_ns=time.monotonic_ns, emit=print):
    """Explicit callable seam; only tests inject paths/factories. Never retries.

    Public output contains only allowlisted status, local time, combined source
    code hash, proven identity match and bounded aggregate counts. A failure may
    omit unknown facts; false must never mean merely unverified.
    """
    result = dict(mode='native_readonly_probe', order_permission='none', executable=False,
                  execution_ready=False, legacy_state_reconciled=False)
    collector, claimed = None, False
    try:
        result['at_ns'] = _timestamp(now_ns)
        result['code_sha256'] = _code_hash()
        _require(type(enabled) is bool, 'invalid_enable')
        if not enabled:
            raise ProbeRefused('probe_disabled')
        env = os.environ if environ is None else environ
        key, secret = env.get('ALPACA_API_KEY'), env.get('ALPACA_SECRET_KEY')
        _require(bool(key) and bool(secret), 'credentials_missing')
        _require(all(type(x) is str and 0 < len(x) <= 512
                     and all(33 <= ord(ch) <= 126 for ch in x) for x in (key, secret)),
                 'invalid_injected_credentials')
        _require(Path(source_database) != Path(state_database), 'state_unavailable')
        initial = load_owned_ledger(source_database)
        # Re-open and validate the whole independent registry on each collector
        # boundary. A pre-request change refuses before constructing transport.
        _require(load_owned_ledger(source_database) == initial, 'inconsistent_ledger')
        claimed = _claim(state_database, result['at_ns'], result['code_sha256'])
        _require(claimed, 'probe_already_claimed')
        _require(load_owned_ledger(source_database) == initial, 'inconsistent_ledger')
        transport = transport_factory(key, secret, enabled=True)
        def bound_ledger():
            current = load_owned_ledger(source_database)
            _require(current == initial, 'inconsistent_ledger')
            return current
        collector = collector_factory(database=state_database, transport=transport,
            expected_fingerprint=initial.account_fingerprint,
            ledger_view=bound_ledger, enabled=True,
            now_ns=now_ns, monotonic_ns=monotonic_ns)
        observation = collector.collect()
        _require(type(observation) is NativeObservation, 'probe_failed')
        result['status'] = 'observed_readonly'
        # No call to native_arguments; raw provider JSON is never persisted.
    except (ProbeRefused, CollectionRefused) as exc:
        code = exc.code
        result['status'] = code if type(code) is str and code in _STATUS_CODES else 'probe_failed'
    except Exception:
        result['status'] = 'probe_failed'
    # The actual collector increments request two only AFTER parsing and binding
    # the account. closed_session therefore truthfully proves that identity. An
    # injected exception on the first GET or an arbitrary factory does not.
    if type(collector) is NativeObservationCollector:
        requests = getattr(collector, '_requests', None)
        if type(requests) is int and 0 <= requests <= 12:
            result['requests'] = requests
            if requests >= 2:
                result['identity_match'] = True
        for name, field, maximum in (('_owned_count', 'owned_count', 4), ('_order_count', 'order_count', 499)):
            value = getattr(collector, name, None)
            if type(value) is int and 0 <= value <= maximum:
                # A zero order count before the scan is unknown, not no orders.
                if (field == 'owned_count' and value > 0) or (field == 'order_count' and getattr(collector, '_pages', 0) > 0):
                    result[field] = value
    try:
        result['at_ns'] = _timestamp(now_ns)
        if claimed:
            _finish(state_database, result)
    except Exception:
        # Do not erase the earlier durable claim, even if final audit fails.
        result['status'] = 'state_unavailable'
    emit(json.dumps(result, sort_keys=True, separators=(',', ':')))
    return result


def main(argv=None, *, emit=print):
    args = sys.argv[1:] if argv is None else argv
    if args not in ([], ['--once']):
        # Do not let argparse echo possibly private unrecognized arguments.
        emit(json.dumps(dict(mode='native_readonly_probe', order_permission='none', executable=False,
                             execution_ready=False, legacy_state_reconciled=False, status='invalid_arguments'), separators=(',', ':')))
        return 2
    result = run_once(enabled=args == ['--once'], emit=emit)
    return 0 if result['status'] in ('probe_disabled', 'probe_already_claimed',
                                    'observed_readonly', 'closed_session') else 1


if __name__ == '__main__':
    raise SystemExit(main())
