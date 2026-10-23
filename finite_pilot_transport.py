"""Finite, anchored capabilities around the reviewed PAPER and source transports.

This module never reads credentials or creates approval. Production authority is
only the compiled, frozen manifest. The default compiled values are None. Test
backend injection is trusted local code, not an authorization mechanism.
"""
from contextlib import contextmanager
from pathlib import Path
import re
import json
import copy
import sqlite3

import finite_pilot_contract as contract
from alpaca_schema import PAPER_ORIGIN
from native_observation_collector import DATA_ORIGIN
from native_paper_execution import NativePaperHTTP, NativePaperDispatcher
from primary_earnings_source import NvidiaReadOnlyHTTP, FEED_URL, release_url


def _require(ok, reason):
    if not ok:
        raise contract.PilotRefused(reason)


@contextmanager
def _read(path):
    """No accidental file creation and no change to the dispatcher's held locks."""
    db = sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True, timeout=0.05)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA trusted_schema=OFF')
        yield db
    finally:
        db.close()


class FinitePilotTransport:
    """Only transport installed in the actual native dispatcher during a pilot.

    The caller owns the seal lock. The native coordinator owns the account lock
    and runner/pipeline/lifecycle write-control locks. Its uncertainty marker is
    already committed before this final boundary journals an attempted write.
    There is no restoration of an unguarded transport after the tick.
    """
    paper_origin = PAPER_ORIGIN
    data_origin = DATA_ORIGIN
    feed = 'iex'

    def __init__(self, key, secret, *, manifest, journal, test_only=False,
                 transport_factory=None):
        contract.require_pilot_manifest(manifest)
        _require(type(test_only) is bool, 'invalid_test_mode')
        _require(test_only or transport_factory is None, 'production_factory_override_forbidden')
        self.manifest, self.journal = manifest, journal
        self.__dispatch = None
        self.__coordinator = None
        self.__pending = None
        factory = NativePaperHTTP if transport_factory is None else transport_factory
        self.__transport = factory(key, secret, enabled=True)
        _require(getattr(self.__transport, 'paper_origin', None) == PAPER_ORIGIN
                 and getattr(self.__transport, 'data_origin', None) == DATA_ORIGIN
                 and getattr(self.__transport, 'feed', None) == 'iex', 'transport_binding')
        self.__native = type(self.__transport) is NativePaperHTTP
        _require(test_only or self.__native, 'reviewed_transport_required')
        if self.__native:
            self.__transport._opener = _FinalOpener(self.__transport._opener, self._open)
            self.__transport._reader._opener = _FinalOpener(self.__transport._reader._opener, self._open)

    def bind(self, coordinator):
        from native_paper_runner import BoundedPaperRunner, NativeMarketSource
        from primary_earnings_source import PrimaryEarningsSource
        _require(self.__dispatch is None and type(coordinator) is BoundedPaperRunner,
                 'single_coordinator_binding_required')
        runner = coordinator.runner
        dispatch = runner.port.dispatcher
        _require(type(dispatch) is NativePaperDispatcher and dispatch.wire.transport is self
                 and type(runner.events) is PrimaryEarningsSource
                 and type(runner.market) is NativeMarketSource
                 and runner.market.port is runner.port, 'reviewed_sources_and_dispatcher_required')
        _require(tuple(dispatch.lifecycle.allowlist) == ('NVDA',)
                 and runner.port.pipeline.config.allowlist == ('NVDA',), 'pilot_universe_mismatch')
        self.__dispatch, self.__coordinator = dispatch, coordinator

    def _bound(self):
        contract.require_pilot_manifest(self.manifest)
        dispatch, coordinator = self.__dispatch, self.__coordinator
        _require(dispatch is not None and coordinator is not None
                 and coordinator.enabled and coordinator.runner.config.enabled
                 and not coordinator.runner.local_failed and dispatch.enabled
                 and dispatch.wire.transport is self, 'pilot_transport_not_bound')
        dispatch.guard.check()
        self.journal.check_stage(self.manifest, 'tick')
        return dispatch, coordinator.runner

    def _native_attempt(self, method, path, body, dispatch, runner):
        """Match the exact committed claim/uncertainty, never caller truth flags."""
        if method == 'POST':
            _require(path == '/v2/orders', 'forbidden_pilot_write_route')
            NativePaperHTTP.validate_payload(body)
            _require(body['symbol'] == 'NVDA', 'pilot_universe_mismatch')
            cid, kind, target = body['client_order_id'], 'submit', body['client_order_id']
        else:
            _require(method == 'DELETE' and body is None and type(path) is str
                     and re.fullmatch(r'/v2/orders/[A-Za-z0-9_-]{1,128}', path),
                     'forbidden_pilot_write_route')
            _require(dispatch.observation is not None, 'fresh_tick_observation_required')
            raw = dispatch.observation.arguments()
            matches = [o for o in raw['orders'] if '/v2/orders/' + o['id'] == path]
            _require(len(matches) == 1, 'owned_cancel_required')
            cid, kind, target = matches[0]['client_order_id'], 'cancel', path
        dispatch.guard.assert_write_allowed()
        dispatch._gate()
        with _read(dispatch.lifecycle.path) as db:
            _require(db.execute('SELECT stopped,attention FROM control').fetchall() == [(0, 0)],
                     'existing_attention_preserved')
            row = db.execute('SELECT state FROM native_dispatch WHERE kind=? AND cid=?',
                             (kind, cid)).fetchall()
            _require(row == [('uncertain',)], 'committed_native_uncertainty_required')
            rows = db.execute('SELECT symbol,side,quantity,price,response FROM intents WHERE cid=?',
                              (cid,)).fetchall()
            _require(len(rows) == 1 and rows[0][0] == 'NVDA', 'owned_native_claim_required')
            if method == 'POST':
                from decimal import Decimal
                symbol, side, quantity, price, response = rows[0]
                _require(response is None and (symbol, side, Decimal(quantity), Decimal(price)) ==
                         (body['symbol'], body['side'], Decimal(body['qty']), Decimal(body['limit_price'])),
                         'native_payload_mismatch')
            else:
                _require(db.execute('SELECT COUNT(*) FROM cancels WHERE cid=?', (cid,)).fetchone() == (1,),
                         'owned_cancel_claim_required')
        with _read(runner.ledger.path) as db:
            rows = db.execute('SELECT dirty,stopped,attention,stage FROM runner_control').fetchall()
            _require(len(rows) == 1 and rows[0][:3] == (1, 0, None)
                     and rows[0][3] in ('submit', 'exit', 'expire'), 'runner_write_control_refused')
        with _read(runner.port.pipeline.database) as db:
            _require(db.execute('SELECT stopped FROM demo_control').fetchall() == [(0,)]
                     and db.execute('SELECT 1 FROM demo_attention LIMIT 1').fetchone() is None,
                     'pipeline_write_control_refused')
        return target

    def _open(self, opener, req, *args, **kwargs):
        # NativePaperHTTP has already validated, encoded and built the Request.
        # Nothing potentially delayed is prepared after the final anchor gate.
        _require(self.__pending is not None, 'pilot_transport_scope_required')
        method, path, body = self.__pending
        origin = DATA_ORIGIN if path.startswith('/v2/stocks/') else PAPER_ORIGIN
        payload = None if body is None else json.dumps(body, sort_keys=True,
                    separators=(',', ':'), allow_nan=False).encode()
        _require(req.get_method() == method and req.full_url == origin + path
                 and req.data == payload, 'prepared_request_mismatch')
        return self._at_wire(lambda: opener.open(req, *args, **kwargs))

    def _at_wire(self, invoke):
        dispatch, runner = self._bound()
        method, path, body = self.__pending
        if method == 'GET':
            return invoke()
        target = self._native_attempt(method, path, body, dispatch, runner)
        # Commits before yield and holds anchor controls over the final clock
        # sample and actual opener.open. Native stop-control locks remain held.
        with self.journal.write_attempt(self.manifest, method, target):
            return invoke()

    def call(self, method, path, body=None):
        self._bound()
        _require(method in ('GET', 'POST', 'DELETE') and self.__pending is None,
                 'forbidden_method_or_reentry')
        _require(method != 'GET' or body is None, 'forbidden_body')
        self.__pending = (method, path, body)
        try:
            if self.__native:
                return self.__transport.call(method, path, body)
            # Canned test backends have no HTTP request construction. Exercise
            # the same final gate immediately around their synthetic wire call.
            return self._at_wire(lambda: self.__transport.call(method, path, body))
        finally:
            self.__pending = None


class FiniteSourceTransport:
    """Every feed/original-HTML GET attempt consumes the anchored finite quota."""
    def __init__(self, *, manifest, journal, guard, test_only=False, transport_factory=None):
        contract.require_pilot_manifest(manifest)
        _require(type(test_only) is bool and (test_only or transport_factory is None),
                 'production_factory_override_forbidden')
        self.manifest, self.journal, self.guard = manifest, journal, guard
        factory = NvidiaReadOnlyHTTP if transport_factory is None else transport_factory
        self.__transport = factory(enabled=True)
        self.__pending = None
        native = type(self.__transport) is NvidiaReadOnlyHTTP
        _require(test_only or native, 'reviewed_source_transport_required')
        if native:
            self.__transport._opener = _FinalOpener(self.__transport._opener, self._open)

    def get(self, url):
        contract.require_pilot_manifest(self.manifest)
        _require(url == FEED_URL or release_url(url) == url, 'forbidden_source_url')
        self.guard.check()
        # Reserve before the attempt, including timeouts and malformed responses.
        self.journal.reserve_source_request(self.manifest)
        _require(self.__pending is None, 'source_reentry_forbidden')
        self.__pending = url
        try:
            return self.__transport.get(url)
        finally:
            self.__pending = None

    def _open(self, opener, req, *args, **kwargs):
        _require(self.__pending is not None and req.full_url == self.__pending
                 and req.get_method() == 'GET' and req.data is None, 'prepared_source_request_mismatch')
        self.guard.check()
        self.journal.check_stage(self.manifest, 'source')
        return opener.open(req, *args, **kwargs)


class RecoveryReadOnlyTransport:
    """No POST/DELETE surface, even with a separately compiled recovery grant."""
    paper_origin = PAPER_ORIGIN
    data_origin = DATA_ORIGIN
    feed = 'iex'

    def __init__(self, key, secret, *, manifest, now_ns, started_ns,
                 test_only=False, transport_factory=None):
        contract.require_recovery_manifest(manifest)
        _require(type(test_only) is bool and (test_only or transport_factory is None),
                 'production_factory_override_forbidden')
        self.manifest, self.now_ns, self.started_ns = manifest, now_ns, started_ns
        self.request_count = 0
        self.__order_scan = []
        self.__pending = None
        factory = NativePaperHTTP if transport_factory is None else transport_factory
        self.__transport = factory(key, secret, enabled=True)
        native = type(self.__transport) is NativePaperHTTP
        _require(test_only or native, 'reviewed_transport_required')
        if native:
            self.__transport._opener = _FinalOpener(self.__transport._opener, self._open)
            self.__transport._reader._opener = _FinalOpener(self.__transport._reader._opener, self._open)

    def _window(self):
        contract.require_recovery_manifest(self.manifest)
        now = self.now_ns()
        _require(type(now) is int and self.manifest.starts_ns <= now < self.manifest.expires_ns
                 and 0 <= now - self.started_ns <= 10 * contract.NS, 'recovery_window_expired')

    def _open(self, opener, req, *args, **kwargs):
        origin = DATA_ORIGIN if self.__pending and self.__pending.startswith('/v2/stocks/') else PAPER_ORIGIN
        _require(self.__pending is not None and req.get_method() == 'GET' and req.data is None
                 and req.full_url == origin + self.__pending, 'recovery_get_only')
        self._window()
        return opener.open(req, *args, **kwargs)

    def call(self, method, path, body=None):
        self._window()
        _require(method == 'GET' and body is None and self.__pending is None, 'recovery_get_only')
        _require(self.request_count < 12, 'recovery_request_cap')
        self.request_count += 1
        self.__pending = path
        try:
            result = self.__transport.call('GET', path)
            from native_observation_collector import ORDER_PATH
            if (path == ORDER_PATH or path.startswith(ORDER_PATH + '&before_order_id=')) and result[0] == 200:
                _require(type(result[1]) is list and len(self.__order_scan) + len(result[1]) <= 500,
                         'bounded_recovery_history_required')
                self.__order_scan.extend(copy.deepcopy(result[1]))
            return result
        finally:
            self.__pending = None

    def legacy_terminal_digests(self, client_ids):
        from alpaca_schema import parse_order
        from native_account_guard import _order_digest
        expected = set(client_ids)
        rows = [(raw['client_order_id'], _order_digest(parse_order(raw), raw))
                for raw in self.__order_scan if raw.get('client_order_id') in expected]
        _require(len(rows) == len(expected) and {cid for cid, _ in rows} == expected,
                 'complete_legacy_terminal_evidence_required')
        return tuple(sorted(rows))


class _FinalOpener:
    """Private boundary invoked only after the reviewed transport builds Request."""
    def __init__(self, opener, gate):
        self.__opener, self.__gate = opener, gate

    def open(self, req, *args, **kwargs):
        return self.__gate(self.__opener, req, *args, **kwargs)
