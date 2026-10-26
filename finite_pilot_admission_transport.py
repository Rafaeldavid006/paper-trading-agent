"""Exact successor GET-only transport; no order/source dispatch capability."""
import copy
import finite_pilot_contract as contract
def require_admission(manifest):
    from finite_pilot_admission import require_admission as checked
    return checked(manifest)
from finite_pilot_transport import _require, _FinalOpener
from alpaca_schema import PAPER_ORIGIN
from native_observation_collector import DATA_ORIGIN
from native_paper_execution import NativePaperHTTP

class AdmissionReadOnlyTransport:
    """No POST/DELETE surface, even with a separately compiled recovery grant."""
    paper_origin = PAPER_ORIGIN
    data_origin = DATA_ORIGIN
    feed = 'iex'

    def __init__(self, key, secret, *, manifest, now_ns, started_ns,
                 test_only=False, transport_factory=None):
        require_admission(manifest)
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
        require_admission(self.manifest)
        now = self.now_ns()
        _require(type(now) is int and self.manifest.starts_ns <= now < self.manifest.admission.expires_ns
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

