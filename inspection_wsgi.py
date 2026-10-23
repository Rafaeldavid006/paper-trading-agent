"""Strict inspection-only web entry: no provider calls or persistent state writes.

Existing authenticated read pages and health remain available. Form login,
logout and every research/paper mutation are unavailable in this release.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import os
import sqlite3

from flask import request, session
from event_core import Store
import finite_pilot_contract as finite_contract
from runtime_state_bundle import _private, _database
from webapp import create_app


class ExistingWebStore(Store):
    """Every connection is pinned, readonly, query-only and never auto-created.

    SQLite permits CREATE IF NOT EXISTS in readonly mode only when it is a
    no-op. Missing schemas, unsafe paths or WAL refuse rather than initialize,
    repair or fall back to the ordinary writable Store connection.
    """
    def __init__(self, path):
        self.path = path
        self.clock = lambda: datetime.now(timezone.utc)
        self._identity = _private(path, public_read=True)
        super().__init__(path)

    @contextmanager
    def db(self):
        with _database(self.path, self._identity, public_read=True) as db:
            db.row_factory = sqlite3.Row
            yield db


_store = ExistingWebStore(os.environ['EVENT_DB'])
app = create_app(store=_store, paper_adapter=None)

# These exact handlers were reviewed as local reads. A future/new route fails
# closed until deliberately reviewed; an authentication or internal marker is
# never an exception. In particular discover's POST branch is not available.
READ_ENDPOINTS = frozenset(('health', 'static', 'index', 'historical_library',
                            'paper_status', 'discover', 'show'))
READ_METHODS = frozenset(('GET', 'HEAD', 'OPTIONS'))


def _denied():
    return dict(reason='inspection_route_readonly', order_permission='none',
                order_permission_scope='web_routes'), 405


def _read_handler(endpoint, handler):
    def guarded(*args, **kwargs):
        if endpoint not in READ_ENDPOINTS or request.method not in READ_METHODS:
            return _denied()
        return handler(*args, **kwargs)
    return guarded


for _endpoint, _handler in tuple(app.view_functions.items()):
    app.view_functions[_endpoint] = _read_handler(_endpoint, _handler)


@app.before_request
def inspection_route_gate():
    if request.method not in READ_METHODS or request.endpoint not in READ_ENDPOINTS:
        return _denied()


@app.after_request
def inspection_no_session_mutation(response):
    # Existing signed-cookie verification is unchanged. Read pages do not issue,
    # refresh or delete cookies, including incidental CSRF-cookie modifications.
    # New login/logout would also require DB writes and are explicitly blocked.
    session.modified = False
    return response


def inspection_health():
    return dict(status='ok', mode='inspection_only', account_verified=False,
                execution_ready=False, background_research_paused=True,
                order_permission='none', order_permission_scope='web_routes',
                finite_pilot_approval_compiled=(
                    type(finite_contract.APPROVED_PILOT) is finite_contract.PilotManifest))


app.view_functions['health'] = inspection_health
