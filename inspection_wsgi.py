"""Existing web UI without the legacy startup provider probe or state writes."""
from contextlib import contextmanager
from datetime import datetime, timezone
import os
import sqlite3

from event_core import Store
from runtime_state_bundle import _private, _database
from webapp import create_app


class ExistingWebStore(Store):
    """App setup may only read existing tables; normal user routes stay unchanged.

    SQLite mode=ro permits CREATE IF NOT EXISTS only when it is truly a no-op.
    Missing schema/WAL/unsafe files refuse startup rather than initialize/repair.
    The Store initializer also uses this read-only connection, never a writer.
    """
    def __init__(self, path):
        self.path = path
        self.clock = lambda: datetime.now(timezone.utc)
        self._startup = True
        self._identity = _private(path, public_read=True)
        super().__init__(path)

    @contextmanager
    def db(self):
        if self._startup:
            with _database(self.path, self._identity, public_read=True) as db:
                db.row_factory = sqlite3.Row
                yield db
        else:
            with super().db() as db:
                yield db


_store = ExistingWebStore(os.environ['EVENT_DB'])
app = create_app(store=_store, paper_adapter=None)
_store._startup = False


def inspection_health():
    return dict(status='ok', mode='inspection_only', account_verified=False,
                background_research_paused=True, order_permission='none')


app.view_functions['health'] = inspection_health
