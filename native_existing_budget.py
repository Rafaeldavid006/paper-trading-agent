"""Existing-state collector quota for a verified, exclusively held cohort.

Only the storage boundary changes: SharedRequestBudget still owns reservation
and audit SQL. There is no initialization, chmod, reset or alternate identity.
Every operation verifies the full cohort and physical file before and after it.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import sqlite3
import time

from native_observation_collector import SharedRequestBudget, CollectionRefused, require
from runtime_state_bundle import RuntimeStateBundle, _private, _SIDECARS


class ExistingStateRequestBudget(SharedRequestBudget):
    def __init__(self, database, guard):
        from native_account_guard import SharedAccountGuard
        require(type(guard) is SharedAccountGuard, 'budget_storage_unavailable')
        bundle = guard._runtime_bundle
        require(type(bundle) is RuntimeStateBundle and guard._held,
                'budget_storage_unavailable')
        require(str(database) == guard.state_database == bundle.paths['guard'],
                'invalid_database_file')
        self.database, self.guard, self.bundle = str(database), guard, bundle
        self._failed = False
        self._audit_rows = set()
        self._check()
        self._identity = bundle._pinned['guard']
        # Validate exact existing schema/state before even a reserved-ID lookup.
        # This inspection never reserves, prunes, changes time, or creates state.
        with self.db() as db:
            schemas = {
                'native_read_budget': [('id', 'INTEGER', 0, None, 1),
                    ('version', 'INTEGER', 1, None, 0), ('high_ns', 'INTEGER', 1, None, 0)],
                'native_read_attempts': [('at_ns', 'INTEGER', 1, None, 0)],
                'native_read_audit': [('at_ns', 'INTEGER', 1, None, 0),
                    ('status', 'TEXT', 1, None, 0), ('attempts', 'INTEGER', 1, None, 0),
                    ('owned_count', 'INTEGER', 1, None, 0), ('order_count', 'INTEGER', 1, None, 0),
                    ('digest', 'TEXT', 0, None, 0)],
            }
            for table, columns in schemas.items():
                require([r[1:6] for r in db.execute('PRAGMA table_info(' + table + ')')] == columns,
                        'budget_state_corrupt')
            meta = db.execute('SELECT id,version,high_ns FROM native_read_budget').fetchall()
            require(len(meta) == 1 and meta[0][:2] == (1, 1) and type(meta[0][2]) is int
                    and 0 <= meta[0][2] <= 2**63 - 1, 'budget_state_corrupt')
            times = [r[0] for r in db.execute('SELECT at_ns FROM native_read_attempts')]
            require(len(times) <= 12 and all(type(t) is int and 0 < t <= meta[0][2] for t in times),
                    'budget_state_corrupt')

    def _check(self):
        require(not self._failed and self.guard._runtime_bundle is self.bundle,
                'budget_storage_unavailable')
        # guard.check also proves same-thread ownership and full-bundle context.
        self.guard.check()
        self.bundle.verify()
        require(_private(self.database) == self.bundle._pinned['guard'], 'invalid_database_file')
        if hasattr(self, '_identity'):
            require(self.bundle._pinned['guard'] == self._identity, 'invalid_database_file')

    @contextmanager
    def db(self):
        db = None
        try:
            self._check()
            require(not any(os.path.lexists(self.database + suffix) for suffix in _SIDECARS),
                    'budget_storage_unavailable')
            db = sqlite3.connect(Path(self.database).as_uri() + '?mode=rw',
                                 uri=True, timeout=0.05)
            db.execute('PRAGMA trusted_schema=OFF')
            db.execute('PRAGMA temp_store=MEMORY')
            db.execute('PRAGMA synchronous=FULL')
            # Full verification uses separate read-only connections while this
            # rollback transaction is open. Do not spill into exclusive locking.
            db.execute('PRAGMA cache_spill=OFF')
            deadline = time.monotonic() + 2
            db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            with db:
                self._check()
                yield db
                self._check()
            self._check()
        except CollectionRefused as exc:
            if exc.code not in ('shared_request_budget_exhausted', 'budget_clock_regression'):
                self._failed = True
            raise
        except Exception:
            # A commit or postcommit integrity error has an uncertain outcome.
            # Poison this collection's adapter; never retry/refund or append a
            # second audit after a possibly committed first audit.
            self._failed = True
            raise CollectionRefused('budget_storage_unavailable') from None
        finally:
            if db is not None:
                db.close()

    def audit(self, *, rowid=None, **values):
        require(rowid is None or rowid in self._audit_rows, 'budget_state_corrupt')
        result = super().audit(rowid=rowid, **values)
        self._audit_rows.add(result)
        return result
