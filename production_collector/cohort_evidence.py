"""Explicit fresh-only evidence schema v4 with collector storage identity.

The vendored v3 record APIs/hash chain are inherited unchanged. This adapter owns
its different application ID, exact schema and immutable cohort marker. It is
not an in-place migration and never recreates a missing database on reopen.
Filesystem anchors rely on the documented cooperating-service-writers model;
these checks are not a hostile-parent SQLite VFS sandbox or rollback detector.
"""
import re

from upstream.storage import durable_store as storage

APPLICATION_ID = 0x45434332
SCHEMA_VERSION = 4
IDENTITY_SCHEMA = 2

TABLE_SQL = {
    'records': '''CREATE TABLE records (
        seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, recorded_at TEXT NOT NULL,
        payload TEXT NOT NULL, previous_hash TEXT NOT NULL, record_hash TEXT NOT NULL UNIQUE)''',
    'blobs': '''CREATE TABLE blobs (
        body_sha256 TEXT PRIMARY KEY, body BLOB NOT NULL)''',
    'versions': '''CREATE TABLE versions (
        version_id TEXT PRIMARY KEY, event_json TEXT NOT NULL)''',
    'observations': '''CREATE TABLE observations (
        record_seq INTEGER PRIMARY KEY REFERENCES records(seq), receipt_json TEXT NOT NULL,
        version_ids_json TEXT NOT NULL, FOREIGN KEY(record_seq) REFERENCES records(seq))''',
    'collector_storage_identity': '''CREATE TABLE collector_storage_identity (
        id INTEGER PRIMARY KEY CHECK(id=1), schema INTEGER NOT NULL, nonce TEXT NOT NULL)''',
}
TRIGGER_SQL = {
    f'immutable_{table}_{operation}':
        f"CREATE TRIGGER immutable_{table}_{operation} BEFORE {operation} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'append_only'); END"
    for table in TABLE_SQL for operation in ('UPDATE', 'DELETE')
}


def _normalized(sql):
    return re.sub(r'\s+', '', sql).rstrip(';').lower()


class GuardedClosingConnection(storage.ClosingConnection):
    """Check retained storage pins at each connection operation/transaction edge.

    close() deliberately remains unconditional so a refused identity can still
    release descriptors and roll back an uncommitted transaction. This guards
    cooperative API use, not arbitrary native cursor/VFS access.
    """
    _storage_guard = None

    def _guard(self):
        if self._storage_guard is not None:
            self._storage_guard()

    def _guarded(self, method, *args, **kwargs):
        self._guard()
        try:
            return method(*args, **kwargs)
        finally:
            self._guard()

    def execute(self, *args, **kwargs):
        return self._guarded(super().execute, *args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._guarded(super().executemany, *args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self._guarded(super().executescript, *args, **kwargs)

    def commit(self):
        return self._guarded(super().commit)

    def rollback(self):
        return self._guarded(super().rollback)

    def __exit__(self, *args):
        # sqlite3's native context manager commits internally without invoking
        # the Python commit override, so bracket it explicitly as well.
        try:
            self._guard()
            result = super().__exit__(*args)
            self._guard()
            return result
        finally:
            self.close()


class CohortEvidenceStore(storage.EvidenceStore):
    def __init__(self, root, *, create, clock=storage.clock_now):
        if type(create) is not bool or type(root.nonce) is not str or not re.fullmatch(r'[0-9a-f]{64}', root.nonce):
            raise ValueError('explicit_cohort_evidence_contract_required')
        self.root, self.path, self.clock = root, root.path / 'evidence.sqlite', clock
        self.root.check_anchors()
        if create:
            self._initialize()
        self._check_identity()

    def _initialize(self):
        # Root has already created this exact private file with O_EXCL. Require
        # its empty preimage; do not adopt or reset any earlier SQLite state.
        self.root.check_anchors()
        if self.path.stat().st_size != 0:
            raise ValueError('fresh_empty_cohort_evidence_required')
        db = self.root.open_database('evidence.sqlite')
        try:
            if (db.execute('PRAGMA application_id').fetchone()[0] != 0
                    or db.execute('PRAGMA user_version').fetchone()[0] != 0
                    or db.execute('SELECT name FROM sqlite_master').fetchone() is not None):
                raise ValueError('fresh_empty_cohort_evidence_required')
            if db.execute('PRAGMA journal_mode=WAL').fetchone()[0].lower() != 'wal':
                raise ValueError('cohort_evidence_wal_required')
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            try:
                for statement in TABLE_SQL.values():
                    db.execute(statement)
                db.execute('INSERT INTO collector_storage_identity VALUES (1, ?, ?)',
                           (IDENTITY_SCHEMA, self.root.nonce))
                for statement in TRIGGER_SQL.values():
                    db.execute(statement)
                db.execute(f'PRAGMA application_id={APPLICATION_ID}')
                db.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
                db.execute('COMMIT')
            except BaseException:
                if db.in_transaction:
                    db.execute('ROLLBACK')
                raise
            self.root.check_anchors()
        finally:
            db.close()

    def _validate_db(self, db):
        if (db.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID
                or db.execute('PRAGMA user_version').fetchone()[0] != SCHEMA_VERSION):
            raise ValueError('cohort_evidence_version_required')
        tables = {name: sql for name, sql in db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        triggers = dict(db.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'"))
        if (set(tables) != set(TABLE_SQL) or set(triggers) != set(TRIGGER_SQL)
                or db.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchone() is not None
                or any(_normalized(tables[name]) != _normalized(sql) for name, sql in TABLE_SQL.items())
                or any(_normalized(triggers[name]) != _normalized(sql) for name, sql in TRIGGER_SQL.items())):
            raise ValueError('cohort_evidence_schema_required')
        rows = db.execute('SELECT id,schema,nonce FROM collector_storage_identity').fetchall()
        if rows != [(1, IDENTITY_SCHEMA, self.root.nonce)]:
            raise ValueError('cohort_evidence_nonce_mismatch')
        if db.execute('PRAGMA journal_mode').fetchone()[0].lower() != 'wal':
            raise ValueError('cohort_evidence_wal_required')

    def _check_identity(self):
        self.root.check_anchors()
        db = self.root.open_database('evidence.sqlite', readonly=True)
        try:
            self._validate_db(db)
            self.root.check_anchors()
        finally:
            db.close()

    def connect(self):
        self._check_identity()
        db = self.root.open_database('evidence.sqlite', factory=GuardedClosingConnection)
        db._storage_guard = self.root.check_anchors
        try:
            # Validate this connection as well, before connection configuration
            # or caller writes; never trust a prior connection's nonce alone.
            self._validate_db(db)
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA synchronous=FULL')
            self.root.check_anchors()
            return db
        except BaseException:
            db.close()
            raise
