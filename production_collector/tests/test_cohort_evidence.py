"""Disposable synthetic schema tests; no real storage or external requests."""
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from cohort_evidence import CohortEvidenceStore, APPLICATION_ID, SCHEMA_VERSION


class SyntheticRoot:
    def __init__(self, path):
        self.path = path
        self.nonce = 'ab' * 32
        self.calls, self.statements = [], []
        self.file = path / 'evidence.sqlite'
        fd = os.open(self.file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        self.identity = (self.file.stat().st_dev, self.file.stat().st_ino)

    def check_anchors(self):
        info = self.file.lstat()
        if self.file.is_symlink() or (info.st_dev, info.st_ino) != self.identity:
            raise ValueError('synthetic_anchor_changed')

    def open_database(self, name, readonly=False, factory=sqlite3.Connection,
                      isolation_level=None, timeout=2):
        self.check_anchors()
        if name != 'evidence.sqlite':
            raise ValueError('synthetic_fixed_database')
        self.calls.append('ro' if readonly else 'rw')
        db = sqlite3.connect(self.file.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                             uri=True, factory=factory, isolation_level=isolation_level, timeout=timeout)
        db.set_trace_callback(self.statements.append)
        return db


class CohortEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = SyntheticRoot(Path(self.temp.name))

    def create(self):
        return CohortEvidenceStore(self.root, create=True)

    def mutate(self, sql):
        connection = self.root.open_database('evidence.sqlite')
        try:
            connection.executescript(sql)
        finally:
            connection.close()

    def test_fresh_versioned_adapter_and_inherited_verify(self):
        store = self.create()
        with store.connect() as db:
            self.assertEqual(db.execute('PRAGMA application_id').fetchone()[0], APPLICATION_ID)
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(db.execute('SELECT * FROM collector_storage_identity').fetchall(),
                             [(1, 2, self.root.nonce)])
        store.verify()
        self.assertTrue(set(self.root.calls) <= {'rw', 'ro'})

    def test_existing_initialization_never_resets(self):
        self.create()
        before = sha256(self.root.file.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, 'fresh_empty'):
            self.create()
        self.assertEqual(sha256(self.root.file.read_bytes()).hexdigest(), before)

    def test_reopen_has_no_ddl_or_data_changes(self):
        self.create()
        self.root.statements.clear()
        store = CohortEvidenceStore(self.root, create=False)
        with store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 0)
        forbidden = ('CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE', 'REPLACE')
        self.assertFalse(any(s.lstrip().upper().startswith(forbidden) for s in self.root.statements))
        self.assertFalse(any('APPLICATION_ID=' in s.upper() or 'USER_VERSION=' in s.upper()
                             for s in self.root.statements))

    def test_missing_file_never_recreated(self):
        self.create()
        self.root.file.unlink()
        with self.assertRaises((OSError, ValueError, sqlite3.Error)):
            CohortEvidenceStore(self.root, create=False)
        self.assertFalse(self.root.file.exists())

    def test_nonce_mismatch_refused(self):
        self.create()
        self.root.nonce = 'cd' * 32
        with self.assertRaisesRegex(ValueError, 'nonce_mismatch'):
            CohortEvidenceStore(self.root, create=False)

    def test_wrong_version_refused(self):
        self.create()
        self.mutate('PRAGMA user_version=3')
        with self.assertRaisesRegex(ValueError, 'version_required'):
            CohortEvidenceStore(self.root, create=False)

    def test_extra_table_and_removed_trigger_refused(self):
        store = self.create()
        self.mutate('CREATE TABLE unexpected(value TEXT)')
        with self.assertRaisesRegex(ValueError, 'schema_required'):
            store.connect()
        self.mutate('DROP TABLE unexpected; DROP TRIGGER immutable_collector_storage_identity_UPDATE')
        with self.assertRaisesRegex(ValueError, 'schema_required'):
            store.connect()

    def test_nonce_marker_is_immutable(self):
        store = self.create()
        for statement in ("UPDATE collector_storage_identity SET nonce='changed'",
                          'DELETE FROM collector_storage_identity'):
            with store.connect() as db, self.assertRaises(sqlite3.IntegrityError):
                db.execute(statement)
        store.verify()

    def test_upstream_v3_database_not_adopted(self):
        self.mutate('PRAGMA application_id=1163281459; PRAGMA user_version=3; CREATE TABLE marker(a)')
        before = sha256(self.root.file.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, 'version_required'):
            CohortEvidenceStore(self.root, create=False)
        self.assertEqual(sha256(self.root.file.read_bytes()).hexdigest(), before)

    def test_late_anchor_failure_prevents_execute_and_commit(self):
        store = self.create()
        guard = self.root.check_anchors
        state = {'valid': True}
        def conditional_guard():
            if not state['valid']:
                raise ValueError('synthetic_anchor_changed')
            guard()
        self.root.check_anchors = conditional_guard
        db = store.connect()
        try:
            state['valid'] = False
            with self.assertRaisesRegex(ValueError, 'anchor_changed'):
                db.execute("INSERT INTO blobs VALUES ('rejected', X'00')")
            state['valid'] = True
            self.assertEqual(db.execute('SELECT COUNT(*) FROM blobs').fetchone()[0], 0)
            db.execute('BEGIN IMMEDIATE')
            db.execute("INSERT INTO blobs VALUES ('uncommitted', X'00')")
            state['valid'] = False
            with self.assertRaisesRegex(ValueError, 'anchor_changed'):
                db.commit()
        finally:
            # Closing must remain possible after the anchor refusal.
            db.close()
        state['valid'] = True
        with store.connect() as check:
            self.assertEqual(check.execute('SELECT COUNT(*) FROM blobs').fetchone()[0], 0)

    def test_context_manager_failure_rolls_back_and_closes(self):
        store = self.create()
        guard = self.root.check_anchors
        state = {'valid': True}
        def conditional_guard():
            if not state['valid']:
                raise ValueError('synthetic_anchor_changed')
            guard()
        self.root.check_anchors = conditional_guard
        db = store.connect()
        with self.assertRaisesRegex(ValueError, 'anchor_changed'):
            with db:
                db.execute('BEGIN IMMEDIATE')
                db.execute("INSERT INTO blobs VALUES ('uncommitted', X'00')")
                state['valid'] = False
        state['valid'] = True
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute('SELECT 1')
        with store.connect() as check:
            self.assertEqual(check.execute('SELECT COUNT(*) FROM blobs').fetchone()[0], 0)

    def test_connection_revalidated_before_mutable_use(self):
        store = self.create()
        original = self.root.open_database
        def substituted(name, readonly=False, **kw):
            db = original(name, readonly=readonly, **kw)
            if not readonly:
                db.execute('DROP TRIGGER immutable_collector_storage_identity_UPDATE')
            return db
        self.root.open_database = substituted
        with self.assertRaisesRegex(ValueError, 'schema_required'):
            store.connect()


if __name__ == '__main__':
    unittest.main()
