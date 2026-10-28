"""Pinned existing-anchor journal for a DEFAULT DISABLED finite local candidate.

No new database, credentials, caller grants, implicit enrollment or reset API.
Normal worker stages hold the existing seal lock and account guard. Recovery
owns those locks itself. Prepared and runner CAS are distinct commits: any
uncertain interval remains startup-blocking and is NEVER retried automatically.
"""
from collections import Counter
from contextlib import ExitStack, contextmanager
import hashlib
import json
import math
import os
import re
import sqlite3
import time

import finite_pilot_contract as contract
from finite_pilot_contract import PilotRefused, require, digest, NS
import runtime_state_bundle as inventory

PREFIX = 'finite_pilot_'
SCHEMA = {
    'finite_pilot_binding': ('CREATE TABLE finite_pilot_binding(id INTEGER PRIMARY KEY CHECK(id=1),'
        'version INTEGER NOT NULL,account TEXT NOT NULL,generation TEXT NOT NULL,seal TEXT NOT NULL,'
        'anchor_device INTEGER NOT NULL,anchor_inode INTEGER NOT NULL,founding_anchor TEXT NOT NULL,'
        'founding_cohort TEXT NOT NULL,inspection_manifest TEXT NOT NULL,inspection_receipt TEXT,'
        'pilot_digest TEXT,recovery_digest TEXT)'),
    'finite_pilot_enrollment': ('CREATE TABLE finite_pilot_enrollment(id INTEGER PRIMARY KEY CHECK(id=1),'
        'digest TEXT NOT NULL,document TEXT NOT NULL)'),
    'finite_pilot_control': ('CREATE TABLE finite_pilot_control(id INTEGER PRIMARY KEY CHECK(id=1),'
        'ticks INTEGER NOT NULL,writes INTEGER NOT NULL,sources INTEGER NOT NULL,dirty INTEGER NOT NULL,'
        'last_ns INTEGER NOT NULL,last_tick_ns INTEGER NOT NULL,last_source_ns INTEGER NOT NULL,'
        'stage TEXT NOT NULL,seq INTEGER NOT NULL)'),
    'finite_pilot_stages': ('CREATE TABLE finite_pilot_stages(seq INTEGER PRIMARY KEY,stage TEXT NOT NULL,'
        'started_ns INTEGER NOT NULL,finished_ns INTEGER)'),
    'finite_pilot_writes': ('CREATE TABLE finite_pilot_writes(method TEXT NOT NULL,target TEXT NOT NULL,'
        'at_ns INTEGER NOT NULL,stage_seq INTEGER NOT NULL,PRIMARY KEY(method,target))'),
    'finite_pilot_sources': ('CREATE TABLE finite_pilot_sources(seq INTEGER PRIMARY KEY,'
        'at_ns INTEGER NOT NULL,stage_seq INTEGER NOT NULL)'),
    'finite_pilot_recovery': ('CREATE TABLE finite_pilot_recovery(id INTEGER PRIMARY KEY CHECK(id=1),'
        'digest TEXT NOT NULL,document TEXT NOT NULL)'),
    'finite_pilot_recovery_journal': ('CREATE TABLE finite_pilot_recovery_journal('
        'seq INTEGER PRIMARY KEY CHECK(seq IN (1,2)),incident TEXT NOT NULL,status TEXT NOT NULL,'
        'at_ns INTEGER NOT NULL,manifest_digest TEXT NOT NULL,old_row TEXT NOT NULL,new_row TEXT NOT NULL,'
        'protected_digest TEXT NOT NULL,guard_digest TEXT NOT NULL,evidence_digest TEXT)'),
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _bounded(db, sql, args=(), limit=20000):
    rows = db.execute(sql, args).fetchmany(limit + 1)
    require(len(rows) <= limit, 'anchor_work_limit')
    return [tuple(r) for r in rows]


def _schema(db, *, absent=False):
    rows = _bounded(db, "SELECT type,name,tbl_name,sql FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY name")
    from finite_recovery_successor_contract import TABLE, SQL
    from finite_pilot_admission_schema import TABLE as ADMISSION_TABLE, SQL as ADMISSION_SQL
    from finite_pilot_session_schema import SCHEMA as SESSION_SCHEMA
    from finite_root_contract import SCHEMA as ROOT_SCHEMA
    ours = [r for r in rows if r[1].startswith(PREFIX) or r[2] in SCHEMA or r[2] in (TABLE,ADMISSION_TABLE) or r[2] in SESSION_SCHEMA or r[2] in ROOT_SCHEMA]
    if not ours and absent:
        return False
    expected = dict(SCHEMA)
    if any(r[1] == TABLE for r in ours):
        expected[TABLE] = SQL
    if any(r[1] == ADMISSION_TABLE for r in ours):
        expected[ADMISSION_TABLE] = ADMISSION_SQL
    from finite_pilot_session_schema import SCHEMA as SESSION_SCHEMA
    present={r[1] for r in ours} & set(SESSION_SCHEMA)
    require(not present or present==set(SESSION_SCHEMA),'session_schema_incomplete')
    if present:expected.update(SESSION_SCHEMA)
    roots={r[1] for r in ours} & set(ROOT_SCHEMA)
    require(not roots or roots==set(ROOT_SCHEMA),'root_schema_incomplete')
    if roots:expected.update(ROOT_SCHEMA)
    require(ours == [('table', name, name, sql) for name, sql in sorted(expected.items())],
            'anchor_journal_schema_invalid')
    require(db.execute('PRAGMA quick_check(1)').fetchall() == [('ok',)], 'anchor_journal_corrupt')
    return True


def _manifest_row(db, table, manifest):
    rows = _bounded(db, 'SELECT * FROM ' + table, limit=1)
    require(rows == [(1, digest(manifest.document()), _json(manifest.document()))],
            'anchor_manifest_changed')


def _eligible_runner(row):
    require(type(row) in (tuple, list) and len(row) == 6 and type(row[0]) is int and row[0] == 1
            and type(row[1]) in (int, float) and math.isfinite(row[1]) and row[1] > 0
            and type(row[2]) is int and row[2] == 1 and type(row[3]) is int and row[3] == 0
            and tuple(row[4:]) == ('readonly_runtime_stage_failed', 'started'),
            'exact_incident_runner_required')
    return tuple(row)


def _recovery_gate(db):
    pilot=contract.APPROVED_PILOT
    if type(pilot) is contract.PilotManifest and pilot.admission is not None:
        from finite_pilot_admission import admission_gate
        admission_gate(db,pilot)
        return
    rows = _bounded(db, 'SELECT * FROM finite_pilot_recovery', limit=1)
    records = _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal ORDER BY seq', limit=2)
    if not rows:
        require(not records, 'recovery_journal_orphaned')
        return
    manifest = contract.require_recovery_manifest(contract.APPROVED_RECOVERY)
    _manifest_row(db, 'finite_pilot_recovery', manifest)
    require(len(records) == 2, 'recovery_incomplete')
    before, after = records
    require(before[0] == 1 and before[2] == 'prepared' and after[0] == 2 and after[2] == 'applied'
            and before[1] == after[1] == manifest.incident_id
            and before[4] == after[4] == digest(manifest.document())
            and before[5] == after[5]
            and digest(_eligible_runner(json.loads(before[5]))) == manifest.original_runner_sha256
            and before[6] == after[6] and before[7] == after[7] == manifest.protected_state_sha256
            and contract._hash(before[8]) and contract._hash(after[8])
            and before[9] is None and contract._hash(after[9])
            and type(before[3]) is int and type(after[3]) is int
            and manifest.starts_ns <= before[3] <= after[3] < manifest.expires_ns,
            'recovery_receipt_invalid')
    new = json.loads(before[6])
    require(new == [1, before[3] / NS, 0, 0, None, 'recovered_readonly']
            and new[1] >= json.loads(before[5])[1], 'recovery_receipt_invalid')


def read_only_startup_gate(bundle):
    """Legacy stage gate, including BEFORE initialize_once. Never creates.

    No journal and no compiled grants preserves the pre-existing readonly first
    enrollment path. Any partial journal closes; a pilot owns the cohort and
    excludes the old independently scheduled source/broker pollers entirely.
    """
    require(type(bundle) is inventory.RuntimeStateBundle, 'typed_bundle_required')
    identity = inventory._private(bundle.paths['anchor'], public_read=True)
    with inventory._database(bundle.paths['anchor'], identity, public_read=True) as db:
        present = _schema(db, absent=True)
        inspection_present = db.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'finite_inspection_%' LIMIT 1").fetchone() is not None
    if not present:
        require(not inspection_present, 'foundation_incomplete')
        require(contract.APPROVED_PILOT is None and contract.APPROVED_RECOVERY is None,
                'approved_journal_missing')
        return True
    bundle.verify()
    journal = AnchorJournal(bundle, time.time_ns)
    journal._verify()
    with journal._db() as db:
        journal._binding_row(db)
        require(not _bounded(db, 'SELECT * FROM finite_pilot_enrollment', limit=1),
                'pilot_owns_runtime')
        require(not _bounded(db, 'SELECT * FROM finite_pilot_control', limit=1)
                and not _bounded(db, 'SELECT * FROM finite_pilot_stages', limit=1)
                and not _bounded(db, 'SELECT * FROM finite_pilot_writes', limit=1)
                and not _bounded(db, 'SELECT * FROM finite_pilot_sources', limit=1),
                'anchor_pilot_orphaned')
        _recovery_gate(db)
    return True


def resolve_approved_bundle(manifest, *, paths=None, test_only=False):
    """Private account lookup against an already approved seal/foundation.

    Never returns a public proof, creates a manifest, reads credentials, or
    permits path overrides in production. Account identity stays inside the
    private verified bundle rather than in public hash-only inspection output.
    """
    if type(manifest) is contract.PilotManifest:
        contract.require_pilot_manifest(manifest)
    else:
        contract.require_recovery_manifest(manifest)
    require(type(test_only) is bool and (test_only or paths is None), 'fixed_paths_required')
    resolved = inventory.FIXED_PATHS if paths is None else paths
    path = resolved['seal']
    identity = inventory._private(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        stat = os.fstat(fd)
        require((stat.st_dev, stat.st_ino) == identity, 'manifest_seal_mismatch')
        raw = os.read(fd, 4097)
    finally:
        os.close(fd)
    require(len(raw) <= 4096 and hashlib.sha256(raw).hexdigest() == manifest.bundle_seal_sha256,
            'manifest_seal_mismatch')
    seal = inventory._load(raw)
    require(type(seal) is dict and seal.get('generation') == manifest.bundle_generation
            and contract._hash(seal.get('fingerprint')), 'manifest_seal_mismatch')
    bundle = inventory.RuntimeStateBundle(seal['fingerprint'], paths=paths, test_only=test_only)
    bundle.verify()
    require(bundle._pinned['seal'] == identity, 'manifest_seal_mismatch')
    journal = AnchorJournal(bundle)
    journal._verify(manifest)
    with journal._db() as db:
        journal._binding_row(db, manifest)
    return bundle


class AnchorJournal:
    def __init__(self, bundle, now_ns=time.time_ns):
        require(type(bundle) is inventory.RuntimeStateBundle and callable(now_ns), 'typed_bundle_required')
        self.bundle, self.now_ns = bundle, now_ns
        self._identity = None

    def _verify(self, manifest=None):
        self.bundle.verify()
        identity = self.bundle._pinned['anchor']
        require(self._identity is None or self._identity == identity, 'anchor_replaced')
        self._identity = identity
        if manifest is not None:
            require(manifest.bundle_generation == inventory.GENERATION,
                    'manifest_bundle_mismatch')
            raw = self.bundle._read_seal(self.bundle._pinned['seal'])
            require(self._seal_digest() == manifest.bundle_seal_sha256
                    and raw['generation'] == manifest.bundle_generation, 'manifest_seal_mismatch')

    @contextmanager
    def _db(self, *, write=False):
        require(self._identity is not None, 'anchor_not_verified')
        with inventory._database(self.bundle.paths['anchor'], self._identity,
                                 write=write, public_read=True) as db:
            yield db

    def _seal_digest(self):
        path = self.bundle.paths['seal']
        identity = self.bundle._pinned['seal']
        require(inventory._private(path) == identity, 'manifest_seal_mismatch')
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            stat = os.fstat(fd)
            require((stat.st_dev, stat.st_ino) == identity, 'manifest_seal_mismatch')
            raw = os.read(fd, 4097)
            require(len(raw) <= 4096, 'manifest_seal_mismatch')
        finally:
            os.close(fd)
        require(inventory._private(path) == identity, 'manifest_seal_mismatch')
        return hashlib.sha256(raw).hexdigest()

    def _anchor_creation_digest(self):
        # Called only while BEGIN IMMEDIATE excludes the application writer.
        # Pinning reviewed pre-enrollment bytes prevents dropping all new tables
        # from looking like first enrollment. Full coherent rollback remains a
        # distinct unsolved local-continuity threat, never automatic recovery.
        path = self.bundle.paths['anchor']
        require(inventory._private(path, public_read=True) == self._identity, 'anchor_replaced')
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            stat = os.fstat(fd)
            require((stat.st_dev, stat.st_ino) == self._identity and stat.st_size <= 64 * 1024 * 1024,
                    'anchor_creation_snapshot_invalid')
            result = hashlib.sha256()
            remaining = stat.st_size
            while remaining:
                block = os.read(fd, min(65536, remaining))
                require(bool(block), 'anchor_creation_snapshot_invalid')
                result.update(block)
                remaining -= len(block)
            require(not os.read(fd, 1), 'anchor_creation_snapshot_invalid')
        finally:
            os.close(fd)
        require(inventory._private(path, public_read=True) == self._identity, 'anchor_replaced')
        return result.hexdigest()

    def _binding(self):
        # Full verification above pins current physical files; only durable
        # identity fields use the already verified migration enrollment.
        return (1, 1, self.bundle.binding, inventory.GENERATION,
                self._seal_digest(), *self.bundle._reference_identity('anchor'))

    def _binding_row(self, db, manifest=None):
        _schema(db)
        if manifest is not None and type(manifest) is contract.PilotManifest:
            from finite_pilot_session_schema import SESSIONS
            present=db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(SESSIONS,)).fetchone()
            require(not present or manifest.session_admission is not None or manifest.root_binding is not None,'legacy_pilot_fenced')
            from finite_root_contract import ROOTS
            newer=db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(ROOTS,)).fetchone()
            require(not newer or manifest.root_binding is not None,'v1_pilot_fenced')
        rows = _bounded(db, 'SELECT * FROM finite_pilot_binding', limit=1)
        require(len(rows) == 1 and rows[0][:7] == self._binding(), 'anchor_binding_changed')
        require(contract._hash(rows[0][7]) and contract._hash(rows[0][8]), 'anchor_founding_binding_invalid')
        if manifest is not None:
            require(rows[0][7:9] == (manifest.anchor_pre_enrollment_sha256, manifest.pre_enrollment_cohort_sha256),
                    'anchor_founding_binding_changed')
        require(contract._hash(rows[0][9]) and contract._hash(rows[0][10]), 'foundation_incomplete')
        receipt = self._inspection_receipt(db)
        require(receipt[2] == rows[0][9] and digest(receipt) == rows[0][10], 'foundation_receipt_changed')
        for index, table in ((11, 'finite_pilot_enrollment'), (12, 'finite_pilot_recovery')):
            marker = rows[0][index]
            enrolled = _bounded(db, 'SELECT * FROM ' + table, limit=1)
            require((marker is None and not enrolled) or
                    (contract._hash(marker) and len(enrolled) == 1 and enrolled[0][1] == marker),
                    'anchor_enrollment_rows_missing')
            if enrolled:
                document = json.loads(enrolled[0][2])
                require(digest(document) == marker and (document.get('anchor_pre_enrollment_sha256'),
                        document.get('pre_enrollment_cohort_sha256')) == rows[0][7:9],
                        'anchor_founding_binding_changed')
        if rows[0][11] is None:
            for table in ('finite_pilot_control', 'finite_pilot_stages', 'finite_pilot_writes', 'finite_pilot_sources'):
                require(not _bounded(db, 'SELECT * FROM ' + table, limit=1), 'anchor_pilot_orphaned')
        if rows[0][12] is None:
            require(not _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal', limit=2),
                    'recovery_journal_orphaned')

    def _now(self, manifest, previous=0, *, active=True):
        now = self.now_ns()
        require(contract._ns(now) and now >= previous, 'pilot_clock_regression')
        require(now < manifest.expires_ns and (not active or now >= manifest.starts_ns),
                'pilot_window_closed')
        return now

    def _inspection_receipt(self, db):
        import finite_pilot_inspection as inspection
        objects = _bounded(db, "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE "
                           "name LIKE 'finite_inspection_%' OR tbl_name=? ORDER BY name", (inspection.TABLE,))
        require(objects == [('table', inspection.TABLE, inspection.TABLE, inspection.SQL)],
                'foundation_receipt_schema_invalid')
        rows = _bounded(db, 'SELECT * FROM ' + inspection.TABLE, limit=1)
        require(len(rows) == 1 and rows[0][0] == 1 and rows[0][1] == contract.INSPECTION_GENERATION
                and contract._hash(rows[0][2]) and rows[0][3] == 'complete'
                and contract._ns(rows[0][4]) and contract._ns(rows[0][5])
                and rows[0][4] <= rows[0][5]
                and rows[0][6:] == self.bundle._reference_identity('anchor'),
                'foundation_incomplete')
        return rows[0]

    def _caller_anchor(self, db):
        require(type(db) is sqlite3.Connection and db.in_transaction, 'foundation_locked_anchor_required')
        require(db.execute('PRAGMA database_list').fetchall() == [(0, 'main', self.bundle.paths['anchor'])],
                'foundation_locked_anchor_required')

    def initialize_foundation(self, manifest, *, anchor_db):
        """Explicit approved metadata initialization, not a trading grant.

        The fixed proof worker owns seal/account/all role writer locks and its
        committed one-shot claim. Schema+binding+completed receipt finalize in
        one subsequent caller transaction; a prior claimed receipt survives any
        interruption. Normal enrollment NEVER invokes this method.
        """
        contract.require_inspection_manifest(manifest)
        require(contract.APPROVED_PILOT is None and contract.APPROVED_RECOVERY is None,
                'inspection_exclusive_scope_required')
        self._verify()
        self._caller_anchor(anchor_db)
        import finite_pilot_inspection as inspection
        receipt = inspection._receipt(anchor_db, self.bundle, manifest)
        require(receipt is not None and receipt[3] == 'claimed', 'foundation_claim_required')
        require(not _schema(anchor_db, absent=True), 'foundation_already_initialized')
        require(manifest.bundle_generation == inventory.GENERATION, 'manifest_bundle_mismatch')
        founding_anchor = self._anchor_creation_digest()
        founding_cohort = self.cohort_state_digest()
        for sql in SCHEMA.values():
            anchor_db.execute(sql)
        anchor_db.execute('INSERT INTO finite_pilot_binding VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (*self._binding(), founding_anchor, founding_cohort, digest(manifest.document()), None, None, None))
        return True

    def finalize_foundation(self, manifest, *, anchor_db):
        contract.require_inspection_manifest(manifest)
        self._caller_anchor(anchor_db)
        _schema(anchor_db)
        rows = _bounded(anchor_db, 'SELECT * FROM finite_pilot_binding', limit=1)
        require(len(rows) == 1 and rows[0][:7] == self._binding()
                and rows[0][9] == digest(manifest.document()) and rows[0][10:] == (None, None, None),
                'foundation_already_or_partially_finalized')
        receipt = self._inspection_receipt(anchor_db)
        require(receipt[2] == rows[0][9], 'foundation_receipt_changed')
        require(self.cohort_state_digest() == rows[0][8], 'foundation_cohort_changed')
        anchor_db.execute('UPDATE finite_pilot_binding SET inspection_receipt=? WHERE id=1 AND inspection_receipt IS NULL',
                          (digest(receipt),))
        return True

    def founding_proofs(self):
        self._verify()
        with self._db() as db:
            self._binding_row(db)
            row = _bounded(db, 'SELECT * FROM finite_pilot_binding', limit=1)[0]
        return dict(anchor_pre_enrollment_sha256=row[7], pre_enrollment_cohort_sha256=row[8])

    def _pilot_initial_state(self, db, manifest):
        # The founding snapshot alone cannot distinguish an anchor restored to
        # the interval after recovery but before pilot enrollment. Bind the
        # first pilot to either the untouched founding cohort or the EXACT
        # journaled recovery outcome, including its unpredictable GET counters.
        if self.cohort_state_digest() == manifest.pre_enrollment_cohort_sha256:
            return
        records = _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal ORDER BY seq', limit=2)
        require(len(records) == 2, 'pilot_initial_cohort_changed')
        _recovery_gate(db)
        recovery = contract.require_recovery_manifest(contract.APPROVED_RECOVERY)
        require(self.protected_state_digest() == recovery.protected_state_sha256,
                'pilot_initial_cohort_changed')
        with inventory._database(self.bundle.paths['runner'], self.bundle._pinned['runner']) as runner:
            require(inventory._singleton(runner, 'runner_control') == tuple(json.loads(records[1][6])),
                    'pilot_initial_cohort_changed')
        require(digest(self._guard_snapshot()) == records[1][8], 'pilot_initial_cohort_changed')

    def _enroll(self, manifest, table):
        self._verify(manifest)
        with self._db() as db:
            if _schema(db, absent=True):
                self._binding_row(db, manifest)
                if _bounded(db, 'SELECT * FROM ' + table, limit=1):
                    _manifest_row(db, table, manifest)
                    return True  # No writer lock or update on exact restart.
        self._now(manifest, active=False)
        with self._db(write=True) as db:
            _schema(db)  # Missing founding tables are never provisioned here.
            self._binding_row(db, manifest)
            existing = _bounded(db, 'SELECT * FROM ' + table, limit=1)
            if existing:
                _manifest_row(db, table, manifest)
                return True  # Exact enrollment is a read-only no-op, never renewal.
            if table == 'finite_pilot_enrollment':
                self._pilot_initial_state(db, manifest)
            else:
                require(self.cohort_state_digest() == manifest.pre_enrollment_cohort_sha256,
                        'recovery_initial_cohort_changed')
            marker = 'pilot_digest' if table == 'finite_pilot_enrollment' else 'recovery_digest'
            require(db.execute('UPDATE finite_pilot_binding SET ' + marker + '=? WHERE id=1 AND ' + marker + ' IS NULL',
                               (digest(manifest.document()),)).rowcount == 1, 'enrollment_already_consumed')
            db.execute('INSERT INTO ' + table + ' VALUES(1,?,?)',
                       (digest(manifest.document()), _json(manifest.document())))
            if table == 'finite_pilot_enrollment':
                require(not _bounded(db, 'SELECT * FROM finite_pilot_control', limit=1)
                        and not _bounded(db, 'SELECT * FROM finite_pilot_stages', limit=1)
                        and not _bounded(db, 'SELECT * FROM finite_pilot_writes', limit=1)
                        and not _bounded(db, 'SELECT * FROM finite_pilot_sources', limit=1),
                        'anchor_pilot_orphaned')
                db.execute("INSERT INTO finite_pilot_control VALUES(1,0,0,0,0,0,0,0,'',0)")
        self._verify(manifest)
        return True

    def enroll_pilot(self, manifest):
        contract.require_pilot_manifest(manifest)
        return self._enroll(manifest, 'finite_pilot_enrollment')

    def enroll_recovery(self, manifest):
        contract.require_recovery_manifest(manifest)
        from readonly_runtime_worker import serialized
        with serialized(self.bundle):
            self._verify(manifest)
            with self._db() as db:
                if _schema(db, absent=True):
                    self._binding_row(db, manifest)
                    rows = _bounded(db, 'SELECT * FROM finite_pilot_recovery', limit=1)
                    if rows:
                        _manifest_row(db, 'finite_pilot_recovery', manifest)
                        records = _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal', limit=2)
                        if records:
                            _recovery_gate(db)  # PREPARED is never a new attempt.
                        return True
            self._now(manifest)
            self._controls(recovery=manifest)
            require(self.protected_state_digest() == manifest.protected_state_sha256,
                    'recovery_protected_state_changed')
            return self._enroll(manifest, 'finite_pilot_recovery')

    def _row(self, db, manifest):
        self._binding_row(db, manifest)
        _manifest_row(db, 'finite_pilot_enrollment', manifest)
        _recovery_gate(db)
        rows = _bounded(db, 'SELECT * FROM finite_pilot_control', limit=1)
        require(len(rows) == 1, 'pilot_control_corrupt')
        names = ('id', 'ticks', 'writes', 'sources', 'dirty', 'last_ns', 'last_tick_ns', 'last_source_ns', 'stage', 'seq')
        row = dict(zip(names, rows[0]))
        require(row['id'] == 1 and all(type(row[k]) is int and row[k] >= 0 for k in names if k != 'stage')
                and row['ticks'] <= manifest.max_ticks and row['writes'] <= manifest.max_write_attempts
                and row['sources'] <= manifest.max_source_requests and row['dirty'] in (0, 1)
                and row['last_ns'] >= max(row['last_tick_ns'], row['last_source_ns'])
                and row['stage'] in ('', 'source', 'tick'), 'pilot_control_corrupt')
        stages = _bounded(db, 'SELECT * FROM finite_pilot_stages ORDER BY seq', limit=200)
        writes = _bounded(db, 'SELECT * FROM finite_pilot_writes ORDER BY method,target', limit=8)
        sources = _bounded(db, 'SELECT * FROM finite_pilot_sources ORDER BY seq', limit=120)
        require(row['seq'] == len(stages) and [r[0] for r in stages] == list(range(1, len(stages)+1))
                and row['ticks'] == sum(r[1] == 'tick' for r in stages)
                and row['writes'] == len(writes) and row['sources'] == len(sources), 'pilot_counters_changed')
        unfinished = [s for s in stages if s[3] is None]
        require((row['dirty'] == 0 and row['stage'] == '' and not unfinished)
                or (row['dirty'] == 1 and len(unfinished) == 1
                    and unfinished[0][:2] == (row['seq'], row['stage'])), 'pilot_stage_corrupt')
        last = {'tick': 0, 'source': 0}
        for seq, stage, started, finished in stages:
            require(stage in last and type(started) is int and manifest.starts_ns <= started < manifest.expires_ns
                    and started <= row['last_ns']
                    and (finished is None or type(finished) is int and started <= finished <= row['last_ns']),
                    'pilot_stage_corrupt')
            cadence = manifest.cadence_ns if stage == 'tick' else manifest.source_cadence_ns
            require(last[stage] == 0 or started - last[stage] >= cadence, 'pilot_stage_corrupt')
            last[stage] = started
        require((row['last_tick_ns'], row['last_source_ns']) == (last['tick'], last['source']),
                'pilot_stage_corrupt')
        for method, target, at, seq in writes:
            require(method in ('POST', 'DELETE') and type(target) is str and 0 < len(target) <= 192
                    and type(seq) is int and 1 <= seq <= len(stages) and stages[seq-1][1] == 'tick'
                    and type(at) is int and stages[seq-1][2] <= at <= row['last_ns'], 'pilot_writes_corrupt')
        require([s[0] for s in sources] == list(range(1, len(sources)+1)), 'pilot_sources_corrupt')
        for _, at, seq in sources:
            require(type(seq) is int and 1 <= seq <= len(stages) and stages[seq-1][1] == 'source'
                    and type(at) is int and stages[seq-1][2] <= at <= row['last_ns'], 'pilot_sources_corrupt')
        return row

    def _controls(self, *, active=False, source_active=False, recovery=None):
        values = {}
        for role in ('runner', 'decision', 'lifecycle', 'source', 'legacy'):
            with inventory._database(self.bundle.paths[role], self.bundle._pinned[role]) as db:
                if role == 'runner':
                    row = inventory._singleton(db, 'runner_control')
                    if recovery is not None:
                        require(digest(_eligible_runner(row)) == recovery.original_runner_sha256,
                                'recovery_runner_changed')
                        values['runner'] = row
                    else:
                        require(row[3:5] == (0, None) and (row[2] in (0, 1) if active else row[2] == 0),
                                'runner_attention_or_stopped')
                elif role == 'decision':
                    require(inventory._singleton(db, 'demo_control')[1] == 0
                            and db.execute('SELECT 1 FROM demo_attention LIMIT 1').fetchone() is None,
                            'decision_attention_or_stopped')
                elif role == 'lifecycle':
                    row = inventory._singleton(db, 'control')
                    require(row[3:5] == (0, 0), 'native_attention_or_stopped')
                    if recovery is not None:
                        for table in ('intents', 'cancels', 'native_dispatch'):
                            require(db.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone() is None,
                                    'recovery_native_claims_require_review')
                elif role == 'source':
                    require(inventory._singleton(db, 'primary_meta')[4] == 0 or source_active,
                            'source_interrupted')
                else:
                    require(inventory._singleton(db, 'paper_readonly')[8] == 0
                            and inventory._singleton(db, 'safety')[1] == 0,
                            'legacy_stopped')
                    for table in ('paper_experiment', 'paper_renewal'):
                        row = inventory._singleton(db, table)
                        require(row[4] != 'stopped', 'legacy_stopped')
        return values

    @staticmethod
    def _exhausted(row, manifest):
        return (row['ticks'] >= manifest.max_ticks or row['writes'] >= manifest.max_write_attempts
                or row['sources'] >= manifest.max_source_requests)

    def startup_gate(self, manifest):
        contract.require_pilot_manifest(manifest)
        self._verify(manifest)
        with self._db() as db:
            row = self._row(db, manifest)
            self._now(manifest, row['last_ns'])
            require(row['dirty'] == 0, 'pilot_stage_incomplete')
            require(not self._exhausted(row, manifest), 'pilot_budget_exhausted')
        self._controls()
        return row

    def check_stage(self, manifest, stage):
        contract.require_pilot_manifest(manifest)
        require(stage in ('source', 'tick'), 'pilot_stage_invalid')
        self._verify(manifest)
        with self._db() as db:
            row = self._row(db, manifest)
            self._now(manifest, row['last_ns'])
            require(row['dirty'] == 1 and row['stage'] == stage, 'pilot_stage_not_active')
        self._controls(active=stage == 'tick', source_active=stage == 'source')
        self._now(manifest, row['last_ns'])  # Last clock-only gate, after local control reads.
        return row

    def begin_stage(self, manifest, stage):
        contract.require_pilot_manifest(manifest)
        require(stage in ('source', 'tick'), 'pilot_stage_invalid')
        self.startup_gate(manifest)
        with self._db(write=True) as db:
            row = self._row(db, manifest)
            now = self._now(manifest, row['last_ns'])
            require(row['dirty'] == 0, 'pilot_stage_incomplete')
            last = row['last_tick_ns'] if stage == 'tick' else row['last_source_ns']
            cadence = manifest.cadence_ns if stage == 'tick' else manifest.source_cadence_ns
            require(not last or now - last >= cadence, 'pilot_cadence_wait')
            require(row['ticks'] < manifest.max_ticks if stage == 'tick'
                    else row['sources'] < manifest.max_source_requests, 'pilot_attempt_cap')
            seq = row['seq'] + 1
            require(seq <= 200, 'pilot_stage_cap')
            db.execute('INSERT INTO finite_pilot_stages VALUES(?,?,?,NULL)', (seq, stage, now))
            column = 'last_tick_ns' if stage == 'tick' else 'last_source_ns'
            db.execute('UPDATE finite_pilot_control SET dirty=1,stage=?,seq=?,last_ns=?,' + column +
                       '=?,ticks=ticks+? WHERE id=1', (stage, seq, now, now, int(stage == 'tick')))
        return self.check_stage(manifest, stage)

    def finish_stage(self, manifest, stage):
        self.check_stage(manifest, stage)
        self._controls()
        with self._db(write=True) as db:
            row = self._row(db, manifest)
            now = self._now(manifest, row['last_ns'])
            require(row['dirty'] == 1 and row['stage'] == stage, 'pilot_stage_not_active')
            changed = db.execute('UPDATE finite_pilot_stages SET finished_ns=? WHERE seq=? AND finished_ns IS NULL',
                                 (now, row['seq'])).rowcount
            require(changed == 1, 'pilot_stage_changed')
            db.execute("UPDATE finite_pilot_control SET dirty=0,stage='',last_ns=? WHERE id=1", (now,))
        with self._db() as db:
            result = self._row(db, manifest)
        result['budget_exhausted'] = self._exhausted(result, manifest)
        return result

    def reserve_source_request(self, manifest):
        self.check_stage(manifest, 'source')
        with self._db(write=True) as db:
            row = self._row(db, manifest)
            now = self._now(manifest, row['last_ns'])
            require(row['dirty'] == 1 and row['stage'] == 'source'
                    and row['sources'] < manifest.max_source_requests, 'pilot_source_cap')
            db.execute('INSERT INTO finite_pilot_sources VALUES(?,?,?)', (row['sources'] + 1, now, row['seq']))
            db.execute('UPDATE finite_pilot_control SET sources=sources+1,last_ns=? WHERE id=1', (now,))
        self.check_stage(manifest, 'source')
        return True

    @contextmanager
    def write_attempt(self, manifest, method, target):
        self.check_stage(manifest, 'tick')
        require(method in ('POST', 'DELETE') and type(target) is str
                and re.fullmatch(r'[A-Za-z0-9_./:-]{1,192}', target), 'pilot_write_target_invalid')
        with self._db(write=True) as db:
            row = self._row(db, manifest)
            now = self._now(manifest, row['last_ns'])
            require(row['dirty'] == 1 and row['stage'] == 'tick'
                    and row['writes'] < manifest.max_write_attempts, 'pilot_write_cap')
            require(db.execute('SELECT 1 FROM finite_pilot_writes WHERE method=? AND target=?',
                               (method, target)).fetchone() is None, 'pilot_write_replay')
            db.execute('INSERT INTO finite_pilot_writes VALUES(?,?,?,?)', (method, target, now, row['seq']))
            db.execute('UPDATE finite_pilot_control SET writes=writes+1,last_ns=? WHERE id=1', (now,))
        # Attempt is durable before entering underlying HTTP, even when this
        # second lock/clock check fails. Nothing refunds or repeats the attempt.
        with self._db(write=True) as db:
            row = self._row(db, manifest)
            require(row['dirty'] == 1 and row['stage'] == 'tick', 'pilot_stage_not_active')
            now = self._now(manifest, row['last_ns'])
            db.execute('UPDATE finite_pilot_control SET last_ns=? WHERE id=1', (now,))
            self._now(manifest, now)
            yield

    def _checkpoint(self, name):
        """No-op crash-test seam. Never used for authority or retry."""

    def protected_state_digest(self):
        return self._state_digest(protected=True)

    def cohort_state_digest(self):
        # Initial pilot prestate includes runner and every mutable guard row,
        # excluding anchor bytes pinned separately to avoid circular enrollment.
        return self._state_digest(protected=False)

    def _state_digest(self, *, protected):
        """Digest full immutable evidence, omitting ONLY separately checked data.

        Runner control is exact-CAS bound. Guard budget/receipt/audit contents
        have a separately archived/validated delta. Guard schema and immutable
        pins stay included. All other cohort and existing anchor tables remain.
        After full verification, durable identity tuples use enrollment
        references; physical opens and in-process race checks remain pinned.
        """
        self._verify()
        result = []
        total = 0
        roles = (*inventory.DB_ROLES, 'anchor', 'manifest') if protected else (*inventory.DB_ROLES, 'manifest')
        for role in roles:
            with inventory._database(self.bundle.paths[role], self.bundle._pinned[role],
                                     public_read=role == 'anchor') as db:
                schemas = _bounded(db, "SELECT type,name,tbl_name,sql FROM sqlite_master "
                                   "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")
                schemas = [r for r in schemas if not (role == 'anchor' and r[1].startswith(PREFIX))]
                values = []
                if protected and role == 'anchor':
                    self.bundle._anchor_schema(db)
                    enrolled_anchor = self.bundle._anchor_row(db)
                    historical = _bounded(db, 'SELECT * FROM events WHERE version_id=?', (enrolled_anchor[6],), limit=1)
                    receipt = self._inspection_receipt(db)
                    # Exempt only the five known mutable application row sets.
                    # Every other anchor table (including diagnostic/probe
                    # consumption, unknown safety metadata and explicit absence
                    # via the schema list) remains immutable compiled evidence.
                    other = []
                    for kind, name, _, _ in schemas:
                        if kind != 'table' or name in inventory._ANCHOR_COLUMNS:
                            continue
                        require(re.fullmatch('[A-Za-z0-9_]+', name), 'protected_table_invalid')
                        extra = _bounded(db, 'SELECT * FROM "' + name + '"')
                        safe = [[{'blob': v.hex()} if isinstance(v, bytes) else v for v in row] for row in extra]
                        safe.sort(key=_json)
                        other.append((name, safe))
                    require(len(_json(other)) <= 16 * 1024 * 1024, 'anchor_work_limit')
                    result.append((role, self.bundle.paths[role], self.bundle._reference_identity(role),
                        db.execute('PRAGMA application_id').fetchone()[0], db.execute('PRAGMA user_version').fetchone()[0],
                        schemas, enrolled_anchor, historical, receipt, other))
                    continue
                for kind, name, _, _ in schemas:
                    if kind != 'table' or (protected and role == 'runner' and name == 'runner_control'):
                        continue
                    require(re.fullmatch('[A-Za-z0-9_]+', name), 'protected_table_invalid')
                    rows = _bounded(db, 'SELECT * FROM "' + name + '"')
                    if protected and role == 'guard':
                        if name == 'account_guard_binding':
                            rows = [r[:-1] for r in rows]
                        elif name == 'native_read_budget':
                            rows = [r[:2] for r in rows]
                        elif name in ('account_guard_attempts', 'native_read_attempts', 'native_read_audit',
                                      'legacy_absence_receipts', 'legacy_native_terminals'):
                            continue
                    # SQLite values are canonicalized without putting private
                    # contents in the journal, errors or public reports.
                    safe = [[{'blob': v.hex()} if isinstance(v, bytes) else v for v in r] for r in rows]
                    safe.sort(key=_json)
                    values.append((name, safe))
                entry = (role, self.bundle.paths[role], self.bundle._reference_identity(role),
                         db.execute('PRAGMA application_id').fetchone()[0],
                         db.execute('PRAGMA user_version').fetchone()[0], schemas, values)
                total += len(_json(entry))
                require(total <= 16 * 1024 * 1024, 'anchor_work_limit')
                result.append(entry)
        if not protected:
            result.append(('seal', self.bundle._reference_identity('seal'), self._seal_digest()))
        return digest(result)

    def application_state_digest(self):
        self._verify()
        with self._db() as db:
            schemas = _bounded(db, "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE "
                               "name NOT LIKE 'sqlite_%' ORDER BY type,name")
            schemas = [r for r in schemas if not r[1].startswith(PREFIX)]
            data = []
            size = 0
            for kind, name, _, _ in schemas:
                if kind != 'table':
                    continue
                require(re.fullmatch('[A-Za-z0-9_]+', name), 'protected_table_invalid')
                rows = _bounded(db, 'SELECT * FROM "' + name + '"')
                safe = [[{'blob': v.hex()} if isinstance(v, bytes) else v for v in row] for row in rows]
                safe.sort(key=_json)
                size += len(_json(safe))
                require(size <= 16 * 1024 * 1024, 'anchor_work_limit')
                data.append((name, safe))
            return digest((schemas, data))

    def _guard_snapshot(self):
        with inventory._database(self.bundle.paths['guard'], self.bundle._pinned['guard']) as db:
            result = {}
            for table in inventory._COLUMNS['guard']:
                # rowid preserves previous audit identities, not just equal rows.
                result[table] = _bounded(db, 'SELECT rowid,* FROM ' + table + ' ORDER BY rowid')
            return result

    def _guard_delta(self, before, after, evidence, prepared_ns, guard):
        observation = evidence.observation
        for table in ('account_guard_binding', 'native_read_budget'):
            require(len(before[table]) == len(after[table]) == 1
                    and before[table][0][:-1] == after[table][0][:-1]
                    and before[table][0][-1] <= after[table][0][-1] <= evidence.finished_ns,
                    'recovery_guard_binding_changed')
        for table in ('legacy_absence_receipts', 'legacy_native_terminals', 'native_read_audit'):
            old, new = dict((r[0], r[1:]) for r in before[table]), dict((r[0], r[1:]) for r in after[table])
            require(all(new.get(k) == v for k, v in old.items()), 'recovery_guard_history_changed')
        # No disappearing unexpired reservation. Duplicates count separately.
        for table, meta in (('account_guard_attempts', 'account_guard_binding'),
                            ('native_read_attempts', 'native_read_budget')):
            old = Counter(r[-1] for r in before[table])
            new = Counter(r[-1] for r in after[table])
            require(all(new[t] >= n for t, n in old.items() if t > after[meta][0][-1] - 60 * NS),
                    'recovery_attempt_refund')
            added = new - old
            require(all(prepared_ns <= t <= evidence.finished_ns for t in added)
                    and sum(new.values()) <= 12
                    and after[meta][0][-1] == max(before[meta][0][-1], max(new, default=0)),
                    'recovery_budget_delta_invalid')
            if table == 'account_guard_attempts':
                require(sum(added.values()) == evidence.request_count, 'recovery_request_count_changed')
            else:
                require(1 <= sum(added.values()) <= evidence.request_count, 'recovery_request_count_changed')
        # Exactly one new successful collector audit; all previous audit rows
        # survive byte-for-byte (including previous failed/uncertain attempts).
        prior = {r[0] for r in before['native_read_audit']}
        added_audits = [r[1:] for r in after['native_read_audit'] if r[0] not in prior]
        require(len(added_audits) == 1 and added_audits[0][1] == 'observed_readonly'
                and added_audits[0][0] == observation.observed_ns
                and 1 <= added_audits[0][2] <= evidence.request_count
                and added_audits[0][-1] == observation.native_digest, 'recovery_audit_invalid')
        require(type(evidence.legacy_terminal_digests) is tuple
                and type(evidence.legacy_absence_receipts) is tuple, 'typed_reconciliation_required')
        expected_absence = {}
        for cid in guard.reserved_client_ids:
            original_row = guard._rows[cid][0]
            receipt_digest = digest([guard.binding, str(guard.legacy_revision), original_row,
                observation.observed_ns, 'authenticated_404_original_quote_moved_reserved'])
            expected_absence[(cid, observation.observed_ns)] = receipt_digest
        require(evidence.legacy_absence_receipts == tuple(sorted(
                (cid, at, value) for (cid, at), value in expected_absence.items())),
                'recovery_absence_evidence_changed')
        before_absence = {(r[1], r[2]): r[3] for r in before['legacy_absence_receipts']}
        require(all(k not in before_absence or before_absence[k] == v for k, v in expected_absence.items()),
                'recovery_guard_history_changed')
        require({(r[1], r[2]): r[3] for r in after['legacy_absence_receipts']}
                == before_absence | expected_absence, 'recovery_receipt_invalid')
        require(all(type(r) is tuple and len(r) == 2 and type(r[0]) is str
                    and contract._hash(r[1]) for r in evidence.legacy_terminal_digests),
                'recovery_terminal_evidence_invalid')
        terminal = dict(evidence.legacy_terminal_digests)
        require(len(terminal) == len(evidence.legacy_terminal_digests)
                and set(terminal) == set(guard.submitted_client_ids), 'recovery_terminal_evidence_invalid')
        before_terminals = {r[1]: r[2] for r in before['legacy_native_terminals']}
        require(all(k in terminal and terminal[k] == v for k, v in before_terminals.items()),
                'recovery_guard_history_changed')
        require({r[1]: r[2] for r in after['legacy_native_terminals']} == terminal,
                'recovery_terminal_evidence_invalid')

    def recover_incident(self, manifest, *, guard, validate):
        contract.require_recovery_manifest(manifest)
        # No callback supplied by a grant file, CLI, bool or arbitrary caller can
        # replace the reviewed worker's real typed-evidence producer.
        import finite_pilot_worker
        from native_account_guard import SharedAccountGuard
        from native_paper_execution import ExecutionObservation
        from readonly_runtime_worker import serialized
        require(validate is finite_pilot_worker.validate_recovery_evidence,
                'reviewed_recovery_validator_required')
        require(type(guard) is SharedAccountGuard and guard.binding == self.bundle.binding
                and guard.legacy_database == self.bundle.paths['legacy']
                and guard.lifecycle_database == self.bundle.paths['lifecycle']
                and guard.state_database == self.bundle.paths['guard'], 'recovery_guard_mismatch')
        with serialized(self.bundle):
            self._verify(manifest)
            with self._db() as db:
                self._binding_row(db)
                _manifest_row(db, 'finite_pilot_recovery', manifest)
                records = _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal', limit=2)
                if records:
                    _recovery_gate(db)
                    return dict(phase='already_recovered_readonly', execution_ready=False,
                                order_permission='none', retry_allowed=False)
            with guard.hold(now_ns=self._now(manifest), read_only=True):
                original_runner = self._controls(recovery=manifest)['runner']
                protected = self.protected_state_digest()
                require(protected == manifest.protected_state_sha256, 'recovery_protected_state_changed')
                application_before = self.application_state_digest()
                before = self._guard_snapshot()
                # Enrollment does not authorize accepting changed audit, quota
                # or reconciliation rows as a newly invented baseline. Pin the
                # approved complete prestate again before the durable attempt.
                require(self.cohort_state_digest() == manifest.pre_enrollment_cohort_sha256,
                        'recovery_initial_cohort_changed')
                prepared = self._now(manifest)
                proposed = (1, prepared / NS, 0, 0, None, 'recovered_readonly')
                require(proposed[1] >= original_runner[1], 'recovery_clock_regression')
                receipt = (manifest.incident_id, prepared, digest(manifest.document()),
                           _json(original_runner), _json(proposed), protected, digest(before))
                with self._db(write=True) as db:
                    self._binding_row(db)
                    _manifest_row(db, 'finite_pilot_recovery', manifest)
                    require(not _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal', limit=2),
                            'recovery_attempt_consumed')
                    # Recovery cannot also replay a begun finite pilot stage.
                    rows = _bounded(db, 'SELECT dirty FROM finite_pilot_control', limit=1)
                    require(not rows or rows == [(0,)], 'pilot_stage_incomplete')
                    db.execute("INSERT INTO finite_pilot_recovery_journal VALUES(1,?,'prepared',?,?,?,?,?,?,NULL)", receipt)
                self._checkpoint('prepared')
                evidence = validate(manifest, bundle=self.bundle, guard=guard, now_ns=self.now_ns)
                require(type(evidence) is contract.RecoveryEvidence
                        and type(evidence.observation) is ExecutionObservation, 'typed_recovery_evidence_required')
                now = self._now(manifest, prepared)
                require(contract._ns(evidence.started_ns) and contract._ns(evidence.finished_ns)
                        and prepared <= evidence.started_ns <= evidence.observation.observed_ns
                        <= evidence.finished_ns <= now and now - evidence.started_ns <= 10 * NS
                        and type(evidence.request_count) is int and 1 <= evidence.request_count <= 12
                        and contract._hash(evidence.observation.native_digest)
                        and evidence.observation.legacy_revision == guard.legacy_revision,
                        'recovery_evidence_stale_or_invalid')
                self._checkpoint('validated')
                # Final cooperating writer exclusion follows established order.
                # Observer needed lifecycle transactions, so acquire these AFTER
                # it returns; compare all protected bytes again under the locks.
                with ExitStack() as stack:
                    handles = {}
                    for role in ('runner', 'decision', 'lifecycle', 'source', 'guard', 'anchor'):
                        handles[role] = stack.enter_context(inventory._database(
                            self.bundle.paths[role], self.bundle._pinned[role], write=True, public_read=role == 'anchor'))
                    self._verify(manifest)
                    self._controls(recovery=manifest)
                    guard.check()
                    require(self.protected_state_digest() == protected, 'recovery_protected_state_changed')
                    require(self.application_state_digest() == application_before, 'recovery_application_state_changed')
                    after = self._guard_snapshot()
                    self._guard_delta(before, after, evidence, prepared, guard)
                    now = self._now(manifest, now)
                    require(now - evidence.started_ns <= 10 * NS, 'recovery_evidence_stale_or_invalid')
                    runner = handles['runner']
                    changed = runner.execute("UPDATE runner_control SET heartbeat=?,dirty=0,attention=NULL,stage='recovered_readonly' "
                        'WHERE id=? AND heartbeat=? AND dirty=? AND stopped=? AND attention=? AND stage=?',
                        (proposed[1], *original_runner)).rowcount
                    require(changed == 1 and inventory._singleton(runner, 'runner_control') == proposed,
                            'recovery_compare_and_set_failed')
                    self._checkpoint('cas_before_commit')
                    runner.commit()  # Separate, intentionally non-atomic with journal.
                    self._checkpoint('cas_committed')
                    # The runner transaction is reacquired immediately; any
                    # competing stop/change wins and keeps PREPARED blocking.
                    runner.execute('BEGIN IMMEDIATE')
                    require(inventory._singleton(runner, 'runner_control') == proposed,
                            'recovery_runner_changed')
                    self._verify(manifest)
                    require(self.protected_state_digest() == protected, 'recovery_protected_state_changed')
                    require(self.application_state_digest() == application_before, 'recovery_application_state_changed')
                    self._controls(active=False)
                    final_ns = self._now(manifest, now)
                    require(final_ns - evidence.started_ns <= 10 * NS, 'recovery_evidence_stale_or_invalid')
                    evidence_digest = digest((evidence.started_ns, evidence.finished_ns, evidence.request_count,
                                              evidence.observation.observed_ns, evidence.observation.native_digest))
                    db = handles['anchor']
                    self._binding_row(db)
                    _manifest_row(db, 'finite_pilot_recovery', manifest)
                    rows = _bounded(db, 'SELECT * FROM finite_pilot_recovery_journal ORDER BY seq', limit=2)
                    require(rows == [(1, manifest.incident_id, 'prepared', *receipt[1:], None)],
                            'recovery_receipt_changed')
                    db.execute("INSERT INTO finite_pilot_recovery_journal VALUES(2,?,'applied',?,?,?,?,?,?,?)",
                               (manifest.incident_id, final_ns, receipt[2], receipt[3], receipt[4],
                                protected, digest(after), evidence_digest))
                    self._checkpoint('applied_before_commit')
                    db.commit()
                    self._checkpoint('applied')
        return dict(phase='recovered_readonly', execution_ready=False, order_permission='none', retry_allowed=False)
