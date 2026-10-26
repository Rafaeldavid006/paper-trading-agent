"""Exact optional append-only admission journal; no implicit provisioning."""
TABLE='finite_pilot_admission_journal'
SQL=('CREATE TABLE finite_pilot_admission_journal('
     'seq INTEGER PRIMARY KEY CHECK(seq IN (1,2)),status TEXT NOT NULL,at_ns INTEGER NOT NULL,'
     'manifest_digest TEXT NOT NULL,successor_rows_digest TEXT NOT NULL,old_row TEXT NOT NULL,'
     'new_row TEXT NOT NULL,protected_digest TEXT NOT NULL,guard_digest TEXT NOT NULL,evidence_digest TEXT)')
