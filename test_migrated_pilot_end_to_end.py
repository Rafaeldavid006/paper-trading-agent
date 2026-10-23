"""Production phase chain on migrated disposable state. No sockets or live state."""
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

import finite_pilot_contract as contract
import finite_pilot_inspection as inspection
import finite_pilot_worker as worker
from finite_pilot_anchor import AnchorJournal
import native_get_diagnostic as consumed_get
import native_schema_diagnostic as old_diagnostic
import native_observation_collector as collector
import native_restart_migration as migration
from runtime_state_bundle import RuntimeStateBundle, GENERATION
import test_readonly_runtime_worker as fixtures
import test_native_cohort_identity_evidence as identity_fixtures
from test_native_restart_identity import device_shift
from test_native_observation_worker import FP
from test_finite_pilot_worker import CannedOpener, canned_native
from test_primary_earnings_source import feed_bytes, item_xml


class MigratedPilotEndToEndTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket', 'socket.create_connection', 'socket.getaddrinfo'):
            self.enterContext(patch(name, side_effect=AssertionError('NETWORK FORBIDDEN')))
        self.h=fixtures.RuntimeWorkerTests();self.h.setUp();self.addCleanup(self.h.doCleanups)
        self.h.now=self.h.now.replace(minute=31)
        self.h.initialize();self.paths=self.h.paths
        now=self.h.kw['now_ns']()
        with sqlite3.connect(self.paths['runner']) as db:
            db.execute("UPDATE runner_control SET heartbeat=?,dirty=1,attention='readonly_runtime_stage_failed',stage='started'",(self.h.now.timestamp(),))
        with sqlite3.connect(self.paths['guard']) as db:
            db.execute("INSERT INTO native_read_audit VALUES(?,'native_schema_refused',6,0,2,NULL)",(now-100*contract.NS,))
            db.execute("INSERT INTO native_read_audit VALUES(?,'provider_read_failed',4,0,0,NULL)",(now-90*contract.NS,))
        with sqlite3.connect(self.paths['anchor']) as db:
            db.execute(old_diagnostic._SQL)
            db.execute("INSERT INTO native_schema_diagnostic_claim VALUES(1,?,?,?,'collection_refused','none','none',4)",
                       (old_diagnostic.GENERATION,'a'*64,now-95*contract.NS))
            db.execute(consumed_get.SQL)
            db.execute("INSERT INTO native_get_diagnostic_claim VALUES(1,?,?,?,'recorded',?,?)",
                       (consumed_get.GENERATION,'b'*64,now-80*contract.NS,
                        json.dumps(dict(status='observed_readonly',attempts=8)),
                        json.dumps(dict(reason='sample_observed',historical_cause_proven=False))))
        identity_fixtures.IdentityEvidenceTests.old_identity_delta(self,device=51)
        backup=self.h.root/'migration-backup';backup.mkdir(mode=0o700)
        plan=migration.prepare(paths=self.paths,test_only=True)
        migration.apply(enabled=True,plan=plan,approved_digest=plan.digest,quiescence_confirmed=True,
                        backup_directory=str(backup),paths=self.paths,test_only=True)
        self.original_runner=self.rows('runner','runner_control')[0]
        self.original=self.history()
        grant=contract.InspectionManifest(contract.INSPECTION_GENERATION,now-contract.NS,
            now+600*contract.NS,GENERATION,'c'*64,'SYNTHETIC-LOCAL-PROOF')
        with patch.object(contract,'APPROVED_INSPECTION',grant):
            result=inspection.run_inspection(paths=self.paths,test_only=True,now_ns=self.h.kw['now_ns'])
        self.assertEqual(result['phase'],'inspection_complete')
        self.proof=result['proof']
        self.recovery=contract.RecoveryManifest(contract.INCIDENT_ID,now-contract.NS,
            now+600*contract.NS,GENERATION,self.proof['bundle_seal_sha256'],
            self.proof['original_runner_sha256'],self.proof['protected_state_sha256'],
            'a'*64,'b'*64,'c'*64,'d'*64,'SYNTHETIC-LOCAL-RECOVERY',
            anchor_pre_enrollment_sha256=self.proof['anchor_pre_enrollment_sha256'],
            pre_enrollment_cohort_sha256=self.proof['pre_enrollment_cohort_sha256'])
        self.enterContext(patch.object(contract,'APPROVED_RECOVERY',self.recovery))
        self.enterContext(patch.object(worker.os,'environ',self.h.env))
        # Only the opener is canned: production HTTP request/JSON/route/final
        # opener guard, observer, quota, parser and reconciliation run unchanged.
        self.enterContext(patch.object(collector.request,'build_opener',return_value=CannedOpener(self.h.wire)))

    def rows(self,role,table):
        with sqlite3.connect(self.paths[role]) as db:
            return db.execute('SELECT * FROM '+table).fetchall()

    def history(self):
        with sqlite3.connect(self.paths['guard']) as db:
            audits=db.execute('SELECT rowid,* FROM native_read_audit ORDER BY rowid LIMIT 2').fetchall()
        return dict(audits=audits,legacy=Path(self.paths['legacy']).read_bytes(),
                    diagnostic=self.rows('anchor','native_schema_diagnostic_claim'),
                    get_receipt=self.rows('anchor','native_get_diagnostic_claim'),
                    migration=self.rows('anchor','native_restart_identity'))

    def recover(self):return worker.run_phase('recover',**self.h.kw)

    def pilot(self):
        now=self.h.kw['now_ns']()
        manifest=contract.PilotManifest('SYNTHETIC-LOCAL-PILOT',self.recovery.starts_ns,
            self.recovery.starts_ns+1800*contract.NS,GENERATION,self.proof['bundle_seal_sha256'],
            ('NVDA',),10,8,120,'e'*64,'SYNTHETIC-LOCAL-PILOT',
            anchor_pre_enrollment_sha256=self.proof['anchor_pre_enrollment_sha256'],
            pre_enrollment_cohort_sha256=self.proof['pre_enrollment_cohort_sha256'])
        self.enterContext(patch.object(contract,'APPROVED_PILOT',manifest))
        kwargs=dict(self.h.kw,broker_factory=lambda *a,**k:canned_native(self.h.wire))
        run=lambda phase:worker.run_phase(phase,**kwargs)
        self.assertEqual(run('initialize')['phase'],'initialize')
        return manifest,run

    def test_real_proof_recovery_remount_source_tick_retains_history(self):
        observed=[]
        def still_halted(*args):
            observed.append(True);self.assertEqual(self.rows('runner','runner_control')[0],self.original_runner)
        self.h.wire.on_call=still_halted
        with device_shift(107),patch('os.fchmod',side_effect=AssertionError('NO CHMOD')):
            result=self.recover()
        self.assertEqual(result['reason'],'incident_recovered')
        self.assertEqual(result['order_permission'],'none')
        self.assertTrue(observed)
        self.assertTrue(all(method=='GET' for method,_,_ in self.h.wire.calls))
        recovery_calls=len(self.h.wire.calls);self.assertLessEqual(recovery_calls,12)
        self.assertEqual(self.history(),self.original)
        self.h.wire.on_call=lambda *args:None
        # Another independently constructed process after another remount must
        # accept logical receipts, but never reset the consumed recovery.
        with device_shift(211):
            self.assertEqual(self.recover()['reason'],'incident_recovery_previously_applied')
            self.assertEqual(len(self.h.wire.calls),recovery_calls)
            manifest,run=self.pilot()
            self.h.http.feed=feed_bytes(item_xml(date='Tue, 20 Oct 2026 13:31:00 GMT'))
            run('source')
            immediate=run('tick')
            self.assertEqual(immediate['outcome'],'waiting_request_budget')
            self.assertEqual(len(self.h.wire.calls),recovery_calls)
            self.assertEqual(self.rows('anchor','finite_pilot_control')[0][1],0)
            outcomes=[]
            for n in range(1,17):
                self.h.now+=timedelta(seconds=20);run('source')
                if n%4==0:outcomes.append(run('tick'))
            self.assertEqual(outcomes[-1]['outcome'],'complete',outcomes)
            self.assertEqual(len([call for call in self.h.wire.calls if call[0]=='POST']),1)
            self.assertEqual(self.rows('lifecycle','native_dispatch')[0][-1],'observed')
            self.assertEqual(self.history(),self.original)
            self.assertEqual(self.rows('anchor','finite_pilot_control')[0][1:4],(4,1,34))
            # Native parser, two independent quotas and audit are all exercised.
            self.assertEqual(len(self.rows('guard','native_read_audit')),7)
            journal=AnchorJournal(RuntimeStateBundle(FP,paths=self.paths,test_only=True),self.h.kw['now_ns'])
            self.assertEqual(journal.startup_gate(manifest)['writes'],1)
        self.assertEqual(self.history(),self.original)

    def test_real_observer_unknown_position_preserves_latch_consumes_recovery(self):
        from test_native_observation_worker import F
        self.h.wire.positions=[dict(F['positions'][0],symbol='NVDA')]
        with device_shift(103):
            with self.assertRaises(Exception):self.recover()
        self.assertEqual(self.rows('runner','runner_control')[0],self.original_runner)
        self.assertEqual(self.rows('anchor','finite_pilot_recovery_journal')[0][2],'prepared')
        count=len(self.h.wire.calls)
        with device_shift(203),self.assertRaisesRegex(contract.PilotRefused,'recovery_incomplete'):
            self.recover()
        self.assertEqual(len(self.h.wire.calls),count)
        self.assertEqual(self.history(),self.original)

    def test_real_recovery_success_then_stale_heartbeat_does_not_silently_refresh(self):
        self.recover();manifest,run=self.pilot()
        self.h.now+=timedelta(seconds=121)
        result=run('tick')
        self.assertEqual(result['phase'],'manual_handoff')
        self.assertEqual([c for c in self.h.wire.calls if c[0]!='GET'],[])
        self.assertEqual(self.history(),self.original)
        self.assertEqual(self.rows('runner','runner_control')[0][4],'stale_heartbeat')


if __name__=='__main__':unittest.main()
