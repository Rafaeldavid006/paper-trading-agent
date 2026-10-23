"""Proof-only deployment graph: works without finite execution worker/transport."""
from contextlib import ExitStack
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

with ExitStack() as block:
    for name in ('socket.socket','socket.create_connection','socket.getaddrinfo','subprocess.Popen'):
        block.enter_context(patch(name,side_effect=AssertionError('OFFLINE ONLY')))
    import finite_pilot_contract as contract
    import finite_pilot_inspection as inspection
    from finite_pilot_anchor import AnchorJournal
    from runtime_state_bundle import GENERATION
    import test_runtime_state_bundle as fixture

class ProofOnlyDeploymentTests(unittest.TestCase):
    def test_fixed_proof_initialization_needs_no_execution_module_or_public_identity(self):
        for name in ('socket.socket','socket.create_connection','socket.getaddrinfo','subprocess.Popen'):
            self.enterContext(patch(name,side_effect=AssertionError('OFFLINE ONLY')))
        b=fixture.BundleTests();b.setUp();self.addCleanup(b.doCleanups);b.enroll()
        now=fixture.NOW
        with sqlite3.connect(b.bundle.paths['runner']) as db:
            db.execute("UPDATE runner_control SET heartbeat=?,dirty=1,stopped=0,attention='readonly_runtime_stage_failed',stage='started' WHERE id=1",(now/contract.NS-25,))
        grant=contract.InspectionManifest(contract.INSPECTION_GENERATION,now-contract.NS,now+60*contract.NS,
                                         GENERATION,'a'*64,'SYNTHETIC-NOT-AUTHORITY')
        before={role:Path(path).read_bytes() for role,path in b.bundle.paths.items() if role!='anchor'}
        with patch.object(contract,'APPROVED_INSPECTION',grant),patch.object(contract,'APPROVED_PILOT',None),patch.object(contract,'APPROVED_RECOVERY',None):
            result=inspection.run_inspection(paths=b.bundle.paths,test_only=True,now_ns=lambda:now)
            self.assertEqual(result['phase'],'inspection_complete')
            self.assertEqual(set(result['proof']),inspection.PROOF_KEYS)
            self.assertEqual(result['order_permission'],'none')
            self.assertEqual(result['state_change'],'empty_journal_initialization')
            self.assertEqual(inspection.run_inspection(paths=b.bundle.paths,test_only=True,now_ns=lambda:now)['phase'],'inspection_consumed')
        self.assertNotIn(b.bundle.binding,json.dumps(result))
        self.assertEqual(before,{role:Path(path).read_bytes() for role,path in b.bundle.paths.items() if role!='anchor'})
        with sqlite3.connect(b.bundle.paths['anchor']) as db:
            for table in ('finite_pilot_enrollment','finite_pilot_recovery','finite_pilot_control','finite_pilot_writes'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone(),(0,))
        self.assertEqual(AnchorJournal(b.bundle).founding_proofs()['pre_enrollment_cohort_sha256'],result['proof']['pre_enrollment_cohort_sha256'])

if __name__=='__main__':unittest.main()
