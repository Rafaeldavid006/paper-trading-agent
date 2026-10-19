import hashlib
import unittest
from unittest.mock import patch
from decimal import Decimal
from pathlib import Path
import test_paper_experiment as fixtures
from paper_adapter import SafetyError
from paper_renewal_gate import inspect_renewal,client_id
from paper_experiment import EXPERIMENT_ID

class RenewalInspectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.OneShotExperimentTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        original=self.fixture.quotes.latest_quote;calls=0
        def moved():
            nonlocal calls
            calls+=1;q=original()
            if calls>1:q.ask=Decimal('250')
            return q
        with patch.object(self.fixture.quotes,'latest_quote',side_effect=moved):
            self.assertEqual('quote_moved',self.fixture.experiment.tick()['reason'])
        self.fixture.transport.calls.clear()

    def inspect(self):return inspect_renewal(self.fixture.database,self.fixture.transport)

    def test_inspection_is_readonly_and_never_permission(self):
        before=Path(self.fixture.database).read_bytes()
        result=self.inspect()
        self.assertTrue(result['eligible_for_separate_approval'])
        self.assertFalse(result['execution_enabled'])
        self.assertEqual(before,Path(self.fixture.database).read_bytes())
        self.assertTrue(all(c[0]=='GET' for c in self.fixture.transport.calls))

    def test_stop_is_not_cleared(self):
        self.fixture.connection.stop()
        with self.assertRaises(SafetyError):self.inspect()
        self.assertEqual([],self.fixture.transport.calls)

    def test_prior_broker_order_or_lookup_failure_blocks(self):
        original=self.fixture.transport.call
        for status in (200,403,429,500):
            def response(method,path,body=None):
                if path.startswith('/v2/orders:'):return status,{}
                return original(method,path,body)
            with patch.object(self.fixture.transport,'call',side_effect=response):
                with self.assertRaises(SafetyError):self.inspect()

    def test_identity_mismatch_blocks(self):
        self.fixture.transport.account['id']='different-fixture-account'
        with self.assertRaises(SafetyError):self.inspect()

    def test_holdings_or_unrelated_orders_block(self):
        self.fixture.transport.positions=[self.fixture.position()]
        with self.assertRaises(SafetyError):self.inspect()
        self.fixture.transport.positions=[]
        self.fixture.transport.orders['other']={'status':'new'}
        with self.assertRaises(SafetyError):self.inspect()

    def test_prior_sell_or_fill_evidence_blocks(self):
        with self.fixture.experiment.db() as c:c.execute("UPDATE intents SET filled='0.1'")
        with self.assertRaises(SafetyError):self.inspect()

    def test_new_generation_ids_do_not_reuse_original(self):
        for side in ('buy','sell'):
            self.assertNotEqual(client_id(EXPERIMENT_ID,side),client_id('unapproved-fixture-renewal',side))

if __name__=='__main__':unittest.main()
