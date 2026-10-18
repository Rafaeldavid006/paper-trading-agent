import unittest
from event_types import research_brief, ROUTES
class EventRoutingTests(unittest.TestCase):
    def test_each_event_keeps_uncertainty_and_no_orders(self):
        for kind in ROUTES:
            with self.subTest(kind=kind):
                r=research_brief([kind])
                self.assertFalse(r["classification_verified"])
                self.assertEqual(r["order_permission"],"none")
                self.assertIn(ROUTES[kind][0],r["questions"])
    def test_joint_shock_preserves_both_channels(self):
        r=research_brief(["monetary_policy","supply_disruption"])
        self.assertIn(ROUTES["monetary_policy"][0],r["questions"])
        self.assertIn(ROUTES["supply_disruption"][0],r["questions"])
    def test_unrecognized_labels_rejected(self):
        for x in [[],["buy"],"earnings",[None]]:
            with self.subTest(x=x),self.assertRaises(ValueError):research_brief(x)
    def test_unknown_is_explicit(self):
        self.assertEqual(research_brief()["event_types"],["unknown"])
    def test_deduplicates_labels(self):
        self.assertEqual(research_brief(["earnings","earnings"])["event_types"],["earnings"])
