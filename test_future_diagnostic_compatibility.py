"""Frozen diagnostic refuses the future schema before mutable/provider access."""
import hashlib
from pathlib import Path
from unittest.mock import Mock,patch
import unittest
import native_schema_diagnostic as diagnostic

class DiagnosticPinCompatibilityTests(unittest.TestCase):
    def test_future_schema_mismatch_preserves_frozen_diagnostic_before_any_state_io(self):
        here=Path(diagnostic.__file__)
        self.assertEqual(hashlib.sha256(here.read_bytes()).hexdigest(),
            'd27203d78fd10e74aaccb1266a7f789c64b7fff6b3fc5674e921808e045c0997')
        self.assertNotEqual(hashlib.sha256(here.with_name('alpaca_schema.py').read_bytes()).hexdigest(),diagnostic.SCHEMA_SHA)
        bomb=Mock(side_effect=AssertionError('NO MUTABLE OR PROVIDER IO'))
        env=Mock();env.get=bomb
        with patch.object(diagnostic,'load_owned_ledger',bomb),patch.object(diagnostic,'RuntimeStateBundle',bomb), \
             patch('sqlite3.connect',bomb),patch('socket.socket',bomb),patch('socket.create_connection',bomb),patch('socket.getaddrinfo',bomb):
            with self.assertRaisesRegex(ValueError,'diagnostic_refused'):
                diagnostic.run_once(enabled=True,environ=env,transport_factory=bomb,now_ns=bomb)
        bomb.assert_not_called()

    def test_disabled_diagnostic_does_not_even_inspect_code_or_state(self):
        with patch.object(diagnostic,'code_hash',side_effect=AssertionError('NO IO')):
            self.assertEqual(diagnostic.run_once()['reason'],'diagnostic_disabled')

if __name__=='__main__':unittest.main()
