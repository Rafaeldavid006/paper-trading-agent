import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from event_core import Store, iso
from price_reaction import Reactions
from live_shadow import run

class ShadowTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.source=Path(self.tmp.name)/'source.sqlite';self.audit=Path(self.tmp.name)/'audit.sqlite'
        self.at=datetime(2026,10,19,14,0,tzinfo=timezone.utc)
        self.now=self.at+timedelta(minutes=8)
        self.store=Store(self.source,clock=lambda:self.at+timedelta(seconds=10))
        self.version,_=self.store.ingest(source='alpaca-news',source_id='123:AAPL',symbol='AAPL',published_at=self.at,
            payload={'headline':'Apple reports quarterly earnings'},url='https://news.example/a')
        self.store.clock=lambda:self.now
        self.store.heartbeat('alpaca-news','ok')
        self.reactions=Reactions(self.store)
        self.bars=[{'t':iso(self.at-timedelta(minutes=1)+timedelta(minutes=i)),'c':100 if i==0 else 101} for i in range(7)]
        self.reactions.measure(self.version,lambda *args:self.bars)
    def reports(self):return run(self.source,self.audit,self.now)
    def test_real_schema_observation_not_execution(self):
        r=self.reports()[0]
        self.assertEqual(r['status'],'observation_candidate')
        self.assertEqual(r['order_permission'],'none')
        self.assertEqual(r['strict_pipeline']['action'],'abstain')
        self.assertFalse(r['strict_pipeline']['event']['verified'])
        self.assertEqual(r['event_version'],self.version)
        self.assertAlmostEqual(r['movement']['latest']['change_pct'],1)
    def test_source_database_byte_unchanged(self):
        before=self.source.read_bytes();self.reports();self.assertEqual(self.source.read_bytes(),before)
    def test_retry_deduplicated(self):
        self.reports();self.reports()
        with sqlite3.connect(self.audit) as c:self.assertEqual(c.execute('SELECT count(*) FROM shadow_observations').fetchone()[0],1)
    def test_missing_reaction(self):
        with self.store.db() as c:c.execute('DELETE FROM price_reactions')
        self.assertIn('no_stored_market_observation',self.reports()[0]['reasons'])
    def test_stale_health(self):
        with self.store.db() as c:c.execute('UPDATE health SET last_success=?',(iso(self.at),))
        self.assertIn('source_coverage_unhealthy_or_stale',self.reports()[0]['reasons'])
    def test_primary_boolean_not_trusted(self):
        with self.store.db() as c:c.execute('UPDATE events SET verified_primary=1')
        self.assertFalse(self.reports()[0]['strict_pipeline']['event']['verified'])
    def test_superseded_versions(self):
        self.store.ingest(source='alpaca-news',source_id='123:AAPL',symbol='AAPL',published_at=self.at,updated_at=self.now,
            payload={'headline':'Apple cuts guidance'},url='https://news.example/a')
        old=next(r for r in self.reports() if r['event_version']==self.version)
        self.assertIn('superseded_event_version',old['reasons'])
    def test_future_reaction_not_used(self):
        with self.store.db() as c:c.execute('UPDATE price_reactions SET received_at=?',(iso(self.now+timedelta(seconds=1)),))
        self.assertIn('no_stored_market_observation',self.reports()[0]['reasons'])
    def test_no_baseline(self):
        with self.store.db() as c:c.execute('UPDATE price_reactions SET raw_bars=?',(json.dumps(self.bars[1:]),))
        self.assertIn('missing_or_old_completed_bars',self.reports()[0]['reasons'])
    def test_same_db_forbidden(self):
        with self.assertRaises(ValueError):run(self.source,self.source,self.now)
    def test_price_claim_recomputed_not_trusted(self):
        with self.store.db() as c:
            row=c.execute('SELECT report FROM price_reactions').fetchone();r=json.loads(row[0]);r['latest']['change_pct']=999
            c.execute('UPDATE price_reactions SET report=?',(json.dumps(r),))
        self.assertAlmostEqual(self.reports()[0]['movement']['latest']['change_pct'],1)
    def test_no_network_or_credentials_required(self):
        from unittest.mock import patch
        with patch('urllib.request.OpenerDirector.open',side_effect=AssertionError('network forbidden')):
            self.assertEqual(self.reports()[0]['status'],'observation_candidate')


class AdversarialShadowTests(ShadowTests):
    def mutate_report(self, **change):
        with self.store.db() as c:
            report=json.loads(c.execute('SELECT report FROM price_reactions').fetchone()[0])
            report.update(change)
            c.execute('UPDATE price_reactions SET report=?',(json.dumps(report),))

    def test_future_correction_cannot_change_current_decision(self):
        self.store.clock=lambda:self.now+timedelta(seconds=1)
        self.store.ingest(source='alpaca-news',source_id='123:AAPL',symbol='AAPL',published_at=self.at,
            updated_at=self.now,payload={'headline':'Apple cuts guidance'},url='https://news.example/a')
        r=self.reports()
        self.assertEqual(len(r),1)
        self.assertNotIn('superseded_event_version',r[0]['reasons'])

    def test_source_content_tamper_fails_provenance(self):
        with self.store.db() as c:c.execute("UPDATE events SET url='https://different.example'")
        self.assertIn('event_provenance_hash_mismatch',self.reports()[0]['reasons'])

    def test_reaction_request_before_event_receipt(self):
        with self.store.db() as c:c.execute('UPDATE price_reactions SET requested_at=?',(iso(self.at),))
        self.assertIn('invalid_market_observation',self.reports()[0]['reasons'])

    def test_wrong_report_event_rejected(self):
        self.mutate_report(event_at=iso(self.at-timedelta(seconds=1)))
        self.assertIn('invalid_market_observation',self.reports()[0]['reasons'])

    def test_adjusted_report_rejected(self):
        self.mutate_report(adjustment='all')
        self.assertIn('invalid_market_observation',self.reports()[0]['reasons'])

    def test_conflicting_duplicate_bar_rejected(self):
        self.bars.append(dict(self.bars[-1],c=500))
        with self.store.db() as c:c.execute('UPDATE price_reactions SET raw_bars=?',(json.dumps(self.bars),))
        self.assertIn('invalid_market_observation',self.reports()[0]['reasons'])

    def test_feed_mismatch_rejected(self):
        self.mutate_report(feed='sip')
        self.assertIn('invalid_market_observation',self.reports()[0]['reasons'])

    def test_nan_bar_rejected(self):
        self.bars[-1]['c']=float('nan')
        with self.store.db() as c:c.execute('UPDATE price_reactions SET raw_bars=?',(json.dumps(self.bars),))
        self.assertIn('invalid_market_observation',self.reports()[0]['reasons'])

    def test_hard_link_to_source_rejected(self):
        import os
        os.link(self.source,self.audit)
        with self.assertRaisesRegex(ValueError,'separate'):self.reports()

    def test_audit_dedup_after_time_changes_but_inputs_same(self):
        self.reports();run(self.source,self.audit,self.now+timedelta(seconds=1))
        with sqlite3.connect(self.audit) as c:self.assertEqual(c.execute('SELECT count(*) FROM shadow_observations').fetchone()[0],1)

    def test_changed_input_gets_append_not_overwrite(self):
        self.reports()
        with self.store.db() as c:c.execute("UPDATE health SET status='error'")
        self.reports()
        with sqlite3.connect(self.audit) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM shadow_observations').fetchone()[0],2)

    def test_capacity_guard_does_not_touch_source(self):
        with self.audit.open('wb') as handle:handle.truncate(64 * 1024 * 1024)
        before=self.source.read_bytes()
        with self.assertRaisesRegex(ValueError,'capacity'):self.reports()
        self.assertEqual(before,self.source.read_bytes())

    def test_production_hook_has_no_execution_or_market_adapter_import(self):
        import ast
        import research_worker,live_shadow,shadow_event_contract
        for module in (research_worker,live_shadow,shadow_event_contract):
            tree=ast.parse(Path(module.__file__).read_text())
            imports=[node.module for node in ast.walk(tree) if isinstance(node,ast.ImportFrom)]
            self.assertNotIn('shadow_market_inputs',imports)
            self.assertNotIn('active_demo_pipeline',imports)
            self.assertNotIn('paper_adapter',imports)

    def test_worker_hook_is_default_off_and_failures_nonfatal(self):
        from unittest.mock import patch,Mock
        import research_worker
        for enable in ('false','true'):
            fake=Mock();fake.clock.return_value=self.now
            with patch.dict('os.environ',{'ENABLE_SHADOW_OBSERVATIONS':enable}), \
                 patch('sys.argv',['research_worker.py','--db',str(self.source),'--once']), \
                 patch.object(research_worker,'Store',return_value=fake), \
                 patch.object(research_worker,'cycle',return_value={'status':'no_recent_pending_event'}), \
                 patch('live_shadow.run',side_effect=ValueError('sensitive internal text')) as audit, \
                 patch('builtins.print') as output:
                research_worker.main()
                self.assertEqual(audit.call_count,int(enable=='true'))
                fake.heartbeat.assert_called_once_with('research-worker','ok')
                self.assertNotIn('sensitive internal text',str(output.call_args_list))

if __name__=='__main__':unittest.main()
