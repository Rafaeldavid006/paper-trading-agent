"""Synthetic schema-2 integration; no live mount, source or broker requests."""
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import enrollment
import filesystem as fs
import owned_transport
import production_release as policy
import recurring
from recurring import LeaseCollector, Lease


class StorageIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.parent=Path(self.temp.name);self.path=self.parent/'state'

    def create(self):
        c=LeaseCollector(self.path,'storage_test',create=True,offline_test_enabled=True)
        self.addCleanup(c.close)
        return c

    def reopen(self):
        c=LeaseCollector(self.path,'storage_test',offline_test_enabled=True)
        self.addCleanup(c.close)
        return c

    def test_three_real_nonce_markers_and_no_upstream_schema_adoption(self):
        c=self.create(); nonce=c.root.nonce
        self.assertEqual(json.loads((self.path/'owner.lock').read_text()),{'schema':2,'nonce':nonce})
        for name in fs.DATABASES:
            db=c.root.open_database(name,readonly=True)
            try:self.assertEqual(db.execute('SELECT id,schema,nonce FROM collector_storage_identity').fetchall(),[(1,2,nonce)])
            finally:db.close()
        self.assertEqual(c.root.config['storage_schema'],2)
        c.close();self.reopen().verify()

    def test_fresh_instance_accepts_coherent_device_remap_without_rewriting_original(self):
        c=self.create(); old_config=json.loads(json.dumps(c.root.config));raw=(self.path/'manifest.json').read_bytes()
        now=int(time.time());lease=Lease('before_remount',now,now+60,10,2,131072,'synthetic')
        c.add_lease(lease);c.close()
        original=fs.stamp
        def remap(s):return {**original(s),'device':s.st_dev+1000}
        with patch.object(fs,'stamp',side_effect=remap):
            reopened=self.reopen();reopened.verify()
            self.assertEqual(reopened.root.config,old_config)
            self.assertEqual(reopened.db.execute('SELECT COUNT(*) FROM leases').fetchone()[0],1)
            reopened.close()
        self.assertEqual((self.path/'manifest.json').read_bytes(),raw)

    def test_retained_instance_never_repins_device(self):
        c=self.create(); original=fs.stamp
        with patch.object(fs,'stamp',side_effect=lambda s:{**original(s),'device':s.st_dev+1}):
            with self.assertRaisesRegex(ValueError,'physical_identity_changed'):c.verify()

    def test_fresh_instance_rejects_mixed_device(self):
        c=self.create();inode=(self.path/'ledger.sqlite').stat().st_ino;c.close();original=fs.stamp
        with patch.object(fs,'stamp',side_effect=lambda s:{**original(s),'device':s.st_dev+(1 if s.st_ino==inode else 0)}):
            with self.assertRaisesRegex(ValueError,'coherent_current_device'):self.reopen()

    def test_missing_database_never_recreated(self):
        c=self.create();c.close();(self.path/'ledger.sqlite').unlink()
        with self.assertRaises((ValueError,OSError)):self.reopen()
        self.assertFalse((self.path/'ledger.sqlite').exists())

    def test_duplicated_owner_never_changes_lock(self):
        c=self.create();before=(self.path/'owner.lock').read_bytes()
        with self.assertRaises((ValueError,OSError)):self.reopen()
        self.assertEqual((self.path/'owner.lock').read_bytes(),before);c.verify()

    def test_lock_replacement_fences_sql_before_write(self):
        c=self.create();p=self.path/'owner.lock';raw=p.read_bytes();p.rename(self.path/'previous.lock');p.write_bytes(raw);p.chmod(0o600)
        with self.assertRaisesRegex(ValueError,'physical_identity_changed'):c.db.execute("UPDATE gates SET revoked=1")
        p.unlink();(self.path/'previous.lock').rename(p)
        self.assertEqual(c.db.execute('SELECT revoked FROM gates').fetchone()[0],0)

    def test_private_setgid_root_supported_without_chmod_parent(self):
        self.parent.chmod(0o2700);c=self.create()
        self.assertEqual(self.path.stat().st_mode & 0o7777,0o2700)
        self.assertEqual(self.parent.stat().st_mode & 0o7777,0o2700)
        c.close();self.reopen().verify()

    def test_nonproduction_group_writable_parent_stays_refused(self):
        self.parent.chmod(0o2770)
        with self.assertRaisesRegex(ValueError,'exclusive_owned'):self.create()
        self.assertFalse(self.path.exists())

    def test_hardlinked_database_and_changed_permissions_refused(self):
        c=self.create();c.close();p=self.path/'ledger.sqlite';os.link(p,self.parent/'duplicate')
        with self.assertRaisesRegex(ValueError,'private_owned'):self.reopen()
        (self.parent/'duplicate').unlink();p.chmod(0o640)
        with self.assertRaisesRegex(ValueError,'private_owned'):self.reopen()

    def test_cohort_marker_immutable_and_mismatch_detected(self):
        c=self.create()
        with self.assertRaises(sqlite3.IntegrityError):c.db.execute("UPDATE collector_storage_identity SET nonce=?",('0'*64,))
        c.db.execute('DROP TRIGGER no_collector_storage_identity_UPDATE')
        c.db.execute("UPDATE collector_storage_identity SET nonce=?",('0'*64,))
        with self.assertRaisesRegex(ValueError,'cohort'):c.verify()
        c.close()
        with self.assertRaisesRegex(ValueError,'cohort'):self.reopen()

    def test_transport_last_boundary_refusal_prevents_spawn(self):
        spawned=[]
        def deny():raise ValueError('synthetic_anchor_changed')
        result=owned_transport.collect('ok',stop=threading.Event(),deadline_mono=time.monotonic()+1,
                                       before_spawn=deny,popen=lambda *a,**kw:spawned.append(a))
        self.assertEqual(spawned,[]);self.assertIsNone(result.child_pid);self.assertIsNot(result.reaped,True)

    def test_anchor_callback_elapsed_deadline_does_not_launch(self):
        spawned=[];clock=[0.0]
        def advance():clock[0]=2.0
        result=owned_transport.collect('ok',stop=threading.Event(),deadline_mono=1.0,
                                       mono=lambda:clock[0],before_spawn=advance,
                                       popen=lambda *a,**kw:spawned.append(a))
        self.assertEqual(spawned,[]);self.assertEqual(result.reason,'deadline_before_spawn')
        self.assertTrue(result.reaped);self.assertIsNone(result.child_pid)

    def test_anchor_callback_stop_does_not_launch(self):
        spawned=[];stop=threading.Event()
        result=owned_transport.collect('ok',stop=stop,deadline_mono=time.monotonic()+1,
                                       before_spawn=stop.set,popen=lambda *a,**kw:spawned.append(a))
        self.assertEqual(spawned,[]);self.assertEqual(result.reason,'cancelled_before_spawn')
        self.assertTrue(result.reaped);self.assertIsNone(result.child_pid)

    def test_root_swap_at_physical_launch_keeps_claim_and_fences_reopen(self):
        c=self.create();now=int(time.time());c.add_lease(Lease('source_free_boundary',now,now+60,10,2,131072,'synthetic'))
        real_collect=recurring.collect;spawned=[];moved=self.parent/'moved'
        def race(*a,**kw):
            self.path.rename(moved);self.path.mkdir(mode=0o700)
            try:return real_collect(*a,**kw)
            finally:self.path.rmdir();moved.rename(self.path)
        with patch.object(recurring,'collect',side_effect=race):
            status=c.tick(_popen=lambda *a,**kw:spawned.append(a))
        self.assertEqual(spawned,[]);self.assertEqual(status['attempts'],1)
        c.close();reopened=self.reopen();self.assertEqual(reopened.status()['attempts'],1)
        reopened.tick(_popen=lambda *a,**kw:spawned.append(a));self.assertEqual(spawned,[])

    def test_snapshot_rechecks_file_replacement(self):
        c=self.create();orig=os.stat;count=0
        def race(path,*args,**kwargs):
            nonlocal count
            value=orig(path,*args,**kwargs)
            if path=='evidence.sqlite' and kwargs.get('dir_fd')==c.root.anchor.root_fd:
                count+=1
                if count==1:
                    p=self.path/'ledger.sqlite';p.rename(self.path/'old-ledger.sqlite');p.write_bytes(b'');p.chmod(0o600)
            return value
        with patch('filesystem.os.stat',side_effect=race):
            with self.assertRaisesRegex(ValueError,'snapshot_changed'):c.root.anchor.snapshot()

    def test_shared_profile_classification_exact_and_nonowned(self):
        source=self.parent.stat()
        def info(uid,gid,mode):
            values=list(source);values[0]=mode;values[4]=uid;values[5]=gid
            return os.stat_result(values)
        good=info(os.getuid()+1,os.getegid(),0o42775)
        with patch('filesystem.os.getgroups',return_value=[os.getegid()]):
            self.assertEqual(fs.parent_policy(good,shared=True),fs.STORAGE_PROFILE)
            for bad in (info(os.getuid(),os.getegid(),0o42775), info(os.getuid()+1,os.getegid(),0o42777), info(os.getuid()+1,os.getegid()+1,0o42775)):
                with self.assertRaises(ValueError):fs.parent_policy(bad,shared=True)

    def test_symlink_sidecar_refused_before_sqlite_open(self):
        c=self.create();c.close();target=self.parent/'sentinel';target.write_bytes(b'untouched')
        sidecar=self.path/'ledger.sqlite-wal'
        if sidecar.exists():sidecar.unlink()
        sidecar.symlink_to(target)
        with patch('filesystem.sqlite3.connect') as connect:
            with self.assertRaisesRegex(ValueError,'private_owned'):self.reopen()
            connect.assert_not_called()
        self.assertEqual(target.read_bytes(),b'untouched')

    def test_persisted_bool_device_is_not_integer_stamp(self):
        c=self.create();nodes=c.root.anchor.snapshot();corrupt=json.loads(json.dumps(nodes))
        for value in corrupt.values():value['device']=True
        with self.assertRaisesRegex(ValueError,'stamp_schema'):fs.validate_nodes(corrupt,nodes)




import test_enrollment_owner as enrollment_fixtures

class RetainedReceiptTests(unittest.TestCase):
    setUp=enrollment_fixtures.EnrollmentOwnerTests.setUp
    tearDown=enrollment_fixtures.EnrollmentOwnerTests.tearDown
    enroll=enrollment_fixtures.EnrollmentOwnerTests.enroll

    def collect_release(self, receipt=None):
        _,_,complete=enrollment._paths(self.release)
        return replace(self.release,phase='collect',enrollment_receipt_sha256=receipt or fs.digest(json.loads(complete.read_text())))

    def test_wrong_receipt_constructor_refuses_before_any_rw_database_open(self):
        self.enroll();release=self.collect_release('0'*64);opened=[];original=fs.PrivateRoot.open_database
        def record(root,name,**kwargs):
            opened.append(kwargs.get('readonly',False));return original(root,name,**kwargs)
        with patch.object(policy,'APPROVED_RELEASE',release),patch.object(policy,'ENROLLMENT_APPROVED',False),patch.object(fs.PrivateRoot,'open_database',record):
            with self.assertRaisesRegex(ValueError,'receipt_pin_mismatch'):
                LeaseCollector(self.root,release.collector_id,live_release=release)
        self.assertTrue(opened);self.assertTrue(all(opened))

    def test_prior_verified_receipt_does_not_bypass_constructor_recheck(self):
        self.enroll();release=self.collect_release();opened=[];original=fs.PrivateRoot.open_database
        def record(root,name,**kwargs):
            opened.append(kwargs.get('readonly',False));return original(root,name,**kwargs)
        with patch.object(policy,'APPROVED_RELEASE',release),patch.object(policy,'ENROLLMENT_APPROVED',False):
            enrollment.verify_enrollment(release,_test_mount=True)
            _,_,complete=enrollment._paths(release);done=json.loads(complete.read_text());done['nonce']='f'*64;complete.write_text(json.dumps(done))
            with patch.object(fs.PrivateRoot,'open_database',record),self.assertRaisesRegex(ValueError,'receipt_pin_mismatch'):
                LeaseCollector(self.root,release.collector_id,live_release=release)
        self.assertTrue(all(opened))

    def test_partial_enroll_root_requires_explicit_bootstrap_reopen(self):
        def crash(where):
            if where=='after_root':raise RuntimeError('synthetic_crash')
        with self.assertRaises(RuntimeError):self.enroll(_fault=crash)
        with self.assertRaises((ValueError,OSError)):
            LeaseCollector(self.root,self.release.collector_id,live_release=self.release)
        _,_,complete=enrollment._paths(self.release);self.assertFalse(complete.exists())

if __name__=='__main__':unittest.main()
