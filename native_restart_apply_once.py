"""Disabled, fixed one-time invocation. No environment/request activation.

Only explicit operator approval may change BOTH literal gates. The separate
attempt/backup directory consumes a startup attempt before child launch; never
remove it or retry automatically. Existing business state is never reset.
"""
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

import native_restart_identity as identity_module
import native_restart_migration as migration
import native_restart_preflight as preflight
import runtime_state_bundle as bundle

ENABLE_APPROVED_MIGRATION = False
EXTERNAL_WRITER_QUIESCENCE_CONFIRMED = False
APPROVED_SNAPSHOT_DIGEST = 'dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d'
EXPECTED_CORE_CODE_DIGEST = 'da12f995c97a3502ab88c90482db37a109a576e9865d19f40bac2cbbf67210ad'
EXPECTED_SNAPSHOT_BYTES = 1896794
BACKUP_ROOT = '/var/data/native-restart-identity-backup-' + APPROVED_SNAPSHOT_DIGEST
ATTEMPT_FILE = 'attempt.json'
SNAPSHOT_DIRECTORY = 'snapshot'
FORMAT = 'native-restart-apply-once-v1'
MIN_FREE_BYTES = 64 * 1024 * 1024
MIN_FREE_INODES = 32
TIMEOUT_SECONDS = 30


class InvocationRefused(ValueError):
    pass


def require(ok, code):
    if not ok:raise InvocationRefused(code)


def report(reason, **extra):
    value=dict(mode='native_restart_identity_apply_once',reason=reason,
        provider_requests=0,order_permission='none',execution_ready=False,
        trading_state_reset=False,automatic_retry=False)
    value.update(extra);return value


def emit(value):
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
    require(len(raw)<=4096,'report_bound_exceeded')
    print(raw,flush=True)


def _arguments(*, test_only, paths=None, backup_root=None, approved_digest=None, expected_bytes=None, checkpoint=None):
    require(type(test_only) is bool,'invalid_arguments')
    require(test_only or (paths is None and backup_root is None and approved_digest is None
            and expected_bytes is None and checkpoint is None),'fixed_invocation_required')
    root=BACKUP_ROOT if backup_root is None else backup_root
    digest=APPROVED_SNAPSHOT_DIGEST if approved_digest is None else approved_digest
    size=EXPECTED_SNAPSHOT_BYTES if expected_bytes is None else expected_bytes
    require(type(root) is str and Path(root).is_absolute() and '..' not in Path(root).parts
            and str(Path(root))==root,'backup_path_invalid')
    require(type(digest) is str and len(digest)==64 and all(c in '0123456789abcdef' for c in digest),
            'approval_digest_invalid')
    require(type(size) is int and 0<size<=16*1024*1024,'snapshot_size_invalid')
    return root,digest,size


def _approved():
    require(ENABLE_APPROVED_MIGRATION is True and EXTERNAL_WRITER_QUIESCENCE_CONFIRMED is True,
            'approval_not_enabled')
    require(preflight.source_digest()==EXPECTED_CORE_CODE_DIGEST,'frozen_code_mismatch')


def _space(fd):
    info=os.fstatvfs(fd)
    require(info.f_frsize>0 and info.f_bavail*info.f_frsize>=MIN_FREE_BYTES,'insufficient_free_space')
    require(info.f_favail>=MIN_FREE_INODES,'insufficient_free_inodes')


def _directory(path):
    bundle._parents(str(Path(path)/'placeholder'))
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        info=os.fstat(fd);named=os.stat(path,follow_symlinks=False)
        require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid()
                and stat.S_IMODE(info.st_mode)==0o700 and
                (info.st_dev,info.st_ino)==(named.st_dev,named.st_ino),'unsafe_attempt_directory')
        return fd,(info.st_dev,info.st_ino)
    except BaseException:os.close(fd);raise


def _check_directory(path,identity):
    fd,actual=_directory(path)
    try:require(actual==identity,'attempt_directory_changed')
    finally:os.close(fd)


def _record(digest,size,status):
    return dict(format=FORMAT,status=status,snapshot_digest=digest,
                core_code_digest=EXPECTED_CORE_CODE_DIGEST,snapshot_bytes=size,
                backup_directory=SNAPSHOT_DIRECTORY)


def _reserve_attempt(root,digest,size):
    """Parent only: durable O_EXCL directory is the no-retry startup fence."""
    parent=Path(root).parent;name=Path(root).name
    bundle._parents(root)
    fd=os.open(str(parent),os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        _space(fd)
        # Any existing entry, including an incomplete or foreign one, consumes
        # this fixed attempt. It is never replaced, cleared or reused.
        try:os.mkdir(name,mode=0o700,dir_fd=fd)
        except FileExistsError:return False
        os.fsync(fd)
        directory,identity=_directory(root)
        try:
            _space(directory)
            claim=os.open(ATTEMPT_FILE,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory)
            try:bundle.RuntimeStateBundle._write(claim,bundle._json(_record(digest,size,'pending')))
            finally:os.close(claim)
            os.fsync(directory);os.fsync(fd);_check_directory(root,identity)
        finally:os.close(directory)
        return True
    finally:os.close(fd)


def _completed_attempt(root,digest,size,*,paths=None,test_only=False):
    """Readonly restart check. A success file alone is never authoritative."""
    directory,pinned=_directory(root)
    claim=None
    try:
        claim=os.open(ATTEMPT_FILE,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
        info=os.fstat(claim);named=os.stat(ATTEMPT_FILE,dir_fd=directory,follow_symlinks=False)
        require(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600
                and info.st_nlink==1 and (info.st_dev,info.st_ino)==(named.st_dev,named.st_ino),
                'unsafe_attempt_record')
        fcntl.flock(claim,fcntl.LOCK_SH|fcntl.LOCK_NB)
        raw=os.read(claim,4097)
        require(len(raw)<=4096 and bundle._load(raw)==_record(digest,size,'completed'),
                'attempt_incomplete_or_unknown')
        state=migration._binding(paths,test_only)
        state.verify()
        with bundle._database(state.paths['anchor'],state._identity('anchor'),public_read=True) as db:
            record=identity_module.read_record(db)
            require(record is not None and record[0][2]=='completed' and record[0][5]==digest,
                    'completed_cohort_mismatch')
        _check_directory(root,pinned)
        current=os.stat(ATTEMPT_FILE,dir_fd=directory,follow_symlinks=False)
        require((current.st_dev,current.st_ino)==(info.st_dev,info.st_ino),'attempt_record_changed')
        return True
    finally:
        if claim is not None:os.close(claim)
        os.close(directory)


def _safe_error(exc):
    safe={'approval_not_enabled','frozen_code_mismatch','invalid_arguments','fixed_invocation_required',
          'backup_path_invalid','approval_digest_invalid','snapshot_size_invalid','insufficient_free_space',
          'insufficient_free_inodes','unsafe_attempt_directory','attempt_directory_changed',
          'unsafe_attempt_record','attempt_already_started','attempt_incomplete_or_unknown',
          'completed_cohort_mismatch','approved_snapshot_changed',
          'approved_snapshot_size_changed','backup_directory_changed','attempt_record_changed'}
    code=exc.args[0] if len(exc.args)==1 and type(exc.args[0]) is str else None
    return code if code in safe else preflight.safe_error(exc)


def run_once(*,test_only=False,paths=None,backup_root=None,approved_digest=None,expected_bytes=None,_checkpoint=None):
    stage='approval';claim=None;directory=None
    try:
        _approved()
        root,digest,size=_arguments(test_only=test_only,paths=paths,backup_root=backup_root,
            approved_digest=approved_digest,expected_bytes=expected_bytes,checkpoint=_checkpoint)
        stage='attempt_claim'
        directory,root_identity=_directory(root)
        claim=os.open(ATTEMPT_FILE,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
        info=os.fstat(claim);named=os.stat(ATTEMPT_FILE,dir_fd=directory,follow_symlinks=False)
        claim_identity=(info.st_dev,info.st_ino)
        require(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600
                and info.st_nlink==1 and claim_identity==(named.st_dev,named.st_ino),'unsafe_attempt_record')
        fcntl.flock(claim,fcntl.LOCK_EX|fcntl.LOCK_NB)
        raw=os.read(claim,4097)
        require(len(raw)<=4096,'unsafe_attempt_record')
        actual=bundle._load(raw)
        require(actual==_record(digest,size,'pending'),'attempt_already_started')
        bundle.RuntimeStateBundle._write(claim,bundle._json(_record(digest,size,'started')))
        os.fsync(directory);_check_directory(root,root_identity)
        stage='snapshot_prepare'
        plan=migration.prepare(paths=paths,test_only=test_only)
        require(plan.digest==digest,'approved_snapshot_changed')
        require(sum(v['size'] for v in json.loads(plan.snapshot).values())==size,'approved_snapshot_size_changed')
        _space(directory)
        stage='backup_directory'
        os.mkdir(SNAPSHOT_DIRECTORY,mode=0o700,dir_fd=directory)
        os.fsync(directory)
        backup=str(Path(root)/SNAPSHOT_DIRECTORY)
        backup_fd,backup_identity=_directory(backup)
        try:
            require(backup_identity[0]==root_identity[0] and not os.listdir(backup_fd),'backup_directory_changed')
            os.fsync(backup_fd)
        finally:os.close(backup_fd)
        _check_directory(root,root_identity)
        current=os.stat(ATTEMPT_FILE,dir_fd=directory,follow_symlinks=False)
        require((current.st_dev,current.st_ino)==claim_identity,'attempt_record_changed')
        stage='apply'
        result=migration.apply(enabled=True,plan=plan,approved_digest=digest,
            quiescence_confirmed=EXTERNAL_WRITER_QUIESCENCE_CONFIRMED,
            backup_directory=backup,paths=paths,test_only=test_only,_checkpoint=_checkpoint)
        _check_directory(root,root_identity)
        current=os.stat(ATTEMPT_FILE,dir_fd=directory,follow_symlinks=False)
        require((current.st_dev,current.st_ino)==claim_identity,'attempt_record_changed')
        bundle.RuntimeStateBundle._write(claim,bundle._json(_record(digest,size,'completed')))
        os.fsync(directory);_check_directory(root,root_identity)
        current=os.stat(ATTEMPT_FILE,dir_fd=directory,follow_symlinks=False)
        require((current.st_dev,current.st_ino)==claim_identity,'attempt_record_changed')
        return report('migration_completed',metadata_outcome='completed',attempt_consumed=True,
            snapshot_matches_approved=True,business_rows_preserved=result['business_rows_preserved'],
            historical_continuity_proven=False)
    except Exception as exc:
        return report('migration_invocation_refused',code=_safe_error(exc),stage=stage,
            metadata_outcome='unknown',
            attempt_consumed=directory is not None)
    finally:
        if claim is not None:os.close(claim)
        if directory is not None:os.close(directory)


def run_startup_gate(*,inspection_only,stop,test_only=False,paths=None,backup_root=None,approved_digest=None,
                     expected_bytes=None,popen=None,monotonic=None,emit_report=emit):
    """Return only after the fixed child is reaped. False forbids web startup."""
    if ENABLE_APPROVED_MIGRATION is False:return True
    child=None;clock=monotonic or time.monotonic
    try:
        require(inspection_only is True,'approval_not_enabled');_approved()
        root,digest,size=_arguments(test_only=test_only,paths=paths,backup_root=backup_root,
            approved_digest=approved_digest,expected_bytes=expected_bytes)
        if stop.is_set():return False
        if os.path.lexists(root) or not _reserve_attempt(root,digest,size):
            _completed_attempt(root,digest,size,paths=paths,test_only=test_only)
            emit_report(report('completed_attempt_verified_no_repeat',metadata_outcome='completed',attempt_consumed=True))
            return not stop.is_set()
        if stop.is_set():return False
        emit_report(report('approved_attempt_reserved',metadata_outcome='not_started',attempt_consumed=True))
        child=(popen or subprocess.Popen)([sys.executable,'-B',str(Path(__file__).resolve()),'--apply-approved-once'],
            stdin=subprocess.DEVNULL,stdout=None,stderr=subprocess.DEVNULL,shell=False,close_fds=True)
        deadline=clock()+TIMEOUT_SECONDS
        while True:
            code=child.poll()
            if code is not None:
                emit_report(report('apply_child_reaped',child_success=code==0,metadata_outcome='see_child_report'))
                if code!=0:return False
                _completed_attempt(root,digest,size,paths=paths,test_only=test_only)
                return not stop.is_set()
            if stop.is_set() or clock()>=deadline:break
            stop.wait(.05)
        child.kill()
        try:child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            emit_report(report('apply_reap_uncertain',metadata_outcome='unknown',attempt_consumed=True));return False
        emit_report(report('apply_cancelled' if stop.is_set() else 'apply_watchdog_expired',
            metadata_outcome='unknown',attempt_consumed=True))
        return False
    except Exception as exc:
        if child is not None:
            try:
                if child.poll() is None:child.kill();child.wait(timeout=1)
            except Exception:
                emit_report(report('apply_reap_uncertain',metadata_outcome='unknown',attempt_consumed=True));return False
        emit_report(report('startup_apply_refused',code=_safe_error(exc),metadata_outcome='unknown'))
        return False


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if args!=['--apply-approved-once']:
        emit(report('migration_invocation_refused',code='invalid_arguments',metadata_outcome='not_started'));return 1
    result=run_once();emit(result)
    return 0 if result['reason']=='migration_completed' else 1


if __name__=='__main__':raise SystemExit(main())
