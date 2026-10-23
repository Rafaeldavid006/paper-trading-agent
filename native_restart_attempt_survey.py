"""Untrusted readonly observations of one fixed failed migration attempt.

Never imports/calls the apply launcher, creates files, changes permissions,
repairs SQLite, or grants migration/trading permission. No raw records/IDs log.
"""
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import native_restart_identity as identity
import runtime_state_bundle as bundle
from native_observation_supervisor import NativeProbeSupervisor

SNAPSHOT_DIGEST='dbe5194ac45805beeb51a7f6dca732b487851ec67923be1ea7fdb143b50d3a1d'
CORE_DIGEST='da12f995c97a3502ab88c90482db37a109a576e9865d19f40bac2cbbf67210ad'
ROOT='/var/data/native-restart-identity-backup-'+SNAPSHOT_DIGEST


def _require(ok):
    if not ok:raise ValueError('survey_refused')


def _code(exc):
    if isinstance(exc,FileNotFoundError):return 'absent'
    if isinstance(exc,PermissionError):return 'access_refused'
    if isinstance(exc,BlockingIOError):return 'lock_busy'
    return 'invalid_or_unavailable'


def _type(info):
    return ('directory' if stat.S_ISDIR(info.st_mode) else 'regular' if stat.S_ISREG(info.st_mode)
            else 'symlink' if stat.S_ISLNK(info.st_mode) else 'other')


def _details(info):
    mode=stat.S_IMODE(info.st_mode)
    return dict(present=True,type=_type(info),owner_matches_runtime=info.st_uid==os.geteuid(),
                mode_octal=format(mode,'04o'),private_directory_access=(mode&0o777)==0o700,
                setgid=bool(mode&stat.S_ISGID),setuid=bool(mode&stat.S_ISUID),sticky=bool(mode&stat.S_ISVTX))


def _physical(info):return info.st_dev,info.st_ino


def _stable(before,after):
    return (_physical(before),before.st_mode,before.st_uid,before.st_gid)==(
            _physical(after),after.st_mode,after.st_uid,after.st_gid)


def _observe_directory(path,*,contents=False):
    """Observe special bits; do not reinterpret them as an operational pass."""
    result={};fd=None
    try:
        bundle._parents(path)
        named=os.stat(path,follow_symlinks=False);result.update(_details(named))
        if not stat.S_ISDIR(named.st_mode):return result
        fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_NONBLOCK)
        opened=os.fstat(fd);_require(_stable(named,opened))
        if contents:
            # Bounded enumeration, never emit any filename or file content.
            count=0
            with os.scandir(fd) as entries:
                for _ in entries:
                    count+=1
                    if count>16:break
            result.update(entries_observed=min(count,16),entries_capped=count>16,empty=count==0)
        _require(_stable(opened,os.stat(path,follow_symlinks=False)))
        result['stable_during_observation']=True
    except Exception as exc:
        result.update(observation=_code(exc),stable_during_observation=False)
        if isinstance(exc,FileNotFoundError):result['present']=False
    finally:
        if fd is not None:os.close(fd)
    return result


def _attempt(root):
    result={};fd=None
    try:
        path=str(Path(root)/'attempt.json');bundle._parents(path)
        named=os.stat(path,follow_symlinks=False);result.update(_details(named))
        _require(stat.S_ISREG(named.st_mode) and named.st_uid==os.geteuid()
                 and stat.S_IMODE(named.st_mode)==0o600 and named.st_nlink==1)
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        info=os.fstat(fd);_require(_stable(named,info) and info.st_nlink==1)
        fcntl.flock(fd,fcntl.LOCK_SH|fcntl.LOCK_NB)
        raw=os.read(fd,4097);_require(len(raw)<=4096)
        value=bundle._load(raw);_require(type(value) is dict)
        status=value.get('status');safe_status=status if type(status) is str and status in ('pending','started','completed') else 'unrecognized'
        expected=dict(format='native-restart-apply-once-v1',status=safe_status,snapshot_digest=SNAPSHOT_DIGEST,
                      core_code_digest=CORE_DIGEST,snapshot_bytes=1896794,backup_directory='snapshot')
        result.update(status=safe_status,exact_fixed_record=safe_status in ('pending','started','completed') and value==expected)
        _require(_stable(info,os.stat(path,follow_symlinks=False)))
        result['stable_during_observation']=True
    except Exception as exc:
        result.update(observation=_code(exc),stable_during_observation=False)
        if isinstance(exc,FileNotFoundError):result['present']=False
    finally:
        if fd is not None:os.close(fd)
    return result


def _anchor(path):
    result={}
    try:
        _require(type(path) is str and bundle._canonical(path)==path)
        physical=bundle._private(path,public_read=True)
        # Never even attempt SQLite access when a recovery/sidecar question exists.
        result['sidecars_absent']=not any(os.path.lexists(path+s) for s in bundle._SIDECARS)
        if not result['sidecars_absent']:
            result['observation']='sidecar_present_not_opened';return result
        with bundle._database(path,physical,public_read=True) as db:
            record=identity.read_record(db,allow_prepared=True)
            result['migration_marker']='absent' if record is None else record[0][2]
            if record is not None:result['preimage_matches_fixed']=record[0][5]==SNAPSHOT_DIGEST
        _require(not any(os.path.lexists(path+s) for s in bundle._SIDECARS))
        result['stable_during_observation']=True
    except Exception as exc:
        result.update(observation=_code(exc),stable_during_observation=False)
    return result


def run_once(*,enabled=False,test_only=False,root=None,anchor=None):
    result=dict(mode='native_restart_attempt_survey',reason='attempt_survey_disabled',trusted=False,
                state_write_attempted=False,provider_requests=0,order_permission='none',execution_ready=False,
                recovery_authorized=False,automatic_retry=False,global_quiescence_proven=False)
    if type(enabled) is not bool or type(test_only) is not bool or (not test_only and (root is not None or anchor is not None)):
        result['reason']='attempt_survey_arguments_refused';return result
    if not enabled:return result
    path=ROOT if root is None else root
    try:_require(type(path) is str and bundle._canonical(path)==path)
    except Exception:
        result['reason']='attempt_survey_arguments_refused';return result
    selected_anchor=os.environ.get('EVENT_DB') if anchor is None else anchor
    result.update(reason='attempt_survey_observed',parent=_observe_directory(str(Path(path).parent)),
                  attempt_directory=_observe_directory(path,contents=True),attempt_record=_attempt(path),
                  backup_directory=_observe_directory(str(Path(path)/'snapshot'),contents=True),
                  anchor=_anchor(selected_anchor))
    return result


class FailedAttemptSurveySupervisor(NativeProbeSupervisor):
    """Reuse the existing finite watchdog/reap lifecycle, fixed readonly child."""
    def __init__(self,*,enabled=False,popen=None,**kwargs):
        spawn=popen or subprocess.Popen
        def fixed_spawn(args):
            _require(len(args)==3 and args[0]==sys.executable and args[2]=='--once'
                     and Path(args[1]).name=='native_observation_worker.py')
            return spawn([sys.executable,'-B',str(Path(__file__).resolve()),'--inspect-failed-attempt-and-audit'],
                         stdin=subprocess.DEVNULL,stdout=None,stderr=subprocess.DEVNULL,
                         shell=False,close_fds=True)
        super().__init__(enabled=enabled,popen=fixed_spawn,**kwargs)

    def _report(self,reason):
        safe={'probe_start_failed','probe_watchdog_expired','probe_finished','probe_process_failed','probe_reap_pending'}
        self._emit(json.dumps(dict(mode='native_restart_attempt_survey_supervisor',
            reason=reason if reason in safe else 'survey_supervisor_refused',provider_requests=0,order_permission='none')),flush=True)


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    valid=args in (['--inspect-failed-attempt'],['--inspect-failed-attempt-and-audit'])
    result=run_once(enabled=valid)
    raw=json.dumps(result,sort_keys=True,separators=(',',':'),allow_nan=False)
    _require(len(raw)<=4096);print(raw,flush=True)
    if args==['--inspect-failed-attempt-and-audit']:
        from native_restart_preflight import main as inspect_existing
        return inspect_existing(['--inspect-restart-preflight'])
    return 0 if valid else 1


if __name__=='__main__':raise SystemExit(main())
