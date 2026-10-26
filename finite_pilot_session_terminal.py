"""Bounded local revocation/expiry receipt, NEVER a provider/recovery operation."""
import json,sys,time
from contextlib import ExitStack
import finite_pilot_contract as c
from finite_pilot_contract import require,digest
from finite_pilot_anchor import resolve_approved_bundle
from finite_pilot_sessions import SessionJournal,_load,_append
from finite_pilot_session_admission import _session_clean_runner
from readonly_runtime_worker import serialized
from native_account_guard import SharedAccountGuard
import runtime_state_bundle as inventory


def record(action,*,paths=None,test_only=False,now_ns=None):
    manifest=c.require_pilot_manifest(c.APPROVED_PILOT)
    require(type(manifest.session_admission) is c.SessionAdmission,'typed_session_required')
    require(action in ('halt','close'),'session_terminal_action_invalid')
    require(type(test_only) is bool and (test_only or paths is None and now_ns is None),'production_overrides_forbidden')
    clock=time.time_ns if now_ns is None else now_ns
    now=clock();a=manifest.session_admission
    require(type(now) is int and manifest.starts_ns<=now<a.close_expires_ns,'session_close_window_invalid')
    if action=='close':require(now>=manifest.expires_ns,'session_close_before_expiry')
    bundle=resolve_approved_bundle(manifest,paths=paths,test_only=test_only);journal=SessionJournal(bundle,clock)
    guard=SharedAccountGuard(bundle.paths['legacy'],bundle.paths['lifecycle'],bundle.binding,
        enabled=True,state_database=bundle.paths['guard'],test_only=test_only,runtime_bundle=bundle)
    with serialized(bundle),guard.hold(now_ns=now,read_only=True):
        journal._verify(manifest)
        with ExitStack() as held:
            handles={role:held.enter_context(inventory._database(bundle.paths[role],bundle._pinned[role],write=True,public_read=role=='anchor'))
                for role in ('runner','decision','lifecycle','source','guard','anchor')}
            db=handles['anchor']
            journal._binding_row(db,manifest)
            docs,rows,counts,events=_load(db,manifest)
            require(manifest.pilot_id in rows and docs[manifest.pilot_id].document()==manifest.document(),'session_not_enrolled')
            row=rows[manifest.pilot_id]
            require(now>=row['last_ns'],'session_clock_regression')
            if row['closed']:return dict(phase='already_closed',order_permission='none',provider_requests=0)
            if action=='halt':
                final=clock()
                require(type(final) is int and now<=final<a.close_expires_ns and final>=row['last_ns'],'session_close_window_invalid')
                if not row['halted']:_append(db,manifest.pilot_id,'HALT',final,dict(reason='supervisor_terminal'))
                _load(db,manifest);db.commit()
                return dict(phase='halted',order_permission='none',provider_requests=0)
            clean=row['applied'] is not None and row['dirty']==0 and row['writes']==0
            if clean:
                try:
                    journal._controls()
                    with inventory._database(bundle.paths['runner'],bundle._pinned['runner']) as runner:
                        _session_clean_runner(inventory._singleton(runner,'runner_control'))
                    with inventory._database(bundle.paths['lifecycle'],bundle._pinned['lifecycle']) as life:
                        require(all(life.execute('SELECT COUNT(*) FROM '+name).fetchone()==(0,)
                            for name in ('intents','cancels','native_dispatch')),'session_order_history_present')
                except (ValueError,TypeError):clean=False
            local=journal.cohort_state_digest()
            final=clock()
            require(type(final) is int and now<=final<a.close_expires_ns,'session_close_window_invalid')
            _append(db,manifest.pilot_id,'CLOSE',final,dict(clean=clean,local_state=local))
            _load(db,manifest);db.commit()
    return dict(phase='scope_ended_clean' if clean else 'scope_ended_unknown',order_permission='none',provider_requests=0,
        broker_state_verified=False)


def main(argv=None):
    args=sys.argv[1:] if argv is None else argv
    if args not in (['--halt'],['--close']):return 2
    try:
        result=record(args[0][2:]);print(json.dumps(dict(mode='finite_session_terminal',**result)),flush=True);return 0
    except Exception:
        print(json.dumps(dict(mode='finite_session_terminal',phase='receipt_refused',order_permission='none',provider_requests=0)),flush=True)
        return 1

if __name__=='__main__':raise SystemExit(main())
