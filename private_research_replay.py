"""One bounded private V2 replay. No source DB access, network or durable cursor."""
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sys
import time

MAX_LINE = 2*1024*1024+128*1024
MAX_TOTAL = 4*1024*1024
MAX_PAGES = 32
ROOT = Path(__file__).resolve().parent
EXPECTED_MANIFEST_SHA256 = '3b89954be25ccfd9c434867c9f2fb3c851c68091d17a2ed44e7f69ce1fbbf530'


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def send(value):
    raw = canonical(value)
    if len(raw) > 4096: raise ValueError('report_bound')
    sys.stdout.buffer.write(raw+b'\n'); sys.stdout.buffer.flush()


def no_network(*args,**kwargs): raise RuntimeError('network_forbidden')


def main():
    if len(sys.argv) != 3 or sys.argv[1] != '--private-replay' or not sys.argv[2].isdigit(): return 2
    parent = int(sys.argv[2])
    try:
        # Render's native runtime is Linux. Do not leave an orphan replay after
        # an unexpected source-parent death; this signal applies only to us.
        import ctypes
        if ctypes.CDLL(None,use_errno=True).prctl(1,signal.SIGKILL) != 0: raise ValueError('parent_guard')
        if os.getppid() != parent: raise ValueError('parent_guard')
        import resource
        for kind,limit in ((resource.RLIMIT_AS,128*1024*1024),(resource.RLIMIT_CPU,8),(resource.RLIMIT_CORE,0)):
            _,hard = resource.getrlimit(kind)
            value = limit if hard == resource.RLIM_INFINITY else min(limit,hard)
            resource.setrlimit(kind,(value,value))
        signal.alarm(35)
        if set(os.environ)-{'LC_CTYPE'} or os.environ.get('LC_CTYPE') not in (None,'C.UTF-8') or os.getppid() != parent: raise ValueError('environment_or_parent')
        from resident_source_supervisor import APPROVED_RESIDENT_SOURCE, APPROVED_PRIVATE_REPLAY
        if APPROVED_PRIVATE_REPLAY is not True: raise ValueError('disabled')
        from resident_source_policy import policy
        expected = policy(json.dumps(APPROVED_RESIDENT_SOURCE))
        socket.socket = socket.create_connection = socket.getaddrinfo = no_network
        manifest_raw = (ROOT/'private_research'/'PROVENANCE.json').read_bytes()
        if hashlib.sha256(manifest_raw).hexdigest() != EXPECTED_MANIFEST_SHA256: raise ValueError('provenance')
        manifest = json.loads(manifest_raw)
        # The manifest is installed alongside reviewed source, never supplied by
        # incoming provider content or a caller-selected filesystem path.
        if type(manifest) is not dict or len(manifest) != 9: raise ValueError('provenance')
        for name,digest in manifest.items():
            if type(name) is not str or name.startswith('/') or '..' in Path(name).parts: raise ValueError('provenance')
            if hashlib.sha256((ROOT/'private_research'/name).read_bytes()).hexdigest() != digest: raise ValueError('provenance')
        import live_source_protocol as installed_protocol
        if hashlib.sha256(Path(installed_protocol.__file__).read_bytes()).hexdigest() != manifest[
            'bridge/vendor/stream_contract/live_source_protocol.py']: raise ValueError('protocol_provenance')
        sys.path.insert(0,str(ROOT/'private_research'))
        import resident_projection as projection
        state = projection.initial_state(expected_policy=expected,
            expected_predecessor=expected['predecessor'],evidence_mode='imported_capture')
        total,pages = 0,0
        while pages < MAX_PAGES:
            if os.getppid() != parent: raise ValueError('parent')
            send({'type':'snapshot','cursor_seq':state['cursor_seq'],'cursor_sha256':state['cursor_sha256']})
            raw = sys.stdin.buffer.readline(MAX_LINE+1)
            if not raw.endswith(b'\n') or len(raw) > MAX_LINE: raise ValueError('input_bound')
            total += len(raw); pages += 1
            if total > MAX_TOTAL or os.getppid() != parent: raise ValueError('replay_bound')
            page = projection.decode(raw)
            transition = projection.apply_snapshot(page,state)
            state = transition['next_state']
            # This is a bounded private observation, not a persistent consumer.
            if len(projection.checkpoint_bytes(state)[0]) > MAX_TOTAL: raise ValueError('state_bound')
            if not state['caught_up']: continue
            now = time.time_ns()
            if now < state['high_ns']: raise ValueError('clock')
            result = projection.research_batch(state,research_registry={
                'bindings':[],'identities':{},'graph':[],'revisions':[]},
                decision_ns=now,market_session='unknown',positions=())
            report = {
                'type':'complete','research_state':'complete','research_frontier_seq':state['cursor_seq'],
                'research_frontier_sha256':state['cursor_sha256'],
                'research_checkpoint_sha256':projection.digest(state),
                'research_report_sha256':hashlib.sha256(canonical(result)).hexdigest(),
                'research_news_versions':len(state['events']),
                'research_distinct_articles':len({e['article_id'] for e in state['events']}),
                'research_quotes':len(state['quotes']),
                'research_control_receipts':len(state['control_receipts']),
                'research_gap_records':len(state['gaps']),
                'research_quarantined_frames':sum(g['gap_reason']=='invalid_provider_frame' for g in state['gaps']),
                'research_observations':len(result.get('observations',[])),
                'research_unresolved':len(result.get('unresolved',[])),
                'research_deferred':len(result.get('deferred',[])),
                'research_pages':pages,'research_input_bytes':total,
                'research_reason':'issuer_registry_missing',
                'research_checkpoint_persisted':False,'research_execution_ready':False,
                'research_provider_coverage_complete':False,'research_order_permission':'none',
                'research_source_requests':0}
            if result.get('blocked_reason'):
                report['research_reason'] = ('source_not_ready' if result['blocked_reason']=='source_not_subscribed_in_retained_protocol'
                    else 'consumer_incomplete')
            if os.getppid() != parent: raise ValueError('parent_guard')
            send(report)
            return 0
        raise ValueError('page_bound')
    except Exception:
        # Never emit exception text, article text, symbols, prices or raw rows.
        send({'type':'refused','research_state':'refused','research_reason':'replay_refused'})
        return 1


if __name__ == '__main__': raise SystemExit(main())
