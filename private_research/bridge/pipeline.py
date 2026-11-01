"""Runnable offline integration of actual retained stream shape and reviewed mapper.
No source transport, credentials, automatic entry, or broker import exists here.
"""
import argparse
import json
from pathlib import Path
from continuation import Policy, observe_entry, observe_exit
from research_bridge import map_article
from retained_stream import read_export, visible_events, canonical, ns, require, Refused, copy
from vendor.evidence_mapper import Refused as MappingRefused


def run(capture, *, bindings, graph, identities, decision_ns, market_session='unknown',
        positions=(), revisions=(), policy=Policy(), max_candidates=1000):
    """max_candidates is per-run compute budget, never a permanent symbol universe.
    All otherwise eligible article versions remain visible for later bounded work.
    """
    require(type(max_candidates) is int and 1<=max_candidates<=100000,'candidate_work_budget')
    observations=[];unresolved=[];deferred=[];considered=0;seen=set()
    for event in visible_events(capture,decision_ns):
        if event['revision_status']!='provider_version_unverified':
            unresolved.append({'article_version':event['version'],'reason':event['revision_status']});continue
        try:
            mapping=map_article(event,bindings=bindings,graph=graph,identities=identities,
                decision_ns=decision_ns,revisions=revisions,allow_synthetic=capture.receipt['synthetic'])
        except (Refused,MappingRefused) as e:
            unresolved.append({'article_version':event['version'],'reason':str(e)});continue
        for candidate in mapping['candidates']:
            for symbol in candidate['symbols']:
                # Distinct relation paths are evidence, not multiple independent
                # entry intents. Conservatively retain the first deterministic
                # path and make duplicate suppression explicit.
                key=(event['version'],candidate['entity_id'],symbol)
                if key in seen: continue
                seen.add(key)
                if considered>=max_candidates:
                    deferred.append({'article_version':event['version'],'symbol':symbol});continue
                considered+=1
                try:
                    observation=observe_entry(mapping=mapping,candidate=candidate,symbol=symbol,
                        quotes=capture.quotes,decision_ns=decision_ns,market_session=market_session,policy=policy)
                except Refused as e:
                    unresolved.append({'article_version':event['version'],'symbol':symbol,'reason':str(e)});continue
                observations.append(observation)
    for position in positions:
        observations.append(observe_exit(position=position,quotes=capture.quotes,decision_ns=decision_ns,source_gaps=capture.gaps,policy=policy))
    return {'schema':'broad-retained-research-batch-v1','capture_receipt':copy(capture.receipt),
        'decision_at_ns':decision_ns,'policy_sha256':policy.digest,'observations':observations,
        'unresolved':unresolved,'deferred':deferred,'work_budget_exhausted':bool(deferred),
        'coverage':{'news':'observed wildcard stream messages only; disconnects/history may be missing',
            'prices':'IEX quotes for grant symbols only','issuer_resolution':'explicit supplied evidence only',
            'rest_pagination_exhaustion':False,'market_complete':False},
        'order_permission':'none','broker_calls':0}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('export');p.add_argument('--expected-head',required=True)
    p.add_argument('--evidence-mode',choices=['fixture','imported_capture'],required=True)
    p.add_argument('--research-input',required=True,help='Local JSON: bindings, graph, identities; optional positions/revisions')
    p.add_argument('--decision-at',required=True,help='Explicit replay/as-of RFC3339 time; never inferred as live freshness')
    p.add_argument('--market-session',choices=['regular','closed','unknown'],default='unknown')
    args=p.parse_args()
    research=json.loads(Path(args.research_input).read_text())
    capture=read_export(args.export,expected_head=args.expected_head,evidence_mode=args.evidence_mode)
    result=run(capture,bindings=research['bindings'],graph=research['graph'],identities=research['identities'],
        positions=research.get('positions',()),revisions=research.get('revisions',()),
        decision_ns=ns(args.decision_at),market_session=args.market_session)
    print(json.dumps(result,sort_keys=True,indent=2,allow_nan=False))

if __name__=='__main__': main()
