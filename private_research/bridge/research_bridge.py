"""Point-in-time stream article → reviewed supplied-evidence mapper.
Provider tags alone are NOT issuer identities. No HTTP or automatic beneficiary claim.
"""
from hashlib import sha256
from urllib.parse import urlsplit
from retained_stream import Refused, require, ns, iso_ceil, copy, canonical
from vendor.evidence_mapper import map_event


def map_article(event, *, bindings, graph, identities, decision_ns, revisions=(), allow_synthetic=False):
    """bindings are independently supplied records, not guessed from news text:
    {provider_tag, entity_key, available_at, evidence_ref}. Tag matches are exact.
    A macro/untagged event may use article_id instead of provider_tag, with citation.
    """
    event,bindings,graph,identities,revisions=copy([event,bindings,graph,identities,list(revisions)])
    require(type(decision_ns) is int and event['first_received_at_ns']<=decision_ns,'event_not_available')
    require(event.get('revision_status','provider_version_unverified')=='provider_version_unverified','event_revision_not_current')
    require(type(bindings) is list and len(bindings)<=10000,'binding_budget')
    require(type(identities) is dict,'identity_registry')
    for identity in identities.values():
        require(type(identity) is dict and type(identity.get('synthetic')) is bool,'identity_synthetic_required')
    identities={k:v for k,v in identities.items() if ns(v['available_at'])<=decision_ns}
    if not allow_synthetic:
        identities={k:v for k,v in identities.items() if not v['synthetic']}
    keys,used=[],[]
    for b in bindings:
        require(type(b) is dict and type(b.get('entity_key')) is str and
                type(b.get('evidence_ref')) is str and bool(b['evidence_ref']),'binding_shape')
        require(type(b.get('synthetic')) is bool,'binding_synthetic_required')
        require(('provider_tag' in b) != ('article_id' in b),'binding_kind')
        if ns(b['available_at'])>decision_ns: continue
        match=(b.get('provider_tag') in event['article']['symbols'] if 'provider_tag' in b
               else b['article_id']==event['article_id'])
        if match:
            require(allow_synthetic or not b['synthetic'],'synthetic_binding_excluded')
            keys.append(b['entity_key']); used.append(b)
    require(bool(keys),'unresolved_event_identity')
    url=event['article'].get('url'); parsed=urlsplit(url or '')
    require(parsed.scheme=='https' and parsed.hostname and not parsed.username and not parsed.password,'missing_or_unsafe_source_url')
    # mapper's microsecond timestamps are rounded up; decision rounds down to
    # avoid availability of a 1ns-future item when the clock has finer precision.
    at=iso_ceil(decision_ns-decision_ns%1000)
    supplied={'id':'alpaca-news:'+event['article_id'],'version':event['version'],
        'entity_keys':list(dict.fromkeys(keys)),'effective_at':iso_ceil(event['updated_at_ns']),
        'observed_at':iso_ceil(event['first_received_at_ns']),
        'available_at':iso_ceil(event['first_received_at_ns']),
        'source_url':url,'summary':event['article'].get('headline') or event['article'].get('summary') or 'Provider article without summary',
        'synthetic':event['synthetic'] or any(b['synthetic'] for b in used) or any(i['synthetic'] for i in identities.values())}
    result=map_event(supplied,graph,identities,decision_at=at,now=at,revisions=revisions,allow_synthetic=allow_synthetic)
    result['stream_provenance']={'article_version':event['version'],'evidence':event['evidence'],
        'first_received_at_ns':event['first_received_at_ns'],'provider_updated_at_ns':event['updated_at_ns'],
        'evidence_mode':event['evidence_mode'],'binding_citations':used,
        'bindings_authenticated':False,'provider_tags_are_issuer_identity':False,
                'observed_gaps':[g for g in event.get('observed_gaps',[]) if g['received_at_ns']<=decision_ns]}
    # Preserve mapper identity while binding the added stream/citation context.
    result['bridge_id']=sha256(canonical(result)).hexdigest()
    return result
