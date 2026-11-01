"""Offline bridge from the existing research_edges/research_evidence schema.

No graph database replacement, entity inference, source fetching, price prediction
or execution. Caller-supplied evidence and identity mappings are attestations.
"""
from datetime import datetime, timezone
from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit

VERSION = 'relationship-evidence-map-v1'
RELATIONS = frozenset(('buys_from','uses_technology','competes_with','substitutes_for',
    'sells_to','finances','exposed_to_rate','exposed_to_currency','regulated_by',
    'operates_in','depends_on_logistics','uses_commodity'))

class Refused(ValueError):
    pass


def _require(ok, reason):
    if not ok:
        raise Refused(reason)


def _time(value):
    _require(type(value) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})',value), 'timestamp_precision_or_shape')
    try:
        at = datetime.fromisoformat(value.replace('Z','+00:00'))
    except ValueError:
        raise Refused('invalid_timestamp') from None
    _require(at.tzinfo is not None and at.utcoffset() is not None, 'timezone_required')
    return at.astimezone(timezone.utc)


def _text(value, field):
    _require(type(value) is str and 0 < len(value) <= 16000 and '\x00' not in value, field)
    return value


def _copy(value):
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


def export_existing_graph(store, *, max_edges=10000):
    """Read-only existing-file SELECT using the existing Store's path only.

    Unlike visible_edges(), this includes evidence_recorded_at for independent
    availability checks. Truncation is explicit; max_edges is a work budget.
    """
    _require(type(max_edges) is int and 1 <= max_edges <= 100000, 'edge_read_budget')
    path = Path(store.path).absolute()
    _require(path.is_file() and not path.is_symlink(), 'existing_graph_file_required')
    original = path.stat()
    identity = (original.st_dev, original.st_ino)
    with path.open('rb') as source:
        header = source.read(100)
    _require(len(header)==100 and header[:16]==b'SQLite format 3\x00', 'invalid_graph_header')
    # mode=ro is not a promise that a WAL connection cannot touch shared-memory
    # sidecars. The legacy source contract is rollback mode only. WAL needs an
    # owner-qualified snapshot, never immutable=1 to ignore journal state.
    _require(header[18:20]==b'\x01\x01', 'graph_rollback_mode_required')
    # Store.db() uses sqlite's create-if-missing default. Do not call it here.
    # A hot journal requiring recovery is the original owner's task, not a
    # reason for this read-only exporter to write or recreate source state.
    try:
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute('''SELECT e.*,v.url,v.title,v.excerpt,v.synthetic,
                v.published_at,v.recorded_at AS evidence_recorded_at
                FROM research_edges e LEFT JOIN research_evidence v ON e.evidence_id=v.id
                ORDER BY e.id LIMIT ?''', (max_edges + 1,)).fetchall()
    except sqlite3.DatabaseError:
        raise Refused('graph_read_unavailable_requires_source_owner') from None
    _require(path.is_file() and not path.is_symlink(), 'graph_file_changed')
    final = path.stat()
    _require((final.st_dev,final.st_ino)==identity, 'graph_file_changed')
    return {'edges':[dict(row) for row in rows[:max_edges]],
            'coverage_complete':len(rows) <= max_edges,
            'adapter':'existing-relationships-schema-v1'}


def map_event(event, graph, identities, *, decision_at, now, revisions=(),
              max_hops=2, max_paths=100, allow_synthetic=False):
    """Produce cited direct/indirect RESEARCH candidates, never beneficiaries facts.

    event: id, version, entity_keys[], effective_at, observed_at, available_at,
           source_url, summary. Entity keys match existing graph origin/target.
    identities[key]: entity_id (stable, including macro entities), available_at,
                     mapping_source, optional symbols[] (display only).
    revisions[]: edge_id, known_at, status corrected/retracted, evidence_ref.
    Supplied correction knowledge is independent of economic effective time.
    """
    event, graph, identities, revisions = _copy([event,graph,identities,list(revisions)])
    at, current = _time(decision_at), _time(now)
    _require(at <= current, 'future_decision')
    _require(type(max_hops) is int and 1 <= max_hops <= 3, 'hop_budget')
    _require(type(max_paths) is int and 1 <= max_paths <= 10000, 'path_budget')
    _require(type(allow_synthetic) is bool, 'synthetic_flag')
    _require(type(event.get('synthetic',False)) is bool, 'event_synthetic_flag')
    _require(allow_synthetic or not event.get('synthetic',False), 'synthetic_event_excluded')
    for field in ('id','version','source_url','summary'):
        _text(event.get(field), 'event_'+field)
    event_url = urlsplit(event['source_url'])
    _require(event_url.scheme=='https' and event_url.hostname and not event_url.username and not event_url.password, 'unsafe_event_url')
    effective, observed, available = (_time(event[k]) for k in ('effective_at','observed_at','available_at'))
    _require(effective <= observed <= available <= at, 'event_not_available')
    _require(type(event.get('entity_keys')) is list and event['entity_keys'], 'event_entities')
    _require(type(graph.get('edges')) is list and type(graph.get('coverage_complete')) is bool, 'graph_shape')
    _require(type(identities) is dict, 'identity_registry')
    registry, unknown = {}, []
    for key, identity in identities.items():
        _text(key, 'entity_key'); _require(type(identity) is dict, 'identity_shape')
        _text(identity.get('entity_id'), 'stable_entity_id')
        _text(identity.get('mapping_source'), 'mapping_source')
        symbols = identity.get('symbols',[])
        _require(type(symbols) is list and all(type(x) is str for x in symbols), 'identity_symbols')
        if _time(identity['available_at']) <= at:
            registry[key] = {name:identity[name] for name in ('entity_id','available_at','mapping_source')}
            registry[key]['symbols'] = symbols
            if 'sector' in identity:
                registry[key]['sector'] = _text(identity['sector'],'sector_display_label')
    revoked = {}
    for revision in revisions:
        _require(type(revision) is dict and revision.get('status') in ('corrected','retracted'), 'revision_shape')
        _text(revision.get('edge_id'), 'revision_edge'); _text(revision.get('evidence_ref'), 'revision_evidence')
        if _time(revision['known_at']) <= at:
            revoked.setdefault(revision['edge_id'],[]).append(revision)
    eligible, exclusions, edge_ids = [], [], set()
    for edge in graph['edges']:
        _require(type(edge) is dict, 'edge_shape')
        eid = _text(edge.get('id'),'edge_id')
        _require(eid not in edge_ids, 'duplicate_edge_identity'); edge_ids.add(eid)
        reason = None
        try:
            for field in ('origin','target','evidence_id','title','excerpt','url','materiality'):
                _text(edge.get(field), 'edge_'+field)
            link = urlsplit(edge['url'])
            _require(link.scheme == 'https' and link.hostname and not link.username and not link.password, 'unsafe_evidence_url')
            _require(edge['origin'] != edge['target'], 'self_edge')
            _require(edge.get('relation') in RELATIONS and edge.get('status') in ('reported','hypothesis'), 'relationship_kind')
            _require(type(edge.get('synthetic')) in (bool,int) and edge['synthetic'] in (False,True,0,1), 'synthetic_value')
            published, evidence_seen, edge_seen = (_time(edge[k]) for k in ('published_at','evidence_recorded_at','recorded_at'))
            start = _time(edge['valid_from'])
            end = _time(edge['valid_until']) if edge.get('valid_until') else None
            _require(end is None or end > start, 'validity_order')
            _require(published <= evidence_seen <= edge_seen <= at, 'relationship_not_known')
            _require(start <= effective and (end is None or at < end), 'relationship_not_valid')
            _require(eid not in revoked, 'relationship_corrected_or_retracted')
            _require(allow_synthetic or not edge['synthetic'], 'synthetic_excluded')
            _require(edge['origin'] in registry and edge['target'] in registry, 'unresolved_entity_identity')
        except (ValueError,TypeError,KeyError) as error:
            reason = str(error) if isinstance(error,Refused) else 'malformed_edge'
        if reason:
            exclusion = {'edge_id':eid,'reason':reason}
            if eid in revoked:
                exclusion['known_revisions'] = revoked[eid]
            exclusions.append(exclusion)
        else:
            eligible.append(edge)
    adjacency = {}
    for edge in sorted(eligible,key=lambda e:e['id']):
        adjacency.setdefault(edge['origin'],[]).append((edge['target'],edge,'forward'))
        adjacency.setdefault(edge['target'],[]).append((edge['origin'],edge,'reverse'))
    candidates, queue, seen_paths, direct_ids = [], [], set(), set()
    for key in dict.fromkeys(event['entity_keys']):
        _text(key,'event_entity_key')
        if key not in registry:
            unknown.append(key)
            continue
        identity = registry[key]
        if identity['entity_id'] not in direct_ids:
            direct_ids.add(identity['entity_id'])
            candidates.append({'kind':'direct_event_entity','entity_key':key, **identity,
                'relationship_status':'no_indirect_claim','path':[], 'direction':'undetermined',
                'synthetic':event.get('synthetic',False),
                'uncertainties':['Event identity and source remain caller supplied and unverified.'],
                'counterarguments':['The event may already be priced, immaterial or incorrectly attributed.'],
                'order_permission':'none'})
        queue.append((key,[],{identity['entity_id']}))
    truncated = False
    while queue:
        current_key, path, visited = queue.pop(0)
        if len(path) >= max_hops:
            continue
        for target, edge, traversal in adjacency.get(current_key,[]):
            entity = registry[target]
            if entity['entity_id'] in visited:
                continue
            step = dict(edge_id=edge['id'], origin=edge['origin'], target=edge['target'],
                relation=edge['relation'], traversal=traversal, evidence_id=edge['evidence_id'],
                url=edge['url'], title=edge['title'], excerpt=edge['excerpt'], status=edge['status'],
                published_at=edge['published_at'], evidence_recorded_at=edge['evidence_recorded_at'],
                relationship_recorded_at=edge['recorded_at'], valid_from=edge['valid_from'],
                valid_until=edge.get('valid_until'), materiality=edge['materiality'],
                synthetic=bool(edge['synthetic']))
            chain = path + [step]
            key = tuple((p['edge_id'],p['traversal']) for p in chain)
            if key in seen_paths:
                continue
            if len(seen_paths) >= max_paths:
                truncated = True
                break
            seen_paths.add(key)
            reported = all(p['status']=='reported' for p in chain)
            candidates.append({'kind':'indirect_exposure_hypothesis','entity_key':target, **entity,
                'relationship_status':'supplied_reported_relationship' if reported else 'unproven_relationship',
                'path':chain,'direction':'undetermined_not_a_beneficiary_prediction',
                'synthetic':event.get('synthetic',False) or any(p['synthetic'] for p in chain),
                'uncertainties':['Event authenticity and incremental economic materiality remain unverified.',
                    'A relationship does not establish revenue timing, margin impact or a price move.',
                    'Reported means the supplied excerpt describes the link; no source authentication was performed.'],
                'counterarguments':['The event may already be priced or anticipated.',
                    'Substitution, capacity, pricing pressure or a shared sector factor can offset the proposed effect.'],
                'order_permission':'none'})
            queue.append((target,chain,visited | {entity['entity_id']}))
        if truncated:
            break
    result = dict(schema=VERSION,event=event,decision_at=decision_at,candidates=candidates,
        unresolved_event_entities=unknown,excluded_edges=exclusions,
        graph_coverage_complete=graph['coverage_complete'],path_budget_exhausted=truncated,
        supplied_scope_exhausted=graph['coverage_complete'] and not truncated and not unknown and not exclusions,
        scope={'max_hops':max_hops,'max_indirect_paths':max_paths},
        real_world_relationship_coverage_complete=False,
        event_verified=False,identity_mapping_authenticated=False,order_permission='none',
        synthetic_allowed=allow_synthetic,
        synthetic_used=any(c['synthetic'] for c in candidates),
        limitations=['Supplied graph and identity records are caller attestations, not independently authenticated facts.',
            'No automatic entity resolution, website fetch, market data, sentiment, valuation or execution.',
            'Potential exposures include adverse and neutral outcomes, not just beneficiaries.'])
    result['mapping_id'] = sha256(json.dumps(result,sort_keys=True,allow_nan=False).encode()).hexdigest()
    return result
