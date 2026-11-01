"""Read-only projection of an externally supplied stable 8b5203a journal export.
Never opens the live journal, polls a source, mutates a cursor, or infers coverage.
The expected head is a caller's attestation, not provider authentication.
"""
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
from vendor.broad_discovery import digest as discovery_digest

NS = 10**9
MAX_FILE = 64*1024*1024
MAX_LINE = 2*1024*1024
MAX_BODY = 1024*1024
SYMBOL = re.compile(r'[A-Z][A-Z0-9.\-]{0,11}')
STAMP = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})')

class Refused(ValueError): pass

def require(ok, code):
    if not ok: raise Refused(code)

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

def unique(items):
    out = {}
    for k,v in items:
        require(k not in out, 'duplicate_json_key')
        out[k]=v
    return out

def invalid(_): raise Refused('nonfinite_number')

def decode(raw, *, prices=False):
    try:
        return json.loads(raw,object_pairs_hook=unique,parse_constant=invalid,
                          parse_float=Decimal if prices else float)
    except (ValueError,UnicodeError) as e:
        raise Refused('invalid_json') from e

def ns(value):
    require(type(value) is str, 'timestamp_shape')
    m=STAMP.fullmatch(value); require(m is not None,'timestamp_shape')
    try:
        dt=datetime.fromisoformat(m[1]+m[3].replace('Z','+00:00')).astimezone(timezone.utc)
        epoch=datetime(1970,1,1,tzinfo=timezone.utc)
        delta=dt-epoch
        result=(delta.days*86400+delta.seconds)*NS+int((m[2] or '').ljust(9,'0'))
        require(0 < result < 2**63,'timestamp_range'); return result
    except (ValueError,OverflowError) as e: raise Refused('timestamp_invalid') from e

def iso_ceil(value):
    """Reviewed mapper accepts microseconds: conservatively round UP, never leak future ns."""
    require(type(value) is int and 0 < value < 2**63,'timestamp_range')
    micros=(value+999)//1000
    return datetime.fromtimestamp(micros//10**6,tz=timezone.utc).replace(microsecond=micros%10**6).isoformat(timespec='microseconds')

def copy(value): return decode(canonical(value))

@dataclass(frozen=True)
class Capture:
    receipt: dict
    events: tuple
    quotes: tuple
    gaps: tuple
    controls: tuple


def read_export(path, *, expected_head, evidence_mode, max_bytes=MAX_FILE):
    """Existing stable local export only. No create/repair/lock of source journal.

    Caller must export a complete newline-terminated prefix under source owner's
    authority, and supply its head independently. A partial last write is refused.
    """
    require(evidence_mode in ('fixture','imported_capture'),'evidence_mode')
    require(type(expected_head) is str and re.fullmatch('[0-9a-f]{64}',expected_head),'expected_head')
    require(type(max_bytes) is int and 1 <= max_bytes <= MAX_FILE,'read_budget')
    path=Path(path).absolute()
    require(str(path) != '/var/data/live-source-stream-v1.jsonl','export_required_not_live_path')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        before=os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= max_bytes,'export_regular_bound')
        parts=[]; left=before.st_size
        while left:
            part=os.read(fd,min(left,1024*1024)); require(bool(part),'export_short_read')
            parts.append(part); left-=len(part)
        after=os.fstat(fd); named=os.stat(path,follow_symlinks=False)
        stamp=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        require(stamp(before)==stamp(after)==stamp(named) and stat.S_ISREG(named.st_mode),'export_changed')
    finally: os.close(fd)
    return project_bytes(b''.join(parts),expected_head=expected_head,evidence_mode=evidence_mode,max_bytes=max_bytes)


def project_bytes(raw, *, expected_head, evidence_mode, max_bytes=MAX_FILE):
    require(type(raw) is bytes and 0 < len(raw) <= max_bytes <= MAX_FILE,'capture_bound')
    require(evidence_mode in ('fixture','imported_capture'),'evidence_mode')
    require(raw.endswith(b'\n'),'incomplete_export')
    seq,head,high,grant=0,'0'*64,0,None
    events,quotes,gaps,controls={},[],[],[]
    attempts={'iex':0,'news':0}; terminal=set(); completed=False
    for line in raw.splitlines(keepends=True):
        require(len(line)<=MAX_LINE and line.endswith(b'\n'),'line_bound')
        row=decode(line)
        require(type(row) is dict and set(row)=={'seq','prev','record','hash'},'chain_shape')
        require(type(row['seq']) is int and row['seq']==seq+1 and row['prev']==head,'chain_sequence')
        digest=sha256(canonical({k:row[k] for k in ('seq','prev','record')})).hexdigest()
        require(row['hash']==digest,'chain_hash')
        r=row['record']; require(type(r) is dict and type(r.get('at_ns')) is int and high<=r['at_ns']<2**63,'record_time')
        require(not completed,'record_after_completion')
        kind=r.get('event'); at=r['at_ns']
        ref={'sequence':row['seq'],'record_sha256':digest,'received_at_ns':at}
        if seq==0:
            require(kind=='grant' and type(r.get('grant')) is dict,'initial_grant')
            grant=r['grant']; syms=grant.get('symbols')
            require(type(syms) is list and 1<=len(syms)<=30 and len(set(syms))==len(syms) and
                    all(type(s) is str and SYMBOL.fullmatch(s) for s in syms),'grant_symbols')
            require(grant.get('news')=='*','grant_news')
        else:
            require(kind in ('boot','attempt','control','data','gap','source_terminal','completed'),'record_event')
            if kind=='attempt':
                source=r.get('source'); require(source in attempts and source not in terminal and
                    r.get('attempt')==attempts[source]+1,'attempt_sequence')
                attempts[source]+=1
            elif kind=='source_terminal':
                require(r.get('source') in attempts,'terminal_source'); terminal.add(r['source'])
                gaps.append({**ref,'source':r['source'],'gap_reason':'source_terminal','continuity_invalid':True})
            elif kind=='completed':
                completed=True
                gaps.append({**ref,'source':'all','gap_reason':'capture_completed','continuity_invalid':True})
            elif kind=='gap': gaps.append({**ref,'source':r.get('source'),'gap_reason':r.get('reason')})
            elif kind=='control': controls.append({**ref,'source':r.get('source'),'status':r.get('status')})
            elif kind=='data':
                source=r.get('source'); require(source in attempts,'data_source')
                try: body=base64.b64decode(r['body_base64'],validate=True)
                except (KeyError,ValueError,TypeError) as e: raise Refused('body_base64') from e
                require(len(body)<=MAX_BODY and sha256(body).hexdigest()==r.get('body_sha256'),'body_integrity')
                try:
                    frame_events,frame_quotes=_project_body(body,source,grant,at,ref,r['body_sha256'],evidence_mode)
                except (ValueError,TypeError,KeyError,OverflowError):
                    # The actual owner retains an invalid provider body before
                    # ending that source. Preserve its chain/bytes references,
                    # quarantine the WHOLE frame, and inhibit continuity. A
                    # summary label is never trusted as proof of valid content.
                    gaps.append({**ref,'source':source,'gap_reason':'invalid_provider_frame',
                        'body_sha256':r['body_sha256'],'continuity_invalid':True})
                else:
                    for version,event in frame_events.items(): events.setdefault(version,event)
                    quotes.extend(frame_quotes)

        seq,head,high=row['seq'],digest,at
    require(head==expected_head,'unexpected_export_head')
    receipt={'schema':'retained-stream-projection-v1','journal_sha256':sha256(raw).hexdigest(),
        'head':head,'records':seq,'retained_bytes':len(raw),'last_received_at_ns':high,'grant':grant,
        'evidence_mode':evidence_mode,'synthetic':evidence_mode=='fixture','prefix_complete_lines':True,
        'provider_coverage_complete':False,'rest_cursor_advanced':False,'order_permission':'none',
        'quarantined_frames':sum(g['gap_reason']=='invalid_provider_frame' for g in gaps)}
    for event in events.values():
        event['capture_receipt']={k:receipt[k] for k in ('head','journal_sha256','evidence_mode','provider_coverage_complete','rest_cursor_advanced')}
        event['observed_gaps']=copy(gaps)
    return Capture(receipt,tuple(events.values()),tuple(quotes),tuple(gaps),tuple(controls))


def visible_events(capture, decision_ns):
    """As-of revision view: never let a future correction rewrite an earlier decision."""
    visible=[copy(e) for e in capture.events if e['first_received_at_ns']<=decision_ns]
    latest={}
    for e in visible: latest[e['article_id']]=max(latest.get(e['article_id'],0),e['updated_at_ns'])
    counts={}
    for e in visible:
        key=(e['article_id'],e['updated_at_ns']); counts[key]=counts.get(key,0)+1
    for e in visible:
        e['revision_status']=('superseded' if e['updated_at_ns']<latest[e['article_id']] else
            'conflicting_same_timestamp' if counts[(e['article_id'],e['updated_at_ns'])]>1 else 'provider_version_unverified')
    return visible


def event_core_rows(capture, *, include_fixtures=False):
    """Same Store.ingest seam as reviewed discovery, without claiming REST exhaustion."""
    rows=[]
    for e in capture.events:
        if e['synthetic'] and not include_fixtures: continue
        for symbol in dict.fromkeys(e['article']['symbols']):
            if not SYMBOL.fullmatch(symbol): continue
            payload=copy(e['article'])
            payload['_discovery']={'version':e['version'],'evidence_mode':e['evidence_mode'],
                'receipt_at':iso_ceil(e['first_received_at_ns']),'revision_status':'provider_version_unverified',
                'stream_evidence':copy(e['evidence']),'original_verified':False,'order_permission':'none'}
            if e['synthetic']: payload['synthetic']=True
            rows.append(dict(source='synthetic-broad-news' if e['synthetic'] else 'alpaca-news',
                source_id=e['article_id']+':'+symbol,symbol=symbol,published_at=e['article']['created_at'],
                updated_at=e['article']['updated_at'],payload=payload,url=e['article'].get('url') or
                'https://data.alpaca.markets/v1beta1/news',verified_primary=False))
    return rows


def _project_body(body, source, grant, at, ref, body_hash, evidence_mode):
    events,quotes={},[]
    items=decode(body,prices=True)
    require(type(items) is list and 0<len(items)<=8192,'body_shape')
    for index,item in enumerate(items):
        require(type(item) is dict,'item_shape')
        evidence={**ref,'body_sha256':body_hash,'item_index':index}
        if source=='news':
            require(item.get('T')=='n' and type(item.get('id')) is int and item['id']>=0,'news_shape')
            created,updated=ns(item.get('created_at')),ns(item.get('updated_at'))
            require(created<=updated<=at,'news_time')
            tags=item.get('symbols'); require(type(tags) is list and len(tags)<=1024 and
                all(type(s) is str and len(s)<=64 for s in tags),'news_tags')
            for field in ('headline','summary','content','url','source','author'):
                require(field not in item or type(item[field]) is str,'news_text')
            article={k:v for k,v in item.items() if k!='T'}
            try: version=discovery_digest([evidence_mode,'alpaca-news',article])
            except ValueError as e: raise Refused('news_value') from e
            if version not in events:
                events[version]={'article_id':str(item['id']),'version':version,'created_at_ns':created,
                    'updated_at_ns':updated,'first_received_at_ns':at,'evidence_mode':evidence_mode,
                    'synthetic':evidence_mode=='fixture','article':article,'evidence':evidence}
        else:
            require(item.get('T')=='q' and item.get('S') in grant['symbols'],'quote_channel')
            stamp=ns(item.get('t')); require(stamp<=at,'future_quote')
            for k in ('bp','ap'):
                require(type(item.get(k)) in (int,Decimal) and Decimal(item[k]).is_finite() and 0<=item[k]<=10**12,'quote_price')
            for k in ('bs','as'):
                require(type(item.get(k)) is int and 0<=item[k]<=10**12,'quote_size')
            require(not item['bp'] or not item['ap'] or item['bp']<=item['ap'],'crossed_quote')
            quotes.append({'symbol':item['S'],'feed':'iex','bid':str(item['bp']),'ask':str(item['ap']),
                'bid_size':item['bs'],'ask_size':item['as'],'provider_at_ns':stamp,
                'received_at_ns':at,'evidence':evidence,'synthetic':evidence_mode=='fixture'})
    return events,quotes
