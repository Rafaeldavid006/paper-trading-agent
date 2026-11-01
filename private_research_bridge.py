"""Bounded owner-held snapshot handoff to one credential-free research child.

No durable consumer, source mutation, external export, provider call or order.
Failure of research is isolated from source acquisition.
"""
import asyncio
import json
import os
from pathlib import Path
import re
import sys
import time

KEY = 'PRIVATE_SOURCE_REPLAY'
MAX_PAGE = 2*1024*1024+128*1024
MAX_TOTAL = 4*1024*1024
HASHES = {'research_frontier_sha256','research_checkpoint_sha256','research_report_sha256'}
COUNTS = {'research_frontier_seq','research_news_versions','research_distinct_articles','research_quotes',
    'research_control_receipts','research_gap_records','research_quarantined_frames','research_observations',
    'research_unresolved','research_deferred','research_pages','research_input_bytes','research_source_requests'}
FALSES = {'research_checkpoint_persisted','research_execution_ready','research_provider_coverage_complete'}
COMPLETE = HASHES|COUNTS|FALSES|{'type','research_state','research_reason','research_order_permission'}
PUBLIC_FIELDS = (COMPLETE-{'type'})|{'research_child_reaped','research_lag_records'}


def require(ok):
    if not ok: raise ValueError('private_replay_refused')


def unique(pairs):
    out = {}
    for key,value in pairs:
        require(key not in out); out[key] = value
    return out


def decode(raw):
    def invalid(_): raise ValueError('private_replay_refused')
    return json.loads(raw,object_pairs_hook=unique,parse_constant=invalid)


class ResearchReplay:
    def __init__(self, owner):
        self.owner,self.child,self.reaped = owner,None,True
        self.public = {'research_state':'waiting_source','research_reason':'not_started',
            'research_checkpoint_persisted':False,'research_execution_ready':False,
            'research_provider_coverage_complete':False,'research_order_permission':'none',
            'research_source_requests':0}

    def status(self): return dict(self.public,research_child_reaped=self.reaped)

    def accept(self, report, page, *, expected_pages=None, expected_bytes=None):
        require(type(report) is dict and set(report) == COMPLETE and report['type']=='complete' and
            report['research_state']=='complete' and report['research_order_permission']=='none' and
            report['research_reason'] in ('issuer_registry_missing','source_not_ready','consumer_incomplete'))
        require(all(type(report[k]) is int and 0 <= report[k] < 2**63 for k in COUNTS))
        require(all(report[k] is False for k in FALSES) and report['research_source_requests']==0)
        require(all(type(report[k]) is str and re.fullmatch('[0-9a-f]{64}',report[k]) for k in HASHES))
        require(page is not None and not page['has_more'] and
            report['research_frontier_seq']==page['next_seq']==page['head_seq'] and
            report['research_frontier_sha256']==page['page_sha256']==page['head_sha256'] and
            1 <= report['research_pages'] <= 32 and 0 < report['research_input_bytes'] <= MAX_TOTAL)
        require(expected_pages is None or report['research_pages']==expected_pages)
        require(expected_bytes is None or report['research_input_bytes']==expected_bytes)
        return {k:v for k,v in report.items() if k != 'type'}

    async def run(self):
        complete = None
        try:
            readiness = time.monotonic()+60
            while not all(s['state'] in ('subscribed_quiet','receiving') for s in self.owner.states.values()):
                require(not self.owner.stop.is_set() and time.monotonic() < readiness)
                await asyncio.sleep(.25)
            self.child = await asyncio.create_subprocess_exec(sys.executable,'-B',str(
                Path(__file__).resolve().with_name('private_research_replay.py')),
                '--private-replay',str(os.getpid()),stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL,
                env={},close_fds=True,limit=8192)
            self.reaped = False; self.public['research_state'] = 'running'
            deadline = time.monotonic()+40
            seq,head,total,pages,page = 0,'0'*64,0,0,None
            while not self.owner.stop.is_set():
                require(time.monotonic() < deadline)
                raw = await asyncio.wait_for(self.child.stdout.readline(),min(8,deadline-time.monotonic()))
                require(raw.endswith(b'\n') and len(raw) <= 4096)
                request = decode(raw)
                if type(request) is dict and request.get('type') == 'complete':
                    complete = self.accept(request,page,expected_pages=pages,expected_bytes=total)
                    require(await asyncio.wait_for(self.child.wait(),2) == 0)
                    self.reaped = True; self.public = complete
                    return
                require(type(request) is dict and set(request)=={'type','cursor_seq','cursor_sha256'} and
                    request['type']=='snapshot' and type(request['cursor_seq']) is int and
                    request['cursor_seq']==seq and request['cursor_sha256']==head and pages < 32)
                # Only the owning process can snapshot. The child has no source
                # descriptor, path selector, credential or direct SQLite handle.
                # This is reviewed-code separation, not a same-UID OS sandbox.
                page = self.owner.journal.snapshot(seq,max_records=64,max_bytes=2*1024*1024)
                require(not page['retention_gap'] and page['from_sha256']==head)
                payload = json.dumps(page,sort_keys=True,separators=(',',':'),allow_nan=False).encode()+b'\n'
                total += len(payload); pages += 1
                require(len(payload) <= MAX_PAGE and total <= MAX_TOTAL and time.monotonic() < deadline)
                self.child.stdin.write(payload)
                await asyncio.wait_for(self.child.stdin.drain(),3)
                seq,head = page['next_seq'],page['page_sha256']
            raise ValueError('source_stopped')
        except asyncio.CancelledError:
            self.public.update(research_state='stopped',research_reason='source_stopped')
            raise
        except Exception:
            self.public.update(research_state='refused',research_reason='replay_refused')
        finally:
            if self.child is not None and not self.reaped:
                try:
                    if self.child.returncode is None: self.child.terminate()
                    await asyncio.wait_for(self.child.wait(),1)
                except Exception:
                    try:
                        self.child.kill(); await asyncio.wait_for(self.child.wait(),1)
                    except Exception: self.public.update(research_state='refused',research_reason='reap_uncertain')
                self.reaped = self.child.returncode is not None
