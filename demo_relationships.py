"""Entirely fictional names, relationships and event. No network calls or orders."""
import json,tempfile
from datetime import datetime,timezone,timedelta
from event_core import Store
from relationships import Relationships

def build_demo():
    base=datetime(2026,10,1,tzinfo=timezone.utc)
    with tempfile.TemporaryDirectory() as tmp:
        store=Store(tmp+'/demo.db',lambda:base)
        graph=Relationships(store)
        a=graph.evidence(url='https://example.org/fictional-relationship-1',title='SYNTHETIC supplier relationship',published_at=base-timedelta(days=1),excerpt='FICTIONAL: Demo Quantum uses Demo Cooling for its laboratory cryogenic equipment.',synthetic=True)
        b=graph.evidence(url='https://example.org/fictional-relationship-2',title='SYNTHETIC second-order relationship',published_at=base-timedelta(days=1),excerpt='FICTIONAL: Demo Cooling buys components from Demo Components. Volumes and margins unknown.',synthetic=True)
        graph.edge(origin='Demo Quantum',target='Demo Cooling',relation='buys_from',evidence_id=a,status='reported',materiality='unknown')
        graph.edge(origin='Demo Cooling',target='Demo Components',relation='buys_from',evidence_id=b,status='reported',materiality='unknown')
        result=graph.investigate(entity='Demo Quantum',event_text='FICTIONAL: a proposed investment was announced. No real politician, company or funding claim is represented.',allow_synthetic=True)
        result['label']='SYNTHETIC DEMO — NOT MARKET RESEARCH'
        return result

if __name__=='__main__':print(json.dumps(build_demo(),ensure_ascii=False,indent=2))
