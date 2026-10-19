"""One explicitly approved renewed Paper plumbing test; no repeating strategy/scheduler."""
import json
import os
import time
import sqlite3
from paper_adapter import PaperHTTP
from paper_renewal import RenewalExperiment, FINAL
from paper_quotes import IEXQuoteHTTP


def run(*, database='/var/data/paper-readonly.sqlite', environ=None,
        factory=RenewalExperiment, broker_factory=PaperHTTP, quote_factory=IEXQuoteHTTP,
        sleep=time.sleep, emit=print):
    env=os.environ if environ is None else environ
    key,secret=env.get('ALPACA_API_KEY'),env.get('ALPACA_SECRET_KEY')
    if not key or not secret:
        emit(json.dumps(dict(mode='paper_renewal',phase='blocked',reason='credentials_missing')),flush=True)
        return
    # Wait for read-only web initialization, without creating or trusting new identity.
    try:
        # Wait only for the local schema migration; no account/key probing retries.
        for attempt in range(10):
            try:
                with sqlite3.connect('file:'+database+'?mode=ro',uri=True) as c:
                    ready='identity_first_seen' in {r[1] for r in c.execute('PRAGMA table_info(paper_readonly)')}
                if ready:break
            except sqlite3.Error:pass
            sleep(1)
        experiment=factory(database,broker_factory(key,secret),quote_factory(key,secret))
    except Exception:
        emit(json.dumps(dict(mode='paper_renewal',phase='blocked',reason='private_state_unavailable')),flush=True)
        return
    previous=None
    while True:
        try: status=experiment.tick()
        except Exception:
            emit(json.dumps(dict(mode='paper_renewal',phase='manual_attention',reason='state_unavailable')),flush=True)
            return
        # ONLY aggregate state + public market opening time. No IDs/account data.
        public={k:status[k] for k in ('phase','reason','next_open')}
        if public!=previous:
            emit(json.dumps(dict(mode='paper_renewal',**public)),flush=True)
            previous=public
        if status['phase'] in FINAL:return
        sleep(10 if status['reason']!='waiting_regular_session' else 60)


if __name__=='__main__':run()
