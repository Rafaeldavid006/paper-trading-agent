"""Durable event-to-research orchestration. No broker access or background worker.

AI is opt-in per run; an interrupted request is never retried automatically.
"""
import hashlib
import json
from event_core import iso, utc
from relationships import Relationships
from ai_research import analyze, validate_review


class Pipeline:
    def __init__(self, store):
        self.store = store
        self.graph = Relationships(store)
        with store.db() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS pipeline_runs(
                id TEXT PRIMARY KEY, event_version TEXT, created_at TEXT,
                state TEXT, packet TEXT, review TEXT, error TEXT)''')

    def prepare(self, version, *, event_types=None):
        now = utc(self.store.clock())
        with self.store.db() as c:
            row = c.execute('SELECT * FROM events WHERE version_id=?', (version,)).fetchone()
            if row is None:
                raise ValueError('unknown event version')
            event = dict(row)
            latest = c.execute('''SELECT version_id FROM events WHERE source=? AND source_id=?
                ORDER BY updated_at DESC,rowid DESC LIMIT 1''',
                (event['source'], event['source_id'])).fetchone()['version_id']
        if latest != version:
            raise ValueError('superseded event version')
        if max(utc(event[k]) for k in ('published_at','updated_at','received_at')) > now:
            raise ValueError('future event timestamp')
        payload = json.loads(event['payload'])
        text = json.dumps(payload, ensure_ascii=False)
        graph = self.graph.investigate(entity=event['symbol'], event_text=text,
                                       as_of=now, event_types=event_types)
        packet = {'event': event, 'graph': graph, 'order_permission': 'none',
                  'scope': 'research_only', 'prepared_at': iso(now)}
        # Same inputs at the same cutoff produce the same run, without duplicate AI calls.
        digest_input = json.dumps(packet, sort_keys=True, ensure_ascii=False)
        run_id = hashlib.sha256(digest_input.encode()).hexdigest()
        with self.store.db() as c:
            c.execute('INSERT OR IGNORE INTO pipeline_runs VALUES (?,?,?,?,?,?,?)',
                      (run_id,version,iso(now),'prepared',digest_input,None,None))
        return self.get(run_id)

    def get(self, run_id):
        with self.store.db() as c:
            row = c.execute('SELECT * FROM pipeline_runs WHERE id=?',(run_id,)).fetchone()
            if row is None:
                raise ValueError('unknown research run')
            result = dict(row)
            current = c.execute('''SELECT version_id FROM events
                WHERE (source,source_id)=(SELECT source,source_id FROM events WHERE version_id=?)
                ORDER BY updated_at DESC,rowid DESC LIMIT 1''', (result['event_version'],)).fetchone()
        result['packet'] = json.loads(result['packet'])
        result['review'] = json.loads(result['review']) if result['review'] else None
        result['superseded'] = current['version_id'] != result['event_version']
        result['order_permission'] = 'none'
        return result

    def review(self, run_id, *, analyst=None, max_daily_calls=5):
        if type(max_daily_calls) is not int or not 1 <= max_daily_calls <= 100:
            raise ValueError('daily call limit must be 1-100')
        run = self.get(run_id)
        if run['superseded']:
            raise ValueError('superseded research cannot be analyzed')
        graph = run['packet']['graph']
        if graph['synthetic_mode'] or not graph['branches']:
            raise ValueError('real evidence-backed branches required')
        # Claim before the network request. Failure or process death leaves no retryable claim.
        with self.store.db() as c:
            c.execute('BEGIN IMMEDIATE')
            c.execute('''CREATE TABLE IF NOT EXISTS ai_attempts(
                run_id TEXT PRIMARY KEY, requested_at TEXT NOT NULL)''')
            state=c.execute('SELECT state FROM pipeline_runs WHERE id=?',(run_id,)).fetchone()['state']
            if state != 'prepared':
                return run
            stamp=iso(self.store.clock())
            used=c.execute('SELECT COUNT(*) FROM ai_attempts WHERE substr(requested_at,1,10)=?',
                           (stamp[:10],)).fetchone()[0]
            if used >= max_daily_calls:
                raise ValueError('daily AI request limit reached')
            changed = c.execute("UPDATE pipeline_runs SET state='analyzing' WHERE id=? AND state='prepared'",
                                (run_id,)).rowcount
            if changed:
                c.execute('INSERT INTO ai_attempts VALUES (?,?)',(run_id,stamp))
        if not changed:
            return self.get(run_id)
        try:
            result = (analyst or analyze)(graph)
            validate_review(result, graph)
        except Exception:
            with self.store.db() as c:
                c.execute("UPDATE pipeline_runs SET state='failed',error=? WHERE id=?",
                          ('analysis_failed_no_automatic_retry',run_id))
            return self.get(run_id)
        with self.store.db() as c:
            c.execute("UPDATE pipeline_runs SET state='reviewed',review=? WHERE id=?",
                      (json.dumps(result,ensure_ascii=False),run_id))
        return self.get(run_id)
