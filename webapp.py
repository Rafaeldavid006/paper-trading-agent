"""Authenticated research dashboard. No trading endpoints or implicit AI calls."""
import os
import re
import secrets
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse
from flask import Flask, Response, abort, flash, redirect, render_template, request, session, url_for
from event_core import Store
from event_types import ROUTES
from relationships import RELATIONS
from pipeline import Pipeline
from monitor import tick
from discovery import Discovery

TYPE_LABELS=dict(earnings='דוחות ותחזיות',merger='מיזוגים ורכישות',financing='גיוס ומימון',
    investment_contract='השקעות וחוזים',monetary_policy='ריבית ומדיניות מוניטרית',
    macro_release='נתוני מאקרו',trade_sanctions='סחר וסנקציות',supply_disruption='שיבושי אספקה',
    technology='טכנולוגיה',regulatory_clinical='רגולציה וניסויים קליניים',
    management_statement='הצהרות הנהלה ואנשי מפתח',market_structure='מבנה שוק וזרימת כספים',unknown='טרם סווג')
RELATION_LABELS=dict(buys_from='רוכש מ־',sells_to='מוכר ל־',uses_technology='משתמש בטכנולוגיה של',
    competes_with='מתחרה ב־',substitutes_for='מהווה חלופה ל־',finances='מממן את',
    exposed_to_rate='חשוף לריבית',exposed_to_currency='חשוף למטבע',regulated_by='מפוקח בידי',
    operates_in='פועל באזור',depends_on_logistics='תלוי בנתיב אספקה',uses_commodity='משתמש בחומר גלם')

def create_app(store=None, analyst=None, collector=None, discovery_transport=None):
    app=Flask(__name__)
    app.secret_key=os.environ.get('FLASK_SECRET_KEY') or secrets.token_hex(32)
    app.config.update(MAX_CONTENT_LENGTH=32768,SESSION_COOKIE_HTTPONLY=True,
                      SESSION_COOKIE_SAMESITE='Strict',SESSION_COOKIE_SECURE=True)
    db_path=os.environ.get('EVENT_DB','events.db')
    if store is None and not Path(db_path).parent.is_dir():
        raise ValueError('EVENT_DB parent directory must exist')
    store=store or Store(db_path)
    pipeline=Pipeline(store)
    discovery=Discovery(store)
    app.extensions['research_pipeline']=pipeline

    def protected(fn):
        @wraps(fn)
        def wrapped(*args,**kwargs):
            password=os.environ.get('DASHBOARD_PASSWORD','')
            if len(password)<20:return 'Configure DASHBOARD_PASSWORD with at least 20 characters.',503
            auth=request.authorization
            supplied=auth.password if auth and auth.password else ''
            if not secrets.compare_digest(password.encode(),supplied.encode()):
                return Response('Login required',401,{'WWW-Authenticate':'Basic realm="Research"'})
            if request.method=='POST':
                token=session.get('csrf','')
                if not token or not secrets.compare_digest(token.encode(),request.form.get('csrf','').encode()):abort(403)
            session.setdefault('csrf',secrets.token_hex(32))
            return fn(*args,**kwargs)
        return wrapped

    @app.after_request
    def headers(response):
        response.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff',
            'X-Frame-Options':'DENY','Referrer-Policy':'no-referrer',
            'Content-Security-Policy':"default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"})
        return response

    def enabled():return os.environ.get('ENABLE_AI_RESEARCH','false').lower()=='true'
    def limit():
        try:value=int(os.environ.get('AI_MAX_CALLS_PER_DAY','5'))
        except ValueError:raise ValueError('invalid AI daily limit') from None
        if not 1<=value<=100:raise ValueError('invalid AI daily limit')
        return value

    @app.context_processor
    def context():
        return dict(csrf=session.get('csrf'),types=sorted(ROUTES),relations=sorted(RELATIONS),
                    type_labels=TYPE_LABELS,relation_labels=RELATION_LABELS,ai_enabled=enabled(),
                    discovery_enabled=os.environ.get('ENABLE_WEB_RESEARCH','false').lower()=='true')

    @app.get('/health')
    def health():return {'status':'ok','mode':'research_only'}

    @app.get('/')
    @protected
    def index():
        with store.db() as c:
            events=[dict(r) for r in c.execute('''SELECT e.* FROM events e WHERE NOT EXISTS
                (SELECT 1 FROM events n WHERE n.source=e.source AND n.source_id=e.source_id
                 AND (n.updated_at>e.updated_at OR (n.updated_at=e.updated_at AND n.rowid>e.rowid)))
                ORDER BY received_at DESC LIMIT 50''')]
            health=[dict(r) for r in c.execute('SELECT * FROM health')]
        import json
        for event in events:
            payload=json.loads(event['payload'])
            event['headline']=payload.get('headline',event['symbol']) if isinstance(payload,dict) else event['symbol']
        configured=bool(os.environ.get('ALPACA_API_KEY') and os.environ.get('ALPACA_SECRET_KEY'))
        return render_template('research.html',mode='index',events=events,health=health,
            news_configured=configured,ai_configured=bool(os.environ.get('OPENAI_API_KEY') and os.environ.get('OPENAI_MODEL')),
            persistent=os.environ.get('PERSISTENT_STORAGE_CONFIRMED')=='true')

    @app.post('/collect')
    @protected
    def collect():
        symbols=os.environ.get('WATCHLIST','SPY,QQQ').upper()
        if len(symbols.split(','))>20 or any(not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,11}',s) for s in symbols.split(',')):
            flash('רשימת המעקב אינה תקינה.');return redirect(url_for('index'))
        try:
            count=(collector or tick)(store,symbols)
            flash(f'נשמרו {count} גרסאות חדשות. מצב הכיסוי מופיע למטה.')
        except Exception:
            store.heartbeat('alpaca-news','error');flash('איסוף החדשות נכשל. לא נוצר איתות ולא בוצעה עסקה.')
        return redirect(url_for('index'))

    @app.post('/prepare/<version>')
    @protected
    def prepare(version):
        try:run=pipeline.prepare(version,event_types=request.form.getlist('event_types') or ['unknown'])
        except ValueError:
            flash('לא ניתן להכין מחקר לגרסה הזאת. רענן את רשימת האירועים.');return redirect(url_for('index'))
        return redirect(url_for('show',run_id=run['id']))

    @app.route('/discovery/<version>',methods=['GET','POST'])
    @protected
    def discover(version):
        with store.db() as c:
            event=c.execute('SELECT symbol FROM events WHERE version_id=?',(version,)).fetchone()
        if not event:abort(404)
        if request.method=='POST':
            if os.environ.get('ENABLE_WEB_RESEARCH','false').lower()!='true':abort(403)
            try:
                discovery.run(version,transport=discovery_transport,max_daily_calls=limit(),
                              retry=request.form.get('retry')=='1')
            except ValueError:
                flash('החיפוש לא הופעל. בדוק הגדרות חיבור, מכסת בקשות וגרסת אירוע.')
            return redirect(url_for('discover',version=version))
        return render_template('research.html',mode='discovery',discovery=discovery.get(version),
                               event_version=version,symbol=event['symbol'])

    @app.get('/research/<run_id>')
    @protected
    def show(run_id):
        try:run=pipeline.get(run_id)
        except ValueError:abort(404)
        return render_template('research.html',mode='research',run=run)

    @app.post('/evidence/<run_id>')
    @protected
    def evidence(run_id):
        try:
            run=pipeline.get(run_id)
            if run['superseded']:raise ValueError('superseded')
            source=request.form.get('source_url','').strip()
            if urlparse(source).scheme!='https':raise ValueError('HTTPS required')
            target=request.form.get('target','').strip()
            if not 1<=len(target)<=100:raise ValueError('target required')
            eid=pipeline.graph.evidence(url=source,title=request.form.get('title',''),
                published_at=request.form.get('published_at',''),excerpt=request.form.get('excerpt',''))
            pipeline.graph.edge(origin=run['packet']['event']['symbol'],target=target,
                relation=request.form.get('relation'),evidence_id=eid,status='hypothesis')
            fresh=pipeline.prepare(run['event_version'],event_types=run['packet']['graph']['research_brief']['event_types'])
            flash('המקור נשמר כקלט מחקר לא מאומת. נוצר מחקר מעודכן.')
            return redirect(url_for('show',run_id=fresh['id']))
        except (ValueError,TypeError):
            flash('לא נשמר קשר. בדוק מקור HTTPS, זמן עם אזור זמן, ציטוט וישות יעד.')
            return redirect(url_for('show',run_id=run_id))

    @app.post('/review/<run_id>')
    @protected
    def review(run_id):
        if not enabled():abort(403)
        try:
            run=pipeline.review(run_id,analyst=analyst,max_daily_calls=limit())
            flash('הניתוח נשמר.' if run['state']=='reviewed' else 'הניתוח לא הושלם. לא יופעל ניסיון חוזר אוטומטי.')
        except ValueError:
            flash('הניתוח נחסם: בדוק ראיות, עדכניות ומכסת בקשות יומית.')
        return redirect(url_for('show',run_id=run_id))

    return app
