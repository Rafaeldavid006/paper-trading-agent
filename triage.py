"""Transparent lexical research queue, never an estimate of market impact."""
import json
import re
from event_core import iso

VERSION = 'lexical-research-v1'
# Headline-only rules reduce false positives from unrelated summary context.
# Scores express queue order, not probability, sentiment, or expected return.
RULES = (
    ('earnings', 85, r'\b(?:reports? (?:q[1-4]|quarterly|annual|earnings)|earnings results|raises? (?:its )?(?:guidance|forecast)|cuts? (?:its )?(?:guidance|forecast)|profit warning)\b', 'דוחות או שינוי תחזית'),
    ('merger', 90, r'\b(?:agrees? to (?:acquire|buy|merge)|acquisition agreement|merger agreement|takeover (?:bid|offer)|to acquire)\b', 'רכישה, מיזוג או הצעת השתלטות'),
    ('financing', 75, r'\b(?:files? for bankruptcy|debt restructuring|public offering|private placement|share offering|secures? (?:funding|financing))\b', 'מימון, דילול או מצוקה פיננסית'),
    ('investment_contract', 80, r'\b(?:awarded? (?:a |an )?(?:\w+ ){0,3}contract|wins? (?:a |an )?(?:\w+ ){0,3}contract|signs? (?:a |an )?(?:supply|purchase|investment) agreement|announces? (?:a |an )?(?:investment|partnership)|invests? in)\b', 'חוזה, השקעה או שותפות שדורשים אימות'),
    ('regulatory_clinical', 90, r'\b(?:fda (?:approves?|rejects?|approval|rejection)|phase [123] (?:results|trial)|clinical trial results|antitrust (?:suit|ruling)|court (?:rules?|blocks?)|grants? approval|product recall)\b', 'החלטה רגולטורית, משפטית או קלינית'),
    ('supply_disruption', 85, r'\b(?:halts? production|plant (?:shutdown|closure)|supply disruption|export (?:ban|restrictions?)|port (?:closure|strike)|production outage)\b', 'שיבוש בייצור, אספקה או יצוא'),
    ('macro_policy', 80, r'\b(?:fed (?:cuts?|raises?|holds?)|interest rate (?:cut|hike|decision)|inflation (?:report|data)|jobs report|imposes? tariffs?|announces? sanctions?|tariff (?:increase|cut))\b', 'נתון מאקרו או שינוי מדיניות'),
    ('technology', 70, r'\b(?:launches? (?:a |an )?(?:new |commercial )?(?:chip|product|platform)|patent (?:granted|awarded)|technology breakthrough|commercial deployment)\b', 'מוצר, טכנולוגיה או פריסה מסחרית'),
)
COMMENTARY = re.compile(r"\b(?:says|opinion|outlook|what sets|why .* could|stocks? to (?:buy|watch)|price target|analyst rating)\b", re.I)
UNCERTAIN = re.compile(r'\b(?:rumou?r|reportedly|may|might|could|considers?|in talks|sources say)\b', re.I)


def screen(event):
    try:
        payload = json.loads(event['payload'])
    except (ValueError, TypeError):
        payload = {}
    headline = payload.get('headline', '') if isinstance(payload, dict) else ''
    headline = headline if isinstance(headline, str) else ''
    hits = [(family, score, reason) for family, score, pattern, reason in RULES
            if re.search(pattern, headline[:4000], re.I)]
    uncertain = bool(UNCERTAIN.search(headline))
    if hits:
        score = max(h[1] for h in hits) - (20 if uncertain else 0)
        reasons = [h[2] for h in hits]
        if uncertain:
            reasons.append('ניסוח מותנה או דיווח לא ודאי; עדיפות מופחתת')
        status = 'candidate'
    elif COMMENTARY.search(headline):
        score, status, reasons = 10, 'commentary', ['כתבת דעה או פרשנות ללא טריגר מזוהה בכותרת']
    else:
        score, status, reasons = 30, 'unclassified', ['לא זוהה טריגר בכללים; זמין לבדיקה ידנית']
    return dict(version=VERSION, status=status, priority=score, reasons=reasons,
                event_types=[h[0] for h in hits], auto_eligible=bool(hits) and score >= 60,
                event_verified=False, order_permission='none')


def record_screenings(store, events):
    """Keep the decision and rule version, including skipped candidates."""
    screened = [(event, screen(event)) for event in events]
    with store.db() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS event_screenings(
            event_version TEXT, rule_version TEXT, screened_at TEXT, result TEXT,
            PRIMARY KEY(event_version,rule_version))''')
        for event, result in screened:
            c.execute('INSERT OR IGNORE INTO event_screenings VALUES (?,?,?,?)',
                      (event['version_id'], VERSION, iso(store.clock()), json.dumps(result, ensure_ascii=False)))
    return screened
