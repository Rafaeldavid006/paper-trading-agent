import os
import secrets
from datetime import datetime, timezone

import requests
from flask import Flask, Response, render_template_string

app = Flask(__name__)

PAGE = """
<!doctype html>
<html lang="he" dir="rtl">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>חשבון הדמו של אביעד</title>
<style>
body{font-family:Arial;background:#101827;color:#eef2ff;
max-width:700px;margin:40px auto;padding:20px}
section{background:#1e293b;padding:20px;border-radius:16px;
margin:16px 0}table{width:100%;text-align:right}
td,th{padding:10px;border-bottom:1px solid #475569}
a{color:#93c5fd}
</style>
<h1>חשבון הדמו של אביעד</h1>
<p>Paper בלבד · תצוגת נתונים · אין ביצוע עסקאות</p>
{% if error %}
<section>{{ error }}</section>
{% else %}
<section>
<p>שווי תיק: {{ account.equity }} דולר</p>
<p>מזומן: {{ account.cash }} דולר</p>
<p>סטטוס החשבון: {{ account.status }}</p>
</section>
<section>
<h2>החזקות</h2>
<table>
<tr><th>סימול</th><th>כמות</th><th>רווח/הפסד בדולר</th></tr>
{% for p in positions %}
<tr><td>{{ p.symbol }}</td><td>{{ p.qty }}</td>
<td>{{ p.unrealized_pl }}</td></tr>
{% else %}
<tr><td colspan="3">אין החזקות</td></tr>
{% endfor %}
</table>
</section>
{% endif %}
<p>זמן הבדיקה (UTC): {{ checked }}</p>
<a href="/">רענון נתונים</a>
</html>
"""


@app.after_request
def headers(response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    return response


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def dashboard():
    from flask import request

    password = os.environ.get("DASHBOARD_PASSWORD", "")
    if len(password) < 20:
        return "יש להגדיר בשרת סיסמת לוח בקרה של לפחות 20 תווים.", 503

    auth = request.authorization
    supplied = auth.password if auth and auth.password else ""
    if not secrets.compare_digest(
        supplied.encode("utf-8"), password.encode("utf-8")
    ):
        return Response(
            "Login required",
            401,
            {"WWW-Authenticate": 'Basic realm="Paper Dashboard"'},
        )

    key = os.environ.get("ALPACA_API_KEY", "")
    secret = os.environ.get("ALPACA_SECRET_KEY", "")
    account, positions, error = {}, [], None

    if not key or not secret:
        error = "האתר פועל. מפתחות חשבון הדמו טרם הוגדרו בשרת."
    else:
        try:
            api_headers = {
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
            }
            base = "https://paper-api.alpaca.markets/v2"
            for path in ("account", "positions"):
                result = requests.get(
                    f"{base}/{path}",
                    headers=api_headers,
                    timeout=(3, 8),
                    allow_redirects=False,
                )
                if result.status_code != 200:
                    raise ValueError("API request failed")
                data = result.json()
                if path == "account":
                    if not isinstance(data, dict):
                        raise ValueError("Invalid account")
                    account = data
                else:
                    if not isinstance(data, list):
                        raise ValueError("Invalid positions")
                    positions = data
        except (requests.RequestException, ValueError):
            error = "לא ניתן לקבל נתונים. בדוק מפתחות Paper וחיבור ל־Alpaca."

    return render_template_string(
        PAGE,
        account=account,
        positions=positions,
        error=error,
        checked=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    )
