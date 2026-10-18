"""Server-revocable dashboard sessions. No plaintext credentials are persisted."""
import hashlib
import hmac
import secrets
from datetime import timedelta
from event_core import iso, utc


class DashboardAuth:
    def __init__(self, store, signing_key):
        self.store = store
        self.key = signing_key.encode() if isinstance(signing_key,str) else signing_key
        with store.db() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS dashboard_sessions(
                token_hash TEXT PRIMARY KEY, expires_at TEXT, password_fingerprint TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS dashboard_login_attempts(
                id INTEGER PRIMARY KEY, attempted_at TEXT, succeeded INTEGER NOT NULL)''')

    def fingerprint(self, password):
        return hmac.new(self.key,b'dashboard-password-v1:'+password.encode(),hashlib.sha256).hexdigest()

    def valid(self, token, password):
        if not isinstance(token,str) or len(token)!=64 or len(password)<20:
            return False
        with self.store.db() as c:
            row=c.execute('SELECT * FROM dashboard_sessions WHERE token_hash=?',
                          (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
        return bool(row and utc(row['expires_at'])>utc(self.store.clock()) and
                    secrets.compare_digest(row['password_fingerprint'],self.fingerprint(password)))

    def claim_attempt(self):
        now=utc(self.store.clock())
        cutoff=iso(now-timedelta(minutes=15))
        with self.store.db() as c:
            c.execute('BEGIN IMMEDIATE')
            failures=c.execute('SELECT COUNT(*) FROM dashboard_login_attempts WHERE attempted_at>=? AND succeeded=0',
                               (cutoff,)).fetchone()[0]
            if failures>=20:
                return None
            return c.execute('INSERT INTO dashboard_login_attempts(attempted_at,succeeded) VALUES (?,0)',
                             (iso(now),)).lastrowid

    def issue(self, password, attempt):
        token=secrets.token_hex(32)
        now=utc(self.store.clock())
        with self.store.db() as c:
            c.execute('UPDATE dashboard_login_attempts SET succeeded=1 WHERE id=?',(attempt,))
            c.execute('DELETE FROM dashboard_sessions WHERE expires_at<=?',(iso(now),))
            c.execute('INSERT INTO dashboard_sessions VALUES (?,?,?)',
                (hashlib.sha256(token.encode()).hexdigest(),iso(now+timedelta(hours=4)),self.fingerprint(password)))
        return token

    def revoke(self, token):
        if not isinstance(token,str):
            return
        with self.store.db() as c:
            c.execute('DELETE FROM dashboard_sessions WHERE token_hash=?',
                      (hashlib.sha256(token.encode()).hexdigest(),))
