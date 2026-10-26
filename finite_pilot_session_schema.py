"""Fixed v1 append-only session schema. No provisioner or automatic migration."""
SESSIONS='finite_pilot_sessions_v1'
EVENTS='finite_pilot_session_events_v1'
SCHEMA={
 SESSIONS: 'CREATE TABLE '+SESSIONS+'(session_id TEXT PRIMARY KEY,authority_id TEXT NOT NULL,manifest_digest TEXT NOT NULL UNIQUE,document TEXT NOT NULL)',
 EVENTS: 'CREATE TABLE '+EVENTS+'(global_seq INTEGER PRIMARY KEY,session_id TEXT NOT NULL,session_seq INTEGER NOT NULL,kind TEXT NOT NULL,at_ns INTEGER NOT NULL,payload TEXT NOT NULL,UNIQUE(session_id,session_seq))',
}
