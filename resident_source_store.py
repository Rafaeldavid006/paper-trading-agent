"""Private transactional bounded receipt window with durable cumulative controls.

Retention intentionally removes old receipts, never resets budgets or chain head.
The retained-prefix checkpoint is local integrity evidence, not an external seal.
"""
import base64
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from live_source_protocol import NS, MAX_MESSAGE, require, unique, invalid
from live_source_store import canonical, PATH as TRIAL_PATH
from resident_source_policy import DAY, PREDECESSOR, TRIAL_GRANT

PATH = '/var/data/resident-source-v2.sqlite3'
FILE_LIMIT = 192*1024*1024
ZERO = '0'*64


def private(path, fd=None):
    info = os.fstat(fd) if fd is not None else os.stat(path,follow_symlinks=False)
    require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600 and
        info.st_uid == os.geteuid() and info.st_nlink == 1, 'resident_identity')
    return info


def sync_parent(path):
    fd = os.open(str(Path(path).parent),os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try: os.fsync(fd)
    finally: os.close(fd)


def verify_predecessor(path=TRIAL_PATH, *, test_only=False):
    require(test_only is True or path == TRIAL_PATH, 'resident_path')
    expected = canonical({'version':1,'grant_sha256':hashlib.sha256(canonical(TRIAL_GRANT)).hexdigest()})+b'\n'
    fd = os.open(str(path)+'.enrolled',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        private(str(path)+'.enrolled',fd)
        require(os.read(fd,len(expected)+1) == expected, 'predecessor_enrollment')
    finally: os.close(fd)
    fd = os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        start = private(path,fd)
        fcntl.flock(fd,fcntl.LOCK_SH|fcntl.LOCK_NB)
        require(start.st_size+len(expected) == PREDECESSOR['bytes'], 'predecessor_size')
        seq, head, last = 0,ZERO,None
        with os.fdopen(os.dup(fd),'rb') as stream:
            for raw in stream:
                require(len(raw) <= 2*1024*1024 and raw.endswith(b'\n'), 'predecessor_shape')
                row = json.loads(raw,object_pairs_hook=unique,parse_constant=invalid)
                require(set(row) == {'seq','prev','record','hash'} and row['seq'] == seq+1 and row['prev'] == head, 'predecessor_chain')
                head = hashlib.sha256(canonical({k:row[k] for k in ('seq','prev','record')})).hexdigest()
                require(row['hash'] == head, 'predecessor_chain')
                seq, last = row['seq'],row['record']
        end = private(path,fd)
        require((start.st_dev,start.st_ino,start.st_size,start.st_mtime_ns,start.st_ctime_ns) ==
            (end.st_dev,end.st_ino,end.st_size,end.st_mtime_ns,end.st_ctime_ns) and
            (end.st_dev,end.st_ino) == (private(path).st_dev,private(path).st_ino), 'predecessor_changed')
        require(seq == PREDECESSOR['seq'] and head == PREDECESSOR['sha256'] and
            last.get('event') == 'completed', 'predecessor_incomplete')
    finally: os.close(fd)
    return dict(PREDECESSOR)


class Store:
    def __init__(self, policy, path=PATH, *, test_only=False, predecessor=None):
        require(test_only is True or path == PATH, 'resident_path')
        self.path,self.policy,self.fd,self.db,self.failed = str(path),policy,None,None,False
        for parent in reversed(Path(path).parents):
            require(stat.S_ISDIR(parent.stat(follow_symlinks=False).st_mode), 'resident_parent')
        require(test_only is True or predecessor is None, 'resident_predecessor')
        self.predecessor = predecessor if test_only is True and predecessor is not None else verify_predecessor()
        require(self.predecessor == PREDECESSOR, 'resident_predecessor')
        self.enrollment = self.path+'.enrolled'
        expected = canonical({'version':2,'policy_sha256':hashlib.sha256(canonical(policy)).hexdigest(),
            'predecessor':PREDECESSOR})+b'\n'
        fresh = not os.path.lexists(self.enrollment)
        self._space(FILE_LIMIT+128*1024*1024)
        require(not any(os.path.lexists(self.path+s) for s in ('-wal','-shm')), 'resident_sidecar')
        if fresh:
            require(not any(os.path.lexists(self.path+s) for s in ('','-journal')), 'resident_enrollment_missing')
            fd = os.open(self.enrollment,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            try:
                require(os.write(fd,expected) == len(expected), 'resident_write'); os.fsync(fd)
            finally: os.close(fd)
            sync_parent(self.path)
        else: require(os.path.lexists(self.path), 'resident_enrolled_state_missing')
        fd = os.open(self.enrollment,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            info = private(self.enrollment,fd)
            require(os.read(fd,len(expected)+1) == expected, 'resident_policy_changed')
            self.enrollment_stamp = (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        finally: os.close(fd)
        try:
            flags = os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK
            self.fd = os.open(self.path,flags|(os.O_CREAT|os.O_EXCL if fresh else 0),0o600)
            fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            info = private(self.path,self.fd)
            self.identity = info.st_dev,info.st_ino
            self._check()
            self.db = sqlite3.connect('file:'+self.path+'?mode=rw',uri=True,isolation_level=None,timeout=0)
            self.db.execute('PRAGMA trusted_schema=OFF')
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.execute('PRAGMA journal_mode=DELETE')
            self.db.execute('PRAGMA secure_delete=ON')
            self.db.execute('PRAGMA temp_store=MEMORY')
            self.db.execute('PRAGMA max_page_count=49152')
            require(self.db.execute('PRAGMA page_size').fetchone()[0] == 4096, 'resident_page_size')
            if fresh:
                self.db.executescript('BEGIN IMMEDIATE; CREATE TABLE state(id INTEGER PRIMARY KEY CHECK(id=1), body BLOB NOT NULL, digest TEXT NOT NULL); CREATE TABLE receipts(seq INTEGER PRIMARY KEY, at_ns INTEGER NOT NULL, body BLOB NOT NULL, digest TEXT NOT NULL); COMMIT;')
                self.state = {'policy':policy,'seq':0,'head':ZERO,'anchor_seq':0,'anchor_hash':ZERO,'anchor_ns':0,
                    'retained_bytes':0,'purged_records':0,'purged_bytes':0,'high_ns':0,
                    'day':0,'boots_today':0,'boots_total':0,'owner_active':False,'last_pulse_ns':0,
                    'sources':{s:{'attempts_today':0,'attempts_total':0,'recent_attempts':[],
                        'retry_ns':0,'terminal':None,'connected':False,'last_receipt_ns':None,
                        'frames_total':0,'records_total':0,'gaps_total':0} for s in ('iex','news')}}
                raw = canonical(self.state)
                self.db.execute('INSERT INTO state VALUES(1,?,?)',(raw,hashlib.sha256(raw).hexdigest()))
                sync_parent(self.path)
            else:
                require(self.db.execute('PRAGMA quick_check').fetchall() == [('ok',)], 'resident_integrity')
                rows = self.db.execute('SELECT body,digest FROM state').fetchall()
                require(len(rows) == 1 and hashlib.sha256(rows[0][0]).hexdigest() == rows[0][1], 'resident_state')
                self.state = json.loads(rows[0][0],object_pairs_hook=unique,parse_constant=invalid)
                require(self.state['policy'] == policy, 'resident_policy_changed')
            require(self.db.execute("SELECT type,name FROM sqlite_master ORDER BY name").fetchall() ==
                [('table','receipts'),('table','state')], 'resident_schema')
            require([r[1:3] for r in self.db.execute('PRAGMA table_info(receipts)')] ==
                [('seq','INTEGER'),('at_ns','INTEGER'),('body','BLOB'),('digest','TEXT')] and
                [r[1:3] for r in self.db.execute('PRAGMA table_info(state)')] ==
                [('id','INTEGER'),('body','BLOB'),('digest','TEXT')], 'resident_schema')
            self._validate_state(self.state); self._verify_chain(); self._check()
        except BaseException:
            self.close(); raise

    def _space(self, amount):
        space = os.statvfs(str(Path(self.path).parent))
        require(space.f_bavail*space.f_frsize >= amount, 'disk_headroom')

    def _check(self):
        require(not self.failed and self.fd is not None, 'resident_unavailable')
        for info in (private(self.path,self.fd),private(self.path)):
            require((info.st_dev,info.st_ino) == self.identity and info.st_size <= FILE_LIMIT, 'resident_identity')
        info = private(self.enrollment)
        require((info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns) == self.enrollment_stamp, 'resident_enrollment_changed')
        require(not any(os.path.lexists(self.path+s) for s in ('-wal','-shm')), 'resident_sidecar')
        if os.path.lexists(self.path+'-journal'): private(self.path+'-journal')

    def _verify_chain(self):
        seq,head,total,high = self.state['anchor_seq'],self.state['anchor_hash'],0,self.state['anchor_ns']
        for index,at,raw,digest in self.db.execute('SELECT seq,at_ns,body,digest FROM receipts ORDER BY seq'):
            require(type(raw) is bytes and len(raw) <= 2*1024*1024, 'resident_chain')
            row = json.loads(raw,object_pairs_hook=unique,parse_constant=invalid)
            require(type(row) is dict and set(row) == {'seq','prev','record'} and
                type(row['record']) is dict and high <= at <= self.state['high_ns'] and
                index == seq+1 and row['seq'] == index and row['prev'] == head and
                row['record']['at_ns'] == at and hashlib.sha256(raw).hexdigest() == digest, 'resident_chain')
            record = row['record']
            require(record.get('event') in ('owner_boot','attempt_reserved','subscription_accepted','control',
                'source_terminal','transport_gap','data','pulse','owner_stopped'), 'resident_chain')
            if 'source' in record: require(record['source'] in ('iex','news'), 'resident_chain')
            if record['event'] == 'data':
                body = base64.b64decode(record['body_base64'],validate=True)
                require(len(body) <= MAX_MESSAGE and hashlib.sha256(body).hexdigest() == record['body_sha256'], 'resident_chain')
            seq,head,total,high = index,digest,total+len(raw),at
        require(seq == self.state['seq'] and head == self.state['head'] and
            total == self.state['retained_bytes'] <= self.policy['retained_bytes'], 'resident_chain')

    def _validate_state(self, value):
        require(type(value) is dict and set(value) == {'policy','seq','head','anchor_seq','anchor_hash',
            'anchor_ns','retained_bytes','purged_records','purged_bytes','high_ns','day','boots_today',
            'boots_total','owner_active','last_pulse_ns','sources'} and value['policy'] == self.policy, 'resident_state')
        for key in ('seq','anchor_seq','anchor_ns','retained_bytes','purged_records','purged_bytes','high_ns',
            'day','boots_today','boots_total','last_pulse_ns'):
            require(type(value[key]) is int and 0 <= value[key] < 2**63, 'resident_state')
        require(all(type(value[k]) is str and re.fullmatch('[0-9a-f]{64}',value[k]) for k in ('head','anchor_hash')) and
            value['anchor_seq'] == value['purged_records'] <= value['seq'] and value['anchor_ns'] <= value['high_ns'] and
            value['day'] == value['high_ns']//DAY and value['boots_today'] <= self.policy['boots_per_day'] and
            value['boots_total'] >= value['boots_today'] and value['last_pulse_ns'] <= value['high_ns'] and
            type(value['owner_active']) is bool and type(value['sources']) is dict and
            set(value['sources']) == {'iex','news'}, 'resident_state')
        for item in value['sources'].values():
            require(type(item) is dict and set(item) == {'attempts_today','attempts_total','recent_attempts','retry_ns',
                'terminal','connected','last_receipt_ns','frames_total','records_total','gaps_total'}, 'resident_state')
            for key in ('attempts_today','attempts_total','retry_ns','frames_total','records_total','gaps_total'):
                require(type(item[key]) is int and 0 <= item[key] < 2**63, 'resident_state')
            require(item['attempts_today'] <= self.policy['attempts_per_day'] and item['attempts_total'] >= item['attempts_today'] and
                type(item['connected']) is bool and (not item['connected'] or value['owner_active']) and
                (item['terminal'] is None or type(item['terminal']) is str and re.fullmatch('[a-z0-9_]{1,80}',item['terminal'])) and
                (item['last_receipt_ns'] is None or type(item['last_receipt_ns']) is int and 0 <= item['last_receipt_ns'] <= value['high_ns']), 'resident_state')
            times = item['recent_attempts']
            require(type(times) is list and len(times) <= self.policy['attempts_per_hour'] and
                all(type(n) is int and 0 <= n <= value['high_ns'] for n in times) and times == sorted(set(times)), 'resident_state')

    def _write(self, event, at, source=None, *, change=None, **values):
        self._check()
        require(type(at) is int and self.state['high_ns'] <= at < 2**63, 'resident_clock')
        state = copy.deepcopy(self.state)
        day = at//DAY
        if day > state['day']:
            state['day'],state['boots_today'] = day,0
            for item in state['sources'].values(): item['attempts_today'] = 0
        require(day == state['day'], 'resident_clock')
        if change: change(state)
        record = dict(event=event,at_ns=at,**values)
        if source is not None: record['source'] = source
        row = {'seq':state['seq']+1,'prev':state['head'],'record':record}
        raw = canonical(row)
        require(len(raw) <= 2*1024*1024, 'resident_record_bound')
        digest = hashlib.sha256(raw).hexdigest()
        state.update(seq=row['seq'],head=digest,high_ns=at)
        state['retained_bytes'] += len(raw)
        # A large age-prune may journal most existing pages. Reserve that worst
        # case plus a floor for the independent ECB/native stores, not only the
        # incoming frame size. Space pressure stops this source before writes.
        self._space(FILE_LIMIT+32*1024*1024+len(raw)*2)
        try:
            self.db.execute('BEGIN IMMEDIATE')
            self.db.execute('INSERT INTO receipts VALUES(?,?,?,?)',(row['seq'],at,raw,digest))
            cutoff = at-self.policy['retained_seconds']*NS
            for seq,stamp,length,head in self.db.execute('SELECT seq,at_ns,length(body),digest FROM receipts ORDER BY seq'):
                if seq == row['seq'] or (state['retained_bytes'] <= self.policy['retained_bytes'] and stamp >= cutoff): break
                state['retained_bytes'] -= length
                state['purged_records'] += 1; state['purged_bytes'] += length
                state['anchor_seq'],state['anchor_hash'],state['anchor_ns'] = seq,head,stamp
            self.db.execute('DELETE FROM receipts WHERE seq<=?',(state['anchor_seq'],))
            self._validate_state(state)
            encoded = canonical(state)
            require(len(encoded) <= 32768, 'resident_state_bound')
            self.db.execute('UPDATE state SET body=?,digest=? WHERE id=1',(encoded,hashlib.sha256(encoded).hexdigest()))
            self.db.execute('COMMIT')
            self._check()
            self.state = state
        except BaseException:
            self.failed = True
            raise

    def boot(self, at):
        previous = self.state['owner_active']
        def change(state):
            require(state['boots_today'] < self.policy['boots_per_day'], 'resident_boot_budget')
            state['boots_today'] += 1; state['boots_total'] += 1
            state['owner_active'],state['last_pulse_ns'] = True,at
            for source in state['sources'].values():
                if source['attempts_total']: source['gaps_total'] += 1
                source['connected'] = False
        self._write('owner_boot',at,change=change,previous_owner_active=previous,
            previous_pulse_ns=self.state['last_pulse_ns'],restart_gap_unfilled=True)

    def due(self, source, at):
        require(at >= self.state['high_ns'], 'resident_clock')
        item = self.state['sources'][source]
        if item['terminal']: return None
        due = max(at,item['retry_ns'])
        if at//DAY == self.state['day'] and item['attempts_today'] >= self.policy['attempts_per_day']:
            due = max(due,(at//DAY+1)*DAY)
        recent = [n for n in item['recent_attempts'] if at-n < 3600*NS]
        if len(recent) >= self.policy['attempts_per_hour']: due = max(due,recent[-self.policy['attempts_per_hour']]+3600*NS)
        return due

    def attempt(self, source, at):
        require(self.due(source,at) == at, 'resident_attempt_budget')
        def change(state):
            item = state['sources'][source]
            item['attempts_today'] += 1; item['attempts_total'] += 1
            item['recent_attempts'] = [n for n in item['recent_attempts'] if at-n < 3600*NS]+[at]
            item['retry_ns'] = at+60*NS
        self._write('attempt_reserved',at,source,change=change)

    def connected(self, source, at):
        self._write('subscription_accepted',at,source,change=lambda state: state['sources'][source].update(connected=True))

    def gap(self, source, at, reason, terminal=False):
        def change(state):
            item = state['sources'][source]
            item['connected'] = False; item['gaps_total'] += 1
            delay = (60,120,300,900,1800,3600,7200,14400)[max(1,min(item['attempts_today'],8))-1]
            item['retry_ns'] = max(item['retry_ns'],at+delay*NS)
            if terminal: item['terminal'] = reason
        self._write('source_terminal' if terminal else 'transport_gap',at,source,change=change,
            reason=reason,unfilled=True,last_receipt_ns=self.state['sources'][source]['last_receipt_ns'])

    def body(self, source, raw, at, summary):
        require(type(raw) is bytes and len(raw) <= MAX_MESSAGE, 'resident_record_bound')
        def change(state):
            item = state['sources'][source]
            item['last_receipt_ns'] = at
            if summary.get('kind') not in ('control','invalid'):
                item['frames_total'] += 1; item['records_total'] += summary['records']
        self._write('data',at,source,change=change,body_base64=base64.b64encode(raw).decode(),
            body_sha256=hashlib.sha256(raw).hexdigest(),
            summary={k:v for k,v in summary.items() if k not in ('quote_times','news_keys')})

    def pulse(self, at):
        self._write('pulse',at,change=lambda state: state.update(last_pulse_ns=at))

    def stopped(self, at, reason):
        def change(state):
            state['owner_active'] = False
            for item in state['sources'].values():
                if item['connected']: item['gaps_total'] += 1
                item['connected'] = False
        self._write('owner_stopped',at,change=change,reason=reason,unfilled=True)

    def snapshot(self, after_seq, *, max_records=64, max_bytes=4*1024*1024):
        """Owner-held local handoff only: no export, consumer ACK or cursor write.

        A consumer behind the retention anchor receives an explicit missing-prefix
        flag. Hashes authenticate local chain integrity, not the provider itself.
        """
        self._check()
        require(type(after_seq) is int and 0 <= after_seq <= self.state['seq'] and
            type(max_records) is int and 1 <= max_records <= 64 and
            type(max_bytes) is int and 2*1024*1024 <= max_bytes <= 4*1024*1024 and
            not self.db.in_transaction, 'resident_snapshot')
        state = copy.deepcopy(self.state)
        start = max(after_seq,state['anchor_seq'])
        previous = (state['anchor_hash'] if start == state['anchor_seq'] else
            self.db.execute('SELECT digest FROM receipts WHERE seq=?',(start,)).fetchone()[0])
        rows,total,next_seq,page_head = [],0,start,previous
        for seq,raw,digest in self.db.execute('SELECT seq,body,digest FROM receipts WHERE seq>? ORDER BY seq LIMIT ?',
            (start,max_records)):
            if total+len(raw) > max_bytes: break
            row = json.loads(raw,object_pairs_hook=unique,parse_constant=invalid)
            require(seq == next_seq+1 and row['seq'] == seq and row['prev'] == page_head and
                hashlib.sha256(raw).hexdigest() == digest, 'resident_chain')
            rows.append(dict(row,hash=digest)); total += len(raw)
            next_seq,page_head = seq,digest
        self._check()
        return {'schema':'resident-source-window-v2','policy':copy.deepcopy(self.policy),
            'policy_sha256':hashlib.sha256(canonical(self.policy)).hexdigest(),'predecessor':dict(PREDECESSOR),
            'head_seq':state['seq'],'head_sha256':state['head'],'anchor_seq':state['anchor_seq'],
            'anchor_sha256':state['anchor_hash'],'purged_records':state['purged_records'],
            'requested_after_seq':after_seq,'from_seq':start,'from_sha256':previous,
            'next_seq':next_seq,'page_sha256':page_head,'rows':rows,'has_more':next_seq < state['seq'],
            'retention_gap':after_seq < state['anchor_seq'],'provider_coverage_complete':False,
            'consumer_cursor_committed':False,'order_permission':'none'}

    def close(self):
        if self.db is not None:
            self.db.close(); self.db = None
        if self.fd is not None:
            os.close(self.fd); self.fd = None
