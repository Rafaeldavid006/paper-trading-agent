"""Private append-only, locked, bounded stream journal; never repairs or resets."""
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat

from live_source_protocol import MAX_MESSAGE, MAX_STORE, Refused, require, unique, invalid

PATH = '/var/data/live-source-stream-v1.jsonl'
LINE_LIMIT = 2*1024*1024


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


class Journal:
    def __init__(self, grant, path=PATH, *, test_only=False):
        require(test_only is True or path == PATH, 'journal_path')
        self.path, self.grant = str(path), grant
        self.fd, self.failed = None, False
        self.seq, self.head, self.size, self.high_ns = 0, '0'*64, 0, 0
        self.attempts = {s:0 for s in ('iex','news')}
        self.terminal = {}
        self.completed = False
        for parent in reversed(Path(path).parents):
            require(stat.S_ISDIR(parent.stat(follow_symlinks=False).st_mode), 'journal_parent')
        require(not any(os.path.lexists(self.path+s) for s in ('-journal','-wal','-shm')), 'journal_sidecar')
        space = os.statvfs(str(Path(path).parent))
        require(space.f_bavail*space.f_frsize >= grant['max_bytes']+128*1024*1024, 'disk_headroom')
        self.enrollment = self.path+'.enrolled'
        newly_enrolled = self._enroll()
        created = False
        try:
            if newly_enrolled:
                self.fd = os.open(self.path, os.O_RDWR|os.O_APPEND|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                created = True
            else:
                self.fd = os.open(self.path, os.O_RDWR|os.O_APPEND|os.O_NOFOLLOW|os.O_NONBLOCK)
            info = os.fstat(self.fd)
            self.identity = info.st_dev,info.st_ino
            self._check()
            fcntl.flock(self.fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
            require(info.st_size+self.enrolled_bytes <= grant['max_bytes'] <= MAX_STORE, 'journal_bound')
            if created:
                self.append({'event':'grant','grant':grant,'at_ns':grant['starts_ns']})
                fd = os.open(str(Path(path).parent),os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                try: os.fsync(fd)
                finally: os.close(fd)
            else:
                require(info.st_size > 0, 'incomplete_journal')
                with os.fdopen(os.dup(self.fd),'rb') as stream:
                    stream.seek(0)
                    while True:
                        raw = stream.readline(LINE_LIMIT+1)
                        if not raw: break
                        require(raw.endswith(b'\n') and len(raw) <= LINE_LIMIT, 'incomplete_journal')
                        row = json.loads(raw,object_pairs_hook=unique,parse_constant=invalid)
                        require(type(row) is dict and set(row) == {'seq','prev','record','hash'}, 'journal_shape')
                        require(type(row['seq']) is int and row['seq'] == self.seq+1 and row['prev'] == self.head, 'journal_chain')
                        digest = hashlib.sha256(canonical({k:row[k] for k in ('seq','prev','record')})).hexdigest()
                        require(row['hash'] == digest, 'journal_hash')
                        self._apply(row['record'])
                        self.seq, self.head = row['seq'], digest
                        self.size += len(raw)
                require(self.size == info.st_size, 'journal_changed')
            self._check()
        except BaseException:
            self.close()
            raise

    def _enroll(self):
        expected = canonical({'version':1,'grant_sha256':hashlib.sha256(canonical(self.grant)).hexdigest()})+b'\n'
        newly_enrolled = False
        if not os.path.lexists(self.enrollment):
            require(not os.path.lexists(self.path), 'enrollment_missing')
            fd = os.open(self.enrollment,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            try:
                require(os.write(fd,expected) == len(expected), 'enrollment_write')
                os.fsync(fd)
            finally: os.close(fd)
            parent = os.open(str(Path(self.path).parent),os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try: os.fsync(parent)
            finally: os.close(parent)
            newly_enrolled = True
        else:
            require(os.path.lexists(self.path), 'enrolled_journal_missing')
        fd = os.open(self.enrollment,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600 and
                    info.st_uid == os.geteuid() and info.st_nlink == 1 and info.st_size == len(expected), 'enrollment_identity')
            require(os.read(fd,2049) == expected, 'enrollment_grant')
            self.enrolled_bytes = info.st_size
            self.enrollment_stamp = (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        finally: os.close(fd)
        return newly_enrolled

    def _check(self):
        require(not self.failed and self.fd is not None, 'journal_unavailable')
        enrolled = os.stat(self.enrollment,follow_symlinks=False)
        require(stat.S_ISREG(enrolled.st_mode) and stat.S_IMODE(enrolled.st_mode) == 0o600 and
                enrolled.st_uid == os.geteuid() and enrolled.st_nlink == 1 and
                (enrolled.st_dev,enrolled.st_ino,enrolled.st_size,enrolled.st_mtime_ns,enrolled.st_ctime_ns) == self.enrollment_stamp, 'enrollment_changed')
        for info in (os.fstat(self.fd),os.stat(self.path,follow_symlinks=False)):
            require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600 and
                    info.st_uid == os.geteuid() and info.st_nlink == 1 and
                    (info.st_dev,info.st_ino) == self.identity, 'journal_identity')

    def _apply(self, record):
        require(type(record) is dict and type(record.get('at_ns')) is int and
                self.high_ns <= record['at_ns'] < 2**63, 'journal_clock')
        event = record.get('event')
        if self.seq == 0:
            require(event == 'grant' and record.get('grant') == self.grant, 'grant_identity')
        else:
            require(event in ('boot','attempt','control','data','gap','source_terminal','completed'), 'journal_event')
            require(not self.completed, 'grant_completed')
            if event == 'attempt':
                source = record.get('source')
                require(source in self.attempts and source not in self.terminal and
                        record.get('attempt') == self.attempts[source]+1 <= self.grant['max_attempts'], 'attempt_budget')
                self.attempts[source] += 1
            elif event == 'source_terminal':
                require(record.get('source') in self.attempts and type(record.get('reason')) is str, 'journal_event')
                self.terminal[record['source']] = record['reason']
            elif event == 'completed':
                self.completed = True
            elif event == 'data':
                require(record.get('source') in self.attempts and type(record.get('body_base64')) is str, 'journal_data')
                raw = base64.b64decode(record['body_base64'],validate=True)
                require(len(raw) <= MAX_MESSAGE and hashlib.sha256(raw).hexdigest() == record.get('body_sha256'), 'journal_data')
        self.high_ns = record['at_ns']

    def append(self, record, *, data=False):
        self._check()
        row = {'seq':self.seq+1,'prev':self.head,'record':record}
        digest = hashlib.sha256(canonical(row)).hexdigest()
        raw = canonical(dict(row,hash=digest))+b'\n'
        limit = self.grant['max_bytes']-self.enrolled_bytes-(8192 if data else 0)
        require(len(raw) <= LINE_LIMIT and self.size+len(raw) <= limit, 'storage_budget')
        require(os.fstat(self.fd).st_size == self.size, 'journal_changed')
        space = os.statvfs(str(Path(self.path).parent))
        require(space.f_bavail*space.f_frsize >= len(raw)+32*1024*1024, 'disk_headroom')
        # Validate and update in-memory state before write; any write uncertainty
        # poisons this owner. No later request can refund or reuse a reservation.
        self._apply(record)
        try:
            pending = raw
            while pending:
                n = os.write(self.fd,pending)
                require(n > 0, 'journal_write')
                pending = pending[n:]
            os.fsync(self.fd)
            self._check()
            self.size += len(raw)
            require(os.fstat(self.fd).st_size == self.size, 'journal_changed')
            self.seq, self.head = row['seq'], digest
        except BaseException:
            self.failed = True
            raise

    def body(self, source, raw, received_ns, summary):
        self.append(dict(event='data',source=source,at_ns=received_ns,
            body_sha256=hashlib.sha256(raw).hexdigest(),body_base64=base64.b64encode(raw).decode(),
            summary={k:v for k,v in summary.items() if k not in ('quote_times','news_keys')}),data=True)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
