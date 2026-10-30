import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager


class LeaseLost(RuntimeError):
    """A stale worker tried to change a job it no longer owns."""


def positive(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be a finite positive number")


def encode(value):
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))


class Queue:
    """One short-lived connection per operation; safe for threads/processes.

    SQLite's BEGIN IMMEDIATE serializes claims. Lease tokens fence stale workers.
    At-least-once execution: application side effects must be idempotent.
    """

    def __init__(self, path="queue.db", clock=time.time):
        if str(path) == ":memory:":
            raise ValueError("Use a filesystem database; connections are per operation")
        self.path, self.clock = str(path), clock
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('queued','running','done','dead')),
                    attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
                    available_at REAL NOT NULL, created_at REAL NOT NULL,
                    worker TEXT, token TEXT, lease_until REAL, result TEXT, error TEXT,
                    dedupe_key TEXT UNIQUE
                );
                CREATE INDEX IF NOT EXISTS ready ON jobs(state,available_at,created_at);
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
                    at REAL NOT NULL, event TEXT NOT NULL, detail TEXT
                );
            """)

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _write(self):
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _event(self, db, job_id, event, detail=None):
        db.execute("INSERT INTO events(job_id,at,event,detail) VALUES(?,?,?,?)",
                   (job_id, self.clock(), event, detail))

    @staticmethod
    def _decode(row):
        if row is None:
            return None
        value = dict(row)
        for field in ("payload", "result"):
            if value[field] is not None:
                value[field] = json.loads(value[field])
        return value

    def enqueue(self, kind, payload, *, max_attempts=3, delay=0, dedupe_key=None):
        if not isinstance(kind, str) or not kind.strip() or len(kind) > 128:
            raise ValueError("kind must be a nonempty string of at most 128 characters")
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be an integer from 1 to 100")
        if isinstance(delay, bool) or not isinstance(delay, (int, float)) or not math.isfinite(delay) or delay < 0:
            raise ValueError("delay must be finite and nonnegative")
        if dedupe_key is not None and (not isinstance(dedupe_key, str) or not dedupe_key or len(dedupe_key) > 256):
            raise ValueError("dedupe_key must be a nonempty string of at most 256 characters")
        packed = encode(payload)
        now = self.clock()
        with self._write() as db:
            if dedupe_key is not None:
                old = db.execute("SELECT * FROM jobs WHERE dedupe_key=?", (dedupe_key,)).fetchone()
                if old:
                    if old["kind"] != kind or old["payload"] != packed or old["max_attempts"] != max_attempts:
                        raise ValueError("dedupe_key already belongs to a different job")
                    return old["id"]
            job_id = uuid.uuid4().hex
            db.execute("""INSERT INTO jobs(id,kind,payload,state,max_attempts,available_at,created_at,dedupe_key)
                          VALUES(?,?,?,'queued',?,?,?,?)""",
                       (job_id, kind, packed, max_attempts, now + delay, now, dedupe_key))
            self._event(db, job_id, "enqueued")
            return job_id

    def _recover(self, db, now):
        expired = db.execute("SELECT * FROM jobs WHERE state='running' AND lease_until<=?", (now,)).fetchall()
        for job in expired:
            state = "dead" if job["attempts"] >= job["max_attempts"] else "queued"
            db.execute("""UPDATE jobs SET state=?,available_at=?,worker=NULL,token=NULL,
                          lease_until=NULL,error='lease expired' WHERE id=?""", (state, now, job["id"]))
            self._event(db, job["id"], "lease_expired", state)
        return len(expired)

    def recover(self):
        with self._write() as db:
            return self._recover(db, self.clock())

    def claim(self, worker, *, lease_seconds=30):
        if not isinstance(worker, str) or not worker.strip():
            raise ValueError("worker must be a nonempty string")
        positive(lease_seconds, "lease_seconds")
        with self._write() as db:
            now = self.clock()
            self._recover(db, now)
            row = db.execute("""SELECT * FROM jobs WHERE state='queued' AND available_at<=?
                                ORDER BY available_at,created_at,id LIMIT 1""", (now,)).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            db.execute("""UPDATE jobs SET state='running',attempts=attempts+1,worker=?,
                          token=?,lease_until=? WHERE id=?""", (worker, token, now + lease_seconds, row["id"]))
            self._event(db, row["id"], "claimed", worker)
            return self._decode(db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())

    def _owned(self, db, job_id, token):
        row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row or row["state"] != "running" or not token or row["token"] != token or row["lease_until"] <= self.clock():
            raise LeaseLost("Job is missing, expired, completed, or owned by another worker")
        return row

    def heartbeat(self, job_id, token, *, lease_seconds=30):
        positive(lease_seconds, "lease_seconds")
        with self._write() as db:
            self._owned(db, job_id, token)
            db.execute("UPDATE jobs SET lease_until=? WHERE id=?", (self.clock() + lease_seconds, job_id))

    def ack(self, job_id, token, result=None):
        packed = encode(result)
        with self._write() as db:
            self._owned(db, job_id, token)
            db.execute("""UPDATE jobs SET state='done',result=?,error=NULL,token=NULL,
                          worker=NULL,lease_until=NULL WHERE id=?""", (packed, job_id))
            self._event(db, job_id, "completed")

    def fail(self, job_id, token, error, *, retry_base=1):
        positive(retry_base, "retry_base")
        with self._write() as db:
            job = self._owned(db, job_id, token)
            state = "dead" if job["attempts"] >= job["max_attempts"] else "queued"
            delay = min(3600, retry_base * 2 ** (job["attempts"] - 1))
            db.execute("""UPDATE jobs SET state=?,available_at=?,error=?,token=NULL,
                          worker=NULL,lease_until=NULL WHERE id=?""",
                       (state, self.clock() + delay, str(error)[:2000], job_id))
            self._event(db, job_id, "failed", state)

    def retry(self, job_id):
        with self._write() as db:
            row = db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row or row["state"] != "dead":
                raise ValueError("Only a dead-letter job can be retried")
            db.execute("UPDATE jobs SET state='queued',attempts=0,available_at=? WHERE id=?", (self.clock(), job_id))
            self._event(db, job_id, "manually_retried")

    def get(self, job_id):
        with self._connect() as db:
            return self._decode(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def jobs(self, *, state=None, limit=50, offset=0):
        if state not in (None, "queued", "running", "done", "dead"):
            raise ValueError("Unknown state")
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise ValueError("limit must be 1..100 and offset nonnegative")
        with self._connect() as db:
            rows = db.execute("""SELECT * FROM jobs WHERE (? IS NULL OR state=?)
                                 ORDER BY created_at,id LIMIT ? OFFSET ?""", (state, state, limit, offset)).fetchall()
            return [self._decode(row) for row in rows]

    def history(self, job_id):
        with self._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM events WHERE job_id=? ORDER BY seq", (job_id,))]
