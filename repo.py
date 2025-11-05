# queuectl/repo.py
import uuid
from datetime import datetime, timedelta
from typing import Optional, List
from .db import tx
from .models import now_iso, ISO

def enqueue(command: str, max_retries: int = 3) -> str:
    job_id = str(uuid.uuid4())
    with tx() as conn:
        conn.execute("""
            INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
        """, (job_id, command, "pending", 0, max_retries, now_iso(), now_iso()))
    return job_id

def list_jobs(state: Optional[str] = None) -> List[dict]:
    with tx() as conn:
        cur = conn.execute(
            "SELECT * FROM jobs WHERE (? IS NULL OR state=?) ORDER BY created_at",
            (state, state)
        )
        return [dict(r) for r in cur.fetchall()]

def get_ready_job() -> Optional[dict]:
    with tx() as conn:
        cur = conn.execute("""
            SELECT id FROM jobs
            WHERE (state='pending' OR state='failed')
              AND (next_run_at IS NULL OR next_run_at <= ?)
            ORDER BY created_at
            LIMIT 1
        """, (now_iso(),))
        row = cur.fetchone()
        if not row: return None
        job_id = row["id"]
        cur2 = conn.execute("""
            UPDATE jobs SET state='running', updated_at=?
            WHERE id=? AND state!='running'
        """, (now_iso(), job_id))
        if cur2.rowcount == 0: return None
        cur3 = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        return dict(cur3.fetchone())

def succeed(job_id: str):
    with tx() as conn:
        conn.execute("""
            UPDATE jobs SET state='succeeded', updated_at=?, last_error=NULL, next_run_at=NULL
            WHERE id=?
        """, (now_iso(), job_id))

def to_failed_with_retry(job: dict, error: str, base_backoff: int = 2):
    attempts = job["attempts"] + 1
    if attempts > job["max_retries"]:
        to_dlq(job, error); return
    delay = base_backoff * (2 ** (attempts - 1))
    next_time = (datetime.utcnow() + timedelta(seconds=delay)).strftime(ISO)
    with tx() as conn:
        conn.execute("""
            UPDATE jobs
            SET state='failed',
                attempts=?,
                last_error=?,
                next_run_at=?,
                updated_at=?
            WHERE id=?
        """, (attempts, error[:1000], next_time, now_iso(), job["id"]))

def to_dlq(job: dict, error: str):
    with tx() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO dlq(id, command, attempts, max_retries, created_at, failed_at, last_error)
            VALUES(?,?,?,?,?,?,?)
        """, (job["id"], job["command"], job["attempts"] + 1, job["max_retries"],
              job["created_at"], now_iso(), error[:1000]))
        conn.execute("""
            UPDATE jobs SET state='dead', attempts=?, last_error=?, updated_at=?, next_run_at=NULL
            WHERE id=?
        """, (job["attempts"] + 1, error[:1000], now_iso(), job["id"]))

def list_dlq() -> List[dict]:
    with tx() as conn:
        cur = conn.execute("SELECT * FROM dlq ORDER BY failed_at DESC")
        return [dict(r) for r in cur.fetchall()]

def requeue_from_dlq(job_id: str) -> Optional[str]:
    with tx() as conn:
        cur = conn.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
        r = cur.fetchone()
        if not r: return None
        conn.execute("DELETE FROM dlq WHERE id=?", (job_id,))
        conn.execute("""
            UPDATE jobs
            SET state='pending', attempts=0, last_error=NULL, next_run_at=NULL, updated_at=?
            WHERE id=?
        """, (now_iso(), job_id))
        return job_id

