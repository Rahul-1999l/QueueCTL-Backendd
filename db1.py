import sqlite3
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path.home() / ".queuectl.db"

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  command TEXT NOT NULL,
  state TEXT NOT NULL,                -- pending | running | succeeded | failed | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_error TEXT,
  next_run_at TEXT
);

CREATE TABLE IF NOT EXISTS dlq (
  id TEXT PRIMARY KEY,
  command TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  max_retries INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  failed_at TEXT NOT NULL,
  last_error TEXT
);
"""

def connect():
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def init():
    conn = connect()
    with conn:
        conn.executescript(SCHEMA)
    conn.close()

@contextmanager
def tx():
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
