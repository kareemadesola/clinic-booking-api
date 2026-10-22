"""Connection pool and schema."""
from psycopg_pool import ConnectionPool

from app.config import DATABASE_URL, DB_POOL_MAX

SCHEMA = """
CREATE TABLE IF NOT EXISTS slots (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    start_raw TEXT,
    end_raw TEXT
);
ALTER TABLE slots ADD COLUMN IF NOT EXISTS start_raw TEXT;
ALTER TABLE slots ADD COLUMN IF NOT EXISTS end_raw TEXT;
CREATE TABLE IF NOT EXISTS bookings (
    seq BIGSERIAL PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    slot_id TEXT NOT NULL REFERENCES slots(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX IF NOT EXISTS one_confirmed_per_slot
    ON bookings (slot_id) WHERE status = 'confirmed';
CREATE INDEX IF NOT EXISTS bookings_email_seq ON bookings (email, seq);
CREATE TABLE IF NOT EXISTS attempts (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS attempts_email_at ON attempts (email, at);
CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    body_hash TEXT NOT NULL,
    status INT NOT NULL,
    response JSONB NOT NULL
);
"""

pool = ConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=DB_POOL_MAX,
    kwargs={"prepare_threshold": None, "autocommit": False},  # pgbouncer-safe
    check=ConnectionPool.check_connection,
    open=False,
)


def init_schema():
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(1)")  # one worker creates the schema
        conn.execute(SCHEMA)


def rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    names = [c.name for c in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]
