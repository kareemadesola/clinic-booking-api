"""Clinic booking API (Koya Cohort 4 assessment, task 1).

FastAPI + Postgres. Correctness under concurrency comes from the database:
  - a partial unique index allows only one confirmed booking per slot
  - advisory transaction locks serialise the rate limit per email and
    idempotency handling per key
"""
import base64
import binascii
import hashlib
import hmac
import json
import math
import os
import random
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ["DATABASE_URL"]
API_KEY = os.environ["API_KEY"]
RATE_LIMIT = 5
RATE_WINDOW_SECONDS = 60
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
INDEX_HTML = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")

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
    max_size=int(os.environ.get("DB_POOL_MAX", "20")),
    kwargs={"prepare_threshold": None, "autocommit": False},  # pgbouncer-safe
    check=ConnectionPool.check_connection,
    open=False,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True)
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(1)")  # one worker creates the schema
        conn.execute(SCHEMA)
    yield
    pool.close()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


def err(status, message, headers=None):
    return JSONResponse({"error": message}, status_code=status, headers=headers)


def iso(dt):
    dt = dt.astimezone(timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if dt.microsecond else "%Y-%m-%dT%H:%M:%SZ"
    return dt.strftime(fmt)


def iso_ms(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_dt(value):
    if not isinstance(value, str):
        raise ValueError("not a string")
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def booking_json(row):
    return {
        "id": row["id"],
        "slot_id": row["slot_id"],
        "name": row["name"],
        "email": row["email"],
        "status": row["status"],
        "created_at": iso_ms(row["created_at"]),
    }


BOOKING_COLS = "id, slot_id, name, email, status, created_at"


def rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    names = [c.name for c in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


@app.middleware("http")
async def require_key(request: Request, call_next):
    if request.url.path == "/":  # the web page itself; its API calls carry the key
        return await call_next(request)
    given = request.headers.get("x-api-key", "")
    if not hmac.compare_digest(given.encode(), API_KEY.encode()):
        return err(401, "missing or invalid API key")
    return await call_next(request)


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML.replace("__API_KEY__", json.dumps(API_KEY)[1:-1])


@app.get("/health")
def health():
    return {"ok": True}


async def read_json(request: Request):
    raw = await request.body()
    try:
        return json.loads(raw), raw
    except (ValueError, UnicodeDecodeError):
        return None, raw


@app.post("/reset")
async def reset(request: Request):
    data, _ = await read_json(request)
    if not isinstance(data, dict) or not isinstance(data.get("slots"), list):
        return err(400, "body must be {\"slots\": [...]}")
    slots = []
    for s in data["slots"]:
        try:
            if not isinstance(s, dict):
                raise ValueError
            sid, provider = s["id"], s["provider"]
            if not isinstance(sid, str) or not sid or not isinstance(provider, str):
                raise ValueError
            slots.append((sid, provider, parse_dt(s["start"]), parse_dt(s["end"]), s["start"].strip(), s["end"].strip()))
        except (KeyError, ValueError, TypeError):
            return err(400, "each slot needs id, provider, start and end")
    if len({s[0] for s in slots}) != len(slots):
        return err(400, "duplicate slot id")
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(2)")
        conn.execute("TRUNCATE bookings, slots, attempts, idempotency RESTART IDENTITY CASCADE")
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO slots (id, provider, start_at, end_at, start_raw, end_raw) VALUES (%s, %s, %s, %s, %s, %s)",
                slots,
            )
    return {"ok": True, "slots": len(slots)}


@app.get("/slots")
def list_slots(provider: str | None = None):
    sql = """
        SELECT s.id, s.provider, s.start_at, s.end_at, s.start_raw, s.end_raw,
               NOT EXISTS (SELECT 1 FROM bookings b
                           WHERE b.slot_id = s.id AND b.status = 'confirmed') AS available
        FROM slots s
        {where}
        ORDER BY s.start_at, s.id
    """
    where, params = ("WHERE s.provider = %s", (provider,)) if provider is not None else ("", ())
    with pool.connection() as conn:
        result = rows(conn, sql.format(where=where), params)
    return {
        "slots": [
            {
                "id": r["id"],
                "provider": r["provider"],
                # echo the timestamps exactly as they were sent to /reset
                "start": r["start_raw"] or iso(r["start_at"]),
                "end": r["end_raw"] or iso(r["end_at"]),
                "available": r["available"],
            }
            for r in result
        ]
    }


def check_rate_limit(conn, email):
    """Count this attempt; return a Retry-After in seconds if the limit is hit.

    The advisory lock and the count-then-insert statement are pipelined into a
    single round trip. Statements still run in order on the server, so the
    count sees every attempt committed before the lock was granted."""
    with conn.pipeline():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("rl:" + email,))
        cur = conn.execute(
            """
            WITH c AS (
                SELECT count(*) AS n, min(at) AS oldest FROM attempts
                WHERE email = %(e)s AND at > clock_timestamp() - make_interval(secs => %(w)s)
            ), i AS (
                INSERT INTO attempts (email) SELECT %(e)s WHERE (SELECT n FROM c) < %(lim)s RETURNING 1
            )
            SELECT n, EXTRACT(EPOCH FROM (oldest + make_interval(secs => %(w)s) - clock_timestamp())) FROM c
            """,
            {"e": email, "w": RATE_WINDOW_SECONDS, "lim": RATE_LIMIT},
        )
    count, wait = cur.fetchone()
    if random.random() < 0.02:  # keep the table small without a delete on every request
        conn.execute("DELETE FROM attempts WHERE at < clock_timestamp() - interval '10 minutes'")
    if count >= RATE_LIMIT:
        return max(1, math.ceil(wait or RATE_WINDOW_SECONDS))
    return None


def create_booking(conn, data):
    """Returns (status, payload). Never raises for expected failures so the
    surrounding transaction (and the rate-limit attempt) is committed."""
    name = data.get("name") if isinstance(data, dict) else None
    email = data.get("email") if isinstance(data, dict) else None
    slot_id = data.get("slot_id") if isinstance(data, dict) else None
    name = name.strip() if isinstance(name, str) else ""
    email = email.strip().lower() if isinstance(email, str) else ""
    valid_email = bool(EMAIL_RE.match(email)) and len(email) <= 254

    if valid_email:
        retry_after = check_rate_limit(conn, email)
        if retry_after is not None:
            return 429, {"error": "too many booking attempts, slow down"}, {"Retry-After": str(retry_after)}

    if not isinstance(slot_id, str) or not slot_id.strip():
        return 400, {"error": "slot_id is required"}, None
    if not name:
        return 400, {"error": "name is required"}, None
    if not email:
        return 400, {"error": "email is required"}, None
    if not valid_email:
        return 400, {"error": "email is not valid"}, None

    created = rows(
        conn,
        "INSERT INTO bookings (id, slot_id, name, email, status) "
        "SELECT %s, s.id, %s, %s, 'confirmed' FROM slots s "
        "WHERE s.id = %s AND s.start_at > clock_timestamp() "
        "ON CONFLICT (slot_id) WHERE status = 'confirmed' DO NOTHING "
        f"RETURNING {BOOKING_COLS}",
        (uuid.uuid4().hex, name, email, slot_id),
    )
    if not created:
        slot = rows(conn, "SELECT start_at <= clock_timestamp() AS started FROM slots WHERE id = %s", (slot_id,))
        if not slot:
            return 404, {"error": "slot not found"}, None
        if slot[0]["started"]:
            return 400, {"error": "slot has already started"}, None
        return 409, {"error": "slot already booked"}, None
    return 201, booking_json(created[0]), None


@app.post("/bookings")
async def post_booking(request: Request):
    data, raw = await read_json(request)
    key = request.headers.get("idempotency-key")
    body_hash = hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode() if data is not None else raw
    ).hexdigest()

    with pool.connection() as conn:
        if key:
            with conn.pipeline():
                conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("idem:" + key,))
                cur = conn.execute("SELECT body_hash, status, response FROM idempotency WHERE key = %s", (key,))
            seen = [dict(zip(("body_hash", "status", "response"), r)) for r in cur.fetchall()]
            if seen:
                if seen[0]["body_hash"] != body_hash:
                    return err(422, "Idempotency-Key was already used with a different request body")
                return JSONResponse(seen[0]["response"], status_code=seen[0]["status"])

        if not isinstance(data, dict):
            status, payload, headers = 400, {"error": "body must be a JSON object"}, None
        else:
            status, payload, headers = create_booking(conn, data)

        if key and status != 429:
            conn.execute(
                "INSERT INTO idempotency (key, body_hash, status, response) VALUES (%s, %s, %s, %s)",
                (key, body_hash, status, json.dumps(payload)),
            )
    return JSONResponse(payload, status_code=status, headers=headers)


@app.get("/bookings")
def list_bookings(email: str | None = None, limit: str | None = None, cursor: str | None = None):
    if limit is None:
        n = 10
    else:
        try:
            n = int(limit)
        except ValueError:
            return err(400, "limit must be an integer from 1 to 50")
        if not 1 <= n <= 50:
            return err(400, "limit must be an integer from 1 to 50")
    if not email or not email.strip():
        return err(400, "email is required")
    after = 0
    if cursor:
        try:
            after = int(base64.urlsafe_b64decode(cursor.encode()).decode())
        except (ValueError, binascii.Error, UnicodeDecodeError):
            return err(400, "invalid cursor")
    with pool.connection() as conn:
        found = rows(
            conn,
            f"SELECT seq, {BOOKING_COLS} FROM bookings WHERE email = %s AND seq > %s ORDER BY seq LIMIT %s",
            (email.strip().lower(), after, n + 1),
        )
    page = found[:n]
    next_cursor = None
    if len(found) > n:
        next_cursor = base64.urlsafe_b64encode(str(page[-1]["seq"]).encode()).decode()
    return {"bookings": [booking_json(r) for r in page], "next_cursor": next_cursor}


@app.get("/bookings/{booking_id}")
def get_booking(booking_id: str):
    with pool.connection() as conn:
        found = rows(conn, f"SELECT {BOOKING_COLS} FROM bookings WHERE id = %s", (booking_id,))
    if not found:
        return err(404, "booking not found")
    return booking_json(found[0])


@app.delete("/bookings/{booking_id}")
def cancel_booking(booking_id: str):
    with pool.connection() as conn:
        updated = rows(
            conn,
            f"UPDATE bookings SET status = 'cancelled' WHERE id = %s AND status = 'confirmed' RETURNING {BOOKING_COLS}",
            (booking_id,),
        )
        if updated:
            return booking_json(updated[0])
        exists = conn.execute("SELECT 1 FROM bookings WHERE id = %s", (booking_id,)).fetchone()
    return err(409, "booking is already cancelled") if exists else err(404, "booking not found")


from starlette.exceptions import HTTPException as StarletteHTTPException  # noqa: E402


@app.exception_handler(StarletteHTTPException)
async def http_error(_, exc: StarletteHTTPException):
    return err(exc.status_code, "not found" if exc.status_code == 404 else str(exc.detail))
