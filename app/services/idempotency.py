"""Level 4: Idempotency-Key handling, stored in Postgres."""
import hashlib
import json


def body_hash(data, raw):
    """Hash of the canonical JSON body (or the raw bytes if it is not JSON)."""
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":")).encode() if data is not None else raw
    return hashlib.sha256(canonical).hexdigest()


def lookup(conn, key):
    """Take a per-key advisory lock, then return the stored outcome or None.

    The lock lasts until the transaction ends, so a concurrent retry waits here
    and then sees the stored response instead of creating a second booking."""
    with conn.pipeline():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("idem:" + key,))
        cur = conn.execute("SELECT body_hash, status, response FROM idempotency WHERE key = %s", (key,))
    found = cur.fetchone()
    return dict(zip(("body_hash", "status", "response"), found)) if found else None


def store(conn, key, hashed, status, payload):
    conn.execute(
        "INSERT INTO idempotency (key, body_hash, status, response) VALUES (%s, %s, %s, %s)",
        (key, hashed, status, json.dumps(payload)),
    )
