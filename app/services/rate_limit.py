"""Level 5: per-email rate limit as a sliding window stored in Postgres."""
import math
import random

from app.config import RATE_LIMIT, RATE_WINDOW_SECONDS


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
