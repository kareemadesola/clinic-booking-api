from fastapi import APIRouter, Request

from app.db import pool
from app.http import err, read_json
from app.timeutil import parse_dt

router = APIRouter()


@router.post("/reset")
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
            # keep the strings as sent so /slots can echo them back unchanged
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
