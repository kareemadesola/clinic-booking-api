from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.db import pool
from app.http import err, read_json
from app.services import booking, idempotency
from app.services.pagination import decode_cursor, encode_cursor

router = APIRouter()


@router.post("/bookings")
async def post_booking(request: Request):
    data, raw = await read_json(request)
    key = request.headers.get("idempotency-key")
    hashed = idempotency.body_hash(data, raw)

    with pool.connection() as conn:
        if key:
            seen = idempotency.lookup(conn, key)
            if seen:
                if seen["body_hash"] != hashed:
                    return err(422, "Idempotency-Key was already used with a different request body")
                return JSONResponse(seen["response"], status_code=seen["status"])

        if not isinstance(data, dict):
            status, payload, headers = 400, {"error": "body must be a JSON object"}, None
        else:
            status, payload, headers = booking.create_booking(conn, data)

        if key and status != 429:  # a rate-limited attempt can be retried later
            idempotency.store(conn, key, hashed, status, payload)
    return JSONResponse(payload, status_code=status, headers=headers)


@router.get("/bookings")
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
            after = decode_cursor(cursor)
        except ValueError:
            return err(400, "invalid cursor")
    with pool.connection() as conn:
        page, has_more = booking.list_bookings(conn, email, after, n)
    next_cursor = encode_cursor(page[-1]["seq"]) if has_more else None
    return {"bookings": [booking.booking_json(r) for r in page], "next_cursor": next_cursor}


@router.get("/bookings/{booking_id}")
def get_booking(booking_id: str):
    with pool.connection() as conn:
        found = booking.find_booking(conn, booking_id)
    return found if found else err(404, "booking not found")


@router.delete("/bookings/{booking_id}")
def cancel_booking(booking_id: str):
    with pool.connection() as conn:
        status, payload = booking.cancel_booking(conn, booking_id)
    return JSONResponse(payload, status_code=status)
