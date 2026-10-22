"""Level 3: booking and cancelling, with the database deciding the winner."""
import uuid

from app.config import EMAIL_RE
from app.db import rows
from app.services.rate_limit import check_rate_limit
from app.timeutil import iso_ms

BOOKING_COLS = "id, slot_id, name, email, status, created_at"


def booking_json(row):
    return {
        "id": row["id"],
        "slot_id": row["slot_id"],
        "name": row["name"],
        "email": row["email"],
        "status": row["status"],
        "created_at": iso_ms(row["created_at"]),
    }


def create_booking(conn, data):
    """Returns (status, payload, headers). Never raises for expected failures so
    the surrounding transaction (and the rate-limit attempt) is committed."""
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

    # One statement: the slot must exist and not have started, and the partial
    # unique index lets exactly one concurrent insert through.
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


def find_booking(conn, booking_id):
    found = rows(conn, f"SELECT {BOOKING_COLS} FROM bookings WHERE id = %s", (booking_id,))
    return booking_json(found[0]) if found else None


def cancel_booking(conn, booking_id):
    """Returns (status, payload). Cancelling leaves the unique index, freeing the slot."""
    updated = rows(
        conn,
        f"UPDATE bookings SET status = 'cancelled' WHERE id = %s AND status = 'confirmed' RETURNING {BOOKING_COLS}",
        (booking_id,),
    )
    if updated:
        return 200, booking_json(updated[0])
    exists = conn.execute("SELECT 1 FROM bookings WHERE id = %s", (booking_id,)).fetchone()
    if exists:
        return 409, {"error": "booking is already cancelled"}
    return 404, {"error": "booking not found"}


def list_bookings(conn, email, after, n):
    """One page of an email's bookings, oldest first. Returns (rows, has_more)."""
    found = rows(
        conn,
        f"SELECT seq, {BOOKING_COLS} FROM bookings WHERE email = %s AND seq > %s ORDER BY seq LIMIT %s",
        (email.strip().lower(), after, n + 1),
    )
    return found[:n], len(found) > n
