from fastapi import APIRouter

from app.db import pool, rows
from app.timeutil import iso

router = APIRouter()


@router.get("/slots")
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
