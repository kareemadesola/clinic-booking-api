# Clinic booking API

FastAPI + Postgres implementation of the Koya assessment spec (levels 1-5), with a small web page at `/`.

## Design
- **No double booking:** a partial unique index (`UNIQUE (slot_id) WHERE status = 'confirmed'`) plus `INSERT ... ON CONFLICT DO NOTHING`. The database decides the winner, so it holds across workers and instances.
- **Idempotency:** the key, a hash of the body and the stored response live in Postgres. A per-key advisory lock makes concurrent retries wait and then replay the stored response (same status and booking id). A different body returns 422.
- **Rate limit:** attempts per email are stored in Postgres and counted in a sliding 60 second window under a per-email advisory lock. Failed attempts count, replays of a stored idempotent response do not.
- **Pagination:** cursor is the opaque (base64) booking sequence number, so pages stay stable when bookings are added or cancelled.

## Run locally
    export DATABASE_URL=postgresql://user:pw@localhost:5432/clinic API_KEY=choose-a-key
    pip install -r requirements.txt
    uvicorn app:app --port 8000
    BASE_URL=http://127.0.0.1:8000 pytest -q test_api.py

## Deploy
Neon (Postgres) for `DATABASE_URL`, Render (see `render.yaml`) for the web service. Set `API_KEY` and `DATABASE_URL` in Render.
