"""X-API-Key check, applied to every route except the web page."""
import hmac

from fastapi import Request

from app.config import API_KEY
from app.http import err


async def require_key(request: Request, call_next):
    if request.url.path == "/":  # the web page itself; its API calls carry the key
        return await call_next(request)
    given = request.headers.get("x-api-key", "")
    if not hmac.compare_digest(given.encode(), API_KEY.encode()):
        return err(401, "missing or invalid API key")
    return await call_next(request)
