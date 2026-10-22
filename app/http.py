"""Small helpers shared by routes."""
import json

from fastapi import Request
from fastapi.responses import JSONResponse


def err(status, message, headers=None):
    return JSONResponse({"error": message}, status_code=status, headers=headers)


async def read_json(request: Request):
    raw = await request.body()
    try:
        return json.loads(raw), raw
    except (ValueError, UnicodeDecodeError):
        return None, raw
