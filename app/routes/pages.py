import json

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from app.config import API_KEY, INDEX_HTML

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML.replace("__API_KEY__", json.dumps(API_KEY)[1:-1])


@router.get("/health")
def health():
    return {"ok": True}
