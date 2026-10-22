"""Settings read from the environment."""
import os
import re
from pathlib import Path

DATABASE_URL = os.environ["DATABASE_URL"]
API_KEY = os.environ["API_KEY"]
DB_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "20"))

RATE_LIMIT = 5
RATE_WINDOW_SECONDS = 60
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

INDEX_HTML = (Path(__file__).parent.parent / "static" / "index.html").read_text(encoding="utf-8")
