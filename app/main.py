"""Clinic booking API (Koya Cohort 4 assessment, task 1).

FastAPI + Postgres. Correctness under concurrency comes from the database:
  - a partial unique index allows only one confirmed booking per slot
  - advisory transaction locks serialise the rate limit per email and
    idempotency handling per key
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.auth import require_key
from app.db import init_schema, pool
from app.http import err
from app.routes import admin, bookings, pages, slots


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True)
    init_schema()
    yield
    pool.close()


async def http_error(_, exc: StarletteHTTPException):
    return err(exc.status_code, "not found" if exc.status_code == 404 else str(exc.detail))


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
app.middleware("http")(require_key)
app.add_exception_handler(StarletteHTTPException, http_error)
for module in (pages, admin, slots, bookings):
    app.include_router(module.router)
