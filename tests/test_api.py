"""Black-box tests against a running server: BASE_URL and API_KEY env vars."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import pytest

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
KEY = os.environ["API_KEY"]
H = {"X-API-Key": KEY}


def ts(delta):
    return (datetime.now(timezone.utc) + delta).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture(autouse=True)
def fresh():
    slots = [
        {"id": "s1", "provider": "Dr Ade", "start": ts(timedelta(days=2)), "end": ts(timedelta(days=2, minutes=30))},
        {"id": "s2", "provider": "Dr Ade", "start": ts(timedelta(days=1)), "end": ts(timedelta(days=1, minutes=30))},
        {"id": "s3", "provider": "Dr Bo", "start": ts(timedelta(days=3)), "end": ts(timedelta(days=3, minutes=30))},
        {"id": "past", "provider": "Dr Bo", "start": ts(-timedelta(hours=2)), "end": ts(-timedelta(hours=1))},
    ] + [
        {"id": f"x{i}", "provider": "Dr Cy", "start": ts(timedelta(days=5, hours=i)), "end": ts(timedelta(days=5, hours=i, minutes=30))}
        for i in range(30)
    ]
    r = httpx.post(f"{BASE}/reset", json={"slots": slots}, headers=H)
    assert r.status_code == 200


def book(slot="s1", email=None, name="Ada Obi", **kw):
    email = email or f"{uuid.uuid4().hex[:8]}@example.com"
    return httpx.post(f"{BASE}/bookings", json={"slot_id": slot, "name": name, "email": email}, headers=H | kw.pop("headers", {}))


def test_auth():
    assert httpx.get(f"{BASE}/slots").status_code == 401
    assert httpx.get(f"{BASE}/slots", headers={"X-API-Key": "nope"}).status_code == 401
    assert httpx.get(f"{BASE}/slots", headers=H).status_code == 200
    assert "error" in httpx.get(f"{BASE}/slots").json()


def test_slots_sorted_filtered_availability():
    slots = httpx.get(f"{BASE}/slots", headers=H).json()["slots"]
    starts = [s["start"] for s in slots]
    assert starts == sorted(starts)
    assert {s["id"] for s in httpx.get(f"{BASE}/slots?provider=Dr Bo", headers=H).json()["slots"]} == {"s3", "past"}
    book("s1")
    by_id = {s["id"]: s for s in httpx.get(f"{BASE}/slots", headers=H).json()["slots"]}
    assert by_id["s1"]["available"] is False and by_id["s2"]["available"] is True


def test_book_get_and_normalisation():
    r = book("s1", email="  Ada@Example.COM ", name="  Ada Obi  ")
    assert r.status_code == 201
    b = r.json()
    assert b["status"] == "confirmed" and b["email"] == "ada@example.com" and b["name"] == "Ada Obi"
    assert httpx.get(f"{BASE}/bookings/{b['id']}", headers=H).json() == b
    assert httpx.get(f"{BASE}/bookings/nope", headers=H).status_code == 404


@pytest.mark.parametrize("body", [
    {}, {"slot_id": "s1", "name": "A"}, {"slot_id": "s1", "email": "a@b.co"}, {"name": "A", "email": "a@b.co"},
    {"slot_id": "s1", "name": "  ", "email": "a@b.co"}, {"slot_id": "s1", "name": "A", "email": "not-an-email"},
    {"slot_id": "", "name": "A", "email": "a@b.co"}, {"slot_id": "s1", "name": 5, "email": "a@b.co"},
])
def test_validation(body):
    assert httpx.post(f"{BASE}/bookings", json=body, headers=H).status_code == 400


def test_missing_and_past_slot():
    assert book("nope").status_code == 404
    assert book("past").status_code == 400


def test_double_booking_and_cancel():
    first = book("s1")
    assert first.status_code == 201
    assert book("s1").status_code == 409
    c = httpx.delete(f"{BASE}/bookings/{first.json()['id']}", headers=H)
    assert c.status_code == 200 and c.json()["status"] == "cancelled"
    assert httpx.delete(f"{BASE}/bookings/{first.json()['id']}", headers=H).status_code == 409
    assert httpx.delete(f"{BASE}/bookings/nope", headers=H).status_code == 404
    assert book("s1").status_code == 201  # slot is free again


def test_concurrent_booking_one_winner():
    def go(_):
        return book("s2").status_code
    with ThreadPoolExecutor(40) as ex:
        codes = list(ex.map(go, range(40)))
    assert codes.count(201) == 1 and codes.count(409) == 39


def test_idempotency():
    k = {"Idempotency-Key": uuid.uuid4().hex}
    body = {"slot_id": "s1", "name": "Ada", "email": "ada@example.com"}
    a = httpx.post(f"{BASE}/bookings", json=body, headers=H | k)
    b = httpx.post(f"{BASE}/bookings", json=body, headers=H | k)
    assert a.status_code == b.status_code == 201 and a.json()["id"] == b.json()["id"]
    c = httpx.post(f"{BASE}/bookings", json=body | {"name": "Other"}, headers=H | k)
    assert c.status_code == 422
    assert len(httpx.get(f"{BASE}/bookings?email=ada@example.com", headers=H).json()["bookings"]) == 1


def test_concurrent_same_idempotency_key():
    k = {"Idempotency-Key": uuid.uuid4().hex}
    body = {"slot_id": "s3", "name": "Ada", "email": "same@example.com"}
    with ThreadPoolExecutor(5) as ex:
        res = list(ex.map(lambda _: httpx.post(f"{BASE}/bookings", json=body, headers=H | k), range(5)))
    assert {r.status_code for r in res} == {201} and len({r.json()["id"] for r in res}) == 1


def test_pagination():
    email = "page@example.com"
    ids = [book(f"x{i}", email=email).json()["id"] for i in range(4)]
    seen, cursor = [], None
    while True:
        url = f"{BASE}/bookings?email=PAGE@example.com&limit=3" + (f"&cursor={cursor}" if cursor else "")
        d = httpx.get(url, headers=H).json()
        seen += [b["id"] for b in d["bookings"]]
        cursor = d["next_cursor"]
        if not cursor:
            break
    assert seen == ids
    for bad in ("0", "51", "abc"):
        assert httpx.get(f"{BASE}/bookings?email={email}&limit={bad}", headers=H).status_code == 400
    assert httpx.get(f"{BASE}/bookings?email=nobody@example.com", headers=H).json() == {"bookings": [], "next_cursor": None}


def test_rate_limit():
    email = "spam@example.com"
    codes = [book("nope", email=email).status_code for _ in range(5)]  # failed attempts count
    assert codes == [404] * 5
    r = book("s1", email=email)
    assert r.status_code == 429 and int(r.headers["Retry-After"]) in range(1, 61)
    assert book("s1", email="other@example.com").status_code == 201


def test_slot_times_echoed_as_sent():
    slots = [
        {"id": "m1", "provider": "Dr Ade", "start": "2031-01-01T09:00:00.000Z", "end": "2031-01-01T09:30:00.000Z"},
        {"id": "m2", "provider": "Dr Ade", "start": "2031-01-01T10:00:00+01:00", "end": "2031-01-01T10:30:00+01:00"},
        {"id": "m3", "provider": "Dr Ade", "start": "2031-01-01T11:00:00Z", "end": "2031-01-01T11:30:00Z"},
    ]
    assert httpx.post(f"{BASE}/reset", json={"slots": slots}, headers=H).status_code == 200
    got = {s["id"]: s for s in httpx.get(f"{BASE}/slots", headers=H).json()["slots"]}
    for s in slots:
        assert got[s["id"]]["start"] == s["start"] and got[s["id"]]["end"] == s["end"]
    # still sorted by the actual instant: m2 (09:00Z) ties m1, m3 is last
    assert [s["id"] for s in httpx.get(f"{BASE}/slots", headers=H).json()["slots"]][-1] == "m3"
