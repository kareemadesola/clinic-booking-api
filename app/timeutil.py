"""Timestamp parsing and formatting."""
from datetime import datetime, timezone


def iso(dt):
    dt = dt.astimezone(timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if dt.microsecond else "%Y-%m-%dT%H:%M:%SZ"
    return dt.strftime(fmt)


def iso_ms(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_dt(value):
    if not isinstance(value, str):
        raise ValueError("not a string")
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
