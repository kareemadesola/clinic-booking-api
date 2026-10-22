"""Opaque cursor: the base64 of a booking's sequence number."""
import base64
import binascii


def encode_cursor(seq):
    return base64.urlsafe_b64encode(str(seq).encode()).decode()


def decode_cursor(cursor):
    """Return the sequence number, or raise ValueError for a bad cursor."""
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("invalid cursor") from exc
