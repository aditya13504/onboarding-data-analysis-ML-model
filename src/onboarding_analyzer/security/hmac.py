from __future__ import annotations
import hmac
import hashlib
import time
from fastapi import HTTPException


def verify_hmac(signature: str, body: bytes, secret: str, tolerance_seconds: int = 300):
    try:
        ts_str, sig = signature.split(",", 1)
        ts = int(ts_str)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid signature header")
    if abs(time.time() - ts) > tolerance_seconds:
        raise HTTPException(status_code=401, detail="signature timestamp expired")
    expected = hmac.new(secret.encode(), msg=f"{ts}.".encode() + body, digestmod=hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=401, detail="invalid signature")
