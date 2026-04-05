from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any


class ShareTokenError(ValueError):
    pass


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(encoded: str) -> bytes:
    padding = "=" * ((4 - (len(encoded) % 4)) % 4)
    return base64.urlsafe_b64decode((encoded + padding).encode("ascii"))


def token_fingerprint(token: str) -> str:
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return digest[:12]


def create_signed_token(payload: dict[str, Any], secret: str, ttl_seconds: int = 7 * 24 * 60 * 60) -> str:
    if not secret:
        raise ShareTokenError("Missing secret")

    now = int(time.time())
    envelope = dict(payload)
    envelope.setdefault("v", 1)
    envelope.setdefault("iat", now)
    envelope.setdefault("exp", now + int(ttl_seconds))

    body = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    body_b64 = _b64url_encode(body)

    sig = hmac.new(secret.encode("utf-8"), body_b64.encode("ascii"), hashlib.sha256).digest()
    sig_b64 = _b64url_encode(sig)

    return f"{body_b64}.{sig_b64}"


def verify_signed_token(token: str, secret: str, now: int | None = None) -> dict[str, Any]:
    if not secret:
        raise ShareTokenError("Missing secret")

    parts = token.split(".")
    if len(parts) != 2:
        raise ShareTokenError("Invalid token format")

    body_b64, sig_b64 = parts
    expected_sig = hmac.new(secret.encode("utf-8"), body_b64.encode("ascii"), hashlib.sha256).digest()

    try:
        provided_sig = _b64url_decode(sig_b64)
    except Exception as exc:  # noqa: BLE001
        raise ShareTokenError("Invalid signature encoding") from exc

    if not hmac.compare_digest(expected_sig, provided_sig):
        raise ShareTokenError("Invalid token signature")

    try:
        payload = json.loads(_b64url_decode(body_b64).decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise ShareTokenError("Invalid token payload") from exc

    current = int(time.time()) if now is None else int(now)
    exp = payload.get("exp")
    if isinstance(exp, int) and current > exp:
        raise ShareTokenError("Token expired")

    return payload

