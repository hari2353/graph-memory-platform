"""Webhook payload signing (pure; SPEC §9 webhook channel).

Stripe-style scheme: body bytes are signed together with the epoch timestamp
so every delivery carries its own fresh signature, and receivers can verify
both authenticity and (bounded) freshness:

    X-GMP-Timestamp: 1782000000
    X-GMP-Signature: sha256=<hex of HMAC-SHA256(secret, b"<ts>.<body>")>
"""

from __future__ import annotations

import hashlib
import hmac

SIGNATURE_HEADER = "X-GMP-Signature"
TIMESTAMP_HEADER = "X-GMP-Timestamp"
_DEFAULT_AGE_TOLERANCE_S = 300


def signature(secret: str, body: bytes, timestamp: str) -> str:
    """``sha256=<hex>`` over ``b"<timestamp>.<body>"`` with the endpoint secret."""
    mac = hmac.new(
        secret.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={mac}"


def verify(
    secret: str,
    body: bytes,
    *,
    provided_signature: str,
    provided_timestamp: str,
    age_tolerance_s: int = _DEFAULT_AGE_TOLERANCE_S,
    now_epoch_s: int | None = None,
) -> bool:
    """Receiver-side check: constant-time match + freshness window."""
    if now_epoch_s is not None:
        try:
            age = now_epoch_s - int(provided_timestamp)
        except ValueError:
            return False
        if abs(age) > age_tolerance_s:
            return False
    expected = signature(secret, body, provided_timestamp)
    return hmac.compare_digest(provided_signature.encode("utf-8"), expected.encode("utf-8"))
