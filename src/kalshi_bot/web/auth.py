from __future__ import annotations

import hashlib
import hmac
import secrets

from kalshi_bot.config import Settings

_PASSWORD_HASH_ITERATIONS = 600_000


def normalize_auth_email(email: str) -> str:
    return email.strip().lower()


def hash_password(password: str, *, salt_hex: str | None = None) -> tuple[str, str]:
    salt = bytes.fromhex(salt_hex) if salt_hex is not None else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _PASSWORD_HASH_ITERATIONS,
    )
    return digest.hex(), salt.hex()


def verify_password(password: str, *, expected_hash: str, salt_hex: str) -> bool:
    candidate_hash, _ = hash_password(password, salt_hex=salt_hex)
    return hmac.compare_digest(candidate_hash, expected_hash)


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def is_registration_email_allowed(settings: Settings, email: str) -> bool:
    return normalize_auth_email(email) in settings.web_auth_allowed_registration_email_set
