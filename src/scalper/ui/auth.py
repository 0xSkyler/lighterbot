"""Panel authentication: one operator password, in-memory sessions, login throttling.

The password is stored only as a salted scrypt hash in ``DATA_DIR/ui_auth.json``
(owner-readable). Deleting that file resets the panel to first-run setup.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path

AUTH_FILE = "ui_auth.json"
MIN_PASSWORD_LENGTH = 8
SESSION_TTL_S = 7 * 24 * 3600
MAX_FAILURES = 5
LOCKOUT_S = 30.0
_SCRYPT = {"n": 2**14, "r": 8, "p": 1, "dklen": 32}


def _hash(password: str, salt: bytes) -> bytes:
    return hashlib.scrypt(password.encode("utf-8"), salt=salt, **_SCRYPT)


class PasswordStore:
    """Salted scrypt hash of the single operator password."""

    def __init__(self, data_dir: Path) -> None:
        self._path = data_dir / AUTH_FILE

    def is_set(self) -> bool:
        return self._path.is_file()

    def set(self, password: str) -> None:
        if len(password) < MIN_PASSWORD_LENGTH:
            raise ValueError(f"the password must be at least {MIN_PASSWORD_LENGTH} characters")
        salt = os.urandom(16)
        record = {
            "salt": base64.b64encode(salt).decode("ascii"),
            "hash": base64.b64encode(_hash(password, salt)).decode("ascii"),
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(record), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self._path)

    def verify(self, password: str) -> bool:
        try:
            record = json.loads(self._path.read_text(encoding="utf-8"))
            salt = base64.b64decode(record["salt"])
            expected = base64.b64decode(record["hash"])
        except (OSError, ValueError, KeyError, TypeError):
            return False
        return hmac.compare_digest(_hash(password, salt), expected)


@dataclass(slots=True)
class Session:
    token: str
    csrf: str
    expires: float


class Sessions:
    """Server-side sessions. Lost on restart by design: nothing sensitive is persisted."""

    def __init__(self, ttl_s: float = SESSION_TTL_S) -> None:
        self._ttl = ttl_s
        self._sessions: dict[str, Session] = {}

    def create(self) -> Session:
        session = Session(secrets.token_urlsafe(32), secrets.token_urlsafe(24), time.time() + self._ttl)
        self._sessions[session.token] = session
        return session

    def get(self, token: str | None) -> Session | None:
        if not token:
            return None
        session = self._sessions.get(token)
        if session is None:
            return None
        if session.expires < time.time():
            del self._sessions[token]
            return None
        return session

    def drop(self, token: str | None) -> None:
        if token:
            self._sessions.pop(token, None)

    def drop_all(self) -> None:
        self._sessions.clear()


class LoginThrottle:
    """Locks further attempts for a while after repeated wrong passwords."""

    def __init__(self, max_failures: int = MAX_FAILURES, lockout_s: float = LOCKOUT_S) -> None:
        self._max = max_failures
        self._lockout = lockout_s
        self._failures = 0
        self._locked_until = 0.0

    def locked_for(self) -> float:
        """Seconds until another attempt is allowed (0 if allowed now)."""
        return max(0.0, self._locked_until - time.monotonic())

    def failed(self) -> None:
        self._failures += 1
        if self._failures >= self._max:
            self._failures = 0
            self._locked_until = time.monotonic() + self._lockout

    def succeeded(self) -> None:
        self._failures = 0
        self._locked_until = 0.0
