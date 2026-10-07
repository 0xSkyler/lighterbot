"""Reading and updating the bot's environment file from the control panel.

Updates keep the file's comments and ordering, replace values in place and are
written atomically. Values are restricted to a conservative character set so the
file means the same thing to this application and to systemd's EnvironmentFile.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path

from ..config import parse_env_file

_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_.:/+\-]*$")
_KEY = re.compile(r"^[A-Z][A-Z0-9_]*$")


def read_env(path: Path) -> dict[str, str]:
    """Current values of the environment file ({} if it does not exist yet)."""
    return parse_env_file(path) if path.is_file() else {}


def is_safe_value(value: str) -> bool:
    return bool(_SAFE_VALUE.match(value))


def _line_key(raw: str) -> str | None:
    line = raw.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):
        line = line[7:].lstrip()
    key, sep, _ = line.partition("=")
    key = key.strip()
    return key if sep and _KEY.match(key) else None


def update_env_file(path: Path, changes: Mapping[str, str]) -> None:
    """Set ``changes`` in the environment file, preserving everything else."""
    for key, value in changes.items():
        if not _KEY.match(key):
            raise ValueError(f"invalid setting name: {key!r}")
        if not is_safe_value(value):
            raise ValueError(f"{key}: the value contains characters that are not allowed")
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    written: set[str] = set()
    out: list[str] = []
    for raw in lines:
        name = _line_key(raw)
        if name is not None and name in changes:
            if name not in written:  # a repeated definition would silently override: keep one
                out.append(f"{name}={changes[name]}")
                written.add(name)
            continue
        out.append(raw)
    missing = [key for key in changes if key not in written]
    if missing:
        if out and out[-1].strip():
            out.append("")
        out.append("# Added by the control panel")
        out.extend(f"{key}={changes[key]}" for key in missing)
    _write(path, "\n".join(out) + "\n")


def _write(path: Path, text: str) -> None:
    mode = (path.stat().st_mode & 0o777) if path.is_file() else 0o600
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except PermissionError:
        # The directory is not writable but the file may be: fall back to writing in place.
        tmp.unlink(missing_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
