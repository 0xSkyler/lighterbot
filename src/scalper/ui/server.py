"""HTTP server of the control panel.

Security model:

* binds to 127.0.0.1 unless remote access is explicitly allowed;
* every request must carry a loopback (or explicitly allowed) ``Host`` header, which
  defeats DNS-rebinding;
* every API call needs a logged-in session; every state-changing call additionally needs
  the session's CSRF token and must not be cross-site;
* the API private key is write-only: it is never sent to the browser;
* a strict Content-Security-Policy: no inline script or style, nothing loaded from elsewhere.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import threading
import time
import webbrowser
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from aiohttp import web

from .. import __version__
from ..control import is_paused, set_paused, submit_command
from ..metrics import utc_date
from ..persistence import utc_iso
from . import data
from .auth import MIN_PASSWORD_LENGTH, SESSION_TTL_S, LoginThrottle, PasswordStore, Session, Sessions
from .envfile import is_safe_value, read_env, update_env_file
from .schema import FIELDS, SECRET_KEYS, SettingsCheck, check_settings, normalise, public_values, schema_json
from .service import (
    STATUS_FRESH_S,
    ActionResult,
    ServiceManager,
    bot_command,
    bot_environment,
    detect_manager,
)

log = logging.getLogger("scalper.ui")

COOKIE = "scalper_session"
STATIC_DIR = Path(__file__).parent / "static"
STATIC_TYPES = {".html": "text/html", ".css": "text/css", ".js": "text/javascript", ".svg": "image/svg+xml"}
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "[::1]", "::1"})
PUBLIC_API = frozenset({"/api/session", "/api/login", "/api/setup"})
FLATTEN_TIMEOUT_S = 180.0
CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; "
    "frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
)


@dataclass(frozen=True, slots=True)
class UiConfig:
    host: str
    port: int
    data_dir: Path
    log_dir: Path
    env_file: Path
    service_mode: str = "auto"
    allowed_hosts: frozenset[str] = frozenset()

    @property
    def is_loopback(self) -> bool:
        return self.host in LOOPBACK_HOSTS


FlattenRunner = Callable[[], Awaitable[ActionResult]]
PANEL_KEY: web.AppKey[Panel] = web.AppKey("panel")


class Panel:
    """Request handlers and the state they share."""

    def __init__(self, cfg: UiConfig, service: ServiceManager, flatten_runner: FlattenRunner | None = None) -> None:
        self.cfg = cfg
        self.service = service
        self.passwords = PasswordStore(cfg.data_dir)
        self.sessions = Sessions()
        self.throttle = LoginThrottle()
        self._flatten_runner = flatten_runner or self._run_flatten_cli
        self._check_cache: tuple[float, SettingsCheck] | None = None
        self._flatten_lock = asyncio.Lock()

    # ------------------------------------------------------------- helpers

    def _env(self) -> dict[str, str]:
        """The environment the bot would start with: the file plus the panel's directories."""
        env = read_env(self.cfg.env_file)
        env.setdefault("DATA_DIR", str(self.cfg.data_dir))
        env.setdefault("LOG_DIR", str(self.cfg.log_dir))
        return env

    def _check(self) -> SettingsCheck:
        try:
            stamp = self.cfg.env_file.stat().st_mtime
        except OSError:
            stamp = 0.0
        if self._check_cache is None or self._check_cache[0] != stamp:
            self._check_cache = (stamp, check_settings(self._env()))
        return self._check_cache[1]

    async def _run_flatten_cli(self) -> ActionResult:
        """Bot not running: run ``lighter-scalper flatten`` (the same production code path)."""
        try:
            process = await asyncio.create_subprocess_exec(
                *bot_command(self.cfg.env_file, "flatten"),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=bot_environment(self.cfg.data_dir, self.cfg.log_dir),
            )
            output, _ = await asyncio.wait_for(process.communicate(), FLATTEN_TIMEOUT_S)
        except (TimeoutError, OSError) as exc:
            return ActionResult(False, f"flatten could not be completed: {type(exc).__name__}")
        text = output.decode("utf-8", "replace").strip()
        tail = "\n".join(text.splitlines()[-8:])
        if process.returncode == 0:
            return ActionResult(True, "The exchange confirms the account is flat.\n" + tail)
        return ActionResult(False, "Flatten did not confirm flat. Check the account in the Lighter app.\n" + tail)

    @staticmethod
    def _json(payload: Any, status: int = 200) -> web.Response:
        return web.json_response(payload, status=status, headers={"Cache-Control": "no-store"})

    @staticmethod
    async def _body(request: web.Request) -> dict[str, Any]:
        try:
            loaded = await request.json()
        except ValueError:
            raise web.HTTPBadRequest(text="invalid JSON") from None
        if not isinstance(loaded, dict):
            raise web.HTTPBadRequest(text="expected a JSON object")
        return loaded

    def _session_response(self, session: Session) -> web.Response:
        response = self._json({"authenticated": True, "setup_required": False, "csrf": session.csrf})
        response.set_cookie(COOKIE, session.token, httponly=True, samesite="Strict", max_age=SESSION_TTL_S, path="/")
        return response

    # ---------------------------------------------------------------- auth

    async def session(self, request: web.Request) -> web.Response:
        session = self.sessions.get(request.cookies.get(COOKIE))
        return self._json(
            {
                "authenticated": session is not None,
                "setup_required": not self.passwords.is_set(),
                "csrf": session.csrf if session else None,
                "min_password_length": MIN_PASSWORD_LENGTH,
                "version": __version__,
            }
        )

    async def setup(self, request: web.Request) -> web.Response:
        """First run only: choose the panel password."""
        if self.passwords.is_set():
            return self._json({"error": "A password is already set."}, 409)
        password = str((await self._body(request)).get("password") or "")
        try:
            await asyncio.to_thread(self.passwords.set, password)
        except ValueError as exc:
            return self._json({"error": str(exc)}, 400)
        log.warning("PANEL_PASSWORD_SET")
        return self._session_response(self.sessions.create())

    async def login(self, request: web.Request) -> web.Response:
        wait = self.throttle.locked_for()
        if wait > 0:
            return self._json({"error": f"Too many attempts. Try again in {int(wait) + 1} s."}, 429)
        password = str((await self._body(request)).get("password") or "")
        if not await asyncio.to_thread(self.passwords.verify, password):
            self.throttle.failed()
            log.warning("PANEL_LOGIN_FAILED remote=%s", request.remote)
            return self._json({"error": "Wrong password."}, 401)
        self.throttle.succeeded()
        return self._session_response(self.sessions.create())

    async def logout(self, request: web.Request) -> web.Response:
        self.sessions.drop(request.cookies.get(COOKIE))
        response = self._json({"authenticated": False})
        response.del_cookie(COOKIE, path="/")
        return response

    async def change_password(self, request: web.Request) -> web.Response:
        body = await self._body(request)
        if not await asyncio.to_thread(self.passwords.verify, str(body.get("current") or "")):
            return self._json({"error": "The current password is wrong."}, 403)
        try:
            await asyncio.to_thread(self.passwords.set, str(body.get("new") or ""))
        except ValueError as exc:
            return self._json({"error": str(exc)}, 400)
        self.sessions.drop_all()  # every session, including this one, must log in again
        log.warning("PANEL_PASSWORD_CHANGED")
        return self._json({"ok": True, "message": "Password changed. Log in again."})

    # ------------------------------------------------------------ overview

    async def overview(self, request: web.Request) -> web.Response:
        service, (status, age) = await asyncio.gather(
            self.service.status(), asyncio.to_thread(data.read_status, self.cfg.data_dir)
        )
        live = service.state == "running" and age is not None and age < STATUS_FRESH_S
        check = self._check()
        return self._json(
            {
                "now": utc_iso(),
                "version": __version__,
                "service": service.to_json(),
                "bot_live": live,
                "status": status if live else None,
                "last_status": None if live else _last_known(status),
                "status_age_s": None if age is None else round(age, 1),
                "paused": await asyncio.to_thread(is_paused, self.cfg.data_dir),
                "config": {"ready": check.ready, "blockers": check.start_blockers},
            }
        )

    async def trades(self, request: web.Request) -> web.Response:
        date = request.query.get("date") or utc_date()
        limit = _clamp(request.query.get("limit"), 200, 1, 1000)
        rows = await asyncio.to_thread(data.read_trades, self.cfg.data_dir, None if date == "all" else date, limit)
        return self._json({"date": date, "trades": rows})

    async def pnl(self, request: web.Request) -> web.Response:
        date = request.query.get("date") or utc_date()
        return self._json(
            {"date": date, "points": await asyncio.to_thread(data.read_pnl_curve, self.cfg.data_dir, date)}
        )

    async def daily(self, request: web.Request) -> web.Response:
        return self._json({"days": await asyncio.to_thread(data.read_daily, self.cfg.data_dir, 30)})

    async def events(self, request: web.Request) -> web.Response:
        limit = _clamp(request.query.get("limit"), 30, 1, 200)
        return self._json({"events": await asyncio.to_thread(data.read_events, self.cfg.data_dir, limit)})

    async def logs(self, request: web.Request) -> web.Response:
        lines = _clamp(request.query.get("lines"), 300, 10, 2000)
        return self._json({"lines": await asyncio.to_thread(data.tail_log, self.cfg.log_dir, lines)})

    # ------------------------------------------------------------- control

    async def service_action(self, request: web.Request) -> web.Response:
        action = request.match_info["action"]
        if action == "start":
            check = self._check()
            if not check.ready:
                return self._json(
                    {"ok": False, "message": "The configuration is not complete.", "blockers": check.start_blockers},
                    409,
                )
            result = await self.service.start()
        elif action == "stop":
            result = await self.service.stop()
        elif action == "restart":
            result = await self.service.restart()
        else:
            raise web.HTTPNotFound()
        log.warning("PANEL_SERVICE action=%s ok=%s", action, result.ok)
        return self._json({"ok": result.ok, "message": result.message}, 200 if result.ok else 409)

    async def bot_action(self, request: web.Request) -> web.Response:
        action = request.match_info["action"]
        if action not in ("pause", "resume", "flatten"):
            raise web.HTTPNotFound()
        service = await self.service.status()
        up = service.state in ("running", "starting")
        data_dir = self.cfg.data_dir
        log.warning("PANEL_BOT action=%s service=%s", action, service.state)
        if action in ("pause", "resume"):
            # The flag is what a (re)starting bot reads; the command is what a running bot acts on.
            await asyncio.to_thread(set_paused, data_dir, action == "pause")
            if up:
                await asyncio.to_thread(submit_command, data_dir, action)
            message = (
                "New entries are paused. An open position is still managed and closed by its rules."
                if action == "pause"
                else "Entries resumed."
            )
            return self._json({"ok": True, "message": message})
        # flatten
        await asyncio.to_thread(set_paused, data_dir, True)
        if up:
            await asyncio.to_thread(submit_command, data_dir, "flatten")
            return self._json(
                {"ok": True, "message": "Flatten sent. Entries are paused; watch the position go to flat."}
            )
        if service.state == "stopping":
            return self._json({"ok": True, "message": "The bot is already stopping and flattens as configured."})
        if self._flatten_lock.locked():
            return self._json({"ok": False, "message": "A flatten is already running."}, 409)
        async with self._flatten_lock:
            result = await self._flatten_runner()
        return self._json({"ok": result.ok, "message": result.message}, 200 if result.ok else 502)

    # ------------------------------------------------------------ settings

    def _settings_payload(self, check: SettingsCheck, service_state: str) -> dict[str, Any]:
        env = read_env(self.cfg.env_file)
        return {
            "groups": schema_json(),
            "values": public_values(env),
            "check": {"value_errors": check.value_errors, "start_blockers": check.start_blockers, "ready": check.ready},
            "env_file": str(self.cfg.env_file),
            "restart_needed": service_state in ("running", "starting"),
        }

    async def get_settings(self, request: web.Request) -> web.Response:
        service = await self.service.status()
        return self._json(self._settings_payload(self._check(), service.state))

    async def save_settings(self, request: web.Request) -> web.Response:
        submitted = (await self._body(request)).get("values")
        if not isinstance(submitted, dict):
            return self._json({"error": "No values were sent."}, 400)
        changes: dict[str, str] = {}
        errors: list[str] = []
        for key, raw in submitted.items():
            field = FIELDS.get(str(key))
            if field is None:
                errors.append(f"{key}: this setting cannot be changed from the panel")
                continue
            value = normalise(field, raw)
            if key in SECRET_KEYS and not value:
                continue  # an empty secret field means "keep the stored one"
            if not is_safe_value(value):
                errors.append(f"{key}: contains characters that are not allowed")
                continue
            changes[field.key] = value
        candidate = self._env()
        candidate.update(changes)
        check = check_settings(candidate)
        errors.extend(check.value_errors)
        if errors:
            return self._json({"ok": False, "errors": errors}, 400)
        if changes:
            try:
                await asyncio.to_thread(update_env_file, self.cfg.env_file, changes)
            except (OSError, ValueError) as exc:
                return self._json({"ok": False, "errors": [f"could not write {self.cfg.env_file}: {exc}"]}, 500)
            self._check_cache = None
            names = ", ".join(sorted(changes))
            log.warning("PANEL_SETTINGS_SAVED keys=%s", names)  # names only, never values
        service = await self.service.status()
        payload = self._settings_payload(self._check(), service.state)
        payload.update({"ok": True, "saved": sorted(changes)})
        return self._json(payload)

    # -------------------------------------------------------------- static

    async def index(self, request: web.Request) -> web.Response:
        return _static("index.html")

    async def static(self, request: web.Request) -> web.Response:
        return _static(request.match_info["name"])


def _last_known(status: dict[str, Any] | None) -> dict[str, Any] | None:
    """What is still worth showing from a stale snapshot once the bot is no longer running."""
    if not status:
        return None
    return {"ts": status.get("ts"), "state": status.get("state"), "metrics": status.get("metrics")}


def _clamp(raw: str | None, default: int, low: int, high: int) -> int:
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(low, min(high, value))


def _static(name: str) -> web.Response:
    path = STATIC_DIR / name
    content_type = STATIC_TYPES.get(path.suffix)
    if content_type is None or path.parent != STATIC_DIR or not path.is_file():
        raise web.HTTPNotFound()
    return web.Response(
        body=path.read_bytes(), content_type=content_type, charset="utf-8", headers={"Cache-Control": "no-cache"}
    )


def _hostname(host_header: str) -> str:
    host = host_header.strip().lower()
    if host.startswith("["):
        return host.split("]")[0] + "]"
    return host.rsplit(":", 1)[0] if ":" in host else host


def build_app(
    cfg: UiConfig, service: ServiceManager | None = None, flatten_runner: FlattenRunner | None = None
) -> web.Application:
    """Create the aiohttp application."""
    manager = service or detect_manager(cfg.service_mode, cfg.data_dir, cfg.log_dir, cfg.env_file)
    panel = Panel(cfg, manager, flatten_runner)
    allowed_hosts = LOOPBACK_HOSTS | cfg.allowed_hosts | ({cfg.host} if not cfg.is_loopback else set())

    @web.middleware
    async def guard(
        request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:
        try:
            response = await _guarded(request, handler)
        except web.HTTPException as exc:
            _secure(exc)
            raise
        _secure(response)
        return response

    def _secure(response: web.StreamResponse) -> None:
        response.headers["Content-Security-Policy"] = CSP
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"

    async def _guarded(
        request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:
        if _hostname(request.headers.get("Host", "")) not in allowed_hosts:
            return Panel._json({"error": "This host name is not allowed to reach the control panel."}, 403)
        if request.method not in ("GET", "HEAD"):
            origin = request.headers.get("Origin")
            if origin is not None and urlsplit(origin).netloc.lower() != request.headers.get("Host", "").lower():
                return Panel._json({"error": "Cross-site request refused."}, 403)
            if request.headers.get("Sec-Fetch-Site") in ("cross-site", "same-site"):
                return Panel._json({"error": "Cross-site request refused."}, 403)
        path = request.path
        if path.startswith("/api/") and path not in PUBLIC_API:
            session = panel.sessions.get(request.cookies.get(COOKIE))
            if session is None:
                return Panel._json({"error": "Not logged in."}, 401)
            if request.method not in ("GET", "HEAD"):
                token = request.headers.get("X-CSRF-Token", "")
                if not hmac.compare_digest(token.encode(), session.csrf.encode()):
                    return Panel._json({"error": "Missing or wrong CSRF token."}, 403)
        return await handler(request)

    app = web.Application(middlewares=[guard], client_max_size=256 * 1024)
    app[PANEL_KEY] = panel
    router = app.router
    router.add_get("/", panel.index)
    router.add_get("/static/{name}", panel.static)
    router.add_get("/api/session", panel.session)
    router.add_post("/api/setup", panel.setup)
    router.add_post("/api/login", panel.login)
    router.add_post("/api/logout", panel.logout)
    router.add_post("/api/password", panel.change_password)
    router.add_get("/api/overview", panel.overview)
    router.add_get("/api/trades", panel.trades)
    router.add_get("/api/pnl", panel.pnl)
    router.add_get("/api/daily", panel.daily)
    router.add_get("/api/events", panel.events)
    router.add_get("/api/logs", panel.logs)
    router.add_post("/api/service/{action}", panel.service_action)
    router.add_post("/api/bot/{action}", panel.bot_action)
    router.add_get("/api/settings", panel.get_settings)
    router.add_post("/api/settings", panel.save_settings)
    return app


def config_from_env(host: str | None = None, port: int | None = None, env_file: Path | None = None) -> UiConfig:
    """Resolve the panel's own settings. Unlike the bot, this must work with an incomplete config."""
    env_path = env_file or Path(os.environ.get("SCALPER_ENV_FILE") or ".env")
    file_values = read_env(env_path)

    def setting(name: str, default: str) -> str:
        return (os.environ.get(name) or file_values.get(name) or default).strip()

    bind = host or setting("UI_HOST", "127.0.0.1")
    if bind not in LOOPBACK_HOSTS and setting("UI_ALLOW_REMOTE", "false").lower() != "true":
        raise SystemExit(
            f"refusing to listen on {bind}: the control panel can start live trading and is served over plain "
            "HTTP. Keep it on 127.0.0.1 and reach it through RustDesk or an SSH tunnel, or set "
            "UI_ALLOW_REMOTE=true if you have put it behind your own encrypted, access-controlled network."
        )
    extra = frozenset(h.strip().lower() for h in setting("UI_ALLOWED_HOSTS", "").split(",") if h.strip())
    return UiConfig(
        host=bind,
        port=port or int(setting("UI_PORT", "8787")),
        data_dir=Path(setting("DATA_DIR", "data")),
        log_dir=Path(setting("LOG_DIR", "logs")),
        env_file=env_path,
        service_mode=setting("UI_SERVICE_MODE", "auto").lower(),
        allowed_hosts=extra,
    )


def run_ui(cfg: UiConfig, *, open_browser: bool = False) -> int:
    """Serve the control panel until interrupted."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    app = build_app(cfg)
    url = f"http://{'127.0.0.1' if cfg.host in ('0.0.0.0', '::') else cfg.host}:{cfg.port}"
    panel = app[PANEL_KEY]
    log.info("CONTROL PANEL %s (service control: %s, data: %s)", url, panel.service.mode, cfg.data_dir)
    if not panel.passwords.is_set():
        log.info("First run: open the panel and choose a password.")
    if open_browser:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    started = time.monotonic()
    web.run_app(app, host=cfg.host, port=cfg.port, print=None, access_log=None)
    log.info("CONTROL PANEL stopped after %.0f s", time.monotonic() - started)
    return 0
