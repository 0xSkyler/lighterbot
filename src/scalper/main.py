"""Command line entry point.

    lighter-scalper run       start the live service (used by systemd)
    lighter-scalper flatten   emergency: cancel BTC orders and close the BTC position
    lighter-scalper status    show the service snapshot and the exchange's view
    lighter-scalper check     validate configuration and connectivity; sends no orders
    lighter-scalper ui        serve the local web control panel (monitor, start/stop, pause, flatten)

There is no paper, shadow or testnet mode. ``run`` and ``flatten`` act on the
live mainnet account named in the environment.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

from .config import Config, load_config
from .engine import Engine, describe, open_session
from .errors import EXIT_FATAL, EXIT_OK, ConfigError, ScalperError
from .execution import Executor
from .logging_setup import log, setup_logging
from .persistence import Journal
from .position import ClientOrderIds
from .risk import Limits
from .status import render_status

SERVICE_ALIVE_S = 5.0


def _env_file(args: argparse.Namespace) -> Path | None:
    explicit = args.env_file or os.environ.get("SCALPER_ENV_FILE")
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise ConfigError(f"environment file not found: {path}")
        return path
    default = Path(".env")
    return default if default.is_file() else None


def _load(args: argparse.Namespace) -> Config:
    return load_config(env_file=_env_file(args))


def _service_alive(cfg: Config) -> bool:
    """True if a running service updated its status file within the last few seconds."""
    try:
        return time.time() - (cfg.data_dir / "status.json").stat().st_mtime < SERVICE_ALIVE_S
    except OSError:
        return False


# ------------------------------------------------------------------- commands


def cmd_run(args: argparse.Namespace) -> int:
    cfg = _load(args)
    listener = setup_logging(cfg.log_dir if cfg.log_to_file else None, cfg.log_level, [cfg.api_private_key])
    try:
        return asyncio.run(Engine(cfg).run())
    finally:
        listener.stop()


async def _flatten(cfg: Config, force: bool) -> int:
    if _service_alive(cfg) and not force:
        print(
            "The lighter-scalper service appears to be running. Stop it first:\n"
            "    sudo systemctl stop lighter-scalper\n"
            "(stopping the service already flattens by default). Use --force to flatten anyway.",
            file=sys.stderr,
        )
        return 2
    session = await open_session(cfg)
    try:
        executor = Executor(cfg, session.meta, session.client, session.metrics, ClientOrderIds())
        result = await executor.flatten_position("MANUAL_FLATTEN", get_bbo=lambda: None, cancel_all=True)
    finally:
        await session.client.close()
    meta = session.meta
    print(f"position before : {meta.fmt_size(result.position_before)} {meta.symbol}")
    print(f"orders sent     : {len(result.orders)}")
    print(f"attempts        : {result.attempts}")
    print(f"result          : {'FLAT (confirmed by the exchange)' if result.flat else 'NOT FLAT'}")
    if not result.flat:
        print(f"last error      : {result.last_error}", file=sys.stderr)
    return EXIT_OK if result.flat else 1


def cmd_flatten(args: argparse.Namespace) -> int:
    cfg = _load(args)
    listener = setup_logging(None, cfg.log_level, [cfg.api_private_key])
    try:
        return asyncio.run(_flatten(cfg, args.force))
    finally:
        listener.stop()


async def _exchange_view(cfg: Config) -> list[str]:
    session = await open_session(cfg)
    try:
        meta = session.meta
        snapshot = await session.client.fetch_account(meta)
        orders = await session.client.fetch_active_orders(meta)
    finally:
        await session.client.close()
    position = snapshot.position
    side = "flat" if not position.signed_size else ("LONG" if position.signed_size > 0 else "SHORT")
    return [
        f"ACCOUNT            : index {cfg.account_index}, tier {session.tier.name}",
        f"AVAILABLE BALANCE  : {snapshot.available_balance} USD (collateral {snapshot.collateral})",
        f"EXCHANGE POSITION  : {side} {meta.fmt_size(abs(position.signed_size))} {meta.symbol}"
        + (f" @ {meta.fmt_price(position.avg_entry_price)}" if position.signed_size else ""),
        f"ACTIVE BTC ORDERS  : {len(orders)}",
        "EXCHANGE LEVERAGE  : "
        + (f"{10_000 / position.imf:.2f}x" if position.imf else "not set")
        + (" isolated" if position.margin_mode == 1 else " cross" if position.margin_mode == 0 else ""),
    ]


def cmd_status(args: argparse.Namespace) -> int:
    try:
        cfg: Config | None = _load(args)
    except ConfigError as exc:
        cfg = None
        print(f"configuration problem (showing the local snapshot only):\n{exc}\n", file=sys.stderr)
    data_dir = cfg.data_dir if cfg is not None else Path(os.environ.get("DATA_DIR", "data"))

    def local() -> str:
        snapshot = Journal.read_status(data_dir)
        if snapshot is None:
            return "no status snapshot found: the service has not run from this directory"
        age = time.time() - (data_dir / "status.json").stat().st_mtime
        header = (
            "SERVICE            : running"
            if age < SERVICE_ALIVE_S
            else f"SERVICE            : NOT RUNNING (snapshot is {age:.0f}s old)"
        )
        return header + "\n" + render_status(snapshot)

    if args.watch:
        try:
            while True:
                sys.stdout.write("\x1b[2J\x1b[H" + local() + "\n")
                sys.stdout.flush()
                time.sleep(1.0)
        except KeyboardInterrupt:
            return EXIT_OK
    print(local())
    if cfg is not None and not args.local:
        listener = setup_logging(None, "WARNING", [cfg.api_private_key])
        try:
            print("\n--- exchange (authoritative) ---")
            print("\n".join(asyncio.run(_exchange_view(cfg))))
        finally:
            listener.stop()
    return EXIT_OK


async def _check(cfg: Config) -> int:
    session = await open_session(cfg)
    try:
        meta = session.meta
        limits = Limits.from_config(cfg, meta)
        snapshot = await session.client.fetch_account(meta)
        orders = await session.client.fetch_active_orders(meta)
        bid, ask = await session.client.fetch_rest_bbo(meta)
    finally:
        await session.client.close()
    for line in describe(cfg, session, limits):
        print(line)
    size = meta.size_for_notional(limits.notional_q, ask)
    print(f"BTC BID / ASK: {meta.fmt_price(bid)} / {meta.fmt_price(ask)}")
    print(f"ORDER SIZE AT CURRENT PRICE: {meta.fmt_size(size)} BTC (minimum {meta.fmt_size(meta.min_size_at(ask))})")
    print(f"AVAILABLE BALANCE: {snapshot.available_balance} USD")
    print(f"EXISTING BTC POSITION: {meta.fmt_size(snapshot.position.signed_size)}")
    print(f"ACTIVE BTC ORDERS: {len(orders)}")
    problems = []
    if meta.status != "active":
        problems.append(f"market status is {meta.status}")
    if size < meta.min_size_at(ask):
        problems.append("position size is below the exchange minimum")
    required = cfg.notional_usd / cfg.leverage
    if snapshot.available_balance < required:
        problems.append(f"available balance is below the margin needed per trade ({required:.2f} USD)")
    for problem in problems:
        print(f"PROBLEM: {problem}")
    print("CHECK: " + ("FAILED" if problems else "OK - no orders were sent"))
    return 1 if problems else EXIT_OK


def cmd_check(args: argparse.Namespace) -> int:
    cfg = _load(args)
    listener = setup_logging(None, cfg.log_level, [cfg.api_private_key])
    try:
        return asyncio.run(_check(cfg))
    finally:
        listener.stop()


def cmd_ui(args: argparse.Namespace) -> int:
    """Serve the control panel. It must work before the trading configuration is complete."""
    from .ui.server import config_from_env, run_ui

    env_file = Path(args.env_file) if args.env_file else None
    return run_ui(config_from_env(args.host, args.port, env_file), open_browser=args.open)


# ----------------------------------------------------------------------- main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lighter-scalper", description="Live BTC perpetual scalper for Lighter mainnet."
    )
    parser.add_argument("--env-file", help="path to the environment file (default: ./.env if present)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="start the live trading service").set_defaults(func=cmd_run)
    flatten = sub.add_parser("flatten", help="emergency: cancel BTC orders and close the BTC position")
    flatten.add_argument("--force", action="store_true", help="flatten even if the service appears to be running")
    flatten.set_defaults(func=cmd_flatten)
    status = sub.add_parser("status", help="show service and exchange status")
    status.add_argument("--watch", action="store_true", help="refresh the local snapshot every second")
    status.add_argument("--local", action="store_true", help="do not query the exchange")
    status.set_defaults(func=cmd_status)
    sub.add_parser("check", help="validate configuration and connectivity without sending orders").set_defaults(
        func=cmd_check
    )
    ui = sub.add_parser("ui", help="serve the local web control panel")
    ui.add_argument("--host", help="address to listen on (default 127.0.0.1)")
    ui.add_argument("--port", type=int, help="port to listen on (default 8787)")
    ui.add_argument("--open", action="store_true", help="open the panel in the default browser")
    ui.set_defaults(func=cmd_ui)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(f"CONFIG_ERROR: {exc}", file=sys.stderr)
        return EXIT_FATAL
    except ScalperError as exc:
        log.error("ERROR class=%s error=%s", exc.error_class.value, exc)
        print(f"{exc.error_class.value}: {exc}", file=sys.stderr)
        return EXIT_FATAL if exc.error_class.value in ("AUTH_ERROR", "CONFIG_ERROR") else 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
