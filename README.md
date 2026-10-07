# Lighter BTC Scalper

A live-only, event-driven scalping bot for the **BTC perpetual on Lighter mainnet**.

> **This software places real leveraged orders with real funds.** There is no paper mode,
> no testnet mode, no shadow mode and no backtester. Leveraged trading can lose the whole
> margin of a position very quickly. Run it only on a dedicated Lighter sub-account funded
> with money you can afford to lose, and supervise the first sessions.

## What it does

```
microstructure indicates immediate direction
        -> open a leveraged BTC position (IOC, slippage-capped)
        -> watch the executable exit price on every book event
        -> the WHOLE position can be closed for positive net realized P&L ("GREEN")
        -> exit immediately (reduce-only IOC)
        -> exchange confirms flat
        -> look for the next opportunity
```

- One market (BTC perp), one position at a time, long and short symmetric.
- No candles, no fixed take-profit, no trailing, no averaging down, no martingale, no pyramiding.
- No forced trade count: if nothing qualifies, it does nothing.
- The exit engine has priority over everything else. While a position is open no entry signal is computed.

## Architecture

```
Lighter WebSocket (public)            Lighter WebSocket (authenticated)
 order_book/1  ticker/1  trade/1       account_orders  account_all_trades
        |                              account_all_positions  user_stats
        v                                         |
  market_data.py --> orderbook.py                 v
        |             (local book + BBO overlay)  account_stream.py
        v                                         |
   signals.py  (rolling 100 ms .. 10 s windows)   |  fills, position, balance
        |                                         |
        +---------------> strategy.py <-----------+
                         (state machine, entry gate, GREEN detector)
                               |
              risk.py  pnl.py  |  rate_limits.py
                               v
                         execution.py  --> lighter_client.py --> official lighter-sdk
                         (nonce lock, IOC orders, flatten)        (signer + REST)
                               ^
                               |
                      reconciliation.py  (startup / reconnect / mismatch / recovery)
                      health.py          (watchdog, stale data, heartbeat, status)
                      persistence.py     (SQLite journal + status file, writer thread)
```

| Module | Responsibility |
| --- | --- |
| `config.py` | Environment loading and fail-closed validation |
| `lighter_client.py` | The only place that talks REST / signs, via the official SDK |
| `market_data.py`, `orderbook.py` | Public stream, local book, sequence checks, BBO overlay |
| `account_stream.py` | Authenticated stream: order updates, fills, position, balance |
| `signals.py` | Deterministic microstructure score |
| `pnl.py` | Executable-close estimate (GREEN), VWAP, break-even, realized P&L |
| `risk.py` | Entry gates, sizing, hard-loss and hold-time checks |
| `strategy.py` | Entry engine, fill state machine, immediate-profit exit engine |
| `execution.py` | Order submission, nonce handling, central `flatten_position()` |
| `reconciliation.py` | Authoritative reconciliation and recovery |
| `state_machine.py` | `STARTING, SYNCING, FLAT, ENTRY_PENDING, PARTIALLY_FILLED, OPEN_LONG, OPEN_SHORT, EXIT_PENDING, PARTIAL_EXIT, RECOVERY, HALTED` |
| `rate_limits.py` | Rolling-window accounting with capacity reserved for exits |
| `wire.py`, `precision.py` | Payload parsing and exact integer price/size/quote arithmetic |
| `persistence.py`, `trade_record.py`, `metrics.py` | SQLite journal (writer thread), per-trade record, rolling statistics |
| `health.py`, `status.py`, `logging_setup.py` | Watchdog and heartbeat, status view, non-blocking rotating logs |
| `engine.py`, `main.py` | Startup sequence, task supervision, graceful shutdown, CLI |
| `control.py` | Operator command channel (pause / resume / flatten / stop) and the persisted pause flag |
| `ui/` | Local web control panel: a separate process that never shares the trading event loop |

Hot-path rules: prices and sizes are exchange-native integers; no disk I/O, no database, no
HTTP logging and no pandas/numpy on the trading path. Logging and persistence only enqueue;
dedicated threads do the I/O. Order submission tasks start eagerly, so an order is signed and
written to the socket before control returns to the event loop.

## Entry algorithm

Every book/ticker/trade message updates rolling in-memory windows (not candles) and wakes the
strategy. When the bot is `FLAT` it computes seven components, each normalised to `[-1, +1]`
(positive = upward pressure):

| Component | Definition | Weight variable |
| --- | --- | --- |
| Book imbalance | `(bid depth - ask depth) / total` over the top `BOOK_DEPTH_LEVELS` | `BOOK_IMBALANCE_WEIGHT` |
| Trade flow | aggressive buy vs sell volume, last 1 s | `TRADE_FLOW_WEIGHT` |
| Micro momentum | mean mid return over 100/250/500/1000 ms, scaled by `MOMENTUM_SCALE_BPS` | `MICRO_MOMENTUM_WEIGHT` |
| BBO momentum | net count of best-bid/ask up-ticks vs down-ticks, last 1 s | `BBO_MOMENTUM_WEIGHT` |
| Microprice | size-weighted microprice vs mid, in half-spreads | `MICROPRICE_WEIGHT` |
| Volume acceleration | 250 ms volume rate vs the 5 s baseline, signed by flow | `VOLUME_ACCEL_WEIGHT` |
| Depth change | bids added / asks pulled over 500 ms | `DEPTH_CHANGE_WEIGHT` |

`score = sum(weight * component) / sum(weights)`. LONG if `score >= ENTRY_SCORE_THRESHOLD`,
SHORT if `score <= -ENTRY_SCORE_THRESHOLD`. Arithmetic only; no model, no remote inference,
no self-tuning.

An entry is sent only if **all** of these hold, otherwise it is skipped (never forced):

- state is `FLAT` and no entry block is active (streams healthy, data fresh, startup synced, leverage confirmed);
- the exchange itself reports no BTC position (and, in taker mode, no resting BTC order; in maker mode
  foreign or stale orders are detected on the order stream and by reconciliation);
- spread <= `MAX_SPREAD_BPS`, 1 s price range <= `MAX_VOLATILITY_BPS`;
- taker mode only: the full size fits in the book within `MAX_ENTRY_SLIPPAGE_BPS` (depth-walked VWAP);
- size is above Lighter's minimum, balance covers the margin;
- rate-limit headroom covers the entry, its exit (plus a cancel in maker mode) **and** a reserve;
  `MAX_ENTRIES_PER_MINUTE` not exceeded.

`FLAT -> ENTRY_PENDING` happens before the order is created; further signals are ignored until that
order resolves. An entry is never re-sent. `ENTRY_MODE` selects how the order is placed:

**`maker` (default): a resting post-only order.** A long rests at the best bid, a short at the best
ask, stepped `MAKER_IMPROVE_TICKS` in front of the queue when the spread has room and always at least
one tick away from the other side. It never pays the spread, and Lighter applies no latency to maker
orders (taker orders on Standard accounts are delayed about 300 ms, which is why a momentum signal
taken with IOC tends to arrive after the move). The price of that is fill uncertainty: the order only
fills if someone trades against it, so many signals produce no trade at all.

While the order rests (`ENTRY_PENDING`) every market event checks it. It is cancelled when it has not
filled after `MAKER_REST_MS`, when the market has moved away by more than `MAKER_MAX_DRIFT_BPS`, when
the signal reverses, when entries become blocked (pause, stale data, shutdown) or when part of it
fills. A cancel is not a return to `FLAT`: the order can still fill until the exchange confirms it is
gone, so the state only moves on a terminal order update (cancelled -> `FLAT`, filled -> `OPEN_*`).
If no confirmation arrives within `ORDER_RESOLVE_TIMEOUT_MS`, recovery reads the exchange and
flattens. After a partial fill the remainder is cancelled first and no reduce-only exit is sent
until that cancel is confirmed, so an exit can never race a still-live entry. If the exchange
rejects the order because it would cross (`canceled-post-only`), nothing happened and the bot is flat.
The order carries a 10 minute expiry, so an order orphaned by a crash cannot rest for long; the next
start cancels it anyway.

**`taker`: `LIMIT + IOC`** priced at best ask/bid plus the slippage cap, so it can never fill beyond
the cap. Certain fill, but it pays the spread and is subject to the taker delay.

Exits are the same in both modes: reduce-only, taking liquidity, sent the moment GREEN is detected.

## How GREEN is calculated

GREEN is **not** last price, mark price, candle colour or UI unrealized P&L. For the remaining
position (size `q`, cost basis from actual entry fills):

1. Take the exit side of the live book: **bids for a long, asks for a short**.
2. Walk the levels best-first until `q` is filled, but no further than
   `MAX_NORMAL_EXIT_SLIPPAGE_BPS` from the best price. This gives the executable close value
   (price impact included).
3. `gross = close_value - entry_cost` (long) or `entry_cost - close_value` (short)
4. `net = gross - exit_fee - entry_fee_paid - safety_buffer`
   - fees use the account's taker fee tick from `accountLimits` (never below `TAKER_FEE_BPS` if set;
     raised automatically if a fill reports a higher fee);
   - `safety_buffer = SAFETY_BUFFER_BPS` of the exit value.
5. `GREEN = (the entire q is fillable) AND (net > max(MIN_PROFIT_USD, MIN_PROFIT_BPS of entry notional))`

All of it is exact integer arithmetic in exchange units (`pnl.estimate_close`, heavily unit tested).
If only part of the position could be closed inside the band, it is not green.

## Exit execution

Evaluated on every market event while exposure exists, in this order:

1. **Hard protection** - best executable price is `MAX_ADVERSE_MOVE_BPS` against the entry, or the
   executable net loss reaches `MAX_LOSS_USD`: reduce-only `MARKET + IOC` inside
   `MAX_EMERGENCY_EXIT_SLIPPAGE_BPS`. Runs before any strategy code, so a strategy fault cannot disable it.
2. **GREEN** - reduce-only `MARKET + IOC` for exactly the remaining size, immediately.
3. **`MAX_HOLD_MS`** - also armed as a timer, so it fires even if no market event arrives.
4. Optional signal reversal (`EXIT_ON_SIGNAL_REVERSAL`, off by default).

`OPEN -> EXIT_PENDING` happens before the order is created, so many green events produce one exit.
If the exit fills partially or not at all, the next order is sized from the **actual remaining**
position, always `reduce_only`; after `EXIT_ESCALATE_AFTER` attempts it uses the emergency band, and
after `MAX_EXIT_ATTEMPTS` the central flatten procedure takes over. Exits are never blocked by the
rate limiter.

`PROFIT_EXIT_MODE`:

- `flat_first` (default): once green is detected, get flat within the normal slippage band.
- `profit_only`: the profit exit is a `LIMIT + IOC` at the break-even-plus-minimum price, so it can
  only fill green; on a miss the position goes back to monitoring with all protections active.

A win is counted only after the exit fill is confirmed and realized P&L is computed from fills.

## Crash, restart and reconnect recovery

- **Every start** assumes a position may exist: connect -> verify API key -> load BTC metadata ->
  read account, position and active orders -> cancel stale bot orders -> confirm leverage ->
  only then `FLAT` (or manage the existing position). `EXISTING_POSITION_ACTION` decides what
  happens to a position that is already there: `manage` (adopt it; it is then closed by green,
  hard loss or `MAX_HOLD_MS`), `flatten`, or `halt`.
- **Account stream reconnect**: entries stop (`FLAT -> SYNCING`) until an authoritative REST read
  confirms the state. A stream outage longer than `ACCOUNT_STREAM_GRACE_MS` with exposure triggers recovery.
- **Market data stale** (`MARKET_DATA_STALE_MS`): no entries; with exposure, recovery.
- **Local/exchange disagreement**, an order with no terminal update, or an ambiguous send
  (timeout / 5xx): recovery. The order is never blindly re-sent.
- **Recovery** = `flatten_position()`: read the account, cancel conflicting orders, send a
  reduce-only close for exactly what the exchange reports, re-read, repeat until the exchange
  shows zero. Reduce-only makes duplicates and late fills harmless (they cannot reverse the position).
- **Persistence**: `scalper.db` (SQLite, WAL) holds the last state snapshot, heartbeat, client order
  index, every completed trade, incident events and daily summaries. It is written by a separate
  thread and is only an aid; the exchange is always the authority.
- **Graceful shutdown** (SIGTERM/SIGINT): stop entering, let an in-flight entry resolve, flatten
  if `SHUTDOWN_POSITION_ACTION=flatten`, then close streams and the journal.
- **systemd**: `Restart=always` with a growing delay; exit code 78 (bad config/credentials) is not
  restarted; a 30 s watchdog restarts a hung process.

## Lighter constraints verified during implementation (October 2026)

Checked against the official docs (apidocs.lighter.xyz), SDK `lighter-sdk` 1.1.6 and the live public API:

| Topic | Finding |
| --- | --- |
| Endpoints | REST `https://mainnet.zklighter.elliot.ai`, WS `wss://mainnet.zklighter.elliot.ai/stream`, chain id 304 |
| BTC market | Found by symbol at startup (currently market id 1): price decimals 1, size decimals 5, min 0.00007 BTC and 10 USD, min initial margin 2% (max 50x), default 20x |
| **Taker latency** | Lighter delays taker orders in the sequencer: **300 ms on Standard and Plus, 140 ms on Premium**. Every exit of this bot, and every entry in `ENTRY_MODE=taker`, is a taker order; maker (post-only) entries are not delayed, but cancels are (300 ms on Standard). A taker fill therefore happens at the book as it is ~0.14-0.3 s after the decision; a detected green is an estimate, not a guaranteed fill price |
| Fees | Standard 0 / 0, Plus 0.5 bps, Premium by staked LIT. Read from `accountLimits`; fee ticks are 1e-6 of notional |
| Rate limits | Standard: **60 requests per rolling minute including order transactions** (about 20 round trips per minute at most). Plus/Premium: 24,000 weighted requests and >= 4,000 `sendTx` per minute plus a volume quota |
| Order book stream | Full snapshot, then deltas about every 50 ms with `begin_nonce`/`nonce` continuity; `ticker` (BBO) updates faster |
| Orders | `LIMIT + IOC` and `MARKET + IOC` with `order_expiry = 0`; maker entries are `LIMIT + POST_ONLY` with a 10 minute expiry (Lighter's minimum is 5 minutes); price is the worst acceptable price; `reduce_only` supported |
| Nonces | Per API key, strictly sequential; an API-level rejection does not consume the nonce |
| API keys | Indexes 4-254 for your own keys (0-3 are Lighter's apps). The API key cannot withdraw to other addresses |
| Auth tokens | Max 8 h; the bot uses 7 h tokens and renews while flat |
| WebSocket | Client must send a frame at least every 2 minutes (the bot pings every 5 s) |

Not verifiable without an account: the exact shape of authenticated stream messages and order
acknowledgements was implemented from the official documentation and SDK models, with tolerant
parsing and REST reconciliation as a backstop. See "First live run".

## Requirements

- Ubuntu 24.04 LTS (or a comparable modern Linux with systemd >= 254), x86_64 or arm64, headless.
- Python 3.12+ (installed by the install script). Idle CPU is near zero; memory is well under 200 MB.
- A synchronised clock (`timedatectl status` must show `System clock synchronized: yes`).
- A low-latency network path to Lighter.

### Lighter account and API key

1. A funded Lighter mainnet account. **Use a dedicated sub-account**: the bot treats any BTC
   position on its account as its own and will close it.
2. The **account index** of that (sub-)account:
   ```bash
   curl -s "https://mainnet.zklighter.elliot.ai/api/v1/accountsByL1Address?l1_address=0xYOUR_WALLET"
   ```
3. An **API key** registered for that account at an index between 4 and 254, created in the
   Lighter web app (API keys) or with the official SDK (`examples/system_setup.py`). You need the
   API key's private key and its index. Your wallet key or seed phrase is never needed by this bot.
4. Do not use the same API key index from any other program: nonces are per key.

## Installation (GitHub -> server)

```bash
git clone https://github.com/0xSkyler/lighterbot.git
cd lighterbot
sudo ./deploy/install.sh
```

The script installs OS packages, creates the `scalper` system user, a virtualenv in
`/opt/lighter-scalper/.venv` with the exact versions in `requirements.txt`, the directories
`/var/lib/lighter-scalper` (state) and `/var/log/lighter-scalper` (logs), the environment file
`/etc/lighter-scalper/lighter-scalper.env` (readable by root and the service user only, credentials
empty), the `lighter-scalper` command, the trading service (enabled, **not started**) and the
control panel service (started; it does not trade).

Then either use the control panel (next section), or a terminal:

```bash
sudo nano /etc/lighter-scalper/lighter-scalper.env   # credentials, risk settings, the two safety gates
sudo lighter-scalper check                           # validates everything; sends no order
sudo systemctl start lighter-scalper                 # LIVE
```

## Control panel

A local web page for watching and controlling the bot: `http://127.0.0.1:8787` on the machine that
runs it. It is a separate process from the trading service, so it cannot slow the bot down, and
restarting or closing it never touches the bot.

### Opening it with RustDesk

The panel listens on the loopback interface only. Pick the setup that matches yours:

1. **RustDesk into the server itself.** The server needs a desktop session and a browser (a
   minimal VPS has neither; install a light desktop such as XFCE, a browser, and RustDesk's Linux
   package first). Connect with RustDesk, open the browser, go to `http://127.0.0.1:8787`.
   A "Lighter Scalper Control Panel" entry is added to the applications menu.
2. **RustDesk into another computer (for example your home PC), server stays headless.** On that
   computer open a terminal and forward the port, then browse to the same address:
   ```bash
   ssh -L 8787:127.0.0.1:8787 <user>@<server>
   ```
3. **Run the bot on a desktop PC instead of a server** (Windows, macOS or Linux without systemd)
   and RustDesk into that PC:
   ```bash
   python -m venv .venv
   .venv/bin/pip install -r requirements.txt && .venv/bin/pip install --no-deps .   # Windows: .venv\Scripts\pip
   .venv/bin/lighter-scalper ui --open                                              # Windows: .venv\Scripts\lighter-scalper
   ```
   Settings are kept in `.env` in that folder; the panel starts and stops the bot itself.

The first visit asks you to choose a panel password. To reset it, delete `ui_auth.json` in the data
directory (`/var/lib/lighter-scalper` on a server) and reload the page.

### What it shows

- **Overview** - realized P&L today (from confirmed exit fills only), trades, win rate, the open
  position with its executable exit price and estimated P&L, time held against `MAX_HOLD_MS`, bid /
  ask / spread, the entry signal score against the threshold, a cumulative P&L chart, why entries
  are blocked (if they are), execution latency, rate-limit usage and recent incidents.
- **Trades** - every completed trade and per-day totals from the journal.
- **Settings** - every setting with validation by the same code the bot uses. The API private key
  can be entered or replaced but is never displayed.
- **Logs** - the live bot log, with a warnings-and-errors filter.

### What the buttons do

| Button | Effect |
| --- | --- |
| **Start** | Starts live trading with the saved settings, after a confirmation. Refused while the configuration is incomplete |
| **Stop** | Graceful stop: resolves any order in flight and flattens first (unless `SHUTDOWN_POSITION_ACTION=keep`) |
| **Restart** | Stop, then start; the bot reconciles with the exchange before trading. Applies saved settings |
| **Pause entries** | No new positions. An open position is still managed and closed by its rules. Survives restarts until you resume |
| **Flatten now** | Cancels the bot's BTC orders, closes the BTC position reduce-only, and pauses entries. Works whether the bot is running, stopped or halted |

Changes saved in Settings take effect the next time the bot starts.

### Panel security

- Loopback only by default. It refuses to listen on another address unless `UI_ALLOW_REMOTE=true`
  is set, because it is plain HTTP and can start live trading; if you must reach it remotely, put it
  behind an SSH tunnel or a private network (WireGuard, Tailscale), not on the open internet.
- Password login (salted scrypt hash on disk), attempts throttled, sessions held in memory.
- Every state-changing request needs the session's CSRF token; cross-site requests and requests
  with a foreign `Host` header are refused; a strict Content-Security-Policy forbids inline or
  third-party script. The page loads nothing from the internet.
- The panel runs as the unprivileged `scalper` user. A polkit rule lets it start, stop and restart
  the one unit `lighter-scalper.service` and nothing else.

```bash
sudo systemctl status lighter-scalper-ui      # the panel's own service
sudo systemctl restart lighter-scalper-ui     # safe at any time: does not affect the bot
```

## Configuration (`.env` fields)

Required to start:

| Variable | Meaning |
| --- | --- |
| `LIVE_TRADING=true` | Safety gate. Anything else aborts startup |
| `I_UNDERSTAND_THIS_USES_REAL_FUNDS=YES` | Safety gate |
| `LIGHTER_ACCOUNT_INDEX` | Account (sub-account) index |
| `LIGHTER_API_KEY_INDEX` | API key index (4-254) |
| `LIGHTER_API_PRIVATE_KEY` | API key private key (hex) |
| `LEVERAGE` | e.g. `25`. Validated against the market, set and read back before any entry |
| `POSITION_MODE` | `fixed_margin` (notional = `MARGIN_PER_TRADE_USD` x `LEVERAGE`) or `fixed_notional` |
| `MARGIN_PER_TRADE_USD` / `NOTIONAL_PER_TRADE_USD` | Position size. The account balance is never used automatically |
| `MIN_PROFIT_USD`, `MIN_PROFIT_BPS` | Minimum net profit for GREEN (larger of the two; at least one > 0) |
| `MAX_ENTRY_SLIPPAGE_BPS` | Entry price cap beyond best ask/bid (taker mode) |
| `MAX_NORMAL_EXIT_SLIPPAGE_BPS`, `MAX_EMERGENCY_EXIT_SLIPPAGE_BPS` | Exit price bands |
| `MAX_ADVERSE_MOVE_BPS`, `MAX_LOSS_USD` | Hard protection (at least one > 0) |
| `MAX_HOLD_MS` | Maximum holding time before flattening |
| `MAX_SPREAD_BPS` | No entry above this spread |
| `MARKET_DATA_STALE_MS` | Data older than this is not traded |
| `ENTRY_SCORE_THRESHOLD` | Score needed to enter (0-1) |

Optional (defaults in `.env.example`): `MARKET=BTC`, `MARGIN_MODE=cross|isolated`,
`ENTRY_MODE=maker|taker`, `MAKER_REST_MS`, `MAKER_IMPROVE_TICKS`, `MAKER_MAX_DRIFT_BPS`,
`SAFETY_BUFFER_BPS`, `PROFIT_EXIT_MODE`, `EXIT_ON_SIGNAL_REVERSAL`, the seven `*_WEIGHT` values,
`BOOK_DEPTH_LEVELS`, `MOMENTUM_SCALE_BPS`, `MAX_VOLATILITY_BPS`, `MAX_ENTRIES_PER_MINUTE`,
`EXISTING_POSITION_ACTION`, `SHUTDOWN_POSITION_ACTION`, `CANCEL_FOREIGN_ORDERS`, `TAKER_FEE_BPS`,
`RATE_LIMIT_PER_MINUTE`, `MAX_EXIT_ATTEMPTS`, `EXIT_ESCALATE_AFTER`, `ORDER_RESOLVE_TIMEOUT_MS`,
`ACCOUNT_STREAM_GRACE_MS`, `RECONCILE_INTERVAL_S`, `USE_TICKER_STREAM`, `LOG_LEVEL`, `LOG_TO_FILE`,
`TRADE_CSV`, `DATA_DIR`, `LOG_DIR`.

Startup aborts (exit code 78, no restart loop) on: missing credentials, invalid account index, BTC
market not found, position size <= 0 or below the exchange minimum, leverage not allowed by the
market, missing minimum profit, missing loss protection, inconsistent slippage limits, any endpoint
other than Lighter mainnet, or a missing safety gate.

## Operating

```bash
sudo systemctl start lighter-scalper      # start live trading
sudo systemctl stop lighter-scalper       # stop (flattens first unless SHUTDOWN_POSITION_ACTION=keep)
sudo systemctl restart lighter-scalper    # restart; reconciles before trading
sudo systemctl status lighter-scalper     # service state
sudo journalctl -u lighter-scalper -f     # live logs
sudo lighter-scalper status               # bot snapshot + the exchange's authoritative view
sudo lighter-scalper status --watch       # refreshing terminal view (reads the local snapshot only)
sudo lighter-scalper check                # config + connectivity check, no orders
sudo lighter-scalper ui                   # serve the control panel by hand (normally a service)
```

Everything above is also available in the control panel.

### Emergency

In the control panel: **Flatten now** (closes the position and pauses entries), or **Stop**.

```bash
sudo systemctl stop lighter-scalper       # stops trading and flattens the BTC position
sudo lighter-scalper flatten              # if the service is down or failed: cancel BTC orders,
                                          # close the BTC position reduce-only, confirm flat
```

`flatten` refuses to run while the service is alive (they would share one API key's nonce); stop
the service first, or pass `--force`. Afterwards confirm with `sudo lighter-scalper status`.
If the server itself is unreachable, close the position in the Lighter web app.

### Logs and data

- `journalctl -u lighter-scalper` and `/var/log/lighter-scalper/scalper.log` (rotated, 6 x 20 MB max).
  ```
  2026-10-07T10:15:01.001Z SIGNAL LONG score=0.870 ...
  2026-10-07T10:15:01.004Z ENTRY_SENT side=BUY qty=0.00298 limit=83702.3 ...
  2026-10-07T10:15:01.320Z ENTRY_FILL avg=83693.9 qty=0.00298 ...
  2026-10-07T10:15:02.016Z GREEN expected_net_pnl=0.0340 ...
  2026-10-07T10:15:02.018Z EXIT_SENT reduce_only=true ...
  2026-10-07T10:15:02.340Z FLAT realized_pnl=0.0298 ...
  ```
- `/var/lib/lighter-scalper/scalper.db` - trades (uuid, side, leverage, size, signal score and
  components, decision/send/fill timestamps, average entry/exit, holding ms, gross, fees, estimated
  slippage, realized P&L, MFE/MAE, exit reason, per-trade latencies), events, state snapshot.
- `/var/lib/lighter-scalper/trades.csv` - the same trades as CSV.
- `/var/lib/lighter-scalper/daily/YYYY-MM-DD.json` - daily summary.
- `/var/lib/lighter-scalper/status.json` - the snapshot shown by `status`.

```bash
sudo sqlite3 /var/lib/lighter-scalper/scalper.db \
  "select entry_fill_at, side, realized_pnl_usd, holding_ms, exit_reason from trades order by rowid desc limit 20;"
```

### Updating

```bash
cd lighterbot
sudo ./deploy/update.sh      # git pull --ff-only, dependencies, units, restart
```

The control panel is restarted. The trading service is restarted only if it was running, so an
update never starts trading you had stopped. State and the environment file are never touched.

## First live run

The public market-data path, the SDK wrapper, signing and all REST reads were exercised against
Lighter mainnet during development. Order submission and the authenticated streams cannot be
exercised without a funded account, so treat the first session as commissioning:

1. `sudo lighter-scalper check` must print `CHECK: OK`.
2. Start with the smallest size Lighter accepts (10 USD notional) and watch
   `journalctl -u lighter-scalper -f` through several complete `ENTRY_SENT -> ENTRY_FILL -> EXIT_SENT -> FLAT` cycles.
3. Compare `sudo lighter-scalper status` with the Lighter web app after each of the first trades.
4. If you ever see `ORDER_UNRESOLVED`, `ACCOUNT_MESSAGE_ERROR` or repeated `RECOVERY_START`, stop
   the service and inspect the log before continuing.

## Troubleshooting

| Symptom | Meaning / action |
| --- | --- |
| Service exits immediately, `CONFIG_ERROR` / exit code 78 | Fix the listed variables in the environment file, then `sudo systemctl start lighter-scalper` |
| `API key check failed` | Wrong account index, key index or private key |
| `ENTRIES BLOCKED: LEVERAGE_UNCONFIRMED` | The exchange did not report the configured leverage. Check `MARGIN_MODE`/`LEVERAGE`; retried at every reconciliation |
| `ENTRIES BLOCKED: FOREIGN_ORDERS` | A resting BTC order not created by the bot exists. Cancel it or set `CANCEL_FOREIGN_ORDERS=true` |
| `MARKET_DATA_STALE`, `MARKET_WS_DISCONNECTED` | Network/exchange issue; the bot reconnects with backoff and resyncs the book |
| `RATE_LIMIT_RESERVE` in skips, HTTP 429 | The account tier's request budget is the limit; lower `MAX_ENTRIES_PER_MINUTE` or use a higher tier |
| No trades at all | Check `sudo lighter-scalper status`: entry blocks, spread vs `MAX_SPREAD_BPS`, `signal_score` vs `ENTRY_SCORE_THRESHOLD` |
| `HALTED` | Deliberate stop (credentials rejected, or `EXISTING_POSITION_ACTION=halt` found a position). Resolve, then restart |
| `EVENT_LOOP_STALL` | The VPS is starved of CPU; entries pause for 2 s after each stall |
| Panel: Start/Stop says it is not allowed | The polkit rule is missing: re-run `sudo ./deploy/install.sh`, or use `systemctl` |
| Panel: "This host name is not allowed" | You opened it by a name other than `127.0.0.1`/`localhost`; use the SSH tunnel, or list the name in `UI_ALLOWED_HOSTS` |
| Panel shows "Bot: not running" while the service is active | The bot has not written a status snapshot for 5 s: check Logs |
| Entries stay paused after a restart | Pausing is persistent by design: press **Resume entries** |

## Credential security

- Secrets live only in the environment file (mode 660, root and the `scalper` service user; no
  other user can read it) or in your local `.env`. `.gitignore` excludes them; only `.env.example`
  is committed.
- The control panel never sends the private key to the browser and never logs setting values.
- The private key is excluded from object reprs, and log output is passed through a redaction
  filter. Configuration errors never echo the key.
- The service runs as the unprivileged `scalper` user with a read-only filesystem apart from its
  data and log directories, no capabilities and no new privileges.
- Use an API key dedicated to this bot, on a dedicated sub-account that holds only the trading capital.
- If a key may have leaked, replace it in Lighter immediately and update the environment file.

## Development

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt && pip install -e ".[dev]"
pytest                 # 389 deterministic tests, no network, no orders
ruff check src tests && mypy
```

The tests cover P&L and VWAP, order-book updates, spread, imbalance and every signal component,
price/quantity rounding, the state machine, duplicate-order prevention, partial fills, reduce-only
quantities, the rate-limit reserve, stale-data handling, nonce handling, the flatten procedure,
startup/reconnect reconciliation, the whole service running against a simulated exchange
(`tests/test_engine.py`), and the control panel (login, CSRF and host guards, settings validation,
secret handling, service control, operator commands).

Deliberately not included: paper/shadow trading, backtesting, other exchanges or markets, grid/DCA/
martingale logic, Telegram control, trade commands from outside the machine, LLM decisions, Docker
(run it under systemd as above).

## License

MIT - see `LICENSE`. No warranty of any kind; you are solely responsible for orders placed by this software.
