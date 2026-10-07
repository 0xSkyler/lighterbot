#!/usr/bin/env bash
# Update an installed Lighter BTC scalper from GitHub.
#
#   cd lighterbot
#   sudo ./deploy/update.sh
#
# Steps: git pull --ff-only -> dependencies, units and permissions -> restart.
# The control panel is always restarted. The trading service is restarted only if it was
# running: an update never starts live trading that you had stopped. A restart sends SIGTERM,
# so the bot resolves in-flight orders and flattens (unless SHUTDOWN_POSITION_ACTION=keep),
# and the new process reconciles with the exchange before it trades.
# Local state (/var/lib/lighter-scalper) and the environment file are never touched.
set -euo pipefail

APP_DIR="/opt/lighter-scalper"
UNIT="lighter-scalper.service"

if [[ "${EUID}" -ne 0 ]]; then
    echo "run as root: sudo ./deploy/update.sh" >&2
    exit 1
fi
if [[ ! -x "${APP_DIR}/.venv/bin/python" ]]; then
    echo "not installed yet: run sudo ./deploy/install.sh first" >&2
    exit 1
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_OWNER="$(stat -c '%U' "${REPO_DIR}")"

echo "==> git pull --ff-only (as ${REPO_OWNER})"
if [[ "${REPO_OWNER}" == "root" ]]; then
    git -C "${REPO_DIR}" pull --ff-only
else
    sudo -u "${REPO_OWNER}" git -C "${REPO_DIR}" pull --ff-only
fi
echo "    now at $(git -C "${REPO_DIR}" -c safe.directory="${REPO_DIR}" rev-parse --short HEAD)"

# Dependencies, unit files, permissions and the control panel (restarted by the installer).
bash "${REPO_DIR}/deploy/install.sh" --upgrade

if systemctl is-active --quiet "${UNIT}"; then
    echo "==> restarting the trading service (it was running)"
    systemctl restart "${UNIT}"
    sleep 2
else
    echo "==> the trading service was not running: left stopped"
fi
systemctl --no-pager --lines=10 status "${UNIT}" || true
