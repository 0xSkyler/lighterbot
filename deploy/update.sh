#!/usr/bin/env bash
# Update an installed Lighter BTC scalper from GitHub and restart it.
#
#   cd lighterbot
#   sudo ./deploy/update.sh
#
# Steps: git pull --ff-only -> dependency update -> service restart.
# The restart sends SIGTERM: the service resolves in-flight orders and flattens (unless
# SHUTDOWN_POSITION_ACTION=keep), and the new process reconciles with the exchange before
# it trades. Local state (/var/lib/lighter-scalper) and the environment file are never touched.
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

echo "==> dependencies"
"${APP_DIR}/.venv/bin/python" -m pip install --quiet -r "${REPO_DIR}/requirements.txt"
"${APP_DIR}/.venv/bin/python" -m pip install --quiet --no-deps --force-reinstall "${REPO_DIR}"

echo "==> systemd unit"
if ! cmp -s "${REPO_DIR}/deploy/${UNIT}" "/etc/systemd/system/${UNIT}"; then
    install -m 0644 -o root -g root "${REPO_DIR}/deploy/${UNIT}" "/etc/systemd/system/${UNIT}"
    systemctl daemon-reload
    echo "    unit file updated"
fi

echo "==> restart"
systemctl restart "${UNIT}"
sleep 2
systemctl --no-pager --lines=15 status "${UNIT}" || true
