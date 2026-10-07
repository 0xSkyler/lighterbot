#!/usr/bin/env bash
# Install the Lighter BTC scalper and its control panel as systemd services on Ubuntu 24.04
# (or compatible).
#
#   git clone https://github.com/0xSkyler/lighterbot.git
#   cd lighterbot
#   sudo ./deploy/install.sh
#
# The script is idempotent. It never writes credentials and never overwrites an existing
# environment file. The trading service is enabled but NOT started: you start it yourself
# (from the control panel or with systemctl) after entering your settings. The control
# panel is started; it does not trade.
#
#   --upgrade   used by deploy/update.sh: skip OS packages and the closing instructions
set -euo pipefail

APP_USER="scalper"
APP_DIR="/opt/lighter-scalper"
ENV_DIR="/etc/lighter-scalper"
ENV_FILE="${ENV_DIR}/lighter-scalper.env"
DATA_DIR="/var/lib/lighter-scalper"
LOG_DIR="/var/log/lighter-scalper"
UNIT="lighter-scalper.service"
UI_UNIT="lighter-scalper-ui.service"
POLKIT_RULE="50-lighter-scalper.rules"
WRAPPER="/usr/local/bin/lighter-scalper"
UI_URL="http://127.0.0.1:8787"

UPGRADE=0
if [[ "${1:-}" == "--upgrade" ]]; then
    UPGRADE=1
fi

if [[ "${EUID}" -ne 0 ]]; then
    echo "run as root: sudo ./deploy/install.sh" >&2
    exit 1
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "==> installing from ${REPO_DIR}"

if [[ "${UPGRADE}" -eq 0 ]]; then
    echo "==> OS packages"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq python3 python3-venv python3-pip ca-certificates git >/dev/null
    # polkit lets the control panel start/stop the trading service without root.
    apt-get install -y -qq polkitd >/dev/null 2>&1 || apt-get install -y -qq policykit-1 >/dev/null 2>&1 || true
fi

if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)'; then
    echo "Python 3.12 or newer is required (found $(python3 --version 2>&1))" >&2
    exit 1
fi

echo "==> service user and directories"
if ! id -u "${APP_USER}" >/dev/null 2>&1; then
    useradd --system --home-dir "${DATA_DIR}" --shell /usr/sbin/nologin "${APP_USER}"
fi
install -d -m 0755 -o root -g root "${APP_DIR}"
# The control panel (user scalper) edits the environment file from its Settings tab.
install -d -m 0770 -o root -g "${APP_USER}" "${ENV_DIR}"
install -d -m 0750 -o "${APP_USER}" -g "${APP_USER}" "${DATA_DIR}" "${LOG_DIR}"

echo "==> Python virtualenv with exact dependency versions"
if [[ ! -x "${APP_DIR}/.venv/bin/python" ]]; then
    python3 -m venv "${APP_DIR}/.venv"
fi
"${APP_DIR}/.venv/bin/python" -m pip install --quiet --upgrade pip
"${APP_DIR}/.venv/bin/python" -m pip install --quiet -r "${REPO_DIR}/requirements.txt"
"${APP_DIR}/.venv/bin/python" -m pip install --quiet --no-deps --force-reinstall "${REPO_DIR}"

echo "==> environment file"
if [[ -f "${ENV_FILE}" ]]; then
    echo "    keeping existing ${ENV_FILE}"
else
    install -m 0660 -o root -g "${APP_USER}" "${REPO_DIR}/.env.example" "${ENV_FILE}"
    echo "    created ${ENV_FILE} from .env.example (credentials are EMPTY)"
fi
# Readable and writable by root and the service user only; never by anyone else.
chgrp "${APP_USER}" "${ENV_FILE}"
chmod 0660 "${ENV_FILE}"

echo "==> command wrapper ${WRAPPER}"
cat >"${WRAPPER}" <<EOF
#!/usr/bin/env bash
# Runs the installed lighter-scalper with the same environment the services use.
export SCALPER_ENV_FILE="\${SCALPER_ENV_FILE:-${ENV_FILE}}"
export DATA_DIR="\${DATA_DIR:-${DATA_DIR}}"
export LOG_DIR="\${LOG_DIR:-${LOG_DIR}}"
exec "${APP_DIR}/.venv/bin/lighter-scalper" "\$@"
EOF
chmod 0755 "${WRAPPER}"

echo "==> systemd units"
install -m 0644 -o root -g root "${REPO_DIR}/deploy/${UNIT}" "/etc/systemd/system/${UNIT}"
install -m 0644 -o root -g root "${REPO_DIR}/deploy/${UI_UNIT}" "/etc/systemd/system/${UI_UNIT}"
systemctl daemon-reload
systemctl enable "${UNIT}" >/dev/null
systemctl enable "${UI_UNIT}" >/dev/null

echo "==> permission for the control panel to start/stop the trading service"
if [[ -d /etc/polkit-1/rules.d ]]; then
    install -m 0644 -o root -g root "${REPO_DIR}/deploy/${POLKIT_RULE}" "/etc/polkit-1/rules.d/${POLKIT_RULE}"
else
    echo "    WARNING: /etc/polkit-1/rules.d not found. The panel will show status and settings,"
    echo "    but Start/Stop/Restart must be done with systemctl."
fi

if [[ -d /usr/share/applications ]]; then
    install -m 0644 -o root -g root "${REPO_DIR}/deploy/lighter-scalper-ui.desktop" \
        /usr/share/applications/lighter-scalper-ui.desktop
fi

echo "==> control panel"
systemctl restart "${UI_UNIT}"

if [[ "${UPGRADE}" -eq 1 ]]; then
    exit 0
fi

echo "==> clock synchronisation"
timedatectl set-ntp true >/dev/null 2>&1 || true
if [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null)" == "yes" ]]; then
    echo "    system clock is synchronised"
else
    echo "    WARNING: the system clock is not reported as synchronised yet."
    echo "    Check with: timedatectl status   (install chrony if it stays unsynchronised)"
fi

cat <<EOF

Installed. The control panel is running; the trading service is enabled but not started.

Control panel (on this machine only): ${UI_URL}
  - Connect to this machine's desktop with RustDesk and open that address in a browser, or
  - from another computer: ssh -L 8787:127.0.0.1:8787 <user>@<this-server>  then open ${UI_URL}
  The first visit asks you to choose the panel password.

Next steps (in the panel, or in a terminal):
  1. Settings: enter LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX and the API key private key,
     review every risk setting, then switch on both live-trading switches.
       (terminal: sudo nano ${ENV_FILE})
  2. Validate without sending any order:   sudo lighter-scalper check
  3. Start live trading: the Start button, or   sudo systemctl start lighter-scalper
  4. Watch it: the panel, or   sudo journalctl -u lighter-scalper -f
EOF
