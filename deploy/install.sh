#!/usr/bin/env bash
# Install the Lighter BTC scalper as a systemd service on Ubuntu 24.04 (or compatible).
#
#   git clone https://github.com/0xSkyler/lighterbot.git
#   cd lighterbot
#   sudo ./deploy/install.sh
#
# The script is idempotent. It never writes credentials and never overwrites an existing
# environment file. The service is enabled but NOT started: you start it after filling in
# /etc/lighter-scalper/lighter-scalper.env.
set -euo pipefail

APP_USER="scalper"
APP_DIR="/opt/lighter-scalper"
ENV_DIR="/etc/lighter-scalper"
ENV_FILE="${ENV_DIR}/lighter-scalper.env"
DATA_DIR="/var/lib/lighter-scalper"
LOG_DIR="/var/log/lighter-scalper"
UNIT="lighter-scalper.service"
WRAPPER="/usr/local/bin/lighter-scalper"

if [[ "${EUID}" -ne 0 ]]; then
    echo "run as root: sudo ./deploy/install.sh" >&2
    exit 1
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "==> installing from ${REPO_DIR}"

echo "==> OS packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip ca-certificates git >/dev/null

if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)'; then
    echo "Python 3.12 or newer is required (found $(python3 --version 2>&1))" >&2
    exit 1
fi

echo "==> service user and directories"
if ! id -u "${APP_USER}" >/dev/null 2>&1; then
    useradd --system --home-dir "${DATA_DIR}" --shell /usr/sbin/nologin "${APP_USER}"
fi
install -d -m 0755 -o root -g root "${APP_DIR}"
install -d -m 0750 -o root -g "${APP_USER}" "${ENV_DIR}"
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
    install -m 0600 -o root -g root "${REPO_DIR}/.env.example" "${ENV_FILE}"
    echo "    created ${ENV_FILE} from .env.example (credentials are EMPTY)"
fi
chmod 0600 "${ENV_FILE}"
chown root:root "${ENV_FILE}"

echo "==> command wrapper ${WRAPPER}"
cat >"${WRAPPER}" <<EOF
#!/usr/bin/env bash
# Runs the installed lighter-scalper with the same environment the service uses.
export SCALPER_ENV_FILE="\${SCALPER_ENV_FILE:-${ENV_FILE}}"
export DATA_DIR="\${DATA_DIR:-${DATA_DIR}}"
export LOG_DIR="\${LOG_DIR:-${LOG_DIR}}"
exec "${APP_DIR}/.venv/bin/lighter-scalper" "\$@"
EOF
chmod 0755 "${WRAPPER}"

echo "==> systemd unit"
install -m 0644 -o root -g root "${REPO_DIR}/deploy/${UNIT}" "/etc/systemd/system/${UNIT}"
systemctl daemon-reload
systemctl enable "${UNIT}" >/dev/null

echo "==> clock synchronisation"
timedatectl set-ntp true >/dev/null 2>&1 || true
if [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null)" == "yes" ]]; then
    echo "    system clock is synchronised"
else
    echo "    WARNING: the system clock is not reported as synchronised yet."
    echo "    Check with: timedatectl status   (install chrony if it stays unsynchronised)"
fi

cat <<EOF

Installed. The service is enabled but not started.

Next steps:
  1. Put your Lighter credentials and settings in the environment file:
         sudo nano ${ENV_FILE}
     Set LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX, LIGHTER_API_PRIVATE_KEY, review every
     risk setting, then set LIVE_TRADING=true and I_UNDERSTAND_THIS_USES_REAL_FUNDS=YES.
  2. Validate without sending any order:
         sudo lighter-scalper check
  3. Start live trading:
         sudo systemctl start lighter-scalper
  4. Watch it:
         sudo lighter-scalper status --watch
         sudo journalctl -u lighter-scalper -f
EOF
