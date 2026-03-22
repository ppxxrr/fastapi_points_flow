#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/fastapi_points_flow"
VENV_DIR="${PROJECT_DIR}/.venv"
FRONTEND_DIR="${PROJECT_DIR}/frontend"

echo "[1/7] enter project dir"
cd "${PROJECT_DIR}"

echo "[2/7] ensure venv exists"
if [ ! -d "${VENV_DIR}" ]; then
  python3 -m venv "${VENV_DIR}"
fi

echo "[3/7] install backend deps"
"${VENV_DIR}/bin/pip" install -r requirements.txt

echo "[4/7] run alembic"
"${VENV_DIR}/bin/alembic" upgrade head

echo "[5/7] build frontend"
cd "${FRONTEND_DIR}"
npm install
npm run build

echo "[6/7] restart services"
sudo systemctl daemon-reload || true
sudo systemctl restart fastapi-web.service
sudo systemctl restart fastapi-daily-incremental-sync.timer
sudo systemctl restart fastapi-daily-incremental-retry.timer

echo "[7/7] show status"
sudo systemctl --no-pager --full status fastapi-web.service | sed -n '1,20p'
sudo systemctl --no-pager --full status fastapi-daily-incremental-sync.timer | sed -n '1,20p'
sudo systemctl --no-pager --full status fastapi-daily-incremental-retry.timer | sed -n '1,20p'

echo "[OK] deploy finished"
