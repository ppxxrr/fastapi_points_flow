#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/fastapi_points_flow"
BACKUP_ENV_FILE="${APP_DIR}/.backup.env"
MAIN_ENV_FILE="${APP_DIR}/.env"
TMP_ROOT="/tmp"
LOCAL_BACKUP_ROOT="${APP_DIR}/backups/weekly"
MOUNT_POINT="/mnt/it_backup"
SHARE_ROOT="//10.95.17.75/it"
SHARE_SUBDIR="备份/数据库"

if [[ -f "${MAIN_ENV_FILE}" ]]; then
  set -a
  source "${MAIN_ENV_FILE}"
  set +a
fi

if [[ -f "${BACKUP_ENV_FILE}" ]]; then
  set -a
  source "${BACKUP_ENV_FILE}"
  set +a
fi

: "${MYSQL_HOST:?MYSQL_HOST is required}"
: "${MYSQL_PORT:?MYSQL_PORT is required}"
: "${MYSQL_USER:?MYSQL_USER is required}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD is required}"
: "${MYSQL_DATABASE:?MYSQL_DATABASE is required}"
: "${SMB_USERNAME:?SMB_USERNAME is required}"
: "${SMB_PASSWORD:?SMB_PASSWORD is required}"

timestamp="$(date '+%Y%m%d_%H%M%S')"
backup_name="fastapi_full_backup_${timestamp}"
tmp_dir="${TMP_ROOT}/${backup_name}"
local_backup_dir="${LOCAL_BACKUP_ROOT}/${backup_name}"
share_backup_dir="${MOUNT_POINT}/${SHARE_SUBDIR}/${backup_name}"

cleanup() {
  if mountpoint -q "${MOUNT_POINT}"; then
    umount "${MOUNT_POINT}" || true
  fi
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

mkdir -p "${tmp_dir}" "${LOCAL_BACKUP_ROOT}" "${MOUNT_POINT}"

cat > "${tmp_dir}/manifest.txt" <<EOF
backup_name=${backup_name}
created_at=$(date --iso-8601=seconds)
server=10.95.17.195
app_dir=${APP_DIR}
restore_scope=full_site_and_database
EOF

git -c safe.directory="${APP_DIR}" -C "${APP_DIR}" remote -v > "${tmp_dir}/git_remote.txt" || true
git -c safe.directory="${APP_DIR}" -C "${APP_DIR}" rev-parse HEAD > "${tmp_dir}/git_head.txt" || true
git -c safe.directory="${APP_DIR}" -C "${APP_DIR}" status --short > "${tmp_dir}/git_status.txt" || true
cp "${MAIN_ENV_FILE}" "${tmp_dir}/app.env"

mysqldump \
  --single-transaction \
  --routines \
  --triggers \
  --events \
  -h"${MYSQL_HOST}" \
  -P"${MYSQL_PORT}" \
  -u"${MYSQL_USER}" \
  --password="${MYSQL_PASSWORD}" \
  "${MYSQL_DATABASE}" | gzip -c > "${tmp_dir}/fastapi_member.sql.gz"

tar \
  --exclude='fastapi_points_flow/.venv' \
  --exclude='fastapi_points_flow/frontend/node_modules' \
  --exclude='fastapi_points_flow/.codex_backups' \
  --exclude='fastapi_points_flow/backups' \
  -czf "${tmp_dir}/site_files.tar.gz" -C /opt fastapi_points_flow

tar -czf "${tmp_dir}/system_configs.tar.gz" \
  /etc/nginx/nginx.conf \
  /etc/nginx/sites-enabled \
  /etc/nginx/sites-available \
  /etc/systemd/system/fastapi*.service \
  /etc/systemd/system/fastapi*.timer \
  2>/dev/null || true

sha256sum "${tmp_dir}"/* > "${tmp_dir}/sha256sums.txt"

mkdir -p "${local_backup_dir}"
cp -a "${tmp_dir}/." "${local_backup_dir}/"

mount -t cifs "${SHARE_ROOT}" "${MOUNT_POINT}" -o "username=${SMB_USERNAME},password=${SMB_PASSWORD},vers=3.0,iocharset=utf8"
mkdir -p "${share_backup_dir}"
cp -a "${tmp_dir}/." "${share_backup_dir}/"

echo "backup completed: ${share_backup_dir}"
