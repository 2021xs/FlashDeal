#!/usr/bin/env bash
set -euo pipefail

MYSQL_CONTAINER="${MYSQL_CONTAINER:-flashdeal-mysql}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-123456}"
DB_NAME="${DB_NAME:-hmdp}"
SQL_PATH="${1:-../../src/main/resources/db/flashdeal.sql}"

if [[ ! -f "${SQL_PATH}" && -f "./flashdeal.sql" ]]; then
  SQL_PATH="./flashdeal.sql"
fi

if [[ ! -f "${SQL_PATH}" ]]; then
  echo "SQL file not found."
  echo "Usage: bash import-db.sh /path/to/flashdeal.sql"
  exit 1
fi

echo "Reset database: ${DB_NAME}"
docker exec -i "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" \
  -e "DROP DATABASE IF EXISTS ${DB_NAME}; CREATE DATABASE ${DB_NAME} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

echo "Import SQL: ${SQL_PATH}"
docker exec -i "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB_NAME}" < "${SQL_PATH}"

echo "Check unique index uk_user_voucher:"
docker exec -i "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB_NAME}" \
  -e "SHOW INDEX FROM tb_voucher_order WHERE Key_name = 'uk_user_voucher';"

echo "Check cancel_time column:"
docker exec -i "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB_NAME}" \
  -e "SHOW COLUMNS FROM tb_voucher_order LIKE 'cancel_time';"

echo "Database import finished."
