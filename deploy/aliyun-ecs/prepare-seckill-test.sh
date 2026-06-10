#!/usr/bin/env bash
set -euo pipefail

START_TS="$(date +%s)"

VOUCHER_ID="${1:-3001}"
STOCK="${2:-1500}"
USER_COUNT="${3:-2000}"
OUTPUT_CSV="${4:-outputs/pressure_test/seckill_tokens.csv}"

MYSQL_CONTAINER="${MYSQL_CONTAINER:-flashdeal-mysql}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-123456}"
DB_NAME="${DB_NAME:-hmdp}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"
RABBITMQ_CONTAINER="${RABBITMQ_CONTAINER:-flashdeal-rabbitmq}"
USER_BATCH_SIZE="${USER_BATCH_SIZE:-1000}"
LOGIN_USER_TTL_SECONDS="${LOGIN_USER_TTL_SECONDS:-2160000}"

SQL_FILE="/tmp/flashdeal_prepare_users_${VOUCHER_ID}_$$.sql"
USER_MAP_FILE="/tmp/flashdeal_prepare_user_map_${VOUCHER_ID}_$$.tsv"
REDIS_PIPE_FILE="/tmp/flashdeal_prepare_tokens_${VOUCHER_ID}_$$.resp"

cleanup() {
  rm -f "${SQL_FILE}" "${USER_MAP_FILE}" "${REDIS_PIPE_FILE}"
}
trap cleanup EXIT

resp_command() {
  local args=("$@")
  printf '*%s\r\n' "${#args[@]}"
  local arg
  for arg in "${args[@]}"; do
    printf '$%s\r\n%s\r\n' "${#arg}" "${arg}"
  done
}

mkdir -p "$(dirname "${OUTPUT_CSV}")"
echo "token" > "${OUTPUT_CSV}"

echo "Start prepare seckill pressure-test data"
echo "startTime=$(date '+%Y-%m-%d %H:%M:%S')"
echo "voucherId=${VOUCHER_ID}"
echo "stock=${STOCK}"
echo "userCount=${USER_COUNT}"
echo "outputCsv=${OUTPUT_CSV}"

echo "Prepare seckill voucher and reset order data"
docker exec -i "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB_NAME}" <<SQL
DELETE FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID};
DELETE FROM tb_seckill_voucher WHERE voucher_id = ${VOUCHER_ID};
DELETE FROM tb_voucher WHERE id = ${VOUCHER_ID};
INSERT INTO tb_voucher
  (id, shop_id, title, sub_title, rules, pay_value, actual_value, type, status, create_time, update_time)
VALUES
  (${VOUCHER_ID}, 1, 'PTS seckill test voucher ${VOUCHER_ID}', 'pressure test only', 'pressure test data only', 100, 100, 1, 1, NOW(), NOW());
INSERT INTO tb_seckill_voucher
  (voucher_id, stock, create_time, begin_time, end_time, update_time)
VALUES
  (${VOUCHER_ID}, ${STOCK}, NOW(), DATE_SUB(NOW(), INTERVAL 1 DAY), DATE_ADD(NOW(), INTERVAL 1 DAY), NOW());
SQL

echo "Generate batched MySQL user SQL: ${SQL_FILE}"
{
  echo "START TRANSACTION;"
  echo "CREATE TEMPORARY TABLE tmp_pts_users (phone varchar(11) PRIMARY KEY, nick_name varchar(64) NOT NULL) ENGINE=MEMORY;"

  for i in $(seq 1 "${USER_COUNT}"); do
    if (( (i - 1) % USER_BATCH_SIZE == 0 )); then
      if (( i > 1 )); then
        echo ";"
      fi
      printf "INSERT INTO tmp_pts_users(phone, nick_name) VALUES\n"
    else
      printf ",\n"
    fi

    PHONE="$(printf '199%08d' "${i}")"
    NICK_NAME="pts_user_${i}"
    printf "('%s','%s')" "${PHONE}" "${NICK_NAME}"
  done
  echo ";"

  echo "INSERT INTO tb_user(phone, password, nick_name, icon, create_time, update_time)"
  echo "SELECT phone, '', nick_name, '', NOW(), NOW() FROM tmp_pts_users"
  echo "ON DUPLICATE KEY UPDATE nick_name = VALUES(nick_name), icon = VALUES(icon), update_time = NOW();"
  echo "COMMIT;"
} > "${SQL_FILE}"

echo "Import batched users into MySQL"
docker exec -i "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB_NAME}" < "${SQL_FILE}"

FIRST_PHONE="$(printf '199%08d' 1)"
LAST_PHONE="$(printf '199%08d' "${USER_COUNT}")"

echo "Export user id mapping from MySQL"
docker exec -i "${MYSQL_CONTAINER}" mysql -N -B -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB_NAME}" \
  -e "SELECT id, phone, nick_name FROM tb_user WHERE phone BETWEEN '${FIRST_PHONE}' AND '${LAST_PHONE}' ORDER BY phone;" \
  > "${USER_MAP_FILE}"

USER_ROWS="$(wc -l < "${USER_MAP_FILE}" | tr -d ' ')"
if [[ "${USER_ROWS}" != "${USER_COUNT}" ]]; then
  echo "Expected ${USER_COUNT} users, but found ${USER_ROWS} in MySQL mapping"
  exit 1
fi

echo "Generate Redis protocol pipeline file: ${REDIS_PIPE_FILE}"
while IFS=$'\t' read -r USER_ID PHONE NICK_NAME; do
  TOKEN="pts_${VOUCHER_ID}_${PHONE}_$(date +%s%N)"
  TOKEN_KEY="login:token:${TOKEN}"
  resp_command HSET "${TOKEN_KEY}" id "${USER_ID}" nickName "${NICK_NAME}" icon "" >> "${REDIS_PIPE_FILE}"
  resp_command EXPIRE "${TOKEN_KEY}" "${LOGIN_USER_TTL_SECONDS}" >> "${REDIS_PIPE_FILE}"
  echo "${TOKEN}" >> "${OUTPUT_CSV}"
done < "${USER_MAP_FILE}"

echo "Write Redis login tokens by redis-cli --pipe"
"${REDIS_CLI}" --pipe < "${REDIS_PIPE_FILE}"

echo "Reset Redis seckill state"
"${REDIS_CLI}" SET "seckill:stock:${VOUCHER_ID}" "${STOCK}" >/dev/null
"${REDIS_CLI}" DEL "seckill:order:${VOUCHER_ID}" >/dev/null

echo "Purge RabbitMQ seckill queues if they exist"
docker exec "${RABBITMQ_CONTAINER}" rabbitmqctl purge_queue flashdeal.seckill.order.queue >/dev/null 2>&1 || true
docker exec "${RABBITMQ_CONTAINER}" rabbitmqctl purge_queue flashdeal.seckill.order.dlq >/dev/null 2>&1 || true

END_TS="$(date +%s)"
COST_SECONDS=$((END_TS - START_TS))

echo "Prepared."
echo "endTime=$(date '+%Y-%m-%d %H:%M:%S')"
echo "costSeconds=${COST_SECONDS}"
echo "voucherId=${VOUCHER_ID}"
echo "stock=${STOCK}"
echo "userCount=${USER_COUNT}"
echo "outputCsv=${OUTPUT_CSV}"
echo "PTS CSV header is: token"
