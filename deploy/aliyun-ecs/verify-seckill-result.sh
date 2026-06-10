#!/usr/bin/env bash
set -euo pipefail

VOUCHER_ID="${1:?Usage: bash verify-seckill-result.sh voucherId initialStock}"
INITIAL_STOCK="${2:?Usage: bash verify-seckill-result.sh voucherId initialStock}"

MYSQL_CONTAINER="${MYSQL_CONTAINER:-flashdeal-mysql}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-123456}"
DB_NAME="${DB_NAME:-hmdp}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"
RABBITMQ_CONTAINER="${RABBITMQ_CONTAINER:-flashdeal-rabbitmq}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"

mysql_scalar() {
  docker exec -i "${MYSQL_CONTAINER}" mysql -N -B -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB_NAME}" -e "$1"
}

read_queue_info() {
  docker exec "${RABBITMQ_CONTAINER}" rabbitmqctl list_queues name messages_ready messages_unacknowledged 2>/dev/null || true
}

echo "Wait for RabbitMQ seckill order queue to drain, timeout=${WAIT_SECONDS}s"
for _ in $(seq 1 "${WAIT_SECONDS}"); do
  QUEUE_INFO="$(read_queue_info)"
  ORDER_QUEUE_LINE="$(echo "${QUEUE_INFO}" | awk '$1=="flashdeal.seckill.order.queue" {print $0}')"
  ORDER_READY="$(echo "${ORDER_QUEUE_LINE}" | awk '{print $2}')"
  ORDER_UNACK="$(echo "${ORDER_QUEUE_LINE}" | awk '{print $3}')"
  ORDER_READY="${ORDER_READY:-0}"
  ORDER_UNACK="${ORDER_UNACK:-0}"
  if (( ORDER_READY == 0 && ORDER_UNACK == 0 )); then
    break
  fi
  sleep 1
done

ORDER_COUNT="$(mysql_scalar "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID};")"
DUP_COUNT="$(mysql_scalar "SELECT COUNT(*) FROM (SELECT user_id, voucher_id, COUNT(*) AS cnt FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID} GROUP BY user_id, voucher_id HAVING cnt > 1) t;")"
MYSQL_STOCK="$(mysql_scalar "SELECT COALESCE((SELECT stock FROM tb_seckill_voucher WHERE voucher_id = ${VOUCHER_ID}), -999999);")"
ORDER_STATUS_DISTRIBUTION="$(mysql_scalar "SELECT status, COUNT(*) FROM tb_voucher_order WHERE voucher_id = ${VOUCHER_ID} GROUP BY status ORDER BY status;")"
REDIS_STOCK="$("${REDIS_CLI}" GET "seckill:stock:${VOUCHER_ID}" || true)"
REDIS_STOCK="${REDIS_STOCK:-nil}"
if [[ ! "${REDIS_STOCK}" =~ ^-?[0-9]+$ ]]; then
  REDIS_STOCK_NUM=-999999
else
  REDIS_STOCK_NUM="${REDIS_STOCK}"
fi

QUEUE_INFO="$(read_queue_info)"
ORDER_QUEUE_LINE="$(echo "${QUEUE_INFO}" | awk '$1=="flashdeal.seckill.order.queue" {print $0}')"
DLQ_LINE="$(echo "${QUEUE_INFO}" | awk '$1=="flashdeal.seckill.order.dlq" {print $0}')"
ORDER_READY="$(echo "${ORDER_QUEUE_LINE}" | awk '{print $2}')"
ORDER_UNACK="$(echo "${ORDER_QUEUE_LINE}" | awk '{print $3}')"
DLQ_READY="$(echo "${DLQ_LINE}" | awk '{print $2}')"
DLQ_UNACK="$(echo "${DLQ_LINE}" | awk '{print $3}')"
ORDER_READY="${ORDER_READY:-0}"
ORDER_UNACK="${ORDER_UNACK:-0}"
DLQ_READY="${DLQ_READY:-0}"
DLQ_UNACK="${DLQ_UNACK:-0}"

echo "Seckill verification"
echo "voucherId=${VOUCHER_ID}"
echo "initialStock=${INITIAL_STOCK}"
echo "orderCount=${ORDER_COUNT}"
echo "duplicateGroupCount=${DUP_COUNT}"
echo "mysqlStock=${MYSQL_STOCK}"
echo "redisStock=${REDIS_STOCK}"
echo "orderStatusDistribution:"
if [[ -n "${ORDER_STATUS_DISTRIBUTION}" ]]; then
  echo "${ORDER_STATUS_DISTRIBUTION}" | awk '{print "status=" $1 ", count=" $2}'
else
  echo "empty"
fi
echo "seckillOrderQueue ready=${ORDER_READY}, unacked=${ORDER_UNACK}"
echo "seckillDlq ready=${DLQ_READY}, unacked=${DLQ_UNACK}"

PASS=true

if (( ORDER_COUNT + MYSQL_STOCK != INITIAL_STOCK )); then
  echo "FAIL: orderCount + mysqlStock != initialStock (${ORDER_COUNT} + ${MYSQL_STOCK} != ${INITIAL_STOCK})"
  PASS=false
else
  echo "PASS: orderCount + mysqlStock == initialStock"
fi

if (( ORDER_COUNT + REDIS_STOCK_NUM != INITIAL_STOCK )); then
  echo "FAIL: orderCount + redisStock != initialStock (${ORDER_COUNT} + ${REDIS_STOCK} != ${INITIAL_STOCK})"
  PASS=false
else
  echo "PASS: orderCount + redisStock == initialStock"
fi

if (( MYSQL_STOCK != REDIS_STOCK_NUM )); then
  echo "FAIL: mysqlStock != redisStock (${MYSQL_STOCK} != ${REDIS_STOCK})"
  PASS=false
else
  echo "PASS: mysqlStock == redisStock"
fi

if (( DUP_COUNT > 0 )); then
  echo "FAIL: duplicate user voucher orders exist"
  PASS=false
else
  echo "PASS: no duplicate orders"
fi

if (( ORDER_COUNT > INITIAL_STOCK )); then
  echo "FAIL: oversold, orderCount exceeds initialStock"
  PASS=false
else
  echo "PASS: no oversell"
fi

if (( MYSQL_STOCK < 0 )); then
  echo "FAIL: MySQL stock is negative"
  PASS=false
fi

if (( REDIS_STOCK_NUM < 0 )); then
  echo "FAIL: Redis stock is negative or missing"
  PASS=false
fi

if (( ORDER_READY != 0 || ORDER_UNACK != 0 )); then
  echo "FAIL: seckill order queue is not drained"
  PASS=false
else
  echo "PASS: seckill order queue is drained"
fi

if (( DLQ_READY != 0 || DLQ_UNACK != 0 )); then
  echo "FAIL: seckill DLQ has messages"
  PASS=false
else
  echo "PASS: seckill DLQ is empty"
fi

if [[ "${PASS}" == "true" ]]; then
  echo "PASS: seckill result verification passed"
else
  echo "FAIL: seckill result verification failed"
  exit 1
fi
