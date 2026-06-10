#!/usr/bin/env bash
set -euo pipefail

LOCAL_CACHE_ENABLED="${1:-true}"
PORT="${2:-8081}"
JAR_PATH="${3:-flash-deal.jar}"
SECKILL_MODE="${4:-async}"
ORDER_TIMEOUT_SECONDS="${5:-7200}"
APP_PID_FILE="app.pid"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/app.log"

RABBIT_CONCURRENCY="${RABBIT_CONCURRENCY:-4}"
RABBIT_MAX_CONCURRENCY="${RABBIT_MAX_CONCURRENCY:-8}"
RABBIT_PREFETCH="${RABBIT_PREFETCH:-20}"
HIKARI_MAX_POOL_SIZE="${HIKARI_MAX_POOL_SIZE:-50}"
HIKARI_MIN_IDLE="${HIKARI_MIN_IDLE:-10}"
BATCH_SIZE="${BATCH_SIZE:-100}"
BATCH_RECEIVE_TIMEOUT_MS="${BATCH_RECEIVE_TIMEOUT_MS:-100}"
BATCH_PREFETCH="${BATCH_PREFETCH:-100}"
BATCH_CONCURRENCY="${BATCH_CONCURRENCY:-1}"
BATCH_MAX_CONCURRENCY="${BATCH_MAX_CONCURRENCY:-1}"
BATCH_RETRY_TIMES="${BATCH_RETRY_TIMES:-2}"

if [[ "${LOCAL_CACHE_ENABLED}" != "true" && "${LOCAL_CACHE_ENABLED}" != "false" ]]; then
  echo "localCacheEnabled must be true or false"
  echo "Usage: bash start-app.sh true 8081 flash-deal.jar async"
  exit 1
fi

if [[ "${SECKILL_MODE}" != "async" && "${SECKILL_MODE}" != "sync" ]]; then
  echo "seckillMode must be async or sync"
  echo "Usage: bash start-app.sh true 8081 flash-deal.jar async 900"
  exit 1
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Jar not found: ${JAR_PATH}"
  exit 1
fi

if [[ -f "${APP_PID_FILE}" ]]; then
  OLD_PID="$(cat "${APP_PID_FILE}")"
  if [[ -n "${OLD_PID}" ]] && kill -0 "${OLD_PID}" >/dev/null 2>&1; then
    echo "Application is already running, pid=${OLD_PID}"
    exit 1
  fi
  rm -f "${APP_PID_FILE}"
fi

mkdir -p "${LOG_DIR}"

MYSQL_URL='jdbc:mysql://127.0.0.1:3307/hmdp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai&characterEncoding=utf8'

nohup java -jar "${JAR_PATH}" \
  --spring.profiles.active=dev \
  --server.port="${PORT}" \
  --spring.datasource.url="${MYSQL_URL}" \
  --spring.datasource.username=root \
  --spring.datasource.password=123456 \
  --spring.redis.host=127.0.0.1 \
  --spring.redis.port=6379 \
  --spring.redis.password= \
  --spring.rabbitmq.host=127.0.0.1 \
  --spring.rabbitmq.port=5672 \
  --spring.rabbitmq.username=guest \
  --spring.rabbitmq.password=guest \
  --local-cache.shop.enabled="${LOCAL_CACHE_ENABLED}" \
  --seckill.order.mode="${SECKILL_MODE}" \
  --order.timeout.seconds="${ORDER_TIMEOUT_SECONDS}" \
  --spring.rabbitmq.listener.simple.concurrency="${RABBIT_CONCURRENCY}" \
  --spring.rabbitmq.listener.simple.max-concurrency="${RABBIT_MAX_CONCURRENCY}" \
  --spring.rabbitmq.listener.simple.prefetch="${RABBIT_PREFETCH}" \
  --spring.datasource.hikari.maximum-pool-size="${HIKARI_MAX_POOL_SIZE}" \
  --spring.datasource.hikari.minimum-idle="${HIKARI_MIN_IDLE}" \
  --seckill.order.batch-consume.batch-size="${BATCH_SIZE}" \
  --seckill.order.batch-consume.receive-timeout-ms="${BATCH_RECEIVE_TIMEOUT_MS}" \
  --seckill.order.batch-consume.prefetch="${BATCH_PREFETCH}" \
  --seckill.order.batch-consume.concurrency="${BATCH_CONCURRENCY}" \
  --seckill.order.batch-consume.max-concurrency="${BATCH_MAX_CONCURRENCY}" \
  --seckill.order.batch-consume.retry-times="${BATCH_RETRY_TIMES}" \
  > "${LOG_FILE}" 2>&1 &

APP_PID="$!"
echo "${APP_PID}" > "${APP_PID_FILE}"

echo "Application started."
echo "pid=${APP_PID}"
echo "port=${PORT}"
echo "local-cache.shop.enabled=${LOCAL_CACHE_ENABLED}"
echo "seckill.order.mode=${SECKILL_MODE}"
echo "order.timeout.seconds=${ORDER_TIMEOUT_SECONDS}"
echo "batch.size=${BATCH_SIZE}"
echo "batch.receive.timeout.ms=${BATCH_RECEIVE_TIMEOUT_MS}"
echo "batch.prefetch=${BATCH_PREFETCH}"
echo "batch.concurrency=${BATCH_CONCURRENCY}"
echo "batch.max-concurrency=${BATCH_MAX_CONCURRENCY}"
echo "batch.retry.times=${BATCH_RETRY_TIMES}"
echo "rabbit.concurrency=${RABBIT_CONCURRENCY}"
echo "rabbit.max-concurrency=${RABBIT_MAX_CONCURRENCY}"
echo "rabbit.prefetch=${RABBIT_PREFETCH}"
echo "hikari.maximum-pool-size=${HIKARI_MAX_POOL_SIZE}"
echo "hikari.minimum-idle=${HIKARI_MIN_IDLE}"
echo "datasource.url=${MYSQL_URL}"
echo "log=${LOG_FILE}"
echo "View logs: tail -f ${LOG_FILE}"
