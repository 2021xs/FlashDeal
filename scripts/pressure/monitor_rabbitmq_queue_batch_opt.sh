#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_DIR}"

RABBITMQ_CONTAINER="${RABBITMQ_CONTAINER:-flashdeal-rabbitmq}"
OUTPUT_DIR="outputs/pressure_test/1000rps"
OUTPUT_FILE="${OUTPUT_DIR}/rabbitmq_queue_batch_opt.csv"

mkdir -p "${OUTPUT_DIR}"
echo "timestamp,queue,ready,unacked" > "${OUTPUT_FILE}"

echo "Monitoring RabbitMQ queues every 2 seconds. Press Ctrl+C to stop."
while true; do
  TS="$(date '+%Y-%m-%d %H:%M:%S')"
  QUEUE_INFO="$(docker exec "${RABBITMQ_CONTAINER}" rabbitmqctl list_queues name messages_ready messages_unacknowledged 2>/dev/null || true)"
  echo "${QUEUE_INFO}" | awk -v ts="${TS}" '
    $1=="flashdeal.seckill.order.queue" || $1=="flashdeal.seckill.order.dlq" {
      printf "%s,%s,%s,%s\n", ts, $1, $2, $3
    }
  ' | tee -a "${OUTPUT_FILE}"
  sleep 2
done
