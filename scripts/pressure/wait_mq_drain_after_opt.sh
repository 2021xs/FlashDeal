#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_DIR}"

RABBITMQ_CONTAINER="${RABBITMQ_CONTAINER:-flashdeal-rabbitmq}"
OUTPUT_DIR="outputs/pressure_test/1000rps"
OUTPUT_FILE="${OUTPUT_DIR}/mq_drain_after_opt.txt"

mkdir -p "${OUTPUT_DIR}"
START_SECONDS="$(date +%s)"

read_queues() {
  docker exec "${RABBITMQ_CONTAINER}" rabbitmqctl list_queues name messages_ready messages_unacknowledged 2>/dev/null || true
}

while true; do
  QUEUE_INFO="$(read_queues)"
  ORDER_LINE="$(echo "${QUEUE_INFO}" | awk '$1=="flashdeal.seckill.order.queue" {print $0}')"
  DLQ_LINE="$(echo "${QUEUE_INFO}" | awk '$1=="flashdeal.seckill.order.dlq" {print $0}')"

  READY="$(echo "${ORDER_LINE}" | awk '{print $2}')"
  UNACKED="$(echo "${ORDER_LINE}" | awk '{print $3}')"
  DLQ_READY="$(echo "${DLQ_LINE}" | awk '{print $2}')"
  DLQ_UNACKED="$(echo "${DLQ_LINE}" | awk '{print $3}')"

  READY="${READY:-0}"
  UNACKED="${UNACKED:-0}"
  DLQ_READY="${DLQ_READY:-0}"
  DLQ_UNACKED="${DLQ_UNACKED:-0}"

  NOW="$(date '+%Y-%m-%d %H:%M:%S')"
  echo "${NOW} ready=${READY}, unacked=${UNACKED}, dlqReady=${DLQ_READY}, dlqUnacked=${DLQ_UNACKED}"

  if (( READY == 0 && UNACKED == 0 )); then
    break
  fi

  sleep 10
done

END_SECONDS="$(date +%s)"
DRAIN_SECONDS="$((END_SECONDS - START_SECONDS))"

{
  echo "startTime=$(date -d "@${START_SECONDS}" '+%Y-%m-%d %H:%M:%S')"
  echo "endTime=$(date -d "@${END_SECONDS}" '+%Y-%m-%d %H:%M:%S')"
  echo "drainSeconds=${DRAIN_SECONDS}"
  echo "finalReady=${READY}"
  echo "finalUnacked=${UNACKED}"
  echo "finalDlqReady=${DLQ_READY}"
  echo "finalDlqUnacked=${DLQ_UNACKED}"
} > "${OUTPUT_FILE}"

echo "MQ drain result written to ${OUTPUT_FILE}"
