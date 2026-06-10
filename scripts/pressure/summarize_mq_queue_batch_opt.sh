#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_DIR}"

INPUT_FILE="outputs/pressure_test/1000rps/rabbitmq_queue_batch_opt.csv"
DRAIN_FILE="outputs/pressure_test/1000rps/mq_drain_batch_opt.txt"
OUTPUT_FILE="outputs/pressure_test/1000rps/mq_summary_batch_opt.md"

if [[ ! -f "${INPUT_FILE}" ]]; then
  echo "CSV file not found: ${INPUT_FILE}"
  exit 1
fi

DRAIN_SECONDS="TODO"
if [[ -f "${DRAIN_FILE}" ]]; then
  DRAIN_SECONDS="$(awk -F= '$1=="drainSeconds" {print $2}' "${DRAIN_FILE}")"
  DRAIN_SECONDS="${DRAIN_SECONDS:-TODO}"
fi

mkdir -p "$(dirname "${OUTPUT_FILE}")"

awk -F, -v drainSeconds="${DRAIN_SECONDS}" '
  NR == 1 { next }
  NF >= 4 {
    if (first == "") {
      first = $1
    }
    last = $1
    ready = $3 + 0
    unacked = $4 + 0
    if (ready > maxReady) {
      maxReady = ready
    }
    if (unacked > maxUnacked) {
      maxUnacked = unacked
    }
    if ($2 == "flashdeal.seckill.order.dlq" && ready > 0) {
      dlqReadyAppeared = "true"
    }
  }
  END {
    if (dlqReadyAppeared == "") {
      dlqReadyAppeared = "false"
    }
    print "# RabbitMQ Queue Summary After Batch Optimization"
    print ""
    print "| Metric | Value |"
    print "| --- | --- |"
    print "| maxReady | " maxReady " |"
    print "| maxUnacked | " maxUnacked " |"
    print "| firstTimestamp | " first " |"
    print "| lastTimestamp | " last " |"
    print "| drainSeconds | " drainSeconds " |"
    print "| dlqReadyAppeared | " dlqReadyAppeared " |"
  }
' "${INPUT_FILE}" > "${OUTPUT_FILE}"

cat "${OUTPUT_FILE}"
echo "Summary written to ${OUTPUT_FILE}"
