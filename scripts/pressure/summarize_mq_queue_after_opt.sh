#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_DIR}"

INPUT_FILE="outputs/pressure_test/1000rps/rabbitmq_queue_after_opt.csv"
OUTPUT_FILE="outputs/pressure_test/1000rps/mq_summary_after_opt.md"

if [[ ! -f "${INPUT_FILE}" ]]; then
  echo "CSV file not found: ${INPUT_FILE}"
  exit 1
fi

mkdir -p "$(dirname "${OUTPUT_FILE}")"

awk -F, '
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
  }
  END {
    print "# RabbitMQ Queue Summary After Optimization"
    print ""
    print "| Metric | Value |"
    print "| --- | --- |"
    print "| maxReady | " maxReady " |"
    print "| maxUnacked | " maxUnacked " |"
    print "| firstTimestamp | " first " |"
    print "| lastTimestamp | " last " |"
  }
' "${INPUT_FILE}" > "${OUTPUT_FILE}"

cat "${OUTPUT_FILE}"
echo "Summary written to ${OUTPUT_FILE}"
