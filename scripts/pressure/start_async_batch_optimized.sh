#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_DIR}"

export RABBIT_CONCURRENCY="${RABBIT_CONCURRENCY:-4}"
export RABBIT_MAX_CONCURRENCY="${RABBIT_MAX_CONCURRENCY:-8}"
export RABBIT_PREFETCH="${RABBIT_PREFETCH:-20}"
export HIKARI_MAX_POOL_SIZE="${HIKARI_MAX_POOL_SIZE:-50}"
export HIKARI_MIN_IDLE="${HIKARI_MIN_IDLE:-10}"
export ORDER_TIMEOUT_SECONDS="${ORDER_TIMEOUT_SECONDS:-7200}"
export BATCH_SIZE="${BATCH_SIZE:-100}"
export BATCH_RECEIVE_TIMEOUT_MS="${BATCH_RECEIVE_TIMEOUT_MS:-100}"
export BATCH_PREFETCH="${BATCH_PREFETCH:-100}"
export BATCH_CONCURRENCY="${BATCH_CONCURRENCY:-1}"
export BATCH_MAX_CONCURRENCY="${BATCH_MAX_CONCURRENCY:-1}"
export BATCH_RETRY_TIMES="${BATCH_RETRY_TIMES:-2}"

echo "Stop current flash-deal.jar process if present"
if [[ -f app.pid ]]; then
  bash deploy/aliyun-ecs/stop-app.sh
fi

PIDS="$(pgrep -f 'java .*flash-deal\.jar' || true)"
if [[ -n "${PIDS}" ]]; then
  echo "Stopping flash-deal.jar pids: ${PIDS}"
  kill ${PIDS} || true
  for _ in $(seq 1 10); do
    if ! pgrep -f 'java .*flash-deal\.jar' >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

PIDS="$(pgrep -f 'java .*flash-deal\.jar' || true)"
if [[ -n "${PIDS}" ]]; then
  echo "Force stopping remaining flash-deal.jar pids: ${PIDS}"
  kill -9 ${PIDS} || true
fi

echo "Start async application with batch consumer optimization"
bash deploy/aliyun-ecs/start-app.sh true 8081 flash-deal.jar async "${ORDER_TIMEOUT_SECONDS}"

sleep 3

echo "Current flash-deal.jar process:"
ps -ef | grep '[h]mdp.jar' || true

echo "Check port 8081:"
if command -v ss >/dev/null 2>&1; then
  ss -lntp | grep ':8081' || true
elif command -v netstat >/dev/null 2>&1; then
  netstat -lntp | grep ':8081' || true
else
  echo "Neither ss nor netstat is available; skip port check."
fi

echo "You can verify with: curl http://127.0.0.1:8081"
echo "HTTP 401 is expected when the endpoint requires login."
