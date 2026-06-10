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

echo "Start optimized async application"
bash deploy/aliyun-ecs/start-app.sh true 8081 flash-deal.jar async 600

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
