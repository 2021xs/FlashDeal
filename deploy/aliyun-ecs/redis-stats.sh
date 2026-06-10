#!/usr/bin/env bash
set -euo pipefail

REDIS_CONTAINER="${REDIS_CONTAINER:-flashdeal-redis}"
ACTION="${1:-all}"

case "${ACTION}" in
  reset)
    docker exec "${REDIS_CONTAINER}" redis-cli CONFIG RESETSTAT
    ;;
  get)
    docker exec "${REDIS_CONTAINER}" redis-cli INFO commandstats \
      | tr -d '\r' \
      | awk -F'[=,]' '/cmdstat_get:/ {print "cmdstat_get calls=" $2}'
    ;;
  all)
    docker exec "${REDIS_CONTAINER}" redis-cli INFO commandstats
    ;;
  *)
    echo "Usage: bash redis-stats.sh reset|get|all"
    exit 1
    ;;
esac
