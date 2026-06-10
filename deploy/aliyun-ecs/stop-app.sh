#!/usr/bin/env bash
set -euo pipefail

APP_PID_FILE="app.pid"

if [[ ! -f "${APP_PID_FILE}" ]]; then
  echo "No app.pid found. Application may not be running."
  exit 0
fi

APP_PID="$(cat "${APP_PID_FILE}")"

if [[ -z "${APP_PID}" ]] || ! kill -0 "${APP_PID}" >/dev/null 2>&1; then
  echo "Process not found. Remove stale pid file: ${APP_PID_FILE}"
  rm -f "${APP_PID_FILE}"
  exit 0
fi

echo "Stopping application, pid=${APP_PID}"
kill "${APP_PID}"

for _ in {1..10}; do
  if ! kill -0 "${APP_PID}" >/dev/null 2>&1; then
    rm -f "${APP_PID_FILE}"
    echo "Application stopped."
    exit 0
  fi
  sleep 1
done

echo "Process still running after 10 seconds, force killing pid=${APP_PID}"
kill -9 "${APP_PID}" >/dev/null 2>&1 || true
rm -f "${APP_PID_FILE}"
echo "Application stopped."
